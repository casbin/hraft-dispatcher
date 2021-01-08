package casbin_hraft_dispatcher

import (
	"github.com/hashicorp/go-multierror"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"

	"go.uber.org/zap"
)

//go:generate mockgen -destination mock/mock_dispatcher_store.go -package mock -source dispatcher_store.go

type DispatcherStore interface {
	Start() error
	Stop() error
	AddNode(serverID string, address string) error
	RemoveNode(serverID string) error
	Apply(buf []byte) error
	Leader() (bool, string)
}

var _ DispatcherStore = &DefaultDispatcherStore{}

// DefaultDispatcherStore is a casbin enforcer backend by raft
type DefaultDispatcherStore struct {
	*DispatcherConfig

	logger *zap.Logger

	// inMemory is used for testing.
	inMemory bool

	raft          *raft.Raft
	transport     raft.Transport
	snapshotStore raft.SnapshotStore
	logStore      raft.LogStore
	stableStore   raft.StableStore
	fms           raft.FSM
	boltStore     *raftboltdb.BoltStore
}

// NewDispatcher return a instance of dispatcher.
func NewDispatcherStore(config *DispatcherConfig) (*DefaultDispatcherStore, error) {
	d := &DefaultDispatcherStore{DispatcherConfig: config}
	return d, nil
}

func (d *DefaultDispatcherStore) ensureLeader() bool {
	return d.raft.State() == raft.Leader
}

// Start performs initialization and runs server
func (d *DefaultDispatcherStore) Start() error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(d.ServerID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", d.RaftAddress)
	transport, err := NewTCPTransport(d.RaftAddress, addr, d.TLSConfig, 3, 10*time.Second, os.Stderr)
	if err != nil {
		d.logger.Error("failed to new tcp transport", zap.Error(err), zap.String("raftAddress", d.RaftAddress))
		return err
	}
	d.transport = transport

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(d.DataDir, RetainSnapshotCount, os.Stderr)
	if err != nil {
		d.logger.Error("failed to new file snapshot store", zap.Error(err), zap.String("dataDir", d.DataDir))
		return err
	}
	d.snapshotStore = snapshots

	// Create the log store and stable store.
	dbPath := filepath.Join(d.DataDir, FileDatabaseName)
	boltDB, err := raftboltdb.NewBoltStore(dbPath)
	if err != nil {
		d.logger.Error("failed to new bolt store", zap.Error(err), zap.String("path", dbPath))
		return err
	}

	d.boltStore = boltDB
	d.logStore = boltDB
	d.stableStore = boltDB

	// Create fms
	fsm, err := NewFSM(d.Enforcer, d.logger)
	if err != nil {
		d.logger.Error("failed to new fsm", zap.Error(err))
		return err
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, fsm, d.logStore, d.stableStore, d.snapshotStore, d.transport)
	if err != nil {
		d.logger.Error("failed to new raft", zap.Error(err))
		return err
	}
	d.raft = ra

	// Must add checking leader to fsm
	fsm.SetEnsureLeader(d.ensureLeader)

	// Runs Raft server
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(d.ServerID),
				Address: transport.LocalAddr(),
			},
		},
	}

	f := ra.BootstrapCluster(configuration)
	if f.Error() != nil {
		d.logger.Error("failed to boostrap cluster", zap.Error(err))
		return err
	}

	return nil
}

// Stop is used to close the raft node, which always returns nil.
func (d *DefaultDispatcherStore) Stop() error {
	var result error
	s := d.raft.Shutdown()
	if s.Error() != nil {
		d.logger.Error("failed to stop the raft server", zap.Error(s.Error()))
		result = multierror.Append(result, s.Error())
	}

	err := d.boltStore.Close()
	if err != nil {
		d.logger.Error("failed to close bolt database", zap.Error(s.Error()))
		result = multierror.Append(result, s.Error())
	}

	return result
}

// AddNode adds a new node to Cluster.
func (d *DefaultDispatcherStore) AddNode(serverID string, address string) error {
	i := d.raft.AddVoter(raft.ServerID(serverID), raft.ServerAddress(address), 0, RaftTimeout)
	return i.Error()
}

// RemoveNode removes a new node from Cluster.
func (d *DefaultDispatcherStore) RemoveNode(serverID string) error {
	i := d.raft.RemoveServer(raft.ServerID(serverID), 0, RaftTimeout)
	return i.Error()
}

func (d *DefaultDispatcherStore) Apply(cmd []byte) error {
	d.logger.Info("apply command to raft", zap.ByteString("command", cmd))
	return d.raft.Apply(cmd, RaftTimeout).Error()
}

func (d *DefaultDispatcherStore) Leader() (isLeader bool, leaderAddr string) {
	leaderAddr = string(d.raft.Leader())
	return d.raft.State() == raft.Leader, leaderAddr
}
