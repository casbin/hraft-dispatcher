package casbin_hraft_dispatcher

import (
	"bytes"
	"encoding/json"
	"golang.org/x/net/http2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"

	"go.uber.org/zap"

	"github.com/pkg/errors"
)

//go:generate mockgen -destination mock/mock_dispatcher_store.go -package mock -source dispatcher_store.go

type DispatcherStore interface {
	Join(serverID string, httpAddress string, raftAddress string) error
	Leader() (bool, string)
	Apply(buf []byte) error
}

// DefaultDispatcherStore is a casbin enforcer backend by raft
type DefaultDispatcherStore struct {
	*DispatcherOption

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
	client        http.Client
}

// NewDispatcher return a instance of dispatcher.
func NewDispatcher(opt *DispatcherOption) (*DefaultDispatcherStore, error) {
	d := &DefaultDispatcherStore{DispatcherOption: opt}
	return d, nil
}

// initialize initializes and checks the server configuration.
func (d *DefaultDispatcherStore) initialize() error {
	if d.Enforcer == nil {
		return errors.New("Enforcer is required")
	}

	if d.TLSConfig == nil {
		return errors.New("TLSConfig is required")
	}

	if d.RaftAddress == "" {
		d.RaftAddress = DefaultRaftAddress
	}

	if d.RaftConfig == nil {
		d.RaftConfig = raft.DefaultConfig()
	}

	if d.DataDir == "" {
		d.DataDir = DefaultDataDir
	}

	if d.ServerID == "" {
		d.ServerID = d.RaftAddress
	}

	if d.logger == nil {
		d.logger = zap.NewExample()
	}

	d.client = http.Client{
		Transport: &http2.Transport{
			AllowHTTP:       false,
			TLSClientConfig: d.TLSConfig,
		},
	}
	return nil
}

func (d *DefaultDispatcherStore) ensureLeader() bool {
	return d.raft.State() == raft.Leader
}

// Start performs initialization and runs server
func (d *DefaultDispatcherStore) Start() error {
	err := d.initialize()
	if err != nil {
		d.logger.Error("cannot to initialize the server", zap.Error(err))
		return err
	}

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(d.ServerID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", d.RaftAddress)
	// TODO using TLS
	transport, err := raft.NewTCPTransport(d.RaftAddress, addr, 3, 10*time.Second, os.Stderr)
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

// Stop close the raft node and http server
func (d *DefaultDispatcherStore) Stop() error {
	s := d.raft.Shutdown()

	if s.Error() != nil {
		d.logger.Error("failed to stop the raft server", zap.Error(s.Error()))
		return s.Error()
	}

	err := d.boltStore.Close()
	if err != nil {
		d.logger.Error("failed to close bolt database", zap.Error(s.Error()))
		return err
	}

	return nil
}

// AddMember adds a new node to Cluster.
func (d *DefaultDispatcherStore) AddMember(id string, address string) error {
	i := d.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, 0)
	return i.Error()
}

// removeMember removes a new node from Cluster.
func (d *DefaultDispatcherStore) RemoveMember(id string, address string) error {
	i := d.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return i.Error()
}

func (d *DefaultDispatcherStore) requestBackend(command Command) error {
	b, _ := json.Marshal(command)
	resp, err := d.client.Post(d.HttpAddress, "application/json", bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	} else {
		d.logger.Error("dispatcher backend is not available", zap.String("status", resp.Status))
		return errors.New("server is not available")
	}
}

// AddPolicies adds policies to enforcer
func (d *DefaultDispatcherStore) AddPolicies(sec string, ptype string, rules [][]string) error {
	command := Command{
		Operation: addOperation,
		Sec:       sec,
		Ptype:     ptype,
		Rules:     rules,
	}
	return d.requestBackend(command)
}

// RemovePolicies removes policies from enforcer
func (d *DefaultDispatcherStore) RemovePolicies(sec string, ptype string, rules [][]string) error {
	command := Command{
		Operation: removeOperation,
		Sec:       sec,
		Ptype:     ptype,
		Rules:     rules,
	}
	return d.requestBackend(command)
}

// RemoveFilteredPolicy removes a role inheritance rule from the current named policy, field filters can be specified.
func (d *DefaultDispatcherStore) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	command := Command{
		Operation:   removeFilteredOperation,
		Sec:         sec,
		Ptype:       ptype,
		FieldIndex:  fieldIndex,
		FieldValues: fieldValues,
	}
	return d.requestBackend(command)
}

// ClearPolicy clears all policy.
func (d *DefaultDispatcherStore) ClearPolicy() error {
	command := Command{
		Operation: clearOperation,
	}
	return d.requestBackend(command)
}

// UpdatePolicy updates policy rule from all instance.
func (d *DefaultDispatcherStore) UpdatePolicy(sec string, ptype string, oldRule, newRule []string) error {
	command := Command{
		Operation: updateOperation,
		Sec:       sec,
		Ptype:     ptype,
		OldRule:   oldRule,
		NewRule:   newRule,
	}
	return d.requestBackend(command)
}

func (d *DefaultDispatcherStore) Apply(cmd []byte) error {
	d.logger.Info("apply command to raft", zap.ByteString("command", cmd))
	return d.raft.Apply(cmd, RaftTimeout).Error()
}

func (d *DefaultDispatcherStore) Leader() (isLeader bool, leaderAddr string) {
	leaderAddr = string(d.raft.Leader())
	return d.raft.State() == raft.Leader, leaderAddr
}
