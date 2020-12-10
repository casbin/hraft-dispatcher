package casbin_hashicorp_raft

import (
	"encoding/json"
	"github.com/casbin/casbin/v2"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/pkg/errors"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	defaultDataDir      = "casbin-raft-data"
	fileDatabaseName    = "casbin-raft"
)

// Dispatcher is a casbin enforcer backend by raft
type Dispatcher struct {
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
}

type DispatcherOption struct {
	// Enforcer is a enforcer of casbin.
	Enforcer casbin.IDistributedEnforcer
	// DataDir holds raft data.
	// The default is DefaultDataDir
	DataDir string
	// ServerID is a unique string identifying this server for all time.
	// The default is ServerAddress value.
	ServerID string
	// ServerAddress is a network address for this server that a transport can contact.
	// The default is "0.0.0.0:6789".
	ServerAddress string
	// RaftConfig is hashicorp-raft configuration.
	// The default use raft.DefaultConfig().
	RaftConfig *raft.Config
}

// NewDispatcher return a instance of dispatcher.
func NewDispatcher(opt *DispatcherOption) (*Dispatcher, error) {
	return &Dispatcher{DispatcherOption: opt}, nil
}

// initialize initializes and checks the server configuration.
func (d *Dispatcher) initialize() error {
	if d.ServerAddress == "" {
		return errors.New("ServerAddress is required")
	}

	if d.Enforcer == nil {
		return errors.New("Enforcer is required")
	}

	if d.RaftConfig == nil {
		d.RaftConfig = raft.DefaultConfig()
	}

	if d.DataDir == "" {
		d.DataDir = defaultDataDir
	}

	if d.ServerID == "" {
		d.ServerID = d.ServerAddress
	}

	if d.logger == nil {
		d.logger = zap.NewExample()
	}

	return nil
}

func (d *Dispatcher) ensureLeader() bool {
	return d.raft.State() == raft.Leader
}

// Start performs initialization and runs server
func (d *Dispatcher) Start() error {
	err := d.initialize()
	if err != nil {
		d.logger.Error("cannot to initialize the server", zap.Error(err))
		return err
	}

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(d.ServerID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", d.ServerAddress)
	transport, err := raft.NewTCPTransport(d.ServerAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		d.logger.Error("cannot to new tcp transport", zap.Error(err), zap.String("serverAddress", d.ServerAddress))
		return err
	}
	d.transport = transport

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(d.DataDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		d.logger.Error("cannot to new file snapshot store", zap.Error(err), zap.String("dataDir", d.DataDir))
		return err
	}
	d.snapshotStore = snapshots

	// Create the log store and stable store.
	dbPath := filepath.Join(d.DataDir, fileDatabaseName)
	boltDB, err := raftboltdb.NewBoltStore(dbPath)
	if err != nil {
		d.logger.Error("cannot to new bolt store", zap.Error(err), zap.String("path", dbPath))
		return err
	}

	d.boltStore = boltDB
	d.logStore = boltDB
	d.stableStore = boltDB

	// Create fms
	fsm, err := NewFSM(d.Enforcer, d.logger)
	if err != nil {
		d.logger.Error("cannot to new fsm", zap.Error(err))
		return err
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, fsm, d.logStore, d.stableStore, d.snapshotStore, d.transport)
	if err != nil {
		d.logger.Error("cannot to new raft", zap.Error(err))
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
		d.logger.Error("cannot to boostrap cluster", zap.Error(err))
		return err
	}

	return nil
}

// Stop close the raft node and http server
func (d *Dispatcher) Stop() error {
	s := d.raft.Shutdown()

	if s.Error() != nil {
		d.logger.Error("cannot to stop the raft server", zap.Error(s.Error()))
		return s.Error()
	}

	err := d.boltStore.Close()
	if err != nil {
		d.logger.Error("cannot to close bolt database", zap.Error(s.Error()))
		return err
	}

	return nil
}

// AddMember adds a new node to Cluster.
func (d *Dispatcher) AddMember(id string, address string) error {
	i := d.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, 0)
	return i.Error()
}

// removeMember removes a new node from Cluster.
func (d *Dispatcher) RemoveMember(id string, address string) error {
	i := d.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return i.Error()
}

// AddPolicies adds policies to casbin enforcer
func (d *Dispatcher) AddPolicies(sec string, ptype string, rules [][]string) error {
	command := Command{
		Operation: addOperation,
		Sec:       sec,
		Ptype:     ptype,
		Rules:     rules,
	}

	buf, err := json.Marshal(command)
	if err != nil {
		return err
	}

	return d.raft.Apply(buf, raftTimeout).Error()
}

// RemovePolicies removes policies from casbin enforcer
func (d *Dispatcher) RemovePolicies(sec string, ptype string, rules [][]string) error {
	command := Command{
		Operation: removeOperation,
		Sec:       sec,
		Ptype:     ptype,
		Rules:     rules,
	}

	buf, err := json.Marshal(command)
	if err != nil {
		return err
	}

	return d.raft.Apply(buf, raftTimeout).Error()
}

// RemoveFilteredPolicy removes a role inheritance rule from the current named policy, field filters can be specified.
func (d *Dispatcher) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	command := Command{
		Operation:   removeFilteredOperation,
		Sec:         sec,
		Ptype:       ptype,
		FieldIndex:  fieldIndex,
		FieldValues: fieldValues,
	}

	buf, err := json.Marshal(command)
	if err != nil {
		return err
	}

	return d.raft.Apply(buf, raftTimeout).Error()
}

// ClearPolicy clears all policy.
func (d *Dispatcher) ClearPolicy() error {
	command := Command{
		Operation: clearOperation,
	}

	buf, err := json.Marshal(command)
	if err != nil {
		return err
	}

	return d.raft.Apply(buf, raftTimeout).Error()
}

// UpdatePolicy updates policy rule from all instance.
func (d *Dispatcher) UpdatePolicy(sec string, ptype string, oldRule, newRule []string) error {
	command := Command{
		Operation: updateOperation,
		Sec:       sec,
		Ptype:     ptype,
		OldRule:   oldRule,
		NewRule:   newRule,
	}

	buf, err := json.Marshal(command)
	if err != nil {
		return err
	}

	return d.raft.Apply(buf, raftTimeout).Error()
}
