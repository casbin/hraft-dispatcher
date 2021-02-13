package store

import (
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/casbin/casbin/v2"
	"github.com/nodece/casbin-hraft-dispatcher/command"
	"github.com/nodece/casbin-hraft-dispatcher/http"
	"google.golang.org/protobuf/proto"

	"github.com/hashicorp/go-multierror"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"

	"go.uber.org/zap"
)

const (
	raftDBName          = "raft.db"
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

var _ http.Store = &Store{}

// Store is responsible for synchronization policy and storage policy by Raft protocol.
type Store struct {
	raftDir       string
	serverAddress string
	serverID      string

	ln                     raft.Transport
	raft                   *raft.Raft
	networkTransportConfig *raft.NetworkTransportConfig
	transport              raft.Transport
	snapshotStore          raft.SnapshotStore
	logStore               raft.LogStore
	stableStore            raft.StableStore
	fms                    raft.FSM
	boltStore              *raftboltdb.BoltStore

	enforcer casbin.IDistributedEnforcer

	// inMemory is used for testing.
	inMemory bool

	logger *zap.Logger
}

type Config struct {
	ID                     string
	Address                string
	Dir                    string
	NetworkTransportConfig *raft.NetworkTransportConfig
	Enforcer               casbin.IDistributedEnforcer
}

// NewStore return a instance of Store.
func NewStore(config *Config) (*Store, error) {
	s := &Store{
		raftDir:                config.Dir,
		serverID:               config.ID,
		serverAddress:          config.Address,
		logger:                 zap.NewExample(),
		networkTransportConfig: config.NetworkTransportConfig,
		enforcer:               config.Enforcer,
	}

	return s, nil
}

// Start performs initialization and runs server
func (s *Store) Start(enableBootstrap bool) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.serverID)

	var transport raft.Transport
	if s.inMemory {
		_, transport = raft.NewInmemTransport(raft.ServerAddress(s.serverAddress))
	} else {
		transport = raft.NewNetworkTransportWithConfig(s.networkTransportConfig)
	}
	s.transport = transport

	var snapshots raft.SnapshotStore
	if s.inMemory {
		snapshots = raft.NewInmemSnapshotStore()
	} else {
		fileSnapshots, err := raft.NewFileSnapshotStore(s.raftDir, retainSnapshotCount, os.Stderr)
		if err != nil {
			s.logger.Error("failed to new file snapshot store", zap.Error(err), zap.String("raftData", s.raftDir))
			return err
		}
		snapshots = fileSnapshots
	}
	s.snapshotStore = snapshots

	if s.inMemory {
		inMemStore := raft.NewInmemStore()
		s.logStore = inMemStore
		s.stableStore = inMemStore
	} else {
		dbPath := filepath.Join(s.raftDir, raftDBName)
		boltDB, err := raftboltdb.NewBoltStore(dbPath)
		if err != nil {
			s.logger.Error("failed to new bolt store", zap.Error(err), zap.String("path", dbPath))
			return err
		}

		s.boltStore = boltDB
		s.logStore = boltDB
		s.stableStore = boltDB
	}

	fsm, err := NewFSM(s.raftDir, s.enforcer)
	if err != nil {
		s.logger.Error("failed to new fsm", zap.Error(err))
		return err
	}

	ra, err := raft.NewRaft(config, fsm, s.logStore, s.stableStore, s.snapshotStore, s.transport)
	if err != nil {
		s.logger.Error("failed to new raft", zap.Error(err))
		return err
	}
	s.raft = ra

	if enableBootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(s.serverID),
					Address: s.transport.LocalAddr(),
				},
			},
		}

		f := ra.BootstrapCluster(configuration)
		if f.Error() != nil && f.Error() != raft.ErrCantBootstrap {
			s.logger.Error("failed to boostrap cluster", zap.Error(f.Error()))
			return f.Error()
		}
	}

	return nil
}

// Stop is used to close the raft node, which always returns nil.
func (s *Store) Stop() error {
	var result error
	shutdown := s.raft.Shutdown()
	if shutdown.Error() != nil {
		s.logger.Error("failed to stop the raft server", zap.Error(shutdown.Error()))
		result = multierror.Append(result, shutdown.Error())
	}

	if !s.inMemory {
		err := s.boltStore.Close()
		if err != nil {
			s.logger.Error("failed to close bolt database", zap.Error(err))
			result = multierror.Append(result, err)
		}
	}

	return result
}

// IsInitializedCluster checks whether the cluster has been initialized.
func (s *Store) IsInitializedCluster() bool {
	if _, err := os.Stat(path.Join(s.raftDir, raftDBName)); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// applyProtoMessage applies a proto message.
func (s *Store) applyProtoMessage(m proto.Message) error {
	cmd, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return s.raft.Apply(cmd, raftTimeout).Error()
}

// AddPolicy implements the http.Store interface.
func (s *Store) AddPolicy(request *command.AddPolicyRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	cmd := &command.Command{
		Type: command.Command_COMMAND_TYPE_ADD,
		Data: data,
	}
	return s.applyProtoMessage(cmd)
}

// RemovePolicy implements the http.Store interface.
func (s *Store) RemovePolicy(request *command.RemovePolicyRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	cmd := &command.Command{
		Type: command.Command_COMMAND_TYPE_REMOVE,
		Data: data,
	}
	return s.applyProtoMessage(cmd)
}

// RemoveFilteredPolicy implements the http.Store interface.
func (s *Store) RemoveFilteredPolicy(request *command.RemoveFilteredPolicyRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	cmd := &command.Command{
		Type: command.Command_COMMAND_TYPE_REMOVE_FILTERED,
		Data: data,
	}
	return s.applyProtoMessage(cmd)
}

// UpdatePolicy implements the http.Store interface.
func (s *Store) UpdatePolicy(request *command.UpdatePolicyRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	cmd := &command.Command{
		Type: command.Command_COMMAND_TYPE_UPDATE,
		Data: data,
	}
	return s.applyProtoMessage(cmd)
}

// ClearPolicy implements the http.Store interface.
func (s *Store) ClearPolicy() error {
	cmd := &command.Command{
		Type: command.Command_COMMAND_TYPE_CLEAR,
		Data: nil,
	}
	return s.applyProtoMessage(cmd)
}

// JoinNode implements the http.Store interface.
func (s *Store) JoinNode(serverID string, address string) error {
	i := s.raft.AddVoter(raft.ServerID(serverID), raft.ServerAddress(address), 0, 0)
	return i.Error()
}

// RemoveNode implements the http.Store interface.
func (s *Store) RemoveNode(serverID string) error {
	i := s.raft.RemoveServer(raft.ServerID(serverID), 0, 0)
	return i.Error()
}

// Leader implements the http.Store interface.
func (s *Store) Leader() (bool, string) {
	return s.raft.State() == raft.Leader, string(s.raft.Leader())
}
