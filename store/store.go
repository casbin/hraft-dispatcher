package store

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/casbin/hraft-dispatcher/store/logstore"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/hraft-dispatcher/command"
	"github.com/casbin/hraft-dispatcher/http"
	"google.golang.org/protobuf/proto"

	"github.com/hashicorp/go-multierror"

	"github.com/hashicorp/raft"
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
	dataDir       string
	serverAddress string
	serverID      string

	raft                   *raft.Raft
	raftConfig             *raft.Config
	networkTransportConfig *raft.NetworkTransportConfig
	transport              raft.Transport
	snapshotStore          raft.SnapshotStore
	logStore               raft.LogStore
	stableStore            raft.StableStore
	fms                    raft.FSM
	boltStore              *logstore.BoltStore

	enforcer casbin.IDistributedEnforcer

	// inMemory is used for testing.
	inMemory bool

	logger *zap.Logger
}

type Config struct {
	ID                     string
	Dir                    string
	NetworkTransportConfig *raft.NetworkTransportConfig
	Enforcer               casbin.IDistributedEnforcer
	RaftConfig             *raft.Config
}

// NewStore return a instance of Store.
func NewStore(logger *zap.Logger, config *Config) (*Store, error) {
	s := &Store{
		dataDir:                config.Dir,
		serverID:               config.ID,
		logger:                 logger,
		networkTransportConfig: config.NetworkTransportConfig,
		enforcer:               config.Enforcer,
		raftConfig:             config.RaftConfig,
	}

	return s, nil
}

// Start performs initialization and runs server
func (s *Store) Start(enableBootstrap bool) error {
	var config *raft.Config
	if s.raftConfig == nil {
		config = raft.DefaultConfig()
		s.raftConfig = config
	} else {
		config = s.raftConfig
	}

	if len(config.LocalID) == 0 {
		config.LocalID = raft.ServerID(s.serverID)
	}

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
		fileSnapshots, err := raft.NewFileSnapshotStore(s.dataDir, retainSnapshotCount, os.Stderr)
		if err != nil {
			s.logger.Error("failed to new file snapshot store", zap.Error(err), zap.String("raftData", s.dataDir))
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
		dbPath := filepath.Join(s.dataDir, raftDBName)
		boltDB, err := logstore.NewBoltStore(dbPath)
		if err != nil {
			s.logger.Error("failed to new bolt store", zap.Error(err), zap.String("path", dbPath))
			return err
		}

		s.boltStore = boltDB
		s.logStore = boltDB
		s.stableStore = boltDB
	}

	fsm, err := NewFSM(s.logger, s.dataDir, s.enforcer)
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
	s.logger.Info(fmt.Sprintf("listening and serving Raft on %s", transport.LocalAddr()))
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

	err := s.networkTransportConfig.Stream.Close()
	if err != nil {
		result = multierror.Append(result, err)
	}

	return result
}

// IsInitializedCluster checks whether the cluster has been initialized.
func (s *Store) IsInitializedCluster() bool {
	if _, err := os.Stat(path.Join(s.dataDir, raftDBName)); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// WaitLeader detects the leader address in the current cluster.
func (s *Store) WaitLeader() error {
	if s.raft.Leader() != "" {
		return nil
	}

	ticker := backoff.NewTicker(backoff.NewExponentialBackOff())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return errors.New("failed to detect the current leader")
		case <-ticker.C:
			if s.raft.Leader() != "" {
				ticker.Stop()
				return nil
			}
		}
	}
}

// Address returns the address of the current node.
func (s *Store) Address() string {
	return s.networkTransportConfig.Stream.Addr().String()
}

// ID returns the id of the current node.
func (s *Store) ID() string {
	return s.serverID
}

// DataDir returns the data directory of the current node.
func (s *Store) DataDir() string {
	return s.dataDir
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
func (s *Store) AddPolicies(request *command.AddPoliciesRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	cmd := &command.Command{
		Type: command.Command_COMMAND_TYPE_ADD_POLICIES,
		Data: data,
	}
	return s.applyProtoMessage(cmd)
}

// RemovePolicies implements the http.Store interface.
func (s *Store) RemovePolicies(request *command.RemovePoliciesRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	cmd := &command.Command{
		Type: command.Command_COMMAND_TYPE_REMOVE_POLICIES,
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
		Type: command.Command_COMMAND_TYPE_REMOVE_FILTERED_POLICY,
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
		Type: command.Command_COMMAND_TYPE_UPDATE_POLICY,
		Data: data,
	}
	return s.applyProtoMessage(cmd)
}

// UpdatePolicies implements the http.Store interface.
func (s *Store) UpdatePolicies(request *command.UpdatePoliciesRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	cmd := &command.Command{
		Type: command.Command_COMMAND_TYPE_UPDATE_POLICIES,
		Data: data,
	}
	return s.applyProtoMessage(cmd)
}

// ClearPolicy implements the http.Store interface.
func (s *Store) ClearPolicy() error {
	cmd := &command.Command{
		Type: command.Command_COMMAND_TYPE_CLEAR_POLICY,
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
	_ = s.WaitLeader()
	return s.raft.State() == raft.Leader, string(s.raft.Leader())
}

// Leader implements the http.Store interface.
func (s *Store) Stats() (map[string]interface{}, error) {
	result := map[string]interface{}{
		"node_id":      s.serverID,
		"node_address": s.networkTransportConfig.Stream.Addr(),
		"leader": map[string]string{
			"address": string(s.raft.Leader()),
		},
		"data_dir": s.dataDir,
	}

	return result, nil
}
