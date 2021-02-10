package hraftdispatcher

import (
	"crypto/tls"
	"github.com/hashicorp/raft"
	"github.com/nodece/casbin-hraft-dispatcher/http"
	"github.com/nodece/casbin-hraft-dispatcher/store"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type HRaftDispatcher struct {
	store     http.Store
	tlsConfig *tls.Config

	logger *zap.Logger
}

func NewHRaftDispatcher(config Config) (*HRaftDispatcher, error) {
	if config.Enforcer == nil {
		return nil, errors.New("Enforcer is not provided in config")
	}

	if len(config.DataDir) == 0 {
		return nil, errors.New("DataDir is not provided in config")
	}

	if len(config.RaftListenAddress) == 0 {
		return nil, errors.New("RaftListenAddress is not provided in config")
	}

	if len(config.HttpListenAddress) == 0 {
		return nil, errors.New("HttpListenAddress is not provided in config")
	}

	if config.TLSConfig == nil {
		return nil, errors.New("TLSConfig is not provided in config")
	}

	if len(config.ServerID) == 0 {
		config.ServerID = config.RaftListenAddress
	}

	logger := zap.NewExample()

	streamLayer, err := store.NewTCPStreamLayer(config.RaftListenAddress, config.TLSConfig)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	storeConfig := &store.Config{
		ID:      config.ServerID,
		Address: config.RaftListenAddress,
		Dir:     config.DataDir,
		NetworkTransportConfig: &raft.NetworkTransportConfig{
			Stream:  streamLayer,
			MaxPool: 5,
			Logger:  nil,
		},
		Enforcer: config.Enforcer,
	}
	s, err := store.NewStore(storeConfig)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	isNewCluster := !s.IsInitializedCluster()
	enableBootstrap := false

	if isNewCluster == true {
		enableBootstrap = true
	}

	if len(config.JoinAddress) != 0 {
		enableBootstrap = false
	}

	if enableBootstrap {
		logger.Info("bootstrapping a new cluster")
	}

	err = s.Start(enableBootstrap)
	if err != nil {
		logger.Error("failed to start raft service", zap.Error(err))
		return nil, err
	}

	if isNewCluster && config.JoinAddress != config.RaftListenAddress && len(config.JoinAddress) != 0 {
		err = s.JoinNode(config.ServerID, config.RaftListenAddress)
		if err != nil {
			logger.Error("failed to join the current node to existing cluster", zap.Error(err))
			return nil, err
		}
	}

	httpService, err := http.NewService(config.HttpListenAddress, config.TLSConfig, s)
	if err != nil {
		return nil, err
	}

	err = httpService.Start()
	if err != nil {
		return nil, err
	}

	return &HRaftDispatcher{
		store:     s,
		tlsConfig: config.TLSConfig,
		logger:    logger,
	}, nil
}
