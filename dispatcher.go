package hraftdispatcher

import (
	"crypto/tls"

	"github.com/casbin/casbin/v2/persist"
	"github.com/hashicorp/raft"
	"github.com/nodece/casbin-hraft-dispatcher/command"
	"github.com/nodece/casbin-hraft-dispatcher/http"
	"github.com/nodece/casbin-hraft-dispatcher/store"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var _ persist.Dispatcher = &HRaftDispatcher{}

// HRaftDispatcher implements the persist.Dispatcher interface.
type HRaftDispatcher struct {
	store       http.Store
	tlsConfig   *tls.Config
	httpService *http.Service
	logger      *zap.Logger
}

// NewHRaftDispatcher returns a HRaftDispatcher.
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
		store:       s,
		tlsConfig:   config.TLSConfig,
		httpService: httpService,
		logger:      logger,
	}, nil
}

//AddPolicies implements the persist.Dispatcher interface.
func (h *HRaftDispatcher) AddPolicies(sec string, pType string, rules [][]string) error {
	var items []*command.StringArray
	for _, rule := range rules {
		var item = &command.StringArray{Items: rule}
		items = append(items, item)
	}

	addPolicyRequest := &command.AddPolicyRequest{
		Sec:   sec,
		PType: pType,
		Rules: items,
	}
	return h.httpService.DoAddPolicyRequest(addPolicyRequest)
}

// RemovePolicies implements the persist.Dispatcher interface.
func (h *HRaftDispatcher) RemovePolicies(sec string, pType string, rules [][]string) error {
	var items []*command.StringArray
	for _, rule := range rules {
		var item = &command.StringArray{Items: rule}
		items = append(items, item)
	}

	request := &command.RemovePolicyRequest{
		Sec:   sec,
		PType: pType,
		Rules: items,
	}
	return h.httpService.DoRemovePolicyRequest(request)
}

// RemoveFilteredPolicy implements the persist.Dispatcher interface.
func (h *HRaftDispatcher) RemoveFilteredPolicy(sec string, pType string, fieldIndex int, fieldValues ...string) error {
	request := &command.RemoveFilteredPolicyRequest{
		Sec:        sec,
		PType:      pType,
		FieldIndex: int32(fieldIndex),
	}
	return h.httpService.DoRemoveFilteredPolicyRequest(request)
}

// ClearPolicy implements the persist.Dispatcher interface.
func (h *HRaftDispatcher) ClearPolicy() error {
	return h.httpService.DoClearPolicyRequest()
}

// UpdatePolicy implements the persist.Dispatcher interface.
func (h *HRaftDispatcher) UpdatePolicy(sec string, pType string, oldRule, newRule []string) error {
	request := &command.UpdatePolicyRequest{
		Sec:     sec,
		PType:   pType,
		OldRule: oldRule,
		NewRule: newRule,
	}
	return h.httpService.DoUpdatePolicyRequest(request)
}
