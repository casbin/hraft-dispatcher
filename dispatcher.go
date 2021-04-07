package hraftdispatcher

import (
	"context"
	"crypto/tls"
	"github.com/soheilhy/cmux"
	"net"

	"github.com/hashicorp/go-multierror"

	"github.com/casbin/casbin/v2/persist"
	"github.com/casbin/hraft-dispatcher/command"
	"github.com/casbin/hraft-dispatcher/http"
	"github.com/casbin/hraft-dispatcher/store"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var _ persist.Dispatcher = &HRaftDispatcher{}

// HRaftDispatcher implements the persist.Dispatcher interface.
type HRaftDispatcher struct {
	store       http.Store
	tlsConfig   *tls.Config
	httpService *http.Service
	shutdownFn  func() error

	logger *zap.Logger
}

// NewHRaftDispatcher returns a HRaftDispatcher.
func NewHRaftDispatcher(config *Config) (*HRaftDispatcher, error) {
	if config == nil {
		return nil, errors.New("config is not provided")
	}

	if config.Enforcer == nil {
		return nil, errors.New("Enforcer is not provided in config")
	}

	if len(config.DataDir) == 0 {
		return nil, errors.New("DataDir is not provided in config")
	}

	if len(config.ListenAddress) == 0 {
		return nil, errors.New("ListenAddress is not provided in config")
	}

	if len(config.ServerID) == 0 {
		config.ServerID = config.ListenAddress
	}

	logger := zap.NewExample()

	var ln net.Listener
	var err error
	if config.TLSConfig == nil {
		ln, err = net.Listen("tcp", config.ListenAddress)
	} else {
		ln, err = tls.Listen("tcp", config.ListenAddress, config.TLSConfig)
	}
	if err != nil {
		return nil, err
	}

	mux := cmux.New(ln)
	httpLn := mux.Match(cmux.HTTP1Fast())
	raftLn := mux.Match(cmux.Any())
	go mux.Serve()

	streamLayer, err := store.NewTCPStreamLayer(raftLn, config.TLSConfig)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	storeConfig := &store.Config{
		ID:  config.ServerID,
		Dir: config.DataDir,
		NetworkTransportConfig: &raft.NetworkTransportConfig{
			Stream:  streamLayer,
			MaxPool: 5,
			Logger:  nil,
		},
		Enforcer:   config.Enforcer,
		RaftConfig: config.RaftConfig,
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
	} else {
		logger.Info("skip bootstrapping a new cluster")
	}

	err = s.Start(enableBootstrap)
	if err != nil {
		logger.Error("failed to start raft service", zap.Error(err))
		return nil, err
	}

	if enableBootstrap {
		err = s.WaitLeader()
		if err != nil {
			logger.Error(err.Error())
		}
	}

	if isNewCluster && config.JoinAddress != config.ListenAddress && len(config.JoinAddress) != 0 {
		logger.Info("start joining the current node to existing cluster")
		err = http.DoJoinNodeRequest(config.JoinAddress, config.ServerID, config.ListenAddress, config.TLSConfig)
		if err != nil {
			logger.Error("failed to join the current node to existing cluster", zap.String("nodeID", config.ServerID), zap.String("nodeAddress", config.ListenAddress), zap.String("clusterAddress", config.JoinAddress), zap.Error(err))
			return nil, err
		}
		logger.Info("the current node has joined to existing cluster")
	}

	httpService, err := http.NewService(httpLn, config.TLSConfig, s)
	if err != nil {
		return nil, err
	}

	err = httpService.Start()
	if err != nil {
		return nil, err
	}

	h := &HRaftDispatcher{
		store:       s,
		tlsConfig:   config.TLSConfig,
		httpService: httpService,
		logger:      logger,
	}

	h.shutdownFn = func() error {
		var ret error

		err := s.Stop()
		if err != nil {
			ret = multierror.Append(ret, err)
		}

		err = httpService.Stop(context.Background())
		if err != nil {
			ret = multierror.Append(ret, err)
		}

		err = ln.Close()
		if err != nil {
			ret = multierror.Append(ret, err)
		}

		return ret
	}

	return h, nil
}

//AddPolicies implements the persist.Dispatcher interface.
func (h *HRaftDispatcher) AddPolicies(sec string, pType string, rules [][]string) error {
	var items []*command.StringArray
	for _, rule := range rules {
		var item = &command.StringArray{Items: rule}
		items = append(items, item)
	}

	addPolicyRequest := &command.AddPoliciesRequest{
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

	request := &command.RemovePoliciesRequest{
		Sec:   sec,
		PType: pType,
		Rules: items,
	}
	return h.httpService.DoRemovePolicyRequest(request)
}

// RemoveFilteredPolicy implements the persist.Dispatcher interface.
func (h *HRaftDispatcher) RemoveFilteredPolicy(sec string, pType string, fieldIndex int, fieldValues ...string) error {
	request := &command.RemoveFilteredPolicyRequest{
		Sec:         sec,
		PType:       pType,
		FieldIndex:  int32(fieldIndex),
		FieldValues: fieldValues,
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

// UpdatePolicies implements the persist.Dispatcher interface.
func (h *HRaftDispatcher) UpdatePolicies(sec string, pType string, oldRules, newRules [][]string) error {
	var olds []*command.StringArray
	for _, rule := range oldRules {
		var item = &command.StringArray{Items: rule}
		olds = append(olds, item)
	}

	var news []*command.StringArray
	for _, rule := range newRules {
		var item = &command.StringArray{Items: rule}
		news = append(news, item)
	}

	request := &command.UpdatePoliciesRequest{
		Sec:      sec,
		PType:    pType,
		OldRules: olds,
		NewRules: news,
	}
	return h.httpService.DoUpdatePoliciesRequest(request)
}

// JoinNode joins a node to the current cluster.
func (h *HRaftDispatcher) JoinNode(serverID, serverAddress string) error {
	request := &command.AddNodeRequest{
		Id:      serverID,
		Address: serverAddress,
	}
	return h.httpService.DoJoinNodeRequest(request)
}

// JoinNode joins a node from the current cluster.
func (h *HRaftDispatcher) RemoveNode(serverID string) error {
	request := &command.RemoveNodeRequest{
		Id: serverID,
	}
	return h.httpService.DoRemoveNodeRequest(request)
}

// Shutdown is used to close the http and raft service.
func (h *HRaftDispatcher) Shutdown() error {
	return h.shutdownFn()
}

// Stats is used to get stats of currently service.
func (h *HRaftDispatcher) Stats() (map[string]interface{}, error) {
	return h.store.Stats()
}
