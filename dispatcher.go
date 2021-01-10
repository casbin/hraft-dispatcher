package casbin_hraft_dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/hashicorp/go-multierror"
	"net/http"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

// HRaftDispatcher implementation a dispatcher base on Hashicorp's Raft for Casbin.
type HRaftDispatcher struct {
	client            *http.Client
	logger            *zap.Logger
	config            *DispatcherConfig
	dispatcherBackend *DispatcherBackend
	dispatcherStore   DispatcherStore
}

// NewHRaftDispatcher creates a HRaftDispatcher.
func NewHRaftDispatcher(config *DispatcherConfig) (*HRaftDispatcher, error) {
	d := &HRaftDispatcher{config: config, logger: zap.NewExample()}
	return d, d.initialize()
}

// initialize is used to initialize the dispatcher config.
func (h *HRaftDispatcher) initialize() error {
	if h.config.TLSConfig == nil {
		return errors.New("TLSConfig is required")
	}
	if h.config.Enforcer == nil {
		return errors.New("Enforcer is required")
	}

	if h.config.HttpAddress == "" {
		h.config.HttpAddress = DefaultHttpAddress
	}
	if h.config.RaftAddress == "" {
		h.config.RaftAddress = DefaultRaftAddress
	}
	if h.config.RaftConfig == nil {
		h.config.RaftConfig = raft.DefaultConfig()
	}
	if h.config.DataDir == "" {
		h.config.DataDir = DefaultDataDir
	}
	if h.config.ServerID == "" {
		h.config.ServerID = h.config.RaftAddress
	}
	h.client = &http.Client{
		Transport: &http2.Transport{
			AllowHTTP:       false,
			TLSClientConfig: h.config.TLSConfig,
		},
	}
	dispatcherStore, err := NewDispatcherStore(h.config)
	if err != nil {
		return err
	}
	h.dispatcherStore = dispatcherStore

	dispatcherBackend, err := NewDispatcherBackend(h.config.HttpAddress, h.config.TLSConfig, dispatcherStore)
	if err != nil {
		return err
	}
	h.dispatcherBackend = dispatcherBackend

	return nil
}

// Start is used to start the all server.
func (h *HRaftDispatcher) Start() error {
	err := h.dispatcherStore.Start()
	if err != nil {
		return err
	}
	return h.dispatcherBackend.Start()
}

// Stop is used to stop the all server.
func (h *HRaftDispatcher) Stop() error {
	var result error
	storeErr := h.dispatcherStore.Stop()
	if storeErr != nil {
		h.logger.Error("failed to stop the raft server", zap.Error(storeErr))
		result = multierror.Append(result, storeErr)
	}

	backendErr := h.dispatcherBackend.Stop(context.Background())
	if backendErr != nil {
		h.logger.Error("failed to stop the backend server", zap.Error(backendErr))
		result = multierror.Append(result, backendErr)
	}

	return result
}

// requestBackend requests the dispatcher backend.
func (h *HRaftDispatcher) requestBackend(command Command) error {
	b, _ := json.Marshal(command)
	resp, err := h.client.Post(h.config.HttpAddress, "application/json", bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	} else {
		h.logger.Error("dispatcher backend is not available", zap.String("status", resp.Status))
		return errors.New("server is not available")
	}
}

// AddPolicies adds policies to enforcer.
func (h *HRaftDispatcher) AddPolicies(sec string, ptype string, rules [][]string) error {
	command := Command{
		Operation: AddOperation,
		Sec:       sec,
		Ptype:     ptype,
		Rules:     rules,
	}
	return h.requestBackend(command)
}

// RemovePolicies removes policies from enforcer.
func (h *HRaftDispatcher) RemovePolicies(sec string, ptype string, rules [][]string) error {
	command := Command{
		Operation: RemoveOperation,
		Sec:       sec,
		Ptype:     ptype,
		Rules:     rules,
	}
	return h.requestBackend(command)
}

// RemoveFilteredPolicy removes a role inheritance rule from the current named policy, field filters can be specified.
func (h *HRaftDispatcher) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	command := Command{
		Operation:   RemoveFilteredOperation,
		Sec:         sec,
		Ptype:       ptype,
		FieldIndex:  fieldIndex,
		FieldValues: fieldValues,
	}
	return h.requestBackend(command)
}

// ClearPolicy clears all policy.
func (h *HRaftDispatcher) ClearPolicy() error {
	command := Command{
		Operation: ClearOperation,
	}
	return h.requestBackend(command)
}

// UpdatePolicy updates policy rule from all instance.
func (h *HRaftDispatcher) UpdatePolicy(sec string, ptype string, oldRule, newRule []string) error {
	command := Command{
		Operation: UpdateOperation,
		Sec:       sec,
		Ptype:     ptype,
		OldRule:   oldRule,
		NewRule:   newRule,
	}
	return h.requestBackend(command)
}
