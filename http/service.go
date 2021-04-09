package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"golang.org/x/net/http2"

	"github.com/hashicorp/raft"

	"github.com/hashicorp/go-multierror"

	"github.com/go-chi/chi"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"

	"github.com/casbin/hraft-dispatcher/command"

	"go.uber.org/zap"
)

//go:generate mockgen -destination ./mocks/mock_store.go -package mocks -source service.go

// Store provides an interface that can be implemented by raft.
type Store interface {
	// AddPolicies adds a set of rules to the current policy.
	AddPolicies(request *command.AddPoliciesRequest) error
	// RemovePolicies removes a set of rules from the current policy.
	RemovePolicies(request *command.RemovePoliciesRequest) error
	// RemoveFilteredPolicy removes a set of rules that match a pattern from the current policy.
	RemoveFilteredPolicy(request *command.RemoveFilteredPolicyRequest) error
	// UpdatePolicy updates a rule of policy.
	UpdatePolicy(request *command.UpdatePolicyRequest) error
	// UpdatePolicies updates a set of rules of policy.
	UpdatePolicies(request *command.UpdatePoliciesRequest) error
	// ClearPolicy clears all policies.
	ClearPolicy() error

	// JoinNode joins a node with a given serverID and network address to cluster.
	JoinNode(serverID string, address string) error
	// RemoveNode removes a node with a given serverID from cluster.
	RemoveNode(serverID string) error
	// Leader checks if it is a leader and returns network address.
	Leader() (bool, string)

	// Stats returns stats.
	Stats() (map[string]interface{}, error)
}

// Service setups a HTTP service for forward data of raft node.
type Service struct {
	srv        *http.Server
	ln         net.Listener
	store      Store
	httpClient *http.Client
	tlsConfig  *tls.Config
	logger     *zap.Logger
}

// NewService creates a Service.
func NewService(ln net.Listener, tlsConfig *tls.Config, store Store) (*Service, error) {
	if ln == nil {
		return nil, errors.New("net.Listener is provided")
	}

	if store == nil {
		return nil, errors.New("store is not provided")
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	if tlsConfig != nil {
		// TODO: using http2.Transport always return unexpected eof on cmux.
		httpClient.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	s := &Service{
		logger:     zap.NewExample(),
		store:      store,
		httpClient: httpClient,
		ln:         ln,
		tlsConfig:  tlsConfig,
	}

	r := chi.NewRouter()
	r.Route("/policies", func(r chi.Router) {
		r.Put("/add", s.handleAddPolicy)
		r.Put("/update", s.handleUpdatePolicy)
		r.Put("/remove", s.handleRemovePolicy)
	})
	r.Route("/nodes", func(r chi.Router) {
		r.Put("/join", s.handleJoinNode)
		r.Put("/remove", s.handleRemoveNode)
	})

	// add pprof
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)

	s.srv = &http.Server{
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       5 * time.Minute,
		TLSConfig:         tlsConfig,
	}

	if tlsConfig != nil {
		_ = http2.ConfigureServer(s.srv, nil)
	}

	return s, nil
}

// Start starts this service.
// It always returns a non-nil error. After Shutdown or Close, the returned error is http.ErrServerClosed.
func (s *Service) Start() error {
	go func() {
		var err error
		if s.tlsConfig == nil {
			s.logger.Info(fmt.Sprintf("listening and serving HTTP on %s", s.ln.Addr()))
		} else {
			s.logger.Info(fmt.Sprintf("listening and serving HTTPS on %s", s.ln.Addr()))
		}
		err = s.srv.Serve(s.ln)
		if err != nil && err != http.ErrServerClosed {
			s.logger.Error("unable to serve http", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops this service.
func (s *Service) Stop(ctx context.Context) error {
	var ret error
	err := s.srv.Shutdown(ctx)
	if err != nil {
		ret = multierror.Append(ret, err)
	}
	err = s.ln.Close()
	if err != nil {
		ret = multierror.Append(ret, err)
	}
	return ret
}

// getRedirectURL returns a URL by the given host.
func (s *Service) getRedirectURL(r *http.Request, host string) string {
	rq := r.URL.RawQuery
	if rq != "" {
		rq = fmt.Sprintf("?%s", rq)
	}

	var scheme string
	if strings.HasPrefix(r.RequestURI, "https") {
		scheme = "https"
	} else if strings.HasPrefix(r.RequestURI, "http") {
		scheme = "http"
	}

	if len(scheme) == 0 {
		scheme = s.GetScheme()
	}

	return fmt.Sprintf("%s://%s%s%s", scheme, host, r.URL.Path, rq)
}

func (s *Service) GetScheme() string {
	scheme := "https"
	if s.tlsConfig == nil {
		scheme = "http"
	}
	return scheme
}

// handleStoreResponse checks the error returned by store.
// If the error is nil, the server returns http.StatusOK.
// If the error is raft.ErrNotLeader, the server forward the request to the leader node,
// otherwise the server returns http.StatusServiceUnavailable.
func (s *Service) handleStoreResponse(err error, w http.ResponseWriter, r *http.Request) {
	if err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	if err == raft.ErrNotLeader {
		isLeader, leaderAddr := s.store.Leader()
		if !isLeader {
			if len(leaderAddr) == 0 {
				s.logger.Error("failed to get the leader address")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			redirectURL := s.getRedirectURL(r, leaderAddr)
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return
		}
	} else {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}

// handleAddPolicy handles the request to add a set of rules.
func (s *Service) handleAddPolicy(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var cmd command.AddPoliciesRequest
	err = jsoniter.Unmarshal(data, &cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = s.store.AddPolicies(&cmd)
	s.handleStoreResponse(err, w, r)
}

// handleRemovePolicy handles the request to remove a set of rules.
func (s *Service) handleRemovePolicy(w http.ResponseWriter, r *http.Request) {
	removeType := r.URL.Query().Get("type")
	switch removeType {
	case "all":
		err := s.store.ClearPolicy()
		s.handleStoreResponse(err, w, r)
	case "filtered":
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var cmd command.RemoveFilteredPolicyRequest
		err = jsoniter.Unmarshal(data, &cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = s.store.RemoveFilteredPolicy(&cmd)
		s.handleStoreResponse(err, w, r)
	case "":
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var cmd command.RemovePoliciesRequest
		err = jsoniter.Unmarshal(data, &cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = s.store.RemovePolicies(&cmd)
		s.handleStoreResponse(err, w, r)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

// handleUpdatePolicy handles the request to update a rule.
func (s *Service) handleUpdatePolicy(w http.ResponseWriter, r *http.Request) {
	updateType := r.URL.Query().Get("type")
	switch updateType {
	case "batch":
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var cmd command.UpdatePoliciesRequest
		err = jsoniter.Unmarshal(data, &cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = s.store.UpdatePolicies(&cmd)
		s.handleStoreResponse(err, w, r)
	case "":
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var cmd command.UpdatePolicyRequest
		err = jsoniter.Unmarshal(data, &cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = s.store.UpdatePolicy(&cmd)
		s.handleStoreResponse(err, w, r)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (s *Service) handleJoinNode(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var cmd command.AddNodeRequest
	err = jsoniter.Unmarshal(data, &cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = s.store.JoinNode(cmd.Id, cmd.Address)
	s.handleStoreResponse(err, w, r)
}

func (s *Service) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var cmd command.RemoveNodeRequest
	err = jsoniter.Unmarshal(data, &cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = s.store.RemoveNode(cmd.Id)
	s.handleStoreResponse(err, w, r)
}

func (s *Service) Addr() string {
	return s.ln.Addr().String()
}

func (s *Service) DoAddPolicyRequest(request *command.AddPoliciesRequest) error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}

	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s://%s/policies/add", s.GetScheme(), s.Addr()), bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	resp, err := s.httpClient.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(http.StatusText(http.StatusServiceUnavailable))
	}

	return nil
}

func (s *Service) DoRemovePolicyRequest(request *command.RemovePoliciesRequest) error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s://%s/policies/remove", s.GetScheme(), s.Addr()), bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	resp, err := s.httpClient.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(http.StatusText(http.StatusServiceUnavailable))
	}

	return nil
}

func (s *Service) DoRemoveFilteredPolicyRequest(request *command.RemoveFilteredPolicyRequest) error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}

	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s://%s/policies/remove?type=filtered", s.GetScheme(), s.Addr()), bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	resp, err := s.httpClient.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(http.StatusText(http.StatusServiceUnavailable))
	}

	return nil
}

func (s *Service) DoClearPolicyRequest() error {
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s://%s/policies/remove?type=all", s.GetScheme(), s.Addr()), nil)
	if err != nil {
		return err
	}

	resp, err := s.httpClient.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(http.StatusText(http.StatusServiceUnavailable))
	}

	return nil
}

func (s *Service) DoUpdatePolicyRequest(request *command.UpdatePolicyRequest) error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s://%s/policies/update", s.GetScheme(), s.Addr()), bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	resp, err := s.httpClient.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(http.StatusText(http.StatusServiceUnavailable))
	}

	return nil
}

func (s *Service) DoUpdatePoliciesRequest(request *command.UpdatePoliciesRequest) error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s://%s/policies/update?type=batch", s.GetScheme(), s.Addr()), bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	resp, err := s.httpClient.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(http.StatusText(http.StatusServiceUnavailable))
	}

	return nil
}

func (s *Service) DoJoinNodeRequest(request *command.AddNodeRequest) error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s://%s/nodes/join", s.GetScheme(), s.Addr()), bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	resp, err := s.httpClient.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(http.StatusText(http.StatusServiceUnavailable))
	}

	return nil
}

func (s *Service) DoRemoveNodeRequest(request *command.RemoveNodeRequest) error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s://%s/nodes/remove", s.GetScheme(), s.Addr()), bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	resp, err := s.httpClient.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(http.StatusText(http.StatusServiceUnavailable))
	}

	return nil
}

func DoJoinNodeRequest(clusterAddress string, nodeID string, nodeAddress string, tlsConfig *tls.Config) error {
	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	client := http.Client{Transport: tr}

	data := &command.AddNodeRequest{
		Address: nodeAddress,
		Id:      nodeID,
	}

	b, err := jsoniter.Marshal(data)
	if err != nil {
		return err
	}

	scheme := "https"
	if tlsConfig == nil {
		scheme = "http"
	}
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s://%s/nodes/join", scheme, clusterAddress), bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	resp, err := client.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(http.StatusText(http.StatusServiceUnavailable))
	}

	return nil
}
