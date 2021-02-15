package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/hashicorp/go-multierror"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"golang.org/x/net/http2"

	"github.com/go-chi/chi"

	"github.com/nodece/casbin-hraft-dispatcher/command"

	"go.uber.org/zap"
)

//go:generate mockgen -destination ./mocks/mock_store.go -package mocks -source service.go

// Store provides an interface that can be implemented by raft.
type Store interface {
	// AddPolicy adds a set of rules to the current policy.
	AddPolicy(request *command.AddPolicyRequest) error
	// RemovePolicy removes a set of rules from the current policy.
	RemovePolicy(request *command.RemovePolicyRequest) error
	// RemoveFilteredPolicy removes a set of rules that match a pattern from the current policy.
	RemoveFilteredPolicy(request *command.RemoveFilteredPolicyRequest) error
	// UpdatePolicy updates a rule of policy.
	UpdatePolicy(request *command.UpdatePolicyRequest) error
	// ClearPolicy clears all policies.
	ClearPolicy() error

	// JoinNode joins a node with a given serverID and network address to cluster.
	JoinNode(serverID string, address string) error
	// RemoveNode removes a node with a given serverID from cluster.
	RemoveNode(serverID string) error
	// Leader checks if it is a leader and returns network address.
	Leader() (bool, string)
}

// Service setups a HTTP service for forward data of raft node.
type Service struct {
	srv        *http.Server
	ln         net.Listener
	store      Store
	httpClient *http.Client

	logger *zap.Logger
}

// NewService creates a Service.
func NewService(address string, tlsConfig *tls.Config, store Store) (*Service, error) {
	if store == nil {
		return nil, errors.New("store is not provided")
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http2.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	s := &Service{
		logger:     zap.NewExample(),
		store:      store,
		httpClient: httpClient,
	}

	r := chi.NewRouter()
	r.With(s.leaderMiddleware).Route("/policies", func(r chi.Router) {
		r.Put("/add", s.handleAddPolicy)
		r.Put("/update", s.handleUpdatePolicy)
		r.Put("/remove", s.handleRemovePolicy)
	})
	r.With(s.leaderMiddleware).Route("/nodes", func(r chi.Router) {
		r.Put("/join", s.handleJoinNode)
		r.Put("/remove", s.handleRemoveNode)
	})

	s.srv = &http.Server{
		Addr:              address,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       5 * time.Minute,
		TLSConfig:         tlsConfig,
	}

	return s, nil
}

// leaderMiddleware checks whether the current node is the leader.
// If this current node is not a leader, the request is forwarded to the leader node.
func (s *Service) leaderMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isLeader, leaderAddr := s.store.Leader()
		if !isLeader {
			http.Redirect(w, r, s.getRedirectURL(r, leaderAddr), http.StatusTemporaryRedirect)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Start starts this service.
// It always returns a non-nil error. After Shutdown or Close, the returned error is http.ErrServerClosed.
func (s *Service) Start() error {
	_ = http2.ConfigureServer(s.srv, nil)

	addr := s.srv.Addr
	if addr == "" {
		addr = ":https"
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.logger.Info(fmt.Sprintf("linstening on %s", ln.Addr()))
	s.ln = ln

	go func() {
		err = s.srv.ServeTLS(ln, "", "")
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
	u := r.URL
	rq := r.URL.RawQuery
	if rq != "" {
		rq = fmt.Sprintf("?%s", rq)
	}
	return fmt.Sprintf("%s://%s%s%s", u.Scheme, host, r.URL.Path, rq)
}

// handleNodes handles request of nodes.
func (s *Service) handleNodes(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusServiceUnavailable)
}

// handleAddPolicy handles the request to add a set of rules.
func (s *Service) handleAddPolicy(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var cmd command.AddPolicyRequest
	err = jsoniter.Unmarshal(data, &cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = s.store.AddPolicy(&cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
}

// handleRemovePolicy handles the request to remove a set of rules.
func (s *Service) handleRemovePolicy(w http.ResponseWriter, r *http.Request) {
	removeType := r.URL.Query().Get("type")
	switch removeType {
	case "all":
		err := s.store.ClearPolicy()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
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
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	case "":
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var cmd command.RemovePolicyRequest
		err = jsoniter.Unmarshal(data, &cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = s.store.RemovePolicy(&cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

// handleUpdatePolicy handles the request to update a rule.
func (s *Service) handleUpdatePolicy(w http.ResponseWriter, r *http.Request) {
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
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
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
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
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
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
}

func (s *Service) Addr() string {
	return s.ln.Addr().String()
}

func (s *Service) DoAddPolicyRequest(request *command.AddPolicyRequest) error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}

	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/policies/add", s.Addr()), bytes.NewBuffer(b))
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

func (s *Service) DoRemovePolicyRequest(request *command.RemovePolicyRequest) error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/policies/remove", s.Addr()), bytes.NewBuffer(b))
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

	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/policies/remove?type=filtered", s.Addr()), bytes.NewBuffer(b))
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
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/policies/remove?type=all", s.Addr()), nil)
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
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/policies/update", s.Addr()), bytes.NewBuffer(b))
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

func (s *Service) DoJoinNodeRequest(request *command.AddNodeRequest)  error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/nodes/join", s.Addr()), bytes.NewBuffer(b))
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

func (s *Service) DoRemoveNodeRequest(request *command.RemoveNodeRequest)  error {
	b, err := jsoniter.Marshal(request)
	if err != nil {
		return err
	}
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/nodes/remove", s.Addr()), bytes.NewBuffer(b))
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
	tr := &http2.Transport{
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

	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/nodes/join", clusterAddress), bytes.NewBuffer(b))
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
