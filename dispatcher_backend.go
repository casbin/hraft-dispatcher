package casbin_hraft_dispatcher

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"time"

	"golang.org/x/net/http2"

	"go.uber.org/zap"
)

type DispatcherBackend struct {
	srv    *http.Server
	logger *zap.Logger
	store  DispatcherStore
}

func NewDispatcherBackend(address string, tlsConfig *tls.Config, store DispatcherStore) (*DispatcherBackend, error) {
	if tlsConfig == nil {
		return nil, errors.New("tlsConfig cannot be nil")
	}

	d := &DispatcherBackend{
		logger: zap.NewExample(),
		store:  store,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/commands", d.commandsHandler)
	mux.HandleFunc("/nodes", d.commandsHandler)

	srv := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       5 * time.Minute,
		TLSConfig:         tlsConfig,
	}
	d.srv = srv
	_ = http2.ConfigureServer(srv, nil)

	return d, nil
}

func (d *DispatcherBackend) Start() error {
	return d.srv.ListenAndServeTLS("", "")
}

func (d *DispatcherBackend) Stop(ctx context.Context) error {
	return d.srv.Shutdown(ctx)
}

func (d *DispatcherBackend) redirectToLeaderServer(w http.ResponseWriter, r *http.Request, host string) {
	u := r.URL
	redirectURL := fmt.Sprintf("%s:%s%s", u.Scheme, host, u.Path)
	d.logger.Info(fmt.Sprintf("redirect the request from %s to %s", r.RequestURI, redirectURL), zap.String("leaderAddr", host))
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

func (d *DispatcherBackend) commandsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		d.logger.Error("not allowed http method")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var cmd Command
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		d.logger.Error("failed to read the request body", zap.Error(err))
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	err = json.Unmarshal(body, &cmd)
	if err != nil {
		d.logger.Error("failed to decode the command", zap.Error(err), zap.ByteString("body", body))
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	isLeader, leaderAddr := d.store.Leader()
	if isLeader {
		err := d.store.Apply(body)
		if err != nil {
			d.logger.Error("failed to apply the command", zap.Error(err), zap.Reflect("command", cmd))
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	} else {
		if leaderAddr == "" {
			d.logger.Error("cannot get the leader address")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		d.redirectToLeaderServer(w, r, leaderAddr)
	}
}

func (d *DispatcherBackend) nodesHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusServiceUnavailable)
}
