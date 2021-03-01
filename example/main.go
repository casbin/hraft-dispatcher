package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/go-chi/chi"
	jsoniter "github.com/json-iterator/go"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	hraftdispatcher "github.com/casbin/hraft-dispatcher"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type TLS struct {
	RootCert string `yaml:"rootCert"`
	Cert     string `yaml:"cert"`
	Key      string `yaml:"key"`
}

type Config struct {
	TLS           TLS    `yaml:"tls"`
	ServerID      string `yaml:"serverID"`
	DataDir       string `yaml:"dataDir"`
	JoinAddress   string `yaml:"joinAddress"`
	ListenAddress string `yaml:"listenAddress"`

	HTTPListenAddress string `yaml:"httpListenAddress"`
}

func main() {
	var configFile string
	flag.StringVar(&configFile, "config-file", "", "The path to the configuration file.")
	if len(os.Args) < 1 {
		log.Fatalf(`Usage: %s --config-file=""`, os.Args[0])
	}
	flag.Parse()

	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to read the configuration filed"))
	}

	var config Config
	err = yaml.Unmarshal(b, &config)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to unmarshal the configuration"))
	}

	// Load cert
	rootCAPool := x509.NewCertPool()
	rootCA, err := ioutil.ReadFile(config.TLS.RootCert)
	if err != nil {
		log.Fatal(err)
	}
	rootCAPool.AppendCertsFromPEM(rootCA)

	cert, err := tls.LoadX509KeyPair(config.TLS.Cert, config.TLS.Key)
	if err != nil {
		log.Fatal(err)
	}

	tlsConfig := &tls.Config{
		RootCAs:      rootCAPool,
		ClientCAs:    rootCAPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{cert},
	}

	// New a Enforcer
	var modelText = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub) && r.obj == p.obj && r.act == p.act
`
	m, err := model.NewModelFromString(modelText)
	if err != nil {
		log.Fatal(err)
	}

	// Adapter is not required here.
	// Dispatcher = Adapter + Watcher
	e, err := casbin.NewDistributedEnforcer(m)
	if err != nil {
		log.Fatal(err)
	}

	// New a Dispatcher
	dispatcher, err := hraftdispatcher.NewHRaftDispatcher(&hraftdispatcher.Config{
		Enforcer:      e,
		JoinAddress:   config.JoinAddress,
		ListenAddress: config.ListenAddress,
		TLSConfig:     tlsConfig,
		DataDir:       config.DataDir,
	})
	if err != nil {
		log.Fatal(err)
	}

	e.SetDispatcher(dispatcher)

	mountHTTP(config.HTTPListenAddress, e)

	done := make(chan bool, 1)
	quit := make(chan os.Signal, 1)

	signal.Notify(quit, os.Interrupt)

	go func() {
		<-quit
		log.Println("server is shutting down...")

		if err := dispatcher.Shutdown(); err != nil {
			log.Fatalf("failed to shutdown the server: %v\n", err)
		}
		close(done)
	}()

	<-done
	log.Println("server stopped")
}

func mountHTTP(address string, enforcer casbin.IDistributedEnforcer) {
	r := chi.NewRouter()
	r.Put("/policies", func(writer http.ResponseWriter, request *http.Request) {
		json, err := ioutil.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		var data []interface{}
		err = jsoniter.Unmarshal(json, &data)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		_, err = enforcer.AddPolicy(data...)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusServiceUnavailable)
			return
		}
	})

	r.Delete("/policies", func(writer http.ResponseWriter, request *http.Request) {
		json, err := ioutil.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		var data []interface{}
		err = jsoniter.Unmarshal(json, &data)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		_, err = enforcer.RemovePolicy(data...)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusServiceUnavailable)
			return
		}
	})

	r.Put("/enforcer", func(writer http.ResponseWriter, request *http.Request) {
		json, err := ioutil.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		var data []interface{}
		err = jsoniter.Unmarshal(json, &data)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		ok, err := enforcer.Enforce(data...)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if ok {
			writer.Write([]byte("Authorized"))
		} else {
			writer.Write([]byte("Unauthorized"))
		}
	})

	go func() {
		err := http.ListenAndServe(address, r)
		if err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
}
