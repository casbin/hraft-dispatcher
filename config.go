package hraftdispatcher

import (
	"crypto/tls"
	"github.com/casbin/casbin/v2"
)

// Config holds dispatcher config.
type Config struct {
	// Enforcer is a enforcer of casbin.
	Enforcer casbin.IDistributedEnforcer
	// ServerID is a unique string identifying this server for all time.
	ServerID string
	// JoinAddress is used to tells the current node to join an existing cluster.
	JoinAddress string
	// DataDir holds raft data.
	DataDir string
	// RaftListenAddress is a network address for raft server.
	// It should be noted that we will use the port of this address plus an offset of 1 as the listen address of the HTTP server.
	// If set to 10.0.10.10:6790, the Raft server runs on 10.0.10.10:6790, the HTTP server runs on10.0.10.10:6791.
	RaftListenAddress string
	// TLSConfig is used to configure a TLS server and client.
	// You have to provide a peer certificate.
	// We recommend using cfssl tool to create this certificates.
	TLSConfig *tls.Config
}
