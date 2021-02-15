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
	// HttpListenAddress is a network address for http service.
	// example: 10.0.10.10:6780
	HttpListenAddress string
	// RaftListenAddress is a network address for raft server.
	// example: 10.0.10.10:6790
	RaftListenAddress string
	// TLSConfig is used to configure a TLS server and client.
	// You have to provide a peer certificate.
	// We recommend using cfssl tool to create this certificates.
	TLSConfig *tls.Config
}
