package casbin_hraft_dispatcher

import (
	"crypto/tls"
	"github.com/casbin/casbin/v2"
	"github.com/hashicorp/raft"
)

// DispatcherConfig holds dispatcher config.
type DispatcherConfig struct {
	// Enforcer is a enforcer of casbin.
	Enforcer casbin.IDistributedEnforcer
	// TLSConfig is used to configure a TLS client or server.
	TLSConfig *tls.Config
	// DataDir holds raft data, default to the DefaultDataDir.
	DataDir string
	// ServerID is a unique string identifying this server for all time, default to the RaftAddress.
	ServerID string
	// RaftAddress is a network address for raft server, default to the DefaultRaftAddress.
	RaftAddress string
	// RaftConfig is hashicorp-raft configuration, default to the raft.DefaultConfig().
	RaftConfig *raft.Config
	// HttpAddress is a network address for dispatcher backend, default to the DefaultHttpAddress.
	HttpAddress string
}
