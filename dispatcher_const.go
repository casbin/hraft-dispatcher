package hraftdispatcher

import "time"

const (
	RetainSnapshotCount = 2
	RaftTimeout         = 10 * time.Second
	FileDatabaseName    = "casbin-raft"
	DefaultDataDir      = "casbin-raft-data"
	DefaultRaftAddress  = "0.0.0.0:6789"
	DefaultHttpAddress  = "0.0.0.0:6790"
)
