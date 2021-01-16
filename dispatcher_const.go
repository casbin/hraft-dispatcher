package hraftdispatcher

import "time"

const (
	RetainSnapshotCount = 2
	RaftTimeout         = 10 * time.Second
	FileDatabaseName    = "casbin-hraft"
	DefaultDataDir      = "casbin-hraft-data"
	DefaultHttpAddress  = ":6790"
)
