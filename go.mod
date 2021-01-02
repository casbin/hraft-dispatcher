module github.com/nodece/casbin-hraft-dispatcher

go 1.15

require (
	github.com/casbin/casbin/v2 v2.19.4
	github.com/hashicorp/raft v1.2.0
	github.com/hashicorp/raft-boltdb v0.0.0-20171010151810-6e5ba93211ea
	github.com/pkg/errors v0.8.1
	go.etcd.io/bbolt v1.3.5 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0
)
