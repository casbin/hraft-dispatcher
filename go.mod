module github.com/nodece/casbin-hraft-dispatcher

go 1.15

require (
	github.com/casbin/casbin/v2 v2.20.1
	github.com/go-chi/chi v1.5.1
	github.com/golang/mock v1.4.4
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hashicorp/raft v1.2.0
	github.com/hashicorp/raft-boltdb v0.0.0-20171010151810-6e5ba93211ea
	github.com/json-iterator/go v1.1.10
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.6.1
	go.etcd.io/bbolt v1.3.5
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20190813141303-74dc4d7220e7
	golang.org/x/sys v0.0.0-20200223170610-d5e6a3e2c0ae // indirect
	golang.org/x/text v0.3.3 // indirect
	google.golang.org/protobuf v1.25.0
)
