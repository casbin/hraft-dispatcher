package e2e

import (
	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	gormadapter "github.com/casbin/gorm-adapter/v3"
	hraftdispatcher "github.com/nodece/casbin-hraft-dispatcher"
	"github.com/stretchr/testify/assert"
	"testing"
)

func newEnforcer() (casbin.IDistributedEnforcer, error) {
	a, err := gormadapter.NewAdapter("postgres", "postgresql://postgres:qplaFceC@127.0.01:5433/casbin")
	if err != nil {
		return nil, err
	}

	m, err := model.NewModelFromString(`
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
`)
	if err != nil {
		return nil, err
	}

	e, err := casbin.NewDistributedEnforcer(m, a)
	if err != nil {
		return nil, err
	}

	return e, nil
}
func TestDispatcher(t *testing.T) {
	e, err := newEnforcer()
	assert.NoError(t, err)

	dispatcher, err := hraftdispatcher.NewHRaftDispatcher(&hraftdispatcher.DispatcherConfig{
		Enforcer:  e,
		TLSConfig: nil,
	})
	assert.NoError(t, err)

}
