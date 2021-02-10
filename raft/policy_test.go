package raft

import (
	"bytes"
	"github.com/golang/mock/gomock"
	"github.com/nodece/casbin-hraft-dispatcher/raft/mocks"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

//go:generate mockgen -destination ./mocks/mock_distributed_enforcer.go -package mocks github.com/casbin/casbin/v2 IDistributedEnforcer

func TestPolicyOperator_AddPolicy(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	e := mocks.NewMockIDistributedEnforcer(ctl)

	dir, err := ioutil.TempDir("", "casbin-hraft-")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	p, err := NewPolicyOperator(dir, e)
	assert.NoError(t, err)

	e.EXPECT().AddPolicySelf(gomock.Any(), "p", "p", [][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}}).Return([][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}}, nil)
	err = p.AddPolicy("p", "p", [][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}})
	assert.NoError(t, err)
}

func TestPolicyOperator_RemovePolicy(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	e := mocks.NewMockIDistributedEnforcer(ctl)

	dir, err := ioutil.TempDir("", "casbin-hraft-")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	p, err := NewPolicyOperator(dir, e)
	assert.NoError(t, err)

	e.EXPECT().RemovePolicySelf(gomock.Any(), "p", "p", [][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}}).Return([][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}}, nil)
	err = p.RemovePolicy("p", "p", [][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}})
	assert.NoError(t, err)
}

func TestPolicyOperator_RemoveFilteredPolicy(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	e := mocks.NewMockIDistributedEnforcer(ctl)

	dir, err := ioutil.TempDir("", "casbin-hraft-")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	p, err := NewPolicyOperator(dir, e)
	assert.NoError(t, err)

	e.EXPECT().RemoveFilteredPolicySelf(gomock.Any(), "p", "p", 0, "role:user").Return([][]string{{"role:user", "/", "GET"}}, nil)
	err = p.RemoveFilteredPolicy("p", "p", 0, "role:user")
	assert.NoError(t, err)
}

func TestPolicyOperator_UpdatePolicy(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	e := mocks.NewMockIDistributedEnforcer(ctl)

	dir, err := ioutil.TempDir("", "casbin-hraft-")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	p, err := NewPolicyOperator(dir, e)
	assert.NoError(t, err)

	e.EXPECT().UpdatePolicySelf(gomock.Any(), "p", "p", []string{"role:admin", "/", "*"}, []string{"role:admin", "/admin", "*"}).Return(true, nil)
	err = p.UpdatePolicy("p", "p", []string{"role:admin", "/", "*"}, []string{"role:admin", "/admin", "*"})
	assert.NoError(t, err)
}

func TestPolicyOperator_LoadPolicy(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	e := mocks.NewMockIDistributedEnforcer(ctl)

	dir, err := ioutil.TempDir("", "casbin-hraft-")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	p, err := NewPolicyOperator(dir, e)
	assert.NoError(t, err)

	e.EXPECT().AddPolicySelf(gomock.Any(), "p", "p", [][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}}).Return([][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}}, nil)
	err = p.AddPolicy("p", "p", [][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}})
	assert.NoError(t, err)

	e.EXPECT().ClearPolicySelf(gomock.Any())
	e.EXPECT().AddPolicySelf(gomock.Any(), "p", "p", [][]string{{"role:admin", "/", "*"}})
	e.EXPECT().AddPolicySelf(gomock.Any(), "p", "p", [][]string{{"role:user", "/", "GET"}})
	err = p.LoadPolicy()
	assert.NoError(t, err)
}

func TestPolicyOperator_Backup_Restore(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	e := mocks.NewMockIDistributedEnforcer(ctl)

	dir, err := ioutil.TempDir("", "casbin-hraft-")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	p, err := NewPolicyOperator(dir, e)
	assert.NoError(t, err)

	e.EXPECT().AddPolicySelf(gomock.Any(), "p", "p", [][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}}).Return([][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}}, nil)
	err = p.AddPolicy("p", "p", [][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}})
	assert.NoError(t, err)

	b, err := p.Backup()
	err = ioutil.WriteFile(path.Join(dir, "backup.db"), b, 0666)
	assert.NoError(t, err)

	err = p.Restore(ioutil.NopCloser(bytes.NewBuffer(b)))
	assert.NoError(t, err)

	e.EXPECT().ClearPolicySelf(gomock.Any())
	e.EXPECT().AddPolicySelf(gomock.Any(), "p", "p", [][]string{{"role:admin", "/", "*"}})
	e.EXPECT().AddPolicySelf(gomock.Any(), "p", "p", [][]string{{"role:user", "/", "GET"}})
	err = p.LoadPolicy()
	assert.NoError(t, err)
}
