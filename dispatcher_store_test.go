package hraftdispatcher

import (
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/nodece/casbin-hraft-dispatcher/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

//go:generate mockgen -destination mocks/mock_distributed_enforcer.go -package mocks github.com/casbin/casbin/v2 IDistributedEnforcer

func TestInmemDispatcherStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	enforcer := mocks.NewMockIDistributedEnforcer(ctrl)
	raftAddress := uuid.New().String()
	store, err := NewInmemDispatcherStore(&DispatcherConfig{
		RaftAddress: raftAddress,
		ServerID:    fmt.Sprintf("test-dispatcher-%s", raftAddress),
		Enforcer:    enforcer,
	})
	assert.NoError(t, err)

	err = store.Start()
	assert.NoError(t, err)
	sec := "p"
	pType := "p"
	rules := [][]string{{
		"role:test", "resource-1", "action-1",
		"role:test", "resource-2", "action-2",
		"role:admin", "*", "*",
	}}
	c := &Command{Operation: AddOperation, Sec: sec, Ptype: pType, Rules: rules}

	data, err := c.ToBytes()
	assert.NoError(t, err)
	enforcer.EXPECT().AddPolicySelf(gomock.Any(), "p", "p", rules).Return(rules, nil)
	err = store.Apply(data)
	assert.NoError(t, err)

	c.Operation = RemoveOperation
	data, err = c.ToBytes()
	enforcer.EXPECT().RemovePolicySelf(gomock.Any(), "p", "p", rules).Return(rules, nil)
	err = store.Apply(data)
	assert.NoError(t, err)

	fieldIndex := 0
	fieldValues := []string{"role:test"}
	c = &Command{Operation: RemoveFilteredOperation, Sec: sec, Ptype: pType, FieldIndex: fieldIndex, FieldValues: fieldValues}
	data, err = c.ToBytes()
	enforcer.EXPECT().RemoveFilteredPolicySelf(gomock.Any(), sec, pType, fieldIndex, fieldValues).Return([][]string{}, nil)
	err = store.Apply(data)
	assert.NoError(t, err)

	newRule := []string{"role:org-admin"}
	oldRule := []string{"role:admin"}
	c = &Command{Operation: UpdateOperation, Sec: sec, Ptype: pType, NewRule: newRule, OldRule: oldRule}
	data, err = c.ToBytes()
	enforcer.EXPECT().UpdatePolicySelf(gomock.Any(), sec, pType, oldRule, newRule).Return([][]string{}, nil)
	err = store.Apply(data)
	assert.NoError(t, err)
}
