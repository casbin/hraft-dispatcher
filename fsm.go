package hraftdispatcher

import (
	"encoding/json"
	"fmt"
	"github.com/casbin/casbin/v2"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
	"io"
	"sync"
)

// FSM is state storage.
type FSM struct {
	commands      []Command
	enforcer      casbin.IDistributedEnforcer
	shouldPersist func() bool
	mutex         *sync.RWMutex
	logger        *zap.Logger
}

func NewFSM(enforcer casbin.IDistributedEnforcer, logger *zap.Logger) (*FSM, error) {
	f := &FSM{
		enforcer:      enforcer,
		logger:        logger,
		mutex:         &sync.RWMutex{},
		shouldPersist: func() bool { return false },
	}

	return f, nil
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	if log.Type != raft.LogCommand {
		return nil
	}
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error("cannot to parse command from raft.Log", zap.Any("log", log), zap.Error(err))
		return err
	}
	return f.apply(cmd)
}

// Apply applies a Raft log entry to the casbin.
func (f *FSM) apply(cmd Command) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.commands = append(f.commands, cmd)
	switch cmd.Operation {
	case AddOperation:
		_, err := f.enforcer.AddPolicySelf(f.shouldPersist, cmd.Sec, cmd.Ptype, cmd.Rules)
		return err
	case RemoveOperation:
		_, err := f.enforcer.RemovePolicySelf(f.shouldPersist, cmd.Sec, cmd.Ptype, cmd.Rules)
		return err
	case RemoveFilteredOperation:
		_, err := f.enforcer.RemoveFilteredPolicySelf(f.shouldPersist, cmd.Sec, cmd.Ptype, cmd.FieldIndex, cmd.FieldValues...)
		return err
	case ClearOperation:
		err := f.enforcer.ClearPolicySelf(f.shouldPersist)
		return err
	case UpdateOperation:
		_, err := f.enforcer.UpdatePolicySelf(f.shouldPersist, cmd.Sec, cmd.Ptype, cmd.OldRule, cmd.NewRule)
		return err
	default:
		err := fmt.Errorf("unknown command: %v", cmd)
		f.logger.Error(err.Error())
		return err
	}
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	f.logger.Info("restore from snapshot")
	var cmds []Command
	if err := json.NewDecoder(rc).Decode(&cmds); err != nil {
		f.logger.Error("cannot to restore from snapshot", zap.Error(err))
		return err
	}

	f.mutex.Lock()
	f.commands = cmds
	f.mutex.Unlock()

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Info("persist the fsm snapshot")
	var cmds []Command

	f.mutex.RLock()
	for _, item := range f.commands {
		cmds = append(cmds, item)
	}
	f.mutex.RUnlock()

	return &fsmSnapshot{commands: cmds}, nil
}
