package raft

import (
	"fmt"

	"github.com/nodece/casbin-hraft-dispatcher/command"
	"google.golang.org/protobuf/proto"

	"io"

	"github.com/casbin/casbin/v2"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// FSM is state storage.
type FSM struct {
	logger         *zap.Logger
	policyOperator *PolicyOperator
}

// NewFSM returns a FSM.
func NewFSM(path string, enforcer casbin.IDistributedEnforcer) (*FSM, error) {
	p, err := NewPolicyOperator(path, enforcer)
	if err != nil {
		return nil, err
	}

	f := &FSM{
		logger:         zap.NewExample(),
		policyOperator: p,
	}
	return f, err
}

// Apply applies log from raft.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd command.Command
	err := proto.Unmarshal(log.Data, &cmd)
	if err != nil {
		f.logger.Error("cannot to unmarshal the command", zap.Error(err), zap.ByteString("command", log.Data))
		return err
	}
	switch cmd.Type {
	case command.Command_COMMAND_TYPE_ADD:
		var request command.AddPolicyRequest
		err := proto.Unmarshal(cmd.Data, &request)
		if err != nil {
			f.logger.Error("cannot to unmarshal the request", zap.Error(err), zap.ByteString("request", cmd.Data))
			return err
		}
		var rules [][]string
		for _, rule := range request.Rules {
			rules = append(rules, rule.GetItems())
		}
		err = f.policyOperator.AddPolicy(request.Sec, request.PType, rules)
		if err != nil {
			f.logger.Error("apply the add policy request failed", zap.Error(err), zap.String("request", request.String()))
		}
		return err
	case command.Command_COMMAND_TYPE_REMOVE:
		var request command.RemovePolicyRequest
		err := proto.Unmarshal(cmd.Data, &request)
		if err != nil {
			f.logger.Error("cannot to unmarshal the request", zap.Error(err), zap.ByteString("request", cmd.Data))
			return err
		}
		var rules [][]string
		for _, rule := range request.Rules {
			rules = append(rules, rule.GetItems())
		}
		err = f.policyOperator.RemovePolicy(request.Sec, request.PType, rules)
		if err != nil {
			f.logger.Error("apply the remove policy request failed", zap.Error(err), zap.String("request", request.String()))
		}
		return err
	case command.Command_COMMAND_TYPE_REMOVE_FILTERED:
		var request command.RemoveFilteredPolicyRequest
		err := proto.Unmarshal(cmd.Data, &request)
		if err != nil {
			f.logger.Error("cannot to unmarshal the request", zap.Error(err), zap.ByteString("request", cmd.Data))
			return err
		}
		err = f.policyOperator.RemoveFilteredPolicy(request.Sec, request.PType, int(request.FieldIndex), request.FieldValues...)
		if err != nil {
			f.logger.Error("apply the remove filtered policy request failed", zap.Error(err), zap.String("request", request.String()))
		}
		return err
	case command.Command_COMMAND_TYPE_UPDATE:
		var request command.UpdatePolicyRequest
		err := proto.Unmarshal(cmd.Data, &request)
		if err != nil {
			f.logger.Error("cannot to unmarshal the request", zap.Error(err), zap.ByteString("request", cmd.Data))
			return err
		}
		err = f.policyOperator.UpdatePolicy(request.Sec, request.PType, request.OldRule, request.NewRule)
		if err != nil {
			f.logger.Error("apply the update policy request failed", zap.Error(err), zap.String("request", request.String()))
		}
		return err
	case command.Command_COMMAND_TYPE_CLEAR:
		err := f.policyOperator.ClearPolicy()
		if err != nil {
			f.logger.Error("apply the clear policy request failed", zap.Error(err))
		}
		return err
	default:
		err := fmt.Errorf("unknown command: %v", log)
		f.logger.Error(err.Error())
		return err
	}
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (f *FSM) Restore(rc io.ReadCloser) error {
	err := f.policyOperator.Restore(rc)
	if err != nil {
		f.logger.Error("failed to restore an FSM from a snapshot", zap.Error(err))
	}
	return err
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	b, err := f.policyOperator.Backup()
	if err != nil {
		f.logger.Error("failed to save the snapshot", zap.Error(err))
	}
	return &fsmSnapshot{data: b, logger: f.logger}, nil
}

type fsmSnapshot struct {
	data   []byte
	logger *zap.Logger
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		if _, err := sink.Write(f.data); err != nil {
			f.logger.Error("cannot to write to sink", zap.Error(err))
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		f.logger.Error("cannot to persist the fsm snapshot", zap.Error(err))
		return sink.Cancel()
	}

	return nil
}

func (f *fsmSnapshot) Release() {
	// noop
}
