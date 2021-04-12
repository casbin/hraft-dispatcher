package store

import (
	"fmt"

	"github.com/casbin/hraft-dispatcher/command"
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
	case command.Command_COMMAND_TYPE_ADD_POLICIES:
		var request command.AddPoliciesRequest
		err := proto.Unmarshal(cmd.Data, &request)
		if err != nil {
			f.logger.Error("cannot to unmarshal the request", zap.Error(err), zap.ByteString("request", cmd.Data))
			return err
		}
		var rules [][]string
		for _, rule := range request.Rules {
			rules = append(rules, rule.GetItems())
		}
		err = f.policyOperator.AddPolicies(request.Sec, request.PType, rules)
		if err != nil {
			f.logger.Error("apply the add policies request failed", zap.Error(err), zap.String("request", request.String()))
		} else {
			f.logger.Info("add policies request applied",
				zap.String("sec", request.Sec),
				zap.String("pType", request.PType),
				zap.Any("rules", rules),
			)
		}
		return err
	case command.Command_COMMAND_TYPE_REMOVE_POLICIES:
		var request command.RemovePoliciesRequest
		err := proto.Unmarshal(cmd.Data, &request)
		if err != nil {
			f.logger.Error("cannot to unmarshal the request", zap.Error(err), zap.ByteString("request", cmd.Data))
			return err
		}
		var rules [][]string
		for _, rule := range request.Rules {
			rules = append(rules, rule.GetItems())
		}
		err = f.policyOperator.RemovePolicies(request.Sec, request.PType, rules)
		if err != nil {
			f.logger.Error("apply the remove policies request failed", zap.Error(err), zap.String("request", request.String()))
		} else {
			f.logger.Info("remove policies request applied",
				zap.String("sec", request.Sec),
				zap.String("pType", request.PType),
				zap.Any("rules", rules),
			)
		}
		return err
	case command.Command_COMMAND_TYPE_REMOVE_FILTERED_POLICY:
		var request command.RemoveFilteredPolicyRequest
		err := proto.Unmarshal(cmd.Data, &request)
		if err != nil {
			f.logger.Error("cannot to unmarshal the request", zap.Error(err), zap.ByteString("request", cmd.Data))
			return err
		}
		err = f.policyOperator.RemoveFilteredPolicy(request.Sec, request.PType, int(request.FieldIndex), request.FieldValues...)
		if err != nil {
			f.logger.Error("apply the remove filtered policy request failed", zap.Error(err), zap.String("request", request.String()))
		} else {
			f.logger.Info("remove filtered policy request applied",
				zap.String("sec", request.Sec),
				zap.String("pType", request.PType),
				zap.Int32("fieldIndex", request.FieldIndex),
				zap.Any("fieldValues", request.FieldValues),
			)
		}
		return err
	case command.Command_COMMAND_TYPE_UPDATE_POLICY:
		var request command.UpdatePolicyRequest
		err := proto.Unmarshal(cmd.Data, &request)
		if err != nil {
			f.logger.Error("cannot to unmarshal the request", zap.Error(err), zap.ByteString("request", cmd.Data))
			return err
		}
		err = f.policyOperator.UpdatePolicy(request.Sec, request.PType, request.OldRule, request.NewRule)
		if err != nil {
			f.logger.Error("apply the update policy request failed", zap.Error(err), zap.String("request", request.String()))
		} else {
			f.logger.Info("update policy request applied",
				zap.String("sec", request.Sec),
				zap.String("pType", request.PType),
				zap.Any("oldRule", request.OldRule),
				zap.Any("newRule", request.NewRule),
			)
		}
		return err
	case command.Command_COMMAND_TYPE_UPDATE_POLICIES:
		var request command.UpdatePoliciesRequest
		err := proto.Unmarshal(cmd.Data, &request)
		if err != nil {
			f.logger.Error("cannot to unmarshal the request", zap.Error(err), zap.ByteString("request", cmd.Data))
			return err
		}
		var oldRules [][]string
		for _, rule := range request.OldRules {
			oldRules = append(oldRules, rule.GetItems())
		}
		var newRules [][]string
		for _, rule := range request.NewRules {
			newRules = append(newRules, rule.GetItems())
		}

		err = f.policyOperator.UpdatePolicies(request.Sec, request.PType, oldRules, newRules)
		if err != nil {
			f.logger.Error("apply the update policies request failed", zap.Error(err), zap.String("request", request.String()))
		} else {
			f.logger.Info("update policies request applied",
				zap.String("sec", request.Sec),
				zap.String("pType", request.PType),
				zap.Any("oldRules", request.OldRules),
				zap.Any("newRules", request.NewRules),
			)
		}
		return err
	case command.Command_COMMAND_TYPE_CLEAR_POLICY:
		err := f.policyOperator.ClearPolicy()
		if err != nil {
			f.logger.Error("apply the clear policy request failed", zap.Error(err))
		} else {
			f.logger.Info("clear policy request applied")
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
	f.logger.Info("start restore")
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
