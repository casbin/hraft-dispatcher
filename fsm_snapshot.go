package hraftdispatcher

import (
	"encoding/json"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

type fsmSnapshot struct {
	commands []Command
	logger   *zap.Logger
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(f.commands)
		if err != nil {
			f.logger.Error("cannot to encode data", zap.Error(err), zap.Any("commands", f.commands))
			return err
		}
		if _, err := sink.Write(b); err != nil {
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
