package hraftdispatcher

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pkg/errors"

	bolt "go.etcd.io/bbolt"

	"io"
	"sync"

	"github.com/casbin/casbin/v2"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

const (
	databaseFilename = "hraft-dispatcher.db"
)

var (
	policyBucketName = []byte("policy")
)

// FSM is state storage.
type FSM struct {
	commands      []Command
	enforcer      casbin.IDistributedEnforcer
	shouldPersist func() bool
	mutex         *sync.RWMutex
	logger        *zap.Logger
	db            *bolt.DB
}

func NewFSM(path string, enforcer casbin.IDistributedEnforcer, logger *zap.Logger) (*FSM, error) {
	f := &FSM{
		enforcer:      enforcer,
		logger:        logger,
		mutex:         &sync.RWMutex{},
		shouldPersist: func() bool { return false },
	}

	dbPath := filepath.Join(path, databaseFilename)
	if err := f.openDBFile(dbPath); err != nil {
		return nil, errors.Wrapf(err, "failed to open bolt file")
	}

	// restore casbin memory from db
	err := f.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(policyBucketName)
		err := bkt.ForEach(func(k, v []byte) error {
			var rule Rule
			err := json.Unmarshal(k, &rule)
			if err != nil {
				return err
			}
			_, err = f.enforcer.AddPolicySelf(f.shouldPersist, rule.Sec, rule.PType, [][]string{rule.Rule})
			if err != nil {
				return err
			}
			return nil
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (f *FSM) openDBFile(dbPath string) error {
	if len(dbPath) == 0 {
		return errors.New("dbPath cannot be an empty")
	}

	boltDB, err := bolt.Open(dbPath, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}

	f.db = boltDB

	return f.CreateBucket(policyBucketName)
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
	f.mutex.Lock()
	f.commands = append(f.commands, cmd)
	f.mutex.Unlock()
	return f.apply(cmd)
}

// Apply applies a Raft log entry to the casbin.
func (f *FSM) apply(cmd Command) error {
	switch cmd.Operation {
	case AddOperation:
		effected, err := f.enforcer.AddPolicySelf(f.shouldPersist, cmd.Sec, cmd.Ptype, cmd.Rules)
		if err != nil {
			return err
		}
		return f.Put(cmd.Sec, cmd.Ptype, effected)
	case RemoveOperation:
		effected, err := f.enforcer.RemovePolicySelf(f.shouldPersist, cmd.Sec, cmd.Ptype, cmd.Rules)
		if err != nil {
			return err
		}
		return f.Delete(cmd.Sec, cmd.Ptype, effected)
	case RemoveFilteredOperation:
		effected, err := f.enforcer.RemoveFilteredPolicySelf(f.shouldPersist, cmd.Sec, cmd.Ptype, cmd.FieldIndex, cmd.FieldValues...)
		if err != nil {
			return err
		}
		return f.Delete(cmd.Sec, cmd.Ptype, effected)
	case ClearOperation:
		err := f.enforcer.ClearPolicySelf(f.shouldPersist)
		if err != nil {
			return err
		}
		return f.db.Update(func(tx *bolt.Tx) error {
			return tx.DeleteBucket(policyBucketName)
		})
	case UpdateOperation:
		effected, err := f.enforcer.UpdatePolicySelf(f.shouldPersist, cmd.Sec, cmd.Ptype, cmd.OldRule, cmd.NewRule)
		if err != nil {
			return err
		}
		if effected == false {
			return nil
		}
		return f.Update(cmd.Sec, cmd.Ptype, cmd.OldRule, cmd.NewRule)
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

	// reset policy bucket
	err := f.DeleteBucket(policyBucketName)
	if err != nil {
		return err
	}
	err = f.CreateBucket(policyBucketName)
	if err != nil {
		return err
	}

	// reset fsm state
	f.commands = cmds

	// reset casbin
	f.enforcer.ClearPolicy()
	for _, cmd := range cmds {
		err = f.apply(cmd)
		if err != nil {
			return err
		}
	}
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

type Rule struct {
	Sec   string   `json:"sec"`
	PType string   `json:"p_type"`
	Rule  []string `json:"rule"`
}

func newRuleBytes(sec, pType string, rule []string) ([]byte, error) {
	r := Rule{
		Sec:   sec,
		PType: pType,
		Rule:  rule,
	}

	key, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return key, nil
}

func (f *FSM) CreateBucket(name []byte) error {
	return f.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(policyBucketName)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to create %s bucket", name))
		}
		return nil
	})
}

func (f *FSM) DeleteBucket(name []byte) error {
	return f.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(name)
	})
}

func (f *FSM) Put(sec, pType string, rules [][]string) error {
	err := f.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(policyBucketName)
		for _, item := range rules {
			key, err := newRuleBytes(sec, pType, item)
			if err != nil {
				return err
			}

			value, err := bkt.NextSequence()
			if err != nil {
				return err
			}

			err = bkt.Put(key, []byte(strconv.FormatUint(value, 10)))
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func (f *FSM) Delete(sec, pType string, rules [][]string) error {
	err := f.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(policyBucketName)
		for _, item := range rules {
			key, err := newRuleBytes(sec, pType, item)
			if err != nil {
				return err
			}

			err = bkt.Delete(key)
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

func (f *FSM) Update(sec, pType string, oldRule, newRule []string) error {
	err := f.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(policyBucketName)

		newKey, err := newRuleBytes(sec, pType, newRule)
		if err != nil {
			return err
		}
		value, err := bkt.NextSequence()
		if err != nil {
			return err
		}

		err = bkt.Put(newKey, []byte(strconv.FormatUint(value, 10)))
		if err != nil {
			return err
		}

		oldKey, err := newRuleBytes(sec, pType, oldRule)
		if err != nil {
			return err
		}
		return bkt.Delete(oldKey)
	})

	return err
}
