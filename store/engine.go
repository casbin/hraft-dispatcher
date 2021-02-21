package store

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/casbin/casbin/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

const (
	databaseFilename = "casbin.db"
)

var (
	policyBucketName = []byte("policy_rules")
)

// PolicyOperator is used to update policies and provide persistence.
type PolicyOperator struct {
	enforcer casbin.IDistributedEnforcer
	db       *bolt.DB
	l        *sync.Mutex
	logger   *zap.Logger
}

// NewPolicyOperator returns a PolicyOperator.
func NewPolicyOperator(path string, e casbin.IDistributedEnforcer) (*PolicyOperator, error) {
	p := &PolicyOperator{
		enforcer: e,
		l:        &sync.Mutex{},
		logger:   zap.NewExample(),
	}
	dbPath := filepath.Join(path, databaseFilename)
	if err := p.openDBFile(dbPath); err != nil {
		return nil, errors.Wrapf(err, "failed to open bolt file")
	}

	return p, nil
}

// openDBFile opens the bolt file.
func (p *PolicyOperator) openDBFile(dbPath string) error {
	if len(dbPath) == 0 {
		return errors.New("dbPath cannot be an empty")
	}

	boltDB, err := bolt.Open(dbPath, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}

	p.db = boltDB

	return p.createBucket(policyBucketName)
}

// Restore is used to restore a database from io.ReadCloser.
func (p *PolicyOperator) Restore(rc io.ReadCloser) error {
	p.l.Lock()
	defer p.l.Unlock()

	dbPath := p.db.Path()
	err := p.db.Close()
	if err != nil {
		p.logger.Error("failed to close database file", zap.Error(err))
		return err
	}

	gz, err := gzip.NewReader(rc)
	if err != nil {
		p.logger.Error("failed to new gzip", zap.Error(err))
		return err
	}

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, gz); err != nil {
		p.logger.Error("failed to copy data", zap.Error(err))
		return err
	}

	err = gz.Close()
	if err != nil {
		p.logger.Error("failed to close the gzip", zap.Error(err))
		return err
	}

	err = ioutil.WriteFile(dbPath, buf.Bytes(), 0600)
	if err != nil {
		p.logger.Error("failed to restore the database file", zap.Error(err))
		return err
	}

	err = p.openDBFile(dbPath)
	if err != nil {
		p.logger.Error("failed to open the database file", zap.Error(err))
		return err
	}

	return nil
}

// Backup writes the database to bytes with gzip.
func (p *PolicyOperator) Backup() ([]byte, error) {
	p.l.Lock()
	defer p.l.Unlock()

	writer := new(bytes.Buffer)
	gz, err := gzip.NewWriterLevel(writer, gzip.BestCompression)

	err = p.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(gz)
		return err
	})
	if err != nil {
		p.logger.Error("failed to backup database file", zap.Error(err))
		return nil, err
	}

	err = gz.Close()
	if err != nil {
		p.logger.Error("failed to close the gzip", zap.Error(err))
		return nil, err
	}

	return writer.Bytes(), nil
}

// createBucket creates a bucket with the given name.
func (p *PolicyOperator) createBucket(name []byte) error {
	return p.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(name)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to create %s bucket", name))
		}
		return nil
	})
}

// LoadPolicy clears the policies held by enforcer, and loads policy from database.
func (p *PolicyOperator) LoadPolicy() error {
	p.l.Lock()
	defer p.l.Unlock()

	err := p.enforcer.ClearPolicySelf(nil)
	if err != nil {
		p.logger.Error("failed to call loadPolicy", zap.Error(err))
		return err
	}

	err = p.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(policyBucketName)
		err := bkt.ForEach(func(k, v []byte) error {
			var rule Rule
			err := jsoniter.Unmarshal(k, &rule)
			if err != nil {
				return err
			}

			_, err = p.enforcer.AddPoliciesSelf(nil, rule.Sec, rule.PType, [][]string{rule.Rule})
			return err
		})
		return err
	})
	if err != nil {
		p.logger.Error("failed to persist to database", zap.Error(err))
	}

	return err
}

// AddPolicies adds a set of rules.
func (p *PolicyOperator) AddPolicies(sec, pType string, rules [][]string) error {
	p.l.Lock()
	defer p.l.Unlock()

	effected, err := p.enforcer.AddPoliciesSelf(nil, sec, pType, rules)
	if err != nil {
		return err
	}
	if len(effected) == 0 {
		return nil
	}

	err = p.db.Update(func(tx *bolt.Tx) error {
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
	if err != nil {
		p.logger.Error("failed to persist to database", zap.Error(err))
	}

	return err
}

// RemovePolicies removes a set of rules.
func (p *PolicyOperator) RemovePolicies(sec, pType string, rules [][]string) error {
	p.l.Lock()
	defer p.l.Unlock()

	effected, err := p.enforcer.RemovePoliciesSelf(nil, sec, pType, rules)
	if err != nil {
		p.logger.Error("failed to call RemovePolicySelf", zap.Error(err))
		return err
	}
	if len(effected) == 0 {
		return nil
	}

	err = p.db.Update(func(tx *bolt.Tx) error {
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

// RemoveFilteredPolicy removes a set of rules that match a pattern.
func (p *PolicyOperator) RemoveFilteredPolicy(sec string, pType string, fieldIndex int, fieldValues ...string) error {
	p.l.Lock()
	defer p.l.Unlock()

	effected, err := p.enforcer.RemoveFilteredPolicySelf(nil, sec, pType, fieldIndex, fieldValues...)
	if err != nil {
		p.logger.Error("failed to call RemoveFilteredPolicySelf", zap.Error(err))
		return err
	}
	if len(effected) == 0 {
		return nil
	}

	err = p.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(policyBucketName)
		for _, item := range effected {
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
	if err != nil {
		p.logger.Error("failed to persist to database", zap.Error(err))
	}

	return err
}

//UpdatePolicy replaces an existing rule.
func (p *PolicyOperator) UpdatePolicy(sec, pType string, oldRule, newRule []string) error {
	p.l.Lock()
	defer p.l.Unlock()

	effected, err := p.enforcer.UpdatePolicySelf(nil, sec, pType, oldRule, newRule)
	if err != nil {
		p.logger.Error("failed to call UpdatePolicySelf", zap.Error(err))
		return err
	}
	if effected == false {
		return nil
	}

	err = p.db.Update(func(tx *bolt.Tx) error {
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
	if err != nil {
		p.logger.Error("failed to persist to database", zap.Error(err))
	}

	return err
}

//UpdatePolicies replaces a set of existing rule.
func (p *PolicyOperator) UpdatePolicies(sec, pType string, oldRules, newRules [][]string) error {
	p.l.Lock()
	defer p.l.Unlock()

	effected, err := p.enforcer.UpdatePoliciesSelf(nil, sec, pType, oldRules, newRules)
	if err != nil {
		p.logger.Error("failed to call UpdatePoliciesSelf", zap.Error(err))
		return err
	}
	if effected == false {
		return nil
	}

	err = p.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(policyBucketName)

		for _, newRule := range newRules {
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
		}

		for _, oldRule := range oldRules {
			oldKey, err := newRuleBytes(sec, pType, oldRule)
			if err != nil {
				return err
			}
			return bkt.Delete(oldKey)
		}
		return nil
	})
	if err != nil {
		p.logger.Error("failed to persist to database", zap.Error(err))
	}

	return nil
}

// ClearPolicy clears all rules.
func (p *PolicyOperator) ClearPolicy() error {
	p.l.Lock()
	defer p.l.Unlock()

	err := p.enforcer.ClearPolicySelf(nil)
	if err != nil {
		p.logger.Error("failed to call ClearPolicySelf", zap.Error(err))
		return err
	}

	err = p.db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(policyBucketName)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucket(policyBucketName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		p.logger.Error("failed to persist to database", zap.Error(err))
	}

	return err
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

	key, err := jsoniter.Marshal(r)
	if err != nil {
		return nil, err
	}
	return key, nil
}
