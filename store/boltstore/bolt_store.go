package boltstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"time"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

const (
	// Permissions to use on the db file. This is only used if the
	// database file does not exist and needs to be created.
	dbFileMode = 0600
)

var (
	// Bucket names we perform transactions in
	dbLogs     = []byte("logs")
	dbConf     = []byte("conf")
	dbSnapMeta = []byte("snap_meta")
	dbSnapData = []byte("snap_data")
	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// BoltStore provides access to BoltDB for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type BoltStore struct {
	// conn is the underlying handle to the db.
	conn *bolt.DB

	// The path to the Bolt database file
	path string

	// retain controls how many snapshots are retained. Must be at least 1.
	retain int
}

// BoltSnapshotSink implements SnapshotSink with bbolt.
type BoltSnapshotSink struct {
	boltStore *BoltStore

	// The metadata of the snapshot
	meta raft.SnapshotMeta
	// The true content of the snapshot
	contents *bytes.Buffer
}

// snapMetaSlice is used to implement Sort interface
type snapMetaSlice []*raft.SnapshotMeta

// Options contains all the configuraiton used to open the BoltDB
type Options struct {
	// Path is the file path to the BoltDB to use
	Path string

	// BoltOptions contains any specific BoltDB options you might
	// want to specify [e.g. open timeout]
	BoltOptions *bolt.Options

	// NoSync causes the database to skip fsync calls after each
	// write to the log. This is unsafe, so it should be used
	// with caution.
	NoSync bool
}

// readOnly returns true if the contained bolt options say to open
// the DB in readOnly mode [this can be useful to tools that want
// to examine the log]
func (o *Options) readOnly() bool {
	return o != nil && o.BoltOptions != nil && o.BoltOptions.ReadOnly
}

// NewBoltStore takes a file path and returns a connected Raft backend.
func NewBoltStore(path string, retain int) (*BoltStore, error) {
	return New(Options{Path: path}, retain)
}

// New uses the supplied options to open the BoltDB and prepare it for use as a raft backend.
func New(options Options, retain int) (*BoltStore, error) {
	// Try to connect
	handle, err := bolt.Open(options.Path, dbFileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}
	handle.NoSync = options.NoSync

	// Create the new store
	store := &BoltStore{
		conn:   handle,
		retain: retain,
		path:   options.Path,
	}

	// If the store was opened read-only, don't try and create buckets
	if !options.readOnly() {
		// Set up our buckets
		if err := store.initialize(); err != nil {
			store.Close()
			return nil, err
		}
	}
	return store, nil
}

// initialize is used to set up all of the buckets.
func (b *BoltStore) initialize() error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Create all the buckets
	if _, err := tx.CreateBucketIfNotExists(dbLogs); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(dbConf); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(dbSnapMeta); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(dbSnapData); err != nil {
		return err
	}

	return tx.Commit()
}

// Close is used to gracefully close the DB connection.
func (b *BoltStore) Close() error {
	return b.conn.Close()
}

// FirstIndex returns the first known index from the Raft log.
func (b *BoltStore) FirstIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if first, _ := curs.First(); first == nil {
		return 0, nil
	} else {
		return bytesToUint64(first), nil
	}
}

// LastIndex returns the last known index from the Raft log.
func (b *BoltStore) LastIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if last, _ := curs.Last(); last == nil {
		return 0, nil
	} else {
		return bytesToUint64(last), nil
	}
}

// GetLog is used to retrieve a log from BoltDB at a given index.
func (b *BoltStore) GetLog(idx uint64, log *raft.Log) error {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbLogs)
	val := bucket.Get(uint64ToBytes(idx))

	if val == nil {
		return raft.ErrLogNotFound
	}
	return decodeMsgPack(val, log)
}

// StoreLog is used to store a single raft log
func (b *BoltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (b *BoltStore) StoreLogs(logs []*raft.Log) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		bucket := tx.Bucket(dbLogs)
		if err := bucket.Put(key, val.Bytes()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *BoltStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)

	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
		// Handle out-of-range log index
		if bytesToUint64(k) > max {
			break
		}

		// Delete in-range log index
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Set is used to set a key/value set outside of the raft log
func (b *BoltStore) Set(k, v []byte) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbConf)
	if err := bucket.Put(k, v); err != nil {
		return err
	}

	return tx.Commit()
}

// Get is used to retrieve a value from the k/v store by key
func (b *BoltStore) Get(k []byte) ([]byte, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbConf)
	val := bucket.Get(k)

	if val == nil {
		return nil, ErrKeyNotFound
	}
	return append([]byte(nil), val...), nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *BoltStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (b *BoltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// encodePeers is used to serialize a Configuration into the old peers format.
// This is here for backwards compatibility when operating with a mix of old
// servers and should be removed once we deprecate support for protocol version 1.
func encodePeers(configuration raft.Configuration, trans raft.Transport) []byte {
	// Gather up all the voters, other suffrage types are not supported by
	// this data format.
	var encPeers [][]byte
	for _, server := range configuration.Servers {
		if server.Suffrage == raft.Voter {
			encPeers = append(encPeers, trans.EncodePeer(server.ID, server.Address))
		}
	}

	// Encode the entire array.
	buf, err := encodeMsgPack(encPeers)
	if err != nil {
		panic(fmt.Errorf("failed to encode peers: %v", err))
	}

	return buf.Bytes()
}

// Create is used to start a new bbolt snapshot
func (b *BoltStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration,
	configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	if version != 1 {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}

	// Get the name of the snapshot
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	name := fmt.Sprintf("%d-%d-%d", term, index, msec)

	sink := &BoltSnapshotSink{
		boltStore: b,
		meta: raft.SnapshotMeta{
			Version:            version,
			ID:                 name,
			Index:              index,
			Term:               term,
			Peers:              encodePeers(configuration, trans),
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
		},
		contents: &bytes.Buffer{},
	}

	return sink, nil
}

// getSnapshot returns a list of snapshot metadata in bbolt store
func (b *BoltStore) getSnapshot(tx *bolt.Tx) []*raft.SnapshotMeta {
	var snapMeta []*raft.SnapshotMeta
	curs := tx.Bucket(dbSnapMeta).Cursor()
	for k, v := curs.First(); k != nil; k, v = curs.Next() {
		tempMeta := new(raft.SnapshotMeta)
		if decodeMsgPack(v, tempMeta) == nil {
			snapMeta = append(snapMeta, tempMeta)
		}
	}

	// sort the snapshot in reverse order (new->old)
	sort.Sort(sort.Reverse(snapMetaSlice(snapMeta)))
	return snapMeta
}

// List returns available snapshots in bbolt store.
func (b *BoltStore) List() ([]*raft.SnapshotMeta, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return []*raft.SnapshotMeta{}, err
	}
	defer tx.Rollback()

	snapMeta := b.getSnapshot(tx)
	if len(snapMeta) > b.retain {
		snapMeta = snapMeta[:b.retain]
	}
	return snapMeta, nil
}

// Open takes a snapshot ID and returns the metadata and a ReadCloser for that snapshot.
func (b *BoltStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	// Get the metadata of the snapshot
	metaBucket := tx.Bucket(dbSnapMeta)
	val := metaBucket.Get([]byte(id))
	if val == nil {
		return nil, nil, fmt.Errorf("[ERR] snapshot: failed to open snapshot metadata. id: %s", id)
	}

	snapMeta := new(raft.SnapshotMeta)
	if err := decodeMsgPack(val, snapMeta); err != nil {
		return nil, nil, err
	}

	// Get the contents of the snapshot
	dataBucket := tx.Bucket(dbSnapData)
	val = dataBucket.Get([]byte(id))
	if val == nil {
		return nil, nil, fmt.Errorf("[ERR] snapshot: failed to open snapshot contents. id: %s", id)
	}

	contents := bytes.NewBuffer(val)
	return snapMeta, ioutil.NopCloser(contents), nil
}

// ID returns the ID of the snapshot, can be used with Open() after the snapshot is finalized.
func (s *BoltSnapshotSink) ID() string {
	return s.meta.ID
}

func (s *BoltSnapshotSink) Cancel() error {
	return nil
}

// Write is used to append the content of the snapshot
func (s *BoltSnapshotSink) Write(b []byte) (int, error) {
	written, err := s.contents.Write(b)
	if err != nil {
		return 0, err
	}
	s.meta.Size += int64(written)
	return written, nil
}

// Close is used to indicate a successful end.
func (s *BoltSnapshotSink) Close() error {
	tx, err := s.boltStore.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	metaBucket := tx.Bucket(dbSnapMeta)
	buf, err := encodeMsgPack(s.meta)
	if err != nil {
		return err
	}
	if err = metaBucket.Put([]byte(s.meta.ID), buf.Bytes()); err != nil {
		return err
	}

	dataBucket := tx.Bucket(dbSnapData)
	if err = dataBucket.Put([]byte(s.meta.ID), s.contents.Bytes()); err != nil {
		return err
	}

	// reap any snapshots beyond the retain count.
	snapMeta := s.boltStore.getSnapshot(tx)
	for i := s.boltStore.retain; i < len(snapMeta); i++ {
		if err := metaBucket.Delete([]byte(snapMeta[i].ID)); err != nil {
			return err
		}

		if err := dataBucket.Delete([]byte(snapMeta[i].ID)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Implement the sort interface for []*SnapshotMeta.
func (s snapMetaSlice) Len() int {
	return len(s)
}

func (s snapMetaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sync performs an fsync on the database file handle. This is not necessary
// under normal operation unless NoSync is enabled, in which this forces the
// database file to sync against the disk.
func (b *BoltStore) Sync() error {
	return b.conn.Sync()
}
