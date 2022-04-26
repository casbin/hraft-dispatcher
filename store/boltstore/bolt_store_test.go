package boltstore

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

const (
	retainSnapshotCount = 2
	SnapshotVersionMax = 1
)

func testBoltStore(t testing.TB) *BoltStore {
	fh, err := ioutil.TempFile("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.RemoveAll(fh.Name())

	// Successfully creates and returns a store
	store, err := NewBoltStore(fh.Name(), retainSnapshotCount)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestBoltStore_Implements(t *testing.T) {
	var store interface{} = &BoltStore{}
	var sink interface{} = &BoltSnapshotSink{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("BoltStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("BoltStore does not implement raft.LogStore")
	}

	if _, ok := store.(raft.SnapshotStore); !ok {
		t.Fatalf("BoltStore does not implement raft.SnapshotStore")
	}
	if _, ok := sink.(raft.SnapshotSink); !ok {
		t.Fatalf("BoltSnapshotSink does not implement raft.SnapshotSink")
	}
}

func TestBoltOptionsTimeout(t *testing.T) {
	fh, err := ioutil.TempFile("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.RemoveAll(fh.Name())

	options := Options{
		Path: fh.Name(),
		BoltOptions: &bolt.Options{
			Timeout: time.Second / 10,
		},
	}
	store, err := New(options, retainSnapshotCount)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer store.Close()

	// trying to open it again should timeout
	doneCh := make(chan error, 1)
	go func() {
		_, err := New(options, retainSnapshotCount)
		doneCh <- err
	}()
	select {
	case err := <-doneCh:
		if err == nil || err.Error() != "timeout" {
			t.Errorf("Expected timeout error but got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("Gave up waiting for timeout response")
	}
}

func TestBoltOptionsReadOnly(t *testing.T) {
	fh, err := ioutil.TempFile("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.RemoveAll(fh.Name())
	store, err := NewBoltStore(fh.Name(), retainSnapshotCount)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}
	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	store.Close()
	options := Options{
		Path: fh.Name(),
		BoltOptions: &bolt.Options{
			Timeout:  time.Second / 10,
			ReadOnly: true,
		},
	}
	roStore, err := New(options, retainSnapshotCount)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer roStore.Close()
	result := new(raft.Log)
	if err := roStore.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Errorf("bad: %v", result)
	}
	// Attempt to store the log, should fail on a read-only store
	err = roStore.StoreLog(log)
	if err != bolt.ErrDatabaseReadOnly {
		t.Errorf("expecting error %v, but got %v", bolt.ErrDatabaseReadOnly, err)
	}
}

func TestNewBoltStore(t *testing.T) {
	fh, err := ioutil.TempFile("", "bolt")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.RemoveAll(fh.Name())

	// Successfully creates and returns a store
	store, err := NewBoltStore(fh.Name(), retainSnapshotCount)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the file was created
	if store.path != fh.Name() {
		t.Fatalf("unexpected file path %q", store.path)
	}
	if _, err := os.Stat(fh.Name()); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Close the store so we can open again
	if err := store.Close(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure our tables were created
	db, err := bolt.Open(fh.Name(), dbFileMode, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if _, err := tx.CreateBucket([]byte(dbLogs)); err != bolt.ErrBucketExists {
		t.Fatalf("bad: %v", err)
	}
	if _, err := tx.CreateBucket([]byte(dbConf)); err != bolt.ErrBucketExists {
		t.Fatalf("bad: %v", err)
	}
	if _, err := tx.CreateBucket([]byte(dbSnapMeta)); err != bolt.ErrBucketExists {
		t.Fatalf("bad: %v", err)
	}
	if _, err := tx.CreateBucket([]byte(dbSnapData)); err != bolt.ErrBucketExists {
		t.Fatalf("bad: %v", err)
	}
}

func TestBoltStore_FirstIndex(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestBoltStore_LastIndex(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestBoltStore_GetLog(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.RemoveAll(store.path)

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

func TestBoltStore_SetLog(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.RemoveAll(store.path)

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestBoltStore_SetLogs(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.RemoveAll(store.path)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestBoltStore_DeleteRange(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.RemoveAll(store.path)

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

func TestBoltStore_Set_Get(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.RemoveAll(store.path)

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

func TestBoltStore_SetUint64_GetUint64(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.RemoveAll(store.path)

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}

func TestBoltStore_Snapshot_Create(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.RemoveAll(store.path)

	// Check no snapshots
	snaps, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("did not expect any snapshots: %v", snaps)
	}

	// Create a new sink
	var configuration raft.Configuration
	configuration.Servers = append(configuration.Servers, raft.Server{
		Suffrage: raft.Voter,
		ID:       raft.ServerID("my id"),
		Address:  raft.ServerAddress("over here"),
	})
	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	sink, err := store.Create(SnapshotVersionMax, 10, 3, configuration, 2, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The sink is not done, should not be in a list!
	snaps, err = store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("did not expect any snapshots: %v", snaps)
	}

	// Write to the sink
	_, err = sink.Write([]byte("first\n"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	_, err = sink.Write([]byte("second\n"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Done!
	err = sink.Close()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Should have a snapshot!
	snaps, err = store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expect a snapshots: %v", snaps)
	}

	// Check the latest
	latest := snaps[0]
	if latest.Index != 10 {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if latest.Term != 3 {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if !reflect.DeepEqual(latest.Configuration, configuration) {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if latest.ConfigurationIndex != 2 {
		t.Fatalf("bad snapshot: %v", *latest)
	}
	if latest.Size != 13 {
		t.Fatalf("bad snapshot: %v", *latest)
	}

	// Read the snapshot
	_, r, err := store.Open(latest.ID)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Read out everything
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Ensure a match
	if bytes.Compare(buf.Bytes(), []byte("first\nsecond\n")) != 0 {
		t.Fatalf("content mismatch")
	}
}

func TestBoltSore_Snapshot_Cancel(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.RemoveAll(store.path)

	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	sink, err := store.Create(SnapshotVersionMax, 10, 3, raft.Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Cancel the snapshot! Should delete
	err = sink.Cancel()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// The sink is canceled, should not be in a list!
	snaps, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("did not expect any snapshots: %v", snaps)
	}
}

func TestBoltStore_Snapshot_Retention(t *testing.T) {
	var err error

	store := testBoltStore(t)
	defer store.Close()
	defer os.RemoveAll(store.path)

	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	for i := 10; i < 15; i++ {
		var sink raft.SnapshotSink
		sink, err = store.Create(SnapshotVersionMax, uint64(i), 3, raft.Configuration{}, 0, trans)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		err = sink.Close()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// Should only have 2 listed!
	var snaps []*raft.SnapshotMeta
	snaps, err = store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expect 2 snapshots: %v", snaps)
	}

	// Check they are the latest
	if snaps[0].Index != 14 {
		t.Fatalf("bad snap: %#v", *snaps[0])
	}
	if snaps[1].Index != 13 {
		t.Fatalf("bad snap: %#v", *snaps[1])
	}
}

func TestBoltStore_Snapshot_Ordering(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.RemoveAll(store.path)

	// Create a new sink
	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())
	sink, err := store.Create(SnapshotVersionMax, 130350, 5, raft.Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = sink.Close()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	sink, err = store.Create(SnapshotVersionMax, 204917, 36, raft.Configuration{}, 0, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	err = sink.Close()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Should only have 2 listed!
	snaps, err := store.List()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("expect 2 snapshots: %v", snaps)
	}

	// Check they are ordered
	if snaps[0].Term != 36 {
		t.Fatalf("bad snap: %#v", *snaps[0])
	}
	if snaps[1].Term != 5 {
		t.Fatalf("bad snap: %#v", *snaps[1])
	}
}
