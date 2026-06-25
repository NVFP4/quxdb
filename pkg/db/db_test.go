package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuxDB(t *testing.T) {
	db := newTestQuxDB(t)

	key := []byte("test")

	err := db.Set(key, []byte("val"))
	assert.NoError(t, err)

	err = db.Set(key, []byte("val2"))
	assert.NoError(t, err)

	err = db.Set(key, []byte("val3"))
	assert.NoError(t, err)

	val, found := db.Get(key)
	assert.True(t, found)
	assert.Equal(t, []byte("val3"), val)

	err = db.Delete(key)
	assert.NoError(t, err)

	val, found = db.Get(key)
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestQuxDBRange(t *testing.T) {
	db := newTestQuxDB(t)

	assert.NoError(t, db.Set([]byte("b"), []byte("b1")))
	assert.NoError(t, db.Set([]byte("a"), []byte("a1")))
	assert.NoError(t, db.Set([]byte("b"), []byte("b2")))
	assert.NoError(t, db.Set([]byte("c"), []byte("c1")))
	assert.NoError(t, db.Delete([]byte("a")))

	assert.Equal(t, []string{"b=b2", "c=c1"}, collectRange(db, nil, nil))
	assert.Equal(t, []string{"b=b2"}, collectRange(db, []byte("b"), []byte("b")))
	assert.Empty(t, collectRange(db, []byte("c"), []byte("b")))
}

func TestQuxDBRangeAcrossMemtables(t *testing.T) {
	db := newTestQuxDB(t)

	assert.NoError(t, db.Set([]byte("b"), []byte("old-b")))
	assert.NoError(t, db.Set([]byte("d"), []byte("old-d")))
	assert.NoError(t, db.Set([]byte("e"), []byte("old-e")))
	assert.NoError(t, db.Set([]byte("stale"), []byte("old-stale")))
	assert.NoError(t, db.Delete([]byte("stale")))

	db.rolloverMemtable(db.committedSeq.Load(), 0)

	assert.NoError(t, db.Set([]byte("a"), []byte("new-a")))
	assert.NoError(t, db.Set([]byte("b"), []byte("new-b")))
	assert.NoError(t, db.Set([]byte("c"), []byte("new-c")))
	assert.NoError(t, db.Delete([]byte("d")))

	assert.Equal(t, []string{"a=new-a", "b=new-b", "c=new-c", "e=old-e"}, collectRange(db, nil, nil))
	assert.Equal(t, []string{"b=new-b", "c=new-c"}, collectRange(db, []byte("b"), []byte("d")))
}

func TestQuxDBRangeUsesCommittedSequence(t *testing.T) {
	db := newTestQuxDB(t)

	require.NoError(t, db.Set([]byte("key"), []byte("committed")))
	readSeq := db.committedSeq.Load()
	require.NoError(t, db.amt.Load().Set(
		newQuxKey([]byte("key"), readSeq+1, quxOpSet),
		[]byte("uncommitted"),
	))

	assert.Equal(t, []string{"key=committed"}, collectRange(db, nil, nil))
}

func TestQuxDBRangeEarlyStopReleasesMemtables(t *testing.T) {
	db := newTestQuxDB(t)

	require.NoError(t, db.Set([]byte("a"), []byte("1")))
	require.NoError(t, db.Set([]byte("b"), []byte("2")))

	for range db.Iter(nil, nil) {
		break
	}

	done := make(chan error, 1)
	go func() {
		done <- db.Set([]byte("c"), []byte("3"))
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("write blocked after range iterator stopped")
	}
}

func TestCommittedSequenceAdvancesAfterSet(t *testing.T) {
	db := newTestQuxDB(t)

	assert.Zero(t, db.committedSeq.Load())
	require.NoError(t, db.Set([]byte("key"), []byte("value")))
	assert.Equal(t, uint64(1), db.committedSeq.Load())
}

func TestCommittedSequenceRestoredFromWAL(t *testing.T) {
	dir := t.TempDir()

	db, err := New(dir)
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	require.NoError(t, db.Set([]byte("first"), []byte("1")))
	require.NoError(t, db.Set([]byte("second"), []byte("2")))
	require.NoError(t, db.Stop(t.Context()))

	reopened, err := New(dir)
	require.NoError(t, err)
	require.NoError(t, reopened.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, reopened.Stop(t.Context()))
	})

	assert.Equal(t, uint64(2), reopened.committedSeq.Load())
	require.NoError(t, reopened.Set([]byte("third"), []byte("3")))
	assert.Equal(t, uint64(3), reopened.committedSeq.Load())
}

func newTestQuxDB(t *testing.T) *QuxDB {
	t.Helper()

	db, err := New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))

	return db
}

func collectRange(db *QuxDB, start, end []byte) []string {
	var items []string
	for key, value := range db.Iter(start, end) {
		items = append(items, string(key)+"="+string(value))
	}
	return items
}
