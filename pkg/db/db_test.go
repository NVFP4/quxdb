package db

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yashgorana/quxdb/pkg/sst"
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

	val, found, err := db.Get(key)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("val3"), val)

	err = db.Delete(key)
	assert.NoError(t, err)

	val, found, err = db.Get(key)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestWriteSizeLimits(t *testing.T) {
	db := newTestQuxDB(t)

	maxKey := bytes.Repeat([]byte{'k'}, MaxKeySize)
	require.NoError(t, db.Set(maxKey, bytes.Repeat([]byte{'v'}, MaxValueSize)))
	_, found, err := db.Get(maxKey)
	require.NoError(t, err)
	assert.True(t, found)

	longKey := append(maxKey, 'k')
	assert.ErrorIs(t, db.Set(longKey, []byte("v")), ErrKeyTooLarge)
	assert.ErrorIs(t, db.Delete(longKey), ErrKeyTooLarge)
	assert.ErrorIs(t, db.Set([]byte("key"), make([]byte, MaxValueSize+1)), ErrValueTooLarge)

	_, found, err = db.Get(longKey)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestPointTombstoneShadowsOlderMemtable(t *testing.T) {
	db := newTestQuxDB(t)

	require.NoError(t, db.Set([]byte("key"), []byte("old")))
	db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	require.NoError(t, db.Delete([]byte("key")))

	value, found, err := db.Get([]byte("key"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, value)
}

func TestPointTombstoneShadowsOlderSST(t *testing.T) {
	db := newTestQuxDB(t)

	require.NoError(t, db.Set([]byte("key"), []byte("old")))
	db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	for i := range cachedImmutables {
		require.NoError(t, db.Set(fmt.Appendf(nil, "before-%d", i), []byte("value")))
		db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	}
	require.Eventually(t, func() bool {
		return db.lsm.currentVersion().Checkpoint().LastSeq == 1 &&
			db.lsm.immutableMemtableCount() == cachedImmutables
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, db.Delete([]byte("key")))
	tombstoneSeq := db.committedSeq.Load()
	db.rolloverMemtable(tombstoneSeq, db.lastCommittedLSN)
	for i := range cachedImmutables {
		require.NoError(t, db.Set(fmt.Appendf(nil, "after-%d", i), []byte("value")))
		db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	}
	require.Eventually(t, func() bool {
		return db.lsm.currentVersion().Checkpoint().LastSeq == tombstoneSeq &&
			db.lsm.immutableMemtableCount() == cachedImmutables
	}, time.Second, 10*time.Millisecond)

	value, found, err := db.Get([]byte("key"))
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, value)
}

func TestQuxDBRange(t *testing.T) {
	db := newTestQuxDB(t)

	assert.NoError(t, db.Set([]byte("b"), []byte("b1")))
	assert.NoError(t, db.Set([]byte("a"), []byte("a1")))
	assert.NoError(t, db.Set([]byte("b"), []byte("b2")))
	assert.NoError(t, db.Set([]byte("c"), []byte("c1")))
	assert.NoError(t, db.Delete([]byte("a")))

	assert.Equal(t, []string{"b=b2", "c=c1"}, collectRange(t, db, nil, nil))
	assert.Equal(t, []string{"b=b2"}, collectRange(t, db, []byte("b"), []byte("b")))
	assert.Empty(t, collectRange(t, db, []byte("c"), []byte("b")))
}

func TestQuxDBRangeAcrossMemtablesAndSSTs(t *testing.T) {
	db := newTestQuxDB(t)

	assert.NoError(t, db.Set([]byte("b"), []byte("old-b")))
	assert.NoError(t, db.Set([]byte("d"), []byte("old-d")))
	assert.NoError(t, db.Set([]byte("e"), []byte("old-e")))
	assert.NoError(t, db.Set([]byte("stale"), []byte("old-stale")))
	assert.NoError(t, db.Delete([]byte("stale")))

	flushedSeq := db.committedSeq.Load()
	db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	for i := range cachedImmutables {
		assert.NoError(t, db.Set(fmt.Appendf(nil, "retained-%d", i), []byte("value")))
		db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	}
	require.Eventually(t, func() bool {
		return db.lsm.currentVersion().Checkpoint().LastSeq == flushedSeq &&
			db.lsm.immutableMemtableCount() == cachedImmutables
	}, time.Second, 10*time.Millisecond)

	assert.NoError(t, db.Set([]byte("a"), []byte("new-a")))
	assert.NoError(t, db.Set([]byte("b"), []byte("new-b")))
	assert.NoError(t, db.Set([]byte("c"), []byte("new-c")))
	assert.NoError(t, db.Delete([]byte("d")))

	assert.Equal(t,
		[]string{"a=new-a", "b=new-b", "c=new-c", "e=old-e", "retained-0=value"},
		collectRange(t, db, nil, nil),
	)
	assert.Equal(t, []string{"b=new-b", "c=new-c"}, collectRange(t, db, []byte("b"), []byte("d")))
}

func TestMemtableCheckpointCapturedAtRollover(t *testing.T) {
	db := newTestQuxDB(t)

	require.NoError(t, db.Set([]byte("one"), []byte("1")))
	require.NoError(t, db.Set([]byte("two"), []byte("2")))
	mt := db.lsm.activeMemtable()
	assert.Zero(t, mt.lastSeq)
	assert.Zero(t, mt.lastLSN)

	db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	assert.Equal(t, uint64(2), mt.lastSeq)
	assert.Equal(t, db.lastCommittedLSN, mt.lastLSN)
}

func TestQuxDBRangeUsesCommittedSequence(t *testing.T) {
	db := newTestQuxDB(t)

	require.NoError(t, db.Set([]byte("key"), []byte("committed")))
	readSeq := db.committedSeq.Load()
	require.NoError(t, db.lsm.activeMemtable().Set(
		newQuxKey([]byte("key"), readSeq+1, quxOpSet),
		[]byte("uncommitted"),
	))

	assert.Equal(t, []string{"key=committed"}, collectRange(t, db, nil, nil))
}

func TestQuxDBRangeEarlyStopReleasesMemtables(t *testing.T) {
	db := newTestQuxDB(t)

	require.NoError(t, db.Set([]byte("a"), []byte("1")))
	require.NoError(t, db.Set([]byte("b"), []byte("2")))

	for range db.Iter(nil, nil).All() {
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

func TestCheckpointRecoveryReplaysOnlyNewerRecords(t *testing.T) {
	dir := t.TempDir()

	db, err := New(dir)
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	expected := map[string]string{"first": "1", "second": "2"}
	require.NoError(t, db.Set([]byte("first"), []byte("1")))
	require.NoError(t, db.Set([]byte("second"), []byte("2")))
	db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	for i := range cachedImmutables {
		key := fmt.Sprintf("retained-%d", i)
		value := fmt.Sprintf("%d", i+3)
		expected[key] = value
		require.NoError(t, db.Set([]byte(key), []byte(value)))
		db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	}

	require.Eventually(t, func() bool {
		return db.lsm.currentVersion().Checkpoint().LastSeq == 2 &&
			db.lsm.immutableMemtableCount() == cachedImmutables
	}, time.Second, 10*time.Millisecond)
	checkpoint := db.lsm.currentVersion().Checkpoint()
	require.NotZero(t, checkpoint.LastLSN)

	newestSeq := uint64(cachedImmutables + 3)
	expected["newest"] = fmt.Sprintf("%d", newestSeq)
	require.NoError(t, db.Set([]byte("newest"), []byte(expected["newest"])))
	require.NoError(t, db.Stop(t.Context()))

	reopened, err := New(dir)
	require.NoError(t, err)
	require.Equal(t, checkpoint, reopened.lsm.currentVersion().Checkpoint())
	require.NoError(t, reopened.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, reopened.Stop(t.Context()))
	})

	assert.Equal(t, newestSeq, reopened.committedSeq.Load())
	for key, want := range expected {
		got, found, err := reopened.Get([]byte(key))
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte(want), got)
	}
}

func TestRecoverySkipsKVsFlushedByMidBatchRollover(t *testing.T) {
	dir := t.TempDir()
	db, err := New(dir)
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))

	// one ~70MiB batch spans two wal segments and rolls the 16MiB memtable over mid-batch
	const n = 4300
	value := bytes.Repeat([]byte{'v'}, 16<<10)
	batch := make([]*writeReq, n)
	for i := range batch {
		batch[i] = newWriteReq()
		batch[i].qkey, batch[i].val = newQuxKey(fmt.Appendf(nil, "key-%04d", i), 0, quxOpSet), value
	}
	db.commitBatch(batch)
	for _, req := range batch {
		require.NoError(t, req.res.err)
	}
	require.Eventually(t, func() bool {
		return db.lsm.currentVersion().Checkpoint().LastSeq > 0
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, db.Stop(t.Context()))

	reopened, err := New(dir)
	require.NoError(t, err)
	checkpoint := reopened.lsm.currentVersion().Checkpoint()
	require.Less(t, checkpoint.LastSeq, uint64(n))
	require.NoError(t, reopened.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, reopened.Stop(t.Context())) })

	view := reopened.lsm.acquire()
	replayed := 0
	for _, mt := range view.memtables {
		replayed += mt.Len()
	}
	view.release()
	assert.Equal(t, n-int(checkpoint.LastSeq), replayed)
	assert.Equal(t, uint64(n), reopened.committedSeq.Load())
	for i := range n {
		got, found, err := reopened.Get(fmt.Appendf(nil, "key-%04d", i))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, got)
	}
}

func TestCorruptBlockSurfacesAsReadError(t *testing.T) {
	dir := t.TempDir()
	db, err := New(dir)
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	require.NoError(t, db.Set([]byte("key"), []byte("value")))
	db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	for i := range cachedImmutables {
		require.NoError(t, db.Set(fmt.Appendf(nil, "later-%d", i), []byte("value")))
		db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	}
	require.Eventually(t, func() bool {
		return db.lsm.currentVersion().Checkpoint().LastSeq == 1
	}, time.Second, 10*time.Millisecond)
	tables := db.lsm.currentVersion().All()
	require.Len(t, tables, 1)
	require.NoError(t, db.Stop(t.Context()))

	data, err := filepath.Glob(filepath.Join(tables[0].Path, "*.qdat"))
	require.NoError(t, err)
	require.Len(t, data, 1)
	raw, err := os.ReadFile(data[0])
	require.NoError(t, err)
	raw[16] ^= 0x01 // inside the first block's records
	require.NoError(t, os.WriteFile(data[0], raw, 0o644))

	reopened, err := New(dir)
	require.NoError(t, err)
	require.NoError(t, reopened.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, reopened.Stop(t.Context())) })

	_, _, err = reopened.Get([]byte("key"))
	require.ErrorIs(t, err, sst.ErrChecksumMismatch)
	it := reopened.Iter(nil, nil)
	for range it.All() {
	}
	require.ErrorIs(t, it.Err(), sst.ErrChecksumMismatch)
}

func TestFailedStartReleasesLockAndCanRetry(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "wal"), 0o755))
	broken := filepath.Join(dir, "wal", "broken.quxwal")
	require.NoError(t, os.WriteFile(broken, bytes.Repeat([]byte{0xab}, 64), 0o644))

	db, err := New(dir)
	require.NoError(t, err)
	require.Error(t, db.Start(t.Context()))

	probe := flock.New(filepath.Join(dir, "quxdb.lock"))
	locked, err := probe.TryLock()
	require.NoError(t, err)
	require.True(t, locked, "failed start kept the lock")
	require.NoError(t, probe.Unlock())

	require.NoError(t, os.Remove(broken))
	require.NoError(t, db.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, db.Stop(t.Context())) })
	require.NoError(t, db.Set([]byte("key"), []byte("value")))
}

func TestStartRejectsLockedDir(t *testing.T) {
	dir := t.TempDir()
	first, err := New(dir)
	require.NoError(t, err)
	require.NoError(t, first.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, first.Stop(t.Context())) })

	second, err := New(dir)
	require.NoError(t, err)
	require.ErrorContains(t, second.Start(t.Context()), "locked by another process")
	require.NoError(t, first.Set([]byte("key"), []byte("value")))
}

func newTestQuxDB(t *testing.T) *QuxDB {
	t.Helper()

	db, err := New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))

	return db
}

func collectRange(t *testing.T, db *QuxDB, start, end []byte) []string {
	t.Helper()
	var items []string
	it := db.Iter(start, end)
	for key, value := range it.All() {
		items = append(items, string(key)+"="+string(value))
	}
	require.NoError(t, it.Err())
	return items
}

func BenchmarkDBSet(b *testing.B) {
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		b.Fatal(err)
	}
	oldStdout := os.Stdout
	os.Stdout = devNull
	defer func() {
		os.Stdout = oldStdout
		devNull.Close()
	}()

	for _, size := range []int{128, 4 << 10, 16 << 10, 128 << 10, 256 << 10, 512 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("value=%d", size), func(b *testing.B) {
			db, err := New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			if err := db.Start(b.Context()); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := db.Stop(b.Context()); err != nil {
					b.Error(err)
				}
			})
			key := []byte("benchmark-key")
			value := make([]byte, size)
			if err := db.Set(key, value); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(key) + len(value)))
			for b.Loop() {
				if err := db.Set(key, value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
