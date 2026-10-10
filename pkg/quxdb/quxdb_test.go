package quxdb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	for i := range defaultMemtableCachedImmutables {
		require.NoError(t, db.Set(fmt.Appendf(nil, "before-%d", i), []byte("value")))
		db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	}
	require.Eventually(t, func() bool {
		return db.lsm.currentVersion().Checkpoint().LastSeq == 1 &&
			db.lsm.immutableMemtableCount() == defaultMemtableCachedImmutables
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, db.Delete([]byte("key")))
	tombstoneSeq := db.committedSeq.Load()
	db.rolloverMemtable(tombstoneSeq, db.lastCommittedLSN)
	for i := range defaultMemtableCachedImmutables {
		require.NoError(t, db.Set(fmt.Appendf(nil, "after-%d", i), []byte("value")))
		db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	}
	require.Eventually(t, func() bool {
		return quxSeq(db.lsm.currentVersion().Checkpoint().LastSeq) == tombstoneSeq &&
			db.lsm.immutableMemtableCount() == defaultMemtableCachedImmutables
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
	assert.Equal(t, []string{"b=b2"}, collectRange(t, db, []byte("b"), []byte("c")))
	assert.Empty(t, collectRange(t, db, []byte("b"), []byte("b")))
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
	for i := range defaultMemtableCachedImmutables {
		assert.NoError(t, db.Set(fmt.Appendf(nil, "retained-%d", i), []byte("value")))
		db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	}
	require.Eventually(t, func() bool {
		return quxSeq(db.lsm.currentVersion().Checkpoint().LastSeq) == flushedSeq &&
			db.lsm.immutableMemtableCount() == defaultMemtableCachedImmutables
	}, time.Second, 10*time.Millisecond)

	assert.NoError(t, db.Set([]byte("a"), []byte("new-a")))
	assert.NoError(t, db.Set([]byte("b"), []byte("new-b")))
	assert.NoError(t, db.Set([]byte("c"), []byte("new-c")))
	assert.NoError(t, db.Delete([]byte("d")))

	assert.Equal(t,
		[]string{"a=new-a", "b=new-b", "c=new-c", "e=old-e", "retained-0=value"},
		collectRange(t, db, nil, nil),
	)
	// e lives in an sst and sits exactly at the exclusive upper bound
	assert.Equal(t, []string{"b=new-b", "c=new-c"}, collectRange(t, db, []byte("b"), []byte("e")))
}

func TestMemtableCheckpointCapturedAtRollover(t *testing.T) {
	db := newTestQuxDB(t)

	require.NoError(t, db.Set([]byte("one"), []byte("1")))
	require.NoError(t, db.Set([]byte("two"), []byte("2")))
	mt := db.lsm.activeMemtable()
	assert.Zero(t, mt.lastSeq)
	assert.Zero(t, mt.lastLSN)

	db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	assert.Equal(t, quxSeq(2), mt.lastSeq)
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

	for range db.Scan(nil, nil) {
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
	assert.Equal(t, quxSeq(1), db.committedSeq.Load())
}

func TestCommittedSequenceRestoredFromWAL(t *testing.T) {
	dir := t.TempDir()

	db, err := New(WithDataDir(dir))
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	require.NoError(t, db.Set([]byte("first"), []byte("1")))
	require.NoError(t, db.Set([]byte("second"), []byte("2")))
	require.NoError(t, db.Stop(t.Context()))

	reopened, err := New(WithDataDir(dir))
	require.NoError(t, err)
	require.NoError(t, reopened.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, reopened.Stop(t.Context()))
	})

	assert.Equal(t, quxSeq(2), reopened.committedSeq.Load())
	require.NoError(t, reopened.Set([]byte("third"), []byte("3")))
	assert.Equal(t, quxSeq(3), reopened.committedSeq.Load())
}

func TestCheckpointRecoveryReplaysOnlyNewerRecords(t *testing.T) {
	dir := t.TempDir()

	db, err := New(WithDataDir(dir))
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	expected := map[string]string{"first": "1", "second": "2"}
	require.NoError(t, db.Set([]byte("first"), []byte("1")))
	require.NoError(t, db.Set([]byte("second"), []byte("2")))
	db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	for i := range defaultMemtableCachedImmutables {
		key := fmt.Sprintf("retained-%d", i)
		value := fmt.Sprintf("%d", i+3)
		expected[key] = value
		require.NoError(t, db.Set([]byte(key), []byte(value)))
		db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	}

	require.Eventually(t, func() bool {
		return db.lsm.currentVersion().Checkpoint().LastSeq == 2 &&
			db.lsm.immutableMemtableCount() == defaultMemtableCachedImmutables
	}, time.Second, 10*time.Millisecond)
	checkpoint := db.lsm.currentVersion().Checkpoint()
	require.NotZero(t, checkpoint.LastLSN)

	newestSeq := quxSeq(defaultMemtableCachedImmutables + 3)
	expected["newest"] = fmt.Sprintf("%d", newestSeq)
	require.NoError(t, db.Set([]byte("newest"), []byte(expected["newest"])))
	require.NoError(t, db.Stop(t.Context()))

	reopened, err := New(WithDataDir(dir))
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
	db, err := New(WithDataDir(dir), WithMemtableBytes(16<<20))
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

	reopened, err := New(WithDataDir(dir), WithMemtableBytes(16<<20))
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
	assert.Equal(t, quxSeq(n), reopened.committedSeq.Load())
	for i := range n {
		got, found, err := reopened.Get(fmt.Appendf(nil, "key-%04d", i))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, value, got)
	}
}

func TestCorruptBlockSurfacesAsReadError(t *testing.T) {
	dir := t.TempDir()
	db, err := New(WithDataDir(dir))
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	require.NoError(t, db.Set([]byte("key"), []byte("value")))
	db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	for i := range defaultMemtableCachedImmutables {
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

	reopened, err := New(WithDataDir(dir))
	require.NoError(t, err)
	require.NoError(t, reopened.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, reopened.Stop(t.Context())) })

	_, _, err = reopened.Get([]byte("key"))
	require.ErrorIs(t, err, sst.ErrChecksumMismatch)
	var scanErr error
	for _, err := range reopened.Scan(nil, nil) {
		scanErr = err
	}
	require.ErrorIs(t, scanErr, sst.ErrChecksumMismatch)
}

func TestFailedStartReleasesLockAndCanRetry(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "wal"), 0o755))
	broken := filepath.Join(dir, "wal", "broken.quxwal")
	require.NoError(t, os.WriteFile(broken, bytes.Repeat([]byte{0xab}, 64), 0o644))

	db, err := New(WithDataDir(dir))
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
	first, err := New(WithDataDir(dir))
	require.NoError(t, err)
	require.NoError(t, first.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, first.Stop(t.Context())) })

	second, err := New(WithDataDir(dir))
	require.NoError(t, err)
	require.ErrorContains(t, second.Start(t.Context()), "locked by another process")
	require.NoError(t, first.Set([]byte("key"), []byte("value")))
}

func newTestQuxDB(t *testing.T) *DB {
	t.Helper()

	db, err := New(WithDataDir(t.TempDir()))
	require.NoError(t, err)
	require.NoError(t, db.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, db.Stop(t.Context())) })

	return db
}

func collectRange(t *testing.T, db *DB, lower, upper []byte) []string {
	t.Helper()
	var items []string
	for e, err := range db.Scan(lower, upper) {
		require.NoError(t, err)
		items = append(items, string(e.Key)+"="+string(e.Value))
	}
	return items
}

func BenchmarkDBSet(b *testing.B) {
	for _, size := range []int{128, 4 << 10, 16 << 10, 128 << 10, 256 << 10, 512 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("value=%d", size), func(b *testing.B) {
			db, err := New(WithDataDir(b.TempDir()))
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

const (
	benchKeys     = 50_000
	benchValueLen = 128
)

// BenchmarkDBReadWrite runs parallel reads while writers update random keys, over memtables and an sst.
func BenchmarkDBReadWrite(b *testing.B) {
	keys := make([][]byte, benchKeys)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "user-key-%08d", i)
	}
	db := newBenchDB(b, keys)

	for _, writers := range []int{0, 8} {
		b.Run(fmt.Sprintf("Get/writers=%d", writers), func(b *testing.B) {
			benchReadWrite(b, db, keys, writers, func(r *rand.Rand) {
				if _, _, err := db.Get(keys[r.IntN(len(keys))]); err != nil {
					b.Error(err)
				}
			})
		})
		b.Run(fmt.Sprintf("Scan50/writers=%d", writers), func(b *testing.B) {
			benchReadWrite(b, db, keys, writers, func(r *rand.Rand) {
				n := 0
				for _, err := range db.Scan(keys[r.IntN(len(keys))], nil) {
					if err != nil {
						b.Error(err)
					}
					if n++; n == 50 {
						break
					}
				}
			})
		})
	}
}

// reports reads/s across b.N parallel reads and the writes/s the writers kept up meanwhile
func benchReadWrite(b *testing.B, db *DB, keys [][]byte, writers int, read func(r *rand.Rand)) {
	var stop atomic.Bool
	var writes atomic.Int64
	var wg sync.WaitGroup
	val := make([]byte, benchValueLen)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 1))
			for !stop.Load() {
				if err := db.Set(keys[r.IntN(len(keys))], val); err != nil {
					b.Error(err)
					return
				}
				writes.Add(1)
			}
		}()
	}

	var seed atomic.Uint64
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewPCG(seed.Add(1), 2))
		for pb.Next() {
			read(r)
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()
	stop.Store(true)
	wg.Wait()

	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "reads/s")
	b.ReportMetric(float64(writes.Load())/elapsed.Seconds(), "writes/s")
}

// loads half the keys into an sst and the rest into memtables, in shuffled order
func newBenchDB(b *testing.B, keys [][]byte) *DB {
	db, err := New(WithDataDir(b.TempDir()))
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

	order := rand.New(rand.NewPCG(1, 2)).Perm(len(keys))
	val := make([]byte, benchValueLen)
	// concurrent sets share wal syncs, so the load stays fast
	load := func(order []int) {
		var wg sync.WaitGroup
		for w := range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := w; i < len(order); i += 32 {
					if err := db.Set(keys[order[i]], val); err != nil {
						b.Error(err)
						return
					}
				}
			}()
		}
		wg.Wait()
	}

	load(order[:len(order)/2])
	flushed := db.committedSeq.Load()
	db.rolloverMemtable(flushed, db.lastCommittedLSN)
	for i := range defaultMemtableCachedImmutables {
		if err := db.Set(fmt.Appendf(nil, "pad-%d", i), val); err != nil {
			b.Fatal(err)
		}
		db.rolloverMemtable(db.committedSeq.Load(), db.lastCommittedLSN)
	}
	for deadline := time.Now().Add(10 * time.Second); quxSeq(db.lsm.currentVersion().Checkpoint().LastSeq) < flushed; {
		if time.Now().After(deadline) {
			b.Fatal("memtable flush timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
	load(order[len(order)/2:])
	return db
}
