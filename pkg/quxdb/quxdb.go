package quxdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"

	"github.com/yashgorana/quxdb/pkg/core"
	"github.com/yashgorana/quxdb/pkg/memtable"
	"github.com/yashgorana/quxdb/pkg/metrics"
	"github.com/yashgorana/quxdb/pkg/pathlib"
	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/wal"
)

const (
	MaxKeySize   = 4 << 10 // largest user key Set or Delete accepts
	MaxValueSize = 4 << 20 // largest value Set accepts
)

var (
	ErrDbReadOnly    = errors.New("db: read-only after a wal failure")
	ErrKeyTooLarge   = fmt.Errorf("db: key exceeds %d bytes", MaxKeySize)
	ErrValueTooLarge = fmt.Errorf("db: value exceeds %d bytes", MaxValueSize)
)

// DB is an LSM key-value store.
type DB struct {
	dataDir string
	opts    Options
	log     *slog.Logger
	flock   *flock.Flock

	reqPool     sync.Pool
	seekKeyPool sync.Pool
	reqChan     chan *writeReq
	workers     sync.WaitGroup

	wal    *wal.WAL
	walEnc batchEncoder

	imtNotify chan struct{}

	lsm       *lsmState
	compactor *lsmCompactor

	committedSeq     atomicSeq
	writeSeq         quxSeq
	lastCommittedLSN wal.LSN
	readOnly         atomic.Pointer[error] // set once by the first wal failure
}

// New opens a DB with DefaultOptions changed by opts.
func New(opts ...Option) (*DB, error) {
	o := DefaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return NewWithOptions(o)
}

// NewWithOptions opens a DB configured by o, start from DefaultOptions.
func NewWithOptions(o Options) (*DB, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}

	dataDir, err := filepath.Abs(o.DataDir)
	if err != nil {
		return nil, err
	}

	if err := pathlib.EnsureDir(dataDir); err != nil {
		return nil, err
	}

	walOpts := o.WAL.Options
	walOpts.Logger = o.Logger.With("mod", "wal")
	w, err := wal.NewWithOptions(dataDir, walOpts)
	if err != nil {
		return nil, err
	}

	lsm, err := newLsmState(dataDir, o)
	if err != nil {
		return nil, err
	}
	db := &DB{
		dataDir:   dataDir,
		opts:      o,
		log:       o.Logger.With("mod", "quxdb"),
		wal:       w,
		walEnc:    newBatchEncoder(o.MaxBatchRequests),
		flock:     flock.New(filepath.Join(dataDir, "quxdb.lock")),
		imtNotify: make(chan struct{}, 1),
		reqChan:   make(chan *writeReq, o.MaxBatchRequests*4), // particular reason why this is 4x
		lsm:       lsm,
	}

	// compaction passes are a convenient time to prune the wal
	db.compactor = newLsmCompactor(dataDir, lsm, o, func() {
		if err := db.wal.Prune(); err != nil {
			db.log.Error("wal prune failed", "err", err)
		}
	})

	db.reqPool.New = func() any {
		return newWriteReq()
	}

	db.seekKeyPool.New = func() any {
		return new([]byte)
	}

	return db, nil
}

func (db *DB) Start(ctx context.Context) error {
	locked, err := db.flock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("db: %s is locked by another process", db.dataDir)
	}

	version := db.lsm.currentVersion()
	checkpoint := version.Checkpoint()
	lastSeq := quxSeq(checkpoint.LastSeq)
	lastLSN := wal.LSN(checkpoint.LastLSN)
	db.committedSeq.Store(lastSeq)

	db.log.Info("wal recovery started", "tables", version.Len(), "checkpointSeq", checkpoint.LastSeq, "afterLSN", lastLSN)
	recoverStart := time.Now()
	var entries, keys int
	checkpointSeq := lastSeq
	err = db.wal.Recover(lastLSN, func(r wal.Entry) error {
		entries++
		for qkey, val := range decodeBatch(r.Data) {
			// flushed before a mid-batch memtable rollover
			if qkey.Seq() <= checkpointSeq {
				continue
			}
			if err := db.setMemtable(qkey, val, lastSeq, lastLSN); err != nil {
				return err
			}
			lastSeq = qkey.Seq()
			keys++
		}
		lastLSN = r.LSN
		db.committedSeq.Store(lastSeq)
		return nil
	}, db.opts.WAL.CorruptionPolicy)
	if err != nil {
		// the caller may retry or inspect the data dir
		return errors.Join(err, db.wal.Close(), db.flock.Unlock())
	}
	db.log.Info("wal recovery completed", "entries", entries, "keys", keys, "lastSeq", lastSeq, "lastLSN", lastLSN,
		"took", time.Since(recoverStart))

	db.writeSeq = lastSeq
	db.lastCommittedLSN = lastLSN
	db.wal.RetainFrom(wal.LSN(checkpoint.LastLSN))

	db.compactor.Start()
	db.compactor.Notify()

	db.workers.Add(2)
	go db.flushMemtables()
	go db.writeLoop()

	db.log.Info("started", "dir", db.dataDir, "memtableType", db.opts.Memtable.Type, "lastSeq", db.committedSeq.Load())
	return nil
}

func (db *DB) Stop(ctx context.Context) error {
	defer db.log.Info("stopped")

	close(db.reqChan)
	db.workers.Wait()
	db.compactor.Stop()

	return errors.Join(
		db.wal.Close(),
		db.lsm.close(),
		db.flock.Unlock(),
		os.Remove(filepath.Join(db.dataDir, "quxdb.lock")),
	)
}

func (db *DB) Set(key []byte, value []byte) error {
	if err := db.Err(); err != nil {
		return err
	}
	req, err := db.enqueueWrite(key, value, quxOpSet)
	if err != nil {
		return err
	}
	return db.awaitResult(req).err
}

// Err returns the error that made the db read-only, or nil.
func (db *DB) Err() error {
	if err := db.readOnly.Load(); err != nil {
		return *err
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool, error) {
	// longer keys are never written
	if len(key) > MaxKeySize {
		return nil, false, nil
	}

	start := time.Now()
	view := db.lsm.acquire()
	defer view.release()
	// pin before loading readSeq so compaction can't drop versions visible at it
	readSeq := db.committedSeq.Load()

	// seek start, the newest version of key at or below readSeq
	bp := db.seekKeyPool.Get().(*[]byte)
	lookupKey := appendQuxKey((*bp)[:0], key, readSeq, quxOp(0xFF))
	defer func() {
		*bp = lookupKey
		db.seekKeyPool.Put(bp)
	}()

	for _, mt := range view.memtables {
		ikey, val, found := mt.Seek(lookupKey)
		if value, exists, done := resolve(key, ikey, val, found); done {
			metrics.SstProbesPerGet.Observe(0)
			metrics.DbGetMemtable.Observe(time.Since(start).Seconds())
			return value, exists, nil
		}
	}

	var probed, negatives, falsePositives int
	for meta := range view.version.PointLookup(key) {
		table := view.tables.Table(meta.ID)
		probed++
		if !table.MayContain(key) {
			negatives++
			continue
		}
		ikey, val, found, err := table.Seek(lookupKey)
		if err != nil {
			observeProbes(probed, negatives, falsePositives)
			metrics.DbGetError.Observe(time.Since(start).Seconds())
			return nil, false, err
		}
		if value, exists, done := resolve(key, ikey, val, found); done {
			metrics.SstBloomTruePositive.Inc()
			observeProbes(probed, negatives, falsePositives)
			metrics.DbGetSST.Observe(time.Since(start).Seconds())
			return value, exists, nil
		}
		falsePositives++
	}

	observeProbes(probed, negatives, falsePositives)
	metrics.DbGetNotFound.Observe(time.Since(start).Seconds())
	return nil, false, nil
}

func observeProbes(probed, negatives, falsePositives int) {
	metrics.SstProbesPerGet.Observe(float64(probed))
	if negatives > 0 {
		metrics.SstBloomNegative.Add(float64(negatives))
	}
	if falsePositives > 0 {
		metrics.SstBloomFalsePositive.Add(float64(falsePositives))
	}
}

// `done` reports whether a newest-first seek result decides the lookup.
func resolve(key, ikey, val []byte, found bool) (value []byte, exists, done bool) {
	if !found {
		return nil, false, false
	}
	switch quxKey(ikey).Match(key) {
	case keyMatchExact:
		return bytes.Clone(val), true, true
	case keyMatchDeleted:
		return nil, false, true
	}
	return nil, false, false
}

func (db *DB) Delete(key []byte) error {
	if err := db.Err(); err != nil {
		return err
	}
	req, err := db.enqueueWrite(key, nil, quxOpDelete)
	if err != nil {
		return err
	}
	return db.awaitResult(req).err
}

// Entry is a key-value pair borrowed from a scan until its next iteration.
type Entry struct {
	Key, Value []byte
}

// Scan yields live entries with keys in [lowerKey, upperKey) from one pinned view, nil bounds are open.
// a read error ends the scan as the final pair.
func (db *DB) Scan(lowerKey, upperKey []byte) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		if lowerKey != nil && upperKey != nil && bytes.Compare(lowerKey, upperKey) >= 0 {
			return
		}

		view := db.lsm.acquire()
		defer view.release()
		// pin before loading readSeq so compaction can't drop versions visible at it
		readSeq := db.committedSeq.Load()

		sources := scanSources(view, lowerKey, upperKey, readSeq)

		cur := newSnapshotIterator(newMergeIterator(sources), readSeq, true)
		for {
			qkey, val, ok := cur.Next()
			if !ok {
				break
			}
			if !yield(Entry{Key: quxKey(qkey).UserKey(), Value: val}, nil) {
				return
			}
		}
		if err := cur.Err(); err != nil {
			yield(Entry{}, err)
		}
	}
}

// kept out of the scan closure so its range-over-func state stays on the stack
func scanSources(view *lsmView, lowerUserKey, upperUserKey []byte, readSeq quxSeq) []core.Iterator {
	seekStart, seekEnd := newSeekRange(lowerUserKey, upperUserKey, readSeq)

	sources := make([]core.Iterator, 0, len(view.memtables)+view.version.Len())
	for _, mt := range view.memtables {
		sources = append(sources, mt.Iterator(seekStart, seekEnd))
	}
	for meta := range view.version.RangeLookup(lowerUserKey, upperUserKey) {
		sources = append(sources, view.tables.Table(meta.ID).Iterator(seekStart, seekEnd))
	}
	return sources
}

func (db *DB) enqueueWrite(key, val []byte, op quxOp) (*writeReq, error) {
	if len(key) > MaxKeySize {
		return nil, ErrKeyTooLarge
	}
	if len(val) > MaxValueSize {
		return nil, ErrValueTooLarge
	}
	w := db.reqPool.Get().(*writeReq)
	w.qkey = appendQuxKey(w.qkey[:0], key, 0, op)
	w.val = val
	w.res = writeResult{}
	w.enqueuedAt = time.Now()

	db.reqChan <- w
	return w, nil
}

func (db *DB) awaitResult(req *writeReq) writeResult {
	<-req.done
	res := req.res

	req.val = nil
	req.res = writeResult{}
	db.reqPool.Put(req)

	return res
}

func (db *DB) writeLoop() {
	defer db.workers.Done()
	defer close(db.imtNotify)
	batch := make([]*writeReq, 0, db.opts.MaxBatchRequests)
	db.log.Debug("write loop started")

	for req := range db.reqChan {
		batch = append(batch, req)
		// sole receiver, so a non-empty buffer never blocks
		for len(batch) < db.opts.MaxBatchRequests && len(db.reqChan) > 0 {
			batch = append(batch, <-db.reqChan)
		}

		db.commitBatch(batch)

		clear(batch)
		batch = batch[:0]
	}
	db.log.Debug("write loop exited")
}

func (db *DB) commitBatch(batch []*writeReq) {
	metrics.DbCommitBacklogSize.Observe(float64(len(db.reqChan)))
	metrics.DbCommitBatchSize.Observe(float64(len(batch)))

	commitStart := time.Now()
	for _, req := range batch {
		db.writeSeq++
		req.qkey.SetSeq(db.writeSeq)
	}

	encodeStart := time.Now()
	parts := db.walEnc.Encode(batch)

	appendStart := time.Now()
	lsn, err := db.wal.Append(parts, wal.DurabilitySynced)
	db.walEnc.Reset()
	if errors.Is(err, wal.ErrWALFailed) {
		err = db.enterReadOnly(err)
	}

	memStart := time.Now()
	if err == nil {
		lastSeq := db.committedSeq.Load()
		var userBytes int
		for _, req := range batch {
			// a rollover here checkpoints the previous wal Entry's lsn
			if err := db.setMemtable(req.qkey, req.val, lastSeq, db.lastCommittedLSN); err != nil {
				panic(fmt.Sprintf("unknown error %v", err))
			}
			lastSeq = req.qkey.Seq()
			userBytes += len(req.qkey.UserKey()) + len(req.val)
		}
		metrics.DbUserBytesWritten.Add(float64(userBytes))
		// one store per batch keeps readers' copy of this line valid between batches
		db.committedSeq.Store(lastSeq)
		db.lastCommittedLSN = lsn
	}

	commitEnd := time.Now()
	commitDur := commitEnd.Sub(commitStart)

	for _, req := range batch {
		// unblock waiters
		req.res = writeResult{
			lsn:       uint64(lsn),
			err:       err,
			queueWait: commitStart.Sub(req.enqueuedAt),
			commitDur: commitDur,
			totalDur:  commitEnd.Sub(req.enqueuedAt),
		}

		metrics.DbSetQueueWaitDuration.Observe(req.res.queueWait.Seconds())
		metrics.DbSetTotalDuration.Observe(req.res.totalDur.Seconds())

		req.done <- struct{}{}
	}

	metrics.DbCommitDuration.Observe(commitDur.Seconds())
	metrics.DbCommitEncodeDuration.Observe(appendStart.Sub(encodeStart).Seconds())
	metrics.DbCommitAppendDuration.Observe(memStart.Sub(appendStart).Seconds())
	metrics.DbCommitMemSetDuration.Observe(commitEnd.Sub(memStart).Seconds())
	db.lsm.publishMemtableMetrics()
}

func (db *DB) setMemtable(qkey quxKey, val []byte, lastSeq quxSeq, lastLSN wal.LSN) error {
	for {
		err := db.lsm.activeMemtable().Set(qkey, val)
		if !errors.Is(err, memtable.ErrMemtableFull) {
			return err
		}
		db.rolloverMemtable(lastSeq, lastLSN)
	}
}

// enterReadOnly stops accepting writes after a wal failure and returns the error writes get from now on.
func (db *DB) enterReadOnly(cause error) error {
	err := fmt.Errorf("%w: %w", ErrDbReadOnly, cause)
	if db.readOnly.CompareAndSwap(nil, &err) {
		db.log.Error("entering read-only mode", "err", cause)
		metrics.DbReadOnly.Set(1)
	}
	return db.Err()
}

func (db *DB) rolloverMemtable(lastSeq quxSeq, lastLSN wal.LSN) {
	mt := db.lsm.rolloverMemtable(lastSeq, lastLSN)

	db.log.Info("memtable rollover", "bytes", mt.SizeBytes(), "keys", mt.Len(), "lastSeq", mt.lastSeq, "lastLSN", mt.lastLSN)

	// non-blocking, the flush worker decides whether enough immutables are queued
	select {
	case db.imtNotify <- struct{}{}:
	default:
	}
}

func (db *DB) flushMemtables() {
	defer db.workers.Done()
	db.log.Debug("memtable flush started")
	for {
		_, ok := <-db.imtNotify
		if !ok {
			break
		}

		mtsToFlush := db.lsm.flushableMemtables(db.opts.Memtable.CachedImmutables)
		if len(mtsToFlush) == 0 {
			db.log.Debug("flush skipped", "queued", db.lsm.immutableMemtableCount(), "retain", db.opts.Memtable.CachedImmutables)
			continue
		}

		newSSTs := make([]*sst.Metadata, 0, len(mtsToFlush))
		for _, mt := range mtsToFlush {
			flushStart := time.Now()
			b, err := sst.NewBuilderWithOptions(sst.BuilderOpts{
				Dir:       db.dataDir,
				ID:        db.lsm.nextTableID(),
				Level:     0,
				Keys:      uint64(mt.Len()),
				SizeBytes: uint64(mt.SizeBytes()),
			}, db.opts.SST)
			if err != nil {
				panic(err)
			}
			c := newSnapshotIterator(mt.Iterator(nil, nil), math.MaxUint64, false)
			for {
				key, val, ok := c.Next()
				if !ok {
					break
				}

				err := b.Add(sst.Record{
					OrderedKey: key,
					FilterKey:  quxKey(key).UserKey(),
					Value:      val,
				})
				if err != nil {
					panic(err)
				}
			}
			sstMeta, err := b.Finalize()
			if err != nil {
				panic(err)
			}
			took := time.Since(flushStart)
			metrics.DbFlushDuration.Observe(took.Seconds())
			metrics.DbFlushBytesWritten.Add(float64(sstMeta.SizeBytes))
			db.log.Info("new sst", "level", sstMeta.Level, "id", sstMeta.ID, "keys", sstMeta.Keys,
				"bytes", sstMeta.SizeBytes, "took", took)
			newSSTs = append(newSSTs, sstMeta)
		}

		if err := db.lsm.replaceMemtablesWithSSTs(mtsToFlush, newSSTs); err != nil {
			panic(err)
		}
		// the catalog checkpoint is durable here
		db.wal.RetainFrom(wal.LSN(db.lsm.currentVersion().Checkpoint().LastLSN))
		db.log.Info("memtable flush", "flushed", len(mtsToFlush), "queued", db.lsm.immutableMemtableCount())

		db.compactor.Notify()
	}
	db.log.Debug("memtable flush exited")
}
