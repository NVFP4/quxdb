package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
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
	memTableType = memtable.BTree
	maxBatch     = 128

	cachedImmutables = 1 // immutable memtables kept in memory to serve reads before flushing

	// truncating on mid-log wal corruption drops acknowledged writes after the damage
	walCorruptionPolicy = wal.StopOnCorruption
)

var ErrReadOnly = errors.New("db: read-only after a wal failure")

type QuxDB struct {
	dataDir string
	flock   *flock.Flock

	reqPool sync.Pool
	reqChan chan *writeReq
	workers sync.WaitGroup

	wal    *wal.WAL
	walEnc batchEncoder

	imtNotify chan struct{}

	lsm       *lsmState
	compactor *lsmCompactor

	committedSeq     atomic.Uint64
	writeSeq         quxSeq
	lastCommittedLSN wal.LSN
	readOnly         atomic.Pointer[error] // set once by the first wal failure
}

func New(dataDir string) (*QuxDB, error) {
	dataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}

	if err := pathlib.EnsureDir(dataDir); err != nil {
		return nil, err
	}

	wal, err := wal.New(dataDir)
	if err != nil {
		return nil, err
	}

	lsm, err := newLsmState(dataDir)
	if err != nil {
		return nil, err
	}
	db := &QuxDB{
		dataDir:   dataDir,
		wal:       wal,
		flock:     flock.New(filepath.Join(dataDir, "quxdb.lock")),
		imtNotify: make(chan struct{}, 1),
		reqChan:   make(chan *writeReq, maxBatch*4), // particular reason why this is 4x
		lsm:       lsm,
	}

	// compaction passes are a convenient time to prune the wal
	db.compactor = newLsmCompactor(dataDir, lsm, func() {
		if err := db.wal.Prune(); err != nil {
			fmt.Printf("db: wal prune error %v\n", err)
		}
	})

	db.reqPool.New = func() any {
		return newWriteReq()
	}

	return db, nil
}

func (db *QuxDB) Start(ctx context.Context) error {
	locked, err := db.flock.TryLock()
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("db: %s is locked by another process", db.dataDir)
	}

	checkpoint := db.lsm.currentVersion().Checkpoint()
	lastSeq := checkpoint.LastSeq
	lastLSN := wal.LSN(checkpoint.LastLSN)
	db.committedSeq.Store(lastSeq)

	fmt.Printf("db: wal recover after %d\n", lastLSN)
	checkpointSeq := lastSeq
	err = db.wal.Recover(lastLSN, func(r wal.Record) error {
		for qkey, val := range decodeBatch(r.Data) {
			// flushed before a mid-batch memtable rollover
			if qkey.Seq() <= checkpointSeq {
				continue
			}
			if err := db.setMemtable(qkey, val, lastSeq, lastLSN); err != nil {
				return err
			}
			lastSeq = qkey.Seq()
		}
		lastLSN = r.LSN
		db.committedSeq.Store(lastSeq)
		return nil
	}, walCorruptionPolicy)
	if err != nil {
		// the caller may retry or inspect the data dir
		return errors.Join(err, db.wal.Close(), db.flock.Unlock())
	}
	fmt.Println("db: wal recover completed")

	db.writeSeq = quxSeq(lastSeq)
	db.lastCommittedLSN = lastLSN
	db.wal.RetainFrom(wal.LSN(checkpoint.LastLSN))

	db.compactor.Start()
	db.compactor.Notify()

	db.workers.Add(2)
	go db.flushMemtables()
	go db.writeLoop()

	fmt.Printf("db: started with dir=%s memtableType=%s lastSeq=%d\n", db.dataDir, memTableType, db.committedSeq.Load())
	return nil
}

func (db *QuxDB) Stop(ctx context.Context) error {
	defer fmt.Println("db: stopped")

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

func (db *QuxDB) Set(key []byte, value []byte) error {
	if err := db.Err(); err != nil {
		return err
	}
	req := db.enqueueWrite(key, value, quxOpSet)
	res := db.awaitResult(req)
	return res.err
}

// Err returns the error that made the db read-only, or nil.
func (db *QuxDB) Err() error {
	if err := db.readOnly.Load(); err != nil {
		return *err
	}
	return nil
}

func (db *QuxDB) Get(key []byte) ([]byte, bool, error) {
	start := time.Now()
	view := db.lsm.acquire()
	defer view.release()
	// pin before loading readSeq so compaction can't drop versions visible at it
	readSeq := db.committedSeq.Load()

	lookupKey := newSeekStart(key, readSeq) // heap alloc

	for _, mt := range view.memtables {
		ikey, val, found := mt.Seek(lookupKey)
		if value, exists, done := resolve(key, ikey, val, found); done {
			metrics.SstProbesPerGet.Observe(0)
			metrics.DbGetMemtable.Observe(time.Since(start).Seconds())
			return value, exists, nil
		}
	}

	var probed, negatives, falsePositives int
	for table := range view.tableCandidates(key) {
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

func (db *QuxDB) Delete(key []byte) error {
	if err := db.Err(); err != nil {
		return err
	}
	req := db.enqueueWrite(key, nil, quxOpDelete)
	res := db.awaitResult(req)
	return res.err
}

// Iterator scans a key range, check Err after ranging over All.
type Iterator struct {
	db         *QuxDB
	start, end []byte
	err        error
}

// Iter returns an iterator over live keys in [start, end], nil bounds are open.
func (db *QuxDB) Iter(start, end []byte) *Iterator {
	return &Iterator{db: db, start: start, end: end}
}

// Err returns the read error that ended the last All, if any.
func (it *Iterator) Err() error {
	return it.err
}

// All yields live keys in order from a view pinned for the whole range.
func (it *Iterator) All() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		it.err = nil
		start, end := it.start, it.end
		if start != nil && end != nil && bytes.Compare(start, end) > 0 {
			return
		}

		view := it.db.lsm.acquire()
		defer view.release()
		readSeq := it.db.committedSeq.Load()

		var startKey, endKey []byte
		if start != nil {
			startKey = newSeekStart(start, readSeq) // heap alloc
		}
		if end != nil {
			endKey = newSeekEndInclusive(end) // heap alloc
		}

		tables := view.tableRangeCandidates(start, end)
		sources := make([]core.Cursor, 0, len(view.memtables)+len(tables))
		for _, mt := range view.memtables {
			sources = append(sources, mt.Cursor(startKey, endKey))
		}
		for _, table := range tables {
			sources = append(sources, table.Cursor(startKey, endKey))
		}

		cur := newMVCCCursor(newMergeCursor(sources), readSeq, false)
		for {
			key, val, ok := cur.Next()
			if !ok {
				it.err = cur.Err()
				return
			}
			if !yield(quxKey(key).UserKey(), val) {
				return
			}
		}
	}
}

func (db *QuxDB) enqueueWrite(key, val []byte, op quxOp) *writeReq {
	w := db.reqPool.Get().(*writeReq)
	w.qkey = newQuxKey(key, 0, op) // heap alloc
	w.val = val
	w.res = writeResult{}
	w.enqueuedAt = time.Now()

	db.reqChan <- w
	return w
}

func (db *QuxDB) awaitResult(req *writeReq) writeResult {
	<-req.done
	res := req.res

	req.qkey, req.val = nil, nil
	req.res = writeResult{}
	db.reqPool.Put(req)

	return res
}

func (db *QuxDB) writeLoop() {
	defer db.workers.Done()
	defer close(db.imtNotify)
	batch := make([]*writeReq, 0, maxBatch)
	fmt.Println("db: write loop started")

	for {
		// block for first req
		req, ok := <-db.reqChan
		if !ok {
			break
		}
		batch = append(batch, req)

	drain:
		for len(batch) < maxBatch {
			select { // non-blocking channel check
			case req, ok := <-db.reqChan:
				if !ok {
					break drain
				}
				batch = append(batch, req)
			default:
				break drain
			}
		}

		db.commitBatch(batch)

		clear(batch)
		batch = batch[:0]
	}
	fmt.Println("db: write loop exited")
}

func (db *QuxDB) commitBatch(batch []*writeReq) {
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
			// a rollover here checkpoints the previous wal Record's lsn
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

func (db *QuxDB) setMemtable(qkey quxKey, val []byte, lastSeq uint64, lastLSN wal.LSN) error {
retry:
	if err := db.lsm.activeMemtable().Set(qkey, val); err != nil {
		if errors.Is(err, memtable.ErrMemtableFull) {
			db.rolloverMemtable(lastSeq, lastLSN)
			goto retry
		}
		return err
	}
	return nil
}

// enterReadOnly stops accepting writes after a wal failure and returns the error writes get from now on.
func (db *QuxDB) enterReadOnly(cause error) error {
	err := fmt.Errorf("%w: %w", ErrReadOnly, cause)
	if db.readOnly.CompareAndSwap(nil, &err) {
		fmt.Printf("db: entering read-only mode: %v\n", cause)
		metrics.DbReadOnly.Set(1)
	}
	return db.Err()
}

func (db *QuxDB) rolloverMemtable(lastSeq uint64, lastLSN wal.LSN) {
	mt := db.lsm.rolloverMemtable(lastSeq, lastLSN)

	fmt.Printf("db: rollover memtable size=%.2fMB keys=%d lastSeq=%d lastLSN=%d\n",
		float64(mt.SizeBytes())/(1024*1024), mt.Len(), mt.lastSeq, mt.lastLSN)

	// non-blocking, the flush worker decides whether enough immutables are queued
	select {
	case db.imtNotify <- struct{}{}:
	default:
	}
}

func (db *QuxDB) flushMemtables() {
	defer db.workers.Done()
	fmt.Println("db: memtable flush started")
	for {
		_, ok := <-db.imtNotify
		if !ok {
			break
		}

		mtsToFlush := db.lsm.flushableMemtables(cachedImmutables)
		if len(mtsToFlush) == 0 {
			continue
		}

		newSSTs := make([]*sst.Metadata, 0, len(mtsToFlush))
		for _, mt := range mtsToFlush {
			flushStart := time.Now()
			b, err := sst.NewBuilder(sst.BuilderOpts{
				Dir:       db.dataDir,
				ID:        db.lsm.nextTableID(),
				Level:     0,
				Keys:      uint64(mt.Len()),
				SizeBytes: uint64(mt.SizeBytes()),
			})
			if err != nil {
				panic(err)
			}
			// a single memtable needs no merge
			c := newMVCCCursor(mt.Cursor(nil, nil), math.MaxUint64, true)
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
			metrics.DbFlushDuration.Observe(time.Since(flushStart).Seconds())
			metrics.DbFlushBytesWritten.Add(float64(sstMeta.SizeBytes))
			fmt.Printf("db: new sst level=%d path=%s\n", sstMeta.Level, sstMeta.Path)
			newSSTs = append(newSSTs, sstMeta)
		}

		if err := db.lsm.replaceMemtablesWithSSTs(mtsToFlush, newSSTs); err != nil {
			panic(err)
		}
		// the catalog checkpoint is durable here
		db.wal.RetainFrom(wal.LSN(db.lsm.currentVersion().Checkpoint().LastLSN))
		fmt.Printf("db: memtable flush flushed=%d queued=%d\n",
			len(mtsToFlush), db.lsm.immutableMemtableCount())

		db.compactor.Notify()
	}
	fmt.Println("db: memtable flush exited")
}
