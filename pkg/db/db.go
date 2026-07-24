package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"

	"github.com/yashgorana/quxdb/pkg/bufpool"
	"github.com/yashgorana/quxdb/pkg/memtable"
	"github.com/yashgorana/quxdb/pkg/metrics"
	"github.com/yashgorana/quxdb/pkg/pathlib"
	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/wal"
)

const (
	memTableType = memtable.BTree
	maxBatch     = 128
	fullSync     = true

	imtFlushThreshold = 1 // holds this many `imt` in memory before flushing
	maxImt            = 4 // `imt` beyond this value stalls write
)

type QuxDB struct {
	committedSeq atomic.Uint64
	_            [56]byte // cache line padding

	// writeSeq and lastCommittedLSN are owned by writeLoop after recovery.
	writeSeq         quxSeq
	lastCommittedLSN wal.LSN

	dataDir string
	flock   *flock.Flock

	reqPool sync.Pool
	reqChan chan *writeReq
	workers sync.WaitGroup

	wal    *wal.WAL
	walBuf [][]byte

	imtNotify chan struct{}

	lsm       *lsmState
	compactor *lsmCompactor
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

	lsm, err := newLsmState(dataDir, maxImt)
	if err != nil {
		return nil, err
	}
	db := &QuxDB{
		dataDir:   dataDir,
		wal:       wal,
		flock:     flock.New(filepath.Join(dataDir, "quxdb.lock")),
		imtNotify: make(chan struct{}, 1),
		reqChan:   make(chan *writeReq, maxBatch*4), // particular reason why this is 4x
		walBuf:    make([][]byte, 0, maxBatch),
		lsm:       lsm,
	}

	db.compactor = newLsmCompactor(dataDir, lsm)

	db.reqPool.New = func() any {
		return newWriteReq()
	}

	return db, nil
}

func (db *QuxDB) Start(ctx context.Context) error {
	_, err := db.flock.TryLock()
	if err != nil {
		return err
	}

	checkpoint := db.lsm.currentVersion().Checkpoint()
	lastSeq := checkpoint.LastSeq
	lastLSN := wal.LSN(checkpoint.LastLSN)
	db.committedSeq.Store(lastSeq)

	fmt.Println("db: open wal")
	if err := db.wal.Open(); err != nil {
		return err
	}

	fmt.Printf("db: wal replay from %d\n", lastLSN)
	var kv quxKV
	lsn, err := db.wal.ReplayAfter(lastLSN, func(r wal.Record) error {
		kv.Decode(r.Data)
		if err := db.setMemtable(kv, lastSeq, lastLSN); err != nil {
			return err
		}
		lastSeq = kv.qkey.Seq()
		lastLSN = r.LSN
		db.committedSeq.Store(lastSeq)
		return nil
	})
	if err != nil {
		if !wal.IsCorruption(err) {
			return err
		}
		fmt.Printf("db: error %v - truncating to lsn=%d\n", err, lsn)
		if truncateErr := db.wal.TruncateFrom(lsn); truncateErr != nil {
			return errors.Join(err, truncateErr)
		}
	}
	fmt.Println("db: wal replay completed")

	db.writeSeq = quxSeq(lastSeq)
	db.lastCommittedLSN = lastLSN

	if err := db.wal.PruneBefore(wal.LSN(checkpoint.LastLSN)); err != nil {
		return err
	}

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
	clear(db.walBuf)

	return errors.Join(
		db.wal.Close(),
		db.lsm.close(),
		db.flock.Unlock(),
		os.Remove(filepath.Join(db.dataDir, "quxdb.lock")),
	)
}

func (db *QuxDB) Set(key []byte, value []byte) error {
	req := db.enqueueWrite(key, value, quxOpSet)
	res := db.awaitResult(req)
	return res.err
}

func (db *QuxDB) Get(key []byte) ([]byte, bool) {
	readSeq := db.committedSeq.Load()
	snapshot := db.lsm.acquireReadSnapshot()
	defer func() {
		db.lsm.reportTableCleanupError(snapshot.release())
	}()

	lookupKey := newSeekStart(key, readSeq) // heap alloc

	ikey, val, found := snapshot.memtables[0].Seek(lookupKey)
	if found {
		if resolved, value, exists := resolvePointLookup(ikey, val, key); resolved {
			return bytes.Clone(value), exists
		}
	}

	for i := len(snapshot.memtables) - 1; i >= 1; i-- {
		ikey, val, found := snapshot.memtables[i].Seek(lookupKey)
		if found {
			if resolved, value, exists := resolvePointLookup(ikey, val, key); resolved {
				return bytes.Clone(value), exists
			}
		}
	}

	tables := snapshot.tableVersion.version.PointLookupCandidates(key)
	for _, t := range tables {
		reader, err := snapshot.openTable(sst.Metadata(t))
		if err != nil {
			fmt.Printf("db: store error %v\n", err)
			return nil, false
		}

		ikey, value, found := reader.Lookup(key, lookupKey)
		reader.Close()
		if found {
			if resolved, value, exists := resolvePointLookup(ikey, value, key); resolved {
				return bytes.Clone(value), exists
			}
		}
	}

	return nil, false
}

func (db *QuxDB) Delete(key []byte) error {
	req := db.enqueueWrite(key, nil, quxOpDelete)
	res := db.awaitResult(req)
	return res.err
}

func (db *QuxDB) Iter(start, end []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		if start != nil && end != nil && bytes.Compare(start, end) > 0 {
			return
		}

		readSeq := db.committedSeq.Load()
		snapshot := db.lsm.acquireReadSnapshot()
		defer func() {
			db.lsm.reportTableCleanupError(snapshot.release())
		}()

		var startKey, endKey []byte
		if start != nil {
			startKey = newSeekStart(start, readSeq) // heap alloc
		}
		if end != nil {
			endKey = newSeekEndInclusive(end) // heap alloc
		}

		memtables := snapshot.memtables
		tables := snapshot.tableVersion.version.RangeLookupCandidates(start, end)

		if len(memtables) == 1 && len(tables) == 0 {
			cur := &mvccCursor{
				cur:     memtables[0].Cursor(startKey, endKey),
				readSeq: readSeq,
			}
			for {
				key, val, ok := cur.Next()
				if !ok {
					return
				}
				userKey, _, op := quxKey(key).Decode()
				if op != quxOpDelete && !yield(userKey, val) {
					return
				}
			}
		}

		cursors := make([]*mvccCursor, 0, len(memtables)+len(tables))
		for _, mt := range memtables {
			cursors = append(cursors, &mvccCursor{
				cur:     mt.Cursor(startKey, endKey),
				readSeq: readSeq,
			})
		}

		readers := make([]sst.Reader, 0, len(tables))
		defer func() {
			for i := range readers {
				db.lsm.reportTableCleanupError(readers[i].Close())
			}
		}()
		for _, table := range tables {
			reader, err := snapshot.openTable(sst.Metadata(table))
			if err != nil {
				fmt.Printf("db: store error %v\n", err)
				return
			}
			readers = append(readers, reader)
			cursors = append(cursors, &mvccCursor{
				cur:     reader.Cursor(startKey, endKey),
				readSeq: readSeq,
			})
		}

		var currentUserKey []byte
		resolved := false

		for qkey, val := range mergeIter(cursors) {
			userKey := qkey.UserKey()
			op := qkey.Op()

			if !bytes.Equal(userKey, currentUserKey) {
				currentUserKey = userKey
				resolved = false
			}

			if resolved {
				continue
			}
			resolved = true

			if op != quxOpDelete && !yield(userKey, val) {
				return
			}
		}
	}
}

func (db *QuxDB) enqueueWrite(key, val []byte, op quxOp) *writeReq {
	w := db.reqPool.Get().(*writeReq)
	w.kv.qkey = newQuxKey(key, 0, op) // heap alloc
	w.kv.val = val
	w.res = writeResult{}
	w.enqueuedAt = time.Now()

	db.reqChan <- w
	return w
}

func (db *QuxDB) awaitResult(req *writeReq) writeResult {
	<-req.done
	res := req.res

	req.kv = quxKV{}
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

	lens := make([]int, len(batch))

	commitStart := time.Now()
	for _, req := range batch {
		db.writeSeq++
		req.kv.qkey.SetSeq(db.writeSeq)
	}

	totalBytes := 0
	for i, req := range batch {
		n := req.kv.EncodedLen()
		lens[i] = n
		totalBytes += n
	}

	// serialize all records one big buffer
	batchBuff := bufpool.Get(uint(totalBytes))
	defer batchBuff.Release()

	// create batch from slices of the big buff
	bufOff := 0
	for i, req := range batch {
		n := lens[i]
		end := bufOff + n
		req.kv.Encode(batchBuff.B[bufOff:end])

		db.walBuf = append(db.walBuf, batchBuff.B[bufOff:end])
		bufOff = end
	}

	appendStart := time.Now()
	results, err := db.wal.AppendBatch(db.walBuf)

	syncStart := time.Now()
	if err == nil && fullSync {
		err = db.wal.Sync()
	}

	memStart := time.Now()
	if err == nil {
		lastSeq := db.committedSeq.Load()
		lastLSN := db.lastCommittedLSN
		for i, req := range batch {
			if results[i].Err != nil {
				continue
			}
			if err := db.setMemtable(req.kv, lastSeq, lastLSN); err != nil {
				panic(fmt.Sprintf("unknown error %v", err))
			}
			lastSeq = req.kv.qkey.Seq()
			lastLSN = results[i].LSN
			db.committedSeq.Store(lastSeq)
		}
		db.lastCommittedLSN = lastLSN
	}

	commitEnd := time.Now()
	commitDur := commitEnd.Sub(commitStart)

	for i, req := range batch {
		lsn := uint64(0)
		reqErr := err
		if reqErr == nil {
			reqErr = results[i].Err
		}
		if reqErr == nil {
			lsn = uint64(results[i].LSN)
		}

		// unblock waiters
		req.res = writeResult{
			lsn:       lsn,
			err:       reqErr,
			queueWait: commitStart.Sub(req.enqueuedAt),
			commitDur: commitDur,
			totalDur:  commitEnd.Sub(req.enqueuedAt),
		}

		metrics.DbSetQueueWaitDuration.Observe(req.res.queueWait.Seconds())
		metrics.DbSetTotalDuration.Observe(req.res.totalDur.Seconds())

		req.done <- struct{}{}
	}

	memDur := commitEnd.Sub(memStart)
	syncDur := memStart.Sub(syncStart)
	appendDur := syncStart.Sub(appendStart)
	encodeDur := appendStart.Sub(commitStart)

	metrics.DbCommitDuration.Observe(commitDur.Seconds())
	metrics.DbCommitEncodeDuration.Observe(encodeDur.Seconds())
	metrics.DbCommitAppendDuration.Observe(appendDur.Seconds())
	metrics.DbCommitSyncDuration.Observe(syncDur.Seconds())
	metrics.DbCommitMemSetDuration.Observe(memDur.Seconds())

	clear(db.walBuf)
	db.walBuf = db.walBuf[:0]
}

func (db *QuxDB) setMemtable(kv quxKV, lastSeq uint64, lastLSN wal.LSN) error {
retry:
	if err := db.lsm.activeMemtable().Set(kv.qkey, kv.val); err != nil {
		if errors.Is(err, memtable.ErrMemtableFull) {
			db.rolloverMemtable(lastSeq, lastLSN)
			goto retry
		}
		return err
	}
	return nil
}

func (db *QuxDB) rolloverMemtable(lastSeq uint64, lastLSN wal.LSN) {
	mt := db.lsm.rolloverMemtable(lastSeq, lastLSN)

	fmt.Printf("db: rollover memtable size=%.2fMB keys=%d lastSeq=%d lastLSN=%d\n",
		float64(mt.SizeBytes())/(1024*1024), mt.Len(), mt.lastSeq, mt.lastLSN)

	// Non-blocking notification; the flush worker determines whether enough
	// immutable memtables are available.
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

		mtsToFlush := db.lsm.flushableMemtables(imtFlushThreshold)
		if len(mtsToFlush) == 0 {
			continue
		}

		newSSTs := make([]*sst.Metadata, 0, len(mtsToFlush))
		for _, mt := range mtsToFlush {
			b, err := sst.NewBuilder(sst.BuilderOpts{
				Dir:       db.dataDir,
				Level:     0,
				Keys:      uint64(mt.Len()),
				SizeBytes: uint64(mt.SizeBytes()),
			})
			if err != nil {
				panic(err)
			}
			c := mt.Cursor(nil, nil)
			for {
				key, val, ok := c.Next()
				if !ok {
					break
				}

				qkey := quxKey(key)
				err := b.Add(sst.Record{
					OrderedKey: key,
					FilterKey:  qkey.UserKey(),
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
			fmt.Printf("db: new sst level=%d path=%s\n", sstMeta.Level, sstMeta.Path)
			newSSTs = append(newSSTs, sstMeta)
		}

		if err := db.lsm.replaceMemtablesWithSSTs(mtsToFlush, newSSTs); err != nil {
			panic(err)
		}
		fmt.Printf("db: memtable flush flushed=%d queued=%d\n",
			len(mtsToFlush), db.lsm.immutableMemtableCount())

		db.compactor.Notify()
	}
	fmt.Println("db: memtable flush exited")
}
