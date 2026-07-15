package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yashgorana/quxdb/pkg/bufpool"
	"github.com/yashgorana/quxdb/pkg/memtable"
	"github.com/yashgorana/quxdb/pkg/metrics"
	"github.com/yashgorana/quxdb/pkg/pathlib"
	"github.com/yashgorana/quxdb/pkg/wal"
)

const (
	memTableType = memtable.BTree
	maxBatch     = 128
	fullSync     = false
)

type QuxDB struct {
	committedSeq atomic.Uint64
	_            [56]byte // cache line padding

	// writeSeq is the last assigned sequence and is owned by writeLoop.
	writeSeq quxSeq

	dataDir string

	reqPool sync.Pool
	reqChan chan *writeReq

	wal    *wal.WAL
	walBuf [][]byte

	amt   atomic.Pointer[quxMemtable] // active mem table
	imt   []*quxMemtable
	imtMu sync.RWMutex
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

	db := &QuxDB{
		dataDir: dataDir,
		wal:     wal,
		imt:     make([]*quxMemtable, 0, 4),
		reqChan: make(chan *writeReq, 4096),
		walBuf:  make([][]byte, 0, maxBatch),
	}

	db.amt.Store(newQuxMemtable())

	db.reqPool.New = func() any {
		return newWriteReq()
	}

	return db, nil
}

func (db *QuxDB) Start(ctx context.Context) error {
	if err := db.wal.Open(); err != nil {
		return err
	}

	fmt.Printf("db replaying wal...\n")

	var kv quxKV
	var keys uint64
	lsn, err := db.wal.Replay(func(r wal.Record) error {
		keys++
		kv.Decode(r.Data)
		return db.setMemtable(kv, r.LSN)
	})
	if err != nil {
		fmt.Printf("replay: %v\ntruncating to lsn=%d\n", err, lsn)
		db.wal.TruncateFrom(lsn)
	}

	if kv.qkey != nil {
		db.committedSeq.Store(kv.qkey.Seq())
	}
	db.writeSeq = quxSeq(db.committedSeq.Load())

	go db.writeLoop()

	fmt.Printf("db started with dir=%s memtable=%s keys=%d seq=%d\n", db.dataDir, memTableType, keys, db.committedSeq.Load())

	return nil
}

func (db *QuxDB) Stop(ctx context.Context) error {
	defer fmt.Println("db stopped")
	return db.wal.Close()
}

func (db *QuxDB) Set(key []byte, value []byte) error {
	req := db.enqueueWrite(key, value, quxOpSet)
	res := db.awaitResult(req)
	return res.err
}

func (db *QuxDB) Get(key []byte) ([]byte, bool) {
	readSeq := db.committedSeq.Load()
	lookupKey := newSeekStart(key, readSeq)

	if value, found, ok := getFromMemtable(db.amt.Load(), key, lookupKey); found {
		return value, ok
	}

	db.imtMu.RLock()
	imt := db.imt
	db.imtMu.RUnlock()

	for i := len(imt) - 1; i >= 0; i-- {
		if value, found, ok := getFromMemtable(imt[i], key, lookupKey); found {
			return value, ok
		}
	}

	return nil, false
}

func getFromMemtable(mt *quxMemtable, key []byte, lookupKey quxKey) (value []byte, found bool, ok bool) {
	internalKey, value, ok := mt.Seek(lookupKey)
	if !ok {
		return nil, false, false
	}

	userKey, _, op := quxKey(internalKey).Decode()
	if !bytes.Equal(userKey, key) {
		return nil, false, false
	}
	if op == quxOpDelete {
		return nil, true, false
	}

	return value, true, true
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

		var startKey, endKey []byte
		if start != nil {
			startKey = newSeekStart(start, readSeq)
		}
		if end != nil {
			endKey = newSeekEnd(end)
		}

		db.imtMu.RLock()
		memtables := make([]*quxMemtable, 0, len(db.imt)+1)
		memtables = append(memtables, db.amt.Load())
		memtables = append(memtables, db.imt...)
		db.imtMu.RUnlock()

		// Fast path: single memtable, no merge heap needed.
		if len(memtables) == 1 {
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

		// General path: k-way merge across active + frozen memtables.
		cursors := make([]*mvccCursor, len(memtables))
		for i, mt := range memtables {
			cursors[i] = &mvccCursor{
				cur:     mt.Cursor(startKey, endKey),
				readSeq: readSeq,
			}
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
	w.kv.qkey = newQuxKey(key, 0, op)
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
	batch := make([]*writeReq, 0, maxBatch)

	for {
		// block for first req
		req, ok := <-db.reqChan
		if !ok {
			return
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
	defer bufpool.Put(batchBuff)

	// create batch from slices of the big buff
	bufOff := 0
	for i, req := range batch {
		n := lens[i]
		end := bufOff + n
		req.kv.Encode(batchBuff[bufOff:end])

		db.walBuf = append(db.walBuf, batchBuff[bufOff:end])
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
		for i, req := range batch {
			if results[i].Err != nil {
				continue
			}
			if err := db.setMemtable(req.kv, results[i].LSN); err != nil {
				panic(fmt.Sprintf("unknown error %v", err))
			}
			db.committedSeq.Store(req.kv.qkey.Seq())
		}
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

func (db *QuxDB) setMemtable(kv quxKV, lsn wal.LSN) error {
retry:
	err := db.amt.Load().Set(kv.qkey, kv.val)
	if err != nil {
		if errors.Is(err, memtable.ErrMemtableFull) {
			db.rolloverMemtable(kv.qkey.Seq(), lsn)
			goto retry
		}
		return err
	}
	return nil
}

func (db *QuxDB) rolloverMemtable(lastSeq uint64, lastLSN wal.LSN) {
	next := newQuxMemtable()

	db.imtMu.Lock()
	amt := db.amt.Load()
	// set seq/lsn info to active mem table
	amt.lastLSN = lastLSN
	amt.lastSeq = lastSeq
	// append active to imt
	db.imt = append(db.imt, amt)
	db.amt.Store(next)
	db.imtMu.Unlock()

	fmt.Printf("froze memtable size=%.2fMB keys=%d lastSeq=%d lastLSN=%d\n", float64(amt.SizeBytes())/(1024*1024), amt.Len(), amt.lastSeq, amt.lastLSN)
}

type quxMemtable struct {
	memtable.Memtable
	lastLSN wal.LSN
	lastSeq uint64
}

func newQuxMemtable() *quxMemtable {
	return &quxMemtable{
		Memtable: memtable.New(memTableType),
	}
}
