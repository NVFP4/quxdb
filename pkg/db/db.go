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
)

type QuxDB struct {
	seq atomic.Uint64
	_   [56]byte // cache line padding

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

	var kv kvPair
	var keys uint64
	lsn, err := db.wal.Replay(func(r wal.Record) error {
		keys++
		kv.Decode(r.Data)
		return db.setMt(kv.qkey, kv.val, r.LSN)
	})
	if err != nil {
		fmt.Printf("replay: %v\ntruncating to lsn=%d\n", err, lsn)
		db.wal.TruncateFrom(lsn)
	}

	// restore last known sequence number
	if kv.qkey != nil {
		db.seq.Store(seqFromQuxKey(kv.qkey))
	}

	go db.writeLoop()

	fmt.Printf("db started with dir=%s memtable=%s keys=%d seq=%d\n", db.dataDir, memTableType, keys, db.seq.Load())

	return nil
}

func (db *QuxDB) Stop(ctx context.Context) error {
	defer fmt.Println("db stopped")
	return db.wal.Close()
}

func (db *QuxDB) Set(key []byte, value []byte) error {
	req := db.enqueueWrite(key, value, opSet)
	res := db.awaitResult(req)
	return res.err
}

func (db *QuxDB) Get(key []byte) ([]byte, bool) {
	readSeq := db.seq.Load()
	lookupKey := newQuxKey(key, readSeq, opMax)

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

func getFromMemtable(mt *quxMemtable, key, lookupKey []byte) (value []byte, found bool, ok bool) {
	internalKey, value, ok := mt.SeekGE(lookupKey)
	if !ok {
		return nil, false, false
	}

	userKey, _, op := decodeQuxKey(internalKey)
	if !bytes.Equal(userKey, key) {
		return nil, false, false
	}
	if op == opDelete {
		return nil, true, false
	}

	return value, true, true
}

func (db *QuxDB) Delete(key []byte) error {
	req := db.enqueueWrite(key, nil, opDelete)
	res := db.awaitResult(req)
	return res.err
}

func (db *QuxDB) All() iter.Seq2[[]byte, []byte] {
	return db.Range(nil, nil)
}

func (db *QuxDB) Range(start, end []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		readSeq := db.seq.Load()
		mt := db.amt.Load()

		var iter memtable.Iterator
		if start != nil {
			iter = mt.IterFrom(newQuxKey(start, readSeq, opMax))
		} else {
			iter = mt.Iter()
		}

		var lastUserKey []byte
		for internalKey, value := range iter {
			userKey, seq, op := decodeQuxKey(internalKey)
			if end != nil && bytes.Compare(userKey, end) > 0 {
				return
			}
			if lastUserKey != nil && bytes.Equal(userKey, lastUserKey) {
				continue
			}
			if seq > readSeq {
				continue
			}

			lastUserKey = bytes.Clone(userKey)
			if op == opDelete {
				continue
			}
			if !yield(userKey, value) {
				return
			}
		}
	}
}

func (db *QuxDB) enqueueWrite(key, val []byte, op dbOP) *writeReq {
	w := db.reqPool.Get().(*writeReq)
	w.qkey = newQuxKey(key, db.seq.Add(1), op)
	w.val = val
	w.res = writeResult{}
	w.enqueuedAt = time.Now()

	db.reqChan <- w
	return w
}

func (db *QuxDB) awaitResult(req *writeReq) writeResult {
	<-req.done
	res := req.res

	req.qkey = nil
	req.val = nil
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

	commitStart := time.Now()

	totalBytes := 0
	for _, req := range batch {
		totalBytes += req.EncodedLen()
	}

	// serialize all records one big buffer
	batchBuff := bufpool.Get(uint(totalBytes))
	defer bufpool.Put(batchBuff)

	// create batch from slices of the big buff
	bufOff := 0
	for _, req := range batch {
		n := req.EncodedLen()
		end := bufOff + n
		req.Encode(batchBuff[bufOff:end])

		db.walBuf = append(db.walBuf, batchBuff[bufOff:end])
		bufOff = end
	}

	appendStart := time.Now()
	lsns, err := db.wal.AppendBatch(db.walBuf)

	syncStart := time.Now()
	if err == nil {
		err = db.wal.Sync()
	} else {
		fmt.Printf("batch got err: %v\n", err)
	}

	memStart := time.Now()
	if err == nil {
		for i, req := range batch {
			if err = db.setMt(req.qkey, req.val, lsns[i]); err != nil {
				panic(fmt.Sprintf("unknown error %v", err))
			}
		}
	}

	commitEnd := time.Now()
	commitDur := commitEnd.Sub(commitStart)

	for i, req := range batch {
		lsn := uint64(0)
		if err == nil {
			lsn = uint64(lsns[i])
		}

		// unblock waiters
		req.res = writeResult{
			lsn:       lsn,
			err:       err,
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

func (db *QuxDB) setMt(qkey quxKey, val []byte, lsn wal.LSN) error {
retry:
	err := db.amt.Load().Set(qkey, val)
	if err != nil {
		if errors.Is(err, memtable.ErrMemtableFull) {
			db.rolloverMemtable(seqFromQuxKey(qkey), lsn)
			goto retry
		}
		return err
	}
	return nil
}

func (db *QuxDB) rolloverMemtable(lastSeq uint64, lastLSN wal.LSN) {
	amt := db.amt.Load()
	// set seq/lsn info to active mem table
	amt.lastLSN = lastLSN
	amt.lastSeq = lastSeq
	// append active to imt
	db.imtMu.Lock()
	db.imt = append(db.imt, amt)
	db.imtMu.Unlock()

	db.amt.Store(newQuxMemtable())
	fmt.Printf("froze memtable keys=%d size=%.2fMB lastSeq=%d lastLSN=%d\n", amt.Len(), float64(amt.SizeBytes())/(1024*1024), amt.lastSeq, amt.lastLSN)
}

type quxMemtable struct {
	memtable.Memtable
	lastLSN wal.LSN
	lastSeq uint64
}

func newQuxMemtable() *quxMemtable {
	return &quxMemtable{
		Memtable: memtable.New(memTableType, memtable.WithComparator(compareInternalKey)),
	}
}
