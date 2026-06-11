package db

import (
	"context"
	"fmt"
	"iter"
	"path/filepath"
	"sync"
	"time"

	"github.com/yashgorana/quxdb/pkg/bufpool"
	"github.com/yashgorana/quxdb/pkg/memtable"
	"github.com/yashgorana/quxdb/pkg/metrics"
	"github.com/yashgorana/quxdb/pkg/pathlib"
	"github.com/yashgorana/quxdb/pkg/wal"
)

const (
	memTableType = memtable.Map
	maxBatch     = 128
)

type QuxDB struct {
	dataDir string

	mt  memtable.MemTable // active mem table
	wal *wal.WAL

	reqPool sync.Pool
	reqChan chan *writeReq
	walBuf  [][]byte
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
		mt:      memtable.New(memTableType),
		wal:     wal,
		reqChan: make(chan *writeReq, 4096),
		walBuf:  make([][]byte, 0, maxBatch),
	}

	db.reqPool.New = func() any {
		return &writeReq{
			done: make(chan struct{}, 1),
		}
	}

	return db, nil
}

func (db *QuxDB) Start(ctx context.Context) error {
	fmt.Printf("db started with dir=%s memtable=%s\n", db.dataDir, memTableType)

	if err := db.wal.Open(); err != nil {
		return err
	}

	lsn, err := db.wal.Replay(func(r wal.Record) error {
		var item kvItem
		item.Decode(r.Data)
		return db.doMemtableOp(item.op, item.key, item.val)
	})
	if err != nil {
		fmt.Printf("replay: %v\ntruncating to lsn=%d\n", err, lsn)
		db.wal.TruncateFrom(lsn)
	}

	fmt.Printf("memtable: keys=%d\n", db.mt.Len())

	go db.writeLoop()

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
	return db.mt.Get(key)
}

func (db *QuxDB) Delete(key []byte) error {
	req := db.enqueueWrite(key, nil, opDelete)
	res := db.awaitResult(req)
	return res.err
}

func (db *QuxDB) All() iter.Seq2[[]byte, []byte] {
	return db.mt.All()
}

func (db *QuxDB) Range(start, end []byte) iter.Seq2[[]byte, []byte] {
	return db.mt.Range(start, end)
}

func (db *QuxDB) enqueueWrite(key, val []byte, op uint8) *writeReq {
	w := db.reqPool.Get().(*writeReq)
	w.key = key
	w.val = val
	w.op = op
	w.res = writeResult{}
	w.enqueuedAt = time.Now()

	db.reqChan <- w
	return w
}

func (db *QuxDB) awaitResult(req *writeReq) writeResult {
	<-req.done
	res := req.res

	req.key = nil
	req.val = nil
	req.op = 0
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
		for _, req := range batch {
			if err = db.doMemtableOp(req.op, req.key, req.val); err != nil {
				fmt.Println("FATAL: Memtable update failed after WAL sync")
				panic(err)
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

func (db *QuxDB) doMemtableOp(op uint8, key, val []byte) error {
	switch op {
	case opSet:
		return db.mt.Set(key, val)
	case opDelete:
		return db.mt.Delete(key)
	default:
		return fmt.Errorf("the fuck is this op")
	}
}
