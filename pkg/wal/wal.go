package wal

import (
	"fmt"
	"sync"
	"time"
)

const (
	walRecordFlags = 0
)

type Record struct {
	LSN  LSN
	Data []byte
}

type WAL struct {
	segments *segmentSet
	writer   *walWriter
	reader   *walReader
	mu       sync.RWMutex
}

func New(walDir string) (*WAL, error) {
	segments := newSegmentSet(walDir)
	return &WAL{
		segments: segments,
		writer:   newWalWriter(segments),
		reader:   newWalReader(segments),
	}, nil
}

func (w *WAL) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.segments.open(); err != nil {
		return err
	}

	for _, seg := range w.segments.segments {
		fmt.Printf("wal segment id=%d size=%dMB cursor=0x%x startOffset=%d flags=%d createdAt='%s'\n",
			seg.sid,
			seg.maxSize/(1024*1024),
			seg.cursor,
			lsnOffset(seg.startLSN),
			seg.flags,
			seg.created.Format(time.RFC3339),
		)
	}

	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.segments.close()
}

func (w *WAL) Read(lsn LSN) ([]byte, LSN, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	rec, next, err := w.reader.read(lsn)
	if err != nil {
		return nil, 0, err
	}
	return rec.data, next, nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.sync()
}

func (w *WAL) Append(data []byte) (lsn LSN, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.append(data, walRecordFlags)
}

func (w *WAL) AppendBatch(batch [][]byte) (lsns []LSN, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.appendBatch(batch, walRecordFlags)
}

func (w *WAL) TruncateFrom(lsn LSN) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.truncate(lsn)
}

func (w *WAL) Replay(fn func(Record) error) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.reader.replay(fn)
}
