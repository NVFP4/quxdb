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
	LSN LSN
	// Data is valid until the next WAL operation, so clone it to retain it.
	Data []byte
}

type WAL struct {
	segments *segmentSet
	writer   *walWriter
	reader   *walReader
	mu       sync.RWMutex
}

func New(walDir string, opts ...Option) (*WAL, error) {
	options, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}
	segments := newSegmentSet(walDir, options.segmentSize)
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
		fmt.Printf("wal: segment id=%d mode=%d size=%dMiB startLSN=%d cursor=0x%x createdAt='%s'\n",
			seg.segId,
			seg.mode,
			seg.segMaxSize>>20,
			seg.startLSN,
			seg.cursor,
			seg.createdAt.Format(time.RFC3339),
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

// Append requires Open and recovery to complete first.
func (w *WAL) Append(data []byte) (lsn LSN, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.append(data, walRecordFlags)
}

// AppendBatch has the same recovery precondition as Append.
func (w *WAL) AppendBatch(batch [][]byte) ([]AppendResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.appendBatch(batch, walRecordFlags)
}

func (w *WAL) TruncateFrom(lsn LSN) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.truncate(lsn)
}

// PruneBefore deletes sealed segments preceding lsn's segment. Zero is a no-op.
func (w *WAL) PruneBefore(lsn LSN) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.segments.pruneBefore(lsn)
}

// Replay replays all records in WAL order.
func (w *WAL) Replay(callback func(Record) error) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.reader.replay(callback)
}

// ReplayAfter replays records following lsn. Zero starts at the oldest record.
func (w *WAL) ReplayAfter(lsn LSN, callback func(Record) error) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.reader.replayAfter(lsn, callback)
}
