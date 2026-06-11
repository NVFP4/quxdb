package wal

import (
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sync"
	"time"
)

type Record struct {
	LSN  LSN
	Data []byte
}

// type iWAL interface {
// 	Open() error
// 	Close() error

// 	Append(data []byte) (LSN, error)
// 	AppendBatch(data [][]byte) ([]LSN, error)

// 	Read(at LSN) (owned []byte, next LSN, err error)
// 	Replay() iter.Seq2[Record, error]

// 	TruncateAfter(lsn LSN) error
// }

type WAL struct {
	activeSegment *walSegment
	segments      []*walSegment
	writer        *walWriter
	path          string
	segId         segID
	mu            sync.RWMutex
}

func New(walDir string) (*WAL, error) {
	return &WAL{
		path:     walDir,
		writer:   &walWriter{},
		segments: make([]*walSegment, 0),
		segId:    math.MaxUint32,
	}, nil
}

func (w *WAL) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.initSegmentsLocked(w.path); err != nil {
		return err
	}

	for _, seg := range w.segments {
		fmt.Printf("wal segment id=%d size=%dMB cursor=0x%x startOffset=%d flags=%d createdAt='%s'\n",
			seg.sid,
			seg.maxSize/(1024*1024),
			seg.cursor,
			lsnOffset(seg.startLSN),
			seg.flags,
			seg.ts.Format(time.RFC3339),
		)
	}

	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var errs []error

	for i := range w.segments {
		errs = append(errs, w.segments[i].close())
	}

	clear(w.segments)
	w.segments = w.segments[:0]
	w.activeSegment = nil

	return errors.Join(errs...)
}

func (w *WAL) Read(lsn LSN) ([]byte, LSN, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	sid := lsnSegID(lsn)

	lastIdx := len(w.segments) - 1
	lastSid := w.segments[lastIdx].sid

	if sid > lastSid {
		return nil, 0, fmt.Errorf("unknown LSN")
	}

	seg := w.segments[sid]

	rec, next, err := seg.read(lsn)
	if err != nil {
		return nil, 0, err
	}
	return rec.data, next, nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.sync(w.activeSegment)
}

func (w *WAL) Append(data []byte) (lsn LSN, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

append:
	lsn, err = w.writer.append(w.activeSegment, data, 0x00)
	if err != nil {
		if errors.Is(err, errSegmentInsufficientSpace) {
			err = w.rolloverLocked()
			if err == nil {
				// rollover was a success, retry
				goto append
			}
		}
		return
	}

	return
}

func (w *WAL) AppendBatch(batch [][]byte) (lsns []LSN, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

append:
	lsns, err = w.writer.appendBatch(w.activeSegment, batch, 0x00)
	if err != nil {
		if errors.Is(err, errSegmentInsufficientSpace) {
			err = w.rolloverLocked()
			if err == nil {
				// rollover was a success, retry
				goto append
			}
		}
		return
	}

	return
}

func (w *WAL) TruncateFrom(lsn LSN) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.truncate(w.activeSegment, lsn)
}

func (w *WAL) Replay(fn func(Record) error) (LSN, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var lsn LSN

	for _, seg := range w.segments {
		lsn = seg.startLSN
		for {
			rec, next, err := seg.read(lsn)
			if err != nil {
				if errors.Is(err, io.EOF) {
					// this segment is over, continue to next one
					break
				}
				return lsn, err
			}
			if err := fn(Record{Data: rec.data, LSN: LSN(rec.lsn)}); err != nil {
				return lsn, err
			}
			lsn = next
		}
	}

	return lsn, nil
}

func (w *WAL) initSegmentsLocked(dir string) error {
	wSeg, err := scanSegments(dir)
	if err != nil {
		return fmt.Errorf("wal open: %w", err)
	}

	if len(wSeg) == 0 {
		return w.rolloverLocked()
	}

	lastIdx := len(wSeg) - 1
	w.segments = append(w.segments, wSeg...)
	w.segId = w.segments[lastIdx].sid
	w.activeSegment = w.segments[lastIdx]

	return nil
}

func (w *WAL) rolloverLocked() error {
	// caller must write-lock mutex
	w.segId++

	seg, err := newSegment(w.path, w.segId)
	if err != nil {
		return err
	}

	w.segments = append(w.segments, seg)
	w.activeSegment = seg

	return nil
}

func scanSegments(dir string) ([]*walSegment, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.quxwal"))
	if err != nil {
		return nil, fmt.Errorf("wal: discover segments %w", err)
	}
	segments := make([]*walSegment, 0, len(files))

	for _, file := range files {
		seg, err := openSegment(file)
		if err != nil {
			return nil, err
		}
		segments = append(segments, seg)
	}

	return segments, nil
}
