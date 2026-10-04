package wal

import (
	"errors"
	"fmt"
	"time"
)

// Record is one payload written by Append and returned by Recover.
type Record struct {
	LSN LSN
	// valid until the callback returns
	Data []byte
}

// WAL is an append-only log of Records, used from one goroutine except RetainFrom and Prune.
type WAL struct {
	segments *segmentSet
	writer   *walWriter
	reader   *walReader
}

// New returns a WAL stored in walDir, call Recover before using it.
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

// Recover passes every Record after `after` to fn, then readies the WAL for Append.
func (w *WAL) Recover(after LSN, fn func(Record) error, policy CorruptionPolicy) error {
	if err := w.segments.open(); err != nil {
		return err
	}

	for _, seg := range w.segments.segments {
		fmt.Printf("wal: segment id=%d mode=%d size=%dMiB startLSN=%d createdAt='%s'\n",
			seg.segId,
			seg.mode,
			seg.segMaxSize>>20,
			seg.startLSN,
			seg.createdAt.Format(time.RFC3339),
		)
	}

	end, err := w.reader.replayAfter(after, fn)
	switch {
	case err == nil:
		w.segments.active.cursor = lsnOffset(end, w.segments.segSize)
	case !isCorruptionError(err):
		return err
	case after != 0 && end == after:
		// cutting here would let the next Record take after's lsn
		return &CorruptionError{LSN: after, Err: err}
	default:
		torn, scanErr := w.segments.tornTail(end)
		if scanErr != nil {
			return errors.Join(err, scanErr)
		}
		switch {
		case torn:
			fmt.Printf("wal: cutting torn tail at lsn=%d: %v\n", end, err)
		case policy == StopOnCorruption:
			return &CorruptionError{LSN: end, Err: err}
		default:
			fmt.Printf("wal: mid-log corruption at lsn=%d, dropping every record after it: %v\n", end, err)
		}
		if err := w.segments.truncateTail(end); err != nil {
			return err
		}
	}

	w.writer.start()
	w.segments.startPreparer()
	return nil
}

// Append writes parts as one Record and returns its LSN once the Record reaches d.
func (w *WAL) Append(parts [][]byte, d Durability) (LSN, error) {
	return w.writer.append(parts, d)
}

// RetainFrom marks Records before lsn as no longer needed, Recover must not ask for them again.
func (w *WAL) RetainFrom(lsn LSN) {
	w.segments.retainFrom.Store(uint64(lsn))
}

// Prune frees disk space held by Records no longer needed.
func (w *WAL) Prune() error {
	err := w.segments.prune()
	// a freed segment can become the next one
	w.segments.notifyPreparer()
	return err
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.writer.stop()
	w.segments.stopPreparer()
	return w.segments.close()
}
