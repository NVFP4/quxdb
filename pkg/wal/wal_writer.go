package wal

import (
	"errors"
)

var (
	errSegmentInsufficientSpace = errors.New("wal: segment insufficient space")
	ErrRecordTooLarge           = errors.New("wal: record too large for segment")
)

type walWriter struct {
	segments *segmentSet
}

func newWalWriter(segments *segmentSet) *walWriter {
	return &walWriter{segments: segments}
}

func (w *walWriter) append(data []byte, flags uint16) (lsn LSN, err error) {
retry:
	lsn, err = w.segments.active.append(data, flags)
	if err == errSegmentInsufficientSpace {
		if err = w.segments.rollover(); err != nil {
			return 0, err
		}
		goto retry
	}
	return lsn, err
}

func (w *walWriter) appendBatch(batch [][]byte, flags uint16) ([]AppendResult, error) {
	if len(batch) == 0 {
		return nil, nil
	}

retry:
	results, err := w.segments.active.appendBatch(batch, flags)
	if err == errSegmentInsufficientSpace {
		if err = w.segments.rollover(); err != nil {
			return nil, err
		}
		goto retry
	}
	return results, err
}

func (w *walWriter) sync() error {
	return w.segments.active.sync()
}

func (w *walWriter) truncate(lsn LSN) error {
	return w.segments.truncateTail(lsn)
}
