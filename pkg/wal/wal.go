package wal

import (
	"errors"
	"log/slog"
)

// Entry is one payload written by Append and returned by Recover.
type Entry struct {
	LSN LSN
	// valid until the callback returns
	Data []byte
}

// WAL is an append-only log of Entries, used from one goroutine except RetainFrom and Prune.
type WAL struct {
	segments *segmentSet
	writer   *walWriter
	reader   *walReader
	log      *slog.Logger
}

// New returns a WAL stored in walDir with DefaultOptions changed by opts, call Recover before using it.
func New(walDir string, opts ...Option) (*WAL, error) {
	o := DefaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return NewWithOptions(walDir, o)
}

// NewWithOptions returns a WAL stored in walDir configured by o, call Recover before using it.
func NewWithOptions(walDir string, o Options) (*WAL, error) {
	if err := validateSegmentSize(o.SegmentBytes); err != nil {
		return nil, err
	}
	segments := newSegmentSet(walDir, o.SegmentBytes, o.Logger)
	return &WAL{
		segments: segments,
		writer:   newWalWriter(segments),
		reader:   newWalReader(segments, o.ReadAheadBytes),
		log:      o.Logger,
	}, nil
}

// Recover passes every Entry after `after` to fn, then readies the WAL for Append.
func (w *WAL) Recover(after LSN, fn func(Entry) error, policy CorruptionPolicy) error {
	if err := w.segments.open(); err != nil {
		return err
	}

	for _, seg := range w.segments.segments {
		w.log.Debug("segment found", "segment", seg.segId, "mode", seg.mode, "size", seg.segMaxSize,
			"startLSN", seg.startLSN, "createdAt", seg.createdAt)
	}

	end, err := w.reader.replayAfter(after, fn)
	switch {
	case err == nil:
		w.segments.active.cursor = lsnOffset(end, w.segments.segSize)
	case !isCorruptionError(err):
		return err
	case after != 0 && end == after:
		// cutting here would let the next Entry take after's lsn
		return &CorruptionError{LSN: after, Err: err}
	default:
		torn, scanErr := w.segments.tornTail(end)
		if scanErr != nil {
			return errors.Join(err, scanErr)
		}
		w.log.Debug("torn tail scan", "lsn", end, "torn", torn, "err", err)
		switch {
		case torn:
			w.log.Warn("cutting torn tail", "lsn", end, "err", err)
		case policy == StopOnCorruption:
			return &CorruptionError{LSN: end, Err: err}
		default:
			w.log.Warn("mid-log corruption, dropping every entry after it", "lsn", end, "err", err)
		}
		if err := w.segments.truncateTail(end); err != nil {
			return err
		}
	}

	w.writer.start()
	w.segments.startPreparer()
	return nil
}

// Append writes parts as one Entry and returns its LSN once the Entry reaches d.
func (w *WAL) Append(parts [][]byte, d Durability) (LSN, error) {
	return w.writer.append(parts, d)
}

// RetainFrom marks Entries before lsn as no longer needed, Recover must not ask for them again.
func (w *WAL) RetainFrom(lsn LSN) {
	if old := w.segments.retainFrom.Swap(uint64(lsn)); old != uint64(lsn) {
		w.log.Debug("retain point advanced", "from", LSN(old), "to", lsn)
	}
}

// Prune frees disk space held by Entries no longer needed.
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
