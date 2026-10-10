package wal

import (
	"errors"
	"fmt"
	"io"
	"slices"
)

type walReader struct {
	segments  *segmentSet
	readAhead int
	buf       []byte // reassembles Entries from partial walRecords during replay
}

func newWalReader(segments *segmentSet, readAhead int) *walReader {
	return &walReader{segments: segments, readAhead: readAhead}
}

// read returns the Entry at lsn and the lsn after it, partial walRecords are reassembled into buf.
func (r *walReader) read(lsn LSN, buf []byte) (data, asm []byte, next LSN, err error) {
	seg, err := r.segments.segmentForLSN(lsn)
	if err != nil {
		return nil, buf, 0, err
	}
	rec, next, err := seg.read(lsn, r.readAhead)
	if err != nil {
		return nil, buf, 0, err
	}
	if !rec.starts() {
		return nil, buf, 0, fmt.Errorf("%w: lsn=%d is not the start of a record", ErrRecordInvalidLSN, lsn)
	}
	if rec.ends() {
		return rec.data, buf, next, nil
	}

	buf = append(buf[:0], rec.data...)
	for {
		// the next partial walRecord opens the following segment
		seg = r.segments.byID[seg.segId+1]
		if seg == nil {
			return nil, buf, 0, fmt.Errorf("%w: lsn=%d is missing its next partial record", ErrRecordTorn, lsn)
		}
		rec, next, err = seg.read(seg.startLSN, r.readAhead)
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("%w: lsn=%d is missing its next partial record", ErrRecordTorn, lsn)
		}
		if err != nil {
			return nil, buf, 0, err
		}
		if rec.starts() {
			return nil, buf, 0, fmt.Errorf("%w: lsn=%d has a record start where its next partial record should be", ErrRecordInvalidFormat, lsn)
		}
		buf = append(buf, rec.data...)
		if rec.ends() {
			return buf, buf, next, nil
		}
	}
}

// replayAfter feeds callback each Entry after last and returns the log end or the failing Entry's lsn.
func (r *walReader) replayAfter(last LSN, callback func(Entry) error) (LSN, error) {
	segments := r.segments.segments
	defer r.segments.active.dropWindow()

	lsn := segments[0].startLSN
	if last != 0 {
		_, asm, next, err := r.read(last, r.buf)
		r.buf = asm
		if err != nil {
			return last, err
		}
		lsn = next
	}

	seg, err := r.segments.segmentForLSN(lsn)
	if err != nil {
		return lsn, err
	}
	startOffset := lsnOffset(lsn, seg.segMaxSize)
	if startOffset < lsnOffset(seg.startLSN, seg.segMaxSize) || startOffset > seg.cursor {
		return lsn, fmt.Errorf("%w: lsn=%d", ErrSegmentLSNInvalid, lsn)
	}

	i := slices.Index(segments, seg)
	if err := closeSegments(segments[:i]); err != nil {
		return lsn, err
	}
	r.segments.log.Debug("replay start", "segment", seg.segId, "offset", startOffset, "lsn", lsn)

	// the oldest segment may open with the tail of a pruned Entry
	orphan := last == 0
	var skipped int
	for {
		if orphan {
			rec, next, err := seg.read(lsn, r.readAhead)
			if err == nil && !rec.starts() {
				lsn = next
				skipped++
				continue
			}
			orphan = errors.Is(err, io.EOF)
			if skipped > 0 {
				r.segments.log.Debug("skipped pruned entry tail", "segment", seg.segId, "walRecords", skipped)
				skipped = 0
			}
		}

		data, asm, next, err := r.read(lsn, r.buf)
		r.buf = asm
		if errors.Is(err, io.EOF) {
			if i == len(segments)-1 {
				return lsn, nil
			}
			if err := r.leave(seg); err != nil {
				return lsn, err
			}
			i++
			seg, lsn = segments[i], segments[i].startLSN
			continue
		}
		if err != nil {
			return lsn, err
		}
		if err := callback(Entry{Data: data, LSN: lsn}); err != nil {
			return lsn, err
		}

		lsn = next
		for seg.segId != lsnSegID(lsn, seg.segMaxSize) {
			if err := r.leave(seg); err != nil {
				return lsn, err
			}
			i++
			seg = segments[i]
		}
	}
}

// leave closes a replayed segment unless it is still being appended to.
func (r *walReader) leave(seg *walSegment) error {
	if seg == r.segments.active {
		return nil
	}
	return seg.close()
}
