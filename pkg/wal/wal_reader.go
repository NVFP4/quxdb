package wal

import (
	"errors"
	"fmt"
	"io"
	"slices"
)

type walReader struct {
	segments *segmentSet
}

func newWalReader(segments *segmentSet) *walReader {
	return &walReader{segments: segments}
}

func (r *walReader) read(lsn LSN) (*walRecord, LSN, error) {
	seg, err := r.segments.segmentForLSN(lsn)
	if err != nil {
		return nil, 0, err
	}
	return seg.read(lsn)
}

func (r *walReader) replay(callback func(Record) error) (LSN, error) {
	return r.replayAfter(0, callback)
}

func (r *walReader) replayAfter(last LSN, callback func(Record) error) (LSN, error) {
	segments := r.segments.segments
	if len(segments) == 0 {
		if last == 0 {
			return 0, nil
		}
		_, _, err := r.read(last)
		return last, err
	}

	start := segments[0].startLSN
	if last != 0 {
		_, next, err := r.read(last)
		if err != nil {
			return last, err
		}
		start = next
	}

	startSegment, err := r.segments.segmentForLSN(start)
	if err != nil {
		return start, err
	}
	startOffset := lsnOffset(start, startSegment.segMaxSize)
	if startOffset < lsnOffset(startSegment.startLSN, startSegment.segMaxSize) || startOffset > startSegment.cursor {
		return start, fmt.Errorf("%w: lsn=%d", ErrSegmentLSNInvalid, start)
	}

	startIndex := slices.Index(segments, startSegment)
	if err := closeSegments(segments[:startIndex]); err != nil {
		return start, err
	}

	lsn := start
	for i, seg := range segments[startIndex:] {
		if i > 0 {
			lsn = seg.startLSN
		}
		var segErr error

		for {
			rec, next, err := seg.read(lsn)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				segErr = err
				break
			}
			if err := callback(Record{Data: rec.data, LSN: LSN(rec.lsn)}); err != nil {
				segErr = err
				break
			}
			lsn = next
		}

		if seg != r.segments.active {
			if err := seg.close(); err != nil {
				segErr = errors.Join(segErr, err)
			}
		}
		if segErr != nil {
			return lsn, segErr
		}
	}

	return lsn, nil
}
