package wal

import (
	"errors"
	"io"
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
	var lsn LSN

	for _, seg := range r.segments.segments {
		lsn = seg.startLSN
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
