package wal

import (
	"errors"
	"fmt"
	"io"

	"github.com/yashgorana/quxdb/pkg/bufpool"
	"github.com/yashgorana/quxdb/pkg/fs"
)

var (
	errSegmentInsufficientSpace = errors.New("wal: segment insufficient space")
)

type walWriter struct{}

func (w *walWriter) append(s *walSegment, data []byte, flags uint16) (LSN, error) {
	lsn := newLSN(s.sid, s.cursor)

	totalBytes := encodedRecordLen(data)
	buf := bufpool.Get(uint(totalBytes))
	defer bufpool.Put(buf)

	_, err := encodeRecord(buf, lsn, flags, data)
	if err != nil {
		return 0, err
	}

	n, err := s.file.WriteAt(buf, s.cursor)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrRecordWrite, err)
	}
	if n != totalBytes {
		return 0, fmt.Errorf("%w: %w", ErrRecordWrite, io.ErrShortWrite)
	}

	s.cursor += int64(totalBytes)
	return lsn, nil
}

func (w *walWriter) appendBatch(s *walSegment, batch [][]byte, flags uint16) ([]LSN, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("wal: no records to append")
	}
	totalBytes := encodedBatchLen(batch)
	recOffset := s.cursor
	bufOffset := 0

	if recOffset+int64(totalBytes) > s.maxSize {
		return nil, errSegmentInsufficientSpace
	}

	lsns := make([]LSN, len(batch))
	buf := bufpool.Get(uint(totalBytes)) // allocate a big buffer pool for the whole batch
	defer bufpool.Put(buf)

	for i, data := range batch {
		lsn := newLSN(s.sid, recOffset)
		lsns[i] = lsn
		n, err := encodeRecord(buf[bufOffset:], lsn, flags, data)
		if err != nil {
			return nil, fmt.Errorf("%w: record encode %w", ErrRecordWrite, err)
		}
		bufOffset += n
		recOffset += int64(n)
	}

	n, err := s.file.WriteAt(buf, s.cursor)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRecordWrite, err)
	}
	if n != totalBytes {
		return nil, fmt.Errorf("%w: %w", ErrRecordWrite, io.ErrUnexpectedEOF)
	}

	s.cursor = recOffset

	return lsns, nil
}

func (w *walWriter) sync(s *walSegment) error {
	return fs.Fdatasync(s.file)
}

func (w *walWriter) truncate(s *walSegment, lsn LSN) error {
	offset := lsnOffset(lsn)
	if err := s.file.Truncate(offset); err != nil {
		return err
	}
	if err := fs.Fdatasync(s.file); err != nil {
		return err
	}
	s.cursor = offset
	return nil
}
