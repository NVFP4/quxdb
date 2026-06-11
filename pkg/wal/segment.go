package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yashgorana/quxdb/pkg/fs"
	"github.com/yashgorana/quxdb/pkg/pathlib"
)

const walMaxSegmentSize = 1 << 26

type walSegment struct {
	// current offset in the wal segment
	cursor int64
	_      [60]byte // cache line padding

	path     string
	file     *os.File
	maxSize  int64
	startLSN LSN
	ts       time.Time
	flags    uint32
	sid      segID
}

func segmentName(id segID) string {
	return fmt.Sprintf("%08d.quxwal", id)
}

func newSegment(dir string, id segID) (*walSegment, error) {
	segPath := filepath.Join(dir, segmentName(id))
	if !pathlib.FileEmpty(segPath) {
		return nil, fmt.Errorf("wal: segment already exists for id=%d", id)
	}

	file, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}

	if err := fs.Fallocate(file, 0, walMaxSegmentSize); err != nil {
		return nil, err
	}

	h, offset, err := writeHeader(file, id, walMaxSegmentSize, 0x00)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}

	sid := segID(h.sid)
	maxSize := int64(h.size)

	seg := &walSegment{
		cursor:   offset,
		file:     file,
		flags:    h.flags,
		maxSize:  maxSize,
		path:     segPath,
		sid:      sid,
		startLSN: newLSN(sid, offset),
		ts:       time.Unix(int64(h.ts), 0).UTC(),
	}

	return seg, nil
}

func openSegment(segPath string) (*walSegment, error) {

	file, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}

	h, startOffset, err := readHeader(file)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}

	next := startOffset
	endOffset := startOffset
	maxSize := int64(h.size)

	for {
		next, err = readRecordSize(file, next)
		if err != nil {
			break
		}
		endOffset = next

		if endOffset > maxSize {
			fmt.Printf("wal: segment id=%d is likely corrupt", h.sid)
		}
	}

	sid := segID(h.sid)
	seg := &walSegment{
		cursor:   endOffset,
		file:     file,
		flags:    h.flags,
		maxSize:  maxSize,
		path:     segPath,
		sid:      sid,
		startLSN: newLSN(sid, startOffset),
		ts:       time.Unix(int64(h.ts), 0).UTC(),
	}

	return seg, nil
}

func (s *walSegment) close() error {
	if s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *walSegment) size() int64 {
	info, err := s.file.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}

func (s *walSegment) read(lsn LSN) (*walRecord, LSN, error) {
	if s.sid != lsnSegID(lsn) {
		return nil, 0, fmt.Errorf("wal: lsn does not belong to this segment")
	}
	offset := lsnOffset(lsn)
	// fallocate will read zeros
	if offset >= s.cursor {
		return nil, 0, io.EOF
	}
	rec, n, err := readRecord(s.file, offset)
	if err != nil {
		return nil, 0, err
	}
	nextOffset := offset + int64(n)
	return rec, newLSN(s.sid, nextOffset), nil
}
