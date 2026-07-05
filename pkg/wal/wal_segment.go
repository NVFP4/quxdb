package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yashgorana/quxdb/pkg/bufpool"
	"github.com/yashgorana/quxdb/pkg/fs"
	"github.com/yashgorana/quxdb/pkg/pathlib"
)

const (
	walSegmentMaxSize = 64 * 1024 * 1024
	walSegmentFlags   = 0
)

var (
	ErrSegmentLSNMismatch  = errors.New("wal: lsn does not belong to this segment")
	ErrSegmentLSNBefore    = errors.New("wal: lsn is before segment start")
	ErrTruncateOutOfBounds = errors.New("wal: truncate lsn outside segment bounds")
)

type walSegment struct {
	path string
	file *os.File

	mmapMu sync.Mutex
	mmap   []byte

	cursor   int64
	maxSize  int64
	startLSN LSN
	created  time.Time
	sid      segID
	flags    uint16
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

	if err := fs.Fallocate(file, 0, walSegmentMaxSize); err != nil {
		return nil, err
	}

	h, offset, err := writeHeader(file, id, walSegmentMaxSize, walSegmentFlags)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}

	sid := segID(h.sid)
	maxSize := int64(h.maxSize)

	seg := &walSegment{
		cursor:   offset,
		file:     file,
		flags:    h.flags,
		maxSize:  maxSize,
		path:     segPath,
		sid:      sid,
		startLSN: newLSN(sid, offset),
		created:  time.Unix(int64(h.created), 0).UTC(),
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

	maxSize := int64(h.maxSize)
	endOffset := getLastOffset(file, startOffset, maxSize)
	if endOffset > maxSize {
		fmt.Printf("wal: segment id=%d is likely corrupt\n", h.sid)
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
		created:  time.Unix(int64(h.created), 0).UTC(),
	}

	return seg, nil
}

func (s *walSegment) close() error {
	return errors.Join(s.munmap(), s.closeFile())
}

func (s *walSegment) closeFile() error {
	if s.file == nil {
		return nil
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	s.file = nil
	return nil
}

func (s *walSegment) openReadWrite() error {
	if s.file != nil {
		return nil
	}
	file, err := os.OpenFile(s.path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	s.file = file
	return nil
}

func (s *walSegment) mmapReadOnly() ([]byte, error) {
	s.mmapMu.Lock()
	defer s.mmapMu.Unlock()

	if s.mmap != nil {
		return s.mmap, nil
	}
	if s.cursor == 0 {
		return nil, nil
	}

	file, err := os.OpenFile(s.path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	mmap, err := fs.Mmap(file, 0, s.cursor)
	closeErr := file.Close()
	if err != nil {
		return nil, errors.Join(err, closeErr)
	}
	if closeErr != nil {
		_ = fs.Munmap(mmap)
		return nil, closeErr
	}

	s.mmap = mmap
	return s.mmap, nil
}

func (s *walSegment) munmap() error {
	s.mmapMu.Lock()
	defer s.mmapMu.Unlock()

	if s.mmap == nil {
		return nil
	}
	if err := fs.Munmap(s.mmap); err != nil {
		return err
	}
	s.mmap = nil
	return nil
}

func (s *walSegment) read(lsn LSN) (*walRecord, LSN, error) {
	if s.file != nil {
		return s.readFrom(s.file, lsn)
	}

	mmap, err := s.mmapReadOnly()
	if err != nil {
		return nil, 0, err
	}
	return s.readFrom(bytes.NewReader(mmap), lsn)
}

func (s *walSegment) readFrom(r io.ReaderAt, lsn LSN) (*walRecord, LSN, error) {
	if s.sid != lsnSegID(lsn) {
		return nil, 0, ErrSegmentLSNMismatch
	}
	offset := lsnOffset(lsn)
	if offset < lsnOffset(s.startLSN) {
		return nil, 0, ErrSegmentLSNBefore
	}
	// Fallocate leaves zeroes after the logical WAL cursor.
	if offset >= s.cursor {
		return nil, 0, io.EOF
	}

	rec, err := readRecord(r, offset, s.cursor)
	if err != nil {
		return nil, 0, err
	}
	if LSN(rec.lsn) != lsn {
		return nil, 0, ErrRecordInvalidLSN
	}

	nextOffset := offset + int64(rec.recLen)
	return rec, newLSN(s.sid, nextOffset), nil
}

func (s *walSegment) append(data []byte, flags uint16) (LSN, error) {
	totalBytes := encodedRecordLen(data)
	if err := s.checkRoom(totalBytes); err != nil {
		return 0, err
	}

	lsn := newLSN(s.sid, s.cursor)
	buf := bufpool.Get(uint(totalBytes))
	defer bufpool.Put(buf)

	encodeRecord(buf, lsn, flags, data)

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

func (s *walSegment) appendBatch(batch [][]byte, flags uint16) ([]LSN, error) {
	totalBytes := encodedBatchLen(batch)
	if err := s.checkRoom(totalBytes); err != nil {
		return nil, err
	}

	recOffset := s.cursor
	bufOffset := 0

	lsns := make([]LSN, len(batch))
	buf := bufpool.Get(uint(totalBytes)) // allocate a big buffer pool for the whole batch
	defer bufpool.Put(buf)

	for i, data := range batch {
		lsn := newLSN(s.sid, recOffset)
		lsns[i] = lsn
		n := encodeRecord(buf[bufOffset:], lsn, flags, data)
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

func (s *walSegment) checkRoom(n int) error {
	if s.cursor+int64(n) < s.maxSize {
		return nil
	}
	if lsnOffset(s.startLSN)+int64(n) >= s.maxSize {
		return ErrRecordTooLarge
	}
	return errSegmentInsufficientSpace
}

func (s *walSegment) sync() error {
	return fs.Fdatasync(s.file)
}

func (s *walSegment) truncate(lsn LSN) error {
	if s.sid != lsnSegID(lsn) {
		return ErrSegmentLSNMismatch
	}
	offset := lsnOffset(lsn)
	if offset < lsnOffset(s.startLSN) || offset > s.cursor {
		return ErrTruncateOutOfBounds
	}
	if err := s.munmap(); err != nil {
		return err
	}
	if err := s.openReadWrite(); err != nil {
		return err
	}
	if err := s.file.Truncate(offset); err != nil {
		return err
	}
	if err := fs.Fdatasync(s.file); err != nil {
		return err
	}
	s.cursor = offset
	return nil
}

func segmentName(id segID) string {
	return fmt.Sprintf("%010d.quxwal", id)
}

func getLastOffset(file io.ReaderAt, startOffset int64, maxSize int64) int64 {
	next := startOffset
	endOffset := startOffset

	for {
		rec, err := readRecordHeader(file, next, maxSize)
		if err != nil {
			break
		}
		next += int64(rec.recLen)
		endOffset = next
	}
	return endOffset
}
