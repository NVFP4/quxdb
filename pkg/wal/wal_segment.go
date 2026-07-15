package wal

import (
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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
	walSegmentMinSize     = 4 << 10  // 4KiB
	walSegmentDefaultSize = 64 << 20 // 64MiB
	walSegmentFlags       = 0
)

var (
	ErrSegmentLSNInvalid   = errors.New("wal: lsn does not belong to this segment")
	ErrTruncateOutOfBounds = errors.New("wal: truncate lsn outside segment bounds")
	ErrSegmentReadOnly     = errors.New("wal: segment is read-only")
)

var (
	crc32Table = crc32.MakeTable(crc32.Castagnoli)
	base32Hex  = base32.HexEncoding.WithPadding(base32.NoPadding)
)

type segmentMode uint8

const (
	segmentModeReadOnly segmentMode = iota
	segmentModeReadWrite
)

type walSegment struct {
	path string
	file *os.File
	mode segmentMode

	mmapMu sync.Mutex
	mmap   []byte

	cursor   uint64
	startLSN LSN

	walHeader
}

func newSegment(dir string, id segID, segSize uint64) (*walSegment, error) {
	createdAt := time.Now().UTC()
	segPath := filepath.Join(dir, walDir, segmentName(createdAt, id))

	if err := pathlib.EnsureParent(segPath); err != nil {
		return nil, err
	}

	if !pathlib.FileEmpty(segPath) {
		return nil, fmt.Errorf("wal: segment already exists for id=%d", id)
	}

	file, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}

	if err := fs.Fallocate(file, 0, int64(segSize)); err != nil {
		return nil, errors.Join(fmt.Errorf("wal: %w", err), file.Close())
	}

	h := walHeader{
		segVer:     walVersion,
		segId:      id,
		createdAt:  createdAt,
		segMaxSize: segSize,
		segFlags:   walSegmentFlags,
	}
	n, err := writeHeader(file, &h)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("wal: %w", err), file.Close())
	}
	offset := uint64(n)

	seg := &walSegment{
		walHeader: h,
		cursor:    offset,
		file:      file,
		path:      segPath,
		startLSN:  newLSN(h.segId, offset, h.segMaxSize),
		mode:      segmentModeReadWrite,
	}

	return seg, nil
}

func openSegment(segPath string, segmentSize uint64, mode segmentMode) (*walSegment, error) {
	var (
		file  *os.File
		err   error
		flags int
	)

	switch mode {
	case segmentModeReadOnly:
		flags = os.O_RDONLY
	case segmentModeReadWrite:
		flags = os.O_RDWR
	default:
		return nil, fmt.Errorf("wal: segment id path=%q has invalid mode=%d", segPath, mode)
	}
	file, err = os.OpenFile(segPath, flags, 0)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}

	h, n, err := readHeader(file)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("wal: %w", err), file.Close())
	}
	if h.segMaxSize != segmentSize {
		return nil, errors.Join(fmt.Errorf("wal: segment id=%d size=%d does not match configured size=%d", h.segId, h.segMaxSize, segmentSize), file.Close())
	}

	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("wal: stat segment id=%d: %w", h.segId, err), file.Close())
	}
	if info.Size() < int64(n) || uint64(info.Size()) > h.segMaxSize {
		return nil, errors.Join(fmt.Errorf("wal: segment id=%d has invalid logical size=%d", h.segId, info.Size()), file.Close())
	}

	seg := &walSegment{
		walHeader: *h,
		cursor:    uint64(info.Size()),
		file:      file,
		path:      segPath,
		startLSN:  newLSN(h.segId, uint64(n), h.segMaxSize),
		mode:      mode,
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

	err := s.file.Close()
	s.file = nil // we cleanup the ref, regardless of the error
	return err
}

func (s *walSegment) openReadWrite() error {
	if s.mode == segmentModeReadWrite && s.file != nil {
		return nil
	}
	if err := s.closeFile(); err != nil {
		return err
	}
	file, err := os.OpenFile(s.path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	s.file = file
	s.mode = segmentModeReadWrite
	return nil
}

func (s *walSegment) openReadOnlyMmapLocked() ([]byte, error) {
	if s.mmap != nil {
		return s.mmap, nil
	}
	if s.mode != segmentModeReadOnly {
		return nil, fmt.Errorf("wal: segment id=%d is not read-only", s.segId)
	}

	if s.file == nil {
		file, err := os.OpenFile(s.path, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		s.file = file
	}

	mmap, err := fs.Mmap(s.file, 0, int64(s.cursor))
	if err != nil {
		return nil, err
	}

	err = fs.Madvice(mmap, fs.MADV_SEQUENTIAL)
	if err != nil {
		fmt.Printf("madvice err %s\n", err)
	}

	if err := s.closeFile(); err != nil {
		_ = fs.Munmap(mmap)
		return nil, err
	}
	s.mmap = mmap
	return mmap, nil
}

func (s *walSegment) getMmap() ([]byte, error) {
	s.mmapMu.Lock()
	defer s.mmapMu.Unlock()

	if s.mmap != nil {
		return s.mmap, nil
	}

	return s.openReadOnlyMmapLocked()
}

func (s *walSegment) munmap() error {
	s.mmapMu.Lock()
	defer s.mmapMu.Unlock()

	if s.mmap == nil {
		return nil
	}
	err := fs.Munmap(s.mmap)
	s.mmap = nil
	return err
}

func (s *walSegment) read(lsn LSN) (rec *walRecord, next LSN, err error) {
	switch s.mode {
	case segmentModeReadWrite:
		if s.file == nil {
			return nil, 0, fmt.Errorf("wal: active segment id=%d has no file descriptor", s.segId)
		}
		return s.readFromReader(s.file, lsn)
	case segmentModeReadOnly:
		mmap, err := s.getMmap()
		if err != nil {
			return nil, 0, err
		}
		return s.readFromBytes(mmap, lsn)
	default:
		return nil, 0, fmt.Errorf("wal: segment id=%d has invalid mode=%d", s.segId, s.mode)
	}
}

func (s *walSegment) readFromReader(r io.ReaderAt, lsn LSN) (*walRecord, LSN, error) {
	offset, err := s.offsetFromLSN(lsn)
	if err != nil {
		return nil, 0, err
	}

	rec, _, err := readRecord(r, offset)
	if err != nil {
		return nil, 0, err
	}
	return s.finishRead(&rec, lsn, offset)
}

func (s *walSegment) readFromBytes(src []byte, lsn LSN) (*walRecord, LSN, error) {
	offset, err := s.offsetFromLSN(lsn)
	if err != nil {
		return nil, 0, err
	}

	rec, err := readRecordBytes(src, offset)
	if err != nil {
		return nil, 0, err
	}
	return s.finishRead(&rec, lsn, offset)
}

func (s *walSegment) offsetFromLSN(lsn LSN) (uint64, error) {
	if s.segId != lsnSegID(lsn, s.segMaxSize) {
		return 0, ErrSegmentLSNInvalid
	}
	offset := lsnOffset(lsn, s.segMaxSize)
	if offset < lsnOffset(s.startLSN, s.segMaxSize) {
		return 0, ErrSegmentLSNInvalid
	}
	// cursor is the logical WAL EOF, whether it came from fstat or appends.
	if offset >= s.cursor {
		return 0, io.EOF
	}
	return offset, nil
}

func (s *walSegment) finishRead(rec *walRecord, lsn LSN, offset uint64) (*walRecord, LSN, error) {
	if LSN(rec.lsn) != lsn {
		return nil, 0, fmt.Errorf("%w: expected=%d got=%d", ErrRecordInvalidLSN, lsn, rec.lsn)
	}

	nextOffset := offset + uint64(rec.recLen)
	if nextOffset > s.cursor || nextOffset > s.segMaxSize {
		return nil, 0, ErrRecordTorn
	}
	return rec, newLSN(s.segId, nextOffset, s.segMaxSize), nil
}

func (s *walSegment) append(data []byte, flags uint16) (LSN, error) {
	if s.mode != segmentModeReadWrite || s.file == nil {
		return 0, ErrSegmentReadOnly
	}
	if err := validateDataLen(data); err != nil {
		return 0, err
	}

	lsn := newLSN(s.segId, s.cursor, s.segMaxSize)
	rec := newRecord(lsn, flags, data)
	totalBytes := int(rec.recLen)
	if err := s.checkRoom(totalBytes); err != nil {
		return 0, err
	}

	buf := bufpool.Get(uint(totalBytes))
	defer bufpool.Put(buf)

	_, err := encodeRecord(buf, &rec)
	if err != nil {
		return 0, err
	}

	if err := s.writeAtCursor(buf); err != nil {
		return 0, err
	}

	s.cursor += uint64(totalBytes)
	return lsn, nil
}

func (s *walSegment) appendBatch(batch [][]byte, flags uint16) ([]AppendResult, error) {
	if s.mode != segmentModeReadWrite || s.file == nil {
		return nil, ErrSegmentReadOnly
	}
	results := make([]AppendResult, len(batch))
	totalBytes := 0
	for i, data := range batch {
		if err := validateDataLen(data); err != nil {
			results[i].Err = err
			continue
		}
		totalBytes += encodedRecordLen(data)
	}

	if totalBytes == 0 {
		return results, nil
	}
	if err := s.checkRoom(totalBytes); err != nil {
		return nil, err
	}

	recOffset := s.cursor
	bufOffset := 0
	buf := bufpool.Get(uint(totalBytes)) // allocate a big buffer pool for the whole batch
	defer bufpool.Put(buf)

	for i, data := range batch {
		if results[i].Err != nil {
			continue
		}

		lsn := newLSN(s.segId, recOffset, s.segMaxSize)
		results[i].LSN = lsn

		rec := newRecord(lsn, flags, data)
		n, err := encodeRecord(buf[bufOffset:], &rec)
		if err != nil {
			results[i].Err = err
			continue
		}
		bufOffset += n
		recOffset += uint64(n)
	}

	if err := s.writeAtCursor(buf); err != nil {
		return nil, err
	}

	s.cursor = recOffset

	return results, nil
}

func (s *walSegment) checkRoom(n int) error {
	if s.cursor+uint64(n) < s.segMaxSize {
		return nil
	}
	if lsnOffset(s.startLSN, s.segMaxSize)+uint64(n) >= s.segMaxSize {
		return ErrRecordTooLarge
	}
	return errSegmentInsufficientSpace
}

func (s *walSegment) sync() error {
	if s.mode != segmentModeReadWrite || s.file == nil {
		return ErrSegmentReadOnly
	}
	return fs.Fdatasync(s.file)
}

func (s *walSegment) writeAtCursor(buf []byte) error {
	n, err := s.file.WriteAt(buf, int64(s.cursor))
	if err == nil && n == len(buf) {
		return nil
	}
	if err == nil {
		err = io.ErrShortWrite
	}
	if truncateErr := s.file.Truncate(int64(s.cursor)); truncateErr != nil {
		return errors.Join(err, truncateErr)
	}
	return err
}

func (s *walSegment) seal() error {
	if s.mode != segmentModeReadWrite || s.file == nil {
		return ErrSegmentReadOnly
	}
	if err := fs.Fdatasync(s.file); err != nil {
		return err
	}
	if err := s.closeFile(); err != nil {
		return err
	}
	s.mode = segmentModeReadOnly
	return nil
}

func (s *walSegment) truncate(lsn LSN) error {
	if s.segId != lsnSegID(lsn, s.segMaxSize) {
		return ErrSegmentLSNInvalid
	}
	offset := lsnOffset(lsn, s.segMaxSize)
	if offset < lsnOffset(s.startLSN, s.segMaxSize) || offset > s.cursor {
		return ErrTruncateOutOfBounds
	}
	if err := s.munmap(); err != nil {
		return err
	}
	if err := s.openReadWrite(); err != nil {
		return err
	}
	if err := s.file.Truncate(int64(offset)); err != nil {
		return err
	}
	if err := fs.Fdatasync(s.file); err != nil {
		return err
	}
	s.cursor = offset
	return nil
}

// Base32Hex(48-bit milliseconds | 32-bit ID)
// timestamps > 48-bits will return undefined behavior
// so worry after 10889-08-02 05:31:50.655 UTC
func segmentName(ts time.Time, id segID) string {
	var buf [10]byte
	u := uint64(ts.UnixMilli())

	// lower 48 bits of ts in big endian
	buf[0] = byte(u >> 40)
	buf[1] = byte(u >> 32)
	buf[2] = byte(u >> 24)
	buf[3] = byte(u >> 16)
	buf[4] = byte(u >> 8)
	buf[5] = byte(u)

	binary.BigEndian.PutUint32(buf[6:], uint32(id))
	return base32Hex.EncodeToString(buf[:]) + ".quxwal"
}

type AppendResult struct {
	LSN LSN
	Err error
}
