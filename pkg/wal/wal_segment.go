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
	"strings"
	"sync"
	"time"

	"github.com/yashgorana/quxdb/pkg/fs"
	"github.com/yashgorana/quxdb/pkg/pathlib"
)

const (
	walSegmentMinSize     = 4 << 10  // 4KiB
	walSegmentDefaultSize = 64 << 20 // 64MiB
	walSegmentFlags       = 0
	walEndMarkerLen       = 8 // zeros after the last record, read as end of segment
)

var (
	ErrSegmentLSNInvalid   = errors.New("wal: lsn does not belong to this segment")
	ErrTruncateOutOfBounds = errors.New("wal: truncate lsn outside segment bounds")
	ErrSegmentReadOnly     = errors.New("wal: segment is read-only")
)

var (
	crc32Table = crc32.MakeTable(crc32.Castagnoli)
	base32Hex  = base32.HexEncoding.WithPadding(base32.NoPadding)

	endMarker [walEndMarkerLen]byte
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

// newSegment creates a full-size zero-filled segment through a temp file, named with suffix.
func newSegment(dir string, id segID, segSize uint64, suffix string) (*walSegment, error) {
	createdAt := time.Now().UTC()
	segPath := filepath.Join(dir, walDir, segmentName(createdAt, id))

	if err := pathlib.EnsureParent(segPath); err != nil {
		return nil, err
	}

	if !pathlib.FileEmpty(segPath) {
		return nil, fmt.Errorf("wal: segment already exists for id=%d", id)
	}

	tmpPath := segPath + walTmpSuffix
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o755)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}
	fail := func(err error) (*walSegment, error) {
		return nil, errors.Join(fmt.Errorf("wal: %w", err), file.Close(), os.Remove(tmpPath))
	}

	if err := fs.Preallocate(file, int64(segSize)); err != nil {
		return fail(err)
	}

	h := newWalHeader(id, segSize, createdAt)
	n, err := writeHeader(file, &h)
	if err != nil {
		return fail(err)
	}
	offset := uint64(n)

	// unwritten extents would make every commit fdatasync convert them
	if err := fs.ZeroFill(file, int64(offset), int64(segSize-offset)); err != nil {
		return fail(err)
	}
	if err := fs.SyncData(file); err != nil {
		return fail(err)
	}
	if err := fs.RenameDurable(tmpPath, segPath+suffix); err != nil {
		return nil, errors.Join(fmt.Errorf("wal: %w", err), file.Close())
	}

	return activeSegment(file, segPath+suffix, h, offset)
}

// recycleSegment reuses a pruned spare as segment id named with suffix, its stale records carry older segment ids.
func recycleSegment(sparePath, dir string, id segID, segSize uint64, suffix string) (*walSegment, error) {
	file, err := os.OpenFile(sparePath, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}
	fail := func(err error) (*walSegment, error) {
		return nil, errors.Join(fmt.Errorf("wal: recycle %s: %w", sparePath, err), file.Close())
	}

	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if uint64(info.Size()) != segSize {
		return fail(fmt.Errorf("size=%d does not match segment size=%d", info.Size(), segSize))
	}

	createdAt := time.Now().UTC()
	h := newWalHeader(id, segSize, createdAt)
	// header and end marker in one write
	var buf [walHeaderLenPadded + walEndMarkerLen]byte
	if _, err := encodeHeader(buf[:], &h); err != nil {
		return fail(err)
	}
	if _, err := file.WriteAt(buf[:], 0); err != nil {
		return fail(err)
	}
	if err := fs.SyncData(file); err != nil {
		return fail(err)
	}
	segPath := filepath.Join(dir, walDir, segmentName(createdAt, id)) + suffix
	if err := fs.RenameDurable(sparePath, segPath); err != nil {
		return fail(err)
	}

	return activeSegment(file, segPath, h, walHeaderLenPadded)
}

func newWalHeader(id segID, segSize uint64, createdAt time.Time) walHeader {
	return walHeader{
		segVer:     walVersion,
		segId:      id,
		createdAt:  createdAt,
		segMaxSize: segSize,
		segFlags:   walSegmentFlags,
	}
}

// activeSegment reopens a just-renamed file under path so io errors name the segment, not its old name.
func activeSegment(file *os.File, path string, h walHeader, offset uint64) (*walSegment, error) {
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("wal: %w", err)
	}
	return &walSegment{
		walHeader: h,
		cursor:    offset,
		file:      file,
		path:      path,
		startLSN:  newLSN(h.segId, offset, h.segMaxSize),
		mode:      segmentModeReadWrite,
	}, nil
}

// activate gives a prepared segment its final name, durably so its records are found after a crash.
func (s *walSegment) activate() error {
	final, ok := strings.CutSuffix(s.path, walPreparedSuffix)
	if !ok {
		return nil
	}
	if err := fs.RenameDurable(s.path, final); err != nil {
		return fmt.Errorf("wal: activate segment id=%d: %w", s.segId, err)
	}
	// reopened so io errors name the final path
	file, err := os.OpenFile(final, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("wal: %w", err)
	}
	err = s.file.Close()
	s.file, s.path = file, final
	return err
}

// openSegment opens an existing segment with its cursor at the segment end.
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
	if uint64(info.Size()) != h.segMaxSize {
		return nil, errors.Join(fmt.Errorf("wal: segment id=%d has invalid size=%d", h.segId, info.Size()), file.Close())
	}

	return &walSegment{
		walHeader: *h,
		cursor:    h.segMaxSize,
		file:      file,
		path:      segPath,
		startLSN:  newLSN(h.segId, uint64(n), h.segMaxSize),
		mode:      mode,
	}, nil
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

	mmap, err := fs.Map(s.file, 0, int64(s.cursor))
	if err != nil {
		return nil, err
	}

	err = fs.Advise(mmap, fs.AdviceSequential)
	if err != nil {
		fmt.Printf("wal: madvise err %s\n", err)
	}

	if err := s.closeFile(); err != nil {
		_ = fs.Unmap(mmap)
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
	err := fs.Unmap(s.mmap)
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
	// space too small for a record ends the segment
	if offset >= s.cursor || recordCapacity(s.segMaxSize-offset) < 0 {
		return 0, io.EOF
	}
	return offset, nil
}

func (s *walSegment) finishRead(rec *walRecord, lsn LSN, offset uint64) (*walRecord, LSN, error) {
	if LSN(rec.lsn) != lsn {
		return nil, 0, fmt.Errorf("%w: expected=%d got=%d", ErrRecordInvalidLSN, lsn, rec.lsn)
	}

	nextOffset := offset + uint64(rec.recLen)
	if nextOffset > s.cursor || nextOffset >= s.segMaxSize {
		return nil, 0, ErrRecordTorn
	}
	return rec, newLSN(s.segId, nextOffset, s.segMaxSize), nil
}

// hasRecordStartFrom reports whether a valid walRecord starting a Record of this segment starts at or after off.
func (s *walSegment) hasRecordStartFrom(off uint64) (bool, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return false, err
	}
	b, err := fs.Map(file, 0, int64(s.segMaxSize))
	if closeErr := file.Close(); err != nil || closeErr != nil {
		return false, errors.Join(err, closeErr)
	}
	defer fs.Unmap(b)

	for off = alignUp8u(off); recordCapacity(s.segMaxSize-off) >= 0; off += 8 {
		if binary.BigEndian.Uint32(b[off:]) != walRecordMagic32 {
			continue
		}
		h, _, err := decodeRecordHeader(b[off:])
		if err != nil || LSN(h.lsn) != newLSN(s.segId, off, s.segMaxSize) {
			continue
		}
		// a partial walRecord without the start flag can belong to the damaged Record itself
		if h.starts() {
			return true, nil
		}
	}
	return false, nil
}

func (s *walSegment) sync() error {
	if s.mode != segmentModeReadWrite || s.file == nil {
		return ErrSegmentReadOnly
	}
	return fs.SyncData(s.file)
}

// recordCapacity is the largest record payload that fits in room bytes.
func recordCapacity(room uint64) int {
	return int((room-1)&^7) - walRecordHeaderLen - walRecordMetaLen
}

func alignUp8u(n uint64) uint64 {
	return (n + 7) &^ 7
}

func (s *walSegment) seal() error {
	if s.mode != segmentModeReadWrite || s.file == nil {
		return ErrSegmentReadOnly
	}
	if err := fs.SyncData(s.file); err != nil {
		return err
	}
	if err := s.closeFile(); err != nil {
		return err
	}
	s.mode = segmentModeReadOnly
	return nil
}

// truncate ends the segment at lsn with an end marker.
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
	if recordCapacity(s.segMaxSize-offset) >= 0 {
		if _, err := s.file.WriteAt(endMarker[:], int64(offset)); err != nil {
			return err
		}
	}
	if err := fs.SyncData(s.file); err != nil {
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
