package wal

/*

WAL SEGMENT HEADER
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
magic		4		QWAL
segVer		2		WAL format version
segFlags	2		WAL flags
segId		4		WAL segment ID
segMaxSize	8		Max segment size in bytes
createdAt	8		Segment creation time as Unix timestamp
crc			4		CRC32C of all preceding header fields
padding		var		Zero padding to the next 8-byte boundary
------------------------------------------------------------------

All int fields are stored in LE byte-order, except for `magic`

*/

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

const (
	walVersion       = 1
	walHeaderMagic32 = 'Q'<<24 | 'W'<<16 | 'A'<<8 | 'L'

	walHeaderLen       = 8*2 + 4*3 + 2*2
	walHeaderLenPadded = (walHeaderLen + 7) &^ 7
)

var (
	ErrHeaderInvalidFormat    = errors.New("header invalid format")
	ErrHeaderInvalidVer       = errors.New("header invalid version")
	ErrHeaderChecksumMismatch = errors.New("header checksum mismatch")
)

type walHeader struct {
	segId      segID
	segVer     uint16
	segFlags   uint16
	crc        uint32
	segMaxSize uint64
	createdAt  time.Time
}

func encodeHeader(dst []byte, h *walHeader) (int, error) {
	off := 0

	binary.BigEndian.PutUint32(dst[off:], walHeaderMagic32)
	off += 4

	binary.LittleEndian.PutUint16(dst[off:], h.segVer)
	off += 2

	binary.LittleEndian.PutUint16(dst[off:], h.segFlags)
	off += 2

	binary.LittleEndian.PutUint32(dst[off:], uint32(h.segId))
	off += 4

	binary.LittleEndian.PutUint64(dst[off:], h.segMaxSize)
	off += 8

	binary.LittleEndian.PutUint64(dst[off:], uint64(h.createdAt.UnixNano()))
	off += 8

	crc := crc32.Checksum(dst[:off], crc32Table)
	h.crc = crc

	binary.LittleEndian.PutUint32(dst[off:], crc)
	off += 4

	return off, nil
}

func decodeHeader(src []byte) (walHeader, int, error) {
	var h walHeader

	off := 0
	magic := binary.BigEndian.Uint32(src[off:])
	if magic != walHeaderMagic32 {
		return h, 0, ErrHeaderInvalidFormat
	}
	off += 4

	h.segVer = binary.LittleEndian.Uint16(src[off:])
	if h.segVer != walVersion {
		return h, 0, ErrHeaderInvalidVer
	}
	off += 2

	h.segFlags = binary.LittleEndian.Uint16(src[off:])
	off += 2

	h.segId = segID(binary.LittleEndian.Uint32(src[off:]))
	off += 4

	h.segMaxSize = binary.LittleEndian.Uint64(src[off:])
	off += 8

	created := binary.LittleEndian.Uint64(src[off:])
	h.createdAt = time.Unix(0, int64(created)).UTC()
	off += 8

	crcOff := off
	h.crc = binary.LittleEndian.Uint32(src[off:])
	off += 4

	expectedCRC := crc32.Checksum(src[:crcOff], crc32Table)
	if h.crc != expectedCRC {
		return h, 0, ErrHeaderChecksumMismatch
	}

	return h, off, nil
}

func writeHeader(w io.WriterAt, h *walHeader) (int, error) {
	var buf [walHeaderLenPadded]byte

	_, err := encodeHeader(buf[:], h)
	if err != nil {
		return 0, err
	}

	n, err := w.WriteAt(buf[:], 0)
	if err != nil {
		return 0, err
	}
	if n != len(buf) {
		return 0, io.ErrShortWrite
	}

	return n, nil
}

func readHeader(r io.ReaderAt) (*walHeader, int, error) {
	var buf [walHeaderLenPadded]byte

	n, err := r.ReadAt(buf[:], 0)
	if err != nil {
		return nil, 0, err
	}
	if n != len(buf) {
		return nil, 0, fmt.Errorf("short read")
	}

	h, n, err := decodeHeader(buf[:])
	if err != nil {
		return nil, 0, err
	}

	return &h, n, nil
}
