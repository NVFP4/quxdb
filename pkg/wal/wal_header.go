package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

/*
===============================================================
WAL Header
===============================================================
magic32    QUXWAL
ver16      WAL Version
flags16    WAL Flags
sid32      WAL Segment ID
maxsize64  WAL Segment Max Size
created64  Unix timestamp
crc32      CRC32C(magic, ver, flags, sid, maxsize, created)
===============================================================
<padding>
===============================================================
*/

const (
	walVersion       = 1
	walHeaderMagic32 = 'Q'<<24 | 'W'<<16 | 'A'<<8 | 'L'

	walHeaderEncLen = 8*2 + 4*3 + 2*2
	walHeaderLen    = (walHeaderEncLen + 7) &^ 7
)

var walCRC32CTable = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrHeaderWrite         = errors.New("header write")
	ErrHeaderRead          = errors.New("header read")
	ErrHeaderInvalidFormat = errors.New("header read: invalid format")
	ErrHeaderInvalidVer    = errors.New("header read: invalid version")
	ErrHeaderInvalidSize   = errors.New("header read: invalid size")
	ErrHeaderCorrupt       = errors.New("header read: corrupted - crc32 mismatch")
)

type walHeader struct {
	sid     uint32
	ver     uint16
	flags   uint16
	crc32c  uint32
	created uint64
	maxSize uint64
}

func writeHeader(w io.WriterAt, sid segID, maxSize uint64, flags uint16) (*walHeader, int64, error) {
	bufOff := 0
	buf := make([]byte, walHeaderLen) // allocated padded buf
	h := &walHeader{
		ver:     walVersion,
		sid:     uint32(sid),
		created: uint64(time.Now().UTC().Unix()),
		maxSize: maxSize,
		flags:   flags,
	}

	// magic32
	binary.BigEndian.PutUint32(buf[bufOff:], walHeaderMagic32)
	bufOff += 4

	// ver16
	binary.LittleEndian.PutUint16(buf[bufOff:], h.ver)
	bufOff += 2

	// flags16
	binary.LittleEndian.PutUint16(buf[bufOff:], h.flags)
	bufOff += 2

	// sid32
	binary.LittleEndian.PutUint32(buf[bufOff:], h.sid)
	bufOff += 4

	// maxSize64
	binary.LittleEndian.PutUint64(buf[bufOff:], h.maxSize)
	bufOff += 8

	// created64
	binary.LittleEndian.PutUint64(buf[bufOff:], h.created)
	bufOff += 8

	crc := crc32.Checksum(buf[:bufOff], walCRC32CTable)
	h.crc32c = crc

	// crc32
	binary.LittleEndian.PutUint32(buf[bufOff:], crc)
	bufOff += 4

	n, err := w.WriteAt(buf, 0)
	if err != nil {
		return nil, -1, fmt.Errorf("%w: %w", ErrHeaderWrite, err)
	}
	if n != len(buf) {
		return nil, -1, fmt.Errorf("%w: %w", ErrHeaderWrite, io.ErrShortWrite)
	}

	return h, walHeaderLen, nil
}

func readHeader(r io.ReaderAt) (*walHeader, int64, error) {
	h := &walHeader{}

	bufOff := 0
	crcOff := 0
	buf := make([]byte, walHeaderLen)

	_, err := r.ReadAt(buf, 0)
	if err != nil {
		return nil, -1, fmt.Errorf("%w: %w", ErrHeaderRead, err)
	}

	magic := binary.BigEndian.Uint32(buf[bufOff:])
	if magic != walHeaderMagic32 {
		return nil, -1, ErrHeaderInvalidFormat
	}
	bufOff += 4

	h.ver = binary.LittleEndian.Uint16(buf[bufOff:])
	if h.ver != walVersion {
		return nil, -1, ErrHeaderInvalidVer
	}
	bufOff += 2

	h.flags = binary.LittleEndian.Uint16(buf[bufOff:])
	bufOff += 2

	h.sid = binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	h.maxSize = binary.LittleEndian.Uint64(buf[bufOff:])
	bufOff += 8

	h.created = binary.LittleEndian.Uint64(buf[bufOff:])
	bufOff += 8

	crcOff = bufOff
	h.crc32c = binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	expectedCRC := crc32.Checksum(buf[:crcOff], walCRC32CTable)
	if h.crc32c != expectedCRC {
		return nil, -1, ErrHeaderCorrupt
	}

	return h, walHeaderLen, nil
}
