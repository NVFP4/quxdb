package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

/*
WAL HEADER
magic32 ver32 segId32 created64 lsn64 flags32 crc32 <padding>
*/

const (
	walVersion       uint32 = 1
	walHeaderMagic32 uint32 = 'Q'<<24 | 'U'<<16 | 'X'<<8 | 0xDB

	// magic32 + ver32 + segId32 + created64 + lsn64 + flags32 + crc32
	walHeaderEncLen = 4*5 + 8*2
	walHeaderLen    = (walHeaderEncLen + 7) &^ 7
)

var walCRC32CTable = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrHeaderWrite         = fmt.Errorf("header write")
	ErrHeaderRead          = fmt.Errorf("header read")
	ErrHeaderInvalidFormat = fmt.Errorf("%w: invalid format", ErrHeaderRead)
	ErrHeaderInvalidVer    = fmt.Errorf("%w: invalid version", ErrHeaderRead)
	ErrHeaderInvalidSize   = fmt.Errorf("%w: invalid size", ErrHeaderRead)
	ErrHeaderCorrupt       = fmt.Errorf("%w: corrupted - crc32 mismatch", ErrHeaderRead)
)

type walHeader struct {
	ver    uint32
	sid    uint32
	flags  uint32
	crc32c uint32
	ts     uint64
	size   uint64
}

func writeHeader(file io.WriterAt, sid segID, maxSize uint64, flags uint32) (*walHeader, int64, error) {
	bufOff := 0
	buf := make([]byte, walHeaderLen) // allocated padded buf
	h := &walHeader{
		ver:   walVersion,
		sid:   uint32(sid),
		ts:    uint64(time.Now().UTC().Unix()),
		size:  maxSize,
		flags: flags,
	}

	binary.BigEndian.PutUint32(buf[bufOff:], walHeaderMagic32)
	bufOff += 4

	binary.LittleEndian.PutUint32(buf[bufOff:], h.ver)
	bufOff += 4

	binary.LittleEndian.PutUint32(buf[bufOff:], h.sid)
	bufOff += 4

	binary.LittleEndian.PutUint64(buf[bufOff:], h.ts)
	bufOff += 8

	binary.LittleEndian.PutUint64(buf[bufOff:], h.size)
	bufOff += 8

	binary.LittleEndian.PutUint32(buf[bufOff:], h.flags)
	bufOff += 4

	crc := crc32.Checksum(buf[:bufOff], walCRC32CTable)
	h.crc32c = crc

	binary.LittleEndian.PutUint32(buf[bufOff:], crc)
	bufOff += 4

	n, err := file.WriteAt(buf, 0)
	if err != nil {
		return nil, -1, fmt.Errorf("%w: %w", ErrHeaderWrite, err)
	}
	if n != len(buf) {
		return nil, -1, fmt.Errorf("%w: %w", ErrHeaderWrite, io.ErrShortWrite)
	}

	return h, walHeaderLen, nil
}

func readHeader(file io.ReaderAt) (*walHeader, int64, error) {
	h := &walHeader{}

	bufOff := 0
	crcOff := 0
	buf := make([]byte, walHeaderLen)

	_, err := file.ReadAt(buf, 0)
	if err != nil {
		return nil, -1, fmt.Errorf("%w: %w", ErrHeaderRead, err)
	}

	magic := binary.BigEndian.Uint32(buf[bufOff:])
	if magic != walHeaderMagic32 {
		return nil, -1, ErrHeaderInvalidFormat
	}
	bufOff += 4

	h.ver = binary.LittleEndian.Uint32(buf[bufOff:])
	if h.ver != walVersion {
		return nil, -1, ErrHeaderInvalidVer
	}
	bufOff += 4

	h.sid = binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	h.ts = binary.LittleEndian.Uint64(buf[bufOff:])
	bufOff += 8

	h.size = binary.LittleEndian.Uint64(buf[bufOff:])
	bufOff += 8

	h.flags = binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	crcOff = bufOff
	h.crc32c = binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	expectedCRC := crc32.Checksum(buf[:crcOff], walCRC32CTable)
	if h.crc32c != expectedCRC {
		return nil, -1, ErrHeaderCorrupt
	}

	return h, walHeaderLen, nil
}
