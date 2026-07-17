package sst

/*

SST FILE HEADER
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
type		4		QDAT / QIDX / QFLT magic
version		2		SST Format Version
createdAt	8		SST creation time as Unix timestamp
crc			4		CRC32C of all preceding header fields
padding		var		Zero padding to the next 8-byte boundary
------------------------------------------------------------------

All fixed-size int fields are stored in LE byte-order, except for `type`

*/

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/yashgorana/quxdb/pkg/codec"
)

const (
	sstVersion   = 1
	sstHeaderLen = ((4 + 2 + 8 + 4) + 7) &^ 7
)

const (
	sstTypeData   sstType = 'Q'<<24 | 'D'<<16 | 'A'<<8 | 'T'
	sstTypeIndex  sstType = 'Q'<<24 | 'I'<<16 | 'D'<<8 | 'X'
	sstTypeFilter sstType = 'Q'<<24 | 'F'<<16 | 'L'<<8 | 'T'
)

type sstType uint32

type sstHeader struct {
	stype     sstType
	version   uint16
	createdAt time.Time
}

func encodeHeader(dst []byte, sh sstHeader) int {
	off := 0
	binary.BigEndian.PutUint32(dst[off:], uint32(sh.stype))
	off += 4
	binary.LittleEndian.PutUint16(dst[off:], sh.version)
	off += 2
	binary.LittleEndian.PutUint64(dst[off:], uint64(sh.createdAt.UTC().UnixMilli()))
	off += 8
	crc := crc32.Checksum(dst[:off], crc32Table)
	binary.LittleEndian.PutUint32(dst[off:], crc)
	off += 4

	return sstHeaderLen
}

func decodeHeader(buf []byte, stype sstType) (sstHeader, int, error) {
	var fm sstHeader

	if len(buf) < sstHeaderLen {
		return fm, 0, fmt.Errorf("sst header: insufficient bytes to decode")
	}

	decoder := codec.NewDecoder(buf)
	st := decoder.Uint32BE("store.type")
	version := decoder.Uint16("store.version")
	createdAt := decoder.Uint64("store.createdAt")
	dataOff := decoder.Offset()
	crc := decoder.Uint32("crc")

	err := decoder.Err()
	if err != nil {
		return fm, 0, err
	}

	if st != uint32(stype) {
		return fm, 0, ErrInvalidFormat
	}

	if version != sstVersion {
		return fm, 0, ErrUnsupportedVersion
	}

	if crc != crc32.Checksum(buf[:dataOff], crc32Table) {
		return fm, 0, ErrChecksumMismatch
	}

	fm.stype = sstType(st)
	fm.version = version
	fm.createdAt = time.UnixMilli(int64(createdAt))
	return fm, sstHeaderLen, nil
}

func writeHeader(w io.Writer, sh sstHeader) (int, error) {
	var buf [sstHeaderLen]byte
	n := encodeHeader(buf[:], sh)
	wn, err := w.Write(buf[:n])
	if err != nil {
		return 0, err
	}
	if wn != n {
		return 0, io.ErrShortWrite
	}
	return wn, nil
}

func readHeader(r io.ReaderAt, stype sstType) (sstHeader, int, error) {
	var buf [sstHeaderLen]byte
	if _, err := r.ReadAt(buf[:], 0); err != nil {
		return sstHeader{}, 0, err
	}
	return decodeHeader(buf[:], stype)
}
