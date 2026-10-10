package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/yashgorana/quxdb/pkg/codec"
)

/*

WAL RECORD HEADER
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
magic		4		QREC
recLen		4		Total encoded record size, including padding
recFlags	2		Record flags
lsn			8		Log sequence number
dataLen		4		Data size in bytes
headerCRC	4		CRC32C of all preceding fields
------------------------------------------------------------------

WAL RECORD FLAGS (recFlags)
------------------------------------------------------------------
Bits				Description
------------------------------------------------------------------
0-13				Reserved
14-15				Record type: 0 full, 1 start, 2 middle, 3 end
------------------------------------------------------------------

WAL RECORD
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
header		26		WAL Record header
data		var		Record data bytes
crc			4		CRC32C of all preceding fields
padding		var		Zero padding to 8-byte alignment
------------------------------------------------------------------

All fixed-size int fields are stored in LE byte-order, except for `magic`.

*/

const (
	recordHeaderLen = 4 + 4 + 2 + 8 + 4 + 4
	recordMetaLen   = 4 // record crc

	recordMagic32 uint32 = 'Q'<<24 | 'R'<<16 | 'E'<<8 | 'C'
)

// partial record position, stored in recFlags bits 14-15
const (
	recordFull          uint16 = 0 << 14
	recordPartialMask   uint16 = 3 << 14
	recordPartialStart  uint16 = 1 << 14
	recordPartialMiddle uint16 = 2 << 14
	recordPartialEnd    uint16 = 3 << 14
)

var (
	ErrRecordInvalidFormat    = errors.New("record invalid format")
	ErrRecordInvalidSize      = errors.New("record invalid size")
	ErrRecordInvalidLSN       = errors.New("record lsn mismatch")
	ErrRecordTorn             = errors.New("record torn")
	ErrRecordChecksumMismatch = errors.New("record checksum mismatch")
)

func isCorruptionError(err error) bool {
	return errors.Is(err, ErrRecordInvalidFormat) ||
		errors.Is(err, ErrRecordInvalidSize) ||
		errors.Is(err, ErrRecordInvalidLSN) ||
		errors.Is(err, ErrRecordTorn) ||
		errors.Is(err, ErrRecordChecksumMismatch)
}

type walRecordHeader struct {
	lsn       uint64
	headerCRC uint32
	recLen    uint32
	recFlags  uint16
	dataLen   uint32
}

func (h *walRecordHeader) partial() uint16 {
	return h.recFlags & recordPartialMask
}

func (h *walRecordHeader) starts() bool {
	p := h.partial()
	return p == recordFull || p == recordPartialStart
}

func (h *walRecordHeader) ends() bool {
	p := h.partial()
	return p == recordFull || p == recordPartialEnd
}

type walRecord struct {
	walRecordHeader
	crc  uint32
	data []byte
}

func encodeRecordHeader(dst []byte, h *walRecordHeader) (int, error) {
	off := 0

	binary.BigEndian.PutUint32(dst[off:], recordMagic32)
	off += 4

	binary.LittleEndian.PutUint32(dst[off:], h.recLen)
	off += 4

	binary.LittleEndian.PutUint16(dst[off:], h.recFlags)
	off += 2

	binary.LittleEndian.PutUint64(dst[off:], h.lsn)
	off += 8

	binary.LittleEndian.PutUint32(dst[off:], h.dataLen)
	off += 4

	h.headerCRC = crc32.Checksum(dst[:off], crc32Table)
	binary.LittleEndian.PutUint32(dst[off:], h.headerCRC)
	off += 4

	return off, nil
}

func decodeRecordHeader(src []byte) (walRecordHeader, int, error) {
	var h walRecordHeader
	if len(src) < recordHeaderLen {
		return h, 0, ErrRecordTorn
	}

	decoder := codec.NewDecoder(src)
	magic := decoder.Uint32BE("record.magic")
	h.recLen = decoder.Uint32("record.length")
	h.recFlags = decoder.Uint16("record.flags")
	h.lsn = decoder.Uint64("record.lsn")
	h.dataLen = decoder.Uint32("record.dataLength")
	crcOff := decoder.Offset()
	h.headerCRC = decoder.Uint32("record.headerCRC")

	if err := decoder.Err(); err != nil {
		return h, 0, err
	}
	if magic == 0 {
		return h, 0, io.EOF
	}
	if magic != recordMagic32 {
		return h, 0, ErrRecordInvalidFormat
	}
	if h.recLen < recordHeaderLen+recordMetaLen || h.recLen%8 != 0 {
		return h, 0, ErrRecordTorn
	}
	if encodedRecordSize(int(h.dataLen)) != int(h.recLen) {
		return h, 0, ErrRecordInvalidSize
	}
	if h.headerCRC != crc32.Checksum(src[:crcOff], crc32Table) {
		return h, 0, ErrRecordChecksumMismatch
	}

	return h, decoder.Offset(), nil
}

func decodeRecord(src []byte) (walRecord, int, error) {
	h, off, err := decodeRecordHeader(src)
	if err != nil {
		return walRecord{}, 0, err
	}
	if int(h.recLen) > len(src) {
		return walRecord{}, 0, ErrRecordTorn
	}

	rec := walRecord{walRecordHeader: h}
	decoder := codec.NewDecoder(src[off:h.recLen])

	// caller must clone this slice as the underlying buffer will be short-lived
	rec.data = decoder.Bytes("record.data", int(rec.dataLen))
	crcOff := off + decoder.Offset()
	rec.crc = decoder.Uint32("record.crc")

	if err := decoder.Err(); err != nil {
		return walRecord{}, 0, err
	}
	if rec.crc != crc32.Checksum(src[:crcOff], crc32Table) {
		return walRecord{}, 0, ErrRecordChecksumMismatch
	}

	return rec, int(rec.recLen), nil
}

func readRecordBytes(src []byte, off uint64) (walRecord, error) {
	boff := int(off)
	if off > uint64(len(src)) || len(src)-boff < recordHeaderLen {
		return walRecord{}, ErrRecordTorn
	}

	h, _, err := decodeRecordHeader(src[boff:])
	if err != nil {
		return walRecord{}, err
	}

	end := boff + int(h.recLen)
	if end > len(src) {
		return walRecord{}, ErrRecordTorn
	}
	rec, _, err := decodeRecord(src[boff:end])
	if err != nil {
		return walRecord{}, err
	}

	return rec, nil
}

func encodedRecordSize(size int) int {
	return alignUp8(recordHeaderLen + recordMetaLen + size)
}

func alignUp8(n int) int {
	return (n + 7) &^ 7
}
