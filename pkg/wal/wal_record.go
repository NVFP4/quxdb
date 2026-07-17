package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
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
recType		2		Record type
recFlags	2		Record flags
lsn			8		Log sequence number
dataLen		4		Data size in bytes
headerCRC	4		CRC32C of all preceding fields
------------------------------------------------------------------

WAL RECORD
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
header		28		WAL Record header
data		var		Record data bytes
crc			4		CRC32C of all preceding fields
padding		var		Zero padding to 8-byte alignment
------------------------------------------------------------------

All fixed-size int fields are stored in LE byte-order, except for `magic`.

*/

const (
	walRecordHeaderLen = 4 + 4 + 2 + 2 + 8 + 4 + 4
	walRecordMetaLen   = 4 // record crc
	walMaxDataSize     = 4 << 20

	walRecordMagic32 uint32 = 'Q'<<24 | 'R'<<16 | 'E'<<8 | 'C'
)

const (
	walRecTypeFull = 1 << iota // full key-value
)

var (
	ErrRecordInvalidFormat    = errors.New("record invalid format")
	ErrRecordInvalidSize      = errors.New("record invalid size")
	ErrRecordInvalidLSN       = errors.New("record lsn mismatch")
	ErrRecordTorn             = errors.New("record torn")
	ErrRecordChecksumMismatch = errors.New("record checksum mismatch")
)

// IsCorruption reports whether err indicates damaged WAL record data.
func IsCorruption(err error) bool {
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
	recType   uint16
	recFlags  uint16
	dataLen   uint32
}

type walRecord struct {
	walRecordHeader
	crc  uint32
	data []byte
}

func encodeRecordHeader(dst []byte, h *walRecordHeader) (int, error) {
	off := 0

	binary.BigEndian.PutUint32(dst[off:], walRecordMagic32)
	off += 4

	binary.LittleEndian.PutUint32(dst[off:], h.recLen)
	off += 4

	binary.LittleEndian.PutUint16(dst[off:], h.recType)
	off += 2

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
	if len(src) < walRecordHeaderLen {
		return h, 0, ErrRecordTorn
	}

	decoder := codec.NewDecoder(src)
	magic := decoder.Uint32BE("record.magic")
	h.recLen = decoder.Uint32("record.length")
	h.recType = decoder.Uint16("record.type")
	h.recFlags = decoder.Uint16("record.flags")
	h.lsn = decoder.Uint64("record.lsn")
	h.dataLen = decoder.Uint32("record.dataLength")
	crcOff := decoder.Offset()
	h.headerCRC = decoder.Uint32("record.headerCRC")

	if err := decoder.Err(); err != nil {
		return h, 0, err
	}
	if magic != walRecordMagic32 {
		return h, 0, ErrRecordInvalidFormat
	}
	if h.recLen < walRecordHeaderLen+walRecordMetaLen || h.recLen%8 != 0 {
		return h, 0, ErrRecordTorn
	}
	if err := validateDataWithinRecordBounds(int(h.dataLen), h.recLen); err != nil {
		return h, 0, err
	}
	if h.headerCRC != crc32.Checksum(src[:crcOff], crc32Table) {
		return h, 0, ErrRecordChecksumMismatch
	}

	return h, decoder.Offset(), nil
}

func newRecord(lsn LSN, flags uint16, data []byte) walRecord {
	return walRecord{
		walRecordHeader: walRecordHeader{
			lsn:      uint64(lsn),
			recLen:   uint32(encodedRecordLen(data)),
			recType:  walRecTypeFull,
			recFlags: flags,
			dataLen:  uint32(len(data)),
		},
		data: data,
	}
}

func encodeRecord(dst []byte, rec *walRecord) (int, error) {
	off, err := encodeRecordHeader(dst, &rec.walRecordHeader)
	if err != nil {
		return 0, err
	}

	dataLen := len(rec.data)
	if dataLen != int(rec.dataLen) {
		return 0, ErrRecordInvalidSize
	}

	copy(dst[off:off+dataLen], rec.data)
	off += dataLen

	rec.crc = crc32.Checksum(dst[:off], crc32Table)
	binary.LittleEndian.PutUint32(dst[off:], rec.crc)
	off += 4

	// zero pad the rest
	clear(dst[off:rec.recLen])

	return int(rec.recLen), nil
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

func readRecordHeader(r io.ReaderAt, off uint64) (walRecord, int, error) {
	var rec walRecord
	var buf [walRecordHeaderLen]byte

	n, err := r.ReadAt(buf[:], int64(off))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return rec, 0, ErrRecordTorn
		}
		return rec, 0, fmt.Errorf("header %w", err)
	}
	if n != walRecordHeaderLen {
		return rec, 0, ErrRecordTorn
	}

	h, _, err := decodeRecordHeader(buf[:])
	if err != nil {
		return rec, 0, err
	}
	rec.walRecordHeader = h

	return rec, n, nil
}

func readRecord(r io.ReaderAt, off uint64) (walRecord, int, error) {
	h, _, err := readRecordHeader(r, off)
	if err != nil {
		return walRecord{}, 0, err
	}

	buf := make([]byte, h.recLen)
	n, err := r.ReadAt(buf, int64(off))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return walRecord{}, 0, ErrRecordTorn
		}
		return walRecord{}, 0, err
	}
	if n != len(buf) {
		return walRecord{}, 0, ErrRecordTorn
	}

	rec, n, err := decodeRecord(buf)
	if err != nil {
		return walRecord{}, 0, err
	}

	return rec, n, nil
}

func readRecordBytes(src []byte, off uint64) (walRecord, error) {
	boff := int(off)
	if off > uint64(len(src)) || len(src)-boff < walRecordHeaderLen {
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

func validateDataWithinRecordBounds(dataLen int, recLen uint32) error {
	if dataLen > walMaxDataSize || encodedRecordSize(dataLen) != int(recLen) {
		return ErrRecordInvalidSize
	}
	return nil
}

func validateDataLen(data []byte) error {
	if len(data) > walMaxDataSize {
		return ErrRecordInvalidSize
	}
	return nil
}

func encodedRecordLen(data []byte) int {
	return encodedRecordSize(len(data))
}

func encodedRecordSize(size int) int {
	return alignUp8(walRecordHeaderLen + walRecordMetaLen + size)
}

func alignUp8(n int) int {
	return (n + 7) &^ 7
}
