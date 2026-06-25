package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

/*
=============================================
WAL Record
=============================================
magic32		QREC
crc32		record crc
hcrc32		header crc
rlen32		record len
rtype16		record type (commit, ...)
rflags16	record flags (compression, ...)
lsn64		LSN
dlen32		data length
=============================================
data		data bytes
=============================================
<padding>
=============================================
*/

const (
	walRecordHeaderLen = 2*2 + 4*5 + 8

	walRecordMagic32 uint32 = 'Q'<<24 | 'R'<<16 | 'E'<<8 | 'C'
)

const (
	walRecTypeFull = 1 << iota // full key-value
)

var (
	ErrRecordWrite         = errors.New("record write")
	ErrRecordRead          = errors.New("record read")
	ErrRecordInvalidFormat = errors.New("record read: invalid format")
	ErrRecordInvalidSize   = errors.New("record read: invalid size")
	ErrRecordInvalidLSN    = errors.New("record read: lsn mismatch")
	ErrRecordTorn          = errors.New("record read: record torn")
	ErrRecordCorrupt       = errors.New("record read: corrupted - crc32 mismatch")
	ErrIncompleteRead      = errors.New("record read: incomplete record read")
)

type walRecordHeader struct {
	lsn       uint64
	recCRC    uint32
	headerCRC uint32
	recLen    uint32
	recType   uint16
	recFlags  uint16
	dataLen   uint32
}

type walRecord struct {
	walRecordHeader
	data []byte
}

func readRecordHeader(r io.ReaderAt, off int64, limit int64) (*walRecord, error) {
	rec := &walRecord{}
	var hBuff [walRecordHeaderLen]byte
	hOff := 0

	_, err := r.ReadAt(hBuff[:], off)
	if err != nil {
		// this EOF is fine, no more headers to read.
		return nil, fmt.Errorf("%w: %w", ErrRecordRead, err)
	}

	// magic32
	magic := binary.BigEndian.Uint32(hBuff[hOff:])
	if magic != walRecordMagic32 {
		return nil, ErrRecordInvalidFormat
	}
	hOff += 4

	// crc32
	rec.recCRC = binary.LittleEndian.Uint32(hBuff[hOff:])
	hOff += 4

	// hcrc32
	rec.headerCRC = binary.LittleEndian.Uint32(hBuff[hOff:])
	hOff += 4
	crcOff := hOff

	// rlen32
	rec.recLen = binary.LittleEndian.Uint32(hBuff[hOff:])
	hOff += 4

	// rtype16
	rec.recType = binary.LittleEndian.Uint16(hBuff[hOff:])
	hOff += 2

	// rflags16
	rec.recFlags = binary.LittleEndian.Uint16(hBuff[hOff:])
	hOff += 2

	// lsn64
	rec.lsn = binary.LittleEndian.Uint64(hBuff[hOff:])
	hOff += 8

	// dlen32
	rec.dataLen = binary.LittleEndian.Uint32(hBuff[hOff:])
	hOff += 4

	// validate bounds
	if rec.recLen < walRecordHeaderLen || rec.recLen%8 != 0 {
		return nil, ErrRecordTorn
	}
	if off < 0 || off+int64(rec.recLen) > limit {
		return nil, ErrRecordInvalidSize
	}
	if alignUp8(walRecordHeaderLen+int(rec.dataLen)) != int(rec.recLen) {
		return nil, ErrRecordInvalidSize
	}

	// CRC32C(rlen, rtype, rflags, lsn, dlen)
	expectedCRC := crc32.Checksum(hBuff[crcOff:hOff], walCRC32CTable)
	if rec.headerCRC != expectedCRC {
		return nil, ErrRecordCorrupt
	}
	return rec, nil
}

func readRecord(r io.ReaderAt, off int64, limit int64) (*walRecord, error) {
	rec, err := readRecordHeader(r, off, limit)
	if err != nil {
		return nil, err
	}

	// now we read the data
	rec.data = make([]byte, uint(rec.dataLen))
	_, err = r.ReadAt(rec.data, int64(off)+walRecordHeaderLen)
	if err != nil {
		// eof here is bad. because we had data.
		if errors.Is(err, io.EOF) {
			return nil, ErrRecordTorn
		}
		return nil, fmt.Errorf("%w: %w at offset %d", ErrRecordRead, err, off)
	}

	// CRC32C(... | data)
	if rec.recCRC != crc32.Update(rec.headerCRC, walCRC32CTable, rec.data) {
		return nil, ErrRecordCorrupt
	}

	return rec, nil
}

func encodeRecord(dest []byte, lsn LSN, flags uint16, data []byte) int {
	bufOff := 0
	dataLen := len(data)
	recLen := alignUp8(walRecordHeaderLen + dataLen)

	// magic32
	binary.BigEndian.PutUint32(dest[bufOff:], walRecordMagic32)
	bufOff += 4

	// crc32
	crcOff := bufOff
	bufOff += 4 // reserve crc32 slot

	// hcrc32
	hcrcOff := bufOff
	bufOff += 4 // reserve hcrc32 slot
	crcStart := bufOff

	// rlen32
	binary.LittleEndian.PutUint32(dest[bufOff:], uint32(recLen))
	bufOff += 4

	// rtype16
	binary.LittleEndian.PutUint16(dest[bufOff:], walRecTypeFull)
	bufOff += 2

	// rflags16
	binary.LittleEndian.PutUint16(dest[bufOff:], flags)
	bufOff += 2

	// lsn64
	binary.LittleEndian.PutUint64(dest[bufOff:], uint64(lsn))
	bufOff += 8

	// dlen32
	binary.LittleEndian.PutUint32(dest[bufOff:], uint32(dataLen))
	bufOff += 4

	// hcrc32
	hcrc := crc32.Checksum(dest[crcStart:bufOff], walCRC32CTable)
	binary.LittleEndian.PutUint32(dest[hcrcOff:], hcrc)

	// data
	copy(dest[bufOff:bufOff+dataLen], data)
	bufOff += dataLen

	// crc32
	crc := crc32.Checksum(dest[crcStart:bufOff], walCRC32CTable)
	binary.LittleEndian.PutUint32(dest[crcOff:], crc)

	// clear pad the rest
	clear(dest[bufOff:recLen])

	return recLen
}

func encodedBatchLen(batch [][]byte) int {
	total := 0
	for _, data := range batch {
		total += alignUp8(walRecordHeaderLen + len(data))
	}
	return total
}

func encodedRecordLen(data []byte) int {
	return alignUp8(walRecordHeaderLen + len(data))
}

func alignUp8(n int) int {
	return (n + 7) &^ 7
}
