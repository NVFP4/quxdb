package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

/*
WAL Record
magic32 crc32 len64 recType16 flags16 lsn64 plen64 <data..> <padding>

crc32       CRC32C(len64 | recType16 | flags16 | lsn64 | plen64 | data)
recType16   record type (commit, ...)
flags16     record flags (compression, ...)
lsn64       LSN
plen        data length
data        data stream
*/

const (
	// magic32 crc32 len64 recType16 flags16 lsn64 plen64
	walRecordHeaderLen = 2*2 + 4*2 + 8*3

	// Record magic ends with 0xFF
	walRecordMagic32 uint32 = 'Q'<<24 | 'U'<<16 | 'X'<<8 | 0xFF
)

const (
	walRecTypeFull = 1 << iota // full key-value
)

var (
	ErrRecordWrite         = fmt.Errorf("record write")
	ErrRecordRead          = fmt.Errorf("record read")
	ErrRecordInvalidFormat = fmt.Errorf("%w: invalid format", ErrRecordRead)
	ErrRecordTorn          = fmt.Errorf("%w: record torn", ErrRecordRead)
	ErrRecordCorrupt       = fmt.Errorf("%w: corrupted - crc32 mismatch", ErrRecordRead)
	ErrIncompleteRead      = fmt.Errorf("%w: incomplete record read", ErrRecordRead)
)

type walRecordHeader struct {
	lsn     uint64
	dataLen uint64
	crc32c  uint32
	recType uint16
	flags   uint16
}

type walRecord struct {
	walRecordHeader
	data []byte
}

func readRecordSize(file io.ReaderAt, off int64) (int64, error) {
	// magic32 + crc32c + recordLen64
	var hBuff [4 + 4 + 8]byte
	hOff := 0

	_, err := file.ReadAt(hBuff[:], off)
	if err != nil {
		return -1, fmt.Errorf("%w: %w", ErrRecordRead, err)
	}

	magic := binary.BigEndian.Uint32(hBuff[hOff:])
	if magic != walRecordMagic32 {
		return -1, ErrRecordInvalidFormat
	}
	hOff += 4

	_ = binary.LittleEndian.Uint32(hBuff[hOff:])
	hOff += 4

	// record len
	len := binary.LittleEndian.Uint64(hBuff[hOff:])
	hOff += 8

	return off + int64(len), nil
}

func readRecord(file io.ReaderAt, off int64) (*walRecord, int, error) {
	rec := &walRecord{}

	var hBuff [walRecordHeaderLen]byte
	hOff := 0

	_, err := file.ReadAt(hBuff[:], off)
	if err != nil {
		// this EOF is fine, no more headers to read.
		return nil, -1, fmt.Errorf("%w: %w", ErrRecordRead, err)
	}

	magic := binary.BigEndian.Uint32(hBuff[hOff:])
	if magic != walRecordMagic32 {
		return nil, -1, ErrRecordInvalidFormat
	}
	hOff += 4

	rec.crc32c = binary.LittleEndian.Uint32(hBuff[hOff:])
	hOff += 4
	crcOff := hOff

	// record len
	_ = binary.LittleEndian.Uint64(hBuff[hOff:])
	hOff += 8

	rec.recType = binary.LittleEndian.Uint16(hBuff[hOff:])
	hOff += 2

	rec.flags = binary.LittleEndian.Uint16(hBuff[hOff:])
	hOff += 2

	rec.lsn = binary.LittleEndian.Uint64(hBuff[hOff:])
	hOff += 8

	rec.dataLen = binary.LittleEndian.Uint64(hBuff[hOff:])
	hOff += 8

	// partialCRC = CRC32C(recType16 | flags16 | lsn64 | plen64)
	expectedCRC := crc32.Checksum(hBuff[crcOff:hOff], walCRC32CTable)

	// now we read the data
	rec.data = make([]byte, uint(rec.dataLen))
	_, err = file.ReadAt(rec.data, int64(off)+int64(hOff))
	if err != nil {
		// eof here is bad. because we had data.
		if errors.Is(err, io.EOF) {
			return nil, -1, ErrRecordTorn
		}
		return nil, -1, fmt.Errorf("%w: %w at offset %d", ErrRecordRead, err, off)
	}

	// expectedCRC = CRC32C(partialCRC | plen64)
	expectedCRC = crc32.Update(expectedCRC, walCRC32CTable, rec.data)
	if rec.crc32c != expectedCRC {
		return nil, -1, ErrRecordCorrupt
	}

	return rec, alignUp8(walRecordHeaderLen + int(rec.dataLen)), nil
}

func encodeRecord(dest []byte, lsn LSN, flags uint16, data []byte) (int, error) {
	bufOff := 0
	dataLen := len(data)
	totalBytes := alignUp8(walRecordHeaderLen + dataLen)

	binary.BigEndian.PutUint32(dest[bufOff:], walRecordMagic32)
	bufOff += 4

	crcOff := bufOff
	bufOff += 4 // reserve crc32 slot
	crcStart := bufOff

	binary.LittleEndian.PutUint64(dest[bufOff:], uint64(totalBytes))
	bufOff += 8

	binary.LittleEndian.PutUint16(dest[bufOff:], walRecTypeFull)
	bufOff += 2

	binary.LittleEndian.PutUint16(dest[bufOff:], flags)
	bufOff += 2

	binary.LittleEndian.PutUint64(dest[bufOff:], uint64(lsn))
	bufOff += 8

	binary.LittleEndian.PutUint64(dest[bufOff:], uint64(dataLen))
	bufOff += 8

	// inline-write data
	copy(dest[bufOff:bufOff+dataLen], data)
	bufOff += dataLen

	// CRC32C(recType16 | flags16 | lsn64 | plen64 | data)
	crc := crc32.Checksum(dest[crcStart:bufOff], walCRC32CTable)
	binary.LittleEndian.PutUint32(dest[crcOff:], crc)

	// clear pad the rest
	clear(dest[bufOff:totalBytes])

	return totalBytes, nil
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
