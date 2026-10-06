package sst

/*

SST FILE FOOTER
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
type		4		QDAT / QIDX / QFLT magic
version		2		SST Format Version
createdAt	8		SST creation time as Unix timestamp
crc			4		CRC32C of all preceding footer fields
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
	sstFooterLen = 4 + 2 + 8 + 4
)

const (
	sstTypeData   sstType = 'Q'<<24 | 'D'<<16 | 'A'<<8 | 'T'
	sstTypeIndex  sstType = 'Q'<<24 | 'I'<<16 | 'D'<<8 | 'X'
	sstTypeFilter sstType = 'Q'<<24 | 'F'<<16 | 'L'<<8 | 'T'
)

type sstType uint32

type sstFooter struct {
	stype     sstType
	version   uint16
	createdAt time.Time
}

func appendFooter(dst []byte, sf sstFooter) []byte {
	start := len(dst)
	dst = binary.BigEndian.AppendUint32(dst, uint32(sf.stype))
	dst = binary.LittleEndian.AppendUint16(dst, sf.version)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(sf.createdAt.UTC().UnixMilli()))
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], crc32Table))
}

// decodes the footer from the last sstFooterLen bytes of file
func decodeFooter(file []byte, stype sstType) (sstFooter, error) {
	var sf sstFooter

	if len(file) < sstFooterLen {
		return sf, fmt.Errorf("sst footer: insufficient bytes to decode")
	}

	buf := file[len(file)-sstFooterLen:]
	decoder := codec.NewDecoder(buf)
	st := decoder.Uint32BE("footer.type")
	version := decoder.Uint16("footer.version")
	createdAt := decoder.Uint64("footer.createdAt")
	crcOff := decoder.Offset()
	crc := decoder.Uint32("footer.crc")

	if err := decoder.Err(); err != nil {
		return sf, err
	}

	if st != uint32(stype) {
		return sf, ErrInvalidFormat
	}

	if version != sstVersion {
		return sf, ErrUnsupportedVersion
	}

	if crc != crc32.Checksum(buf[:crcOff], crc32Table) {
		return sf, ErrChecksumMismatch
	}

	sf.stype = sstType(st)
	sf.version = version
	sf.createdAt = time.UnixMilli(int64(createdAt))
	return sf, nil
}

func writeFooter(w io.Writer, sf sstFooter) (int, error) {
	var buf [sstFooterLen]byte
	b := appendFooter(buf[:0], sf)
	n, err := w.Write(b)
	if err != nil {
		return 0, err
	}
	if n != len(b) {
		return 0, io.ErrShortWrite
	}
	return n, nil
}

func readFooter(r io.ReaderAt, size int64, stype sstType) (sstFooter, error) {
	if size < sstFooterLen {
		return sstFooter{}, fmt.Errorf("sst footer: insufficient bytes to decode")
	}
	var buf [sstFooterLen]byte
	if _, err := r.ReadAt(buf[:], size-sstFooterLen); err != nil {
		return sstFooter{}, err
	}
	return decodeFooter(buf[:], stype)
}
