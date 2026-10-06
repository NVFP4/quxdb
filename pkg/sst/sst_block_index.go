package sst

/*

SST BLOCK INDEX ENTRY
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
keyLen		var		Length of the Key
key			var 	Upper bound separator key for the record
recOff		var		Start offset of the indexed record
------------------------------------------------------------------

SST BLOCK INDEX
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
magic		4		QBIX
nEntries	var		Number of entries in the index
entries[]	var		SST Block Index entry
------------------------------------------------------------------
*/

import (
	"bytes"
	"encoding/binary"
	"math/bits"

	"github.com/yashgorana/quxdb/pkg/codec"
)

const (
	blockIndexMagic32 uint32 = 'Q'<<24 | 'B'<<16 | 'I'<<8 | 'X'
	blockIndexHeader         = 6 // magic + uvarint entry count
)

type blockIndexEntry struct {
	key    []byte // upper bound of the span's keys
	offset uint32 // first record of the span
}

// maps each span's upper bound key to its first record offset
type blockIndex struct {
	entries   []blockIndexEntry
	sizeBytes uint64
}

func newBlockIndex(cap int) blockIndex {
	return blockIndex{
		entries:   make([]blockIndexEntry, 0, cap),
		sizeBytes: blockIndexHeader,
	}
}

func (bi *blockIndex) add(minKey, maxKey []byte, offset int) {
	if n := len(bi.entries); n > 0 {
		last := &bi.entries[n-1]

		if sep := separator(last.key, minKey); sep != nil {
			off := int(last.offset)
			bi.sizeBytes -= uint64(encodedBlockIndexEntryLen(last.key, off) - encodedBlockIndexEntryLen(sep, off))
			last.key = sep
		}
	}

	bi.entries = append(bi.entries, blockIndexEntry{maxKey, uint32(offset)})
	bi.sizeBytes += uint64(encodedBlockIndexEntryLen(maxKey, offset))
}

func encodedBlockIndexEntryLen(key []byte, offset int) int {
	return len(key) + uvarintSize(len(key)) + uvarintSize(offset)
}

func (bi *blockIndex) clear() {
	clear(bi.entries)
	bi.entries = bi.entries[:0]
	bi.sizeBytes = blockIndexHeader
}

func encodeBlockIndex(dst []byte, bi *blockIndex) []byte {
	// block index magic
	dst = binary.BigEndian.AppendUint32(dst, blockIndexMagic32)

	// number of entries
	dst = binary.AppendUvarint(dst, uint64(len(bi.entries)))

	// | keyLen | key | offset | ...
	for _, entry := range bi.entries {
		// key len
		dst = binary.AppendUvarint(dst, uint64(len(entry.key)))
		// key
		dst = append(dst, entry.key...)
		// record offset
		dst = binary.AppendUvarint(dst, uint64(entry.offset))
	}

	return dst
}

// returns the record offset of the first entry with key >= target
func searchBlockIndex(src, target []byte) (uint32, bool, error) {
	decoder := codec.NewDecoder(src)
	if decoder.Uint32BE("magic") != blockIndexMagic32 {
		return 0, false, ErrInvalidFormat
	}

	ne := decoder.UVarint("entries")
	for range ne {
		keyLen := decoder.UVarint("blockIndex.keyLen")
		key := decoder.Bytes("blockIndex.key", int(keyLen))
		off := uint32(decoder.UVarint("blockIndex.recOff"))
		if err := decoder.Err(); err != nil {
			return 0, false, err
		}
		if bytes.Compare(key, target) >= 0 {
			return off, true, nil
		}
	}
	return 0, false, decoder.Err()
}

func uvarintSize(x int) int {
	if x == 0 {
		return 1
	}
	// Calculate bits needed and divide by 7
	bits := bits.Len64(uint64(x))
	size := (bits + 6) / 7
	return size
}
