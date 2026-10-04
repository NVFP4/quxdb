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
nEntries	var		Number of entries in the index
entries[]	var		SST Block Index entry
indexSpan	var		Block span of the Block Index
*/

import (
	"bytes"
	"encoding/binary"
	"math/bits"

	"github.com/yashgorana/quxdb/pkg/codec"
)

const (
	blockIndexMagic32 uint32 = 'Q'<<24 | 'B'<<16 | 'I'<<8 | 'X'
	blockIndexHeader         = 8
)

type blockIndexEntry struct {
	key    []byte // inclusive upper fence key for the records
	offset uint32 // start offset of the records
}

// block index stores each indexed span’s first key
type BlockIndex struct {
	entries   []blockIndexEntry
	sizeBytes uint64
}

func newBlockIndex(cap int) BlockIndex {
	return BlockIndex{
		entries:   make([]blockIndexEntry, 0, cap),
		sizeBytes: blockIndexHeader,
	}
}

func (bi *BlockIndex) Len() int {
	return len(bi.entries)
}

func (bi *BlockIndex) SizeBytes() uint64 {
	return bi.sizeBytes
}

func (bi *BlockIndex) Get(idx int) *blockIndexEntry {
	if idx < 0 || idx >= len(bi.entries) {
		return nil
	}
	return &bi.entries[idx]
}

func (bi *BlockIndex) add(minKey, maxKey []byte, offset int) {
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

func (bi *BlockIndex) clear() {
	clear(bi.entries)
	bi.entries = bi.entries[:0]
	bi.sizeBytes = blockIndexHeader
}

func encodeBlockIndex(dst []byte, bi *BlockIndex) []byte {
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
