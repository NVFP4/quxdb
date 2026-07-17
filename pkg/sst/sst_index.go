package sst

/*

SST INDEX ENTRY
------------------------------------------------------------------
keyLen		var		Key size in bytes
key			var		Separator Key bytes
spanOff		var		Indexed span offset
spanSize	var		Indexed span size in bytes
------------------------------------------------------------------

SST INDEX
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
entries		var		Number of entries in the index
entry[]		var		SST Index entries
crc			4		CRC32C of all preceding fields
------------------------------------------------------------------

SST INDEX FILE (.qidx)
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
header		24		QIDX File Header (see sst_header.go)
index		var		SST Index Data
------------------------------------------------------------------

All fixed-size int fields are stored in LE byte-order, except for `magic`

*/

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/yashgorana/quxdb/pkg/codec"
	"github.com/yashgorana/quxdb/pkg/fs"
)

const (
	indexBufCap = 256 << 10 // 256KiB buffer
)

type sparseIndexEntry struct {
	// fence or separator. never stores an owned slice.
	sepKey []byte
	// span of block
	span Span
}

type SparseIndex struct {
	entries []sparseIndexEntry
}

func newSparseIndex(cap int) SparseIndex {
	return SparseIndex{
		entries: make([]sparseIndexEntry, 0, cap),
	}
}

func (si *SparseIndex) Len() int {
	return len(si.entries)
}

func (si *SparseIndex) SearchSpan(key []byte) (Span, bool) {
	return si.SpanAt(si.SearchSpanIndex(key))
}

func (si *SparseIndex) SearchSpanIndex(key []byte) int {
	lo, hi := 0, len(si.entries)

	// find first entry such that key <= separator
	for lo < hi {
		mid := lo + ((hi - lo) >> 1)
		cmp := bytes.Compare(si.entries[mid].sepKey, key)
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

func (si *SparseIndex) SpanAt(i int) (Span, bool) {
	if i < 0 || i >= len(si.entries) {
		return Span{}, false
	}
	return si.entries[i].span, true
}

// expects a valid `minKey` > `maxKey` & `span`
func (si *SparseIndex) add(minKey, maxKey []byte, span Span) error {
	// not gonna compare minKey > maxKey - wasted compute on an obvious precondition
	if len(minKey) == 0 || len(maxKey) == 0 || span.Size == 0 {
		return fmt.Errorf("sparse index: invalid keys to add: min=%v max=%v span=%v", minKey, maxKey, span)
	}

	// shorten last maxKey
	if en := len(si.entries); en > 0 {
		last := &si.entries[en-1]

		// separator must lastMax <= separator < thisMin
		if bytes.Compare(minKey, last.sepKey) <= 0 {
			return fmt.Errorf("sparse index: minKey(%v) must be greater than previous upperBoundKey(%v)", minKey, last.sepKey)
		}

		buf := make([]byte, max(len(last.sepKey), len(minKey)))
		n := shortestSeparator(last.sepKey, minKey, buf)
		if n > 0 {
			last.sepKey = buf[:n]
		}
	}

	si.entries = append(si.entries, sparseIndexEntry{
		span:   span,
		sepKey: maxKey,
	})

	return nil
}

func (si *SparseIndex) clear() {
	clear(si.entries)
	si.entries = si.entries[:0]
}

// sparse index, but retains it's underlying slice that the index references
type MappedSparseIndex struct {
	SparseIndex
	mmap []byte
}

func (mi *MappedSparseIndex) Close() error {
	err := fs.Munmap(mi.mmap)
	mi.SparseIndex.clear()
	mi.mmap = nil
	return err
}

// use `start` if this returns -1
func shortestSeparator(start, limit, dst []byte) int {
	ls := len(start)
	n := min(len(limit), ls)

	i := 0
	for i < n && start[i] == limit[i] {
		i++
	}

	// start is a prefix of limit, so start itself is already shortest.
	if i == ls {
		return -1
	}

	max := ls - 1

	s := start[i]
	lim := limit[i]
	next := s + 1

	if next > s && i <= max && (next < lim || next == lim && i+1 < len(limit)) {
		dst = dst[:i+1]
		copy(dst, start[:i])
		dst[i] = next
		return i + 1
	}

	// Keep start[i], which is already < limit[i], and increment the first
	// later byte that can be incremented.
	for j := i + 1; j <= max; j++ {
		s = start[j]
		if s != 0xff {
			dst = dst[:j+1]
			copy(dst, start[:j])
			dst[j] = s + 1
			return j + 1
		}
	}

	return -1
}

// ------ sparse index codec ------

func encodeSparseIndex(dst []byte, si *SparseIndex) []byte {
	start := len(dst)
	// entries count
	dst = binary.AppendUvarint(dst, uint64(len(si.entries)))
	// entries
	for _, idx := range si.entries {
		// key len
		dst = binary.AppendUvarint(dst, uint64(len(idx.sepKey)))
		// key
		dst = append(dst, idx.sepKey...)
		// file span of the block
		dst = binary.AppendUvarint(dst, uint64(idx.span.Offset))
		dst = binary.AppendUvarint(dst, uint64(idx.span.Size))
	}
	// crc
	crc := crc32.Checksum(dst[start:], crc32Table)
	dst = binary.LittleEndian.AppendUint32(dst, crc)
	return dst
}

func decodeSparseIndex(src []byte) (SparseIndex, int, error) {
	var si SparseIndex
	decoder := codec.NewDecoder(src)

	// verify the whole payload
	end := len(src)
	crcOff := end - 4
	crc := decoder.Uint32At("crc", crcOff)
	if crc != crc32.Checksum(src[0:crcOff], crc32Table) {
		return si, 0, fmt.Errorf("checksum mismatch")
	}

	// load up entries
	ne := decoder.UVarint("entries")
	entries := make([]sparseIndexEntry, ne)
	for i := range ne {
		entry := sparseIndexEntry{}
		len := decoder.UVarint("sparseIndex.keyLen")
		entry.sepKey = decoder.Bytes("sparseIndex.key", int(len))
		entry.span.Offset = int(decoder.UVarint("sparseIndex.spanOff"))
		entry.span.Size = int(decoder.UVarint("sparseIndex.spanSize"))
		entries[i] = entry
	}

	// return any decoder error
	err := decoder.Err()
	if err != nil {
		return si, 0, err
	}

	si.entries = entries
	return si, decoder.Offset(), nil
}

// ------ sparse index io ------

func WriteSparseIndex(w io.Writer, idx *SparseIndex) (int, error) {
	// write metadata header
	h := sstHeader{sstTypeIndex, sstVersion, time.Now()}
	hn, err := writeHeader(w, h)
	if err != nil {
		return 0, fmt.Errorf("sparse index write %w", err)
	}

	// write data
	buf := make([]byte, 0, indexBufCap)
	buf = encodeSparseIndex(buf, idx)
	dn, err := w.Write(buf)
	if err != nil {
		return 0, fmt.Errorf("sparse index write %w", err)
	}

	return hn + dn, nil
}

func OpenSparseIndex(path string) (*MappedSparseIndex, error) {
	mmapBytes, err := mmapRead(path, fs.MADV_WILLNEED)
	if err != nil {
		return nil, err
	}

	// decoded header
	_, n, err := decodeHeader(mmapBytes[0:], sstTypeIndex)
	if err != nil {
		_ = fs.Munmap(mmapBytes)
		return nil, fmt.Errorf("sparse index decode %w", err)
	}

	// decode payload
	idx, _, err := decodeSparseIndex(mmapBytes[n:])
	if err != nil {
		_ = fs.Munmap(mmapBytes)
		return nil, fmt.Errorf("sparse index decode %w", err)
	}

	return &MappedSparseIndex{
		SparseIndex: idx,
		mmap:        mmapBytes,
	}, nil
}
