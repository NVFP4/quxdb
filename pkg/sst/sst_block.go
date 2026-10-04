package sst

/*

SST BLOCK RECORD
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
lenKey		var		Key size in bytes
lenVal		var		Value size in bytes
key			var		Key bytes
val			var		Value bytes
------------------------------------------------------------------

SST BLOCK
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
magic		4		QBLK
blockLen	4		Total bytes of the whole block (till crc)
recLen		4		Total bytes of records
records[]	var		Contiguous bytes of SST Block records
index		var		SST Block Index (see sst_block_index.go)
crc			4		CRC32C of all preceding fields
padding 	var 	Zero padding to the next 8-byte boundary
------------------------------------------------------------------

SST BLOCK DATA FILE (.qdat)
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
header		18		QDAT File Header (see sst_header.go)
blocks[]	var		SST Blocks
------------------------------------------------------------------

All fixed-size int fields are stored in LE byte-order, except for `magic`

*/

import (
	"bytes"
	"fmt"

	"github.com/yashgorana/quxdb/pkg/core"
	"github.com/yashgorana/quxdb/pkg/fs"
)

type recordView struct {
	key   []byte
	value []byte
	size  int
}

type Block struct {
	data   []byte // raw contiguous byes
	recLen int
	index  BlockIndex
}

func newBlock(indexCap int) Block {
	return Block{
		index: newBlockIndex(indexCap),
	}
}

func (b *Block) clear() {
	b.index.clear()
}

func (b *Block) Seek(target []byte) (key []byte, value []byte, ok bool, err error) {
	entry, found := b.index.Search(target)
	if !found {
		return nil, nil, false, nil
	}
	return b.scan(target, entry.offset, uint32(b.recLen))
}

func (b *Block) scan(target []byte, start uint32, limit uint32) ([]byte, []byte, bool, error) {
	for offset := start; offset < limit; {
		rec, err := b.recordAt(offset)
		if err != nil {
			return nil, nil, false, fmt.Errorf("block record at %d: %w", offset, err)
		}

		if bytes.Compare(rec.key, target) >= 0 {
			return rec.key, rec.value, true, nil
		}

		offset += uint32(rec.size)
	}

	return nil, nil, false, nil
}

func (b *Block) recordAt(offset uint32) (recordView, error) {
	off := int(offset)
	return decodeBlockRecord(b.data[off:])
}

func (b *Block) Cursor(start, end []byte) core.Cursor {
	c := blockCursor{}
	c.reset(b, start, end)
	return &c
}

// ------ block io ------

type MappedBlockData struct {
	mmap []byte
}

func (bd *MappedBlockData) BlockAt(span Span) (Block, error) {
	if span.Offset > len(bd.mmap) || span.Offset+span.Size > len(bd.mmap) {
		return Block{}, fmt.Errorf("%w: block span out of bounds", ErrCorrupt)
	}
	block, _, err := decodeBlock(bd.mmap[span.Offset : span.Offset+span.Size])
	return block, err
}

func (bd *MappedBlockData) Close() error {
	return fs.Unmap(bd.mmap)
}

func OpenBlockData(path string) (*MappedBlockData, error) {
	mmapBytes, err := fs.MapFile(path, fs.AdviceRandom)
	if err != nil {
		return nil, err
	}

	size := len(mmapBytes)
	_, _, err = decodeHeader(mmapBytes[max(size-sstHeaderLen, 0):], sstTypeData)
	if err != nil {
		_ = fs.Unmap(mmapBytes)
		return nil, fmt.Errorf("block data decode %w", err)
	}

	return &MappedBlockData{mmapBytes}, nil
}
