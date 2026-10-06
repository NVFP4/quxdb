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
padding 	var 	Zero padding to the next page boundary,
                    absent after the last block
------------------------------------------------------------------

SST BLOCK DATA FILE (.qdat)
------------------------------------------------------------------
Field		Bytes	Description
------------------------------------------------------------------
blocks[]	var		SST Blocks
footer		18		QDAT File Footer (see sst_footer.go)
------------------------------------------------------------------

All fixed-size int fields are stored in LE byte-order, except for `magic`

*/

import (
	"fmt"

	"github.com/yashgorana/quxdb/pkg/core"
	"github.com/yashgorana/quxdb/pkg/fs"
)

type recordView struct {
	key   []byte
	value []byte
	size  int
}

// Block is a read-only view of an encoded block.
type Block struct {
	recLen   int
	data     []byte // raw block bytes (mmap ref)
	rawIndex []byte // encoded block index (mmap ref)
}

// Seek returns the first entry at or after target within the block.
func (b *Block) Seek(target []byte) (key, value []byte, ok bool, err error) {
	var it blockIterator
	it.reset(b, target, nil)
	key, value, ok = it.Next()
	return key, value, ok, it.Err()
}

func (b *Block) Iterator(start, end []byte) core.Iterator {
	c := blockIterator{}
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

	if _, err = decodeFooter(mmapBytes, sstTypeData); err != nil {
		_ = fs.Unmap(mmapBytes)
		return nil, fmt.Errorf("block data decode %w", err)
	}

	return &MappedBlockData{mmapBytes}, nil
}
