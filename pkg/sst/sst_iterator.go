package sst

import (
	"github.com/yashgorana/quxdb/pkg/core"
)

type sstIterator struct {
	blocks *MappedBlockData
	index  *SparseIndex

	// block ordinals, endBlock is exclusive
	nextBlock int
	endBlock  int
	endKey    []byte

	cur  blockIterator
	done bool
	err  error
}

func newSSTIterator(blocks *MappedBlockData, index *SparseIndex, startKey, endKey []byte) *sstIterator {
	c := &sstIterator{}
	c.init(blocks, index, startKey, endKey)
	return c
}

// positions on the first block holding startKey, nil startKey is the first block
func (c *sstIterator) init(blocks *MappedBlockData, index *SparseIndex, startKey, endKey []byte) {
	*c = sstIterator{blocks: blocks, index: index, endBlock: index.Len(), endKey: endKey}
	if startKey != nil {
		c.nextBlock = index.SearchSpanIndex(startKey)
	}
	if endKey != nil {
		if last := index.SearchSpanIndex(endKey); last < c.endBlock {
			c.endBlock = last + 1
		}
	}
	c.advanceBlock(startKey)
}

func (c *sstIterator) Next() (key, value []byte, ok bool) {
	for !c.done {
		if key, value, ok = c.cur.Next(); ok {
			return key, value, true
		}

		if err := c.cur.Err(); err != nil {
			c.err = err
			c.done = true
			break
		}

		c.advanceBlock(nil)
	}

	return nil, nil, false
}

func (c *sstIterator) Err() error {
	return c.err
}

// loads the next block, startKey only bounds the first one
func (c *sstIterator) advanceBlock(startKey []byte) {
	if c.nextBlock >= c.endBlock {
		c.done = true
		return
	}

	blockIdx := c.nextBlock
	c.nextBlock++

	span, ok := c.index.SpanAt(blockIdx)
	if !ok {
		c.err = ErrCorrupt
		c.done = true
		return
	}

	block, err := c.blocks.BlockAt(span)
	if err != nil {
		c.err = err
		c.done = true
		return
	}

	// only the last block needs the upper bound
	var endKey []byte
	if blockIdx+1 == c.endBlock {
		endKey = c.endKey
	}
	c.cur.reset(&block, startKey, endKey)
}

var _ core.Iterator = (*sstIterator)(nil)
