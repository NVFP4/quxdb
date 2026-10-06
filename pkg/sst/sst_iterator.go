package sst

import (
	"bytes"

	"github.com/yashgorana/quxdb/pkg/core"
)

type sstIterator struct {
	blocks *MappedBlockData
	index  *SparseIndex

	// block ordinals, endBlock is exclusive
	firstBlock int
	nextBlock  int
	endBlock   int

	startKey []byte
	endKey   []byte

	cur  blockIterator
	done bool
	err  error
}

func newSSTIterator(
	blocks *MappedBlockData,
	index *SparseIndex,
	startKey, endKey []byte,
) *sstIterator {
	numBlocks := index.Len()

	firstBlock := 0
	if startKey != nil {
		firstBlock = index.SearchSpanIndex(startKey)
	}

	endBlock := numBlocks
	if endKey != nil {
		lastBlock := index.SearchSpanIndex(endKey)
		if lastBlock < numBlocks {
			endBlock = lastBlock + 1
		}
	}

	empty := firstBlock >= endBlock ||
		startKey != nil && endKey != nil && bytes.Compare(startKey, endKey) > 0

	return &sstIterator{
		blocks:     blocks,
		index:      index,
		firstBlock: firstBlock,
		nextBlock:  firstBlock,
		endBlock:   endBlock,
		startKey:   startKey,
		endKey:     endKey,
		done:       empty,
	}
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

		if !c.advanceBlock() {
			break
		}
	}

	return nil, nil, false
}

func (c *sstIterator) Err() error {
	return c.err
}

func (c *sstIterator) advanceBlock() bool {
	if c.nextBlock >= c.endBlock {
		c.done = true
		return false
	}

	blockIdx := c.nextBlock
	c.nextBlock++

	span, ok := c.index.SpanAt(blockIdx)
	if !ok {
		c.err = ErrCorrupt
		c.done = true
		return false
	}

	block, err := c.blocks.BlockAt(span)
	if err != nil {
		c.err = err
		c.done = true
		return false
	}

	// only the first and last blocks need key bounds
	var lowerKey, upperKey []byte
	if blockIdx == c.firstBlock {
		lowerKey = c.startKey
	}
	if blockIdx+1 == c.endBlock {
		upperKey = c.endKey
	}

	c.cur.reset(&block, lowerKey, upperKey)
	return true
}

var _ core.Iterator = (*sstIterator)(nil)
