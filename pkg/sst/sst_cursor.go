package sst

import (
	"bytes"

	"github.com/yashgorana/quxdb/pkg/core"
)

type sstCursor struct {
	data  *MappedBlockData
	index *SparseIndex

	first int
	next  int
	stop  int

	start []byte
	end   []byte

	block blockCursor
	done  bool
	err   error
}

func newSSTCursor(
	data *MappedBlockData,
	index *SparseIndex,
	start, end []byte,
) *sstCursor {
	n := index.Len()

	first := 0
	if start != nil {
		first = index.SearchSpanIndex(start)
	}

	stop := n
	if end != nil {
		last := index.SearchSpanIndex(end)
		if last < n {
			stop = last + 1
		}
	}

	empty := first >= stop ||
		start != nil && end != nil && bytes.Compare(start, end) > 0

	return &sstCursor{
		data:  data,
		index: index,
		first: first,
		next:  first,
		stop:  stop,
		start: start,
		end:   end,
		done:  empty,
	}
}

func (c *sstCursor) Next() (key, value []byte, ok bool) {
	for !c.done {
		if key, value, ok = c.block.Next(); ok {
			return key, value, true
		}

		if err := c.block.Err(); err != nil {
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

func (c *sstCursor) Err() error {
	return c.err
}

func (c *sstCursor) advanceBlock() bool {
	if c.next >= c.stop {
		c.done = true
		return false
	}

	ordinal := c.next
	c.next++

	span, ok := c.index.SpanAt(ordinal)
	if !ok {
		c.err = ErrCorrupt
		c.done = true
		return false
	}

	block, err := c.data.BlockAt(span)
	if err != nil {
		c.err = err
		c.done = true
		return false
	}

	var lower, upper []byte
	if ordinal == c.first {
		lower = c.start
	}
	if ordinal+1 == c.stop {
		upper = c.end
	}

	c.block.reset(&block, lower, upper)
	return true
}

var _ core.Cursor = (*sstCursor)(nil)
