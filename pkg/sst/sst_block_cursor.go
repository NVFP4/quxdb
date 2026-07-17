package sst

import (
	"bytes"

	"github.com/yashgorana/quxdb/pkg/core"
)

const (
	blockMagic32   uint32 = 'Q'<<24 | 'B'<<16 | 'L'<<8 | 'K'
	blockHeaderLen        = 4 + 4 + 4
)

// iterates within the block
type blockCursor struct {
	data  []byte
	off   int
	limit int
	start []byte
	end   []byte
	err   error
}

func (c *blockCursor) Next() (key, value []byte, ok bool) {
	for c.err == nil && c.off < c.limit {
		// Bound decoding to the records region. A malformed record must not
		// consume bytes from the block index.
		rec, err := decodeBlockRecord(c.data[c.off:c.limit])
		if err != nil {
			c.err = err
			c.off = c.limit
			return nil, nil, false
		}
		c.off += rec.size

		if c.start != nil {
			if bytes.Compare(rec.key, c.start) < 0 {
				continue
			}
			c.start = nil // remaining records are necessarily >= start
		}

		// Inclusive upper bound, matching memtables.
		if c.end != nil && bytes.Compare(rec.key, c.end) > 0 {
			c.off = c.limit
			return nil, nil, false
		}

		return rec.key, rec.value, true
	}

	return nil, nil, false
}

func (c *blockCursor) Err() error {
	return c.err
}

func (c *blockCursor) reset(b *Block, start, end []byte) {
	*c = blockCursor{
		data:  b.data,
		off:   blockHeaderLen,
		limit: blockHeaderLen + b.recLen,
		start: start,
		end:   end,
	}

	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		c.off = c.limit
		return
	}

	// Only used when the block has a lower bound.
	if start != nil {
		entry, ok := b.index.Search(start)
		if !ok {
			c.off = c.limit
			return
		}

		off := int(entry.offset)
		if off < blockHeaderLen || off >= c.limit {
			c.err = ErrCorrupt
			c.off = c.limit
			return
		}
		c.off = off
	}
}

var _ core.Cursor = (*blockCursor)(nil)
