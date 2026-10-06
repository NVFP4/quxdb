package sst

import (
	"bytes"

	"github.com/yashgorana/quxdb/pkg/core"
)

const (
	blockMagic32   uint32 = 'Q'<<24 | 'B'<<16 | 'L'<<8 | 'K'
	blockHeaderLen        = 4 + 4 + 4
	blockCRCLen           = 4
)

// iterates within the block
type blockIterator struct {
	data     []byte // raw block bytes, mmap backed
	recOff   int    // offset of the next record
	recEnd   int    // end of the records region
	startKey []byte
	endKey   []byte
	err      error
}

func (c *blockIterator) Next() (key, value []byte, ok bool) {
	for c.err == nil && c.recOff < c.recEnd {
		// Bound decoding to the records region. A malformed record must not
		// consume bytes from the block index.
		rec, err := decodeBlockRecord(c.data[c.recOff:c.recEnd])
		if err != nil {
			c.err = err
			c.recOff = c.recEnd
			return nil, nil, false
		}
		c.recOff += rec.size

		if c.startKey != nil {
			if bytes.Compare(rec.key, c.startKey) < 0 {
				continue
			}
			c.startKey = nil // remaining records are necessarily >= startKey
		}

		// Inclusive upper bound, matching memtables.
		if c.endKey != nil && bytes.Compare(rec.key, c.endKey) > 0 {
			c.recOff = c.recEnd
			return nil, nil, false
		}

		return rec.key, rec.value, true
	}

	return nil, nil, false
}

func (c *blockIterator) Err() error {
	return c.err
}

func (c *blockIterator) reset(block *Block, startKey, endKey []byte) {
	*c = blockIterator{
		data:     block.data,
		recOff:   blockHeaderLen,
		recEnd:   blockHeaderLen + block.recLen,
		startKey: startKey,
		endKey:   endKey,
	}

	// Only used when the block has a lower bound.
	if startKey != nil {
		seekOff, ok, err := searchBlockIndex(block.rawIndex, startKey)
		if err != nil || !ok {
			c.err = err
			c.recOff = c.recEnd
			return
		}

		if int(seekOff) < blockHeaderLen || int(seekOff) >= c.recEnd {
			c.err = ErrCorrupt
			c.recOff = c.recEnd
			return
		}
		c.recOff = int(seekOff)
	}
}

var _ core.Iterator = (*blockIterator)(nil)
