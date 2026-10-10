package quxdb

import (
	"encoding/binary"
	"iter"
	"math/bits"
	"slices"
)

const (
	batchRefValueMin = 4096    // values this long are referenced, not copied
	batchBufRetain   = 1 << 20 // reset drops a buffer grown past this
)

// batchEncoder encodes a commit batch as wal parts: a uvarint count, then each key and value.
type batchEncoder struct {
	buf         []byte
	parts       [][]byte
	partsRetain int // reset drops a parts slice grown past this
}

func newBatchEncoder(maxBatch int) batchEncoder {
	return batchEncoder{partsRetain: 4 * maxBatch}
}

// Encode returns the batch as parts, valid until Reset.
func (e *batchEncoder) Encode(reqs []*writeReq) [][]byte {
	n := sizeUvarint(len(reqs))
	for _, req := range reqs {
		n += inlineLen(req.qkey, req.val)
	}
	// grown once, parts slice into buf
	e.buf = slices.Grow(e.buf[:0], n)
	e.parts = e.parts[:0]

	start := 0
	e.buf = binary.AppendUvarint(e.buf, uint64(len(reqs)))
	for _, req := range reqs {
		e.buf = binary.AppendUvarint(e.buf, uint64(len(req.qkey)))
		e.buf = append(e.buf, req.qkey...)
		e.buf = binary.AppendUvarint(e.buf, uint64(len(req.val)))
		if len(req.val) < batchRefValueMin {
			e.buf = append(e.buf, req.val...)
			continue
		}
		e.parts = append(e.parts, e.buf[start:], req.val)
		start = len(e.buf)
	}
	if start < len(e.buf) {
		e.parts = append(e.parts, e.buf[start:])
	}
	return e.parts
}

// Reset releases the encoded batch and drops buffers a huge batch inflated.
func (e *batchEncoder) Reset() {
	clear(e.parts)
	e.parts = e.parts[:0]
	if cap(e.parts) > e.partsRetain {
		e.parts = nil
	}
	e.buf = e.buf[:0]
	if cap(e.buf) > batchBufRetain {
		e.buf = nil
	}
}

// inlineLen is the bytes a key and value copy into the buffer.
func inlineLen(qkey quxKey, val []byte) int {
	n := sizeUvarint(len(qkey)) + len(qkey) + sizeUvarint(len(val))
	if len(val) < batchRefValueMin {
		n += len(val)
	}
	return n
}

// decodeBatch yields each key and value of an encoded batch, both alias data.
func decodeBatch(data []byte) iter.Seq2[quxKey, []byte] {
	return func(yield func(quxKey, []byte) bool) {
		count, off := binary.Uvarint(data)
		for range count {
			qkey, n := decodeBytes(data[off:])
			off += n
			val, n := decodeBytes(data[off:])
			off += n
			if !yield(qkey, val) {
				return
			}
		}
	}
}

// decodeBytes reads a uvarint length prefixed byte string.
func decodeBytes(buf []byte) ([]byte, int) {
	l, n := binary.Uvarint(buf)
	end := n + int(l)
	return buf[n:end], end
}

func sizeUvarint(x int) int {
	return (bits.Len64(uint64(x)|1) + 6) / 7
}
