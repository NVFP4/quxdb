package db

import (
	"bytes"
	"iter"
)

type mergeCursor struct {
	cur   *mvccCursor
	qkey  quxKey
	value []byte
}

func (c *mergeCursor) advance() bool {
	qk, value, ok := c.cur.Next()
	if !ok {
		c.qkey = nil
		c.value = nil
		return false
	}

	c.qkey = quxKey(qk)
	c.value = value
	return true
}

type mergeHeap []*mergeCursor

func (h *mergeHeap) push(cursor *mergeCursor) {
	*h = append(*h, cursor)

	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if bytes.Compare((*h)[parent].qkey, (*h)[child].qkey) <= 0 {
			return
		}
		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		child = parent
	}
}

func (h *mergeHeap) advanceRoot() {
	root := (*h)[0]
	if root.advance() {
		h.siftDown()
		return
	}

	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]

	if len(*h) > 0 {
		h.siftDown()
	}
}

func (h *mergeHeap) siftDown() {
	for parent := 0; ; {
		left := 2*parent + 1
		if left >= len(*h) {
			break
		}

		child := left
		right := left + 1
		if right < len(*h) && bytes.Compare((*h)[right].qkey, (*h)[left].qkey) < 0 {
			child = right
		}
		if bytes.Compare((*h)[parent].qkey, (*h)[child].qkey) <= 0 {
			break
		}

		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		parent = child
	}
}

// mergeIter performs k-way merge of cursors by qkey.
func mergeIter(cursors []*mvccCursor) iter.Seq2[quxKey, []byte] {
	return func(yield func(quxKey, []byte) bool) {
		h := make(mergeHeap, 0, len(cursors))
		for _, cur := range cursors {
			mc := &mergeCursor{cur: cur}
			if mc.advance() {
				h.push(mc)
			}
		}

		for len(h) > 0 {
			cursor := h[0]
			qkey := cursor.qkey
			val := cursor.value

			if !yield(qkey, val) {
				return
			}

			h.advanceRoot()
		}
	}
}
