package db

import (
	"bytes"
	"iter"

	"github.com/yashgorana/quxdb/pkg/core"
)

type mergeCursor struct {
	cur   core.Cursor
	qkey  quxKey
	value []byte
	order int
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

func newMergeHeap[T core.Cursor](cursors []T) (mergeHeap, error) {
	h := make(mergeHeap, 0, len(cursors))
	for i, cur := range cursors {
		mc := &mergeCursor{cur: cur, order: i}
		if mc.advance() {
			h.push(mc)
			continue
		}
		if err := cur.Err(); err != nil {
			return nil, err
		}
	}
	return h, nil
}

func (h *mergeHeap) push(cursor *mergeCursor) {
	*h = append(*h, cursor)

	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if !mergeLess((*h)[child], (*h)[parent]) {
			return
		}
		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		child = parent
	}
}

func (h *mergeHeap) advanceRoot() error {
	root := (*h)[0]
	if root.advance() {
		h.siftDown()
		return nil
	}

	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]

	if len(*h) > 0 {
		h.siftDown()
	}
	return root.cur.Err()
}

func (h *mergeHeap) siftDown() {
	for parent := 0; ; {
		left := 2*parent + 1
		if left >= len(*h) {
			break
		}

		child := left
		right := left + 1
		if right < len(*h) && mergeLess((*h)[right], (*h)[left]) {
			child = right
		}
		if !mergeLess((*h)[child], (*h)[parent]) {
			break
		}

		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		parent = child
	}
}

func mergeLess(a, b *mergeCursor) bool {
	cmp := bytes.Compare(a.qkey, b.qkey)
	return cmp < 0 || cmp == 0 && a.order < b.order
}

// mergeIter performs k-way merge of cursors by qkey.
func mergeIter[T core.Cursor](cursors []T) iter.Seq2[quxKey, []byte] {
	return func(yield func(quxKey, []byte) bool) {
		h, err := newMergeHeap(cursors)
		if err != nil {
			return
		}

		for len(h) > 0 {
			cursor := h[0]
			if !yield(cursor.qkey, cursor.value) {
				return
			}
			if err := h.advanceRoot(); err != nil {
				return
			}
		}
	}
}
