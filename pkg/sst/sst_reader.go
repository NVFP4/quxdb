package sst

import "github.com/yashgorana/quxdb/pkg/core"

type Reader struct {
	store *Store
	table *tableHandle
}

func (r *Reader) Lookup(filterKey, orderedKey []byte) (key, value []byte, found bool) {
	if !r.MayContain(filterKey) {
		return nil, nil, false
	}

	return r.Seek(orderedKey)
}

func (r *Reader) Seek(orderedKey []byte) (key, value []byte, found bool) {
	span, ok := r.table.sst.index.SearchSpan(orderedKey)
	if !ok {
		return nil, nil, false
	}

	block, err := r.table.sst.data.BlockAt(span)
	if err != nil {
		return nil, nil, false
	}

	return block.Seek(orderedKey)
}

func (r *Reader) MayContain(filterKey []byte) bool {
	return r.table.sst.filter.Contains(filterKey)
}

// Cursor must not outlive Reader
func (r *Reader) Cursor(start, end []byte) core.Cursor {
	return newSSTCursor(
		r.table.sst.data,
		&r.table.sst.index.SparseIndex,
		start, end,
	)
}

func (r *Reader) Close() error {
	return r.store.release(r.table)
}
