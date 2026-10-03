package sst

import "github.com/yashgorana/quxdb/pkg/core"

// Lookup seeks orderedKey if the filter may hold filterKey.
func (s *SST) Lookup(filterKey, orderedKey []byte) (key, value []byte, found bool) {
	if !s.MayContain(filterKey) {
		return nil, nil, false
	}

	return s.Seek(orderedKey)
}

// Seek returns the entry at or after orderedKey within its indexed block.
func (s *SST) Seek(orderedKey []byte) (key, value []byte, found bool) {
	span, ok := s.index.SearchSpan(orderedKey)
	if !ok {
		return nil, nil, false
	}

	block, err := s.data.BlockAt(span)
	if err != nil {
		return nil, nil, false
	}

	return block.Seek(orderedKey)
}

// MayContain reports whether the bloom filter may hold filterKey.
func (s *SST) MayContain(filterKey []byte) bool {
	return s.filter.Contains(filterKey)
}

// Cursor iterates a key range and must not outlive the pinning View.
func (s *SST) Cursor(start, end []byte) core.Cursor {
	return newSSTCursor(
		s.data,
		&s.index.SparseIndex,
		start, end,
	)
}
