package sst

import "github.com/yashgorana/quxdb/pkg/core"

// Seek returns the entry at or after orderedKey within its indexed block.
func (s *SST) Seek(orderedKey []byte) (key, value []byte, found bool, err error) {
	span, ok := s.index.SearchSpan(orderedKey)
	if !ok {
		return nil, nil, false, nil
	}

	block, err := s.data.BlockAt(span)
	if err != nil {
		return nil, nil, false, err
	}

	return block.Seek(orderedKey)
}

// MayContain reports whether the bloom filter may hold filterKey.
func (s *SST) MayContain(filterKey []byte) bool {
	return s.filter.Contains(filterKey)
}

// Iterator iterates a key range and must not outlive the pinning View.
func (s *SST) Iterator(start, end []byte) core.Iterator {
	return newSSTIterator(
		s.data,
		&s.index.SparseIndex,
		start, end,
	)
}
