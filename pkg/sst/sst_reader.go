package sst

import "github.com/yashgorana/quxdb/pkg/core"

// Seek returns the first entry at or after orderedKey.
func (s *SST) Seek(orderedKey []byte) (key, value []byte, found bool, err error) {
	var it sstIterator
	it.init(s.data, &s.index.SparseIndex, orderedKey, nil)
	key, value, found = it.Next()
	return key, value, found, it.Err()
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
