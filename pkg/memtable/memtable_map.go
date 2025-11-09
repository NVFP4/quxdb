package memtable

import (
	"slices"
	"sync"
)

type mapMemTable struct {
	sync.RWMutex
	data      map[string][]byte
	sizeBytes int64
}

var _ MemTable = (*mapMemTable)(nil)

func newMapMemTable() *mapMemTable {
	return &mapMemTable{
		data: make(map[string][]byte),
	}
}

func (m *mapMemTable) Get(key []byte) ([]byte, bool) {
	m.RLock()
	defer m.RUnlock()

	value, ok := m.data[string(key)]
	return value, ok
}

func (m *mapMemTable) Set(key []byte, value []byte) error {
	m.Lock()
	defer m.Unlock()

	kStr := string(key)
	if oldVal, ok := m.data[kStr]; ok {
		m.sizeBytes -= int64(len(key) + len(oldVal))
	}
	m.data[kStr] = value
	m.sizeBytes += int64(len(key) + len(value))
	return nil
}

func (m *mapMemTable) Delete(key []byte) error {
	m.Lock()
	defer m.Unlock()

	kStr := string(key)
	if oldVal, ok := m.data[kStr]; ok {
		m.sizeBytes -= int64(len(key) + len(oldVal))
		delete(m.data, kStr)
	}
	return nil
}

func (m *mapMemTable) SizeBytes() int {
	m.RLock()
	defer m.RUnlock()
	return int(m.sizeBytes)
}

func (m *mapMemTable) Len() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.data)
}

func (m *mapMemTable) All() MemTableIterator {
	return m.Range(nil, nil)
}

func (m *mapMemTable) From(key []byte) MemTableIterator {
	return m.Range(key, nil)
}

func (m *mapMemTable) Range(start, end []byte) MemTableIterator {
	return func(yield func([]byte, []byte) bool) {
		m.RLock()
		// collect keys
		keys := make([]string, 0, len(m.data))
		for k := range m.data {
			if (start == nil || k >= string(start)) && (end == nil || k <= string(end)) {
				keys = append(keys, k)
			}
		}
		m.RUnlock()

		// sort keys
		slices.Sort(keys)

		// yield key-value pairs
		for _, k := range keys {
			m.RLock()
			val, ok := m.data[k]
			m.RUnlock()
			if !ok {
				continue
			}
			if !yield([]byte(k), val) {
				return
			}
		}
	}
}
