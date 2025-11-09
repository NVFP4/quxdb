package memtable

import "iter"

type MemTableIterator = iter.Seq2[[]byte, []byte]

type MemTable interface {
	// Get the value for the key from the memtable
	Get(key []byte) (value []byte, ok bool)
	// Set the key-value pair in the memtable
	Set(key []byte, value []byte) error
	// Delete the key-value pair from the memtable
	Delete(key []byte) error
	// Returns the size of the memtable in bytes
	SizeBytes() int
	// Returns the number of key-value pairs in the memtable
	Len() int
	// Returns an iterator starting from a specific key
	From(key []byte) MemTableIterator
	// Returns an iterator for a range of key-value pairs between [start, end] in the memtable
	Range(start, end []byte) MemTableIterator
	// Returns an iterator for all key-value pairs in the memtable
	All() MemTableIterator
}

// ----------------------------------------------------------------------------

type MemTableType string

const (
	Map MemTableType = "map"
)

func New(t MemTableType) MemTable {
	switch t {
	case Map:
		return newMapMemTable()
	default:
		panic("invalid memtable type")
	}
}
