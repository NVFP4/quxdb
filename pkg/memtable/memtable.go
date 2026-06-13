package memtable

import (
	"bytes"
	"errors"
	"iter"
)

type Iterator = iter.Seq2[[]byte, []byte]

type Comparator func(a, b []byte) int

var ErrMemtableFull = errors.New("memtable full")

type Options struct {
	Comparator Comparator
}

type Option func(*Options)

func WithComparator(cmp Comparator) Option {
	return func(opts *Options) {
		opts.Comparator = cmp
	}
}

func defaultOptions() Options {
	return Options{Comparator: bytes.Compare}
}

type Memtable interface {
	// Get the value for the key from the memtable
	Get(key []byte) (value []byte, ok bool)
	// SeekGE returns the first key-value pair whose key is >= key.
	SeekGE(key []byte) (foundKey []byte, value []byte, ok bool)
	// Set the key-value pair in the memtable
	Set(key, value []byte) error
	// Delete the key-value pair from the memtable
	Delete(key []byte)
	// Returns the size of the memtable in bytes
	SizeBytes() int
	// Returns the number of key-value pairs in the memtable
	Len() int
	// Returns an iterator starting from a specific key
	IterFrom(key []byte) Iterator
	// Returns an iterator for a range of key-value pairs between [start, end] in the memtable
	IterRange(start, end []byte) Iterator
	// Returns an iterator for all key-value pairs in the memtable
	Iter() Iterator
}

// ----------------------------------------------------------------------------

type MemTableType int

const (
	Skiplist MemTableType = 1 << iota
	BTree
)

func (t MemTableType) String() string {
	switch t {
	case Skiplist:
		return "skiplist"
	case BTree:
		return "btree"
	default:
		return "unknown"
	}
}

func New(t MemTableType, opts ...Option) Memtable {
	switch t {
	case Skiplist:
		return newSkiplistMemtable(opts...)
	case BTree:
		return newBTreeMemtable(opts...)
	default:
		panic("invalid memtable type")
	}
}
