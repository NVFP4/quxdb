package memtable

import (
	"bytes"
	"errors"

	"github.com/yashgorana/quxdb/pkg/core"
)

// MemTableType selects a memtable implementation.
type MemTableType int

const (
	Skiplist MemTableType = 1 << iota
	BTree
)

var (
	// ErrMemtableFull reports a write that doesn't fit in the capacity.
	ErrMemtableFull = errors.New("memtable full")
	// ErrKeyTooLarge reports a key longer than a memtable stores.
	ErrKeyTooLarge = errors.New("memtable: key exceeds 65535 bytes")
)

// Memtable is a sorted in-memory key-value store, safe for concurrent calls.
type Memtable interface {
	// Get returns key's value, borrowed and stable until Clear.
	Get(key []byte) (value []byte, ok bool)

	// Seek returns the first pair with a key >= key, borrowed and stable until Clear.
	Seek(key []byte) (foundKey []byte, value []byte, ok bool)

	// Set inserts key or replaces its value.
	Set(key, value []byte) error

	// SizeBytes returns the live key and value bytes, without index overhead or replaced values.
	SizeBytes() int

	// Len returns the number of keys.
	Len() int

	// Clear drops every entry and invalidates borrowed views and iterators.
	Clear()

	// Iterator returns an iterator over [start, end] that may see later writes, nil bounds are open.
	// end is borrowed until the iterator is exhausted.
	Iterator(start, end []byte) core.Iterator
}

// ----------------------------------------------------------------------------

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

// ----------------------------------------------------------------------------

// Option configures a memtable.
type Option func(*Options)

// Comparator orders keys like bytes.Compare, it must be consistent, concurrency safe and read-only.
type Comparator func(a, b []byte) int

// Options holds memtable settings.
type Options struct {
	Comparator    Comparator
	CapacityBytes int
}

// WithComparator sets the key order, nil panics.
func WithComparator(cmp Comparator) Option {
	return func(opts *Options) {
		opts.Comparator = cmp
	}
}

// WithCapacityBytes caps retained bytes, keys and values plus index overhead, negative panics.
func WithCapacityBytes(capacity int) Option {
	return func(opts *Options) {
		opts.CapacityBytes = capacity
	}
}

func defaultOptions() Options {
	return Options{
		Comparator:    bytes.Compare,
		CapacityBytes: 16 << 20,
	}
}

func makeOptions(opts ...Option) Options {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.Comparator == nil {
		panic("memtable: nil comparator")
	}
	if cfg.CapacityBytes < 0 {
		panic("memtable: negative capacity")
	}
	return cfg
}

// New constructs a memtable of the requested type.
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
