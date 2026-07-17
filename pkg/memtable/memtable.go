package memtable

import (
	"bytes"
	"errors"

	"github.com/yashgorana/quxdb/pkg/core"
)

type MemTableType int

const (
	Skiplist MemTableType = 1 << iota
	BTree
)

var ErrMemtableFull = errors.New("memtable full")

// Memtable implementations are safe for concurrent method calls. Borrowed
// results and individual Cursor values retain the narrower ownership and
// concurrency rules documented below.
type Memtable interface {
	// Get the value for the key from the memtable.
	// The result is a borrowed, immutable view with capacity equal to its
	// length. Clone it before mutation or retaining it across writes.
	Get(key []byte) (value []byte, ok bool)

	// Seek returns the first key-value pair whose key is >= key.
	// Results are borrowed, immutable views with capacity equal to their
	// length. Clone them before mutation or retaining them across writes.
	Seek(key []byte) (foundKey []byte, value []byte, ok bool)

	// Set the key-value pair in the memtable
	Set(key, value []byte) error

	// SizeBytes returns the live size of the key-value pairs in the memtable.
	// It excludes tree/skiplist metadata and obsolete arena bytes from updates.
	SizeBytes() int

	// Returns the number of key-value pairs in the memtable
	Len() int

	// Clear removes all entries and drops references to data and structural
	// allocations. The memtable remains reusable and allocates lazily on the
	// next Set. Clear invalidates borrowed views and cursors; callers must stop
	// using them before calling Clear. Reclamation is scheduled by the Go
	// runtime rather than forced synchronously.
	Clear()

	// Cursor returns a weakly consistent cursor over the inclusive [start, end]
	// range. A nil bound is unbounded. Bounds are borrowed and must remain
	// immutable until the cursor is exhausted.
	Cursor(start, end []byte) core.Cursor
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

type Option func(*Options)

// Comparator defines the total order used by a memtable. It must be
// deterministic, transitive, antisymmetric, safe for concurrent calls, and
// must neither mutate its arguments nor call back into the memtable.
type Comparator func(a, b []byte) int

type Options struct {
	Comparator Comparator
}

// WithComparator sets the memtable ordering. Passing nil causes construction
// to panic.
func WithComparator(cmp Comparator) Option {
	return func(opts *Options) {
		opts.Comparator = cmp
	}
}

func defaultOptions() Options {
	return Options{Comparator: bytes.Compare}
}

func makeOptions(opts ...Option) Options {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.Comparator == nil {
		panic("memtable: nil comparator")
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
