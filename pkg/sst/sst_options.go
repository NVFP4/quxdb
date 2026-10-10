package sst

import (
	"fmt"

	"github.com/yashgorana/quxdb/pkg/bloom"
)

const (
	defaultBlockTargetBytes = 32 << 10 // ~32 KiB Data
	defaultIndexStrideBytes = 2 << 10  // index key every 2 KiB
	defaultIndexStrideKeys  = 8        // index key every 8 keys
	defaultFilterBitsPerKey = 12
)

// Option configures a table build.
type Option func(*Options)

// Options holds table layout settings, readers don't depend on them.
type Options struct {
	BlockTargetBytes int   `yaml:"blockTargetBytes"`
	IndexStrideBytes int   `yaml:"indexStrideBytes"`
	IndexStrideKeys  int   `yaml:"indexStrideKeys"`
	FilterBitsPerKey uint8 `yaml:"filterBitsPerKey"`
}

// DefaultOptions returns the default table layout.
func DefaultOptions() Options {
	return Options{
		BlockTargetBytes: defaultBlockTargetBytes,
		IndexStrideBytes: defaultIndexStrideBytes,
		IndexStrideKeys:  defaultIndexStrideKeys,
		FilterBitsPerKey: defaultFilterBitsPerKey,
	}
}

// Validate reports settings a build can't use.
func (o Options) Validate() error {
	if o.BlockTargetBytes <= 0 {
		return fmt.Errorf("sst: block target bytes %d must be positive", o.BlockTargetBytes)
	}
	if o.FilterBitsPerKey == 0 || o.FilterBitsPerKey > bloom.MaxBitsPerKey {
		return fmt.Errorf("sst: filter bits per key %d outside [1, %d]", o.FilterBitsPerKey, bloom.MaxBitsPerKey)
	}
	return nil
}

// WithBlockTargetBytes sets the target size of a data block.
func WithBlockTargetBytes(size int) Option {
	return func(o *Options) {
		o.BlockTargetBytes = size
	}
}

// WithIndexStrideBytes sets the record bytes between block index entries.
func WithIndexStrideBytes(size int) Option {
	return func(o *Options) {
		o.IndexStrideBytes = size
	}
}

// WithIndexStrideKeys sets the records between block index entries.
func WithIndexStrideKeys(keys int) Option {
	return func(o *Options) {
		o.IndexStrideKeys = keys
	}
}

// WithFilterBitsPerKey sets the bloom filter bits per key.
func WithFilterBitsPerKey(bits uint8) Option {
	return func(o *Options) {
		o.FilterBitsPerKey = bits
	}
}
