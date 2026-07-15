package wal

import "fmt"

type Option func(*walOptions)

type walOptions struct {
	segmentSize uint64
}

func defaultWALOptions() walOptions {
	return walOptions{segmentSize: walSegmentDefaultSize}
}

// WithSegmentSize configures the physical size of WAL segments.
func WithSegmentSize(size uint64) Option {
	return func(o *walOptions) {
		o.segmentSize = size
	}
}

func applyOptions(opts []Option) (walOptions, error) {
	o := defaultWALOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if err := validateSegmentSize(o.segmentSize); err != nil {
		return walOptions{}, err
	}
	return o, nil
}

func validateSegmentSize(size uint64) error {
	if size < walSegmentMinSize {
		return fmt.Errorf("wal: segment size %d too small", size)
	}
	if size%8 != 0 {
		return fmt.Errorf("wal: segment size %d is not 8-byte aligned", size)
	}
	return nil
}
