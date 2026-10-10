package wal

import (
	"fmt"
	"log/slog"
)

const (
	defaultSegmentBytes   = 64 << 20
	defaultReadAheadBytes = 64 << 10 // bytes recovery preads past each record
)

// Option configures a WAL.
type Option func(*Options)

// Options holds WAL settings.
type Options struct {
	SegmentBytes   uint64       `yaml:"segmentBytes"`
	ReadAheadBytes int          `yaml:"readAheadBytes"`
	Logger         *slog.Logger `yaml:"-"`
}

// DefaultOptions returns the default WAL settings.
func DefaultOptions() Options {
	return Options{
		SegmentBytes:   defaultSegmentBytes,
		ReadAheadBytes: defaultReadAheadBytes,
		Logger:         slog.New(slog.DiscardHandler),
	}
}

// WithLogger sets the logger, discarded by default.
func WithLogger(logger *slog.Logger) Option {
	return func(o *Options) {
		o.Logger = logger
	}
}

// WithSegmentBytes sets the size of each WAL file.
func WithSegmentBytes(size uint64) Option {
	return func(o *Options) {
		o.SegmentBytes = size
	}
}

// WithReadAheadBytes sets the minimum bytes recovery reads from a segment at once.
func WithReadAheadBytes(size int) Option {
	return func(o *Options) {
		o.ReadAheadBytes = size
	}
}

func validateSegmentSize(size uint64) error {
	if size < segmentMinSize {
		return fmt.Errorf("wal: segment size %d too small", size)
	}
	if size%8 != 0 {
		return fmt.Errorf("wal: segment size %d is not 8-byte aligned", size)
	}
	return nil
}
