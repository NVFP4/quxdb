package vset

import "log/slog"

const defaultMaxStaleRecords = 1024 // dead records that trigger a catalog snapshot

// Option configures a VersionSet.
type Option func(*Options)

// Options holds catalog settings.
type Options struct {
	MaxStaleRecords int          `yaml:"maxStaleRecords"`
	Logger          *slog.Logger `yaml:"-"`
}

// DefaultOptions returns the default catalog settings.
func DefaultOptions() Options {
	return Options{
		MaxStaleRecords: defaultMaxStaleRecords,
		Logger:          slog.New(slog.DiscardHandler),
	}
}

// WithLogger sets the logger, discarded by default.
func WithLogger(logger *slog.Logger) Option {
	return func(o *Options) {
		o.Logger = logger
	}
}

// WithMaxStaleRecords sets how many dead catalog records trigger a snapshot.
func WithMaxStaleRecords(n int) Option {
	return func(o *Options) {
		o.MaxStaleRecords = n
	}
}
