package wal

import (
	"errors"
	"fmt"
)

var (
	ErrMidLogCorruption = errors.New("wal: damaged record followed by valid records")
	ErrNotRecovered     = errors.New("wal: append before recover")
	ErrWALFailed        = errors.New("wal: io failure, reopen to append again")
)

// CorruptionError reports a damaged Entry at LSN with valid Entries after it.
type CorruptionError struct {
	LSN LSN
	Err error
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("%v: lsn=%d: %v", ErrMidLogCorruption, e.LSN, e.Err)
}

func (e *CorruptionError) Unwrap() error {
	return e.Err
}

func (e *CorruptionError) Is(target error) bool {
	return target == ErrMidLogCorruption
}

// CorruptionPolicy decides what Recover does with a damaged Entry that has valid Entries after it.
type CorruptionPolicy uint8

const (
	// StopOnCorruption fails Recover and leaves the log unchanged.
	StopOnCorruption CorruptionPolicy = iota
	// TruncateOnCorruption drops the damaged Entry and every Entry after it.
	TruncateOnCorruption
)

func (p CorruptionPolicy) String() string {
	switch p {
	case StopOnCorruption:
		return "stop"
	case TruncateOnCorruption:
		return "truncate"
	default:
		return "unknown"
	}
}

// UnmarshalText parses "stop" or "truncate".
func (p *CorruptionPolicy) UnmarshalText(text []byte) error {
	switch string(text) {
	case "stop":
		*p = StopOnCorruption
	case "truncate":
		*p = TruncateOnCorruption
	default:
		return fmt.Errorf("wal: unknown corruption policy %q", text)
	}
	return nil
}
