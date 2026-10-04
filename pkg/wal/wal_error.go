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

// CorruptionError reports a damaged Record at LSN with valid Records after it.
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

// CorruptionPolicy decides what Recover does with a damaged Record that has valid Records after it.
type CorruptionPolicy uint8

const (
	// StopOnCorruption fails Recover and leaves the log unchanged.
	StopOnCorruption CorruptionPolicy = iota
	// TruncateOnCorruption drops the damaged Record and every Record after it.
	TruncateOnCorruption
)
