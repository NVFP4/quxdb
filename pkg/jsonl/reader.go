package jsonl

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
)

var (
	ErrTornRead = errors.New("jsonl: torn read")
)

type Reader[T any] struct {
	br *bufio.Reader

	offset     int
	safeOffset int
	lines      int
}

func NewReader[T any](r io.Reader) *Reader[T] {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)

	return &Reader[T]{
		br: bufio.NewReader(r),
	}
}

func (r *Reader[T]) Read() (output T, err error) {
	lineStart := r.offset
	line, err := r.br.ReadBytes('\n') // will trigger io.EOF
	r.offset += len(line)

	hasNewline := len(line) > 0 && line[len(line)-1] == '\n'
	if err == io.EOF && !hasNewline {
		// EOF after bytes is a torn line.
		if len(line) > 0 {
			return output, fmt.Errorf("%w at byte %d: %w", ErrTornRead, lineStart, err)
		}
		// EOF with no bytes is a clean end
		return output, err
	}

	if err != nil {
		return output, err
	}

	err = json.Unmarshal(line, &output)
	if err != nil {
		return output, err
	}

	r.safeOffset = r.offset
	r.lines++

	return output, nil
}

func (r *Reader[T]) Records() iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for {
			output, err := r.Read()
			if err == io.EOF {
				return
			}

			if err != nil {
				var zero T
				yield(zero, err)
				return
			}

			if !yield(output, nil) {
				return
			}
		}
	}
}

func (r *Reader[T]) SafeOffset() int {
	return r.safeOffset
}

func (r *Reader[T]) Offset() int {
	return r.offset
}

func (r *Reader[T]) Lines() int {
	return r.lines
}
