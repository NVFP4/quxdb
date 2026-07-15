package jsonl

import (
	"encoding/json"
	"fmt"
	"io"
)

type Writer struct {
	w io.Writer
}

func NewWriter(w io.Writer) Writer {
	return Writer{
		w: w,
	}
}

func (w Writer) Write(data any) (int, error) {
	j, err := json.Marshal(data)
	if err != nil {
		return -1, fmt.Errorf("could not json marshal data: %w", err)
	}

	j = append(j, '\n')

	n, err := w.w.Write(j)
	if err != nil {
		return -1, fmt.Errorf("could not write json data to underlying io.Writer: %w", err)
	}

	return n, nil
}
