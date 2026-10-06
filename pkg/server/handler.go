package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/yashgorana/quxdb/pkg/bufpool"
	"github.com/yashgorana/quxdb/pkg/db"
)

var (
	okBody      = []byte("OK")
	octetStream = []string{"application/octet-stream"}
)

func renderPlainText(w http.ResponseWriter, r *http.Request, status int, v string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	w.Write([]byte(v))
}

// renderOK sends 200 "OK" with a sniffed content type, so no header map is cloned.
func renderOK(w http.ResponseWriter) {
	w.Write(okBody) //nolint:errcheck
}

func renderBinary(w http.ResponseWriter, r *http.Request, status int, v []byte) {
	w.Header()["Content-Type"] = octetStream
	w.WriteHeader(http.StatusOK)
	w.Write(v)
}

func renderJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(true)
	if err := enc.Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(buf.Bytes()) //nolint:errcheck
}

// readValue reads a non-empty body up to db.MaxValueSize, writing the error response on failure.
func readValue(w http.ResponseWriter, r *http.Request, buf *bufpool.Buf) ([]byte, bool) {
	var body []byte
	var err error
	switch n := r.ContentLength; {
	case n > db.MaxValueSize:
		err = &http.MaxBytesError{Limit: db.MaxValueSize}
	case n > 0:
		*buf = bufpool.Get(uint(n))
		_, err = io.ReadFull(r.Body, buf.B)
		body = buf.B
	case n < 0:
		// chunked
		body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, db.MaxValueSize))
	}
	if maxErr, ok := errors.AsType[*http.MaxBytesError](err); ok {
		http.Error(w, fmt.Sprintf("value exceeds %d bytes limit", maxErr.Limit), http.StatusRequestEntityTooLarge)
		return nil, false
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
		return nil, false
	}
	if len(body) == 0 {
		http.Error(w, "no value provided", http.StatusBadRequest)
		return nil, false
	}
	return body, true
}
