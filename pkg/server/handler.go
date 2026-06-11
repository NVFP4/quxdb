package server

import (
	"bytes"
	"encoding/json"
	"net/http"
)

func RenderAny(w http.ResponseWriter, r *http.Request, status int, v []byte) {
	w.WriteHeader(status)
	w.Write(v)
}

func RenderPlainText(w http.ResponseWriter, r *http.Request, status int, v string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	w.Write([]byte(v))
}

func RenderBinary(w http.ResponseWriter, r *http.Request, status int, v []byte) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	w.Write(v)
}

func RenderJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
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
