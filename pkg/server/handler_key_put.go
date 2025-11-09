package server

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/yashgorana/quxdb/pkg/db"
)

const maxBodyBytes = 1024 * 1024 // 1MB

func hPutKey(db *db.QuxDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")
		if key == "" {
			http.Error(w, "no key provided", http.StatusBadRequest)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				http.Error(w, fmt.Sprintf("value exceeds %d bytes limit", maxErr.Limit), http.StatusRequestEntityTooLarge)
				return
			}

			http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
			return
		}

		if len(body) == 0 {
			http.Error(w, "no value provided", http.StatusBadRequest)
			return
		}

		// write to store
		putStart := time.Now()
		err = db.Set([]byte(key), body)
		dur := time.Since(putStart)

		if err != nil {
			http.Error(w, fmt.Sprintf("failed to set value: %v", err), http.StatusInternalServerError)
			return
		}

		dbPutDuration.Observe(dur.Seconds())

		RenderBinary(w, r, http.StatusOK, body)
	}
}
