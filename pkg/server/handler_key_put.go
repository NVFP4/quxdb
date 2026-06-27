package server

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"
	"github.com/yashgorana/quxdb/pkg/db"
)

const maxBodyBytes = 1024 * 1024 // 1MB

func hPutKey(db *db.QuxDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, err := url.PathUnescape(chi.URLParam(r, "key"))
		if err != nil {
			http.Error(w, "invalid key", http.StatusBadRequest)
			return
		}
		if key == "" {
			http.Error(w, "no key provided", http.StatusBadRequest)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			if maxErr, ok := errors.AsType[*http.MaxBytesError](err); ok {
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
		err = db.Set([]byte(key), body)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to set value: %v", err), http.StatusInternalServerError)
			return
		}

		RenderPlainText(w, r, http.StatusOK, "OK")
	}
}
