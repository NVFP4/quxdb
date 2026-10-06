package server

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/yashgorana/quxdb/pkg/bufpool"
	"github.com/yashgorana/quxdb/pkg/db"
)

func hPutKey(store *db.QuxDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")

		// db.Set copies the value
		var buf bufpool.Buf
		defer buf.Release()
		body, ok := readValue(w, r, &buf)
		if !ok {
			return
		}

		err := store.Set([]byte(key), body)
		if errors.Is(err, db.ErrDbReadOnly) {
			http.Error(w, fmt.Sprintf("failed to set value: %v", err), http.StatusServiceUnavailable)
			return
		}
		if errors.Is(err, db.ErrKeyTooLarge) {
			http.Error(w, err.Error(), http.StatusRequestURITooLong)
			return
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to set value: %v", err), http.StatusInternalServerError)
			return
		}

		renderOK(w)
	}
}
