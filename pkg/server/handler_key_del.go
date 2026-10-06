package server

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/yashgorana/quxdb/pkg/db"
)

func hDeleteKey(store *db.QuxDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")

		err := store.Delete([]byte(key))
		if errors.Is(err, db.ErrDbReadOnly) {
			http.Error(w, fmt.Sprintf("failed to delete key: %v", err), http.StatusServiceUnavailable)
			return
		}
		if errors.Is(err, db.ErrKeyTooLarge) {
			http.Error(w, err.Error(), http.StatusRequestURITooLong)
			return
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to delete key: %v", err), http.StatusInternalServerError)
			return
		}

		renderOK(w)
	}
}
