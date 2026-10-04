package server

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"
	"github.com/yashgorana/quxdb/pkg/db"
)

func hDeleteKey(store *db.QuxDB) http.HandlerFunc {
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

		err = store.Delete([]byte(key))
		if errors.Is(err, db.ErrReadOnly) {
			http.Error(w, fmt.Sprintf("failed to delete key: %v", err), http.StatusServiceUnavailable)
			return
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to delete key: %v", err), http.StatusInternalServerError)
			return
		}

		RenderPlainText(w, r, http.StatusOK, "OK")
	}
}
