package server

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/yashgorana/quxdb/pkg/db"
)

func hDeleteKey(db *db.QuxDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")
		if key == "" {
			http.Error(w, "no key provided", http.StatusBadRequest)
			return
		}

		if err := db.Delete([]byte(key)); err != nil {
			http.Error(w, fmt.Sprintf("failed to delete key: %v", err), http.StatusInternalServerError)
			return
		}

		RenderPlainText(w, r, http.StatusOK, "OK")
	}
}
