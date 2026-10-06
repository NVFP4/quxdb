package server

import (
	"bufio"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"
	"github.com/yashgorana/quxdb/pkg/db"
)

func hGetKey(db *db.QuxDB) http.HandlerFunc {
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

		value, ok, err := db.Get([]byte(key))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		RenderBinary(w, r, http.StatusOK, value)
	}
}

func hGetKeys(db *db.QuxDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()

		var lower, upper []byte
		if s := q.Get("lower"); s != "" {
			lower = []byte(s)
		}
		if s := q.Get("upper"); s != "" {
			upper = []byte(s)
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		bw := bufio.NewWriter(w)
		defer bw.Flush()
		for e, err := range db.Scan(lower, upper) {
			if err != nil {
				// status is already sent, so abort instead of ending cleanly
				panic(http.ErrAbortHandler)
			}
			_, _ = bw.Write(e.Key)
			_ = bw.WriteByte('\t')
			_, _ = bw.Write(e.Value)
			_ = bw.WriteByte('\n')
		}
	}
}
