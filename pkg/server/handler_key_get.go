package server

import (
	"bufio"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/yashgorana/quxdb/pkg/db"
)

func hGetKey(db *db.QuxDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")
		if key == "" {
			http.Error(w, "no key provided", http.StatusBadRequest)
			return
		}

		value, ok := db.Get([]byte(key))
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

		var start, end []byte
		if s := q.Get("start"); s != "" {
			start = []byte(s)
		}
		if e := q.Get("end"); e != "" {
			end = []byte(e)
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		bw := bufio.NewWriter(w)
		defer bw.Flush()
		for key, value := range db.Iter(start, end) {
			_, _ = bw.Write(key)
			_ = bw.WriteByte('\t')
			_, _ = bw.Write(value)
			_ = bw.WriteByte('\n')
		}
	}
}
