package server

import (
	"bufio"
	"net/http"

	"github.com/yashgorana/quxdb/pkg/db"
)

func hGetKey(db *db.QuxDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		value, ok, err := db.Get([]byte(key))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		renderBinary(w, r, http.StatusOK, value)
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
				// status already sent, abort the response
				panic(http.ErrAbortHandler)
			}
			_, _ = bw.Write(e.Key)
			_ = bw.WriteByte('\t')
			_, _ = bw.Write(e.Value)
			_ = bw.WriteByte('\n')
		}
	}
}
