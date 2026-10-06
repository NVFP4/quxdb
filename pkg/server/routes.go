package server

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/yashgorana/quxdb/pkg/db"
)

func setupHttpRoutes(db *db.QuxDB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", indexHandler)
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("GET /k/{$}", hGetKeys(db))
	mux.HandleFunc("GET /k/{key}", hGetKey(db))
	mux.HandleFunc("PUT /k/{key}", hPutKey(db))
	mux.HandleFunc("DELETE /k/{key}", hDeleteKey(db))
	mux.Handle("GET /metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{EnableOpenMetrics: true},
	))
	return middlewares(mux, promInstrument, recoverer)
}
