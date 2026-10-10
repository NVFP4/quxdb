package server

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/yashgorana/quxdb/pkg/quxdb"
)

func setupHttpRoutes(db *quxdb.DB, log *slog.Logger) http.Handler {
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
	return middlewares(mux, instrument(log), recoverer(log))
}
