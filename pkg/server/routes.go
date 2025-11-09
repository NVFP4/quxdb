package server

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/metrics"

	"github.com/yashgorana/quxdb/pkg/db"
)

func setupHttpRoutes(db *db.QuxDB) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)
	r.Use(metrics.Collector(metrics.CollectorOpts{
		Host:  false,
		Proto: true,
	}))

	r.Get("/", indexHandler)
	r.Get("/health", healthHandler)
	r.Route("/k", func(r chi.Router) {
		r.Get("/", hGetKeys(db))
		r.Get("/{key}", hGetKey(db))
		r.Put("/{key}", hPutKey(db))
		r.Delete("/{key}", hDeleteKey(db))
	})
	r.Handle("/metrics", metrics.Handler())

	return r
}
