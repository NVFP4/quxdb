package server

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
)

var httpRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name: "http_request_duration_seconds",
	Help: "Latency of completed http requests by route and status",

	NativeHistogramBucketFactor:     1.1,
	NativeHistogramMaxBucketNumber:  100,
	NativeHistogramMinResetDuration: time.Hour,
}, []string{"endpoint", "status"})

func init() {
	prometheus.MustRegister(httpRequestDuration)
}

// httpCollector records the latency of each request under its chi route pattern.
func httpCollector(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		dur := time.Since(start)

		// pattern is complete only after routing ran
		endpoint := "<no-match>"
		if pattern := chi.RouteContext(r.Context()).RoutePattern(); pattern != "" {
			endpoint = r.Method + " " + pattern
		}
		status := ww.Status()
		if status == 0 {
			status = http.StatusOK
		}
		httpRequestDuration.WithLabelValues(endpoint, strconv.Itoa(status)).Observe(dur.Seconds())
	})
}
