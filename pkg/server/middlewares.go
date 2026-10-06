package server

import (
	"fmt"
	"net/http"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	httpRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_duration_seconds",
		Help: "Latency of completed http requests by route and status",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"endpoint", "status"})

	statusWriters = sync.Pool{New: func() any { return new(statusWriter) }}
)

func init() {
	prometheus.MustRegister(httpRequestDuration)
}

type middleware func(http.Handler) http.Handler

// middlewares wraps h in mws, the first one outermost.
func middlewares(h http.Handler, mws ...middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// promInstrument records the latency of each request under its route pattern.
func promInstrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := statusWriters.Get().(*statusWriter)
		sw.ResponseWriter, sw.status = w, 0
		next.ServeHTTP(sw, r)
		dur := time.Since(start)

		status := sw.status
		if status == 0 {
			status = http.StatusOK
		}
		sw.ResponseWriter = nil
		statusWriters.Put(sw)

		// set by the mux during routing
		endpoint := r.Pattern
		if endpoint == "" {
			endpoint = "<no-match>"
		}
		httpRequestDuration.WithLabelValues(endpoint, strconv.Itoa(status)).Observe(dur.Seconds())
	})
}

// recoverer turns a handler panic into a 500 and logs its stack, except http.ErrAbortHandler.
func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if rec == http.ErrAbortHandler {
				panic(rec)
			}
			fmt.Printf("server: panic serving %s %s: %v\n%s", r.Method, r.URL.Path, rec, debug.Stack())
			w.WriteHeader(http.StatusInternalServerError)
		}()
		next.ServeHTTP(w, r)
	})
}
