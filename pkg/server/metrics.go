package server

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Database Specific Metrics
	dbPutDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "db_memtable_put_duration_seconds",
		Help: "Latency of MemTable PUT operations.",
		// Custom buckets for low-latency DB ops.
		Buckets: prometheus.ExponentialBucketsRange(
			1e-6,
			5,
			101,
		),
	})

	dbGetDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "db_memtable_get_duration_seconds",
		Help: "Latency of MemTable GET operations.",
		// Custom buckets for low-latency DB ops:
		Buckets: prometheus.ExponentialBucketsRange(
			1e-6,
			5,
			101,
		),
	})
)

func init() {
	prometheus.MustRegister(dbPutDuration)
	prometheus.MustRegister(dbGetDuration)
}
