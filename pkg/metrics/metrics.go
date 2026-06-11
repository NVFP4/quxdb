package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// DB Metrics
	DbCommitBacklogSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_commit_backlog_size",
		Help: "Number of requests queued up at the start of a commit. Range is [0, 128]",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  128,
		NativeHistogramMinResetDuration: time.Hour,
	})

	DbCommitBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_commit_batch_size",
		Help: "Number of requests in a commit batch. Range is [1, 128]",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  128,
		NativeHistogramMinResetDuration: time.Hour,
	})

	DbCommitDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_commit_duration_seconds",
		Help: "Time spent committing a batch to disk",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})

	DbCommitEncodeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_commit_encode_duration_seconds",
		Help: "",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})

	DbCommitAppendDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_commit_append_duration_seconds",
		Help: "",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})

	DbCommitSyncDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_commit_sync_duration_seconds",
		Help: "",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})

	DbCommitMemSetDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_commit_memset_duration_seconds",
		Help: "",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})

	DbSetQueueWaitDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_set_queue_wait_duration_seconds",
		Help: "Time spent waiting in the commit queue",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})

	DbSetTotalDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_set_total_duration_seconds",
		Help: "End-to-end duration of a Set operation",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})
)

func init() {
	prometheus.MustRegister(DbCommitBacklogSize)
	prometheus.MustRegister(DbCommitBatchSize)
	prometheus.MustRegister(DbCommitDuration)
	prometheus.MustRegister(DbCommitEncodeDuration)
	prometheus.MustRegister(DbCommitAppendDuration)
	prometheus.MustRegister(DbCommitSyncDuration)
	prometheus.MustRegister(DbCommitMemSetDuration)
	prometheus.MustRegister(DbSetQueueWaitDuration)
	prometheus.MustRegister(DbSetTotalDuration)
}
