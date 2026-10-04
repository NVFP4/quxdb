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

	DbReadOnly = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "quxdb_read_only",
		Help: "1 once a wal failure has stopped writes, 0 otherwise",
	})

	DbUserBytesWritten = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quxdb_user_bytes_written_total",
		Help: "Key and value bytes of committed writes",
	})

	DbMemtableBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "quxdb_memtable_bytes",
		Help: "Bytes held by the active and immutable memtables",
	})

	DbImmutableMemtables = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "quxdb_immutable_memtables",
		Help: "Immutable memtables waiting to be flushed or kept for reads",
	})

	DbGetDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "quxdb_get_duration_seconds",
		Help: "Duration of a Get by result: found_memtable, found_sst, not_found or error",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"result"})
	DbGetMemtable = DbGetDuration.WithLabelValues("found_memtable")
	DbGetSST      = DbGetDuration.WithLabelValues("found_sst")
	DbGetNotFound = DbGetDuration.WithLabelValues("not_found")
	DbGetError    = DbGetDuration.WithLabelValues("error")

	DbFlushDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_flush_duration_seconds",
		Help: "Time to write one memtable to an l0 table",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})

	DbFlushBytesWritten = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quxdb_flush_bytes_written_total",
		Help: "Data block bytes written by memtable flushes",
	})

	DbCompactionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "quxdb_compaction_duration_seconds",
		Help: "Duration of one compaction by kind: move or merge",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"kind"})
	DbCompactionMove  = DbCompactionDuration.WithLabelValues("move")
	DbCompactionMerge = DbCompactionDuration.WithLabelValues("merge")

	DbCompactionBytesRead = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quxdb_compaction_bytes_read_total",
		Help: "Data block bytes of tables merged by compaction",
	})

	DbCompactionBytesWritten = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quxdb_compaction_bytes_written_total",
		Help: "Data block bytes of tables written by compaction",
	})

	DbCompactionErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quxdb_compaction_errors_total",
		Help: "Compaction passes that ended in an error",
	})

	// SST Metrics
	SstTables = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "quxdb_sst_tables",
		Help: "Number of tables in each lsm level",
	}, []string{"level"})

	SstBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "quxdb_sst_bytes",
		Help: "Data block bytes of the tables in each lsm level",
	}, []string{"level"})

	SstProbesPerGet = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_sst_probes_per_get",
		Help: "Tables whose bloom filter a Get checked",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})

	SstBloomChecks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "quxdb_sst_bloom_checks_total",
		Help: "Bloom filter checks by Get: negative, true_positive or false_positive",
	}, []string{"result"})
	SstBloomNegative      = SstBloomChecks.WithLabelValues("negative")
	SstBloomTruePositive  = SstBloomChecks.WithLabelValues("true_positive")
	SstBloomFalsePositive = SstBloomChecks.WithLabelValues("false_positive")

	// WAL Metrics
	WalWriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_wal_write_duration_seconds",
		Help: "Duration of one pwritev of a wal record",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})

	WalSyncDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "quxdb_wal_sync_duration_seconds",
		Help: "Duration of the fdatasync ending a synced append",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	})

	WalBytesWritten = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quxdb_wal_bytes_written_total",
		Help: "Bytes written to wal segments, framing included",
	})

	WalSegments = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "quxdb_wal_segments",
		Help: "Wal segments retained for recovery, the active one included",
	})

	WalRetainedBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "quxdb_wal_retained_bytes",
		Help: "Disk bytes of retained wal segments",
	})

	WalRolloverDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "quxdb_wal_rollover_duration_seconds",
		Help: "Time to switch segments by whether the next one was prepared, stalled or unprepared",

		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"segment"})
	WalRolloverPrepared   = WalRolloverDuration.WithLabelValues("prepared")
	WalRolloverStalled    = WalRolloverDuration.WithLabelValues("stalled")
	WalRolloverUnprepared = WalRolloverDuration.WithLabelValues("unprepared")
)

func init() {
	prometheus.MustRegister(DbCommitBacklogSize)
	prometheus.MustRegister(DbCommitBatchSize)
	prometheus.MustRegister(DbCommitDuration)
	prometheus.MustRegister(DbCommitEncodeDuration)
	prometheus.MustRegister(DbCommitAppendDuration)
	prometheus.MustRegister(DbCommitMemSetDuration)
	prometheus.MustRegister(DbSetQueueWaitDuration)
	prometheus.MustRegister(DbSetTotalDuration)
	prometheus.MustRegister(DbReadOnly)
	prometheus.MustRegister(DbUserBytesWritten)
	prometheus.MustRegister(DbMemtableBytes)
	prometheus.MustRegister(DbImmutableMemtables)
	prometheus.MustRegister(DbGetDuration)
	prometheus.MustRegister(DbFlushDuration)
	prometheus.MustRegister(DbFlushBytesWritten)
	prometheus.MustRegister(DbCompactionDuration)
	prometheus.MustRegister(DbCompactionBytesRead)
	prometheus.MustRegister(DbCompactionBytesWritten)
	prometheus.MustRegister(DbCompactionErrors)
	prometheus.MustRegister(SstTables)
	prometheus.MustRegister(SstBytes)
	prometheus.MustRegister(SstProbesPerGet)
	prometheus.MustRegister(SstBloomChecks)
	prometheus.MustRegister(WalWriteDuration)
	prometheus.MustRegister(WalSyncDuration)
	prometheus.MustRegister(WalBytesWritten)
	prometheus.MustRegister(WalSegments)
	prometheus.MustRegister(WalRetainedBytes)
	prometheus.MustRegister(WalRolloverDuration)
}
