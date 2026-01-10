// Package metrics provides Prometheus metrics for the Vex search engine.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "vex"

var (
	// QueryConcurrency tracks per-namespace concurrent query execution.
	QueryConcurrency = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "query_concurrency",
			Help:      "Number of concurrent queries per namespace",
		},
		[]string{"namespace"},
	)

	// TailBytes tracks unindexed WAL bytes per namespace.
	TailBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tail_bytes",
			Help:      "Unindexed WAL bytes per namespace",
		},
		[]string{"namespace"},
	)

	// TailEntries tracks number of WAL entries retained in tail per namespace.
	TailEntries = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "tail_entries",
			Help:      "Unindexed WAL entry count per namespace",
		},
		[]string{"namespace"},
	)

	// CacheHits tracks cache hits per cache type and namespace.
	CacheHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "cache_hits_total",
			Help:      "Total cache hits",
		},
		[]string{"cache_type", "namespace"}, // cache_type: "disk" or "ram"
	)

	// CacheMisses tracks cache misses per cache type and namespace.
	CacheMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "cache_misses_total",
			Help:      "Total cache misses",
		},
		[]string{"cache_type", "namespace"}, // cache_type: "disk" or "ram"
	)

	// ANNClusterRangeCacheHits tracks cache hits for ANN cluster range reads.
	ANNClusterRangeCacheHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "ann_cluster_range_cache_hits_total",
			Help:      "Total ANN cluster range cache hits",
		},
		[]string{"namespace"},
	)

	// ANNClusterRangeCacheMisses tracks cache misses for ANN cluster range reads.
	ANNClusterRangeCacheMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "ann_cluster_range_cache_misses_total",
			Help:      "Total ANN cluster range cache misses",
		},
		[]string{"namespace"},
	)

	// IndexLag tracks the lag between WAL head and indexed WAL seq per namespace.
	IndexLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "index_lag_sequences",
			Help:      "Index lag (wal_head_seq - indexed_wal_seq) per namespace",
		},
		[]string{"namespace"},
	)

	// ObjectStoreOps tracks object store operations.
	ObjectStoreOps = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "objectstore_ops_total",
			Help:      "Total object store operations",
		},
		[]string{"operation", "status"}, // operation: get/put/delete/list, status: success/error
	)

	// ObjectStoreLatency tracks object store operation latency.
	ObjectStoreLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "objectstore_latency_seconds",
			Help:      "Object store operation latency in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	// WALCommitLatency tracks WAL commit latency.
	WALCommitLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "wal_commit_latency_seconds",
			Help:      "WAL commit latency in seconds",
			Buckets:   prometheus.DefBuckets,
		},
	)

	// WALCommitsTotal tracks total WAL commits.
	WALCommitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "wal_commits_total",
			Help:      "Total WAL commits",
		},
		[]string{"status"}, // success/error
	)

	// WALBytesWritten tracks total bytes written to WAL.
	WALBytesWritten = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "wal_bytes_written_total",
			Help:      "Total bytes written to WAL",
		},
	)

	// QueriesTotal tracks total queries executed.
	QueriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "queries_total",
			Help:      "Total queries executed",
		},
		[]string{"namespace", "query_type", "status"}, // query_type: vector/attribute/bm25/aggregate
	)

	// DocumentsIndexed tracks total documents indexed (upserted).
	DocumentsIndexed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "documents_indexed_total",
			Help:      "Total documents indexed",
		},
		[]string{"namespace"},
	)

	// DocumentsIndexedCurrent tracks current documents indexed (approximate).
	DocumentsIndexedCurrent = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "documents_indexed",
			Help:      "Current documents indexed (approximate)",
		},
		[]string{"namespace"},
	)

	// QueryLatency tracks query execution latency.
	QueryLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "query_latency_seconds",
			Help:      "Query execution latency in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"namespace", "query_type"},
	)

	// SegmentCounts tracks index segment counts by level.
	SegmentCounts = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "segment_count",
			Help:      "Indexed segment counts by namespace and level",
		},
		[]string{"namespace", "level"}, // level: total/l0/l1/l2
	)

	// CompactionsTotal tracks compaction attempts by namespace and level.
	CompactionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "compactions_total",
			Help:      "Total compactions by namespace and level",
		},
		[]string{"namespace", "level", "status"}, // level: l0_l1/l1_l2, status: success/error
	)

	// CompactionDuration tracks compaction duration by namespace and level.
	CompactionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "compaction_duration_seconds",
			Help:      "Compaction duration in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"namespace", "level"}, // level: l0_l1/l1_l2
	)

	// CompactionsInProgress tracks running compactions by namespace and level.
	CompactionsInProgress = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "compactions_in_progress",
			Help:      "Compactions currently running",
		},
		[]string{"namespace", "level"},
	)

	// CacheTemperature tracks cache temperature classification per cache type.
	// Temperature is encoded as: 0 = cold, 1 = warm, 2 = hot.
	CacheTemperature = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cache_temperature",
			Help:      "Cache temperature classification (0=cold, 1=warm, 2=hot)",
		},
		[]string{"cache_type"}, // "disk" or "ram"
	)

	// CacheHitRatio tracks cache hit ratio per cache type.
	CacheHitRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cache_hit_ratio",
			Help:      "Cache hit ratio (0.0-1.0)",
		},
		[]string{"cache_type"}, // "disk" or "ram"
	)

	// NamespaceCacheTemperature tracks cache temperature per namespace.
	// Temperature is encoded as: 0 = cold, 1 = warm, 2 = hot.
	NamespaceCacheTemperature = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "namespace_cache_temperature",
			Help:      "Cache temperature classification per namespace (0=cold, 1=warm, 2=hot)",
		},
		[]string{"namespace", "cache_type"},
	)
)

// IncQueryConcurrency increments the query concurrency gauge for a namespace.
func IncQueryConcurrency(ns string) {
	QueryConcurrency.WithLabelValues(ns).Inc()
}

// DecQueryConcurrency decrements the query concurrency gauge for a namespace.
func DecQueryConcurrency(ns string) {
	QueryConcurrency.WithLabelValues(ns).Dec()
}

// SetTailBytes sets the unindexed tail bytes for a namespace.
func SetTailBytes(ns string, bytes int64) {
	TailBytes.WithLabelValues(ns).Set(float64(bytes))
}

// SetTailEntries sets the tail entry count for a namespace.
func SetTailEntries(ns string, count int) {
	if count < 0 {
		count = 0
	}
	TailEntries.WithLabelValues(ns).Set(float64(count))
}

// IncCacheHit increments the cache hit counter.
func IncCacheHit(cacheType, ns string) {
	CacheHits.WithLabelValues(cacheType, ns).Inc()
}

// IncCacheMiss increments the cache miss counter.
func IncCacheMiss(cacheType, ns string) {
	CacheMisses.WithLabelValues(cacheType, ns).Inc()
}

// IncANNClusterRangeCacheHit increments the ANN cluster range cache hit counter.
func IncANNClusterRangeCacheHit(ns string) {
	ANNClusterRangeCacheHits.WithLabelValues(ns).Inc()
}

// IncANNClusterRangeCacheMiss increments the ANN cluster range cache miss counter.
func IncANNClusterRangeCacheMiss(ns string) {
	ANNClusterRangeCacheMisses.WithLabelValues(ns).Inc()
}

// SetIndexLag sets the index lag for a namespace.
func SetIndexLag(ns string, lag uint64) {
	IndexLag.WithLabelValues(ns).Set(float64(lag))
}

// ObserveObjectStoreOp records an object store operation.
func ObserveObjectStoreOp(operation string, latencySeconds float64, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	ObjectStoreOps.WithLabelValues(operation, status).Inc()
	ObjectStoreLatency.WithLabelValues(operation).Observe(latencySeconds)
}

// ObserveWALCommit records a WAL commit.
func ObserveWALCommit(latencySeconds float64, bytesWritten int64, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	WALCommitsTotal.WithLabelValues(status).Inc()
	WALCommitLatency.Observe(latencySeconds)
	if err == nil && bytesWritten > 0 {
		WALBytesWritten.Add(float64(bytesWritten))
	}
}

// ObserveQuery records a query execution.
func ObserveQuery(ns, queryType string, latencySeconds float64, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	QueriesTotal.WithLabelValues(ns, queryType, status).Inc()
	QueryLatency.WithLabelValues(ns, queryType).Observe(latencySeconds)
}

// SetSegmentCounts updates segment count metrics for a namespace.
func SetSegmentCounts(ns string, total, l0, l1, l2 int) {
	SegmentCounts.WithLabelValues(ns, "total").Set(float64(total))
	SegmentCounts.WithLabelValues(ns, "l0").Set(float64(l0))
	SegmentCounts.WithLabelValues(ns, "l1").Set(float64(l1))
	SegmentCounts.WithLabelValues(ns, "l2").Set(float64(l2))
}

// IncCompactionInProgress increments the in-progress compaction gauge.
func IncCompactionInProgress(ns, level string) {
	CompactionsInProgress.WithLabelValues(ns, level).Inc()
}

// DecCompactionInProgress decrements the in-progress compaction gauge.
func DecCompactionInProgress(ns, level string) {
	CompactionsInProgress.WithLabelValues(ns, level).Dec()
}

// ObserveCompaction records a compaction result and duration.
func ObserveCompaction(ns, level string, durationSeconds float64, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	CompactionsTotal.WithLabelValues(ns, level, status).Inc()
	CompactionDuration.WithLabelValues(ns, level).Observe(durationSeconds)
}

// SetDocumentsIndexed sets the current documents indexed gauge.
func SetDocumentsIndexed(ns string, count int64) {
	DocumentsIndexedCurrent.WithLabelValues(ns).Set(float64(count))
}

// AddDocumentsIndexed increments the documents indexed counter by the given count.
func AddDocumentsIndexed(ns string, count int64) {
	if count > 0 {
		DocumentsIndexed.WithLabelValues(ns).Add(float64(count))
	}
}

// TemperatureToValue converts a temperature string to a numeric value for Prometheus.
// Returns 0 for cold, 1 for warm, 2 for hot.
func TemperatureToValue(temp string) float64 {
	switch temp {
	case "hot":
		return 2
	case "warm":
		return 1
	default:
		return 0
	}
}

// SetCacheTemperature sets the cache temperature metric for a cache type.
func SetCacheTemperature(cacheType string, temp string) {
	CacheTemperature.WithLabelValues(cacheType).Set(TemperatureToValue(temp))
}

// SetCacheHitRatio sets the cache hit ratio metric for a cache type.
func SetCacheHitRatio(cacheType string, ratio float64) {
	CacheHitRatio.WithLabelValues(cacheType).Set(ratio)
}

// SetNamespaceCacheTemperature sets the cache temperature metric for a namespace.
func SetNamespaceCacheTemperature(ns, cacheType, temp string) {
	NamespaceCacheTemperature.WithLabelValues(ns, cacheType).Set(TemperatureToValue(temp))
}
