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

// IncCacheHit increments the cache hit counter.
func IncCacheHit(cacheType, ns string) {
	CacheHits.WithLabelValues(cacheType, ns).Inc()
}

// IncCacheMiss increments the cache miss counter.
func IncCacheMiss(cacheType, ns string) {
	CacheMisses.WithLabelValues(cacheType, ns).Inc()
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
