package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestQueryConcurrencyMetric(t *testing.T) {
	// Reset the metrics for testing
	QueryConcurrency.Reset()

	ns := "test-namespace"

	// Initially should be 0
	val := testutil.ToFloat64(QueryConcurrency.WithLabelValues(ns))
	if val != 0 {
		t.Errorf("expected initial value 0, got %f", val)
	}

	// Increment concurrency
	IncQueryConcurrency(ns)
	val = testutil.ToFloat64(QueryConcurrency.WithLabelValues(ns))
	if val != 1 {
		t.Errorf("expected value 1 after inc, got %f", val)
	}

	// Increment again
	IncQueryConcurrency(ns)
	val = testutil.ToFloat64(QueryConcurrency.WithLabelValues(ns))
	if val != 2 {
		t.Errorf("expected value 2 after second inc, got %f", val)
	}

	// Decrement
	DecQueryConcurrency(ns)
	val = testutil.ToFloat64(QueryConcurrency.WithLabelValues(ns))
	if val != 1 {
		t.Errorf("expected value 1 after dec, got %f", val)
	}

	// Decrement again
	DecQueryConcurrency(ns)
	val = testutil.ToFloat64(QueryConcurrency.WithLabelValues(ns))
	if val != 0 {
		t.Errorf("expected value 0 after second dec, got %f", val)
	}
}

func TestQueryConcurrencyMultipleNamespaces(t *testing.T) {
	QueryConcurrency.Reset()

	ns1 := "namespace-1"
	ns2 := "namespace-2"

	IncQueryConcurrency(ns1)
	IncQueryConcurrency(ns1)
	IncQueryConcurrency(ns2)

	val1 := testutil.ToFloat64(QueryConcurrency.WithLabelValues(ns1))
	val2 := testutil.ToFloat64(QueryConcurrency.WithLabelValues(ns2))

	if val1 != 2 {
		t.Errorf("expected ns1 value 2, got %f", val1)
	}
	if val2 != 1 {
		t.Errorf("expected ns2 value 1, got %f", val2)
	}
}

func TestTailBytesMetric(t *testing.T) {
	TailBytes.Reset()

	ns := "test-namespace"

	SetTailBytes(ns, 1024)
	val := testutil.ToFloat64(TailBytes.WithLabelValues(ns))
	if val != 1024 {
		t.Errorf("expected 1024, got %f", val)
	}

	SetTailBytes(ns, 2048)
	val = testutil.ToFloat64(TailBytes.WithLabelValues(ns))
	if val != 2048 {
		t.Errorf("expected 2048, got %f", val)
	}

	// Test with different namespace
	ns2 := "namespace-2"
	SetTailBytes(ns2, 512)
	val2 := testutil.ToFloat64(TailBytes.WithLabelValues(ns2))
	if val2 != 512 {
		t.Errorf("expected 512, got %f", val2)
	}

	// First namespace should remain unchanged
	val = testutil.ToFloat64(TailBytes.WithLabelValues(ns))
	if val != 2048 {
		t.Errorf("expected ns1 still 2048, got %f", val)
	}
}

func TestCacheHitMissMetrics(t *testing.T) {
	CacheHits.Reset()
	CacheMisses.Reset()

	// Test disk cache metrics
	IncCacheHit("disk")
	IncCacheHit("disk")
	IncCacheMiss("disk")

	hitsVal := testutil.ToFloat64(CacheHits.WithLabelValues("disk"))
	missesVal := testutil.ToFloat64(CacheMisses.WithLabelValues("disk"))

	if hitsVal != 2 {
		t.Errorf("expected disk hits 2, got %f", hitsVal)
	}
	if missesVal != 1 {
		t.Errorf("expected disk misses 1, got %f", missesVal)
	}

	// Test RAM cache metrics
	IncCacheHit("ram")
	IncCacheMiss("ram")
	IncCacheMiss("ram")
	IncCacheMiss("ram")

	ramHits := testutil.ToFloat64(CacheHits.WithLabelValues("ram"))
	ramMisses := testutil.ToFloat64(CacheMisses.WithLabelValues("ram"))

	if ramHits != 1 {
		t.Errorf("expected ram hits 1, got %f", ramHits)
	}
	if ramMisses != 3 {
		t.Errorf("expected ram misses 3, got %f", ramMisses)
	}
}

func TestIndexLagMetric(t *testing.T) {
	IndexLag.Reset()

	ns := "test-namespace"

	SetIndexLag(ns, 5)
	val := testutil.ToFloat64(IndexLag.WithLabelValues(ns))
	if val != 5 {
		t.Errorf("expected 5, got %f", val)
	}

	// Simulate catching up
	SetIndexLag(ns, 0)
	val = testutil.ToFloat64(IndexLag.WithLabelValues(ns))
	if val != 0 {
		t.Errorf("expected 0, got %f", val)
	}
}

func TestObjectStoreOpsMetric(t *testing.T) {
	ObjectStoreOps.Reset()
	ObjectStoreLatency.Reset()

	// Record a successful operation
	ObserveObjectStoreOp("get", 0.005, nil)

	successOps := testutil.ToFloat64(ObjectStoreOps.WithLabelValues("get", "success"))
	if successOps != 1 {
		t.Errorf("expected 1 success get op, got %f", successOps)
	}

	// Record a failed operation
	ObserveObjectStoreOp("get", 0.010, errors.New("connection failed"))

	errorOps := testutil.ToFloat64(ObjectStoreOps.WithLabelValues("get", "error"))
	if errorOps != 1 {
		t.Errorf("expected 1 error get op, got %f", errorOps)
	}

	// Success count should still be 1
	successOps = testutil.ToFloat64(ObjectStoreOps.WithLabelValues("get", "success"))
	if successOps != 1 {
		t.Errorf("expected still 1 success get op, got %f", successOps)
	}

	// Test other operations
	ObserveObjectStoreOp("put", 0.020, nil)
	ObserveObjectStoreOp("delete", 0.001, nil)
	ObserveObjectStoreOp("list", 0.050, nil)

	putOps := testutil.ToFloat64(ObjectStoreOps.WithLabelValues("put", "success"))
	deleteOps := testutil.ToFloat64(ObjectStoreOps.WithLabelValues("delete", "success"))
	listOps := testutil.ToFloat64(ObjectStoreOps.WithLabelValues("list", "success"))

	if putOps != 1 {
		t.Errorf("expected 1 put op, got %f", putOps)
	}
	if deleteOps != 1 {
		t.Errorf("expected 1 delete op, got %f", deleteOps)
	}
	if listOps != 1 {
		t.Errorf("expected 1 list op, got %f", listOps)
	}
}

func TestObjectStoreLatencyMetric(t *testing.T) {
	ObjectStoreLatency.Reset()

	// Record multiple observations
	ObserveObjectStoreOp("get", 0.001, nil)
	ObserveObjectStoreOp("get", 0.002, nil)
	ObserveObjectStoreOp("get", 0.003, nil)

	// Verify the histogram exists and has data by checking it doesn't panic
	count := testutil.CollectAndCount(ObjectStoreLatency)
	if count == 0 {
		t.Error("expected histogram to have observations")
	}
}

func TestWALCommitLatencyMetric(t *testing.T) {
	// Reset counters (note: histograms don't have a Reset method on the base type)
	WALCommitsTotal.Reset()

	// Record successful commits
	ObserveWALCommit(0.010, 1024, nil)
	ObserveWALCommit(0.020, 2048, nil)

	successCommits := testutil.ToFloat64(WALCommitsTotal.WithLabelValues("success"))
	if successCommits != 2 {
		t.Errorf("expected 2 success commits, got %f", successCommits)
	}

	// Record a failed commit
	ObserveWALCommit(0.005, 0, errors.New("commit failed"))

	errorCommits := testutil.ToFloat64(WALCommitsTotal.WithLabelValues("error"))
	if errorCommits != 1 {
		t.Errorf("expected 1 error commit, got %f", errorCommits)
	}
}

func TestWALBytesWrittenMetric(t *testing.T) {
	// Get current value (can't reset counters easily)
	initialVal := testutil.ToFloat64(WALBytesWritten)

	ObserveWALCommit(0.010, 1000, nil)
	val := testutil.ToFloat64(WALBytesWritten)
	if val != initialVal+1000 {
		t.Errorf("expected %f, got %f", initialVal+1000, val)
	}

	ObserveWALCommit(0.010, 500, nil)
	val = testutil.ToFloat64(WALBytesWritten)
	if val != initialVal+1500 {
		t.Errorf("expected %f, got %f", initialVal+1500, val)
	}

	// Error commits should not add bytes
	ObserveWALCommit(0.010, 100, errors.New("failed"))
	val = testutil.ToFloat64(WALBytesWritten)
	if val != initialVal+1500 {
		t.Errorf("expected %f (no change), got %f", initialVal+1500, val)
	}
}

func TestQueryMetrics(t *testing.T) {
	QueriesTotal.Reset()

	ns := "test-namespace"

	// Record successful queries of different types
	ObserveQuery(ns, "vector", 0.010, nil)
	ObserveQuery(ns, "vector", 0.020, nil)
	ObserveQuery(ns, "attribute", 0.005, nil)
	ObserveQuery(ns, "bm25", 0.015, nil)
	ObserveQuery(ns, "aggregate", 0.025, nil)

	vectorSuccess := testutil.ToFloat64(QueriesTotal.WithLabelValues(ns, "vector", "success"))
	attrSuccess := testutil.ToFloat64(QueriesTotal.WithLabelValues(ns, "attribute", "success"))
	bm25Success := testutil.ToFloat64(QueriesTotal.WithLabelValues(ns, "bm25", "success"))
	aggSuccess := testutil.ToFloat64(QueriesTotal.WithLabelValues(ns, "aggregate", "success"))

	if vectorSuccess != 2 {
		t.Errorf("expected 2 vector queries, got %f", vectorSuccess)
	}
	if attrSuccess != 1 {
		t.Errorf("expected 1 attribute query, got %f", attrSuccess)
	}
	if bm25Success != 1 {
		t.Errorf("expected 1 bm25 query, got %f", bm25Success)
	}
	if aggSuccess != 1 {
		t.Errorf("expected 1 aggregate query, got %f", aggSuccess)
	}

	// Record a failed query
	ObserveQuery(ns, "vector", 0.001, errors.New("query failed"))

	vectorError := testutil.ToFloat64(QueriesTotal.WithLabelValues(ns, "vector", "error"))
	if vectorError != 1 {
		t.Errorf("expected 1 vector error, got %f", vectorError)
	}

	// Success count should remain unchanged
	vectorSuccess = testutil.ToFloat64(QueriesTotal.WithLabelValues(ns, "vector", "success"))
	if vectorSuccess != 2 {
		t.Errorf("expected still 2 vector successes, got %f", vectorSuccess)
	}
}

func TestQueryLatencyHistogram(t *testing.T) {
	QueryLatency.Reset()

	ns := "test-namespace"

	ObserveQuery(ns, "vector", 0.001, nil)
	ObserveQuery(ns, "vector", 0.002, nil)
	ObserveQuery(ns, "vector", 0.003, nil)

	// Verify the histogram exists and has data by checking it doesn't panic
	count := testutil.CollectAndCount(QueryLatency)
	if count == 0 {
		t.Error("expected histogram to have observations")
	}
}
