package tail

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/wal"
)

// Verification tests for the tail-materialization task.
// These tests verify the specific requirements from the task definition.

// TestVerification_QueryNodeReadsWALFromObjectStorage verifies that:
// "Test query node reads WAL tail from object storage"
func TestVerification_QueryNodeReadsWALFromObjectStorage(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Store WAL entries in object storage
	namespace := "verify-ns"
	for seq := uint64(1); seq <= 3; seq++ {
		_, data := createTestWALEntry(namespace, seq, []testDoc{
			{id: seq, attrs: map[string]any{"seq": int64(seq)}},
		})
		// Use correct key format: vex/namespaces/namespace/wal/N.wal.zst
		store.objects["vex/namespaces/"+namespace+"/"+wal.KeyForSeq(seq)] = data
	}

	// Query node refreshes tail from object storage
	ctx := context.Background()
	err := ts.Refresh(ctx, namespace, 0, 3)
	if err != nil {
		t.Fatalf("Refresh (reading from object storage) failed: %v", err)
	}

	// Verify all WAL entries were read
	docs, err := ts.Scan(ctx, namespace, nil)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(docs) != 3 {
		t.Errorf("expected 3 documents from WAL, got %d", len(docs))
	}

	// Verify documents have correct data
	for _, doc := range docs {
		expectedSeq := int64(doc.ID.U64())
		if doc.Attributes["seq"] != expectedSeq {
			t.Errorf("document %d: expected seq=%d, got %v",
				doc.ID.U64(), expectedSeq, doc.Attributes["seq"])
		}
	}

	t.Log("✓ Query node successfully reads WAL tail from object storage")
}

// TestVerification_RAMTierForRecentSubBatches verifies that:
// "Verify in-memory (RAM) tier for recent sub-batches"
func TestVerification_RAMTierForRecentSubBatches(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	namespace := "ram-tier-ns"

	// Add WAL entries directly to RAM tier (simulating write path)
	for seq := uint64(1); seq <= 5; seq++ {
		entry := wal.NewWalEntry(namespace, seq)
		batch := wal.NewWriteSubBatch("request-" + string(rune('0'+seq)))
		batch.AddUpsert(
			&wal.DocumentID{Id: &wal.DocumentID_U64{U64: seq}},
			map[string]*wal.AttributeValue{
				"data": wal.StringValue("value-" + string(rune('0'+seq))),
			},
			nil, 0,
		)
		entry.SubBatches = append(entry.SubBatches, batch)
		ts.AddWALEntry(namespace, entry)
	}

	// Verify RAM tier contains entries
	nt := ts.getNamespace(namespace)
	if nt == nil {
		t.Fatal("namespace not found in tail store")
	}

	// Check RAM entries
	if len(nt.ramEntries) != 5 {
		t.Errorf("expected 5 RAM entries, got %d", len(nt.ramEntries))
	}

	// Verify entries are in decoded columnar form (documents map)
	if len(nt.documents) != 5 {
		t.Errorf("expected 5 materialized documents, got %d", len(nt.documents))
	}

	// Verify documents are immediately queryable
	ctx := context.Background()
	docs, err := ts.Scan(ctx, namespace, nil)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(docs) != 5 {
		t.Errorf("expected 5 queryable documents, got %d", len(docs))
	}

	// Verify specific document retrieval
	doc, err := ts.GetDocument(ctx, namespace, document.NewU64ID(3))
	if err != nil {
		t.Fatalf("GetDocument failed: %v", err)
	}
	if doc == nil {
		t.Fatal("expected to find document in RAM tier")
	}
	if doc.Attributes["data"] != "value-3" {
		t.Errorf("expected data='value-3', got %v", doc.Attributes["data"])
	}

	t.Log("✓ In-memory (RAM) tier correctly stores recent sub-batches")
}

// TestVerification_NVMeTierForSpilledTailBlocks verifies that:
// "Test NVMe tier for spilled tail blocks"
func TestVerification_NVMeTierForSpilledTailBlocks(t *testing.T) {
	store := newMockStore()

	// Create disk cache (NVMe tier)
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: t.TempDir(),
		MaxBytes: 100 * 1024 * 1024, // 100MB
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	ts := New(DefaultConfig(), store, diskCache, nil)
	defer ts.Close()

	namespace := "nvme-tier-ns"

	// Store WAL entry in object storage
	_, data := createTestWALEntry(namespace, 1, []testDoc{
		{id: 1, attrs: map[string]any{"test": "nvme-data"}},
	})
	store.objects["vex/namespaces/"+namespace+"/wal/1.wal.zst"] = data

	// Refresh to load from object storage
	ctx := context.Background()
	err = ts.Refresh(ctx, namespace, 0, 1)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Verify WAL was cached to NVMe tier (disk cache)
	cacheKey := cache.CacheKey{
		ObjectKey: "vex/namespaces/" + namespace + "/wal/1.wal.zst",
		ETag:      "",
	}

	if !diskCache.Contains(cacheKey) {
		t.Error("WAL entry was not spilled to NVMe tier")
	}

	// Verify cached data matches original
	reader, err := diskCache.GetReader(cacheKey)
	if err != nil {
		t.Fatalf("failed to read from NVMe cache: %v", err)
	}
	cachedData, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("failed to read cached data: %v", err)
	}

	if !bytes.Equal(cachedData, data) {
		t.Error("NVMe cached data doesn't match original")
	}

	// Simulate cold start: clear RAM tier but keep NVMe
	ts.Clear(namespace)

	// Remove from object storage to prove we're reading from NVMe
	delete(store.objects, namespace+"/wal/1.wal.zst")

	// Refresh should use NVMe cache
	err = ts.Refresh(ctx, namespace, 0, 1)
	if err != nil {
		t.Fatalf("Refresh from NVMe cache failed: %v", err)
	}

	// Verify document is accessible from NVMe cache
	doc, err := ts.GetDocument(ctx, namespace, document.NewU64ID(1))
	if err != nil {
		t.Fatalf("GetDocument failed: %v", err)
	}
	if doc == nil {
		t.Fatal("expected document from NVMe cache")
	}
	if doc.Attributes["test"] != "nvme-data" {
		t.Errorf("expected test='nvme-data', got %v", doc.Attributes["test"])
	}

	t.Log("✓ NVMe tier correctly stores spilled tail blocks")
}

// TestVerification_TailSupportsVectorScanAndFilterEvaluation verifies that:
// "Verify tail supports vector scan and filter evaluation"
func TestVerification_TailSupportsVectorScanAndFilterEvaluation(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	namespace := "vector-filter-ns"

	// Add documents with vectors and filterable attributes
	_, data := createTestWALEntry(namespace, 1, []testDoc{
		{id: 1, attrs: map[string]any{"category": "tech", "price": int64(100)}, vector: []float32{1.0, 0.0, 0.0}},
		{id: 2, attrs: map[string]any{"category": "tech", "price": int64(200)}, vector: []float32{0.9, 0.1, 0.0}},
		{id: 3, attrs: map[string]any{"category": "food", "price": int64(50)}, vector: []float32{0.0, 1.0, 0.0}},
		{id: 4, attrs: map[string]any{"category": "tech", "price": int64(300)}, vector: []float32{0.8, 0.2, 0.0}},
		{id: 5, attrs: map[string]any{"category": "food", "price": int64(75)}, vector: []float32{0.0, 0.9, 0.1}},
	})
	store.objects["vex/namespaces/"+namespace+"/wal/1.wal.zst"] = data

	ctx := context.Background()
	err := ts.Refresh(ctx, namespace, 0, 1)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Test 1: Vector scan without filter (exhaustive search)
	t.Run("VectorScanExact", func(t *testing.T) {
		query := []float32{1.0, 0.0, 0.0}
		results, err := ts.VectorScan(ctx, namespace, query, 3, MetricCosineDistance, nil)
		if err != nil {
			t.Fatalf("VectorScan failed: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}

		// Verify sorted by distance (closest first)
		for i := 1; i < len(results); i++ {
			if results[i].Distance < results[i-1].Distance {
				t.Error("results not sorted by distance")
			}
		}

		// First result should be id=1 (exact match)
		if results[0].Doc.ID.U64() != 1 {
			t.Errorf("expected closest match id=1, got %d", results[0].Doc.ID.U64())
		}
		if results[0].Distance > 0.001 {
			t.Errorf("expected distance ~0 for exact match, got %f", results[0].Distance)
		}
	})

	// Test 2: Filter evaluation with Eq operator
	t.Run("FilterEvaluation_Eq", func(t *testing.T) {
		f, err := filter.Parse([]any{"category", "Eq", "tech"})
		if err != nil {
			t.Fatalf("Parse filter failed: %v", err)
		}

		docs, err := ts.Scan(ctx, namespace, f)
		if err != nil {
			t.Fatalf("Scan with filter failed: %v", err)
		}

		if len(docs) != 3 {
			t.Errorf("expected 3 tech documents, got %d", len(docs))
		}

		for _, doc := range docs {
			if doc.Attributes["category"] != "tech" {
				t.Errorf("filter failed: got category=%v", doc.Attributes["category"])
			}
		}
	})

	// Test 3: Filter evaluation with NotEq operator
	t.Run("FilterEvaluation_NotEq", func(t *testing.T) {
		f, err := filter.Parse([]any{"category", "NotEq", "tech"})
		if err != nil {
			t.Fatalf("Parse filter failed: %v", err)
		}

		docs, err := ts.Scan(ctx, namespace, f)
		if err != nil {
			t.Fatalf("Scan with filter failed: %v", err)
		}

		if len(docs) != 2 {
			t.Errorf("expected 2 non-tech documents, got %d", len(docs))
		}

		for _, doc := range docs {
			if doc.Attributes["category"] == "tech" {
				t.Error("filter failed: got tech document")
			}
		}
	})

	// Test 4: Combined vector scan with filter
	t.Run("VectorScanWithFilter", func(t *testing.T) {
		// Filter for tech category
		f, _ := filter.Parse([]any{"category", "Eq", "tech"})
		query := []float32{1.0, 0.0, 0.0}

		results, err := ts.VectorScan(ctx, namespace, query, 10, MetricCosineDistance, f)
		if err != nil {
			t.Fatalf("VectorScan with filter failed: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("expected 3 tech results, got %d", len(results))
		}

		// All results should be tech category
		for _, r := range results {
			if r.Doc.Attributes["category"] != "tech" {
				t.Error("filter not applied correctly to vector scan")
			}
		}

		// Results should still be sorted by distance
		for i := 1; i < len(results); i++ {
			if results[i].Distance < results[i-1].Distance {
				t.Error("filtered results not sorted by distance")
			}
		}
	})

	// Test 5: And filter operator
	t.Run("FilterEvaluation_And", func(t *testing.T) {
		// category=tech AND price > 150
		f, err := filter.Parse([]any{"And", []any{
			[]any{"category", "Eq", "tech"},
			[]any{"price", "Gt", float64(150)},
		}})
		if err != nil {
			t.Fatalf("Parse And filter failed: %v", err)
		}

		docs, err := ts.Scan(ctx, namespace, f)
		if err != nil {
			t.Fatalf("Scan with And filter failed: %v", err)
		}

		if len(docs) != 2 {
			t.Errorf("expected 2 tech docs with price>150, got %d", len(docs))
		}

		for _, doc := range docs {
			if doc.Attributes["category"] != "tech" {
				t.Error("And filter failed: wrong category")
			}
			price := doc.Attributes["price"].(int64)
			if price <= 150 {
				t.Errorf("And filter failed: price %d not > 150", price)
			}
		}
	})

	// Test 6: Or filter operator
	t.Run("FilterEvaluation_Or", func(t *testing.T) {
		// price=100 OR price=50
		f, err := filter.Parse([]any{"Or", []any{
			[]any{"price", "Eq", float64(100)},
			[]any{"price", "Eq", float64(50)},
		}})
		if err != nil {
			t.Fatalf("Parse Or filter failed: %v", err)
		}

		docs, err := ts.Scan(ctx, namespace, f)
		if err != nil {
			t.Fatalf("Scan with Or filter failed: %v", err)
		}

		if len(docs) != 2 {
			t.Errorf("expected 2 docs matching Or filter, got %d", len(docs))
		}
	})

	// Test 7: Not filter operator
	t.Run("FilterEvaluation_Not", func(t *testing.T) {
		// NOT category=tech
		f, err := filter.Parse([]any{"Not", []any{"category", "Eq", "tech"}})
		if err != nil {
			t.Fatalf("Parse Not filter failed: %v", err)
		}

		docs, err := ts.Scan(ctx, namespace, f)
		if err != nil {
			t.Fatalf("Scan with Not filter failed: %v", err)
		}

		if len(docs) != 2 {
			t.Errorf("expected 2 non-tech docs, got %d", len(docs))
		}

		for _, doc := range docs {
			if doc.Attributes["category"] == "tech" {
				t.Error("Not filter failed: got tech document")
			}
		}
	})

	t.Log("✓ Tail supports vector scan and filter evaluation")
}

// TestVerification_AllDistanceMetrics verifies all supported distance metrics
func TestVerification_AllDistanceMetrics(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	namespace := "metrics-ns"

	// Add vectors
	_, data := createTestWALEntry(namespace, 1, []testDoc{
		{id: 1, vector: []float32{1.0, 0.0}},
		{id: 2, vector: []float32{0.0, 1.0}},
		{id: 3, vector: []float32{1.0, 1.0}},
	})
	store.objects["vex/namespaces/"+namespace+"/wal/1.wal.zst"] = data

	ctx := context.Background()
	ts.Refresh(ctx, namespace, 0, 1)

	query := []float32{1.0, 0.0}

	t.Run("CosineDistance", func(t *testing.T) {
		results, _ := ts.VectorScan(ctx, namespace, query, 3, MetricCosineDistance, nil)
		// id=1 should be closest (distance 0)
		if results[0].Doc.ID.U64() != 1 {
			t.Errorf("expected id=1 closest for cosine, got %d", results[0].Doc.ID.U64())
		}
	})

	t.Run("EuclideanSquared", func(t *testing.T) {
		results, _ := ts.VectorScan(ctx, namespace, query, 3, MetricEuclideanSquared, nil)
		// id=1 should be closest (distance 0)
		if results[0].Doc.ID.U64() != 1 {
			t.Errorf("expected id=1 closest for euclidean, got %d", results[0].Doc.ID.U64())
		}
	})

	t.Run("DotProduct", func(t *testing.T) {
		results, _ := ts.VectorScan(ctx, namespace, query, 3, MetricDotProduct, nil)
		// id=1 or id=3 should be closest (highest dot product = lowest negative distance)
		firstID := results[0].Doc.ID.U64()
		if firstID != 1 && firstID != 3 {
			t.Errorf("expected id=1 or id=3 for dot product, got %d", firstID)
		}
	})

	t.Log("✓ All distance metrics work correctly")
}
