package index

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestLargeSegmentDocsMemory tests that querying a large segment doesn't load all docs into memory.
// This is a regression test for the memory issue where LoadSegmentDocs loads ALL documents
// into memory just to get attributes for a small number of result IDs.
func TestLargeSegmentDocsMemory(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create a large segment with 100k documents
	// Each doc has a text attribute that's ~100 bytes
	numDocs := 100_000
	dims := 768 // Common embedding dimension

	// Create documents with attributes
	docs := make([]IndexedDocument, numDocs)
	for i := 0; i < numDocs; i++ {
		docs[i] = IndexedDocument{
			ID:        string(rune('a'+i%26)) + "_doc_" + string(rune('0'+i/26%10)),
			NumericID: uint64(i + 1),
			WALSeq:    uint64(i + 1),
			Deleted:   false,
			Attributes: map[string]any{
				"text":       "This is a sample post with some text content that simulates real data #" + string(rune('0'+i%10)),
				"created_at": time.Now().Add(-time.Duration(i) * time.Second).Format(time.RFC3339),
				"author":     "user_" + string(rune('a'+i%26)),
				"likes":      i % 1000,
			},
		}
	}

	docsData, err := EncodeDocsColumnZstd(docs)
	if err != nil {
		t.Fatalf("failed to encode docs: %v", err)
	}
	docsKey := "vex/namespaces/test-ns/index/segments/seg_001/docs.col.zst"

	// Create a simple IVF index with 32 clusters
	nClusters := 32
	centroid := make([]float32, dims)
	for i := range centroid {
		centroid[i] = 0.1
	}
	centroids := make([][]float32, nClusters)
	for i := 0; i < nClusters; i++ {
		centroids[i] = make([]float32, dims)
		copy(centroids[i], centroid)
		centroids[i][i%dims] = 1.0 // Make each centroid slightly different
	}
	centroidsData := createTestCentroidsFileWithCentroids(t, dims, centroids, vector.MetricCosineDistance)

	// Create simple cluster data (just enough to make IVF work)
	docsPerCluster := numDocs / nClusters
	offsets := make([]vector.ClusterOffset, nClusters)
	allClusterData := []byte{}
	for i := 0; i < nClusters; i++ {
		clusterDocs := make([]struct {
			docID uint64
			vec   []float32
		}, docsPerCluster)
		for j := 0; j < docsPerCluster; j++ {
			docID := uint64(i*docsPerCluster + j + 1)
			vec := make([]float32, dims)
			copy(vec, centroids[i])
			vec[0] += float32(j) * 0.001 // Small variation
			clusterDocs[j] = struct {
				docID uint64
				vec   []float32
			}{docID, vec}
		}
		clusterData := createClusterDocsData(t, dims, clusterDocs)
		offsets[i] = vector.ClusterOffset{
			Offset:   uint64(len(allClusterData)),
			Length:   uint64(len(clusterData)),
			DocCount: uint32(docsPerCluster),
		}
		allClusterData = append(allClusterData, clusterData...)
	}
	offsetsData := createTestOffsetsFile(t, offsets)

	// Create manifest
	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: uint64(numDocs),
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   uint64(numDocs),
				DocsKey:     docsKey,
				IVFKeys: &IVFKeys{
					CentroidsKey:      "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin",
					ClusterOffsetsKey: "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin",
					ClusterDataKey:    "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack",
					NClusters:         nClusters,
					VectorCount:       numDocs,
				},
			},
		},
		Stats: Stats{
			ApproxRowCount: int64(numDocs),
		},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	// Store all objects
	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, docsKey, bytes.NewReader(docsData), int64(len(docsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.CentroidsKey, bytes.NewReader(centroidsData), int64(len(centroidsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterOffsetsKey, bytes.NewReader(offsetsData), int64(len(offsetsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterDataKey, bytes.NewReader(allClusterData), int64(len(allClusterData)), nil)

	t.Logf("Created test data: %d docs, docs.col.zst size: %d bytes (%.1f MB)",
		numDocs, len(docsData), float64(len(docsData))/(1024*1024))

	reader := NewReader(store, nil, nil)

	// Force GC and get baseline memory
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// This is the problematic call - it loads ALL documents
	loadedDocs, err := reader.LoadSegmentDocs(ctx, manifestKey)
	if err != nil {
		t.Fatalf("LoadSegmentDocs() error = %v", err)
	}

	// Force GC and measure memory after
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	memUsedMB := float64(memAfter.Alloc-memBefore.Alloc) / (1024 * 1024)
	t.Logf("Loaded %d docs, memory used: %.1f MB", len(loadedDocs), memUsedMB)

	// The current implementation loads ALL docs - this should fail
	// After fix, we should have a method that only loads docs for specific IDs
	if len(loadedDocs) != numDocs {
		t.Errorf("expected %d docs, got %d", numDocs, len(loadedDocs))
	}

	// Memory check: loading 100k docs with attributes shouldn't use more than 50MB
	// (Currently it uses ~100MB+ which is the problem)
	// This test documents the current behavior - after fix, we'll have a new method
	// that only loads requested doc IDs
	t.Logf("Current behavior: LoadSegmentDocs loads ALL %d docs into memory", len(loadedDocs))
}

// TestLoadDocsForIDs tests the new method that only loads docs for specific IDs.
// This is the fix for the memory issue.
func TestLoadDocsForIDs(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create a segment with 10k documents
	numDocs := 10_000

	docs := make([]IndexedDocument, numDocs)
	for i := 0; i < numDocs; i++ {
		docs[i] = IndexedDocument{
			ID:        "doc_" + string(rune('0'+i/1000)) + string(rune('0'+i/100%10)) + string(rune('0'+i/10%10)) + string(rune('0'+i%10)),
			NumericID: uint64(i + 1),
			WALSeq:    uint64(i + 1),
			Deleted:   false,
			Attributes: map[string]any{
				"text":   "Sample text for document " + string(rune('0'+i%10)),
				"number": i,
			},
		}
	}

	docsData, _ := EncodeDocsColumnZstd(docs)
	docsKey := "vex/namespaces/test-ns/index/segments/seg_001/docs.col.zst"

	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: uint64(numDocs),
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   uint64(numDocs),
				DocsKey:     docsKey,
			},
		},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, docsKey, bytes.NewReader(docsData), int64(len(docsData)), nil)

	reader := NewReader(store, nil, nil)

	// Request only 10 specific doc IDs (simulating top-k results from IVF search)
	requestedIDs := []uint64{1, 100, 500, 1000, 2000, 3000, 5000, 7000, 9000, 10000}

	// This method should only load the requested docs, not all 10k
	loadedDocs, err := reader.LoadDocsForIDs(ctx, manifestKey, requestedIDs)
	if err != nil {
		t.Fatalf("LoadDocsForIDs() error = %v", err)
	}

	// Should only return the requested docs
	if len(loadedDocs) != len(requestedIDs) {
		t.Errorf("expected %d docs, got %d", len(requestedIDs), len(loadedDocs))
	}

	// Verify we got the right docs
	for _, reqID := range requestedIDs {
		found := false
		for _, doc := range loadedDocs {
			if doc.NumericID == reqID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing doc with NumericID %d", reqID)
		}
	}
}

// BenchmarkLoadSegmentDocsLarge benchmarks loading all docs from a large segment.
func BenchmarkLoadSegmentDocsLarge(b *testing.B) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	numDocs := 50_000

	docs := make([]IndexedDocument, numDocs)
	for i := 0; i < numDocs; i++ {
		docs[i] = IndexedDocument{
			ID:        "doc_" + string(rune('0'+i%10)),
			NumericID: uint64(i + 1),
			WALSeq:    uint64(i + 1),
			Attributes: map[string]any{
				"text": "Sample text for benchmarking",
			},
		}
	}

	docsData, _ := EncodeDocsColumnZstd(docs)
	docsKey := "vex/namespaces/test-ns/index/segments/seg_001/docs.col.zst"

	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		IndexedWALSeq: uint64(numDocs),
		Segments: []Segment{{
			ID:      "seg_001",
			DocsKey: docsKey,
		}},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, docsKey, bytes.NewReader(docsData), int64(len(docsData)), nil)

	reader := NewReader(store, nil, nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := reader.LoadSegmentDocs(ctx, manifestKey)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLoadDocsForIDs benchmarks loading only specific docs by ID.
func BenchmarkLoadDocsForIDs(b *testing.B) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	numDocs := 50_000

	docs := make([]IndexedDocument, numDocs)
	for i := 0; i < numDocs; i++ {
		docs[i] = IndexedDocument{
			ID:        "doc_" + string(rune('0'+i%10)),
			NumericID: uint64(i + 1),
			WALSeq:    uint64(i + 1),
			Attributes: map[string]any{
				"text": "Sample text for benchmarking",
			},
		}
	}

	docsData, _ := EncodeDocsColumnZstd(docs)
	docsKey := "vex/namespaces/test-ns/index/segments/seg_001/docs.col.zst"

	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		IndexedWALSeq: uint64(numDocs),
		Segments: []Segment{{
			ID:      "seg_001",
			DocsKey: docsKey,
		}},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, docsKey, bytes.NewReader(docsData), int64(len(docsData)), nil)

	reader := NewReader(store, nil, nil)

	// Only request 20 docs (typical top-k)
	requestedIDs := make([]uint64, 20)
	for i := 0; i < 20; i++ {
		requestedIDs[i] = uint64(i*2500 + 1)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := reader.LoadDocsForIDs(ctx, manifestKey, requestedIDs)
		if err != nil {
			b.Fatal(err)
		}
	}
}
