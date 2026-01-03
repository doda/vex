package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestReaderLoadManifest tests manifest loading from object storage.
func TestReaderLoadManifest(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create a test manifest
	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: 10,
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
			},
		},
		Stats: Stats{
			SegmentCount: 1,
		},
	}

	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}

	manifestKey := "vex/namespaces/test-ns/index/manifests/000000000000000001.idx.json"
	_, err = store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	if err != nil {
		t.Fatalf("failed to put manifest: %v", err)
	}

	reader := NewReader(store, nil, nil)

	loaded, err := reader.LoadManifest(ctx, manifestKey)
	if err != nil {
		t.Fatalf("LoadManifest() error = %v", err)
	}

	if loaded.Namespace != "test-ns" {
		t.Errorf("expected namespace 'test-ns', got '%s'", loaded.Namespace)
	}
	if loaded.IndexedWALSeq != 10 {
		t.Errorf("expected IndexedWALSeq 10, got %d", loaded.IndexedWALSeq)
	}
	if len(loaded.Segments) != 1 {
		t.Errorf("expected 1 segment, got %d", len(loaded.Segments))
	}
}

// TestLoadManifestNotFound tests manifest loading when the manifest doesn't exist.
func TestLoadManifestNotFound(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	reader := NewReader(store, nil, nil)

	loaded, err := reader.LoadManifest(ctx, "nonexistent-key")
	if err != nil {
		t.Fatalf("LoadManifest() error = %v", err)
	}
	if loaded != nil {
		t.Errorf("expected nil manifest for nonexistent key, got %+v", loaded)
	}
}

// TestGetIVFReader tests loading an IVF reader from object storage.
func TestGetIVFReader(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create test IVF data
	dims := 4
	nClusters := 2

	// Create centroids file
	centroidsData := createTestCentroidsFile(t, dims, nClusters, vector.MetricCosineDistance)

	// Create cluster offsets file
	offsets := []vector.ClusterOffset{
		{Offset: 0, Length: uint64((8 + dims*4) * 2), DocCount: 2},                        // Cluster 0: 2 docs
		{Offset: uint64((8 + dims*4) * 2), Length: uint64((8 + dims*4) * 3), DocCount: 3}, // Cluster 1: 3 docs
	}
	offsetsData := createTestOffsetsFile(t, offsets)

	// Create cluster data file
	clusterData := createTestClusterData(t, dims, offsets)

	// Create manifest with IVF keys
	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: 10,
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
				IVFKeys: &IVFKeys{
					CentroidsKey:      "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin",
					ClusterOffsetsKey: "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin",
					ClusterDataKey:    "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack",
					NClusters:         nClusters,
					VectorCount:       5,
				},
			},
		},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	// Store all objects
	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.CentroidsKey, bytes.NewReader(centroidsData), int64(len(centroidsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterOffsetsKey, bytes.NewReader(offsetsData), int64(len(offsetsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterDataKey, bytes.NewReader(clusterData), int64(len(clusterData)), nil)

	reader := NewReader(store, nil, nil)

	ivfReader, clusterDataKey, err := reader.GetIVFReader(ctx, "test-ns", manifestKey, 1)
	if err != nil {
		t.Fatalf("GetIVFReader() error = %v", err)
	}
	if ivfReader == nil {
		t.Fatal("expected non-nil IVF reader")
	}
	if clusterDataKey != manifest.Segments[0].IVFKeys.ClusterDataKey {
		t.Errorf("expected cluster data key '%s', got '%s'", manifest.Segments[0].IVFKeys.ClusterDataKey, clusterDataKey)
	}
	if ivfReader.Dims != dims {
		t.Errorf("expected dims %d, got %d", dims, ivfReader.Dims)
	}
	if ivfReader.NClusters != nClusters {
		t.Errorf("expected %d clusters, got %d", nClusters, ivfReader.NClusters)
	}
}

// TestSearchWithMultiRange tests ANN search using multi-range optimization.
func TestSearchWithMultiRange(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	dims := 4
	nClusters := 2

	// Create test vectors
	// Cluster 0 centroid: [1, 0, 0, 0]
	// Cluster 1 centroid: [0, 1, 0, 0]
	centroid0 := []float32{1, 0, 0, 0}
	centroid1 := []float32{0, 1, 0, 0}

	// Create centroids file
	centroidsData := createTestCentroidsFileWithCentroids(t, dims, [][]float32{centroid0, centroid1}, vector.MetricCosineDistance)

	// Cluster 0 contains doc 1, 2 near centroid 0
	// Cluster 1 contains doc 3, 4, 5 near centroid 1
	cluster0Docs := []struct {
		docID uint64
		vec   []float32
	}{
		{1, []float32{0.9, 0.1, 0, 0}},
		{2, []float32{0.95, 0.05, 0, 0}},
	}
	cluster1Docs := []struct {
		docID uint64
		vec   []float32
	}{
		{3, []float32{0.1, 0.9, 0, 0}},
		{4, []float32{0.05, 0.95, 0, 0}},
		{5, []float32{0.15, 0.85, 0, 0}},
	}

	// Create cluster data
	cluster0Data := createClusterDocsData(t, dims, cluster0Docs)
	cluster1Data := createClusterDocsData(t, dims, cluster1Docs)
	allClusterData := append(cluster0Data, cluster1Data...)

	// Create cluster offsets
	offsets := []vector.ClusterOffset{
		{Offset: 0, Length: uint64(len(cluster0Data)), DocCount: uint32(len(cluster0Docs))},
		{Offset: uint64(len(cluster0Data)), Length: uint64(len(cluster1Data)), DocCount: uint32(len(cluster1Docs))},
	}
	offsetsData := createTestOffsetsFile(t, offsets)

	// Create manifest with IVF keys
	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: 10,
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
				IVFKeys: &IVFKeys{
					CentroidsKey:      "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin",
					ClusterOffsetsKey: "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin",
					ClusterDataKey:    "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack",
					NClusters:         nClusters,
					VectorCount:       5,
				},
			},
		},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	// Store all objects
	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.CentroidsKey, bytes.NewReader(centroidsData), int64(len(centroidsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterOffsetsKey, bytes.NewReader(offsetsData), int64(len(offsetsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterDataKey, bytes.NewReader(allClusterData), int64(len(allClusterData)), nil)

	reader := NewReader(store, nil, nil)

	ivfReader, clusterDataKey, err := reader.GetIVFReader(ctx, "test-ns", manifestKey, 1)
	if err != nil {
		t.Fatalf("GetIVFReader() error = %v", err)
	}

	// Query vector close to centroid 0
	queryVec := []float32{1, 0, 0, 0}
	nProbe := 1 // Only search nearest cluster
	topK := 3

	results, err := reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, topK, nProbe)
	if err != nil {
		t.Fatalf("SearchWithMultiRange() error = %v", err)
	}

	// With nProbe=1, we should only search cluster 0 (nearest to query)
	// So we should get docs 1 and 2
	if len(results) != 2 {
		t.Errorf("expected 2 results with nProbe=1, got %d", len(results))
	}

	// Doc 2 should be first (closest to query: [0.95, 0.05, 0, 0])
	if len(results) > 0 && results[0].DocID != 2 {
		t.Errorf("expected first result to be doc 2, got %d", results[0].DocID)
	}

	// Now test with nProbe=2 to search both clusters
	nProbe = 2
	topK = 5

	results, err = reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, topK, nProbe)
	if err != nil {
		t.Fatalf("SearchWithMultiRange() error = %v", err)
	}

	// Should get all 5 documents
	if len(results) != 5 {
		t.Errorf("expected 5 results with nProbe=2, got %d", len(results))
	}

	// Results should be sorted by distance (ascending)
	for i := 1; i < len(results); i++ {
		if results[i].Distance < results[i-1].Distance {
			t.Errorf("results not sorted: result[%d].Distance (%f) < result[%d].Distance (%f)",
				i, results[i].Distance, i-1, results[i-1].Distance)
		}
	}
}

func TestANNTopKHeapBoundsCandidates(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	dims := 8
	nClusters := 2
	topK := 5
	nProbe := 2
	docsPerCluster := 2000

	centroid0 := make([]float32, dims)
	centroid1 := make([]float32, dims)
	centroid0[0] = 1
	centroid1[1] = 1

	centroidsData := createTestCentroidsFileWithCentroids(t, dims, [][]float32{centroid0, centroid1}, vector.MetricCosineDistance)

	cluster0Data := createUniformClusterData(dims, docsPerCluster, 1, 0.9)
	cluster1Data := createUniformClusterData(dims, docsPerCluster, docsPerCluster+1, 0.1)
	allClusterData := append(cluster0Data, cluster1Data...)

	offsets := []vector.ClusterOffset{
		{Offset: 0, Length: uint64(len(cluster0Data)), DocCount: uint32(docsPerCluster)},
		{Offset: uint64(len(cluster0Data)), Length: uint64(len(cluster1Data)), DocCount: uint32(docsPerCluster)},
	}
	offsetsData := createTestOffsetsFile(t, offsets)

	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: 10,
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
				IVFKeys: &IVFKeys{
					CentroidsKey:      "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin",
					ClusterOffsetsKey: "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin",
					ClusterDataKey:    "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack",
					NClusters:         nClusters,
					VectorCount:       docsPerCluster * nClusters,
				},
			},
		},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.CentroidsKey, bytes.NewReader(centroidsData), int64(len(centroidsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterOffsetsKey, bytes.NewReader(offsetsData), int64(len(offsetsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterDataKey, bytes.NewReader(allClusterData), int64(len(allClusterData)), nil)

	reader := NewReader(store, nil, nil)
	ivfReader, clusterDataKey, err := reader.GetIVFReader(ctx, "test-ns", manifestKey, 1)
	if err != nil {
		t.Fatalf("GetIVFReader() error = %v", err)
	}

	queryVec := make([]float32, dims)
	queryVec[0] = 1

	results, maxCandidates, err := reader.searchWithMultiRangeStats(ctx, ivfReader, clusterDataKey, queryVec, topK, nProbe)
	if err != nil {
		t.Fatalf("searchWithMultiRangeStats() error = %v", err)
	}
	if len(results) > topK {
		t.Errorf("expected at most %d results, got %d", topK, len(results))
	}
	if maxCandidates > topK {
		t.Errorf("expected max candidates <= %d, got %d", topK, maxCandidates)
	}
}

func TestANNTopKHeapBoundsStreaming(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	dims := 256
	topK := 5
	nProbe := 1

	entrySize := 8 + dims*4
	docsPerCluster := (MaxClusterSizeForBatch / entrySize) + 128
	if docsPerCluster < topK*4 {
		docsPerCluster = topK * 4
	}

	centroid := make([]float32, dims)
	centroid[0] = 1
	centroidsData := createTestCentroidsFileWithCentroids(t, dims, [][]float32{centroid}, vector.MetricCosineDistance)

	clusterData := createUniformClusterData(dims, docsPerCluster, 1, 0.2)
	offsets := []vector.ClusterOffset{
		{Offset: 0, Length: uint64(len(clusterData)), DocCount: uint32(docsPerCluster)},
	}
	offsetsData := createTestOffsetsFile(t, offsets)

	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: 10,
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
				IVFKeys: &IVFKeys{
					CentroidsKey:      "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin",
					ClusterOffsetsKey: "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin",
					ClusterDataKey:    "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack",
					NClusters:         1,
					VectorCount:       docsPerCluster,
				},
			},
		},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.CentroidsKey, bytes.NewReader(centroidsData), int64(len(centroidsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterOffsetsKey, bytes.NewReader(offsetsData), int64(len(offsetsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterDataKey, bytes.NewReader(clusterData), int64(len(clusterData)), nil)

	reader := NewReader(store, nil, nil)
	ivfReader, clusterDataKey, err := reader.GetIVFReader(ctx, "test-ns", manifestKey, 1)
	if err != nil {
		t.Fatalf("GetIVFReader() error = %v", err)
	}

	queryVec := make([]float32, dims)
	queryVec[0] = 1

	results, maxCandidates, err := reader.searchWithMultiRangeStats(ctx, ivfReader, clusterDataKey, queryVec, topK, nProbe)
	if err != nil {
		t.Fatalf("searchWithMultiRangeStats() error = %v", err)
	}
	if len(results) > topK {
		t.Errorf("expected at most %d results, got %d", topK, len(results))
	}
	if maxCandidates > topK {
		t.Errorf("expected max candidates <= %d, got %d", topK, maxCandidates)
	}
}

func TestStreamingChunkedRangesAligned(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	dims := 32
	topK := 5
	nProbe := 1

	entrySize := 8 + dims*4
	docsPerCluster := (MaxClusterSizeForBatch / entrySize) + 128
	if docsPerCluster < topK*4 {
		docsPerCluster = topK * 4
	}

	clusterData, topIDs := createStreamingClusterData(dims, docsPerCluster, topK)
	centroid := make([]float32, dims)
	centroid[0] = 1
	centroidsData := createTestCentroidsFileWithCentroids(t, dims, [][]float32{centroid}, vector.MetricCosineDistance)
	offsets := []vector.ClusterOffset{
		{Offset: 0, Length: uint64(len(clusterData)), DocCount: uint32(docsPerCluster)},
	}
	offsetsData := createTestOffsetsFile(t, offsets)

	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: 10,
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
				IVFKeys: &IVFKeys{
					CentroidsKey:      "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin",
					ClusterOffsetsKey: "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin",
					ClusterDataKey:    "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack",
					NClusters:         1,
					VectorCount:       docsPerCluster,
				},
			},
		},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.CentroidsKey, bytes.NewReader(centroidsData), int64(len(centroidsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterOffsetsKey, bytes.NewReader(offsetsData), int64(len(offsetsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterDataKey, bytes.NewReader(clusterData), int64(len(clusterData)), nil)

	rangeStore := newRangeStore(store)
	chunkBytes := entrySize*37 + 13
	reader := NewReaderWithOptions(rangeStore, nil, nil, ReaderOptions{LargeClusterChunkBytes: chunkBytes})
	ivfReader, clusterDataKey, err := reader.GetIVFReader(ctx, "test-ns", manifestKey, 1)
	if err != nil {
		t.Fatalf("GetIVFReader() error = %v", err)
	}

	queryVec := make([]float32, dims)
	queryVec[0] = 1

	results, err := reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, topK, nProbe)
	if err != nil {
		t.Fatalf("SearchWithMultiRange() error = %v", err)
	}
	if len(results) != topK {
		t.Fatalf("expected %d results, got %d", topK, len(results))
	}

	expectedIDs := make(map[uint64]struct{}, len(topIDs))
	for _, id := range topIDs {
		expectedIDs[id] = struct{}{}
	}
	for _, res := range results {
		if _, ok := expectedIDs[res.DocID]; !ok {
			t.Fatalf("unexpected docID %d in top-k results", res.DocID)
		}
		if res.Distance > 0.0001 {
			t.Fatalf("expected top-k distance near zero, got %.4f", res.Distance)
		}
	}

	alignedChunk := (chunkBytes / entrySize) * entrySize
	if alignedChunk == 0 {
		alignedChunk = entrySize
	}
	expectedRanges := make(map[string]struct{})
	for offset := 0; offset < len(clusterData); offset += alignedChunk {
		end := offset + alignedChunk - 1
		if end >= len(clusterData) {
			end = len(clusterData) - 1
		}
		expectedRanges[rangeKeyWithRange(clusterDataKey, int64(offset), int64(end))] = struct{}{}
	}

	var rangeCalls []string
	for _, call := range rangeStore.Calls() {
		if strings.HasPrefix(call, clusterDataKey+"#") {
			rangeCalls = append(rangeCalls, call)
		}
	}
	if len(rangeCalls) != len(expectedRanges) {
		t.Fatalf("expected %d range calls, got %d", len(expectedRanges), len(rangeCalls))
	}

	for call := range expectedRanges {
		start, end := parseRangeBounds(t, call, clusterDataKey)
		if rangeStore.GetCount(clusterDataKey, start, end) != 1 {
			t.Fatalf("expected range call %s to occur once", call)
		}
		if start%int64(entrySize) != 0 {
			t.Fatalf("range start %d not aligned to entry size %d", start, entrySize)
		}
		if (end+1)%int64(entrySize) != 0 {
			t.Fatalf("range end %d not aligned to entry size %d", end, entrySize)
		}
	}
}

func TestSearchWithMultiRangeF16(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	dims := 4
	nClusters := 2

	centroid0 := []float32{1, 0, 0, 0}
	centroid1 := []float32{0, 1, 0, 0}

	centroidsData := createTestCentroidsFileWithDType(t, dims, [][]float32{centroid0, centroid1}, vector.MetricCosineDistance, vector.DTypeF16)

	cluster0Docs := []struct {
		docID uint64
		vec   []float32
	}{
		{1, []float32{0.9, 0.1, 0, 0}},
		{2, []float32{0.95, 0.05, 0, 0}},
	}
	cluster1Docs := []struct {
		docID uint64
		vec   []float32
	}{
		{3, []float32{0.1, 0.9, 0, 0}},
		{4, []float32{0.05, 0.95, 0, 0}},
		{5, []float32{0.15, 0.85, 0, 0}},
	}

	cluster0Data := createClusterDocsDataWithDType(t, dims, cluster0Docs, vector.DTypeF16)
	cluster1Data := createClusterDocsDataWithDType(t, dims, cluster1Docs, vector.DTypeF16)
	allClusterData := append(cluster0Data, cluster1Data...)

	offsets := []vector.ClusterOffset{
		{Offset: 0, Length: uint64(len(cluster0Data)), DocCount: uint32(len(cluster0Docs))},
		{Offset: uint64(len(cluster0Data)), Length: uint64(len(cluster1Data)), DocCount: uint32(len(cluster1Docs))},
	}
	offsetsData := createTestOffsetsFile(t, offsets)

	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: 10,
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
				IVFKeys: &IVFKeys{
					CentroidsKey:      "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin",
					ClusterOffsetsKey: "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin",
					ClusterDataKey:    "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack",
					NClusters:         nClusters,
					VectorCount:       5,
				},
			},
		},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.CentroidsKey, bytes.NewReader(centroidsData), int64(len(centroidsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterOffsetsKey, bytes.NewReader(offsetsData), int64(len(offsetsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterDataKey, bytes.NewReader(allClusterData), int64(len(allClusterData)), nil)

	reader := NewReader(store, nil, nil)
	ivfReader, clusterDataKey, err := reader.GetIVFReader(ctx, "test-ns", manifestKey, 1)
	if err != nil {
		t.Fatalf("GetIVFReader() error = %v", err)
	}

	queryVec := []float32{1, 0, 0, 0}
	nProbe := 1
	topK := 3

	results, err := reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, topK, nProbe)
	if err != nil {
		t.Fatalf("SearchWithMultiRange() error = %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results with nProbe=1, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != 2 {
		t.Errorf("expected first result to be doc 2, got %d", results[0].DocID)
	}
}

// TestExactDistanceComputation tests that exact distances are computed in final top_k.
func TestExactDistanceComputation(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	dims := 4
	nClusters := 1

	// Known vectors with known cosine distances from query [1, 0, 0, 0]
	// cosine_distance = 1 - cos(theta) = 1 - dot(a, b) / (|a| * |b|)
	testDocs := []struct {
		docID        uint64
		vec          []float32
		expectedDist float32
	}{
		{1, []float32{1, 0, 0, 0}, 0.0},           // Identical, distance = 0
		{2, []float32{0.707, 0.707, 0, 0}, 0.293}, // 45 degrees, distance ≈ 0.293
		{3, []float32{0, 1, 0, 0}, 1.0},           // Orthogonal, distance = 1
	}

	// Create centroids (single cluster containing all docs)
	centroidsData := createTestCentroidsFileWithCentroids(t, dims, [][]float32{{1, 0, 0, 0}}, vector.MetricCosineDistance)

	// Create cluster data
	docsForCluster := make([]struct {
		docID uint64
		vec   []float32
	}, len(testDocs))
	for i, d := range testDocs {
		docsForCluster[i] = struct {
			docID uint64
			vec   []float32
		}{d.docID, d.vec}
	}
	clusterData := createClusterDocsData(t, dims, docsForCluster)

	offsets := []vector.ClusterOffset{
		{Offset: 0, Length: uint64(len(clusterData)), DocCount: uint32(len(testDocs))},
	}
	offsetsData := createTestOffsetsFile(t, offsets)

	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: 10,
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
				IVFKeys: &IVFKeys{
					CentroidsKey:      "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin",
					ClusterOffsetsKey: "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin",
					ClusterDataKey:    "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack",
					NClusters:         nClusters,
					VectorCount:       len(testDocs),
				},
			},
		},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.CentroidsKey, bytes.NewReader(centroidsData), int64(len(centroidsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterOffsetsKey, bytes.NewReader(offsetsData), int64(len(offsetsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterDataKey, bytes.NewReader(clusterData), int64(len(clusterData)), nil)

	reader := NewReader(store, nil, nil)
	ivfReader, clusterDataKey, _ := reader.GetIVFReader(ctx, "test-ns", manifestKey, 1)

	queryVec := []float32{1, 0, 0, 0}
	results, err := reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, 10, 1)
	if err != nil {
		t.Fatalf("SearchWithMultiRange() error = %v", err)
	}

	// Verify exact distances match expected values
	for _, res := range results {
		var expected float32
		for _, d := range testDocs {
			if d.docID == res.DocID {
				expected = d.expectedDist
				break
			}
		}
		if math.Abs(float64(res.Distance-expected)) > 0.01 {
			t.Errorf("DocID %d: expected distance %.3f, got %.3f", res.DocID, expected, res.Distance)
		}
	}
}

// TestNProbeSelection tests that nProbe centroids are searched.
func TestNProbeSelection(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	dims := 4
	nClusters := 5

	// Create 5 distinct centroids spread along different axes
	centroids := [][]float32{
		{1, 0, 0, 0},
		{0, 1, 0, 0},
		{0, 0, 1, 0},
		{0, 0, 0, 1},
		{0.5, 0.5, 0.5, 0.5},
	}

	centroidsData := createTestCentroidsFileWithCentroids(t, dims, centroids, vector.MetricCosineDistance)

	// Each cluster has one doc
	allClusterData := []byte{}
	offsets := make([]vector.ClusterOffset, nClusters)
	for i := 0; i < nClusters; i++ {
		docData := createClusterDocsData(t, dims, []struct {
			docID uint64
			vec   []float32
		}{{uint64(i + 1), centroids[i]}})
		offsets[i] = vector.ClusterOffset{
			Offset:   uint64(len(allClusterData)),
			Length:   uint64(len(docData)),
			DocCount: 1,
		}
		allClusterData = append(allClusterData, docData...)
	}
	offsetsData := createTestOffsetsFile(t, offsets)

	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: 10,
		Segments: []Segment{
			{
				ID:          "seg_001",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
				IVFKeys: &IVFKeys{
					CentroidsKey:      "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin",
					ClusterOffsetsKey: "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin",
					ClusterDataKey:    "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack",
					NClusters:         nClusters,
					VectorCount:       nClusters,
				},
			},
		},
	}

	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey("test-ns", 1)

	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.CentroidsKey, bytes.NewReader(centroidsData), int64(len(centroidsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterOffsetsKey, bytes.NewReader(offsetsData), int64(len(offsetsData)), nil)
	store.Put(ctx, manifest.Segments[0].IVFKeys.ClusterDataKey, bytes.NewReader(allClusterData), int64(len(allClusterData)), nil)

	reader := NewReader(store, nil, nil)
	ivfReader, clusterDataKey, _ := reader.GetIVFReader(ctx, "test-ns", manifestKey, 1)

	// Query along the first axis [1, 0, 0, 0]
	queryVec := []float32{1, 0, 0, 0}

	// Test nProbe=1: should only return doc 1 (cluster 0)
	results, err := reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, 10, 1)
	if err != nil {
		t.Fatalf("SearchWithMultiRange(nProbe=1) error = %v", err)
	}
	if len(results) != 1 {
		t.Errorf("nProbe=1: expected 1 result, got %d", len(results))
	}
	if len(results) > 0 && results[0].DocID != 1 {
		t.Errorf("nProbe=1: expected doc 1, got doc %d", results[0].DocID)
	}

	// Test nProbe=2: should return docs 1 and 5 (cluster 0 and cluster 4 are closest)
	results, err = reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, 10, 2)
	if err != nil {
		t.Fatalf("SearchWithMultiRange(nProbe=2) error = %v", err)
	}
	if len(results) != 2 {
		t.Errorf("nProbe=2: expected 2 results, got %d", len(results))
	}

	// Test nProbe=5: should return all 5 docs
	results, err = reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, 10, 5)
	if err != nil {
		t.Fatalf("SearchWithMultiRange(nProbe=5) error = %v", err)
	}
	if len(results) != 5 {
		t.Errorf("nProbe=5: expected 5 results, got %d", len(results))
	}
}

// TestMultiRangeFetcher tests the multi-range fetcher for cluster data.
func TestMultiRangeFetcher(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create test cluster data with 3 clusters
	dims := 4
	cluster0 := createClusterDocsData(t, dims, []struct {
		docID uint64
		vec   []float32
	}{{1, []float32{1, 0, 0, 0}}})
	cluster1 := createClusterDocsData(t, dims, []struct {
		docID uint64
		vec   []float32
	}{{2, []float32{0, 1, 0, 0}}})
	cluster2 := createClusterDocsData(t, dims, []struct {
		docID uint64
		vec   []float32
	}{{3, []float32{0, 0, 1, 0}}})

	allData := append(append(cluster0, cluster1...), cluster2...)
	clusterDataKey := "test-cluster-data.pack"
	store.Put(ctx, clusterDataKey, bytes.NewReader(allData), int64(len(allData)), nil)

	fetcher := NewMultiRangeClusterDataFetcher(store, nil, clusterDataKey)

	// Fetch ranges for clusters 0 and 2 (skipping 1)
	ranges := []vector.ByteRange{
		{Offset: 0, Length: uint64(len(cluster0)), ClusterID: 0},
		{Offset: uint64(len(cluster0) + len(cluster1)), Length: uint64(len(cluster2)), ClusterID: 2},
	}

	result, err := fetcher.FetchRanges(ctx, ranges)
	if err != nil {
		t.Fatalf("FetchRanges() error = %v", err)
	}

	if len(result) != 2 {
		t.Errorf("expected 2 cluster data entries, got %d", len(result))
	}

	if !bytes.Equal(result[0], cluster0) {
		t.Errorf("cluster 0 data mismatch")
	}
	if !bytes.Equal(result[2], cluster2) {
		t.Errorf("cluster 2 data mismatch")
	}
}

type rangeBlock struct {
	started chan struct{}
	unblock chan struct{}
	once    sync.Once
}

type rangeStore struct {
	inner  objectstore.Store
	mu     sync.Mutex
	blocks map[string]*rangeBlock
	counts map[string]int
	calls  []string
}

func newRangeStore(inner objectstore.Store) *rangeStore {
	return &rangeStore{
		inner:  inner,
		blocks: make(map[string]*rangeBlock),
		counts: make(map[string]int),
	}
}

func rangeKey(key string, opts *objectstore.GetOptions) string {
	if opts == nil || opts.Range == nil {
		return key
	}
	return fmt.Sprintf("%s#%d-%d", key, opts.Range.Start, opts.Range.End)
}

func rangeKeyWithRange(key string, start, end int64) string {
	return fmt.Sprintf("%s#%d-%d", key, start, end)
}

func (s *rangeStore) BlockRange(key string, start, end int64) (<-chan struct{}, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	started := make(chan struct{})
	unblock := make(chan struct{})
	s.blocks[rangeKeyWithRange(key, start, end)] = &rangeBlock{
		started: started,
		unblock: unblock,
	}
	return started, unblock
}

func (s *rangeStore) GetCount(key string, start, end int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[rangeKeyWithRange(key, start, end)]
}

func (s *rangeStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	rKey := rangeKey(key, opts)
	s.mu.Lock()
	s.counts[rKey]++
	s.calls = append(s.calls, rKey)
	block := s.blocks[rKey]
	s.mu.Unlock()

	if block != nil {
		block.once.Do(func() {
			close(block.started)
		})
		select {
		case <-block.unblock:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}

	return s.inner.Get(ctx, key, opts)
}

func (s *rangeStore) Calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := make([]string, len(s.calls))
	copy(calls, s.calls)
	return calls
}

func parseRangeBounds(t *testing.T, call, key string) (int64, int64) {
	t.Helper()
	prefix := key + "#"
	if !strings.HasPrefix(call, prefix) {
		t.Fatalf("unexpected range call %s for key %s", call, key)
	}
	rangePart := strings.TrimPrefix(call, prefix)
	parts := strings.SplitN(rangePart, "-", 2)
	if len(parts) != 2 {
		t.Fatalf("invalid range call %s", call)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		t.Fatalf("invalid range start %s: %v", parts[0], err)
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		t.Fatalf("invalid range end %s: %v", parts[1], err)
	}
	return start, end
}

func (s *rangeStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	return s.inner.Head(ctx, key)
}

func (s *rangeStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	return s.inner.Put(ctx, key, body, size, opts)
}

func (s *rangeStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	return s.inner.PutIfAbsent(ctx, key, body, size, opts)
}

func (s *rangeStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	return s.inner.PutIfMatch(ctx, key, body, size, etag, opts)
}

func (s *rangeStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s *rangeStore) List(ctx context.Context, opts *objectstore.ListOptions) (*objectstore.ListResult, error) {
	return s.inner.List(ctx, opts)
}

func TestMultiRangeFetcherParallel(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	dims := 4
	cluster0 := createClusterDocsData(t, dims, []struct {
		docID uint64
		vec   []float32
	}{{1, []float32{1, 0, 0, 0}}})
	cluster1 := createClusterDocsData(t, dims, []struct {
		docID uint64
		vec   []float32
	}{{2, []float32{0, 1, 0, 0}}})

	gap := 8192
	padding := make([]byte, gap)
	allData := append(cluster0, padding...)
	offset1 := len(allData)
	allData = append(allData, cluster1...)

	clusterDataKey := "test-cluster-data-parallel.pack"
	store.Put(ctx, clusterDataKey, bytes.NewReader(allData), int64(len(allData)), nil)

	rangeStore := newRangeStore(store)
	start0, unblock0 := rangeStore.BlockRange(clusterDataKey, 0, int64(len(cluster0)-1))
	start1, unblock1 := rangeStore.BlockRange(clusterDataKey, int64(offset1), int64(offset1+len(cluster1)-1))

	fetcher := NewMultiRangeClusterDataFetcher(rangeStore, nil, clusterDataKey)
	fetcher.maxParallel = 2

	ranges := []vector.ByteRange{
		{Offset: 0, Length: uint64(len(cluster0)), ClusterID: 0},
		{Offset: uint64(offset1), Length: uint64(len(cluster1)), ClusterID: 1},
	}

	type fetchResult struct {
		data map[int][]byte
		err  error
	}
	done := make(chan fetchResult, 1)
	go func() {
		data, err := fetcher.FetchRanges(ctx, ranges)
		done <- fetchResult{data: data, err: err}
	}()

	waitForStart := func(ch <-chan struct{}, label string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("expected %s range fetch to start in parallel", label)
		}
	}

	waitForStart(start0, "first")
	waitForStart(start1, "second")

	close(unblock0)
	close(unblock1)

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("FetchRanges() error = %v", res.err)
		}
		if !bytes.Equal(res.data[0], cluster0) {
			t.Errorf("cluster 0 data mismatch")
		}
		if !bytes.Equal(res.data[1], cluster1) {
			t.Errorf("cluster 1 data mismatch")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FetchRanges() did not complete")
	}
}

func TestMultiRangeFetcherCache(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	dims := 4
	cluster0 := createClusterDocsData(t, dims, []struct {
		docID uint64
		vec   []float32
	}{{1, []float32{1, 0, 0, 0}}})
	cluster1 := createClusterDocsData(t, dims, []struct {
		docID uint64
		vec   []float32
	}{{2, []float32{0, 1, 0, 0}}})

	allData := append(cluster0, cluster1...)
	clusterDataKey := "test-cluster-data-cache.pack"
	store.Put(ctx, clusterDataKey, bytes.NewReader(allData), int64(len(allData)), nil)

	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: t.TempDir(),
		MaxBytes: int64(len(allData)) * 2,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	cacheKey := cache.CacheKey{
		ObjectKey: fmt.Sprintf("%s#%d-%d", clusterDataKey, 0, len(cluster0)),
	}
	diskCache.PutBytes(cacheKey, cluster0)

	rangeStore := newRangeStore(store)
	fetcher := NewMultiRangeClusterDataFetcher(rangeStore, diskCache, clusterDataKey)

	ranges := []vector.ByteRange{
		{Offset: 0, Length: uint64(len(cluster0)), ClusterID: 0},
		{Offset: uint64(len(cluster0)), Length: uint64(len(cluster1)), ClusterID: 1},
	}

	result, err := fetcher.FetchRanges(ctx, ranges)
	if err != nil {
		t.Fatalf("FetchRanges() error = %v", err)
	}

	if !bytes.Equal(result[0], cluster0) {
		t.Errorf("cluster 0 data mismatch")
	}
	if !bytes.Equal(result[1], cluster1) {
		t.Errorf("cluster 1 data mismatch")
	}

	if got := rangeStore.GetCount(clusterDataKey, 0, int64(len(cluster0)-1)); got != 0 {
		t.Errorf("expected cached range to avoid store fetch, got %d", got)
	}
	if got := rangeStore.GetCount(clusterDataKey, int64(len(cluster0)), int64(len(allData)-1)); got != 1 {
		t.Errorf("expected uncached range to fetch once, got %d", got)
	}
}

// Helper functions to create test data

func createTestCentroidsFile(t *testing.T, dims, nClusters int, metric vector.DistanceMetric) []byte {
	centroids := make([][]float32, nClusters)
	for i := 0; i < nClusters; i++ {
		centroids[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			if j == i%dims {
				centroids[i][j] = 1.0
			}
		}
	}
	return createTestCentroidsFileWithCentroids(t, dims, centroids, metric)
}

func createTestCentroidsFileWithCentroids(t *testing.T, dims int, centroids [][]float32, metric vector.DistanceMetric) []byte {
	return createTestCentroidsFileWithDType(t, dims, centroids, metric, vector.DTypeF32)
}

func createTestCentroidsFileWithDType(t *testing.T, dims int, centroids [][]float32, metric vector.DistanceMetric, dtype vector.DType) []byte {
	// Build an IVFIndex and use its serialization methods
	idx := &vector.IVFIndex{
		Dims:      dims,
		DType:     dtype,
		Metric:    metric,
		NClusters: len(centroids),
		Centroids: make([]float32, 0, dims*len(centroids)),
	}
	for _, c := range centroids {
		idx.Centroids = append(idx.Centroids, c...)
	}

	buf := &bytes.Buffer{}
	if err := idx.WriteCentroidsFile(buf); err != nil {
		t.Fatalf("failed to write centroids file: %v", err)
	}
	return buf.Bytes()
}

func createTestOffsetsFile(t *testing.T, offsets []vector.ClusterOffset) []byte {
	// Build a minimal IVFIndex to use its serialization methods
	idx := &vector.IVFIndex{
		NClusters:      len(offsets),
		ClusterOffsets: offsets,
	}

	buf := &bytes.Buffer{}
	if err := idx.WriteClusterOffsetsFile(buf); err != nil {
		t.Fatalf("failed to write offsets file: %v", err)
	}
	return buf.Bytes()
}

func createTestClusterData(t *testing.T, dims int, offsets []vector.ClusterOffset) []byte {
	totalSize := uint64(0)
	for _, o := range offsets {
		totalSize += o.Length
	}
	data := make([]byte, totalSize)

	// Fill with placeholder data (docID + vector)
	pos := 0
	for clusterID, o := range offsets {
		for i := 0; i < int(o.DocCount); i++ {
			docID := uint64(clusterID*100 + i + 1)
			binary.LittleEndian.PutUint64(data[pos:], docID)
			pos += 8
			for j := 0; j < dims; j++ {
				binary.LittleEndian.PutUint32(data[pos:], math.Float32bits(float32(clusterID)))
				pos += 4
			}
		}
	}

	return data
}

func createClusterDocsData(t *testing.T, dims int, docs []struct {
	docID uint64
	vec   []float32
}) []byte {
	return createClusterDocsDataWithDType(t, dims, docs, vector.DTypeF32)
}

func createClusterDocsDataWithDType(t *testing.T, dims int, docs []struct {
	docID uint64
	vec   []float32
}, dtype vector.DType) []byte {
	buf := &bytes.Buffer{}

	for _, doc := range docs {
		binary.Write(buf, binary.LittleEndian, doc.docID)
		switch dtype {
		case vector.DTypeF16:
			for _, v := range doc.vec {
				binary.Write(buf, binary.LittleEndian, vector.Float32ToFloat16(v))
			}
		default:
			for _, v := range doc.vec {
				binary.Write(buf, binary.LittleEndian, v)
			}
		}
	}

	return buf.Bytes()
}

func createUniformClusterData(dims, docs int, startID int, value float32) []byte {
	entrySize := 8 + dims*4
	data := make([]byte, entrySize*docs)
	pos := 0
	valueBits := math.Float32bits(value)

	for i := 0; i < docs; i++ {
		binary.LittleEndian.PutUint64(data[pos:], uint64(startID+i))
		pos += 8
		for j := 0; j < dims; j++ {
			binary.LittleEndian.PutUint32(data[pos:], valueBits)
			pos += 4
		}
	}

	return data
}

func createStreamingClusterData(dims, docs, topK int) ([]byte, []uint64) {
	entrySize := 8 + dims*4
	data := make([]byte, entrySize*docs)
	pos := 0
	topIDs := make([]uint64, 0, topK)

	for i := 0; i < docs; i++ {
		docID := uint64(i + 1)
		binary.LittleEndian.PutUint64(data[pos:], docID)
		pos += 8

		isTop := i >= docs-topK
		if isTop {
			topIDs = append(topIDs, docID)
		}

		for j := 0; j < dims; j++ {
			var v float32
			switch j {
			case 0:
				if isTop {
					v = 1
				}
			case 1:
				if !isTop {
					v = 1
				}
			}
			binary.LittleEndian.PutUint32(data[pos:], math.Float32bits(v))
			pos += 4
		}
	}

	return data, topIDs
}
