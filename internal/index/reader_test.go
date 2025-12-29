package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"testing"
	"time"

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
