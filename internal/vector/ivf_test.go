package vector

import (
	"bytes"
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func TestIVFBuilder_Basic(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)

	// Add some vectors
	err := builder.AddVector(1, []float32{1, 0, 0, 0})
	if err != nil {
		t.Fatalf("AddVector failed: %v", err)
	}
	err = builder.AddVector(2, []float32{0.9, 0.1, 0, 0})
	if err != nil {
		t.Fatalf("AddVector failed: %v", err)
	}
	err = builder.AddVector(3, []float32{0, 1, 0, 0})
	if err != nil {
		t.Fatalf("AddVector failed: %v", err)
	}
	err = builder.AddVector(4, []float32{0, 0.9, 0.1, 0})
	if err != nil {
		t.Fatalf("AddVector failed: %v", err)
	}

	if builder.Count() != 4 {
		t.Errorf("expected 4 vectors, got %d", builder.Count())
	}

	idx, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if idx.Dims != dims {
		t.Errorf("expected dims=%d, got %d", dims, idx.Dims)
	}
	if idx.NClusters != 2 {
		t.Errorf("expected 2 clusters, got %d", idx.NClusters)
	}
	if idx.TotalVectorCount() != 4 {
		t.Errorf("expected 4 total vectors, got %d", idx.TotalVectorCount())
	}
}

func TestIVFBuilder_DimensionMismatch(t *testing.T) {
	builder := NewIVFBuilder(4, MetricCosineDistance, 2)

	// Try to add a vector with wrong dimensions
	err := builder.AddVector(1, []float32{1, 0, 0})
	if err == nil {
		t.Error("expected error for dimension mismatch")
	}
}

func TestIVFBuilder_EmptyBuild(t *testing.T) {
	builder := NewIVFBuilder(4, MetricCosineDistance, 2)

	_, err := builder.Build()
	if err != ErrNoVectors {
		t.Errorf("expected ErrNoVectors, got %v", err)
	}
}

func TestIVFBuilder_BuildWithSinglePassStreaming(t *testing.T) {
	dims := 3
	nClusters := 2
	vectors := [][]float32{
		{1, 0, 0},
		{0.9, 0.1, 0},
		{0, 1, 0},
		{0, 0.9, 0.1},
		{0, 0, 1},
	}

	rand.Seed(1)
	builder := NewIVFBuilder(dims, MetricCosineDistance, nClusters)
	for i, vec := range vectors {
		if err := builder.AddVector(uint64(i+1), vec); err != nil {
			t.Fatalf("AddVector failed: %v", err)
		}
	}
	memIdx, err := builder.BuildWithSinglePass()
	if err != nil {
		t.Fatalf("BuildWithSinglePass failed: %v", err)
	}

	rand.Seed(1)
	streamBuilder := NewIVFBuilder(dims, MetricCosineDistance, nClusters)
	for i, vec := range vectors {
		if err := streamBuilder.AddVector(uint64(i+1), vec); err != nil {
			t.Fatalf("AddVector failed: %v", err)
		}
	}

	var buf bytes.Buffer
	streamIdx, size, err := streamBuilder.BuildWithSinglePassStreaming(&buf)
	if err != nil {
		t.Fatalf("BuildWithSinglePassStreaming failed: %v", err)
	}

	if size != int64(buf.Len()) {
		t.Fatalf("expected stream size %d, got %d", buf.Len(), size)
	}

	if streamIdx.Dims != memIdx.Dims || streamIdx.NClusters != memIdx.NClusters {
		t.Fatalf("streamed index dims/clusters mismatch: dims %d/%d clusters %d/%d",
			streamIdx.Dims, memIdx.Dims, streamIdx.NClusters, memIdx.NClusters)
	}

	if !reflect.DeepEqual(streamIdx.ClusterOffsets, memIdx.ClusterOffsets) {
		t.Fatalf("cluster offsets mismatch between streaming and in-memory builds")
	}

	if !bytes.Equal(buf.Bytes(), memIdx.GetClusterDataBytes()) {
		t.Fatalf("cluster data mismatch between streaming and in-memory builds")
	}
}

func TestIVFBuilder_FewVectors(t *testing.T) {
	// Test when we have fewer vectors than clusters
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 10)

	// Add only 3 vectors
	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0, 1, 0, 0})
	builder.AddVector(3, []float32{0, 0, 1, 0})

	idx, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Should have reduced to 3 clusters
	if idx.NClusters != 3 {
		t.Errorf("expected 3 clusters, got %d", idx.NClusters)
	}
}

func TestIVFIndex_Search(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)

	// Add clustered vectors
	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0.9, 0.1, 0, 0})
	builder.AddVector(3, []float32{0, 1, 0, 0})
	builder.AddVector(4, []float32{0, 0.9, 0.1, 0})

	idx, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Search for vector similar to first cluster
	query := []float32{0.95, 0.05, 0, 0}
	results, err := idx.Search(query, 2, 2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// First result should be closest to query
	if results[0].Distance > results[1].Distance {
		t.Error("results not sorted by distance")
	}

	// Results should include docs from first cluster
	foundDoc1Or2 := false
	for _, r := range results {
		if r.DocID == 1 || r.DocID == 2 {
			foundDoc1Or2 = true
			break
		}
	}
	if !foundDoc1Or2 {
		t.Error("expected to find doc 1 or 2 in results")
	}
}

func TestIVFIndex_SearchQueryDimMismatch(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)
	builder.AddVector(1, []float32{1, 0, 0, 0})

	idx, _ := builder.Build()

	// Query with wrong dimensions
	_, err := idx.Search([]float32{1, 0, 0}, 1, 1)
	if err == nil {
		t.Error("expected error for query dimension mismatch")
	}
}

func TestIVFIndex_EuclideanMetric(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricEuclideanSquared, 2)

	builder.AddVector(1, []float32{0, 0, 0, 0})
	builder.AddVector(2, []float32{1, 1, 1, 1})

	idx, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Query close to origin
	results, err := idx.Search([]float32{0.1, 0.1, 0.1, 0.1}, 1, 2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Should find vector at origin
	if results[0].DocID != 1 {
		t.Errorf("expected doc 1, got doc %d", results[0].DocID)
	}
}

func TestIVFIndex_DotProductMetric(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricDotProduct, 2)

	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0, 1, 0, 0})

	idx, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Query aligned with doc 1
	results, err := idx.Search([]float32{1, 0, 0, 0}, 1, 2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Should find doc 1 (highest dot product)
	if results[0].DocID != 1 {
		t.Errorf("expected doc 1, got doc %d", results[0].DocID)
	}
}

func TestIVFIndex_WriteCentroidsFile(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)

	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0, 1, 0, 0})

	idx, _ := builder.Build()

	// Write centroids file
	var buf bytes.Buffer
	err := idx.WriteCentroidsFile(&buf)
	if err != nil {
		t.Fatalf("WriteCentroidsFile failed: %v", err)
	}

	// Read it back
	readDims, readNClusters, readMetric, readDType, readCentroids, err := ReadCentroidsFile(&buf)
	if err != nil {
		t.Fatalf("ReadCentroidsFile failed: %v", err)
	}

	if readDims != dims {
		t.Errorf("dims mismatch: expected %d, got %d", dims, readDims)
	}
	if readNClusters != idx.NClusters {
		t.Errorf("nClusters mismatch: expected %d, got %d", idx.NClusters, readNClusters)
	}
	if readMetric != MetricCosineDistance {
		t.Errorf("metric mismatch: expected %s, got %s", MetricCosineDistance, readMetric)
	}
	if readDType != DTypeF32 {
		t.Errorf("dtype mismatch: expected %s, got %s", DTypeF32, readDType)
	}
	if len(readCentroids) != len(idx.Centroids) {
		t.Errorf("centroids length mismatch: expected %d, got %d", len(idx.Centroids), len(readCentroids))
	}
}

func TestIVFIndex_WriteClusterOffsetsFile(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)

	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0, 1, 0, 0})

	idx, _ := builder.Build()

	// Write offsets file
	var buf bytes.Buffer
	err := idx.WriteClusterOffsetsFile(&buf)
	if err != nil {
		t.Fatalf("WriteClusterOffsetsFile failed: %v", err)
	}

	// Read it back
	offsets, err := ReadClusterOffsetsFile(&buf)
	if err != nil {
		t.Fatalf("ReadClusterOffsetsFile failed: %v", err)
	}

	if len(offsets) != idx.NClusters {
		t.Errorf("offsets length mismatch: expected %d, got %d", idx.NClusters, len(offsets))
	}

	// Total doc count should match
	var totalDocs uint32
	for _, o := range offsets {
		totalDocs += o.DocCount
	}
	if totalDocs != 2 {
		t.Errorf("expected 2 total docs, got %d", totalDocs)
	}
}

func TestIVFIndex_ClusterDataRoundtrip(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)

	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0.9, 0.1, 0, 0})
	builder.AddVector(3, []float32{0, 1, 0, 0})

	idx, _ := builder.Build()

	// Read vectors from each cluster
	totalDocsFound := 0
	for c := 0; c < idx.NClusters; c++ {
		docIDs, vectors, err := idx.GetClusterVectors(c)
		if err != nil {
			t.Fatalf("GetClusterVectors failed for cluster %d: %v", c, err)
		}

		totalDocsFound += len(docIDs)

		// Verify vector dimensions
		for i, vec := range vectors {
			if len(vec) != dims {
				t.Errorf("cluster %d, doc %d: expected %d dims, got %d", c, docIDs[i], dims, len(vec))
			}
		}
	}

	if totalDocsFound != 3 {
		t.Errorf("expected 3 total docs, found %d", totalDocsFound)
	}
}

func TestIVFIndex_LoadFromComponents(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)

	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0, 1, 0, 0})

	original, _ := builder.Build()

	// Serialize all components
	var centroidsBuf bytes.Buffer
	original.WriteCentroidsFile(&centroidsBuf)

	var offsetsBuf bytes.Buffer
	original.WriteClusterOffsetsFile(&offsetsBuf)

	clusterData := original.GetClusterDataBytes()

	// Read back and reconstruct
	readDims, readNClusters, readMetric, readDType, readCentroids, _ := ReadCentroidsFile(&centroidsBuf)
	readOffsets, _ := ReadClusterOffsetsFile(&offsetsBuf)

	loaded := LoadIVFIndex(readCentroids, readDims, readNClusters, readDType, readMetric, readOffsets, clusterData)

	// Search should work on loaded index
	results, err := loaded.Search([]float32{1, 0, 0, 0}, 1, 2)
	if err != nil {
		t.Fatalf("Search on loaded index failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestIVFIndex_CentroidsFileBinaryFormat(t *testing.T) {
	// Test the specific file format as defined in the spec:
	// vectors.centroids.bin is created (small, cacheable)
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)

	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0, 1, 0, 0})

	idx, _ := builder.Build()

	centroidsBytes := idx.GetCentroidsBytes()

	// Verify magic number at start
	if len(centroidsBytes) < 4 {
		t.Fatal("centroids file too short")
	}

	magic := uint32(centroidsBytes[0]) | uint32(centroidsBytes[1])<<8 |
		uint32(centroidsBytes[2])<<16 | uint32(centroidsBytes[3])<<24

	if magic != IVFMagic {
		t.Errorf("expected magic 0x%X, got 0x%X", IVFMagic, magic)
	}

	// File should be small (small number of centroids * dims * 4 bytes + header)
	expectedMinSize := 20 + (idx.NClusters * dims * 4) // header + centroids
	if len(centroidsBytes) < expectedMinSize {
		t.Errorf("centroids file too small: expected >= %d bytes, got %d", expectedMinSize, len(centroidsBytes))
	}
}

func TestIVFIndex_ClusterOffsetsFileBinaryFormat(t *testing.T) {
	// Test the specific file format as defined in the spec:
	// vectors.cluster_offsets.bin maps cluster to offset/length
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)

	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0, 1, 0, 0})

	idx, _ := builder.Build()

	offsetsBytes := idx.GetClusterOffsetsBytes()

	// Verify magic number at start
	if len(offsetsBytes) < 4 {
		t.Fatal("offsets file too short")
	}

	magic := uint32(offsetsBytes[0]) | uint32(offsetsBytes[1])<<8 |
		uint32(offsetsBytes[2])<<16 | uint32(offsetsBytes[3])<<24

	if magic != IVFMagic {
		t.Errorf("expected magic 0x%X, got 0x%X", IVFMagic, magic)
	}

	// Each offset entry is: offset(8) + length(8) + docCount(4) + padding(4) = 24 bytes
	expectedSize := 12 + (idx.NClusters * 24) // header + offsets
	if len(offsetsBytes) != expectedSize {
		t.Errorf("offsets file size: expected %d bytes, got %d", expectedSize, len(offsetsBytes))
	}
}

func TestIVFIndex_ClusterDataFormat(t *testing.T) {
	// Test the specific file format as defined in the spec:
	// vectors.clusters.pack contains packed cluster data
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)

	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0, 1, 0, 0})

	idx, _ := builder.Build()

	clusterData := idx.GetClusterDataBytes()

	// Each vector entry is: docID(8) + vector(dims * 4)
	entrySize := 8 + dims*4
	// Total should be 2 entries
	expectedSize := 2 * entrySize
	if len(clusterData) != expectedSize {
		t.Errorf("cluster data size: expected %d bytes, got %d", expectedSize, len(clusterData))
	}
}

func TestIVFIndex_ClusterDataFormatF16(t *testing.T) {
	dims := 4
	builder := NewIVFBuilderWithDType(dims, DTypeF16, MetricCosineDistance, 2)

	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0, 1, 0, 0})

	idx, _ := builder.Build()
	clusterData := idx.GetClusterDataBytes()

	entrySize := 8 + dims*2
	expectedSize := 2 * entrySize
	if len(clusterData) != expectedSize {
		t.Errorf("cluster data size: expected %d bytes, got %d", expectedSize, len(clusterData))
	}
	if idx.DType != DTypeF16 {
		t.Errorf("dtype mismatch: expected %s, got %s", DTypeF16, idx.DType)
	}
}

func TestIVFIndex_ColdQueryFlow(t *testing.T) {
	// Test the cold query flow as defined in the spec:
	// 1. Load centroids (small file)
	// 2. Find nearest nprobe centroids
	// 3. Fetch cluster data for selected clusters
	// 4. Compute distances and select top_k

	dims := 8
	nClusters := 4
	nVectors := 100

	// Build an index with random vectors
	builder := NewIVFBuilder(dims, MetricCosineDistance, nClusters)

	rand.Seed(42)
	for i := 0; i < nVectors; i++ {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		builder.AddVector(uint64(i+1), vec)
	}

	idx, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Serialize to simulate cold storage
	var centroidsBuf bytes.Buffer
	idx.WriteCentroidsFile(&centroidsBuf)

	var offsetsBuf bytes.Buffer
	idx.WriteClusterOffsetsFile(&offsetsBuf)

	clusterData := idx.GetClusterDataBytes()

	// === Step 1: Load centroids ===
	loadedDims, loadedNClusters, loadedMetric, loadedDType, loadedCentroids, err := ReadCentroidsFile(&centroidsBuf)
	if err != nil {
		t.Fatalf("Failed to load centroids: %v", err)
	}

	// === Step 2: Load offsets ===
	loadedOffsets, err := ReadClusterOffsetsFile(&offsetsBuf)
	if err != nil {
		t.Fatalf("Failed to load offsets: %v", err)
	}

	// === Step 3: Reconstruct index for search ===
	coldIdx := LoadIVFIndex(loadedCentroids, loadedDims, loadedNClusters, loadedDType, loadedMetric, loadedOffsets, clusterData)

	// === Step 4: Search ===
	query := make([]float32, dims)
	for i := range query {
		query[i] = rand.Float32()
	}

	topK := 10
	nProbe := 2

	results, err := coldIdx.Search(query, topK, nProbe)
	if err != nil {
		t.Fatalf("Cold query search failed: %v", err)
	}

	if len(results) > topK {
		t.Errorf("expected at most %d results, got %d", topK, len(results))
	}

	// Verify results are sorted by distance
	for i := 1; i < len(results); i++ {
		if results[i-1].Distance > results[i].Distance {
			t.Error("results not sorted by distance")
		}
	}

	// Verify all results are valid doc IDs
	for _, r := range results {
		if r.DocID == 0 || r.DocID > uint64(nVectors) {
			t.Errorf("invalid doc ID: %d", r.DocID)
		}
	}
}

func TestIVFIndex_NProbeAffectsRecall(t *testing.T) {
	// Test that increasing nprobe improves recall
	dims := 8
	nClusters := 8
	nVectors := 200

	builder := NewIVFBuilder(dims, MetricCosineDistance, nClusters)

	rand.Seed(42)
	vectors := make([][]float32, nVectors)
	for i := 0; i < nVectors; i++ {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
		builder.AddVector(uint64(i+1), vec)
	}

	idx, _ := builder.Build()

	// Pick a query vector
	query := make([]float32, dims)
	for i := range query {
		query[i] = rand.Float32()
	}

	// Find true top-K using exhaustive search
	type distDoc struct {
		docID uint64
		dist  float32
	}
	trueTopK := make([]distDoc, 0, nVectors)
	for i, vec := range vectors {
		dist := computeDistance(query, vec, MetricCosineDistance)
		trueTopK = append(trueTopK, distDoc{docID: uint64(i + 1), dist: dist})
	}
	// Sort by distance
	for i := 0; i < len(trueTopK); i++ {
		for j := i + 1; j < len(trueTopK); j++ {
			if trueTopK[i].dist > trueTopK[j].dist {
				trueTopK[i], trueTopK[j] = trueTopK[j], trueTopK[i]
			}
		}
	}

	topK := 10

	// Compare recall at different nprobe values
	recalls := make([]float64, 0)
	for nProbe := 1; nProbe <= nClusters; nProbe *= 2 {
		results, _ := idx.Search(query, topK, nProbe)

		// Calculate recall
		trueSet := make(map[uint64]bool)
		for i := 0; i < topK && i < len(trueTopK); i++ {
			trueSet[trueTopK[i].docID] = true
		}

		hits := 0
		for _, r := range results {
			if trueSet[r.DocID] {
				hits++
			}
		}

		recall := float64(hits) / float64(len(trueSet))
		recalls = append(recalls, recall)
	}

	// Recall should generally increase (or stay the same) with higher nprobe
	// We won't require strict monotonicity due to clustering randomness
	// But the last recall should be >= first recall
	if len(recalls) > 1 && recalls[len(recalls)-1] < recalls[0] {
		t.Errorf("recall decreased from %.2f to %.2f with more probes", recalls[0], recalls[len(recalls)-1])
	}
}

func TestIVFIndex_SearchEmptyIndex(t *testing.T) {
	idx := &IVFIndex{
		Dims:      4,
		NClusters: 0,
	}

	_, err := idx.Search([]float32{1, 0, 0, 0}, 1, 1)
	if err != ErrEmptyIndex {
		t.Errorf("expected ErrEmptyIndex, got %v", err)
	}
}

func TestIVFIndex_InvalidClusterID(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricCosineDistance, 2)
	builder.AddVector(1, []float32{1, 0, 0, 0})
	idx, _ := builder.Build()

	_, _, err := idx.GetClusterVectors(-1)
	if err == nil {
		t.Error("expected error for negative cluster ID")
	}

	_, _, err = idx.GetClusterVectors(100)
	if err == nil {
		t.Error("expected error for out-of-range cluster ID")
	}
}

func TestIVFIndex_InvalidCentroidsFile(t *testing.T) {
	// Test invalid magic number
	data := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, _, _, _, _, err := ReadCentroidsFile(bytes.NewReader(data))
	if err == nil {
		t.Error("expected error for invalid magic number")
	}

	// Test invalid version
	data = make([]byte, 8)
	magic := IVFMagic
	data[0] = byte(magic)
	data[1] = byte(magic >> 8)
	data[2] = byte(magic >> 16)
	data[3] = byte(magic >> 24)
	data[4] = 0xFF // invalid version
	_, _, _, _, _, err = ReadCentroidsFile(bytes.NewReader(data))
	if err == nil {
		t.Error("expected error for invalid version")
	}
}

func TestDistanceMetrics(t *testing.T) {
	a := []float32{1, 0, 0, 0}
	b := []float32{0, 1, 0, 0}
	c := []float32{1, 0, 0, 0} // same as a

	// Cosine distance
	dist := cosineDistance(a, c)
	if math.Abs(float64(dist)) > 0.001 {
		t.Errorf("cosine distance between identical vectors should be ~0, got %f", dist)
	}

	dist = cosineDistance(a, b)
	if math.Abs(float64(dist)-1.0) > 0.001 {
		t.Errorf("cosine distance between orthogonal vectors should be ~1, got %f", dist)
	}

	// Euclidean squared
	dist = euclideanSquared(a, c)
	if dist != 0 {
		t.Errorf("euclidean distance between identical vectors should be 0, got %f", dist)
	}

	dist = euclideanSquared(a, b)
	if math.Abs(float64(dist)-2.0) > 0.001 {
		t.Errorf("euclidean distance: expected 2, got %f", dist)
	}

	// Dot product
	dp := dotProduct(a, c)
	if dp != 1 {
		t.Errorf("dot product of identical unit vectors should be 1, got %f", dp)
	}

	dp = dotProduct(a, b)
	if dp != 0 {
		t.Errorf("dot product of orthogonal vectors should be 0, got %f", dp)
	}
}

func TestKMeans_Convergence(t *testing.T) {
	// Test that k-means converges on a simple dataset
	dims := 2
	vectors := []float32{
		// Cluster 1: around (1, 0)
		1, 0,
		0.9, 0.1,
		1.1, -0.1,
		// Cluster 2: around (0, 1)
		0, 1,
		0.1, 0.9,
		-0.1, 1.1,
	}

	centroids, assignments, err := kmeans(vectors, dims, 2, 50, 0.001, MetricEuclideanSquared)
	if err != nil {
		t.Fatalf("kmeans failed: %v", err)
	}

	if len(centroids) != 2*dims {
		t.Errorf("expected %d centroid values, got %d", 2*dims, len(centroids))
	}

	if len(assignments) != 6 {
		t.Errorf("expected 6 assignments, got %d", len(assignments))
	}

	// Check that vectors from the same group are assigned to the same cluster
	// First 3 should be in one cluster, last 3 in another
	cluster0 := assignments[0]
	for i := 1; i < 3; i++ {
		if assignments[i] != cluster0 {
			t.Errorf("vectors 0-2 should be in same cluster")
		}
	}

	cluster1 := assignments[3]
	for i := 4; i < 6; i++ {
		if assignments[i] != cluster1 {
			t.Errorf("vectors 3-5 should be in same cluster")
		}
	}

	// The two clusters should be different
	if cluster0 == cluster1 {
		t.Error("two distinct groups should be in different clusters")
	}
}
