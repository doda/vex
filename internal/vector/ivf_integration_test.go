package vector

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"testing"
)

// TestIVFIntegration_TaskVerification verifies all steps from the task spec:
// 1. Test vectors.centroids.bin is created (small, cacheable)
// 2. Verify vectors.clusters.pack contains packed cluster data
// 3. Test vectors.cluster_offsets.bin maps cluster to offset/length
// 4. Verify cold query flow: load centroids, find nearest clusters, fetch cluster data
func TestIVFIntegration_TaskVerification(t *testing.T) {
	// Setup: Create a moderately sized index
	dims := 16
	nClusters := 8
	nVectors := 500

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

	// === STEP 1: Test vectors.centroids.bin is created (small, cacheable) ===
	t.Run("CentroidsFileCreated", func(t *testing.T) {
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

		// Verify it's small (header + centroids data)
		// Header: magic(4) + version(4) + dims(4) + nclusters(4) + metric(4) = 20 bytes
		// Centroids: nClusters * dims * 4 bytes
		expectedSize := 20 + (idx.NClusters * dims * 4)
		if len(centroidsBytes) != expectedSize {
			t.Errorf("centroids file size mismatch: expected %d, got %d", expectedSize, len(centroidsBytes))
		}

		// Verify it's cacheable (small enough for RAM)
		// For a typical deployment with 256 clusters and 1536 dims:
		// 20 + 256 * 1536 * 4 = ~1.5MB (easily cacheable)
		if len(centroidsBytes) > 10*1024*1024 { // 10MB limit for "small"
			t.Error("centroids file too large to be cacheable")
		}

		t.Logf("Centroids file: %d bytes (%.2f KB)", len(centroidsBytes), float64(len(centroidsBytes))/1024)
	})

	// === STEP 2: Verify vectors.clusters.pack contains packed cluster data ===
	t.Run("ClusterDataPackFormat", func(t *testing.T) {
		clusterData := idx.GetClusterDataBytes()

		// Each vector entry is: docID (8 bytes) + vector (dims * 4 bytes)
		entrySize := 8 + dims*4
		expectedSize := nVectors * entrySize

		if len(clusterData) != expectedSize {
			t.Errorf("cluster data size mismatch: expected %d, got %d", expectedSize, len(clusterData))
		}

		// Verify data can be parsed as packed clusters
		totalDocsFound := 0
		for clusterID := 0; clusterID < idx.NClusters; clusterID++ {
			docIDs, vectors, err := idx.GetClusterVectors(clusterID)
			if err != nil {
				t.Errorf("failed to get cluster %d vectors: %v", clusterID, err)
				continue
			}

			totalDocsFound += len(docIDs)

			// Verify vector dimensions
			for i, vec := range vectors {
				if len(vec) != dims {
					t.Errorf("cluster %d, doc %d: expected %d dims, got %d", clusterID, docIDs[i], dims, len(vec))
				}
			}
		}

		if totalDocsFound != nVectors {
			t.Errorf("expected %d total docs, found %d", nVectors, totalDocsFound)
		}

		t.Logf("Cluster data: %d bytes (%.2f KB)", len(clusterData), float64(len(clusterData))/1024)
	})

	// === STEP 3: Test vectors.cluster_offsets.bin maps cluster to offset/length ===
	t.Run("ClusterOffsetsMapping", func(t *testing.T) {
		offsetsBytes := idx.GetClusterOffsetsBytes()

		// Verify magic number
		magic := uint32(offsetsBytes[0]) | uint32(offsetsBytes[1])<<8 |
			uint32(offsetsBytes[2])<<16 | uint32(offsetsBytes[3])<<24
		if magic != IVFMagic {
			t.Errorf("expected magic 0x%X, got 0x%X", IVFMagic, magic)
		}

		// Header: magic(4) + version(4) + nclusters(4) = 12 bytes
		// Each offset entry: offset(8) + length(8) + docCount(4) + padding(4) = 24 bytes
		expectedSize := 12 + (idx.NClusters * 24)
		if len(offsetsBytes) != expectedSize {
			t.Errorf("offsets file size mismatch: expected %d, got %d", expectedSize, len(offsetsBytes))
		}

		// Verify offsets are valid
		clusterData := idx.GetClusterDataBytes()
		for i, offset := range idx.ClusterOffsets {
			// Verify offset doesn't exceed cluster data size
			if offset.Offset > uint64(len(clusterData)) {
				t.Errorf("cluster %d: offset %d exceeds cluster data size %d", i, offset.Offset, len(clusterData))
			}

			// Verify offset + length doesn't exceed cluster data size
			if offset.Offset+offset.Length > uint64(len(clusterData)) {
				t.Errorf("cluster %d: offset+length %d exceeds cluster data size %d",
					i, offset.Offset+offset.Length, len(clusterData))
			}

			// Verify length matches expected entry count
			entrySize := 8 + dims*4
			expectedLength := uint64(offset.DocCount) * uint64(entrySize)
			if offset.Length != expectedLength {
				t.Errorf("cluster %d: length mismatch: expected %d, got %d", i, expectedLength, offset.Length)
			}
		}

		t.Logf("Cluster offsets file: %d bytes for %d clusters", len(offsetsBytes), idx.NClusters)
	})

	// === STEP 4: Verify cold query flow ===
	t.Run("ColdQueryFlow", func(t *testing.T) {
		// Serialize all components (simulating object storage)
		var centroidsBuf bytes.Buffer
		idx.WriteCentroidsFile(&centroidsBuf)
		centroidsData := centroidsBuf.Bytes()

		var offsetsBuf bytes.Buffer
		idx.WriteClusterOffsetsFile(&offsetsBuf)
		offsetsData := offsetsBuf.Bytes()

		clusterData := idx.GetClusterDataBytes()

		// === STEP 4a: Load centroids (small, from RAM cache) ===
		loadedDims, loadedNClusters, loadedMetric, loadedDType, loadedCentroids, err := ReadCentroidsFile(bytes.NewReader(centroidsData))
		if err != nil {
			t.Fatalf("Failed to load centroids: %v", err)
		}

		if loadedDims != dims {
			t.Errorf("dims mismatch: expected %d, got %d", dims, loadedDims)
		}
		if loadedNClusters != idx.NClusters {
			t.Errorf("nclusters mismatch: expected %d, got %d", idx.NClusters, loadedNClusters)
		}
		if loadedMetric != MetricCosineDistance {
			t.Errorf("metric mismatch: expected %s, got %s", MetricCosineDistance, loadedMetric)
		}

		// === STEP 4b: Load offsets (small, from RAM cache) ===
		loadedOffsets, err := ReadClusterOffsetsFile(bytes.NewReader(offsetsData))
		if err != nil {
			t.Fatalf("Failed to load offsets: %v", err)
		}

		if len(loadedOffsets) != idx.NClusters {
			t.Errorf("offsets count mismatch: expected %d, got %d", idx.NClusters, len(loadedOffsets))
		}

		// Track which clusters were fetched
		fetchedClusters := make(map[int]bool)
		fetchCallCount := 0

		// === STEP 4c: Create reader with fetcher (simulates object storage range reads) ===
		fetcher := func(ctx context.Context, offset, length uint64) ([]byte, error) {
			fetchCallCount++
			// Identify which cluster this fetch is for
			for i, o := range loadedOffsets {
				if o.Offset == offset && o.Length == length {
					fetchedClusters[i] = true
					break
				}
			}
			return clusterData[offset : offset+length], nil
		}

		reader := NewIVFReaderWithDType(loadedDims, loadedNClusters, loadedDType, loadedMetric, loadedCentroids, loadedOffsets, fetcher)

		// === STEP 4d: Search (find nearest clusters, fetch cluster data) ===
		query := make([]float32, dims)
		for i := range query {
			query[i] = rand.Float32()
		}

		topK := 10
		nProbe := 4

		results, err := reader.Search(context.Background(), query, topK, nProbe)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		// Verify results
		if len(results) > topK {
			t.Errorf("expected at most %d results, got %d", topK, len(results))
		}

		// Verify only nProbe clusters were fetched
		if len(fetchedClusters) != nProbe {
			t.Errorf("expected %d clusters fetched, got %d", nProbe, len(fetchedClusters))
		}

		// Verify results are sorted by distance
		for i := 1; i < len(results); i++ {
			if results[i-1].Distance > results[i].Distance {
				t.Error("results not sorted by distance")
			}
		}

		// Verify results come from fetched clusters
		for _, r := range results {
			if !fetchedClusters[r.ClusterID] {
				t.Errorf("result doc %d came from unfetched cluster %d", r.DocID, r.ClusterID)
			}
		}

		t.Logf("Cold query completed: %d fetch calls, %d clusters probed, %d results",
			fetchCallCount, len(fetchedClusters), len(results))
	})
}

// TestIVFIntegration_FileFormats verifies the binary file formats
func TestIVFIntegration_FileFormats(t *testing.T) {
	dims := 4
	builder := NewIVFBuilder(dims, MetricEuclideanSquared, 2)

	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0, 1, 0, 0})

	idx, _ := builder.Build()

	t.Run("CentroidsFormat", func(t *testing.T) {
		data := idx.GetCentroidsBytes()
		reader := bytes.NewReader(data)

		// Read and verify header
		var magic, version, dimsU32, nClusters uint32
		var metricBytes [4]byte

		binary.Read(reader, binary.LittleEndian, &magic)
		binary.Read(reader, binary.LittleEndian, &version)
		binary.Read(reader, binary.LittleEndian, &dimsU32)
		binary.Read(reader, binary.LittleEndian, &nClusters)
		binary.Read(reader, binary.LittleEndian, &metricBytes)

		if magic != IVFMagic {
			t.Errorf("magic: expected 0x%X, got 0x%X", IVFMagic, magic)
		}
		if version != IVFVersion {
			t.Errorf("version: expected %d, got %d", IVFVersion, version)
		}
		if int(dimsU32) != dims {
			t.Errorf("dims: expected %d, got %d", dims, dimsU32)
		}
		if int(nClusters) != idx.NClusters {
			t.Errorf("nClusters: expected %d, got %d", idx.NClusters, nClusters)
		}
		if metricBytes[0] != 1 { // EuclideanSquared = 1
			t.Errorf("metric: expected 1, got %d", metricBytes[0])
		}
	})

	t.Run("OffsetsFormat", func(t *testing.T) {
		data := idx.GetClusterOffsetsBytes()
		reader := bytes.NewReader(data)

		// Read and verify header
		var magic, version, nClusters uint32

		binary.Read(reader, binary.LittleEndian, &magic)
		binary.Read(reader, binary.LittleEndian, &version)
		binary.Read(reader, binary.LittleEndian, &nClusters)

		if magic != IVFMagic {
			t.Errorf("magic: expected 0x%X, got 0x%X", IVFMagic, magic)
		}
		if version != IVFVersion {
			t.Errorf("version: expected %d, got %d", IVFVersion, version)
		}
		if int(nClusters) != idx.NClusters {
			t.Errorf("nClusters: expected %d, got %d", idx.NClusters, nClusters)
		}

		// Read and verify offset entries
		for i := 0; i < int(nClusters); i++ {
			var offset, length uint64
			var docCount, padding uint32

			binary.Read(reader, binary.LittleEndian, &offset)
			binary.Read(reader, binary.LittleEndian, &length)
			binary.Read(reader, binary.LittleEndian, &docCount)
			binary.Read(reader, binary.LittleEndian, &padding)

			if uint64(offset) != idx.ClusterOffsets[i].Offset {
				t.Errorf("cluster %d: offset mismatch", i)
			}
			if uint64(length) != idx.ClusterOffsets[i].Length {
				t.Errorf("cluster %d: length mismatch", i)
			}
			if docCount != idx.ClusterOffsets[i].DocCount {
				t.Errorf("cluster %d: docCount mismatch", i)
			}
		}
	})

	t.Run("ClusterDataFormat", func(t *testing.T) {
		data := idx.GetClusterDataBytes()

		// Each entry is: docID(8) + vector(dims*4)
		entrySize := 8 + dims*4
		if len(data)%entrySize != 0 {
			t.Errorf("cluster data size %d not divisible by entry size %d", len(data), entrySize)
		}

		// Parse and verify entries
		reader := bytes.NewReader(data)
		entriesRead := 0

		for reader.Len() > 0 {
			var docID uint64
			binary.Read(reader, binary.LittleEndian, &docID)

			vec := make([]float32, dims)
			for i := 0; i < dims; i++ {
				binary.Read(reader, binary.LittleEndian, &vec[i])
			}

			if docID != 1 && docID != 2 {
				t.Errorf("unexpected docID: %d", docID)
			}

			entriesRead++
		}

		if entriesRead != 2 {
			t.Errorf("expected 2 entries, read %d", entriesRead)
		}
	})
}

// TestIVFIntegration_RecallMeasurement verifies search quality
func TestIVFIntegration_RecallMeasurement(t *testing.T) {
	dims := 8
	nClusters := 16
	nVectors := 1000

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

	// Generate query and compute true nearest neighbors (exhaustive)
	query := make([]float32, dims)
	for i := range query {
		query[i] = rand.Float32()
	}

	type distDoc struct {
		docID uint64
		dist  float32
	}
	trueNN := make([]distDoc, nVectors)
	for i, vec := range vectors {
		dist := computeDistance(query, vec, MetricCosineDistance)
		trueNN[i] = distDoc{docID: uint64(i + 1), dist: dist}
	}
	// Sort by distance
	for i := 0; i < len(trueNN); i++ {
		for j := i + 1; j < len(trueNN); j++ {
			if trueNN[j].dist < trueNN[i].dist {
				trueNN[i], trueNN[j] = trueNN[j], trueNN[i]
			}
		}
	}

	topK := 10

	// Test recall at different nprobe values
	for _, nProbe := range []int{1, 2, 4, 8, 16} {
		results, _ := idx.Search(query, topK, nProbe)

		trueSet := make(map[uint64]bool)
		for i := 0; i < topK; i++ {
			trueSet[trueNN[i].docID] = true
		}

		hits := 0
		for _, r := range results {
			if trueSet[r.DocID] {
				hits++
			}
		}

		recall := float64(hits) / float64(topK)
		t.Logf("nprobe=%d: recall@%d = %.2f%%", nProbe, topK, recall*100)

		// At nprobe=nClusters, recall should be 100% (full scan)
		if nProbe == nClusters && recall < 1.0 {
			t.Errorf("expected 100%% recall at full scan, got %.2f%%", recall*100)
		}
	}
}
