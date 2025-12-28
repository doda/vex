package vector

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
)

func TestIVFReader_ColdQueryFlow(t *testing.T) {
	// This test simulates the complete cold query flow:
	// 1. Build an index
	// 2. Serialize to "object storage" (bytes)
	// 3. Load centroids and offsets (small, cacheable)
	// 4. Create a reader that fetches cluster data on demand
	// 5. Search using the reader

	dims := 8
	nClusters := 4
	nVectors := 100

	// Build an index
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

	// Serialize to "object storage"
	var centroidsBuf bytes.Buffer
	idx.WriteCentroidsFile(&centroidsBuf)
	centroidsData := centroidsBuf.Bytes()

	var offsetsBuf bytes.Buffer
	idx.WriteClusterOffsetsFile(&offsetsBuf)
	offsetsData := offsetsBuf.Bytes()

	clusterData := idx.GetClusterDataBytes()

	// Create a reader that fetches cluster data on demand
	fetcher := func(ctx context.Context, offset, length uint64) ([]byte, error) {
		if offset+length > uint64(len(clusterData)) {
			return nil, fmt.Errorf("range out of bounds")
		}
		return clusterData[offset : offset+length], nil
	}

	reader, err := LoadIVFReaderFromData(centroidsData, offsetsData, fetcher)
	if err != nil {
		t.Fatalf("LoadIVFReaderFromData failed: %v", err)
	}

	// Search using the reader
	query := make([]float32, dims)
	for i := range query {
		query[i] = rand.Float32()
	}

	topK := 10
	nProbe := 2

	results, err := reader.Search(context.Background(), query, topK, nProbe)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
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

func TestIVFReader_SearchMatchesIndex(t *testing.T) {
	// Verify that IVFReader produces same results as IVFIndex.Search
	dims := 4
	nClusters := 2

	builder := NewIVFBuilder(dims, MetricCosineDistance, nClusters)
	builder.AddVector(1, []float32{1, 0, 0, 0})
	builder.AddVector(2, []float32{0.9, 0.1, 0, 0})
	builder.AddVector(3, []float32{0, 1, 0, 0})
	builder.AddVector(4, []float32{0, 0.9, 0.1, 0})

	idx, _ := builder.Build()

	// Serialize and create reader
	var centroidsBuf bytes.Buffer
	idx.WriteCentroidsFile(&centroidsBuf)

	var offsetsBuf bytes.Buffer
	idx.WriteClusterOffsetsFile(&offsetsBuf)

	clusterData := idx.GetClusterDataBytes()

	fetcher := func(ctx context.Context, offset, length uint64) ([]byte, error) {
		return clusterData[offset : offset+length], nil
	}

	reader, _ := LoadIVFReaderFromData(centroidsBuf.Bytes(), offsetsBuf.Bytes(), fetcher)

	// Search both and compare
	query := []float32{0.95, 0.05, 0, 0}
	topK := 4
	nProbe := 2

	indexResults, _ := idx.Search(query, topK, nProbe)
	readerResults, err := reader.Search(context.Background(), query, topK, nProbe)
	if err != nil {
		t.Fatalf("Reader search failed: %v", err)
	}

	if len(indexResults) != len(readerResults) {
		t.Errorf("result count mismatch: index=%d, reader=%d", len(indexResults), len(readerResults))
	}

	// Results should be in same order with same doc IDs
	for i := range indexResults {
		if indexResults[i].DocID != readerResults[i].DocID {
			t.Errorf("result %d: index docID=%d, reader docID=%d",
				i, indexResults[i].DocID, readerResults[i].DocID)
		}
	}
}

func TestIVFReader_EmptyCluster(t *testing.T) {
	// Test handling of empty clusters
	dims := 4

	reader := NewIVFReader(
		dims, 2, MetricCosineDistance,
		[]float32{1, 0, 0, 0, 0, 1, 0, 0}, // 2 centroids
		[]ClusterOffset{
			{Offset: 0, Length: 0, DocCount: 0}, // Empty cluster
			{Offset: 0, Length: 0, DocCount: 0}, // Empty cluster
		},
		func(ctx context.Context, offset, length uint64) ([]byte, error) {
			return nil, nil
		},
	)

	results, err := reader.Search(context.Background(), []float32{1, 0, 0, 0}, 10, 2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestIVFReader_DimensionMismatch(t *testing.T) {
	reader := NewIVFReader(
		4, 1, MetricCosineDistance,
		[]float32{1, 0, 0, 0},
		[]ClusterOffset{{Offset: 0, Length: 0, DocCount: 0}},
		nil,
	)

	_, err := reader.Search(context.Background(), []float32{1, 0, 0}, 10, 1)
	if err == nil {
		t.Error("expected error for dimension mismatch")
	}
}

func TestIVFReader_EmptyIndex(t *testing.T) {
	reader := NewIVFReader(4, 0, MetricCosineDistance, nil, nil, nil)

	_, err := reader.Search(context.Background(), []float32{1, 0, 0, 0}, 10, 1)
	if err != ErrEmptyIndex {
		t.Errorf("expected ErrEmptyIndex, got %v", err)
	}
}

func TestGetClusterRanges(t *testing.T) {
	reader := NewIVFReader(
		4, 3, MetricCosineDistance,
		make([]float32, 12),
		[]ClusterOffset{
			{Offset: 0, Length: 100, DocCount: 5},
			{Offset: 100, Length: 200, DocCount: 10},
			{Offset: 300, Length: 50, DocCount: 2},
		},
		nil,
	)

	ranges := reader.GetClusterRanges([]int{0, 2})
	if len(ranges) != 2 {
		t.Fatalf("expected 2 ranges, got %d", len(ranges))
	}

	if ranges[0].Offset != 0 || ranges[0].Length != 100 {
		t.Errorf("range 0: expected offset=0 length=100, got offset=%d length=%d",
			ranges[0].Offset, ranges[0].Length)
	}

	if ranges[1].Offset != 300 || ranges[1].Length != 50 {
		t.Errorf("range 1: expected offset=300 length=50, got offset=%d length=%d",
			ranges[1].Offset, ranges[1].Length)
	}
}

func TestMergeAdjacentRanges(t *testing.T) {
	tests := []struct {
		name     string
		ranges   []ByteRange
		maxGap   uint64
		expected []ByteRange
	}{
		{
			name:     "no ranges",
			ranges:   nil,
			maxGap:   100,
			expected: nil,
		},
		{
			name: "single range",
			ranges: []ByteRange{
				{Offset: 0, Length: 100},
			},
			maxGap: 100,
			expected: []ByteRange{
				{Offset: 0, Length: 100},
			},
		},
		{
			name: "adjacent ranges merged",
			ranges: []ByteRange{
				{Offset: 0, Length: 100},
				{Offset: 100, Length: 100},
			},
			maxGap: 0,
			expected: []ByteRange{
				{Offset: 0, Length: 200},
			},
		},
		{
			name: "ranges with small gap merged",
			ranges: []ByteRange{
				{Offset: 0, Length: 100},
				{Offset: 110, Length: 100},
			},
			maxGap: 20,
			expected: []ByteRange{
				{Offset: 0, Length: 210},
			},
		},
		{
			name: "ranges with large gap not merged",
			ranges: []ByteRange{
				{Offset: 0, Length: 100},
				{Offset: 200, Length: 100},
			},
			maxGap: 50,
			expected: []ByteRange{
				{Offset: 0, Length: 100},
				{Offset: 200, Length: 100},
			},
		},
		{
			name: "unsorted ranges",
			ranges: []ByteRange{
				{Offset: 200, Length: 100},
				{Offset: 0, Length: 100},
				{Offset: 100, Length: 100},
			},
			maxGap: 0,
			expected: []ByteRange{
				{Offset: 0, Length: 300},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := MergeAdjacentRanges(tt.ranges, tt.maxGap)
			if len(merged) != len(tt.expected) {
				t.Errorf("expected %d ranges, got %d", len(tt.expected), len(merged))
				return
			}
			for i := range merged {
				if merged[i].Offset != tt.expected[i].Offset ||
					merged[i].Length != tt.expected[i].Length {
					t.Errorf("range %d: expected offset=%d length=%d, got offset=%d length=%d",
						i, tt.expected[i].Offset, tt.expected[i].Length,
						merged[i].Offset, merged[i].Length)
				}
			}
		})
	}
}

func TestIVFReader_FetcherError(t *testing.T) {
	dims := 4

	reader := NewIVFReader(
		dims, 1, MetricCosineDistance,
		[]float32{1, 0, 0, 0},
		[]ClusterOffset{{Offset: 0, Length: 100, DocCount: 5}},
		func(ctx context.Context, offset, length uint64) ([]byte, error) {
			return nil, fmt.Errorf("storage unavailable")
		},
	)

	_, err := reader.Search(context.Background(), []float32{1, 0, 0, 0}, 10, 1)
	if err == nil {
		t.Error("expected error from fetcher")
	}
}
