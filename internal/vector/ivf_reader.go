package vector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"unsafe"
)

// IVFReader provides access to IVF index data stored in object storage.
// It implements the cold query flow:
// 1. Load centroids (small, cacheable in RAM)
// 2. Find nearest clusters
// 3. Fetch cluster data (via range reads)
// 4. Compute distances and select top_k
type IVFReader struct {
	// Dims is the number of dimensions in each vector.
	Dims int

	// NClusters is the number of clusters in the index.
	NClusters int

	// Metric is the distance metric used for similarity calculations.
	Metric DistanceMetric

	// Centroids are the cluster center vectors, loaded from centroids file.
	Centroids []float32

	// ClusterOffsets maps each cluster to its offset/length in cluster data.
	ClusterOffsets []ClusterOffset

	// clusterDataFetcher is a function to fetch cluster data by byte range.
	clusterDataFetcher ClusterDataFetcher
}

// ClusterDataFetcher is a function that fetches cluster data for a given byte range.
// This allows for efficient range reads from object storage.
type ClusterDataFetcher func(ctx context.Context, offset, length uint64) ([]byte, error)

// NewIVFReader creates a reader from pre-loaded centroids and offsets.
func NewIVFReader(
	dims, nClusters int,
	metric DistanceMetric,
	centroids []float32,
	offsets []ClusterOffset,
	fetcher ClusterDataFetcher,
) *IVFReader {
	return &IVFReader{
		Dims:               dims,
		NClusters:          nClusters,
		Metric:             metric,
		Centroids:          centroids,
		ClusterOffsets:     offsets,
		clusterDataFetcher: fetcher,
	}
}

// LoadIVFReaderFromData loads an IVF reader from raw file data.
// This is useful when files are already cached or loaded in memory.
func LoadIVFReaderFromData(
	centroidsData, offsetsData []byte,
	fetcher ClusterDataFetcher,
) (*IVFReader, error) {
	// Parse centroids file
	dims, nClusters, metric, centroids, err := ReadCentroidsFile(bytes.NewReader(centroidsData))
	if err != nil {
		return nil, fmt.Errorf("failed to read centroids: %w", err)
	}

	// Parse offsets file
	offsets, err := ReadClusterOffsetsFile(bytes.NewReader(offsetsData))
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster offsets: %w", err)
	}

	return NewIVFReader(dims, nClusters, metric, centroids, offsets, fetcher), nil
}

// Search performs approximate nearest neighbor search using the IVF index.
// This implements the cold query flow:
// 1. Find nearest nprobe centroids
// 2. Fetch cluster data for selected clusters
// 3. Compute distances and select top_k
func (r *IVFReader) Search(ctx context.Context, query []float32, topK, nProbe int) ([]IVFSearchResult, error) {
	if len(query) != r.Dims {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", r.Dims, len(query))
	}
	if topK <= 0 {
		return nil, fmt.Errorf("topK must be positive, got %d", topK)
	}
	if r.NClusters == 0 {
		return nil, ErrEmptyIndex
	}
	if nProbe <= 0 {
		nProbe = DefaultNProbe
	}
	if nProbe > r.NClusters {
		nProbe = r.NClusters
	}

	// Step 1: Find nearest centroids (using in-memory centroids)
	nearestClusters := r.findNearestCentroids(query, nProbe)

	// Step 2: Fetch cluster data for selected clusters
	results := make([]IVFSearchResult, 0)
	for _, clusterID := range nearestClusters {
		clusterResults, err := r.searchCluster(ctx, query, clusterID)
		if err != nil {
			return nil, fmt.Errorf("failed to search cluster %d: %w", clusterID, err)
		}
		results = append(results, clusterResults...)
	}

	// Step 3: Sort by distance and return top K
	sortResultsByDistance(results)
	if len(results) > topK {
		results = results[:topK]
	}

	return results, nil
}

// findNearestCentroids returns the indices of the nProbe nearest centroids.
func (r *IVFReader) findNearestCentroids(query []float32, nProbe int) []int {
	type centroidDist struct {
		idx  int
		dist float32
	}

	distances := make([]centroidDist, r.NClusters)
	for i := 0; i < r.NClusters; i++ {
		centroid := r.Centroids[i*r.Dims : (i+1)*r.Dims]
		dist := computeDistance(query, centroid, r.Metric)
		distances[i] = centroidDist{idx: i, dist: dist}
	}

	// Partial sort to find top nProbe (O(n) selection)
	for i := 0; i < nProbe; i++ {
		minIdx := i
		for j := i + 1; j < len(distances); j++ {
			if distances[j].dist < distances[minIdx].dist {
				minIdx = j
			}
		}
		distances[i], distances[minIdx] = distances[minIdx], distances[i]
	}

	result := make([]int, nProbe)
	for i := 0; i < nProbe; i++ {
		result[i] = distances[i].idx
	}
	return result
}

// searchCluster fetches and searches a single cluster.
func (r *IVFReader) searchCluster(ctx context.Context, query []float32, clusterID int) ([]IVFSearchResult, error) {
	offset := r.ClusterOffsets[clusterID]
	if offset.DocCount == 0 {
		return nil, nil
	}

	// Fetch cluster data using range read
	data, err := r.clusterDataFetcher(ctx, offset.Offset, offset.Length)
	if err != nil {
		return nil, err
	}

	// Parse and score vectors
	results := make([]IVFSearchResult, 0, offset.DocCount)
	reader := bytes.NewReader(data)

	for i := 0; i < int(offset.DocCount); i++ {
		docID, vec, err := readClusterEntry(reader, r.Dims)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		dist := computeDistance(query, vec, r.Metric)
		results = append(results, IVFSearchResult{
			DocID:     docID,
			Distance:  dist,
			ClusterID: clusterID,
		})
	}

	return results, nil
}

// GetClusterRanges returns the byte ranges needed to fetch the given clusters.
// This can be used to optimize multi-range GET operations.
func (r *IVFReader) GetClusterRanges(clusterIDs []int) []ByteRange {
	ranges := make([]ByteRange, 0, len(clusterIDs))
	for _, id := range clusterIDs {
		if id >= 0 && id < len(r.ClusterOffsets) {
			o := r.ClusterOffsets[id]
			if o.DocCount > 0 {
				ranges = append(ranges, ByteRange{
					Offset:    o.Offset,
					Length:    o.Length,
					ClusterID: id,
				})
			}
		}
	}
	return ranges
}

// ByteRange represents a byte range in the cluster data file.
type ByteRange struct {
	Offset    uint64
	Length    uint64
	ClusterID int
}

// MergeAdjacentRanges merges adjacent or overlapping byte ranges to reduce
// the number of range requests to object storage.
func MergeAdjacentRanges(ranges []ByteRange, maxGap uint64) []ByteRange {
	if len(ranges) <= 1 {
		return ranges
	}

	// Sort by offset
	for i := 0; i < len(ranges); i++ {
		for j := i + 1; j < len(ranges); j++ {
			if ranges[j].Offset < ranges[i].Offset {
				ranges[i], ranges[j] = ranges[j], ranges[i]
			}
		}
	}

	merged := make([]ByteRange, 0, len(ranges))
	current := ranges[0]

	for i := 1; i < len(ranges); i++ {
		next := ranges[i]
		currentEnd := current.Offset + current.Length
		nextStart := next.Offset

		// Merge if adjacent or overlapping (with small gap tolerance)
		if nextStart <= currentEnd+maxGap {
			newEnd := next.Offset + next.Length
			if newEnd > currentEnd {
				current.Length = newEnd - current.Offset
			}
			current.ClusterID = -1 // Merged range covers multiple clusters
		} else {
			merged = append(merged, current)
			current = next
		}
	}
	merged = append(merged, current)

	return merged
}

// sortResultsByDistance sorts results by distance (ascending).
func sortResultsByDistance(results []IVFSearchResult) {
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Distance < results[i].Distance {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}

// readClusterEntry reads a single (docID, vector) entry from cluster data.
func readClusterEntry(r io.Reader, dims int) (uint64, []float32, error) {
	var docID uint64
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	docID = uint64(buf[0]) | uint64(buf[1])<<8 | uint64(buf[2])<<16 | uint64(buf[3])<<24 |
		uint64(buf[4])<<32 | uint64(buf[5])<<40 | uint64(buf[6])<<48 | uint64(buf[7])<<56

	vec := make([]float32, dims)
	vecBuf := make([]byte, dims*4)
	if _, err := io.ReadFull(r, vecBuf); err != nil {
		return 0, nil, err
	}
	for i := 0; i < dims; i++ {
		bits := uint32(vecBuf[i*4]) | uint32(vecBuf[i*4+1])<<8 |
			uint32(vecBuf[i*4+2])<<16 | uint32(vecBuf[i*4+3])<<24
		vec[i] = float32frombits(bits)
	}

	return docID, vec, nil
}

// float32frombits converts bits to float32.
func float32frombits(b uint32) float32 {
	return *(*float32)(unsafe.Pointer(&b))
}
