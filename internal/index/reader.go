// Package index provides index reading functionality for query nodes.
package index

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// Reader provides access to index data from object storage.
// It loads manifests and IVF indexes on demand, with caching.
type Reader struct {
	store     objectstore.Store
	diskCache *cache.DiskCache
	ramCache  *cache.MemoryCache

	// Per-namespace IVF reader cache
	mu      sync.RWMutex
	readers map[string]*cachedIVFReader
}

// cachedIVFReader holds a cached IVF reader for a segment.
type cachedIVFReader struct {
	reader        *vector.IVFReader
	manifestSeq   uint64
	segmentID     string
	clusterDataKey string
}

// NewReader creates a new index reader.
func NewReader(store objectstore.Store, diskCache *cache.DiskCache, ramCache *cache.MemoryCache) *Reader {
	return &Reader{
		store:     store,
		diskCache: diskCache,
		ramCache:  ramCache,
		readers:   make(map[string]*cachedIVFReader),
	}
}

// LoadManifest loads the manifest for a namespace from the given key.
func (r *Reader) LoadManifest(ctx context.Context, manifestKey string) (*Manifest, error) {
	if manifestKey == "" {
		return nil, nil
	}

	// Try to load from cache first
	cacheKey := cache.CacheKey{ObjectKey: manifestKey}
	if r.diskCache != nil {
		if reader, err := r.diskCache.GetReader(cacheKey); err == nil {
			data, err := io.ReadAll(reader)
			reader.Close()
			if err == nil {
				var manifest Manifest
				if err := json.Unmarshal(data, &manifest); err == nil {
					return &manifest, nil
				}
			}
		}
	}

	// Load from object storage
	reader, _, err := r.store.Get(ctx, manifestKey, nil)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	// Cache the manifest
	if r.diskCache != nil {
		r.diskCache.PutBytes(cacheKey, data)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	return &manifest, nil
}

// GetIVFReader returns an IVF reader for the given namespace.
// It loads the manifest and creates a reader for the first segment with IVF data.
// The reader is cached for subsequent queries.
func (r *Reader) GetIVFReader(ctx context.Context, namespace string, manifestKey string, manifestSeq uint64) (*vector.IVFReader, string, error) {
	if manifestKey == "" {
		return nil, "", nil
	}

	// Check cache first
	readerKey := fmt.Sprintf("%s/%d", namespace, manifestSeq)
	r.mu.RLock()
	cached, ok := r.readers[readerKey]
	r.mu.RUnlock()
	if ok {
		return cached.reader, cached.clusterDataKey, nil
	}

	// Load manifest
	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		return nil, "", err
	}
	if manifest == nil {
		return nil, "", nil
	}

	// Find the first segment with IVF data
	// In production, we'd want to search all segments; for now pick the first one with IVF
	var ivfSegment *Segment
	for i := range manifest.Segments {
		if manifest.Segments[i].IVFKeys.HasIVF() {
			ivfSegment = &manifest.Segments[i]
			break
		}
	}
	if ivfSegment == nil {
		return nil, "", nil
	}

	// Load centroids
	centroidsData, err := r.loadObject(ctx, ivfSegment.IVFKeys.CentroidsKey)
	if err != nil {
		return nil, "", fmt.Errorf("failed to load centroids: %w", err)
	}

	// Load cluster offsets
	offsetsData, err := r.loadObject(ctx, ivfSegment.IVFKeys.ClusterOffsetsKey)
	if err != nil {
		return nil, "", fmt.Errorf("failed to load cluster offsets: %w", err)
	}

	// Create a fetcher for cluster data using range reads
	clusterDataKey := ivfSegment.IVFKeys.ClusterDataKey
	fetcher := r.createClusterDataFetcher(ctx, clusterDataKey)

	// Create IVF reader
	ivfReader, err := vector.LoadIVFReaderFromData(centroidsData, offsetsData, fetcher)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create IVF reader: %w", err)
	}

	// Cache the reader
	r.mu.Lock()
	r.readers[readerKey] = &cachedIVFReader{
		reader:         ivfReader,
		manifestSeq:    manifestSeq,
		segmentID:      ivfSegment.ID,
		clusterDataKey: clusterDataKey,
	}
	r.mu.Unlock()

	return ivfReader, clusterDataKey, nil
}

// loadObject loads an object from storage, using cache if available.
func (r *Reader) loadObject(ctx context.Context, key string) ([]byte, error) {
	// Try cache first
	cacheKey := cache.CacheKey{ObjectKey: key}
	if r.diskCache != nil {
		if reader, err := r.diskCache.GetReader(cacheKey); err == nil {
			data, err := io.ReadAll(reader)
			reader.Close()
			if err == nil {
				return data, nil
			}
		}
	}

	// Load from object storage
	reader, _, err := r.store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// Cache the data
	if r.diskCache != nil {
		r.diskCache.PutBytes(cacheKey, data)
	}

	return data, nil
}

// createClusterDataFetcher creates a ClusterDataFetcher that uses range reads.
func (r *Reader) createClusterDataFetcher(ctx context.Context, clusterDataKey string) vector.ClusterDataFetcher {
	return func(fetchCtx context.Context, offset, length uint64) ([]byte, error) {
		// Create a cache key specific to this range
		cacheKey := cache.CacheKey{
			ObjectKey: fmt.Sprintf("%s#%d-%d", clusterDataKey, offset, length),
		}

		// Try cache first
		if r.diskCache != nil {
			if reader, err := r.diskCache.GetReader(cacheKey); err == nil {
				data, err := io.ReadAll(reader)
				reader.Close()
				if err == nil {
					return data, nil
				}
			}
		}

		// Use range GET from object storage
		opts := &objectstore.GetOptions{
			Range: &objectstore.ByteRange{
				Start: int64(offset),
				End:   int64(offset + length - 1), // S3 ranges are inclusive
			},
		}

		reader, _, err := r.store.Get(fetchCtx, clusterDataKey, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch cluster data range [%d, %d): %w", offset, offset+length, err)
		}
		defer reader.Close()

		data, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read cluster data: %w", err)
		}

		// Cache the range data
		if r.diskCache != nil {
			r.diskCache.PutBytes(cacheKey, data)
		}

		return data, nil
	}
}

// MultiRangeClusterDataFetcher creates a fetcher that can batch multiple range requests.
// This is an optimization for fetching multiple clusters in a single round-trip.
type MultiRangeClusterDataFetcher struct {
	store         objectstore.Store
	diskCache     *cache.DiskCache
	clusterDataKey string
}

// NewMultiRangeClusterDataFetcher creates a new multi-range fetcher.
func NewMultiRangeClusterDataFetcher(store objectstore.Store, diskCache *cache.DiskCache, clusterDataKey string) *MultiRangeClusterDataFetcher {
	return &MultiRangeClusterDataFetcher{
		store:         store,
		diskCache:     diskCache,
		clusterDataKey: clusterDataKey,
	}
}

// FetchRanges fetches multiple byte ranges and returns the data keyed by cluster ID.
// It uses MergeAdjacentRanges to combine nearby ranges and reduce request count.
func (f *MultiRangeClusterDataFetcher) FetchRanges(ctx context.Context, ranges []vector.ByteRange) (map[int][]byte, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	result := make(map[int][]byte)

	// First, check cache for each range
	uncached := make([]vector.ByteRange, 0, len(ranges))
	for _, r := range ranges {
		cacheKey := cache.CacheKey{
			ObjectKey: fmt.Sprintf("%s#%d-%d", f.clusterDataKey, r.Offset, r.Length),
		}
		if f.diskCache != nil {
			if reader, err := f.diskCache.GetReader(cacheKey); err == nil {
				data, err := io.ReadAll(reader)
				reader.Close()
				if err == nil {
					result[r.ClusterID] = data
					continue
				}
			}
		}
		uncached = append(uncached, r)
	}

	if len(uncached) == 0 {
		return result, nil
	}

	// Merge adjacent ranges to reduce number of requests
	// Use a 4KB gap tolerance for merging
	merged := vector.MergeAdjacentRanges(uncached, 4096)

	// Fetch each merged range
	for _, mr := range merged {
		opts := &objectstore.GetOptions{
			Range: &objectstore.ByteRange{
				Start: int64(mr.Offset),
				End:   int64(mr.Offset + mr.Length - 1),
			},
		}

		reader, _, err := f.store.Get(ctx, f.clusterDataKey, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch range [%d, %d): %w", mr.Offset, mr.Offset+mr.Length, err)
		}

		data, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read range data: %w", err)
		}

		// If this is a merged range covering multiple clusters, we need to split it
		if mr.ClusterID == -1 {
			// Find which original ranges are covered by this merged range
			for _, orig := range uncached {
				if orig.Offset >= mr.Offset && orig.Offset+orig.Length <= mr.Offset+mr.Length {
					// Extract this cluster's data from the merged result
					start := orig.Offset - mr.Offset
					end := start + orig.Length
					if end <= uint64(len(data)) {
						clusterData := make([]byte, orig.Length)
						copy(clusterData, data[start:end])
						result[orig.ClusterID] = clusterData

						// Cache the individual cluster data
						if f.diskCache != nil {
							cacheKey := cache.CacheKey{
								ObjectKey: fmt.Sprintf("%s#%d-%d", f.clusterDataKey, orig.Offset, orig.Length),
							}
							f.diskCache.PutBytes(cacheKey, clusterData)
						}
					}
				}
			}
		} else {
			// Single cluster range
			result[mr.ClusterID] = data

			// Cache it
			if f.diskCache != nil {
				cacheKey := cache.CacheKey{
					ObjectKey: fmt.Sprintf("%s#%d-%d", f.clusterDataKey, mr.Offset, mr.Length),
				}
				f.diskCache.PutBytes(cacheKey, data)
			}
		}
	}

	return result, nil
}

// SearchWithMultiRange performs ANN search using multi-range fetching for efficiency.
func (r *Reader) SearchWithMultiRange(ctx context.Context, ivfReader *vector.IVFReader, clusterDataKey string, query []float32, topK, nProbe int) ([]vector.IVFSearchResult, error) {
	if ivfReader == nil {
		return nil, nil
	}

	// Find nearest centroids
	nearestClusters := findNearestCentroids(ivfReader, query, nProbe)

	// Get byte ranges for all selected clusters
	ranges := ivfReader.GetClusterRanges(nearestClusters)

	// Fetch all cluster data using multi-range optimization
	fetcher := NewMultiRangeClusterDataFetcher(r.store, r.diskCache, clusterDataKey)
	clusterData, err := fetcher.FetchRanges(ctx, ranges)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch cluster data: %w", err)
	}

	// Search each cluster and compute exact distances
	var results []vector.IVFSearchResult
	for clusterID, data := range clusterData {
		clusterResults, err := searchClusterData(data, query, clusterID, ivfReader.Dims, ivfReader.Metric)
		if err != nil {
			return nil, fmt.Errorf("failed to search cluster %d: %w", clusterID, err)
		}
		results = append(results, clusterResults...)
	}

	// Sort by distance and return top K
	sortResultsByDistance(results)
	if len(results) > topK {
		results = results[:topK]
	}

	return results, nil
}

// findNearestCentroids finds the nProbe nearest centroids to the query vector.
func findNearestCentroids(ivfReader *vector.IVFReader, query []float32, nProbe int) []int {
	if nProbe <= 0 {
		nProbe = vector.DefaultNProbe
	}
	if nProbe > ivfReader.NClusters {
		nProbe = ivfReader.NClusters
	}

	type centroidDist struct {
		idx  int
		dist float32
	}

	distances := make([]centroidDist, ivfReader.NClusters)
	for i := 0; i < ivfReader.NClusters; i++ {
		centroid := ivfReader.Centroids[i*ivfReader.Dims : (i+1)*ivfReader.Dims]
		dist := computeDistance(query, centroid, ivfReader.Metric)
		distances[i] = centroidDist{idx: i, dist: dist}
	}

	// Partial sort to find top nProbe
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

// searchClusterData searches a cluster's data and computes exact distances.
func searchClusterData(data []byte, query []float32, clusterID int, dims int, metric vector.DistanceMetric) ([]vector.IVFSearchResult, error) {
	if len(data) == 0 {
		return nil, nil
	}

	reader := bytes.NewReader(data)
	entrySize := 8 + dims*4 // docID (8 bytes) + vector (dims * 4 bytes)
	numEntries := len(data) / entrySize

	results := make([]vector.IVFSearchResult, 0, numEntries)

	for i := 0; i < numEntries; i++ {
		// Read docID
		docIDBuf := make([]byte, 8)
		if _, err := io.ReadFull(reader, docIDBuf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		docID := uint64(docIDBuf[0]) | uint64(docIDBuf[1])<<8 | uint64(docIDBuf[2])<<16 | uint64(docIDBuf[3])<<24 |
			uint64(docIDBuf[4])<<32 | uint64(docIDBuf[5])<<40 | uint64(docIDBuf[6])<<48 | uint64(docIDBuf[7])<<56

		// Read vector
		vecBuf := make([]byte, dims*4)
		if _, err := io.ReadFull(reader, vecBuf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			bits := uint32(vecBuf[j*4]) | uint32(vecBuf[j*4+1])<<8 |
				uint32(vecBuf[j*4+2])<<16 | uint32(vecBuf[j*4+3])<<24
			vec[j] = float32frombits(bits)
		}

		// Compute exact distance
		dist := computeDistance(query, vec, metric)
		results = append(results, vector.IVFSearchResult{
			DocID:     docID,
			Distance:  dist,
			ClusterID: clusterID,
		})
	}

	return results, nil
}

// computeDistance calculates the distance between two vectors.
func computeDistance(a, b []float32, metric vector.DistanceMetric) float32 {
	switch metric {
	case vector.MetricCosineDistance:
		return cosineDistance(a, b)
	case vector.MetricEuclideanSquared:
		return euclideanSquared(a, b)
	case vector.MetricDotProduct:
		return -dotProduct(a, b)
	default:
		return cosineDistance(a, b)
	}
}

func cosineDistance(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	cosine := dot / (sqrt32(normA) * sqrt32(normB))
	return 1.0 - cosine
}

func euclideanSquared(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

func dotProduct(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

func sqrt32(x float32) float32 {
	// Newton-Raphson approximation
	if x <= 0 {
		return 0
	}
	guess := x / 2
	for i := 0; i < 10; i++ {
		guess = (guess + x/guess) / 2
	}
	return guess
}

func float32frombits(b uint32) float32 {
	return *(*float32)(unsafe.Pointer(&b))
}

// sortResultsByDistance sorts results by distance (ascending).
func sortResultsByDistance(results []vector.IVFSearchResult) {
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Distance < results[i].Distance {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}

// LoadFilterIndexes loads filter bitmap indexes for a segment.
// Returns a map of attribute name -> FilterIndex.
func (r *Reader) LoadFilterIndexes(ctx context.Context, filterKeys []string) (map[string]*filter.FilterIndex, error) {
	if len(filterKeys) == 0 {
		return nil, nil
	}

	result := make(map[string]*filter.FilterIndex)
	for _, key := range filterKeys {
		data, err := r.loadObject(ctx, key)
		if err != nil {
			// Skip missing filter indexes
			continue
		}
		idx, err := filter.Deserialize(data)
		if err != nil {
			continue
		}
		result[idx.Header().Attribute] = idx
	}

	return result, nil
}

// EvaluateFilterOnIndex evaluates a filter against filter bitmap indexes.
// Returns a bitmap of matching row IDs (docIDs).
func (r *Reader) EvaluateFilterOnIndex(f *filter.Filter, filterIndexes map[string]*filter.FilterIndex, totalDocs uint32) *roaring.Bitmap {
	if f == nil || len(filterIndexes) == 0 {
		return nil
	}
	return evaluateFilterBitmap(f, filterIndexes, totalDocs)
}

// evaluateFilterBitmap recursively evaluates filter conditions against bitmap indexes.
func evaluateFilterBitmap(f *filter.Filter, indexes map[string]*filter.FilterIndex, totalDocs uint32) *roaring.Bitmap {
	if f == nil {
		return nil
	}

	switch f.Op {
	case filter.OpAnd:
		return evaluateAndBitmap(f.Children, indexes, totalDocs)
	case filter.OpOr:
		return evaluateOrBitmap(f.Children, indexes, totalDocs)
	case filter.OpNot:
		if len(f.Children) > 0 {
			child := evaluateFilterBitmap(f.Children[0], indexes, totalDocs)
			if child == nil {
				return nil
			}
			// Create "all" bitmap and subtract
			all := roaring.NewBitmap()
			all.AddRange(0, uint64(totalDocs))
			all.AndNot(child)
			return all
		}
		return nil
	default:
		return evaluateLeafBitmap(f, indexes, totalDocs)
	}
}

func evaluateAndBitmap(children []*filter.Filter, indexes map[string]*filter.FilterIndex, totalDocs uint32) *roaring.Bitmap {
	if len(children) == 0 {
		return nil
	}

	var result *roaring.Bitmap
	for _, child := range children {
		childBitmap := evaluateFilterBitmap(child, indexes, totalDocs)
		if childBitmap == nil {
			// If any child cannot be evaluated with indexes, return nil
			return nil
		}
		if result == nil {
			result = childBitmap
		} else {
			result.And(childBitmap)
		}
		// Early exit if result is empty
		if result.IsEmpty() {
			return result
		}
	}
	return result
}

func evaluateOrBitmap(children []*filter.Filter, indexes map[string]*filter.FilterIndex, totalDocs uint32) *roaring.Bitmap {
	if len(children) == 0 {
		return nil
	}

	result := roaring.NewBitmap()
	for _, child := range children {
		childBitmap := evaluateFilterBitmap(child, indexes, totalDocs)
		if childBitmap == nil {
			// If any child cannot be evaluated with indexes, return nil
			return nil
		}
		result.Or(childBitmap)
	}
	return result
}

func evaluateLeafBitmap(f *filter.Filter, indexes map[string]*filter.FilterIndex, totalDocs uint32) *roaring.Bitmap {
	idx, ok := indexes[f.Attr]
	if !ok {
		// No index for this attribute
		return nil
	}

	switch f.Op {
	case filter.OpEq:
		return idx.Eq(f.Value)
	case filter.OpNotEq:
		all := roaring.NewBitmap()
		all.AddRange(0, uint64(totalDocs))
		return idx.NotEq(f.Value, all)
	case filter.OpLt:
		return idx.Lt(f.Value)
	case filter.OpLte:
		return idx.Lte(f.Value)
	case filter.OpGt:
		return idx.Gt(f.Value)
	case filter.OpGte:
		return idx.Gte(f.Value)
	case filter.OpIn:
		if values, ok := f.Value.([]any); ok {
			return idx.In(values)
		}
		return nil
	case filter.OpContains:
		return idx.Contains(f.Value)
	case filter.OpContainsAny:
		if values, ok := f.Value.([]any); ok {
			return idx.ContainsAny(values)
		}
		return nil
	default:
		return nil
	}
}

// SearchWithFilter performs ANN search with a filter bitmap constraint.
// Only vectors whose docIDs are in the filter bitmap are considered.
func (r *Reader) SearchWithFilter(ctx context.Context, ivfReader *vector.IVFReader, clusterDataKey string, query []float32, topK, nProbe int, filterBitmap *roaring.Bitmap) ([]vector.IVFSearchResult, error) {
	if ivfReader == nil {
		return nil, nil
	}

	// Find nearest centroids
	nearestClusters := findNearestCentroids(ivfReader, query, nProbe)

	// Get byte ranges for all selected clusters
	ranges := ivfReader.GetClusterRanges(nearestClusters)

	// Fetch all cluster data using multi-range optimization
	fetcher := NewMultiRangeClusterDataFetcher(r.store, r.diskCache, clusterDataKey)
	clusterData, err := fetcher.FetchRanges(ctx, ranges)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch cluster data: %w", err)
	}

	// Search each cluster with filter applied
	var results []vector.IVFSearchResult
	for clusterID, data := range clusterData {
		clusterResults, err := searchClusterDataWithFilter(data, query, clusterID, ivfReader.Dims, ivfReader.Metric, filterBitmap)
		if err != nil {
			return nil, fmt.Errorf("failed to search cluster %d: %w", clusterID, err)
		}
		results = append(results, clusterResults...)
	}

	// Sort by distance and return top K
	sortResultsByDistance(results)
	if len(results) > topK {
		results = results[:topK]
	}

	return results, nil
}

// searchClusterDataWithFilter searches a cluster's data with a filter bitmap.
func searchClusterDataWithFilter(data []byte, query []float32, clusterID int, dims int, metric vector.DistanceMetric, filterBitmap *roaring.Bitmap) ([]vector.IVFSearchResult, error) {
	if len(data) == 0 {
		return nil, nil
	}

	reader := bytes.NewReader(data)
	entrySize := 8 + dims*4 // docID (8 bytes) + vector (dims * 4 bytes)
	numEntries := len(data) / entrySize

	results := make([]vector.IVFSearchResult, 0, numEntries)

	for i := 0; i < numEntries; i++ {
		// Read docID
		docIDBuf := make([]byte, 8)
		if _, err := io.ReadFull(reader, docIDBuf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		docID := uint64(docIDBuf[0]) | uint64(docIDBuf[1])<<8 | uint64(docIDBuf[2])<<16 | uint64(docIDBuf[3])<<24 |
			uint64(docIDBuf[4])<<32 | uint64(docIDBuf[5])<<40 | uint64(docIDBuf[6])<<48 | uint64(docIDBuf[7])<<56

		// Read vector
		vecBuf := make([]byte, dims*4)
		if _, err := io.ReadFull(reader, vecBuf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Apply filter if present
		if filterBitmap != nil && !filterBitmap.Contains(uint32(docID)) {
			continue
		}

		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			bits := uint32(vecBuf[j*4]) | uint32(vecBuf[j*4+1])<<8 |
				uint32(vecBuf[j*4+2])<<16 | uint32(vecBuf[j*4+3])<<24
			vec[j] = float32frombits(bits)
		}

		// Compute exact distance
		dist := computeDistance(query, vec, metric)
		results = append(results, vector.IVFSearchResult{
			DocID:     docID,
			Distance:  dist,
			ClusterID: clusterID,
		})
	}

	return results, nil
}

// GetManifestSegments returns all segments from the manifest with their filter keys.
func (r *Reader) GetManifestSegments(ctx context.Context, manifestKey string) ([]Segment, error) {
	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, nil
	}
	return manifest.Segments, nil
}

// IndexedDocument represents a document read from an index segment.
// Uses default JSON field names to match the compactor's docs column format.
type IndexedDocument struct {
	ID         string
	NumericID  uint64
	WALSeq     uint64
	Deleted    bool
	Attributes map[string]any
}

// LoadSegmentDocs loads documents from all segments in a manifest.
// Documents are returned with their WAL sequence for deduplication.
func (r *Reader) LoadSegmentDocs(ctx context.Context, manifestKey string) ([]IndexedDocument, error) {
	if manifestKey == "" || r.store == nil {
		return nil, nil
	}

	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, nil
	}

	var allDocs []IndexedDocument
	for _, seg := range manifest.Segments {
		docs, err := r.loadDocsFromSegment(ctx, seg)
		if err != nil {
			// Log error and continue with other segments
			continue
		}
		allDocs = append(allDocs, docs...)
	}

	return allDocs, nil
}

// loadDocsFromSegment reads documents from a single segment.
func (r *Reader) loadDocsFromSegment(ctx context.Context, seg Segment) ([]IndexedDocument, error) {
	if seg.DocsKey == "" {
		return nil, nil
	}

	data, err := r.loadObject(ctx, seg.DocsKey)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	// Decode column format (JSON encoded docs, matching compactor format)
	var docs []IndexedDocument
	if err := json.Unmarshal(data, &docs); err != nil {
		return nil, fmt.Errorf("failed to parse docs data: %w", err)
	}

	return docs, nil
}

// LoadFTSIndexes loads FTS indexes from all segments in a manifest.
// Returns a map of attribute name -> list of FTS indexes (one per segment).
// Multiple indexes for the same attribute need to be merged for BM25 scoring.
func (r *Reader) LoadFTSIndexes(ctx context.Context, manifestKey string) (map[string][][]byte, error) {
	if manifestKey == "" || r.store == nil {
		return nil, nil
	}

	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, nil
	}

	result := make(map[string][][]byte)
	for _, seg := range manifest.Segments {
		for _, ftsKey := range seg.FTSKeys {
			data, err := r.loadObject(ctx, ftsKey)
			if err != nil {
				// Skip if we can't load this FTS index
				continue
			}
			// Extract attribute name from the key
			// Format: <segmentKey>/fts/<attribute>.idx
			attrName := extractAttrNameFromFTSKey(ftsKey)
			if attrName != "" {
				result[attrName] = append(result[attrName], data)
			}
		}
	}

	return result, nil
}

// extractAttrNameFromFTSKey extracts the attribute name from an FTS key.
// Key format: .../fts/<attribute>.idx
func extractAttrNameFromFTSKey(key string) string {
	// Find /fts/ in the key
	idx := -1
	for i := len(key) - 1; i >= 4; i-- {
		if key[i-4:i+1] == "/fts/" {
			idx = i + 1
			break
		}
	}
	if idx == -1 || idx >= len(key) {
		return ""
	}
	// Extract attribute name (before .idx suffix)
	rest := key[idx:]
	if len(rest) > 4 && rest[len(rest)-4:] == ".idx" {
		return rest[:len(rest)-4]
	}
	return ""
}

// Clear removes cached readers for a namespace.
func (r *Reader) Clear(namespace string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove all readers for this namespace
	for key := range r.readers {
		if len(key) > len(namespace) && key[:len(namespace)+1] == namespace+"/" {
			delete(r.readers, key)
		}
	}
}

// Close releases all resources.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readers = make(map[string]*cachedIVFReader)
	return nil
}
