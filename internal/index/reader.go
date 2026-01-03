// Package index provides index reading functionality for query nodes.
package index

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/klauspost/compress/zstd"
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
	// largeClusterChunkBytes controls streaming chunk size for large clusters.
	largeClusterChunkBytes int

	// Per-namespace IVF reader cache
	mu      sync.RWMutex
	readers map[string]*cachedIVFReader
}

// ReaderOptions configures index reader behavior.
type ReaderOptions struct {
	// LargeClusterChunkBytes controls chunk size for streaming large clusters.
	// If zero, DefaultLargeClusterChunkBytes is used.
	LargeClusterChunkBytes int
}

const DefaultLargeClusterChunkBytes = 5 * 1024 * 1024

// cachedIVFReader holds a cached IVF reader for a segment.
type cachedIVFReader struct {
	reader         *vector.IVFReader
	manifestSeq    uint64
	segmentID      string
	clusterDataKey string
}

// NewReader creates a new index reader.
func NewReader(store objectstore.Store, diskCache *cache.DiskCache, ramCache *cache.MemoryCache) *Reader {
	return NewReaderWithOptions(store, diskCache, ramCache, ReaderOptions{})
}

// NewReaderWithOptions creates a new index reader with custom options.
func NewReaderWithOptions(store objectstore.Store, diskCache *cache.DiskCache, ramCache *cache.MemoryCache, opts ReaderOptions) *Reader {
	return &Reader{
		store:                  store,
		diskCache:              diskCache,
		ramCache:               ramCache,
		largeClusterChunkBytes: opts.LargeClusterChunkBytes,
		readers:                make(map[string]*cachedIVFReader),
	}
}

// readAllWithContext reads all bytes from a reader, honoring context cancellation.
// This prevents S3 reads from blocking indefinitely when the context is cancelled.
func readAllWithContext(ctx context.Context, r io.Reader) ([]byte, error) {
	if ctx == nil {
		return io.ReadAll(r)
	}
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(r)
		select {
		case done <- result{data: data, err: err}:
		default:
		}
	}()

	select {
	case <-ctx.Done():
		if closer, ok := r.(io.Closer); ok {
			_ = closer.Close()
		}
		return nil, ctx.Err()
	case res := <-done:
		return res.data, res.err
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
			data, err := readAllWithContext(ctx, reader)
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

	data, err := readAllWithContext(ctx, reader)
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
			data, err := readAllWithContext(ctx, reader)
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

	data, err := readAllWithContext(ctx, reader)
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
				data, err := readAllWithContext(ctx, reader)
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

		data, err := readAllWithContext(ctx, reader)
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
	store          objectstore.Store
	diskCache      *cache.DiskCache
	clusterDataKey string
	maxParallel    int
}

// NewMultiRangeClusterDataFetcher creates a new multi-range fetcher.
func NewMultiRangeClusterDataFetcher(store objectstore.Store, diskCache *cache.DiskCache, clusterDataKey string) *MultiRangeClusterDataFetcher {
	return &MultiRangeClusterDataFetcher{
		store:          store,
		diskCache:      diskCache,
		clusterDataKey: clusterDataKey,
		maxParallel:    4,
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
				data, err := readAllWithContext(ctx, reader)
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

	maxParallel := f.maxParallel
	if maxParallel <= 0 {
		maxParallel = 1
	}
	if maxParallel > len(merged) {
		maxParallel = len(merged)
	}

	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	var once sync.Once
	sem := make(chan struct{}, maxParallel)

	for _, mr := range merged {
		mr := mr
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			opts := &objectstore.GetOptions{
				Range: &objectstore.ByteRange{
					Start: int64(mr.Offset),
					End:   int64(mr.Offset + mr.Length - 1),
				},
			}

			reader, _, err := f.store.Get(fetchCtx, f.clusterDataKey, opts)
			if err != nil {
				once.Do(func() {
					firstErr = fmt.Errorf("failed to fetch range [%d, %d): %w", mr.Offset, mr.Offset+mr.Length, err)
					cancel()
				})
				return
			}

			// Use LimitReader to only read the expected amount (minio may return more than requested)
			limitedReader := io.LimitReader(reader, int64(mr.Length))
			data, err := readAllWithContext(fetchCtx, limitedReader)
			reader.Close()
			if err != nil {
				once.Do(func() {
					firstErr = fmt.Errorf("failed to read range data: %w", err)
					cancel()
				})
				return
			}

			mu.Lock()
			defer mu.Unlock()

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
		}()
	}

	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}

	return result, nil
}

// MaxClusterSizeForBatch is the max cluster size to load into memory.
// Larger clusters are streamed to avoid OOM.
const MaxClusterSizeForBatch = 10 * 1024 * 1024 // 10MB

type topKCollector struct {
	limit   int
	heap    topKHeap
	maxSize int
}

type topKHeap []vector.IVFSearchResult

func (h topKHeap) Len() int           { return len(h) }
func (h topKHeap) Less(i, j int) bool { return h[i].Distance > h[j].Distance }
func (h topKHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *topKHeap) Push(x any) {
	*h = append(*h, x.(vector.IVFSearchResult))
}

func (h *topKHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func newTopKCollector(limit int) *topKCollector {
	if limit < 1 {
		limit = 1
	}
	return &topKCollector{limit: limit}
}

func (c *topKCollector) consider(res vector.IVFSearchResult) {
	if len(c.heap) < c.limit {
		heap.Push(&c.heap, res)
		if len(c.heap) > c.maxSize {
			c.maxSize = len(c.heap)
		}
		return
	}
	if len(c.heap) == 0 {
		return
	}
	if res.Distance < c.heap[0].Distance {
		c.heap[0] = res
		heap.Fix(&c.heap, 0)
	}
}

func (c *topKCollector) results() []vector.IVFSearchResult {
	if len(c.heap) == 0 {
		return nil
	}
	results := make([]vector.IVFSearchResult, len(c.heap))
	copy(results, c.heap)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})
	return results
}

// SearchWithMultiRange performs ANN search using multi-range fetching for efficiency.
func (r *Reader) SearchWithMultiRange(ctx context.Context, ivfReader *vector.IVFReader, clusterDataKey string, query []float32, topK, nProbe int) ([]vector.IVFSearchResult, error) {
	results, _, err := r.searchANN(ctx, ivfReader, clusterDataKey, query, topK, nProbe, nil)
	return results, err
}

func (r *Reader) searchWithMultiRangeStats(ctx context.Context, ivfReader *vector.IVFReader, clusterDataKey string, query []float32, topK, nProbe int) ([]vector.IVFSearchResult, int, error) {
	results, collector, err := r.searchANN(ctx, ivfReader, clusterDataKey, query, topK, nProbe, nil)
	if collector == nil {
		return results, 0, err
	}
	return results, collector.maxSize, err
}

func (r *Reader) searchANN(ctx context.Context, ivfReader *vector.IVFReader, clusterDataKey string, query []float32, topK, nProbe int, filterBitmap *roaring.Bitmap) ([]vector.IVFSearchResult, *topKCollector, error) {
	if ivfReader == nil {
		return nil, nil, nil
	}

	collector := newTopKCollector(topK)
	if err := r.collectANN(ctx, ivfReader, clusterDataKey, query, nProbe, filterBitmap, collector); err != nil {
		return nil, collector, err
	}

	return collector.results(), collector, nil
}

func (r *Reader) collectANN(ctx context.Context, ivfReader *vector.IVFReader, clusterDataKey string, query []float32, nProbe int, filterBitmap *roaring.Bitmap, collector *topKCollector) error {
	// Find nearest centroids
	nearestClusters := findNearestCentroids(ivfReader, query, nProbe)

	// Get byte ranges for all selected clusters
	ranges := ivfReader.GetClusterRanges(nearestClusters)

	// Separate small clusters (can batch) from large clusters (must stream)
	var smallRanges []vector.ByteRange
	var largeRanges []vector.ByteRange
	for _, rng := range ranges {
		if rng.Length <= MaxClusterSizeForBatch {
			smallRanges = append(smallRanges, rng)
		} else {
			largeRanges = append(largeRanges, rng)
		}
	}

	// Process small clusters in batch
	if len(smallRanges) > 0 {
		fetcher := NewMultiRangeClusterDataFetcher(r.store, r.diskCache, clusterDataKey)
		clusterData, err := fetcher.FetchRanges(ctx, smallRanges)
		if err != nil {
			return fmt.Errorf("failed to fetch cluster data: %w", err)
		}

		for clusterID, data := range clusterData {
			if err := searchClusterDataInto(data, query, clusterID, ivfReader.Dims, ivfReader.DType, ivfReader.Metric, filterBitmap, collector); err != nil {
				return fmt.Errorf("failed to search cluster %d: %w", clusterID, err)
			}
		}
	}

	// Stream large clusters one at a time
	for _, rng := range largeRanges {
		if err := r.searchClusterStreaming(ctx, clusterDataKey, rng, query, ivfReader.Dims, ivfReader.DType, ivfReader.Metric, filterBitmap, collector); err != nil {
			continue // Skip failed clusters
		}
	}

	return nil
}

func (r *Reader) streamingChunkSize(entrySize int) int {
	chunkSize := r.largeClusterChunkBytes
	if chunkSize <= 0 {
		chunkSize = DefaultLargeClusterChunkBytes
	}
	if chunkSize < entrySize {
		chunkSize = entrySize
	}
	aligned := (chunkSize / entrySize) * entrySize
	if aligned == 0 {
		aligned = entrySize
	}
	return aligned
}

// searchClusterStreaming searches a large cluster by streaming data in chunks.
func (r *Reader) searchClusterStreaming(ctx context.Context, clusterDataKey string, rng vector.ByteRange, query []float32, dims int, dtype vector.DType, metric vector.DistanceMetric, filterBitmap *roaring.Bitmap, collector *topKCollector) error {
	// Each vector entry is: 8 bytes (docID) + dims*element bytes (vector data)
	entrySize := 8 + dims*vectorBytesPerElement(dtype)
	chunkSize := r.streamingChunkSize(entrySize)

	offset := rng.Offset
	remaining := rng.Length

	for remaining > 0 {
		fetchLen := uint64(chunkSize)
		if fetchLen > remaining {
			fetchLen = remaining
		}

		opts := &objectstore.GetOptions{
			Range: &objectstore.ByteRange{
				Start: int64(offset),
				End:   int64(offset + fetchLen - 1),
			},
		}

		reader, _, err := r.store.Get(ctx, clusterDataKey, opts)
		if err != nil {
			return fmt.Errorf("failed to fetch cluster chunk: %w", err)
		}

		// Use LimitReader to only read the expected amount (minio may return more than requested)
		limitedReader := io.LimitReader(reader, int64(fetchLen))
		data, err := readAllWithContext(ctx, limitedReader)
		reader.Close()
		if err != nil {
			return fmt.Errorf("failed to read cluster chunk: %w", err)
		}

		// Search this chunk
		if err := searchClusterDataInto(data, query, rng.ClusterID, dims, dtype, metric, filterBitmap, collector); err != nil {
			return err
		}

		offset += fetchLen
		remaining -= fetchLen
	}

	return nil
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

// searchClusterDataInto searches a cluster's data and computes exact distances.
func searchClusterDataInto(data []byte, query []float32, clusterID int, dims int, dtype vector.DType, metric vector.DistanceMetric, filterBitmap *roaring.Bitmap, collector *topKCollector) error {
	if len(data) == 0 {
		return nil
	}

	reader := bytes.NewReader(data)
	entrySize := 8 + dims*vectorBytesPerElement(dtype)
	numEntries := len(data) / entrySize

	docIDBuf := make([]byte, 8)
	vec := make([]float32, dims)

	for i := 0; i < numEntries; i++ {
		// Read docID
		if _, err := io.ReadFull(reader, docIDBuf); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		docID := uint64(docIDBuf[0]) | uint64(docIDBuf[1])<<8 | uint64(docIDBuf[2])<<16 | uint64(docIDBuf[3])<<24 |
			uint64(docIDBuf[4])<<32 | uint64(docIDBuf[5])<<40 | uint64(docIDBuf[6])<<48 | uint64(docIDBuf[7])<<56

		// Read vector
		if err := readVectorElements(reader, vec, dtype); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if filterBitmap != nil && !filterBitmap.Contains(uint32(docID)) {
			continue
		}

		// Compute exact distance
		dist := computeDistance(query, vec, metric)
		if collector != nil {
			collector.consider(vector.IVFSearchResult{
				DocID:     docID,
				Distance:  dist,
				ClusterID: clusterID,
			})
		}
	}

	return nil
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

func vectorBytesPerElement(dtype vector.DType) int {
	if dtype == vector.DTypeF16 {
		return 2
	}
	return 4
}

func readVectorElements(r io.Reader, out []float32, dtype vector.DType) error {
	switch dtype {
	case vector.DTypeF16:
		buf := make([]byte, len(out)*2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		for i := range out {
			bits := binary.LittleEndian.Uint16(buf[i*2:])
			out[i] = vector.Float16ToFloat32(bits)
		}
	default:
		buf := make([]byte, len(out)*4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		for i := range out {
			bits := binary.LittleEndian.Uint32(buf[i*4:])
			out[i] = float32frombits(bits)
		}
	}
	return nil
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
	results, _, err := r.searchANN(ctx, ivfReader, clusterDataKey, query, topK, nProbe, filterBitmap)
	return results, err
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

// LoadIVFSegmentDocs loads documents for the first IVF segment in a manifest.
// Returns nil when no IVF segment or docs column exists.
func (r *Reader) LoadIVFSegmentDocs(ctx context.Context, manifestKey string) ([]IndexedDocument, error) {
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

	var ivfSegment *Segment
	for i := range manifest.Segments {
		if manifest.Segments[i].IVFKeys.HasIVF() {
			ivfSegment = &manifest.Segments[i]
			break
		}
	}
	if ivfSegment == nil {
		return nil, nil
	}

	return r.loadDocsFromSegment(ctx, *ivfSegment)
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

// LoadDocsForIDs loads only the documents matching the specified numeric IDs.
// This is more memory-efficient than LoadSegmentDocs when you only need a subset
// of documents (e.g., top-k results from IVF search).
// Important: This searches ALL segments to find tombstones that may exist in later segments.
func (r *Reader) LoadDocsForIDs(ctx context.Context, manifestKey string, ids []uint64) ([]IndexedDocument, error) {
	if manifestKey == "" || r.store == nil || len(ids) == 0 {
		return nil, nil
	}

	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, nil
	}

	// Build a set of requested IDs for O(1) lookup
	idSet := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}

	// Collect all versions of requested docs from all segments.
	// We must search all segments because a tombstone in a later segment
	// should override a document in an earlier segment.
	var allDocs []IndexedDocument
	for _, seg := range manifest.Segments {
		// Don't remove from idSet - we need to find ALL versions including tombstones
		docs, err := r.loadDocsForIDsFromSegment(ctx, seg, copyIDSet(idSet))
		if err != nil {
			continue
		}
		allDocs = append(allDocs, docs...)
	}

	// Deduplicate: keep the document with the highest WAL seq for each ID
	// This ensures tombstones (higher WAL seq) properly shadow earlier versions
	docsByID := make(map[uint64]IndexedDocument)
	for _, doc := range allDocs {
		if existing, ok := docsByID[doc.NumericID]; !ok || doc.WALSeq > existing.WALSeq {
			docsByID[doc.NumericID] = doc
		}
	}

	result := make([]IndexedDocument, 0, len(docsByID))
	for _, doc := range docsByID {
		result = append(result, doc)
	}

	return result, nil
}

// copyIDSet creates a copy of an ID set
func copyIDSet(idSet map[uint64]struct{}) map[uint64]struct{} {
	copy := make(map[uint64]struct{}, len(idSet))
	for id := range idSet {
		copy[id] = struct{}{}
	}
	return copy
}

// partialDoc is used for efficient streaming - defers attribute parsing until we know we need the doc
type partialDoc struct {
	ID         string          `json:"ID"`
	NumericID  uint64          `json:"NumericID"`
	WALSeq     uint64          `json:"WALSeq"`
	Deleted    bool            `json:"Deleted"`
	Attributes json.RawMessage `json:"Attributes"` // Defer parsing until we know we need this doc
}

// loadDocsForIDsFromSegment loads only documents matching the requested IDs from a segment.
// Uses streaming from object storage with zstd decompression to avoid loading all documents into memory.
// Optimized to defer attribute parsing until we confirm a document matches.
func (r *Reader) loadDocsForIDsFromSegment(ctx context.Context, seg Segment, idSet map[uint64]struct{}) ([]IndexedDocument, error) {
	if seg.DocsKey == "" {
		return nil, nil
	}

	// Try to use disk cache for faster reads
	var reader io.ReadCloser
	cacheKey := cache.CacheKey{ObjectKey: seg.DocsKey}
	if r.diskCache != nil {
		if cachedReader, err := r.diskCache.GetReader(cacheKey); err == nil {
			reader = cachedReader
		}
	}

	// Fall back to object storage if not cached
	if reader == nil {
		storeReader, _, err := r.store.Get(ctx, seg.DocsKey, nil)
		if err != nil {
			if objectstore.IsNotFoundError(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to get docs: %w", err)
		}
		reader = storeReader
	}
	defer reader.Close()

	// Peek first 4 bytes to check for zstd magic (0x28B52FFD)
	magic := make([]byte, 4)
	n, err := io.ReadFull(reader, magic)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to read docs magic: %w", err)
	}
	if n == 0 {
		return nil, nil
	}

	// Create a reader that includes the magic bytes we already read
	fullReader := io.MultiReader(bytes.NewReader(magic[:n]), reader)

	// Check for zstd magic bytes and wrap with decompressor if needed
	var jsonReader io.Reader
	isZstd := n >= 4 && magic[0] == 0x28 && magic[1] == 0xB5 && magic[2] == 0x2F && magic[3] == 0xFD
	if isZstd {
		// Use low memory mode and limit window size to reduce memory usage
		zstdReader, err := zstd.NewReader(fullReader,
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(32*1024*1024), // Limit to 32MB window
			zstd.WithDecoderConcurrency(1),          // Single-threaded to reduce memory
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer zstdReader.Close()
		jsonReader = zstdReader
	} else {
		// Data is not zstd compressed, use as-is
		jsonReader = fullReader
	}

	// Use streaming JSON decoder directly from the reader (no buffering the whole file)
	decoder := json.NewDecoder(jsonReader)

	// Read opening bracket
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON start: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected JSON array, got %v", token)
	}

	var result []IndexedDocument

	// Stream through each document - use partialDoc to defer attribute parsing
	// This significantly reduces memory allocation for non-matching documents
	for decoder.More() {
		var partial partialDoc
		if err := decoder.Decode(&partial); err != nil {
			return nil, fmt.Errorf("failed to decode document: %w", err)
		}

		// Only fully parse attributes for documents that match requested IDs
		if _, ok := idSet[partial.NumericID]; ok {
			// Parse attributes only for matching documents
			var attrs map[string]any
			if len(partial.Attributes) > 0 {
				if err := json.Unmarshal(partial.Attributes, &attrs); err != nil {
					// If attributes fail to parse, use empty map
					attrs = make(map[string]any)
				}
			}

			result = append(result, IndexedDocument{
				ID:         partial.ID,
				NumericID:  partial.NumericID,
				WALSeq:     partial.WALSeq,
				Deleted:    partial.Deleted,
				Attributes: attrs,
			})

			// Remove from set to track what we've found
			delete(idSet, partial.NumericID)

			// If we've found all requested IDs, stop early
			if len(idSet) == 0 {
				break
			}
		}
	}

	return result, nil
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
