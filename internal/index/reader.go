// Package index provides index reading functionality for query nodes.
package index

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/klauspost/compress/zstd"
	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/fts"
	"github.com/vexsearch/vex/internal/metrics"
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
	mu       sync.RWMutex
	readers  map[string]*cachedIVFReader
	ftsCache map[string]*fts.Index
	ftsTerms map[string][]string

	docOffsetsCache map[string][]uint64
	docIDRowMu      sync.RWMutex
	docIDRowCache   map[string]map[uint64]uint32
	lastManifestSeq map[string]uint64
}

// ReaderOptions configures index reader behavior.
type ReaderOptions struct {
	// LargeClusterChunkBytes controls chunk size for streaming large clusters.
	// If zero, DefaultLargeClusterChunkBytes is used.
	LargeClusterChunkBytes int
}

const (
	DefaultLargeClusterChunkBytes = 8 * 1024 * 1024
	multiRangeMergeGapBytes       = 64 * 1024

	// RAMCacheShardID is the shard identifier used for index objects cached in RAM.
	RAMCacheShardID = "warm"
)

// cachedIVFReader holds a cached IVF reader for a segment.
type cachedIVFReader struct {
	reader         *vector.IVFReader
	manifestSeq    uint64
	segmentID      string
	clusterDataKey string
	segment        Segment
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
		ftsCache:               make(map[string]*fts.Index),
		ftsTerms:               make(map[string][]string),
		docOffsetsCache:        make(map[string][]uint64),
		docIDRowCache:          make(map[string]map[uint64]uint32),
		lastManifestSeq:        make(map[string]uint64),
	}
}

// readAllWithContext reads all bytes from a reader, honoring context cancellation.
// This prevents S3 reads from blocking indefinitely when the context is cancelled.
func readAllWithContext(ctx context.Context, r io.Reader) ([]byte, error) {
	if ctx == nil || ctx.Done() == nil {
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

func readAllLocal(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}

type objectLoadOptions struct {
	useRAM     bool
	ramItem    cache.CacheItemType
	ramKey     cache.MemoryCacheKey
	ramEnabled bool
}

type objectLoadOption func(*objectLoadOptions)

func withRAMCache(itemType cache.CacheItemType) objectLoadOption {
	return func(opts *objectLoadOptions) {
		opts.useRAM = true
		opts.ramItem = itemType
	}
}

func namespaceFromObjectKey(key string) string {
	const marker = "/namespaces/"
	if idx := strings.Index(key, marker); idx != -1 {
		start := idx + len(marker)
		if start >= len(key) {
			return ""
		}
		rest := key[start:]
		if slash := strings.IndexByte(rest, '/'); slash != -1 {
			return rest[:slash]
		}
		return rest
	}
	if strings.HasPrefix(key, "namespaces/") {
		rest := strings.TrimPrefix(key, "namespaces/")
		if slash := strings.IndexByte(rest, '/'); slash != -1 {
			return rest[:slash]
		}
		return rest
	}
	if strings.HasPrefix(key, "/namespaces/") {
		rest := strings.TrimPrefix(key, "/namespaces/")
		if slash := strings.IndexByte(rest, '/'); slash != -1 {
			return rest[:slash]
		}
		return rest
	}
	return ""
}

func (r *Reader) ramCacheKey(objectKey string, itemType cache.CacheItemType) (cache.MemoryCacheKey, bool) {
	if r.ramCache == nil {
		return cache.MemoryCacheKey{}, false
	}
	ns := namespaceFromObjectKey(objectKey)
	if ns == "" {
		return cache.MemoryCacheKey{}, false
	}
	return cache.MemoryCacheKey{
		Namespace: ns,
		ShardID:   RAMCacheShardID,
		ItemID:    objectKey,
		ItemType:  itemType,
	}, true
}

// LoadManifest loads the manifest for a namespace from the given key.
func (r *Reader) LoadManifest(ctx context.Context, manifestKey string) (*Manifest, error) {
	if manifestKey == "" {
		return nil, nil
	}

	data, err := r.loadObject(ctx, manifestKey, withRAMCache(cache.TypePostingDict))
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	r.noteManifest(manifestKey, &manifest)
	return &manifest, nil
}

func (r *Reader) noteManifest(manifestKey string, manifest *Manifest) {
	if manifest == nil {
		return
	}

	namespace := manifest.Namespace
	if namespace == "" {
		namespace = parseNamespaceFromManifestKey(manifestKey)
	}
	if namespace == "" {
		return
	}

	totalSegments := 0
	l0Segments := 0
	l1Segments := 0
	l2Segments := 0
	for _, seg := range manifest.Segments {
		totalSegments++
		switch seg.Level {
		case L0:
			l0Segments++
		case L1:
			l1Segments++
		case L2:
			l2Segments++
		}
	}
	metrics.SetSegmentCounts(namespace, totalSegments, l0Segments, l1Segments, l2Segments)
	metrics.SetDocumentsIndexed(namespace, manifest.Stats.ApproxRowCount)

	seq, ok := parseManifestSeq(manifestKey)
	if !ok {
		return
	}

	r.mu.Lock()
	prev, seen := r.lastManifestSeq[namespace]
	if seen && seq <= prev {
		r.mu.Unlock()
		return
	}
	r.lastManifestSeq[namespace] = seq
	r.clearNamespaceCachesLocked(namespace)
	r.mu.Unlock()

	r.clearDocIDRowCache(namespace)
}

func parseManifestSeq(manifestKey string) (uint64, bool) {
	if manifestKey == "" || !strings.HasSuffix(manifestKey, ".idx.json") {
		return 0, false
	}
	base := manifestKey
	if slash := strings.LastIndexByte(base, '/'); slash >= 0 && slash+1 < len(base) {
		base = base[slash+1:]
	}
	base = strings.TrimSuffix(base, ".idx.json")
	if base == "" {
		return 0, false
	}
	seq, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

func parseNamespaceFromManifestKey(manifestKey string) string {
	const prefix = "vex/namespaces/"
	if !strings.HasPrefix(manifestKey, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(manifestKey, prefix)
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		return ""
	}
	return rest[:slash]
}

func selectIVFSegment(segments []Segment) *Segment {
	candidates := make([]Segment, 0, len(segments))
	for _, seg := range segments {
		if seg.IVFKeys != nil && seg.IVFKeys.HasIVF() {
			candidates = append(candidates, seg)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	// Prefer the segment that is most up-to-date, then the largest, then higher LSM levels.
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.EndWALSeq != b.EndWALSeq {
			return a.EndWALSeq > b.EndWALSeq
		}
		av, bv := ivfVectorCount(a), ivfVectorCount(b)
		if av != bv {
			return av > bv
		}
		if a.Level != b.Level {
			return a.Level > b.Level
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.After(b.CreatedAt)
		}
		return a.ID > b.ID
	})

	return &candidates[0]
}

func ivfVectorCount(seg Segment) int64 {
	switch {
	case seg.IVFKeys != nil && seg.IVFKeys.VectorCount > 0:
		return int64(seg.IVFKeys.VectorCount)
	case seg.Stats.RowCount > 0:
		return seg.Stats.RowCount
	default:
		return seg.Stats.LogicalBytes
	}
}

// GetIVFReader returns an IVF reader for the given namespace.
// It loads the manifest and creates a reader for the best available IVF segment
// (preferring newer, larger, higher-level segments). The reader is cached for
// subsequent queries.
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

	ivfSegment := selectIVFSegment(manifest.Segments)
	if ivfSegment == nil {
		return nil, "", nil
	}

	// Load centroids
	centroidsData, err := r.loadObject(ctx, ivfSegment.IVFKeys.CentroidsKey, withRAMCache(cache.TypeCentroid))
	if err != nil {
		return nil, "", fmt.Errorf("failed to load centroids: %w", err)
	}

	// Load cluster offsets
	offsetsData, err := r.loadObject(ctx, ivfSegment.IVFKeys.ClusterOffsetsKey, withRAMCache(cache.TypeCentroid))
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
		segment:        *ivfSegment,
	}
	r.mu.Unlock()

	return ivfReader, clusterDataKey, nil
}

// GetIVFSegment returns the preferred segment with IVF data for the manifest.
func (r *Reader) GetIVFSegment(ctx context.Context, namespace string, manifestKey string, manifestSeq uint64) (*Segment, error) {
	if manifestKey == "" {
		return nil, nil
	}

	readerKey := fmt.Sprintf("%s/%d", namespace, manifestSeq)
	r.mu.RLock()
	cached, ok := r.readers[readerKey]
	r.mu.RUnlock()
	if ok && cached.segment.ID != "" {
		seg := cached.segment
		return &seg, nil
	}

	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, nil
	}

	ivfSegment := selectIVFSegment(manifest.Segments)
	if ivfSegment != nil {
		seg := *ivfSegment
		return &seg, nil
	}

	return nil, nil
}

// loadObject loads an object from storage, using cache if available.
func (r *Reader) loadObject(ctx context.Context, key string, opts ...objectLoadOption) ([]byte, error) {
	if key == "" {
		return nil, nil
	}

	cfg := objectLoadOptions{}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.useRAM {
		if ramKey, ok := r.ramCacheKey(key, cfg.ramItem); ok {
			cfg.ramEnabled = true
			cfg.ramKey = ramKey
			if data, err := r.ramCache.Get(ramKey); err == nil {
				return data, nil
			}
		}
	}

	// Try cache first
	cacheKey := cache.CacheKey{ObjectKey: key}
	if r.diskCache != nil {
		if reader, err := r.diskCache.GetReader(cacheKey); err == nil {
			data, err := readAllLocal(reader)
			reader.Close()
			if err == nil {
				if cfg.ramEnabled {
					_ = r.ramCache.Put(cfg.ramKey, data)
				}
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
	if cfg.ramEnabled {
		_ = r.ramCache.Put(cfg.ramKey, data)
	}

	return data, nil
}

// loadObjectRange loads a byte range from storage, using the disk cache when available.
func (r *Reader) loadObjectRange(ctx context.Context, key string, start, length uint64) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}

	cacheKey := cache.CacheKey{
		ObjectKey: fmt.Sprintf("%s#%d-%d", key, start, length),
	}

	if r.diskCache != nil {
		if reader, err := r.diskCache.GetReader(cacheKey); err == nil {
			data, err := readAllLocal(reader)
			reader.Close()
			if err == nil {
				return data, nil
			}
		}
	}

	opts := &objectstore.GetOptions{
		Range: &objectstore.ByteRange{
			Start: int64(start),
			End:   int64(start + length - 1),
		},
	}

	reader, _, err := r.store.Get(ctx, key, opts)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := readAllWithContext(ctx, reader)
	if err != nil {
		return nil, err
	}

	if r.diskCache != nil {
		r.diskCache.PutBytes(cacheKey, data)
	}

	return data, nil
}

// createClusterDataFetcher creates a ClusterDataFetcher that uses range reads.
func (r *Reader) createClusterDataFetcher(ctx context.Context, clusterDataKey string) vector.ClusterDataFetcher {
	ns := namespaceFromObjectKey(clusterDataKey)
	return func(fetchCtx context.Context, offset, length uint64) ([]byte, error) {
		// Create a cache key specific to this range
		cacheKey := cache.CacheKey{
			ObjectKey: fmt.Sprintf("%s#%d-%d", clusterDataKey, offset, length),
		}

		// Try cache first
		if r.diskCache != nil {
			if reader, err := r.diskCache.GetReader(cacheKey); err == nil {
				metrics.IncANNClusterRangeCacheHit(ns)
				data, err := readAllLocal(reader)
				reader.Close()
				if err == nil {
					return data, nil
				}
			} else if errors.Is(err, cache.ErrCacheMiss) {
				metrics.IncANNClusterRangeCacheMiss(ns)
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
	namespace      string
}

// NewMultiRangeClusterDataFetcher creates a new multi-range fetcher.
func NewMultiRangeClusterDataFetcher(store objectstore.Store, diskCache *cache.DiskCache, clusterDataKey string) *MultiRangeClusterDataFetcher {
	maxParallel := runtime.NumCPU()
	if maxParallel < 4 {
		maxParallel = 4
	}
	return &MultiRangeClusterDataFetcher{
		store:          store,
		diskCache:      diskCache,
		clusterDataKey: clusterDataKey,
		maxParallel:    maxParallel,
		namespace:      namespaceFromObjectKey(clusterDataKey),
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
				metrics.IncANNClusterRangeCacheHit(f.namespace)
				data, err := readAllLocal(reader)
				reader.Close()
				if err == nil {
					result[r.ClusterID] = data
					continue
				}
			} else if errors.Is(err, cache.ErrCacheMiss) {
				metrics.IncANNClusterRangeCacheMiss(f.namespace)
			}
		}
		uncached = append(uncached, r)
	}

	if len(uncached) == 0 {
		return result, nil
	}

	// Merge adjacent ranges to reduce number of requests
	merged := vector.MergeAdjacentRanges(uncached, multiRangeMergeGapBytes)

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
	seen    map[uint64]struct{} // Track DocIDs we've already added
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
	return &topKCollector{
		limit: limit,
		seen:  make(map[uint64]struct{}),
	}
}

func (c *topKCollector) consider(res vector.IVFSearchResult) {
	// Skip if we've already seen this DocID (deduplication)
	if _, exists := c.seen[res.DocID]; exists {
		return
	}

	if len(c.heap) < c.limit {
		c.seen[res.DocID] = struct{}{}
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
		// Remove old docID from seen map
		delete(c.seen, c.heap[0].DocID)
		// Add new docID to seen map
		c.seen[res.DocID] = struct{}{}
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

	distCalc := newDistanceCalculator(query, ivfReader.Metric)
	collector := newTopKCollector(topK)
	if err := r.collectANN(ctx, ivfReader, clusterDataKey, distCalc, nProbe, filterBitmap, collector); err != nil {
		return nil, collector, err
	}

	return collector.results(), collector, nil
}

func (r *Reader) collectANN(ctx context.Context, ivfReader *vector.IVFReader, clusterDataKey string, distCalc distanceCalculator, nProbe int, filterBitmap *roaring.Bitmap, collector *topKCollector) error {
	t0 := time.Now()
	// Find nearest centroids
	nearestClusters := findNearestCentroids(ivfReader, distCalc, nProbe)

	// Get byte ranges for all selected clusters
	ranges := ivfReader.GetClusterRanges(nearestClusters)
	centroidMs := time.Since(t0).Milliseconds()

	debug := indexDebugEnabled()

	// Separate small clusters (can batch) from large clusters (must stream)
	var smallRanges []vector.ByteRange
	var largeRanges []vector.ByteRange
	var totalSmallBytes, totalLargeBytes uint64
	for _, rng := range ranges {
		if rng.Length <= MaxClusterSizeForBatch {
			smallRanges = append(smallRanges, rng)
			totalSmallBytes += rng.Length
		} else {
			largeRanges = append(largeRanges, rng)
			totalLargeBytes += rng.Length
		}
	}
	if debug {
		indexDebugf("[index] collectANN setup nProbe=%d small=%d/%dMB large=%d/%dMB dur=%dms",
			nProbe, len(smallRanges), totalSmallBytes/1024/1024, len(largeRanges), totalLargeBytes/1024/1024, centroidMs)
	}

	// Process small clusters in batch
	if len(smallRanges) > 0 {
		t1 := time.Now()
		fetcher := NewMultiRangeClusterDataFetcher(r.store, r.diskCache, clusterDataKey)
		clusterData, err := fetcher.FetchRanges(ctx, smallRanges)
		fetchMs := time.Since(t1).Milliseconds()
		if err != nil {
			return fmt.Errorf("failed to fetch cluster data: %w", err)
		}

		t2 := time.Now()
		for clusterID, data := range clusterData {
			if err := searchClusterDataInto(data, distCalc, clusterID, ivfReader.Dims, ivfReader.DType, filterBitmap, collector); err != nil {
				return fmt.Errorf("failed to search cluster %d: %w", clusterID, err)
			}
		}
		searchMs := time.Since(t2).Milliseconds()
		if debug {
			indexDebugf("[index] collectANN small_clusters fetch_ms=%d search_ms=%d", fetchMs, searchMs)
		}
	}

	// Stream large clusters in parallel to utilize all CPU cores
	if len(largeRanges) > 0 {
		t3 := time.Now()
		var wg sync.WaitGroup
		// Create per-goroutine collectors to avoid contention
		collectors := make([]*topKCollector, len(largeRanges))
		for i := range collectors {
			collectors[i] = newTopKCollector(collector.limit)
		}

		maxParallel := runtime.NumCPU()
		if maxParallel < 1 {
			maxParallel = 1
		}
		sem := make(chan struct{}, maxParallel)

		for i, rng := range largeRanges {
			wg.Add(1)
			sem <- struct{}{}
			go func(idx int, byteRange vector.ByteRange) {
				defer wg.Done()
				defer func() { <-sem }()
				// Each goroutine uses its own collector
				if err := r.searchClusterStreaming(ctx, clusterDataKey, byteRange, distCalc, ivfReader.Dims, ivfReader.DType, filterBitmap, collectors[idx]); err != nil {
					// Skip failed clusters
				}
			}(i, rng)
		}
		wg.Wait()
		parallelMs := time.Since(t3).Milliseconds()

		// Merge results from all collectors
		t4 := time.Now()
		totalResults := 0
		for _, c := range collectors {
			for _, res := range c.results() {
				collector.consider(res)
				totalResults++
			}
		}
		mergeMs := time.Since(t4).Milliseconds()
		if debug {
			indexDebugf("[index] collectANN large_clusters parallel_ms=%d merge_ms=%d results=%d", parallelMs, mergeMs, totalResults)
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
// Uses disk cache to avoid repeated S3 fetches for the same chunks.
func (r *Reader) searchClusterStreaming(ctx context.Context, clusterDataKey string, rng vector.ByteRange, distCalc distanceCalculator, dims int, dtype vector.DType, filterBitmap *roaring.Bitmap, collector *topKCollector) error {
	// Each vector entry is: 8 bytes (docID) + dims*element bytes (vector data)
	entrySize := 8 + dims*vectorBytesPerElement(dtype)
	chunkSize := r.streamingChunkSize(entrySize)

	ns := namespaceFromObjectKey(clusterDataKey)
	offset := rng.Offset
	remaining := rng.Length

	for remaining > 0 {
		fetchLen := uint64(chunkSize)
		if fetchLen > remaining {
			fetchLen = remaining
		}

		// Try disk cache first
		cacheKey := cache.CacheKey{
			ObjectKey: fmt.Sprintf("%s#%d-%d", clusterDataKey, offset, fetchLen),
		}

		var data []byte
		var err error
		cacheHit := false

		if r.diskCache != nil {
			if cacheReader, cacheErr := r.diskCache.GetReader(cacheKey); cacheErr == nil {
				metrics.IncANNClusterRangeCacheHit(ns)
				data, err = readAllLocal(cacheReader)
				cacheReader.Close()
				if err == nil {
					cacheHit = true
				}
			} else if errors.Is(cacheErr, cache.ErrCacheMiss) {
				metrics.IncANNClusterRangeCacheMiss(ns)
			}
		}

		// Fetch from S3 if not in cache
		if !cacheHit {
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
			data, err = readAllWithContext(ctx, limitedReader)
			reader.Close()
			if err != nil {
				return fmt.Errorf("failed to read cluster chunk: %w", err)
			}

			// Store in cache for next time
			if r.diskCache != nil {
				r.diskCache.PutBytes(cacheKey, data)
			}
		}

		// Search this chunk
		if err := searchClusterDataInto(data, distCalc, rng.ClusterID, dims, dtype, filterBitmap, collector); err != nil {
			return err
		}

		offset += fetchLen
		remaining -= fetchLen
	}

	return nil
}

// findNearestCentroids finds the nProbe nearest centroids to the query vector.
func findNearestCentroids(ivfReader *vector.IVFReader, distCalc distanceCalculator, nProbe int) []int {
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
		dist := distCalc.distance(centroid)
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
func searchClusterDataInto(data []byte, distCalc distanceCalculator, clusterID int, dims int, dtype vector.DType, filterBitmap *roaring.Bitmap, collector *topKCollector) error {
	if len(data) == 0 || dims == 0 {
		return nil
	}

	entrySize := 8 + dims*vectorBytesPerElement(dtype)
	if entrySize <= 0 {
		return nil
	}
	numEntries := len(data) / entrySize
	if numEntries == 0 {
		return nil
	}

	var vecBuf []float32
	if dtype == vector.DTypeF16 {
		vecBuf = make([]float32, dims)
	}

	for i := 0; i < numEntries; i++ {
		entryStart := i * entrySize
		entry := data[entryStart : entryStart+entrySize]

		docID := binary.LittleEndian.Uint64(entry[:8])
		if filterBitmap != nil && !filterBitmap.Contains(uint32(docID)) {
			continue
		}

		rawVec := entry[8:]
		var vec []float32
		switch dtype {
		case vector.DTypeF16:
			if err := decodeFloat16Vector(rawVec, vecBuf); err != nil {
				return err
			}
			vec = vecBuf
		default:
			var err error
			vec, err = vectorBytesToFloat32(rawVec, dims)
			if err != nil {
				return err
			}
		}

		dist := distCalc.distance(vec)
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

type distanceCalculator struct {
	metric    vector.DistanceMetric
	query     []float32
	queryNorm float64
}

func newDistanceCalculator(query []float32, metric vector.DistanceMetric) distanceCalculator {
	calculator := distanceCalculator{
		metric: metric,
		query:  query,
	}
	calculator.queryNorm = vectorNorm(query)
	return calculator
}

func (d distanceCalculator) distance(vec []float32) float32 {
	switch d.metric {
	case vector.MetricCosineDistance:
		return d.cosine(vec)
	case vector.MetricEuclideanSquared:
		return euclideanSquared(d.query, vec)
	case vector.MetricDotProduct:
		return -dotProduct(d.query, vec)
	default:
		return d.cosine(vec)
	}
}

func (d distanceCalculator) cosine(vec []float32) float32 {
	if d.queryNorm == 0 {
		return 1.0
	}
	var dot, normB float64
	for i := range d.query {
		a := float64(d.query[i])
		b := float64(vec[i])
		dot += a * b
		normB += b * b
	}
	if normB == 0 {
		return 1.0
	}
	denom := d.queryNorm * math.Sqrt(normB)
	if denom == 0 {
		return 1.0
	}
	return 1.0 - float32(dot/denom)
}

func vectorNorm(a []float32) float64 {
	var sum float64
	for _, v := range a {
		sum += float64(v) * float64(v)
	}
	return math.Sqrt(sum)
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

func vectorBytesPerElement(dtype vector.DType) int {
	if dtype == vector.DTypeF16 {
		return 2
	}
	return 4
}

func vectorBytesToFloat32(data []byte, dims int) ([]float32, error) {
	expected := dims * 4
	if len(data) < expected || dims == 0 {
		return nil, fmt.Errorf("invalid vector data length: got=%d want=%d", len(data), expected)
	}
	// Cluster pack layout is 8-byte aligned, so reinterpreting as float32 is safe and avoids allocations.
	return unsafe.Slice((*float32)(unsafe.Pointer(&data[0])), dims), nil
}

func decodeFloat16Vector(data []byte, out []float32) error {
	expected := len(out) * 2
	if len(data) < expected {
		return fmt.Errorf("invalid float16 vector length: got=%d want=%d", len(data), expected)
	}
	for i := range out {
		bits := binary.LittleEndian.Uint16(data[i*2:])
		out[i] = vector.Float16ToFloat32(bits)
	}
	return nil
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
type IndexedDocument = DocColumn

// LoadSegmentDocs loads documents from all segments in a manifest.
// Documents are returned with their WAL sequence for deduplication.
func (r *Reader) LoadSegmentDocs(ctx context.Context, manifestKey string) ([]IndexedDocument, error) {
	if manifestKey == "" || r.store == nil {
		return nil, nil
	}

	manifestStart := time.Now()
	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		if indexDebugEnabled() {
			indexDebugf("[index] load_segment_docs manifest=%s err=%v dur=%s", manifestKey, err, time.Since(manifestStart))
		}
		return nil, err
	}
	if manifest == nil {
		if indexDebugEnabled() {
			indexDebugf("[index] load_segment_docs manifest=%s empty dur=%s", manifestKey, time.Since(manifestStart))
		}
		return nil, nil
	}
	if indexDebugEnabled() {
		indexDebugf("[index] load_segment_docs manifest=%s segments=%d dur=%s", manifestKey, len(manifest.Segments), time.Since(manifestStart))
	}

	var allDocs []IndexedDocument
	for _, seg := range manifest.Segments {
		segStart := time.Now()
		docs, err := r.loadDocsFromSegment(ctx, seg)
		if err != nil {
			// Log error and continue with other segments
			if indexDebugEnabled() {
				indexDebugf("[index] load_segment_docs segment_key=%s err=%v dur=%s", seg.DocsKey, err, time.Since(segStart))
			}
			continue
		}
		if indexDebugEnabled() {
			indexDebugf("[index] load_segment_docs segment_key=%s docs=%d dur=%s", seg.DocsKey, len(docs), time.Since(segStart))
		}
		allDocs = append(allDocs, docs...)
	}

	return allDocs, nil
}

// LoadIVFSegmentDocs loads documents for the preferred IVF segment in a manifest.
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

	ivfSegment := selectIVFSegment(manifest.Segments)
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

	loadStart := time.Now()
	data, err := r.loadObject(ctx, seg.DocsKey)
	if err != nil {
		if indexDebugEnabled() {
			indexDebugf("[index] load_docs obj=%s err=%v dur=%s", seg.DocsKey, err, time.Since(loadStart))
		}
		return nil, err
	}
	if indexDebugEnabled() {
		indexDebugf("[index] load_docs obj=%s bytes=%d dur=%s", seg.DocsKey, len(data), time.Since(loadStart))
	}
	if len(data) == 0 {
		return nil, nil
	}

	decoded := data
	if IsZstdCompressed(data) {
		decompressStart := time.Now()
		decoded, err = DecompressZstd(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress docs: %w", err)
		}
		if indexDebugEnabled() {
			indexDebugf("[index] load_docs obj=%s decompress_bytes=%d dur=%s", seg.DocsKey, len(decoded), time.Since(decompressStart))
		}
	}

	decodeStart := time.Now()
	docs, err := DecodeDocsColumn(decoded)
	if err == nil {
		if indexDebugEnabled() {
			indexDebugf("[index] load_docs obj=%s docs=%d decode_dur=%s", seg.DocsKey, len(docs), time.Since(decodeStart))
		}
		return docs, nil
	}
	if errors.Is(err, ErrDocsColumnFormat) || errors.Is(err, ErrDocsColumnVersion) {
		var fallbackDocs []IndexedDocument
		if err := json.Unmarshal(decoded, &fallbackDocs); err != nil {
			return nil, fmt.Errorf("failed to parse docs data: %w", err)
		}
		return fallbackDocs, nil
	}

	return nil, err
}

// LoadDocsForIDsInSegment loads documents matching the specified numeric IDs from a single segment.
// This avoids scanning unrelated segments when the caller already knows which segment holds the IDs.
func (r *Reader) LoadDocsForIDsInSegment(ctx context.Context, seg Segment, ids []uint64) ([]IndexedDocument, error) {
	if seg.DocsKey == "" || r.store == nil || len(ids) == 0 {
		return nil, nil
	}

	idSet := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}

	rowMap, err := r.getDocIDRowMap(ctx, seg)
	if err != nil {
		return nil, err
	}
	if rowMap == nil {
		return nil, nil
	}

	rowIDs := make([]uint32, 0, len(idSet))
	for id := range idSet {
		if rowID, ok := rowMap[id]; ok {
			rowIDs = append(rowIDs, rowID)
		}
	}
	if len(rowIDs) == 0 {
		return nil, nil
	}

	docsByRow, err := r.LoadDocsForRowIDs(ctx, seg, rowIDs)
	if err != nil {
		return nil, err
	}
	if len(docsByRow) == 0 {
		return nil, nil
	}

	result := make([]IndexedDocument, 0, len(docsByRow))
	for _, rowID := range rowIDs {
		doc, ok := docsByRow[rowID]
		if !ok {
			continue
		}
		if _, ok := idSet[doc.NumericID]; !ok {
			continue
		}
		result = append(result, doc)
	}

	return result, nil
}

// LoadDocsForIDs loads only the documents matching the specified numeric IDs.
// This is more memory-efficient than LoadSegmentDocs when you only need a subset
// of documents (e.g., top-k results from IVF search).
// Important: This scans segments newest-first to honor tombstones in later segments,
// stopping per ID once the freshest version is found.
func (r *Reader) LoadDocsForIDs(ctx context.Context, manifestKey string, ids []uint64) ([]IndexedDocument, error) {
	if manifestKey == "" || r.store == nil || len(ids) == 0 {
		return nil, nil
	}

	debug := indexDebugEnabled()
	t0 := time.Now()
	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, nil
	}
	if debug {
		indexDebugf("[index] load_docs_for_ids manifest=%s segments=%d dur=%s ids=%v", manifestKey, len(manifest.Segments), time.Since(t0), ids)
	}

	segments := append([]Segment(nil), manifest.Segments...)
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].EndWALSeq != segments[j].EndWALSeq {
			return segments[i].EndWALSeq > segments[j].EndWALSeq
		}
		if !segments[i].CreatedAt.Equal(segments[j].CreatedAt) {
			return segments[i].CreatedAt.After(segments[j].CreatedAt)
		}
		if segments[i].Level != segments[j].Level {
			return segments[i].Level > segments[j].Level
		}
		return segments[i].ID > segments[j].ID
	})

	docsByID := make(map[uint64]IndexedDocument, len(ids))
	pending := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		pending[id] = struct{}{}
	}

	for i, seg := range segments {
		if len(pending) == 0 {
			break
		}

		idsToFetch := make([]uint64, 0, len(pending))
		for id := range pending {
			idsToFetch = append(idsToFetch, id)
		}

		segStart := time.Now()
		docs, err := r.LoadDocsForIDsInSegment(ctx, seg, idsToFetch)
		if debug {
			indexDebugf("[index] load_docs_for_ids segment=%s level=%d idx=%d ids=%d dur=%s err=%v", seg.ID, seg.Level, i, len(idsToFetch), time.Since(segStart), err)
		}
		if err != nil {
			continue
		}

		for _, doc := range docs {
			if _, ok := pending[doc.NumericID]; !ok {
				continue
			}
			docsByID[doc.NumericID] = doc
			delete(pending, doc.NumericID)
		}
	}

	result := make([]IndexedDocument, 0, len(docsByID))
	for _, doc := range docsByID {
		result = append(result, doc)
	}

	return result, nil
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

	buffered := bufio.NewReader(jsonReader)
	peek, err := buffered.Peek(len(docsColumnMagic))
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to peek docs column magic: %w", err)
	}
	if len(peek) == len(docsColumnMagic) && bytes.Equal(peek, docsColumnMagic[:]) {
		return DecodeDocsColumnForIDs(buffered, idSet)
	}

	// Use streaming JSON decoder directly from the reader (no buffering the whole file)
	decoder := json.NewDecoder(buffered)

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

func parseDocOffsets(data []byte) ([]uint64, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("offsets data too short")
	}
	if string(data[:4]) != "DOFF" {
		return nil, fmt.Errorf("invalid offsets magic")
	}
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != 1 {
		return nil, fmt.Errorf("unsupported offsets version %d", version)
	}
	count := binary.LittleEndian.Uint64(data[8:16])
	expected := 16 + int((count+1)*8)
	if len(data) < expected {
		return nil, fmt.Errorf("offsets data truncated")
	}
	offsets := make([]uint64, count+1)
	for i := 0; i < int(count)+1; i++ {
		start := 16 + (i * 8)
		offsets[i] = binary.LittleEndian.Uint64(data[start : start+8])
	}
	return offsets, nil
}

func (r *Reader) loadDocOffsets(ctx context.Context, docsKey string) ([]uint64, error) {
	if r.store == nil {
		return nil, nil
	}
	offsetsKey := DocsOffsetsKey(docsKey)
	if offsetsKey == "" {
		return nil, nil
	}

	r.mu.RLock()
	cached := r.docOffsetsCache[offsetsKey]
	r.mu.RUnlock()
	if cached != nil {
		return cached, nil
	}

	data, err := r.loadObject(ctx, offsetsKey, withRAMCache(cache.TypeDocColumn))
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	offsets, err := parseDocOffsets(data)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	if existing := r.docOffsetsCache[offsetsKey]; existing != nil {
		r.mu.Unlock()
		return existing, nil
	}
	r.docOffsetsCache[offsetsKey] = offsets
	r.mu.Unlock()

	return offsets, nil
}

func (r *Reader) getDocIDRowMap(ctx context.Context, seg Segment) (map[uint64]uint32, error) {
	if seg.DocsKey == "" || r.store == nil {
		return nil, nil
	}

	r.docIDRowMu.RLock()
	cached := r.docIDRowCache[seg.DocsKey]
	r.docIDRowMu.RUnlock()
	if cached != nil {
		return cached, nil
	}

	loaded, err := r.buildDocIDRowMap(ctx, seg)
	if err != nil || loaded == nil {
		return nil, err
	}

	r.docIDRowMu.Lock()
	if existing := r.docIDRowCache[seg.DocsKey]; existing != nil {
		r.docIDRowMu.Unlock()
		return existing, nil
	}
	r.docIDRowCache[seg.DocsKey] = loaded
	r.docIDRowMu.Unlock()
	return loaded, nil
}

func (r *Reader) buildDocIDRowMap(ctx context.Context, seg Segment) (map[uint64]uint32, error) {
	var capacity int
	if offsets, err := r.loadDocOffsets(ctx, seg.DocsKey); err == nil && len(offsets) > 1 {
		capacity = len(offsets) - 1
	}

	start := time.Now()
	if idMapKey := DocsIDMapKey(seg.DocsKey); idMapKey != "" {
		data, err := r.loadObject(ctx, idMapKey, withRAMCache(cache.TypeDocColumn))
		if err == nil && len(data) > 0 {
			ids, err := DecodeDocIDMap(data)
			if err == nil {
				rowMap := buildDocIDRowMapFromIDs(ids)
				if indexDebugEnabled() {
					indexDebugf("[index] load_doc_id_map key=%s rows=%d dur=%s", idMapKey, len(rowMap), time.Since(start))
				}
				return rowMap, nil
			}
			if indexDebugEnabled() {
				indexDebugf("[index] load_doc_id_map key=%s err=%v", idMapKey, err)
			}
		} else if err != nil && !objectstore.IsNotFoundError(err) {
			return nil, err
		}
	}

	var reader io.ReadCloser
	cacheKey := cache.CacheKey{ObjectKey: seg.DocsKey}
	if r.diskCache != nil {
		if cachedReader, err := r.diskCache.GetReader(cacheKey); err == nil {
			reader = cachedReader
		}
	}
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

	magic := make([]byte, 4)
	n, err := io.ReadFull(reader, magic)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to read docs magic: %w", err)
	}
	if n == 0 {
		return nil, nil
	}

	fullReader := io.MultiReader(bytes.NewReader(magic[:n]), reader)

	var docReader io.Reader
	isZstd := n >= 4 && magic[0] == 0x28 && magic[1] == 0xB5 && magic[2] == 0x2F && magic[3] == 0xFD
	if isZstd {
		zstdReader, err := zstd.NewReader(fullReader,
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(32*1024*1024),
			zstd.WithDecoderConcurrency(1),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer zstdReader.Close()
		docReader = zstdReader
	} else {
		docReader = fullReader
	}

	buffered := bufio.NewReader(docReader)
	peek, err := buffered.Peek(len(docsColumnMagic))
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to peek docs column magic: %w", err)
	}

	var rowMap map[uint64]uint32
	if len(peek) == len(docsColumnMagic) && bytes.Equal(peek, docsColumnMagic[:]) {
		rowMap, err = buildDocIDRowMapFromDocsColumn(buffered, capacity)
	} else {
		rowMap, err = buildDocIDRowMapFromJSON(ctx, buffered, capacity)
	}
	if err != nil {
		return nil, err
	}

	if indexDebugEnabled() {
		indexDebugf("[index] build_doc_id_rows docs_key=%s rows=%d dur=%s", seg.DocsKey, len(rowMap), time.Since(start))
	}
	return rowMap, nil
}

func buildDocIDRowMapFromDocsColumn(r io.Reader, capacity int) (map[uint64]uint32, error) {
	docCount, _, err := readDocsColumnHeader(r)
	if err != nil {
		return nil, err
	}
	if docCount == 0 {
		return map[uint64]uint32{}, nil
	}
	if capacity < docCount {
		capacity = docCount
	}
	ids := make([]uint64, docCount)
	if err := binary.Read(r, binary.LittleEndian, ids); err != nil {
		return nil, fmt.Errorf("failed to read numeric IDs: %w", err)
	}
	rowMap := make(map[uint64]uint32, capacity)
	for i, id := range ids {
		rowMap[id] = uint32(i)
	}
	return rowMap, nil
}

func buildDocIDRowMapFromIDs(ids []uint64) map[uint64]uint32 {
	rowMap := make(map[uint64]uint32, len(ids))
	for i, id := range ids {
		rowMap[id] = uint32(i)
	}
	return rowMap
}

func buildDocIDRowMapFromJSON(ctx context.Context, r *bufio.Reader, capacity int) (map[uint64]uint32, error) {
	decoder := json.NewDecoder(r)
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON start: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected JSON array, got %v", token)
	}

	rowMap := make(map[uint64]uint32, capacity)
	var idx uint32
	for decoder.More() {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var partial struct {
			NumericID uint64 `json:"NumericID"`
		}
		if err := decoder.Decode(&partial); err != nil {
			return nil, fmt.Errorf("failed to decode doc ID: %w", err)
		}
		rowMap[partial.NumericID] = idx
		idx++
	}

	return rowMap, nil
}

func isZstdMagic(data []byte) bool {
	return len(data) >= 4 && data[0] == 0x28 && data[1] == 0xB5 && data[2] == 0x2F && data[3] == 0xFD
}

func normalizeDocJSON(data []byte) []byte {
	trimmed := bytes.TrimSpace(data)
	for len(trimmed) > 0 && trimmed[0] == ',' {
		trimmed = bytes.TrimSpace(trimmed[1:])
	}
	for len(trimmed) > 0 {
		last := trimmed[len(trimmed)-1]
		if last != ',' && last != ']' {
			break
		}
		trimmed = bytes.TrimSpace(trimmed[:len(trimmed)-1])
	}
	return trimmed
}

func decodeDocBytes(data []byte) (IndexedDocument, error) {
	normalized := normalizeDocJSON(data)
	var doc IndexedDocument
	if len(normalized) == 0 {
		return doc, fmt.Errorf("empty doc data")
	}
	if err := json.Unmarshal(normalized, &doc); err != nil {
		return doc, err
	}
	return doc, nil
}

func (r *Reader) loadDocsForRowIDsWithOffsets(ctx context.Context, docsKey string, rowIDs []uint32, offsets []uint64) (map[uint32]IndexedDocument, bool, error) {
	if len(offsets) < 2 {
		return nil, false, nil
	}

	var cachedFile *os.File
	var readerAt io.ReaderAt
	cacheKey := cache.CacheKey{ObjectKey: docsKey}
	if r.diskCache != nil {
		if path, err := r.diskCache.Get(cacheKey); err == nil {
			file, err := os.Open(path)
			if err == nil {
				cachedFile = file
				readerAt = file
			}
		}
	}
	if cachedFile != nil {
		defer cachedFile.Close()
	}

	var magic []byte
	var err error
	if readerAt != nil {
		magic = make([]byte, 4)
		n, readErr := readerAt.ReadAt(magic, 0)
		if readErr != nil && readErr != io.EOF {
			return nil, false, readErr
		}
		magic = magic[:n]
	} else {
		magic, err = r.loadObjectRange(ctx, docsKey, 0, 4)
		if err != nil {
			return nil, false, err
		}
	}
	if isZstdMagic(magic) {
		return nil, false, nil
	}

	sortedIDs := append([]uint32(nil), rowIDs...)
	sort.Slice(sortedIDs, func(i, j int) bool { return sortedIDs[i] < sortedIDs[j] })
	result := make(map[uint32]IndexedDocument, len(sortedIDs))

	for _, id := range sortedIDs {
		if int(id)+1 >= len(offsets) {
			continue
		}
		start := offsets[id]
		end := offsets[id+1]
		if end <= start {
			continue
		}
		length := end - start

		var data []byte
		if readerAt != nil {
			section := io.NewSectionReader(readerAt, int64(start), int64(length))
			data, err = readAllWithContext(ctx, section)
		} else {
			data, err = r.loadObjectRange(ctx, docsKey, start, length)
		}
		if err != nil {
			return nil, true, err
		}
		if readerAt == nil && len(data) != int(length) {
			if r.diskCache != nil {
				if _, err := r.loadObject(ctx, docsKey); err == nil {
					if path, err := r.diskCache.Get(cacheKey); err == nil {
						file, err := os.Open(path)
						if err == nil {
							cachedFile = file
							readerAt = file
							defer cachedFile.Close()
							section := io.NewSectionReader(readerAt, int64(start), int64(length))
							data, err = readAllWithContext(ctx, section)
							if err != nil {
								return nil, true, err
							}
						}
					}
				}
			}
			if readerAt == nil && len(data) != int(length) {
				return nil, false, nil
			}
		}
		doc, err := decodeDocBytes(data)
		if err != nil && readerAt != nil {
			if cachedFile != nil {
				_ = cachedFile.Close()
				cachedFile = nil
			}
			readerAt = nil
			if r.diskCache != nil {
				_ = r.diskCache.Delete(cacheKey)
			}
			data, err = r.loadObjectRange(ctx, docsKey, start, length)
			if err != nil {
				return nil, true, err
			}
			doc, err = decodeDocBytes(data)
		}
		if err != nil {
			return nil, true, err
		}
		result[id] = doc
	}

	return result, true, nil
}

// LoadDocsForRowIDs loads documents by their row index within the segment's docs column.
// This uses streaming decode and skips parsing attributes for non-matching rows.
func (r *Reader) LoadDocsForRowIDs(ctx context.Context, seg Segment, rowIDs []uint32) (map[uint32]IndexedDocument, error) {
	if seg.DocsKey == "" || len(rowIDs) == 0 {
		return nil, nil
	}

	idSet := make(map[uint32]struct{}, len(rowIDs))
	var maxID uint32
	for _, id := range rowIDs {
		idSet[id] = struct{}{}
		if id > maxID {
			maxID = id
		}
	}

	if offsets, err := r.loadDocOffsets(ctx, seg.DocsKey); err == nil && len(offsets) > 0 {
		if int(maxID)+1 < len(offsets) {
			if docs, used, err := r.loadDocsForRowIDsWithOffsets(ctx, seg.DocsKey, rowIDs, offsets); err != nil {
				return nil, err
			} else if used {
				return docs, nil
			}
		}
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
	var dataReader io.Reader
	isZstd := n >= 4 && magic[0] == 0x28 && magic[1] == 0xB5 && magic[2] == 0x2F && magic[3] == 0xFD
	if isZstd {
		// Use low memory mode and limit window size to reduce memory usage
		zstdReader, err := zstd.NewReader(fullReader,
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(32*1024*1024),
			zstd.WithDecoderConcurrency(1),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer zstdReader.Close()
		dataReader = zstdReader
	} else {
		dataReader = fullReader
	}

	// Check for column format magic bytes
	buffered := bufio.NewReader(dataReader)
	peek, err := buffered.Peek(len(docsColumnMagic))
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to peek docs column magic: %w", err)
	}
	if len(peek) == len(docsColumnMagic) && bytes.Equal(peek, docsColumnMagic[:]) {
		docCols, err := DecodeDocsColumnForRowIDs(buffered, idSet)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to decode docs column for row IDs: %w", err)
		}
		result := make(map[uint32]IndexedDocument, len(docCols))
		for rowID, col := range docCols {
			result[rowID] = docColumnToIndexedDocument(col)
		}
		return result, nil
	}

	// Fall back to JSON format
	decoder := json.NewDecoder(buffered)
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON start: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("expected JSON array, got %v", token)
	}

	result := make(map[uint32]IndexedDocument, len(idSet))
	var idx uint32
	for decoder.More() {
		if idx > maxID && len(idSet) == 0 {
			break
		}

		if _, ok := idSet[idx]; ok {
			var doc IndexedDocument
			if err := decoder.Decode(&doc); err != nil {
				return nil, fmt.Errorf("failed to decode doc: %w", err)
			}
			result[idx] = doc
			delete(idSet, idx)
		} else {
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				return nil, fmt.Errorf("failed to skip doc: %w", err)
			}
		}
		idx++
		if len(idSet) == 0 && idx > maxID {
			break
		}
	}

	return result, nil
}

// docColumnToIndexedDocument converts a DocColumn to an IndexedDocument.
func docColumnToIndexedDocument(col DocColumn) IndexedDocument {
	return IndexedDocument{
		ID:         col.ID,
		NumericID:  col.NumericID,
		WALSeq:     col.WALSeq,
		Deleted:    col.Deleted,
		Attributes: col.Attributes,
		Vector:     col.Vector,
	}
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
			// Format: <segmentKey>/fts.<attribute>.bm25
			attrName := extractAttrNameFromFTSKey(ftsKey)
			if attrName != "" {
				result[attrName] = append(result[attrName], data)
			}
		}
	}

	return result, nil
}

// FTSSegmentIndex ties a deserialized FTS index to its segment metadata.
type FTSSegmentIndex struct {
	Segment Segment
	Index   *fts.Index
}

// FTSSegmentTerms ties a term list to its segment metadata.
type FTSSegmentTerms struct {
	Segment Segment
	Terms   []string
}

// LoadFTSTermsForField loads term lists for a specific field across segments.
func (r *Reader) LoadFTSTermsForField(ctx context.Context, manifestKey, field string) ([]FTSSegmentTerms, error) {
	if manifestKey == "" || r.store == nil || field == "" {
		return nil, nil
	}

	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, nil
	}

	var results []FTSSegmentTerms
	for _, seg := range manifest.Segments {
		for _, termsKey := range seg.FTSTermKeys {
			attrName := extractAttrNameFromFTSTermsKey(termsKey)
			if attrName != field {
				continue
			}

			terms, err := r.loadFTSTerms(ctx, termsKey)
			if err != nil || len(terms) == 0 {
				continue
			}

			results = append(results, FTSSegmentTerms{
				Segment: seg,
				Terms:   terms,
			})
		}
	}

	return results, nil
}

// LoadFTSIndexesForFieldInSegments loads deserialized FTS indexes for a field across selected segments.
func (r *Reader) LoadFTSIndexesForFieldInSegments(ctx context.Context, manifestKey, field string, segments []Segment) ([]FTSSegmentIndex, error) {
	if manifestKey == "" || r.store == nil || field == "" {
		return nil, nil
	}
	if len(segments) == 0 {
		return nil, nil
	}

	var results []FTSSegmentIndex
	for _, seg := range segments {
		for _, ftsKey := range seg.FTSKeys {
			attrName := extractAttrNameFromFTSKey(ftsKey)
			if attrName != field {
				continue
			}

			idx, err := r.loadFTSIndex(ctx, ftsKey)
			if err != nil || idx == nil {
				continue
			}

			results = append(results, FTSSegmentIndex{
				Segment: seg,
				Index:   idx,
			})
		}
	}

	return results, nil
}

// LoadFTSIndexesForField loads deserialized FTS indexes for a specific field across segments.
func (r *Reader) LoadFTSIndexesForField(ctx context.Context, manifestKey, field string) ([]FTSSegmentIndex, error) {
	if manifestKey == "" || r.store == nil || field == "" {
		return nil, nil
	}

	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, nil
	}

	var results []FTSSegmentIndex
	for _, seg := range manifest.Segments {
		for _, ftsKey := range seg.FTSKeys {
			attrName := extractAttrNameFromFTSKey(ftsKey)
			if attrName != field {
				continue
			}

			idx, err := r.loadFTSIndex(ctx, ftsKey)
			if err != nil || idx == nil {
				continue
			}

			results = append(results, FTSSegmentIndex{
				Segment: seg,
				Index:   idx,
			})
		}
	}

	return results, nil
}

// LoadFTSIndexesForFieldWithLimit loads FTS indexes for a field across a limited number of segments.
// The newest segments by WAL sequence are preferred when maxSegments > 0.
func (r *Reader) LoadFTSIndexesForFieldWithLimit(ctx context.Context, manifestKey, field string, maxSegments int) ([]FTSSegmentIndex, error) {
	if maxSegments <= 0 {
		return r.LoadFTSIndexesForField(ctx, manifestKey, field)
	}
	if manifestKey == "" || r.store == nil || field == "" {
		return nil, nil
	}

	manifest, err := r.LoadManifest(ctx, manifestKey)
	if err != nil {
		return nil, err
	}
	if manifest == nil {
		return nil, nil
	}

	segments := manifest.Segments
	if maxSegments > 0 && len(segments) > maxSegments {
		ordered := make([]Segment, len(segments))
		copy(ordered, segments)
		sort.Slice(ordered, func(i, j int) bool {
			if ordered[i].EndWALSeq == ordered[j].EndWALSeq {
				return ordered[i].ID > ordered[j].ID
			}
			return ordered[i].EndWALSeq > ordered[j].EndWALSeq
		})
		segments = ordered[:maxSegments]
	}

	var results []FTSSegmentIndex
	for _, seg := range segments {
		for _, ftsKey := range seg.FTSKeys {
			attrName := extractAttrNameFromFTSKey(ftsKey)
			if attrName != field {
				continue
			}

			idx, err := r.loadFTSIndex(ctx, ftsKey)
			if err != nil || idx == nil {
				continue
			}

			results = append(results, FTSSegmentIndex{
				Segment: seg,
				Index:   idx,
			})
		}
	}

	return results, nil
}

func (r *Reader) loadFTSIndex(ctx context.Context, key string) (*fts.Index, error) {
	r.mu.RLock()
	cached := r.ftsCache[key]
	r.mu.RUnlock()
	if cached != nil {
		return cached, nil
	}

	data, err := r.loadObject(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	idx, err := fts.Deserialize(data)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	if existing := r.ftsCache[key]; existing != nil {
		r.mu.Unlock()
		return existing, nil
	}
	r.ftsCache[key] = idx
	r.mu.Unlock()

	return idx, nil
}

func (r *Reader) loadFTSTerms(ctx context.Context, key string) ([]string, error) {
	r.mu.RLock()
	cached := r.ftsTerms[key]
	r.mu.RUnlock()
	if cached != nil {
		return cached, nil
	}

	data, err := r.loadObject(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	terms, err := DecodeFTSTerms(data)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	if existing := r.ftsTerms[key]; existing != nil {
		r.mu.Unlock()
		return existing, nil
	}
	r.ftsTerms[key] = terms
	r.mu.Unlock()

	return terms, nil
}

// extractAttrNameFromFTSKey extracts the attribute name from an FTS key.
// Key format: .../fts.<attribute>.bm25
func extractAttrNameFromFTSKey(key string) string {
	lastSlash := strings.LastIndexByte(key, '/')
	if lastSlash == -1 || lastSlash+1 >= len(key) {
		return ""
	}
	rest := key[lastSlash+1:]
	if !strings.HasPrefix(rest, "fts.") || !strings.HasSuffix(rest, ".bm25") {
		return ""
	}
	attrName := strings.TrimSuffix(strings.TrimPrefix(rest, "fts."), ".bm25")
	if attrName == "" {
		return ""
	}
	return attrName
}

// extractAttrNameFromFTSTermsKey extracts the attribute name from an FTS term list key.
// Key format: .../fts.<attribute>.terms
func extractAttrNameFromFTSTermsKey(key string) string {
	lastSlash := strings.LastIndexByte(key, '/')
	if lastSlash == -1 || lastSlash+1 >= len(key) {
		return ""
	}
	rest := key[lastSlash+1:]
	if !strings.HasPrefix(rest, "fts.") || !strings.HasSuffix(rest, ".terms") {
		return ""
	}
	attrName := strings.TrimSuffix(strings.TrimPrefix(rest, "fts."), ".terms")
	if attrName == "" {
		return ""
	}
	return attrName
}

// Clear removes cached readers for a namespace.
func (r *Reader) Clear(namespace string) {
	r.mu.Lock()
	r.clearNamespaceCachesLocked(namespace)
	r.mu.Unlock()

	r.clearDocIDRowCache(namespace)
}

// Close releases all resources.
func (r *Reader) Close() error {
	r.mu.Lock()
	r.readers = make(map[string]*cachedIVFReader)
	r.ftsCache = make(map[string]*fts.Index)
	r.ftsTerms = make(map[string][]string)
	r.docOffsetsCache = make(map[string][]uint64)
	r.lastManifestSeq = make(map[string]uint64)
	r.mu.Unlock()

	r.docIDRowMu.Lock()
	r.docIDRowCache = make(map[string]map[uint64]uint32)
	r.docIDRowMu.Unlock()
	return nil
}

func (r *Reader) clearNamespaceCachesLocked(namespace string) {
	// Remove all readers for this namespace.
	for key := range r.readers {
		if len(key) > len(namespace) && key[:len(namespace)+1] == namespace+"/" {
			delete(r.readers, key)
		}
	}

	// Remove cached FTS indexes for this namespace.
	prefix := "vex/namespaces/" + namespace + "/"
	for key := range r.ftsCache {
		if strings.HasPrefix(key, prefix) {
			delete(r.ftsCache, key)
		}
	}
	for key := range r.ftsTerms {
		if strings.HasPrefix(key, prefix) {
			delete(r.ftsTerms, key)
		}
	}

	for key := range r.docOffsetsCache {
		if strings.HasPrefix(key, prefix) {
			delete(r.docOffsetsCache, key)
		}
	}
}

func (r *Reader) clearDocIDRowCache(namespace string) {
	prefix := "vex/namespaces/" + namespace + "/"
	r.docIDRowMu.Lock()
	for key := range r.docIDRowCache {
		if strings.HasPrefix(key, prefix) {
			delete(r.docIDRowCache, key)
		}
	}
	r.docIDRowMu.Unlock()
}
