// Package warmer provides background cache warming for namespaces.
package warmer

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// WarmTask represents a task to warm the cache for a namespace.
type WarmTask struct {
	Namespace  string
	EnqueuedAt time.Time
}

// Warmer handles background cache warming for namespaces.
type Warmer struct {
	mu sync.Mutex

	store        objectstore.Store
	stateManager *namespace.StateManager
	diskCache    *cache.DiskCache
	ramCache     *cache.MemoryCache
	cfg          Config

	tasks   chan WarmTask
	workers int
	wg      sync.WaitGroup
	done    chan struct{}
}

// Config holds configuration for the cache warmer.
type Config struct {
	Workers             int   // Number of concurrent warming workers
	QueueSize           int   // Size of the task queue
	DocsHeaderBytes     int64 // Max bytes to read for docs column header
	HotFilterBitmapKeys int   // Max filter bitmap objects to prefetch per segment
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		Workers:             2,
		QueueSize:           100,
		DocsHeaderBytes:     64 * 1024,
		HotFilterBitmapKeys: 4,
	}
}

// New creates a new cache warmer.
func New(store objectstore.Store, stateManager *namespace.StateManager, diskCache *cache.DiskCache, ramCache *cache.MemoryCache, cfg Config) *Warmer {
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 100
	}
	if cfg.DocsHeaderBytes <= 0 {
		cfg.DocsHeaderBytes = 64 * 1024
	}
	if cfg.HotFilterBitmapKeys <= 0 {
		cfg.HotFilterBitmapKeys = 4
	}

	w := &Warmer{
		store:        store,
		stateManager: stateManager,
		diskCache:    diskCache,
		ramCache:     ramCache,
		cfg:          cfg,
		tasks:        make(chan WarmTask, cfg.QueueSize),
		workers:      cfg.Workers,
		done:         make(chan struct{}),
	}

	// Start worker goroutines
	for i := 0; i < cfg.Workers; i++ {
		w.wg.Add(1)
		go w.worker()
	}

	return w
}

// Enqueue adds a cache warming task to the queue.
// Returns immediately without blocking. Returns true if enqueued, false if queue is full.
func (w *Warmer) Enqueue(namespace string) bool {
	task := WarmTask{
		Namespace:  namespace,
		EnqueuedAt: time.Now(),
	}

	select {
	case w.tasks <- task:
		return true
	default:
		// Queue is full, drop the task
		return false
	}
}

// worker processes warming tasks from the queue.
func (w *Warmer) worker() {
	defer w.wg.Done()

	for {
		select {
		case <-w.done:
			return
		case task := <-w.tasks:
			w.warmNamespace(context.Background(), task.Namespace)
		}
	}
}

// warmNamespace warms the cache for a single namespace.
func (w *Warmer) warmNamespace(ctx context.Context, ns string) {
	if w.store == nil || w.stateManager == nil {
		return
	}

	// Pin the namespace to prevent eviction during warming
	if w.diskCache != nil {
		prefix := "vex/namespaces/" + ns + "/"
		w.diskCache.Pin(prefix)
		defer w.diskCache.Unpin(prefix)
	}

	// Load namespace state to get the current manifest
	loaded, err := w.stateManager.Load(ctx, ns)
	if err != nil {
		return
	}

	state := loaded.State

	// Find the current manifest key
	manifestKey := state.Index.ManifestKey
	if manifestKey == "" && state.Index.ManifestSeq != 0 {
		manifestKey = index.ManifestKey(ns, state.Index.ManifestSeq)
	}
	if manifestKey == "" {
		return
	}

	// Load the manifest to get segment keys
	manifest, err := w.loadManifest(ctx, manifestKey)
	if err != nil {
		return
	}

	// Prefetch objects for each segment
	for _, seg := range manifest.Segments {
		w.prefetchSegment(ctx, seg)
	}
}

// loadManifest loads a manifest from object storage.
func (w *Warmer) loadManifest(ctx context.Context, key string) (*index.Manifest, error) {
	reader, _, err := w.store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var manifest index.Manifest
	if err := manifest.UnmarshalJSON(data); err != nil {
		return nil, err
	}

	return &manifest, nil
}

// prefetchSegment prefetches cacheable objects for a segment.
// According to the spec, we prefetch:
// - centroids (small, RAM-cacheable)
// - cluster offsets (small, cacheable)
// - filter bitmaps
// - doc column headers
func (w *Warmer) prefetchSegment(ctx context.Context, seg index.Segment) {
	// Prefetch IVF keys if present
	if seg.IVFKeys != nil {
		// Centroids - small file, cache in RAM
		if seg.IVFKeys.CentroidsKey != "" {
			w.prefetchToCache(ctx, seg.IVFKeys.CentroidsKey, true, cache.TypeCentroid)
		}
		// Cluster offsets - small file, cacheable
		if seg.IVFKeys.ClusterOffsetsKey != "" {
			w.prefetchToCache(ctx, seg.IVFKeys.ClusterOffsetsKey, true, cache.TypeCentroid)
		}
		// Note: We don't prefetch ClusterDataKey as it's large
	}

	// Prefetch filter bitmap keys
	for _, filterKey := range selectHotKeys(seg.FilterKeys, w.cfg.HotFilterBitmapKeys) {
		w.prefetchToCache(ctx, filterKey, false, cache.TypeFilterBitmap)
	}

	// Prefetch FTS indexes for BM25 queries
	for _, ftsKey := range seg.FTSKeys {
		w.prefetchToCache(ctx, ftsKey, false, cache.TypePostingDict)
	}

	// Prefetch docs key (column headers)
	if seg.DocsKey != "" {
		w.prefetchDocsHeader(ctx, seg.DocsKey)
		if offsetsKey := index.DocsOffsetsKey(seg.DocsKey); offsetsKey != "" {
			w.prefetchToCache(ctx, offsetsKey, false, cache.TypeDocColumn)
		}
		if idMapKey := index.DocsIDMapKey(seg.DocsKey); idMapKey != "" {
			w.prefetchToCache(ctx, idMapKey, false, cache.TypeDocColumn)
		}
	}
}

// prefetchToCache fetches an object and caches it.
func (w *Warmer) prefetchToCache(ctx context.Context, key string, toRAM bool, itemType cache.CacheItemType) {
	if w.store == nil {
		return
	}

	reader, _, err := w.store.Get(ctx, key, nil)
	if err != nil {
		return
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return
	}

	diskCacheKey := cache.CacheKey{
		ObjectKey: key,
	}

	// Cache to disk
	if w.diskCache != nil {
		w.diskCache.PutBytes(diskCacheKey, data)
	}

	// Also cache to RAM if requested (for small hot objects)
	// RAM cache uses a different key structure for shard-aware eviction
	if toRAM && w.ramCache != nil {
		// Extract namespace from key (format: vex/namespaces/<ns>/...)
		ns := extractNamespace(key)
		ramCacheKey := cache.MemoryCacheKey{
			Namespace: ns,
			ShardID:   "warm",
			ItemID:    key,
			ItemType:  itemType,
		}
		w.ramCache.Put(ramCacheKey, data)
	}
}

func (w *Warmer) prefetchDocsHeader(ctx context.Context, key string) {
	if w.store == nil {
		return
	}
	if w.cfg.DocsHeaderBytes <= 0 {
		return
	}

	data, _, err := w.readPrefix(ctx, key, w.cfg.DocsHeaderBytes)
	if err != nil {
		return
	}

	if w.ramCache != nil {
		ns := extractNamespace(key)
		ramCacheKey := cache.MemoryCacheKey{
			Namespace: ns,
			ShardID:   "warm",
			ItemID:    key,
			ItemType:  cache.TypeDocColumn,
		}
		w.ramCache.Put(ramCacheKey, data)
	}
}

func (w *Warmer) readPrefix(ctx context.Context, key string, maxBytes int64) ([]byte, *objectstore.ObjectInfo, error) {
	if maxBytes <= 0 {
		return nil, nil, nil
	}

	opts := &objectstore.GetOptions{
		Range: &objectstore.ByteRange{
			Start: 0,
			End:   maxBytes - 1,
		},
	}
	reader, info, err := w.store.Get(ctx, key, opts)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, err
	}

	return data, info, nil
}

func selectHotKeys(keys []string, limit int) []string {
	if limit <= 0 || len(keys) <= limit {
		return keys
	}
	result := make([]string, limit)
	copy(result, keys[:limit])
	return result
}

// extractNamespace extracts the namespace from an object key.
func extractNamespace(key string) string {
	// Keys have format: vex/namespaces/<namespace>/...
	const prefix = "vex/namespaces/"
	if len(key) <= len(prefix) {
		return ""
	}
	if key[:len(prefix)] != prefix {
		return ""
	}
	rest := key[len(prefix):]
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			return rest[:i]
		}
	}
	return rest
}

// QueueLen returns the current length of the task queue.
func (w *Warmer) QueueLen() int {
	return len(w.tasks)
}

// Close shuts down the warmer and waits for workers to finish.
func (w *Warmer) Close() error {
	close(w.done)
	w.wg.Wait()
	return nil
}
