// Package tail implements tail materialization for unindexed WAL overlay.
//
// Tail is the unindexed portion of the WAL that query nodes must scan for
// strong consistency. It implements a tiered storage model:
//
//   - Tier 0 (RAM): Recently committed WAL sub-batches in decoded columnar form
//   - Tier 1 (NVMe): Spilled decoded tail blocks (zstd compressed)
//   - Tier 2 (ObjectStore): Source-of-truth WAL objects
//
// Tail blocks support:
//   - Vector scan (exact search)
//   - Filter evaluation via standard filter operators
package tail

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	ErrNamespaceNotFound = errors.New("namespace not found")
	ErrSeqNotFound       = errors.New("WAL sequence not found")
	ErrTailTooLarge      = errors.New("unindexed tail exceeds limit")
)

// Document represents a materialized document in the tail.
type Document struct {
	ID         document.ID
	Attributes map[string]any
	Vector     []float32
	WalSeq     uint64
	SubBatchID int
	Deleted    bool
}

// VectorScanResult contains the result of a vector scan operation.
type VectorScanResult struct {
	Doc      *Document
	Distance float64
}

// Store provides access to tail data for a namespace.
type Store interface {
	// Refresh loads/updates tail data from object storage for the given WAL range.
	// Range is (afterSeq, upToSeq] - afterSeq is exclusive, upToSeq is inclusive.
	Refresh(ctx context.Context, namespace string, afterSeq, upToSeq uint64) error

	// Scan returns all documents in the tail for the given namespace.
	// If f is non-nil, only documents matching the filter are returned.
	// Results are ordered by WAL sequence (newest first for deduplication).
	Scan(ctx context.Context, namespace string, f *filter.Filter) ([]*Document, error)

	// ScanWithByteLimit scans documents within a byte limit for eventual consistency.
	// Uses newest WAL entries first until the byte limit is reached.
	// byteLimitBytes of 0 means no limit (same as Scan).
	ScanWithByteLimit(ctx context.Context, namespace string, f *filter.Filter, byteLimitBytes int64) ([]*Document, error)

	// VectorScan performs an exhaustive vector similarity search over tail documents.
	// Returns the top-k results sorted by distance (ascending).
	VectorScan(ctx context.Context, namespace string, queryVector []float32, topK int, metric DistanceMetric, f *filter.Filter) ([]VectorScanResult, error)

	// VectorScanWithByteLimit performs vector scan within a byte limit for eventual consistency.
	// Uses newest WAL entries first until the byte limit is reached.
	// byteLimitBytes of 0 means no limit (same as VectorScan).
	VectorScanWithByteLimit(ctx context.Context, namespace string, queryVector []float32, topK int, metric DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]VectorScanResult, error)

	// GetDocument returns a specific document by ID from the tail.
	// Returns nil if the document is not in the tail.
	GetDocument(ctx context.Context, namespace string, id document.ID) (*Document, error)

	// TailBytes returns the approximate size of unindexed tail data for the namespace.
	TailBytes(namespace string) int64

	// Clear removes all tail data for a namespace.
	Clear(namespace string)

	// AddWALEntry adds a WAL entry directly to the tail (for write path integration).
	// This puts the entry in the RAM tier.
	AddWALEntry(namespace string, entry *wal.WalEntry)

	// Close releases resources.
	Close() error
}

// DistanceMetric represents the distance metric for vector search.
type DistanceMetric string

const (
	MetricCosineDistance   DistanceMetric = "cosine_distance"
	MetricEuclideanSquared DistanceMetric = "euclidean_squared"
	MetricDotProduct       DistanceMetric = "dot_product"
)

// Config holds configuration for the tail store.
type Config struct {
	// MaxRAMBytes is the maximum RAM to use for the RAM tier per namespace.
	// Default: 256MB
	MaxRAMBytes int64

	// MaxNVMeBytes is the maximum NVMe disk space to use for spilled tail blocks.
	// Default: 2GB
	MaxNVMeBytes int64

	// EventualTailCapBytes is the max tail bytes to search in eventual consistency mode.
	// Default: 128MiB (per spec)
	EventualTailCapBytes int64

	// NVMeCachePath is the path for NVMe tier storage.
	NVMeCachePath string
}

// DefaultConfig returns the default tail store configuration.
func DefaultConfig() Config {
	return Config{
		MaxRAMBytes:          256 * 1024 * 1024,  // 256 MB
		MaxNVMeBytes:         2 * 1024 * 1024 * 1024, // 2 GB
		EventualTailCapBytes: 128 * 1024 * 1024,  // 128 MiB
		NVMeCachePath:        "/tmp/vex-tail",
	}
}

// TailStore implements the Store interface with tiered storage.
type TailStore struct {
	mu sync.RWMutex

	cfg         Config
	objectStore objectstore.Store
	diskCache   *cache.DiskCache
	ramCache    *cache.MemoryCache

	// Per-namespace tail state
	namespaces map[string]*namespaceTail
}

// namespaceTail holds the tail state for a single namespace.
type namespaceTail struct {
	mu sync.RWMutex

	// WAL entries in RAM tier, keyed by seq
	ramEntries map[uint64]*walEntryCache

	// Materialized documents (deduplicated view)
	// Key is document ID string
	documents map[string]*Document

	// Loaded WAL sequence range (afterSeq, upToSeq]
	afterSeq uint64
	upToSeq  uint64

	// Approximate bytes in tail
	tailBytes int64
}

// walEntryCache holds a decoded WAL entry in the RAM tier.
type walEntryCache struct {
	entry      *wal.WalEntry
	documents  []*Document
	sizeBytes  int64
	compressed []byte // Original compressed data for NVMe spill
}

// New creates a new TailStore.
func New(cfg Config, store objectstore.Store, diskCache *cache.DiskCache, ramCache *cache.MemoryCache) *TailStore {
	if cfg.MaxRAMBytes == 0 {
		cfg = DefaultConfig()
	}

	return &TailStore{
		cfg:         cfg,
		objectStore: store,
		diskCache:   diskCache,
		ramCache:    ramCache,
		namespaces:  make(map[string]*namespaceTail),
	}
}

// getOrCreateNamespace gets or creates the namespace tail state.
func (ts *TailStore) getOrCreateNamespace(namespace string) *namespaceTail {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if nt, ok := ts.namespaces[namespace]; ok {
		return nt
	}

	nt := &namespaceTail{
		ramEntries: make(map[uint64]*walEntryCache),
		documents:  make(map[string]*Document),
	}
	ts.namespaces[namespace] = nt
	return nt
}

// getNamespace gets the namespace tail state if it exists.
func (ts *TailStore) getNamespace(namespace string) *namespaceTail {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.namespaces[namespace]
}

func (ts *TailStore) Refresh(ctx context.Context, namespace string, afterSeq, upToSeq uint64) error {
	nt := ts.getOrCreateNamespace(namespace)
	nt.mu.Lock()
	defer nt.mu.Unlock()

	// Determine which sequences we need to load
	startSeq := afterSeq + 1
	if nt.upToSeq > afterSeq {
		startSeq = nt.upToSeq + 1
	}

	if startSeq > upToSeq {
		// Already have everything
		return nil
	}

	decoder, err := wal.NewDecoder()
	if err != nil {
		return err
	}
	defer decoder.Close()

	// Load WAL entries from object storage
	for seq := startSeq; seq <= upToSeq; seq++ {
		key := wal.KeyForSeq(seq)

		// Check if in NVMe cache first
		cacheKey := cache.CacheKey{
			ObjectKey: namespace + "/" + key,
			ETag:      "", // We don't have ETag for WAL entries
		}

		var compressed []byte

		if ts.diskCache != nil {
			if reader, err := ts.diskCache.GetReader(cacheKey); err == nil {
				compressed, err = readAll(reader)
				reader.Close()
				if err != nil {
					compressed = nil
				}
			}
		}

		if compressed == nil {
			// Fetch from object storage
			reader, _, err := ts.objectStore.Get(ctx, namespace+"/"+key, nil)
			if err != nil {
				if objectstore.IsNotFoundError(err) {
					// WAL entry doesn't exist yet
					continue
				}
				return err
			}
			compressed, err = readAll(reader)
			reader.Close()
			if err != nil {
				return err
			}

			// Store in NVMe cache
			if ts.diskCache != nil {
				ts.diskCache.PutBytes(cacheKey, compressed)
			}
		}

		// Decode and materialize
		entry, err := decoder.Decode(compressed)
		if err != nil {
			return err
		}

		docs := materializeEntry(entry)
		entryCache := &walEntryCache{
			entry:      entry,
			documents:  docs,
			sizeBytes:  int64(len(compressed)),
			compressed: compressed,
		}

		nt.ramEntries[seq] = entryCache
		nt.tailBytes += entryCache.sizeBytes

		// Update deduplicated document view
		for _, doc := range docs {
			idStr := doc.ID.String()
			existing, exists := nt.documents[idStr]
			if !exists || doc.WalSeq > existing.WalSeq ||
				(doc.WalSeq == existing.WalSeq && doc.SubBatchID > existing.SubBatchID) {
				nt.documents[idStr] = doc
			}
		}
	}

	nt.afterSeq = afterSeq
	nt.upToSeq = upToSeq

	return nil
}

func (ts *TailStore) Scan(ctx context.Context, namespace string, f *filter.Filter) ([]*Document, error) {
	nt := ts.getNamespace(namespace)
	if nt == nil {
		return nil, nil
	}

	nt.mu.RLock()
	defer nt.mu.RUnlock()

	var results []*Document
	for _, doc := range nt.documents {
		if doc.Deleted {
			continue
		}

		if f != nil {
			filterDoc := buildFilterDoc(doc)
			if !f.Eval(filterDoc) {
				continue
			}
		}

		results = append(results, doc)
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].WalSeq != results[j].WalSeq {
			return results[i].WalSeq > results[j].WalSeq
		}
		return results[i].SubBatchID > results[j].SubBatchID
	})

	return results, nil
}

func (ts *TailStore) VectorScan(ctx context.Context, namespace string, queryVector []float32, topK int, metric DistanceMetric, f *filter.Filter) ([]VectorScanResult, error) {
	nt := ts.getNamespace(namespace)
	if nt == nil {
		return nil, nil
	}

	nt.mu.RLock()
	defer nt.mu.RUnlock()

	var candidates []VectorScanResult

	for _, doc := range nt.documents {
		if doc.Deleted || doc.Vector == nil {
			continue
		}

		if f != nil {
			filterDoc := buildFilterDoc(doc)
			if !f.Eval(filterDoc) {
				continue
			}
		}

		dist := computeDistance(queryVector, doc.Vector, metric)
		candidates = append(candidates, VectorScanResult{
			Doc:      doc,
			Distance: dist,
		})
	}

	// Sort by distance
	sortByDistance(candidates)

	if len(candidates) > topK {
		candidates = candidates[:topK]
	}

	return candidates, nil
}

func (ts *TailStore) GetDocument(ctx context.Context, namespace string, id document.ID) (*Document, error) {
	nt := ts.getNamespace(namespace)
	if nt == nil {
		return nil, nil
	}

	nt.mu.RLock()
	defer nt.mu.RUnlock()

	doc, ok := nt.documents[id.String()]
	if !ok || doc.Deleted {
		return nil, nil
	}

	return doc, nil
}

func (ts *TailStore) TailBytes(namespace string) int64 {
	nt := ts.getNamespace(namespace)
	if nt == nil {
		return 0
	}

	nt.mu.RLock()
	defer nt.mu.RUnlock()

	return nt.tailBytes
}

func (ts *TailStore) Clear(namespace string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	delete(ts.namespaces, namespace)
}

func (ts *TailStore) AddWALEntry(namespace string, entry *wal.WalEntry) {
	nt := ts.getOrCreateNamespace(namespace)
	nt.mu.Lock()
	defer nt.mu.Unlock()

	docs := materializeEntry(entry)

	// Estimate size (we don't have compressed data here)
	estimatedSize := int64(0)
	for _, batch := range entry.SubBatches {
		for _, m := range batch.Mutations {
			estimatedSize += int64(len(m.Vector))
			for _, attr := range m.Attributes {
				estimatedSize += estimateAttributeSize(attr)
			}
		}
	}

	entryCache := &walEntryCache{
		entry:     entry,
		documents: docs,
		sizeBytes: estimatedSize,
	}

	nt.ramEntries[entry.Seq] = entryCache
	nt.tailBytes += entryCache.sizeBytes

	// Update deduplicated document view
	for _, doc := range docs {
		idStr := doc.ID.String()
		existing, exists := nt.documents[idStr]
		if !exists || doc.WalSeq > existing.WalSeq ||
			(doc.WalSeq == existing.WalSeq && doc.SubBatchID > existing.SubBatchID) {
			nt.documents[idStr] = doc
		}
	}

	// Update sequence range
	if entry.Seq > nt.upToSeq {
		nt.upToSeq = entry.Seq
	}
}

func (ts *TailStore) Close() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.namespaces = make(map[string]*namespaceTail)
	return nil
}

// ScanWithByteLimit scans documents within a byte limit for eventual consistency.
// Uses newest WAL entries first until the byte limit is reached.
func (ts *TailStore) ScanWithByteLimit(ctx context.Context, namespace string, f *filter.Filter, byteLimitBytes int64) ([]*Document, error) {
	if byteLimitBytes <= 0 {
		return ts.Scan(ctx, namespace, f)
	}

	nt := ts.getNamespace(namespace)
	if nt == nil {
		return nil, nil
	}

	nt.mu.RLock()
	defer nt.mu.RUnlock()

	// Collect allowed WAL seqs from entries within the byte limit
	allowedSeqs := ts.getSeqsWithinByteLimit(nt, byteLimitBytes)

	var results []*Document
	for _, doc := range nt.documents {
		if doc.Deleted {
			continue
		}

		// Check if document's WAL entry is within byte limit
		if !allowedSeqs[doc.WalSeq] {
			continue
		}

		if f != nil {
			filterDoc := buildFilterDoc(doc)
			if !f.Eval(filterDoc) {
				continue
			}
		}

		results = append(results, doc)
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].WalSeq != results[j].WalSeq {
			return results[i].WalSeq > results[j].WalSeq
		}
		return results[i].SubBatchID > results[j].SubBatchID
	})

	return results, nil
}

// VectorScanWithByteLimit performs vector scan within a byte limit for eventual consistency.
func (ts *TailStore) VectorScanWithByteLimit(ctx context.Context, namespace string, queryVector []float32, topK int, metric DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]VectorScanResult, error) {
	if byteLimitBytes <= 0 {
		return ts.VectorScan(ctx, namespace, queryVector, topK, metric, f)
	}

	nt := ts.getNamespace(namespace)
	if nt == nil {
		return nil, nil
	}

	nt.mu.RLock()
	defer nt.mu.RUnlock()

	// Collect allowed WAL seqs from entries within the byte limit
	allowedSeqs := ts.getSeqsWithinByteLimit(nt, byteLimitBytes)

	var candidates []VectorScanResult

	for _, doc := range nt.documents {
		if doc.Deleted || doc.Vector == nil {
			continue
		}

		// Check if document's WAL entry is within byte limit
		if !allowedSeqs[doc.WalSeq] {
			continue
		}

		if f != nil {
			filterDoc := buildFilterDoc(doc)
			if !f.Eval(filterDoc) {
				continue
			}
		}

		dist := computeDistance(queryVector, doc.Vector, metric)
		candidates = append(candidates, VectorScanResult{
			Doc:      doc,
			Distance: dist,
		})
	}

	// Sort by distance
	sortByDistance(candidates)

	if len(candidates) > topK {
		candidates = candidates[:topK]
	}

	return candidates, nil
}

// buildFilterDoc creates a filter.Document from a tail.Document, including the "id" field.
// This is necessary for filters like ["id", "Gt", last_id] to work with pagination.
func buildFilterDoc(doc *Document) filter.Document {
	filterDoc := make(filter.Document)
	for k, v := range doc.Attributes {
		filterDoc[k] = v
	}
	// Add "id" field for filtering by document ID
	filterDoc["id"] = docIDToFilterValue(doc.ID)
	return filterDoc
}

// docIDToFilterValue converts a document.ID to a value suitable for filter evaluation.
// This is used to enable filtering by "id" field.
func docIDToFilterValue(id document.ID) any {
	switch id.Type() {
	case document.IDTypeU64:
		return id.U64()
	case document.IDTypeUUID:
		return id.UUID().String()
	case document.IDTypeString:
		return id.String()
	default:
		return id.String()
	}
}

// getSeqsWithinByteLimit returns WAL seqs within the byte limit.
// WAL entries are ordered newest-first (by seq descending) and included until limit is reached.
func (ts *TailStore) getSeqsWithinByteLimit(nt *namespaceTail, byteLimitBytes int64) map[uint64]bool {
	// Collect all WAL seqs and sort descending (newest first)
	seqs := make([]uint64, 0, len(nt.ramEntries))
	for seq := range nt.ramEntries {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool {
		return seqs[i] > seqs[j]
	})

	allowedSeqs := make(map[uint64]bool)
	var accumulatedBytes int64

	for _, seq := range seqs {
		entry := nt.ramEntries[seq]
		if accumulatedBytes+entry.sizeBytes > byteLimitBytes {
			break
		}
		accumulatedBytes += entry.sizeBytes
		allowedSeqs[seq] = true
	}

	return allowedSeqs
}
