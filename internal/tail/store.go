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
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/guardrails"
	"github.com/vexsearch/vex/internal/metrics"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNamespaceNotFound = errors.New("namespace not found")
	ErrSeqNotFound       = errors.New("WAL sequence not found")
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

	// MaxEntries is the maximum number of WAL entries to keep per namespace.
	// Default: 200k (0 = no limit).
	MaxEntries int
}

// DefaultConfig returns the default tail store configuration.
func DefaultConfig() Config {
	return Config{
		MaxRAMBytes:          256 * 1024 * 1024,      // 256 MB
		MaxNVMeBytes:         2 * 1024 * 1024 * 1024, // 2 GB
		EventualTailCapBytes: 128 * 1024 * 1024,      // 128 MiB
		NVMeCachePath:        "/tmp/vex-tail",
		MaxEntries:           200000,
	}
}

// TailStore implements the Store interface with tiered storage.
type TailStore struct {
	mu sync.RWMutex

	cfg         Config
	objectStore objectstore.Store
	diskCache   *cache.DiskCache
	ramCache    *cache.MemoryCache
	guardrails  *guardrails.Manager

	// Per-namespace tail state
	namespaces map[string]*namespaceTail
}

// namespaceTail holds the tail state for a single namespace.
type namespaceTail struct {
	mu sync.RWMutex

	// WAL entries tracked for the namespace (RAM or spilled)
	entries map[uint64]*walEntryState

	// Loaded WAL sequence range (afterSeq, upToSeq]
	afterSeq uint64
	upToSeq  uint64

	// Approximate bytes in tail
	tailBytes int64

	// Approximate bytes kept in RAM
	ramBytes int64
}

// walEntryState tracks an entry's RAM/spill state.
type walEntryState struct {
	entry     *wal.WalEntry
	documents []*Document
	sizeBytes int64
	fullKey   string
	inRAM     bool
}

// New creates a new TailStore.
func New(cfg Config, store objectstore.Store, diskCache *cache.DiskCache, ramCache *cache.MemoryCache) *TailStore {
	return NewWithGuardrails(cfg, store, diskCache, ramCache, nil)
}

// NewWithGuardrails creates a new TailStore with optional guardrails enforcement.
func NewWithGuardrails(cfg Config, store objectstore.Store, diskCache *cache.DiskCache, ramCache *cache.MemoryCache, guard *guardrails.Manager) *TailStore {
	if cfg.MaxRAMBytes == 0 {
		cfg = DefaultConfig()
	}

	return &TailStore{
		cfg:         cfg,
		objectStore: store,
		diskCache:   diskCache,
		ramCache:    ramCache,
		guardrails:  guard,
		namespaces:  make(map[string]*namespaceTail),
	}
}

// getOrCreateNamespace gets or creates the namespace tail state.
func (ts *TailStore) getOrCreateNamespace(namespace string) (*namespaceTail, bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if nt, ok := ts.namespaces[namespace]; ok {
		return nt, false
	}

	nt := &namespaceTail{
		entries: make(map[uint64]*walEntryState),
	}
	ts.namespaces[namespace] = nt
	return nt, true
}

// getNamespace gets the namespace tail state if it exists.
func (ts *TailStore) getNamespace(namespace string) *namespaceTail {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.namespaces[namespace]
}

func (ts *TailStore) Refresh(ctx context.Context, namespace string, afterSeq, upToSeq uint64) error {
	nt, created := ts.getOrCreateNamespace(namespace)
	if ts.guardrails != nil {
		_, isNew, err := ts.guardrails.Load(namespace, nil)
		if err != nil {
			return err
		}
		if isNew {
			ts.syncGuardrailsNamespaces()
		}
	}

	// Determine which sequences we need to load
	nt.mu.Lock()
	startSeq := afterSeq + 1
	if nt.upToSeq > afterSeq {
		startSeq = nt.upToSeq + 1
	}
	needsPrune := afterSeq > nt.afterSeq

	if startSeq > upToSeq && !needsPrune {
		// Already have everything
		nt.mu.Unlock()
		return nil
	}
	nt.mu.Unlock()

	if created && ts.guardrails != nil {
		if err := ts.guardrails.AcquireColdFill(ctx); err != nil {
			return err
		}
		defer ts.guardrails.ReleaseColdFill()
	}

	nt.mu.Lock()
	if needsPrune {
		if ts.pruneIndexedEntriesLocked(namespace, nt, afterSeq) {
			if err := ts.updateGuardrailsTailBytesLocked(namespace, nt); err != nil {
				nt.mu.Unlock()
				return err
			}
		}
	}

	startSeq = afterSeq + 1
	if nt.upToSeq > afterSeq {
		startSeq = nt.upToSeq + 1
	}

	if startSeq > upToSeq {
		entryCount := len(nt.entries)
		nt.afterSeq = afterSeq
		nt.mu.Unlock()
		metrics.SetTailEntries(namespace, entryCount)
		return nil
	}
	nt.mu.Unlock()

	decoder, err := wal.NewDecoder()
	if err != nil {
		return err
	}
	defer decoder.Close()

	// Load WAL entries from object storage
	for seq := startSeq; seq <= upToSeq; seq++ {
		key := wal.KeyForSeq(seq)
		fullKey := "vex/namespaces/" + namespace + "/" + key

		// Check if in NVMe cache first
		cacheKey := cache.CacheKey{
			ObjectKey: fullKey,
			ETag:      "", // We don't have ETag for WAL entries
		}

		var compressed []byte

		if ts.diskCache != nil {
			if reader, err := ts.diskCache.GetReader(cacheKey); err == nil {
				compressed, err = readAllWithContext(ctx, reader)
				reader.Close()
				if err != nil {
					compressed = nil
				}
			}
		}

		if compressed == nil {
			// Fetch from object storage
			reader, _, err := ts.objectStore.Get(ctx, fullKey, nil)
			if err != nil {
				if objectstore.IsNotFoundError(err) {
					return fmt.Errorf("%w: %d", ErrSeqNotFound, seq)
				}
				return err
			}
			compressed, err = readAllWithContext(ctx, reader)
			reader.Close()
			if err != nil {
				return err
			}

			// Store in NVMe cache
			if ts.diskCache != nil {
				ts.diskCache.PutBytes(cacheKey, compressed)
			}
		}

		sizeBytes := int64(len(compressed))
		nt.mu.Lock()
		if _, exists := nt.entries[seq]; exists {
			nt.mu.Unlock()
			continue
		}
		keepInRAM := ts.shouldKeepInRAM(nt, sizeBytes)
		nt.mu.Unlock()

		var entry *wal.WalEntry
		var docs []*Document
		if keepInRAM {
			entry, err = decoder.Decode(compressed)
			if err != nil {
				return err
			}
			docs = materializeEntry(entry)
		}
		entryState := &walEntryState{
			entry:     entry,
			documents: docs,
			sizeBytes: sizeBytes,
			fullKey:   fullKey,
			inRAM:     keepInRAM,
		}

		nt.mu.Lock()
		if _, exists := nt.entries[seq]; !exists {
			nt.entries[seq] = entryState
			nt.tailBytes += entryState.sizeBytes
			if keepInRAM {
				nt.ramBytes += entryState.sizeBytes
			}
			if seq > nt.upToSeq {
				nt.upToSeq = seq
			}

			if err := ts.enforceRAMCapLocked(nt); err != nil {
				nt.mu.Unlock()
				return err
			}
			if err := ts.enforceGuardrailsCapLocked(nt); err != nil {
				nt.mu.Unlock()
				return err
			}
			if ts.enforceEntryCapLocked(namespace, nt) {
				if err := ts.updateGuardrailsTailBytesLocked(namespace, nt); err != nil {
					nt.mu.Unlock()
					return err
				}
			} else if err := ts.updateGuardrailsTailBytesLocked(namespace, nt); err != nil {
				nt.mu.Unlock()
				return err
			}
		}
		nt.mu.Unlock()
	}

	nt.mu.Lock()
	entryCount := len(nt.entries)
	nt.afterSeq = afterSeq
	nt.mu.Unlock()

	metrics.SetTailEntries(namespace, entryCount)

	return nil
}

func (ts *TailStore) pruneIndexedEntriesLocked(namespace string, nt *namespaceTail, indexedSeq uint64) bool {
	if indexedSeq == 0 {
		return false
	}

	pruned := false
	for seq, entry := range nt.entries {
		if seq > indexedSeq {
			continue
		}
		delete(nt.entries, seq)
		nt.tailBytes -= entry.sizeBytes
		if entry.inRAM {
			nt.ramBytes -= entry.sizeBytes
		}
		if ts.diskCache != nil {
			cacheKey := cache.CacheKey{
				ObjectKey: entry.fullKey,
				ETag:      "",
			}
			_ = ts.diskCache.Delete(cacheKey)
		}
		pruned = true
	}

	return pruned
}

func (ts *TailStore) Scan(ctx context.Context, namespace string, f *filter.Filter) ([]*Document, error) {
	return ts.scanDocuments(ctx, namespace, f, false, 0)
}

// ScanIncludingDeleted returns all documents in the tail, including deleted ones.
// Deleted docs are returned regardless of filter so they can shadow indexed versions.
func (ts *TailStore) ScanIncludingDeleted(ctx context.Context, namespace string, f *filter.Filter) ([]*Document, error) {
	return ts.scanDocuments(ctx, namespace, f, true, 0)
}

// ScanIncludingDeletedWithByteLimit returns documents within the byte limit, including deleted ones.
// Deleted docs are returned regardless of filter so they can shadow indexed versions.
func (ts *TailStore) ScanIncludingDeletedWithByteLimit(ctx context.Context, namespace string, f *filter.Filter, byteLimitBytes int64) ([]*Document, error) {
	return ts.scanDocuments(ctx, namespace, f, true, byteLimitBytes)
}

func (ts *TailStore) VectorScan(ctx context.Context, namespace string, queryVector []float32, topK int, metric DistanceMetric, f *filter.Filter) ([]VectorScanResult, error) {
	return ts.vectorScan(ctx, namespace, queryVector, topK, metric, f, 0)
}

func (ts *TailStore) scanDocuments(ctx context.Context, namespace string, f *filter.Filter, includeDeleted bool, byteLimitBytes int64) ([]*Document, error) {
	if byteLimitBytes < 0 {
		byteLimitBytes = 0
	}

	entries, err := ts.entriesForScan(namespace, byteLimitBytes)
	if err != nil {
		return nil, err
	}

	results := make([]*Document, 0, len(entries))
	seen := make(map[string]struct{})

	for _, entry := range entries {
		docs := entry.docs
		if docs == nil {
			docs, err = ts.loadEntryDocuments(ctx, entry.fullKey)
			if err != nil {
				return nil, err
			}
		}

		for _, doc := range docs {
			idStr := doc.ID.String()
			if _, ok := seen[idStr]; ok {
				continue
			}
			seen[idStr] = struct{}{}

			if doc.Deleted {
				if includeDeleted {
					results = append(results, doc)
				}
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
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].WalSeq != results[j].WalSeq {
			return results[i].WalSeq > results[j].WalSeq
		}
		return results[i].SubBatchID > results[j].SubBatchID
	})

	return results, nil
}

func (ts *TailStore) vectorScan(ctx context.Context, namespace string, queryVector []float32, topK int, metric DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]VectorScanResult, error) {
	if byteLimitBytes < 0 {
		byteLimitBytes = 0
	}

	entries, err := ts.entriesForScan(namespace, byteLimitBytes)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	var candidates []VectorScanResult

	for _, entry := range entries {
		docs := entry.docs
		if docs == nil {
			docs, err = ts.loadEntryDocuments(ctx, entry.fullKey)
			if err != nil {
				return nil, err
			}
		}

		for _, doc := range docs {
			idStr := doc.ID.String()
			if _, ok := seen[idStr]; ok {
				continue
			}
			seen[idStr] = struct{}{}

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
	}

	sortByDistance(candidates)
	if len(candidates) > topK {
		candidates = candidates[:topK]
	}

	return candidates, nil
}

func (ts *TailStore) GetDocument(ctx context.Context, namespace string, id document.ID) (*Document, error) {
	entries, err := ts.entriesForScan(namespace, 0)
	if err != nil {
		return nil, err
	}

	idStr := id.String()
	for _, entry := range entries {
		docs := entry.docs
		if docs == nil {
			docs, err = ts.loadEntryDocuments(ctx, entry.fullKey)
			if err != nil {
				return nil, err
			}
		}

		for _, doc := range docs {
			if doc.ID.String() != idStr {
				continue
			}
			if doc.Deleted {
				return nil, nil
			}
			return doc, nil
		}
	}

	return nil, nil
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
	if ts.guardrails != nil {
		ts.guardrails.Evict(namespace)
	}
	metrics.SetTailEntries(namespace, 0)
}

func (ts *TailStore) AddWALEntry(namespace string, entry *wal.WalEntry) {
	nt, _ := ts.getOrCreateNamespace(namespace)
	if ts.guardrails != nil {
		if _, isNew, err := ts.guardrails.Load(namespace, nil); err == nil && isNew {
			ts.syncGuardrailsNamespaces()
		}
	}
	nt.mu.Lock()
	defer nt.mu.Unlock()

	sizeBytes, ok := compressedEntrySize(entry)
	if !ok {
		sizeBytes = estimateEntrySize(entry)
	}
	keepInRAM := ts.shouldKeepInRAM(nt, sizeBytes)
	var docs []*Document
	if keepInRAM {
		docs = materializeEntry(entry)
	}

	fullKey := "vex/namespaces/" + namespace + "/" + wal.KeyForSeq(entry.Seq)
	entryState := &walEntryState{
		entry:     entry,
		documents: docs,
		sizeBytes: sizeBytes,
		fullKey:   fullKey,
		inRAM:     keepInRAM,
	}

	if !entryState.inRAM {
		if ts.diskCache != nil && entryState.entry != nil {
			if err := ts.spillEntryLocked(entryState); err != nil {
				return
			}
		} else {
			entryState.entry = nil
			entryState.documents = nil
		}
	}

	nt.entries[entry.Seq] = entryState
	nt.tailBytes += entryState.sizeBytes
	if entryState.inRAM {
		nt.ramBytes += entryState.sizeBytes
	}

	if err := ts.enforceRAMCapLocked(nt); err != nil {
		return
	}
	if err := ts.enforceGuardrailsCapLocked(nt); err != nil {
		return
	}
	if ts.enforceEntryCapLocked(namespace, nt) {
		if err := ts.updateGuardrailsTailBytesLocked(namespace, nt); err != nil {
			return
		}
	} else if err := ts.updateGuardrailsTailBytesLocked(namespace, nt); err != nil {
		return
	}

	// Update sequence range
	if entry.Seq > nt.upToSeq {
		nt.upToSeq = entry.Seq
	}

	metrics.SetTailEntries(namespace, len(nt.entries))
}

func (ts *TailStore) Close() error {
	ts.mu.Lock()
	var namespaces []string
	for ns := range ts.namespaces {
		namespaces = append(namespaces, ns)
	}
	defer ts.mu.Unlock()

	ts.namespaces = make(map[string]*namespaceTail)
	if ts.guardrails != nil {
		ts.guardrails.Clear()
	}
	for _, ns := range namespaces {
		metrics.SetTailEntries(ns, 0)
	}
	return nil
}

// ScanWithByteLimit scans documents within a byte limit for eventual consistency.
// Uses newest WAL entries first until the byte limit is reached.
func (ts *TailStore) ScanWithByteLimit(ctx context.Context, namespace string, f *filter.Filter, byteLimitBytes int64) ([]*Document, error) {
	if byteLimitBytes <= 0 {
		return ts.Scan(ctx, namespace, f)
	}
	return ts.scanDocuments(ctx, namespace, f, false, byteLimitBytes)
}

// VectorScanWithByteLimit performs vector scan within a byte limit for eventual consistency.
func (ts *TailStore) VectorScanWithByteLimit(ctx context.Context, namespace string, queryVector []float32, topK int, metric DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]VectorScanResult, error) {
	if byteLimitBytes <= 0 {
		return ts.VectorScan(ctx, namespace, queryVector, topK, metric, f)
	}
	return ts.vectorScan(ctx, namespace, queryVector, topK, metric, f, byteLimitBytes)
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

type entrySnapshot struct {
	seq      uint64
	size     int64
	docs     []*Document
	fullKey  string
	inMemory bool
}

func (ts *TailStore) entriesForScan(namespace string, byteLimitBytes int64) ([]entrySnapshot, error) {
	nt := ts.getNamespace(namespace)
	if nt == nil {
		return nil, nil
	}

	nt.mu.RLock()
	defer nt.mu.RUnlock()

	seqs := make([]uint64, 0, len(nt.entries))
	for seq := range nt.entries {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool {
		return seqs[i] > seqs[j]
	})

	entries := make([]entrySnapshot, 0, len(seqs))
	var accumulatedBytes int64
	limit := byteLimitBytes > 0

	for _, seq := range seqs {
		entry := nt.entries[seq]
		if entry == nil {
			continue
		}
		if limit && accumulatedBytes+entry.sizeBytes > byteLimitBytes {
			break
		}
		accumulatedBytes += entry.sizeBytes
		snapshot := entrySnapshot{
			seq:      seq,
			size:     entry.sizeBytes,
			docs:     entry.documents,
			fullKey:  entry.fullKey,
			inMemory: entry.inRAM,
		}
		if !entry.inRAM {
			snapshot.docs = nil
		}
		entries = append(entries, snapshot)
	}

	return entries, nil
}

func (ts *TailStore) loadEntryDocuments(ctx context.Context, fullKey string) ([]*Document, error) {
	compressed, err := ts.loadCompressedEntry(ctx, fullKey)
	if err != nil {
		return nil, err
	}

	decoder, err := wal.NewDecoder()
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	decoded, err := decoder.Decode(compressed)
	if err != nil {
		return nil, err
	}
	return materializeEntry(decoded), nil
}

func (ts *TailStore) loadCompressedEntry(ctx context.Context, fullKey string) ([]byte, error) {
	cacheKey := cache.CacheKey{
		ObjectKey: fullKey,
		ETag:      "",
	}

	if ts.diskCache != nil {
		if reader, err := ts.diskCache.GetReader(cacheKey); err == nil {
			compressed, err := readAllWithContext(ctx, reader)
			reader.Close()
			if err == nil {
				return compressed, nil
			}
		}
	}

	reader, _, err := ts.objectStore.Get(ctx, fullKey, nil)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return nil, fmt.Errorf("%w: %s", ErrSeqNotFound, fullKey)
		}
		return nil, err
	}
	compressed, err := readAllWithContext(ctx, reader)
	reader.Close()
	if err != nil {
		return nil, err
	}

	if ts.diskCache != nil {
		ts.diskCache.PutBytes(cacheKey, compressed)
	}

	return compressed, nil
}

func (ts *TailStore) enforceRAMCapLocked(nt *namespaceTail) error {
	if ts.cfg.MaxRAMBytes <= 0 {
		return nil
	}

	for nt.ramBytes > ts.cfg.MaxRAMBytes {
		var oldestSeq uint64
		found := false
		for seq, entry := range nt.entries {
			if entry == nil || !entry.inRAM {
				continue
			}
			if !found || seq < oldestSeq {
				oldestSeq = seq
				found = true
			}
		}

		if !found {
			break
		}

		entry := nt.entries[oldestSeq]
		if entry == nil || !entry.inRAM {
			break
		}

		if err := ts.spillEntryLocked(entry); err != nil {
			return err
		}
		nt.ramBytes -= entry.sizeBytes
	}

	return nil
}

func (ts *TailStore) enforceGuardrailsCapLocked(nt *namespaceTail) error {
	if ts.guardrails == nil {
		return nil
	}

	capBytes := ts.guardrails.GetConfig().MaxTailBytesPerNamespace
	if capBytes <= 0 {
		return nil
	}

	for nt.ramBytes > capBytes {
		var oldestSeq uint64
		found := false
		for seq, entry := range nt.entries {
			if entry == nil || !entry.inRAM {
				continue
			}
			if !found || seq < oldestSeq {
				oldestSeq = seq
				found = true
			}
		}

		if !found {
			break
		}

		entry := nt.entries[oldestSeq]
		if entry == nil || !entry.inRAM {
			break
		}

		if err := ts.spillEntryLocked(entry); err != nil {
			return err
		}
		nt.ramBytes -= entry.sizeBytes
	}

	return nil
}

func (ts *TailStore) enforceEntryCapLocked(namespace string, nt *namespaceTail) bool {
	if ts.cfg.MaxEntries <= 0 {
		return false
	}

	over := len(nt.entries) - ts.cfg.MaxEntries
	if over <= 0 {
		return false
	}

	seqs := make([]uint64, 0, len(nt.entries))
	for seq := range nt.entries {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool {
		return seqs[i] < seqs[j]
	})

	for i := 0; i < over && i < len(seqs); i++ {
		seq := seqs[i]
		entry := nt.entries[seq]
		delete(nt.entries, seq)
		if entry != nil {
			nt.tailBytes -= entry.sizeBytes
			if entry.inRAM {
				nt.ramBytes -= entry.sizeBytes
			}
			if ts.diskCache != nil {
				cacheKey := cache.CacheKey{
					ObjectKey: entry.fullKey,
					ETag:      "",
				}
				_ = ts.diskCache.Delete(cacheKey)
			}
		}
	}

	log.Printf("[tail] namespace=%s entries=%d cap=%d dropped=%d", namespace, len(nt.entries), ts.cfg.MaxEntries, over)
	return true
}

func (ts *TailStore) shouldKeepInRAM(nt *namespaceTail, sizeBytes int64) bool {
	if ts.guardrails == nil {
		return true
	}

	capBytes := ts.guardrails.GetConfig().MaxTailBytesPerNamespace
	if capBytes <= 0 {
		return true
	}

	return nt.ramBytes+sizeBytes <= capBytes
}

func (ts *TailStore) updateGuardrailsTailBytesLocked(namespace string, nt *namespaceTail) error {
	if ts.guardrails == nil {
		return nil
	}

	if err := ts.guardrails.SetTailBytes(namespace, nt.ramBytes); err != nil {
		if errors.Is(err, guardrails.ErrTailBytesExceeded) {
			return nil
		}
		return err
	}
	return nil
}

func (ts *TailStore) spillEntryLocked(entry *walEntryState) error {
	if entry == nil || !entry.inRAM {
		return nil
	}

	if ts.diskCache != nil && entry.entry != nil {
		encoder, err := wal.NewEncoder()
		if err != nil {
			return err
		}
		result, err := encoder.Encode(entry.entry)
		encoder.Close()
		if err != nil {
			return err
		}
		cacheKey := cache.CacheKey{
			ObjectKey: entry.fullKey,
			ETag:      "",
		}
		if _, err := ts.diskCache.PutBytes(cacheKey, result.Data); err != nil {
			return err
		}
	}

	entry.entry = nil
	entry.documents = nil
	entry.inRAM = false
	return nil
}

func compressedEntrySize(entry *wal.WalEntry) (int64, bool) {
	encoder, err := wal.NewEncoder()
	if err != nil {
		return 0, false
	}
	defer encoder.Close()

	entryCopy := proto.Clone(entry).(*wal.WalEntry)
	result, err := encoder.Encode(entryCopy)
	if err != nil {
		return 0, false
	}
	return int64(len(result.Data)), true
}

func estimateEntrySize(entry *wal.WalEntry) int64 {
	estimatedSize := int64(0)
	for _, batch := range entry.SubBatches {
		for _, m := range batch.Mutations {
			estimatedSize += int64(len(m.Vector))
			for _, attr := range m.Attributes {
				estimatedSize += estimateAttributeSize(attr)
			}
		}
	}
	return estimatedSize
}

func (ts *TailStore) syncGuardrailsNamespaces() {
	if ts.guardrails == nil {
		return
	}

	allowed := make(map[string]struct{})
	for _, ns := range ts.guardrails.Namespaces() {
		allowed[ns] = struct{}{}
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	for ns := range ts.namespaces {
		if _, ok := allowed[ns]; !ok {
			delete(ts.namespaces, ns)
			metrics.SetTailEntries(ns, 0)
		}
	}
}
