// Package indexer implements the indexer process that watches WAL and builds indexes.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	ErrIndexerClosed   = errors.New("indexer is closed")
	ErrNamespaceClosed = errors.New("namespace indexer is closed")
)

// IndexerConfig holds configuration for the indexer.
type IndexerConfig struct {
	// PollInterval is how often to check for new WAL entries.
	PollInterval time.Duration
	// NamespacePollInterval is how often to scan for new namespaces.
	NamespacePollInterval time.Duration

	// Format version configuration for upgrade strategy support.
	// These allow the indexer to be configured to write old format versions
	// during a rolling upgrade for N-1 compatibility.

	// WriteWALVersion specifies which WAL format version to write.
	// 0 means use the current version.
	WriteWALVersion int
	// WriteManifestVersion specifies which manifest format version to write.
	// 0 means use the current version.
	WriteManifestVersion int
}

// DefaultConfig returns default indexer configuration.
func DefaultConfig() *IndexerConfig {
	return &IndexerConfig{
		PollInterval:          time.Second,
		NamespacePollInterval: 10 * time.Second,
		WriteWALVersion:       wal.FormatVersion,
		WriteManifestVersion:  0, // 0 means use current
	}
}

// GetWriteWALVersion returns the WAL format version to write.
// If not set, returns the current version.
func (c *IndexerConfig) GetWriteWALVersion() int {
	if c.WriteWALVersion == 0 {
		return wal.FormatVersion
	}
	return c.WriteWALVersion
}

// GetWriteManifestVersion returns the manifest format version to write.
// If not set, returns the current version.
func (c *IndexerConfig) GetWriteManifestVersion() int {
	if c.WriteManifestVersion == 0 {
		return index.CurrentManifestVersion
	}
	return c.WriteManifestVersion
}

// ValidateVersionConfig validates that the configured format versions are writable.
func (c *IndexerConfig) ValidateVersionConfig() error {
	walVersion := c.GetWriteWALVersion()
	if err := wal.CheckWALFormatVersion(walVersion); err != nil {
		return fmt.Errorf("invalid WriteWALVersion %d: %w", walVersion, err)
	}

	manifestVersion := c.GetWriteManifestVersion()
	if err := index.CheckManifestFormatVersion(manifestVersion); err != nil {
		return fmt.Errorf("invalid WriteManifestVersion %d: %w", manifestVersion, err)
	}
	return nil
}

// WALProcessor is called when WAL entries need to be processed.
// It receives the namespace, the WAL range to process (exclusive start, inclusive end),
// and the current state. Returns the number of bytes indexed.
type WALProcessor func(ctx context.Context, ns string, startSeq, endSeq uint64, state *namespace.State) (int64, error)

// Indexer watches namespace states and processes WAL ranges.
type Indexer struct {
	store        objectstore.Store
	stateManager *namespace.StateManager
	config       *IndexerConfig
	processor    WALProcessor

	mu       sync.Mutex
	watchers map[string]*namespaceWatcher
	closed   bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a new indexer.
func New(store objectstore.Store, stateManager *namespace.StateManager, config *IndexerConfig, processor WALProcessor) *Indexer {
	if config == nil {
		config = DefaultConfig()
	}
	ctx, cancel := context.WithCancel(context.Background())
	idx := &Indexer{
		store:        store,
		stateManager: stateManager,
		config:       config,
		processor:    processor,
		watchers:     make(map[string]*namespaceWatcher),
		ctx:          ctx,
		cancel:       cancel,
	}
	if processor == nil {
		idx.processor = idx.defaultProcessor
	}
	return idx
}

// defaultProcessor reads WAL entries in the range and returns total bytes read.
// This is a basic processor that reads WAL but doesn't build indexes yet.
// Full index building (IVF segments, manifests) will be implemented in later tasks.
func (i *Indexer) defaultProcessor(ctx context.Context, ns string, startSeq, endSeq uint64, state *namespace.State) (int64, error) {
	_, totalBytes, err := i.ProcessWALRange(ctx, ns, startSeq, endSeq)
	if err != nil {
		return 0, err
	}
	return totalBytes, nil
}

// Start begins the indexer's background processing.
func (i *Indexer) Start() {
	i.wg.Add(1)
	go i.discoveryLoop()
}

// Stop gracefully shuts down the indexer.
func (i *Indexer) Stop() error {
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return ErrIndexerClosed
	}
	i.closed = true
	i.cancel()

	// Stop all namespace watchers
	for _, w := range i.watchers {
		w.stop()
	}
	i.mu.Unlock()

	i.wg.Wait()
	return nil
}

// WatchNamespace starts watching a specific namespace for WAL changes.
func (i *Indexer) WatchNamespace(ns string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed {
		return ErrIndexerClosed
	}

	if _, exists := i.watchers[ns]; exists {
		return nil // Already watching
	}

	w := newNamespaceWatcher(i, ns)
	i.watchers[ns] = w
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		w.run()
	}()

	return nil
}

// UnwatchNamespace stops watching a namespace.
func (i *Indexer) UnwatchNamespace(ns string) {
	i.mu.Lock()
	w, exists := i.watchers[ns]
	if exists {
		delete(i.watchers, ns)
	}
	i.mu.Unlock()

	if w != nil {
		w.stop()
	}
}

// IsWatching returns true if the namespace is being watched.
func (i *Indexer) IsWatching(ns string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	_, exists := i.watchers[ns]
	return exists
}

// NamespaceCount returns the number of namespaces being watched.
func (i *Indexer) NamespaceCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.watchers)
}

// discoveryLoop periodically scans object storage for new namespaces.
func (i *Indexer) discoveryLoop() {
	defer i.wg.Done()

	ticker := time.NewTicker(i.config.NamespacePollInterval)
	defer ticker.Stop()

	// Initial discovery
	i.discoverNamespaces()

	for {
		select {
		case <-i.ctx.Done():
			return
		case <-ticker.C:
			i.discoverNamespaces()
		}
	}
}

// discoverNamespaces scans object storage for namespaces and starts watchers.
func (i *Indexer) discoverNamespaces() {
	ctx, cancel := context.WithTimeout(i.ctx, 30*time.Second)
	defer cancel()

	// List namespace state files - look for meta/state.json files
	prefix := "vex/namespaces/"
	var marker string
	seen := make(map[string]bool)

	for {
		result, err := i.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			// Log error but continue
			return
		}

		// Extract namespace names from object keys
		for _, obj := range result.Objects {
			// Key is like "vex/namespaces/my-ns/meta/state.json"
			ns := extractNamespaceFromKey(obj.Key)
			if ns != "" && !seen[ns] {
				seen[ns] = true
				i.WatchNamespace(ns)
			}
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}
}

// extractNamespaceFromKey extracts namespace name from a key like "vex/namespaces/my-ns/meta/state.json"
func extractNamespaceFromKey(key string) string {
	const base = "vex/namespaces/"
	if !strings.HasPrefix(key, base) {
		return ""
	}
	rest := strings.TrimPrefix(key, base)
	// rest is like "my-ns/meta/state.json" or "my-ns/wal/1.wal.zst"
	if idx := strings.Index(rest, "/"); idx > 0 {
		ns := rest[:idx]
		// Only return namespaces that have a state.json file
		if strings.HasSuffix(key, "/meta/state.json") {
			return ns
		}
	}
	return ""
}

// namespaceWatcher watches a single namespace for WAL changes.
type namespaceWatcher struct {
	indexer   *Indexer
	namespace string

	ctx    context.Context
	cancel context.CancelFunc
}

func newNamespaceWatcher(indexer *Indexer, ns string) *namespaceWatcher {
	ctx, cancel := context.WithCancel(indexer.ctx)
	return &namespaceWatcher{
		indexer:   indexer,
		namespace: ns,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (w *namespaceWatcher) stop() {
	w.cancel()
}

func (w *namespaceWatcher) run() {
	ticker := time.NewTicker(w.indexer.config.PollInterval)
	defer ticker.Stop()

	// Initial check
	w.checkAndProcess()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.checkAndProcess()
		}
	}
}

func (w *namespaceWatcher) checkAndProcess() {
	ctx, cancel := context.WithTimeout(w.ctx, 60*time.Second)
	defer cancel()

	// Load namespace state
	loaded, err := w.indexer.stateManager.Load(ctx, w.namespace)
	if err != nil {
		if errors.Is(err, namespace.ErrStateNotFound) || errors.Is(err, namespace.ErrNamespaceTombstoned) {
			// Namespace deleted or doesn't exist, stop watching
			w.indexer.UnwatchNamespace(w.namespace)
			return
		}
		// Other error, will retry next tick
		return
	}

	state := loaded.State

	// Check if there's work to do: WAL range (indexed_wal_seq+1 .. head_seq]
	startSeq := state.Index.IndexedWALSeq
	endSeq := state.WAL.HeadSeq

	if startSeq >= endSeq {
		// Nothing to index
		return
	}

	// Process WAL range
	bytesIndexed, err := w.indexer.processor(ctx, w.namespace, startSeq, endSeq, state)
	if err != nil {
		// Error processing, will retry next tick
		return
	}

	// Update state to advance indexed_wal_seq.
	// Always advance when processing succeeds, even if bytesIndexed is 0
	// (e.g., empty WAL entries or already-processed data).
	// Full index publishing is handled by later tasks.
	updatedState, err := w.indexer.stateManager.AdvanceIndex(
		ctx,
		w.namespace,
		loaded.ETag,
		state.Index.ManifestKey, // Keep current manifest
		state.Index.ManifestSeq, // Keep current manifest seq
		endSeq,                  // Advance indexed_wal_seq
		bytesIndexed,
	)
	if err != nil {
		return
	}

	// Mark any pending rebuilds as ready now that we've caught up.
	// A rebuild is considered ready when we've indexed up to the WAL head.
	w.markPendingRebuildsReady(ctx, updatedState)
}

// markPendingRebuildsReady marks all not-ready pending rebuilds as ready.
// This is called after the indexer has processed WAL entries, indicating
// the index is now caught up with the current schema configuration.
func (w *namespaceWatcher) markPendingRebuildsReady(ctx context.Context, loaded *namespace.LoadedState) {
	if loaded == nil || loaded.State == nil {
		return
	}

	pendingRebuilds := loaded.State.Index.PendingRebuilds
	if len(pendingRebuilds) == 0 {
		return
	}

	currentETag := loaded.ETag
	version := int(loaded.State.Index.ManifestSeq)

	for _, pr := range pendingRebuilds {
		if pr.Ready {
			continue // Already ready
		}

		// Mark this rebuild as ready
		updated, err := w.indexer.stateManager.MarkRebuildReady(
			ctx,
			w.namespace,
			currentETag,
			pr.Kind,
			pr.Attribute,
			version,
		)
		if err != nil {
			// Error marking ready - will retry on next poll
			break
		}
		currentETag = updated.ETag
	}
}

// ProcessWALRange reads WAL entries in the range (startSeq, endSeq] and returns total bytes.
// This is a helper method that can be used by index builders.
func (i *Indexer) ProcessWALRange(ctx context.Context, ns string, startSeq, endSeq uint64) ([]*wal.WalEntry, int64, error) {
	var entries []*wal.WalEntry
	var totalBytes int64

	decoder, err := wal.NewDecoder()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create decoder: %w", err)
	}
	defer decoder.Close()

	for seq := startSeq + 1; seq <= endSeq; seq++ {
		entry, bytes, err := i.readWALEntry(ctx, ns, seq, decoder)
		if err != nil {
			if objectstore.IsNotFoundError(err) {
				// WAL entry doesn't exist yet - possible race condition
				// Return what we have so far
				break
			}
			return nil, 0, fmt.Errorf("failed to read WAL entry %d: %w", seq, err)
		}
		entries = append(entries, entry)
		totalBytes += bytes
	}

	return entries, totalBytes, nil
}

// readWALEntry reads and decodes a single WAL entry.
func (i *Indexer) readWALEntry(ctx context.Context, ns string, seq uint64, decoder *wal.Decoder) (*wal.WalEntry, int64, error) {
	key := fmt.Sprintf("vex/namespaces/%s/%s", ns, wal.KeyForSeq(seq))

	reader, info, err := i.store.Get(ctx, key, nil)
	if err != nil {
		return nil, 0, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read WAL data: %w", err)
	}

	entry, err := decoder.Decode(data)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode WAL entry: %w", err)
	}

	return entry, info.Size, nil
}

// GetWALRange returns the range of WAL entries that need to be indexed.
// Returns (startSeq, endSeq) where the range is (startSeq, endSeq].
func (i *Indexer) GetWALRange(ctx context.Context, ns string) (uint64, uint64, error) {
	loaded, err := i.stateManager.Load(ctx, ns)
	if err != nil {
		return 0, 0, err
	}

	return loaded.State.Index.IndexedWALSeq, loaded.State.WAL.HeadSeq, nil
}

// HasUnindexedWAL returns true if the namespace has unindexed WAL entries.
func (i *Indexer) HasUnindexedWAL(ctx context.Context, ns string) (bool, error) {
	start, end, err := i.GetWALRange(ctx, ns)
	if err != nil {
		return false, err
	}
	return start < end, nil
}
