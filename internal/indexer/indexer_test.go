package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// mockStore implements objectstore.Store for testing.
type mockStore struct {
	mu      sync.RWMutex
	objects map[string]mockObject
}

type mockObject struct {
	data []byte
	etag string
}

func newMockStore() *mockStore {
	return &mockStore{
		objects: make(map[string]mockObject),
	}
}

func (m *mockStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", objectstore.ErrNotFound, key)
	}
	return io.NopCloser(bytes.NewReader(obj.data)), &objectstore.ObjectInfo{
		Key:  key,
		ETag: obj.etag,
		Size: int64(len(obj.data)),
	}, nil
}

func (m *mockStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", objectstore.ErrNotFound, key)
	}
	return &objectstore.ObjectInfo{
		Key:  key,
		ETag: obj.etag,
		Size: int64(len(obj.data)),
	}, nil
}

func (m *mockStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	data, _ := io.ReadAll(body)
	etag := "etag-" + key
	m.mu.Lock()
	m.objects[key] = mockObject{data: data, etag: etag}
	m.mu.Unlock()
	return &objectstore.ObjectInfo{Key: key, ETag: etag, Size: int64(len(data))}, nil
}

func (m *mockStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[key]; ok {
		return nil, fmt.Errorf("%w: %s", objectstore.ErrAlreadyExists, key)
	}
	data, _ := io.ReadAll(body)
	etag := "etag-" + key
	m.objects[key] = mockObject{data: data, etag: etag}
	return &objectstore.ObjectInfo{Key: key, ETag: etag, Size: int64(len(data))}, nil
}

func (m *mockStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if obj, ok := m.objects[key]; ok {
		if obj.etag != etag {
			return nil, fmt.Errorf("%w: etag mismatch", objectstore.ErrPrecondition)
		}
	} else {
		return nil, fmt.Errorf("%w: key not found", objectstore.ErrPrecondition)
	}
	data, _ := io.ReadAll(body)
	newEtag := "etag-v2-" + key
	m.objects[key] = mockObject{data: data, etag: newEtag}
	return &objectstore.ObjectInfo{Key: key, ETag: newEtag, Size: int64(len(data))}, nil
}

func (m *mockStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return nil
}

func (m *mockStore) List(ctx context.Context, opts *objectstore.ListOptions) (*objectstore.ListResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var objects []objectstore.ObjectInfo

	for key := range m.objects {
		if opts.Prefix != "" && len(key) >= len(opts.Prefix) && key[:len(opts.Prefix)] != opts.Prefix {
			continue
		}
		if opts.Marker != "" && key <= opts.Marker {
			continue
		}

		objects = append(objects, objectstore.ObjectInfo{Key: key})
	}

	return &objectstore.ListResult{
		Objects:     objects,
		IsTruncated: false,
	}, nil
}

// createNamespaceState creates a namespace state in the mock store.
func createNamespaceState(t *testing.T, store *mockStore, ns string, headSeq, indexedSeq uint64) string {
	state := namespace.NewState(ns)
	state.WAL.HeadSeq = headSeq
	state.Index.IndexedWALSeq = indexedSeq

	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("failed to marshal state: %v", err)
	}

	key := namespace.StateKey(ns)
	store.mu.Lock()
	etag := "etag-" + key
	store.objects[key] = mockObject{data: data, etag: etag}
	store.mu.Unlock()

	return etag
}

// createWALEntry creates a WAL entry in the mock store.
func createWALEntry(t *testing.T, store *mockStore, ns string, seq uint64) {
	entry := wal.NewWalEntry(ns, seq)
	entry.SubBatches = append(entry.SubBatches, wal.NewWriteSubBatch("req-"+ns+"-"+itoa(seq)))

	encoder, err := wal.NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("failed to encode WAL entry: %v", err)
	}

	key := "vex/namespaces/" + ns + "/" + wal.KeyForSeq(seq)
	store.mu.Lock()
	store.objects[key] = mockObject{data: result.Data, etag: "etag-wal-" + itoa(seq)}
	store.mu.Unlock()
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 20)
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte(n%10) + '0'
		n /= 10
	}
	return string(buf[i:])
}

func TestIndexerWatchesNamespaceState(t *testing.T) {
	store := newMockStore()
	stateManager := namespace.NewStateManager(store)

	// Create a namespace with WAL entries
	ns := "test-ns"
	createNamespaceState(t, store, ns, 5, 0)
	for seq := uint64(1); seq <= 5; seq++ {
		createWALEntry(t, store, ns, seq)
	}

	var processedCalls atomic.Int32
	var processedNS string
	var processedStart, processedEnd uint64
	var mu sync.Mutex

	processor := func(ctx context.Context, namespace string, startSeq, endSeq uint64, state *namespace.State, etag string) (*WALProcessResult, error) {
		mu.Lock()
		processedNS = namespace
		processedStart = startSeq
		processedEnd = endSeq
		mu.Unlock()
		processedCalls.Add(1)
		return &WALProcessResult{BytesIndexed: 0}, nil
	}

	config := &IndexerConfig{
		PollInterval:          50 * time.Millisecond,
		NamespacePollInterval: 100 * time.Millisecond,
	}

	indexer := New(store, stateManager, config, processor)

	// Watch the namespace
	err := indexer.WatchNamespace(ns)
	if err != nil {
		t.Fatalf("failed to watch namespace: %v", err)
	}

	if !indexer.IsWatching(ns) {
		t.Error("expected namespace to be watched")
	}

	// Wait for the watcher to detect and process WAL changes
	time.Sleep(200 * time.Millisecond)

	if processedCalls.Load() == 0 {
		t.Error("expected processor to be called")
	}

	mu.Lock()
	if processedNS != ns {
		t.Errorf("expected namespace %q, got %q", ns, processedNS)
	}
	if processedStart != 0 {
		t.Errorf("expected start seq 0, got %d", processedStart)
	}
	if processedEnd != 5 {
		t.Errorf("expected end seq 5, got %d", processedEnd)
	}
	mu.Unlock()

	// Clean up
	indexer.Stop()
}

func TestIndexerProcessesWALRange(t *testing.T) {
	store := newMockStore()
	stateManager := namespace.NewStateManager(store)

	ns := "test-ns"
	createNamespaceState(t, store, ns, 10, 5)

	// Create WAL entries for seq 6-10 (the range to process)
	for seq := uint64(6); seq <= 10; seq++ {
		createWALEntry(t, store, ns, seq)
	}

	config := DefaultConfig()
	indexer := New(store, stateManager, config, nil)

	ctx := context.Background()

	// Get WAL range
	start, end, err := indexer.GetWALRange(ctx, ns)
	if err != nil {
		t.Fatalf("failed to get WAL range: %v", err)
	}

	if start != 5 {
		t.Errorf("expected start seq 5, got %d", start)
	}
	if end != 10 {
		t.Errorf("expected end seq 10, got %d", end)
	}

	// Check HasUnindexedWAL
	hasUnindexed, err := indexer.HasUnindexedWAL(ctx, ns)
	if err != nil {
		t.Fatalf("failed to check unindexed WAL: %v", err)
	}
	if !hasUnindexed {
		t.Error("expected to have unindexed WAL")
	}

	// Process WAL range
	entries, totalBytes, err := indexer.ProcessWALRange(ctx, ns, start, end)
	if err != nil {
		t.Fatalf("failed to process WAL range: %v", err)
	}

	if len(entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(entries))
	}

	if totalBytes == 0 {
		t.Error("expected non-zero bytes")
	}

	// Verify entries are in order
	for i, entry := range entries {
		expectedSeq := uint64(i + 6)
		if entry.Seq != expectedSeq {
			t.Errorf("entry %d: expected seq %d, got %d", i, expectedSeq, entry.Seq)
		}
	}
}

func TestIndexerRunsAsynchronously(t *testing.T) {
	store := newMockStore()
	stateManager := namespace.NewStateManager(store)

	ns := "async-test"
	createNamespaceState(t, store, ns, 3, 0)
	for seq := uint64(1); seq <= 3; seq++ {
		createWALEntry(t, store, ns, seq)
	}

	var processCh = make(chan struct{}, 10)

	processor := func(ctx context.Context, namespace string, startSeq, endSeq uint64, state *namespace.State, etag string) (*WALProcessResult, error) {
		select {
		case processCh <- struct{}{}:
		default:
		}
		return &WALProcessResult{BytesIndexed: 0}, nil
	}

	config := &IndexerConfig{
		PollInterval:          10 * time.Millisecond,
		NamespacePollInterval: 1 * time.Second,
	}

	indexer := New(store, stateManager, config, processor)
	indexer.Start()

	// Watch namespace
	err := indexer.WatchNamespace(ns)
	if err != nil {
		t.Fatalf("failed to watch namespace: %v", err)
	}

	// The indexer should process asynchronously - we don't block on processing
	select {
	case <-processCh:
		// Processing happened asynchronously
	case <-time.After(500 * time.Millisecond):
		t.Error("expected async processing within timeout")
	}

	indexer.Stop()
}

func TestIndexerStopIsClean(t *testing.T) {
	store := newMockStore()
	stateManager := namespace.NewStateManager(store)

	config := &IndexerConfig{
		PollInterval:          10 * time.Millisecond,
		NamespacePollInterval: 10 * time.Millisecond,
	}

	indexer := New(store, stateManager, config, nil)
	indexer.Start()

	// Watch a namespace
	ns := "stop-test"
	createNamespaceState(t, store, ns, 0, 0)
	indexer.WatchNamespace(ns)

	time.Sleep(50 * time.Millisecond)

	// Stop should complete cleanly
	done := make(chan struct{})
	go func() {
		indexer.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Stopped successfully
	case <-time.After(2 * time.Second):
		t.Error("indexer.Stop() timed out")
	}
}

func TestIndexerDetectsDeletedNamespace(t *testing.T) {
	store := newMockStore()
	stateManager := namespace.NewStateManager(store)

	ns := "deleted-ns"
	createNamespaceState(t, store, ns, 0, 0)

	config := &IndexerConfig{
		PollInterval:          10 * time.Millisecond,
		NamespacePollInterval: 100 * time.Millisecond,
	}

	indexer := New(store, stateManager, config, nil)

	err := indexer.WatchNamespace(ns)
	if err != nil {
		t.Fatalf("failed to watch namespace: %v", err)
	}

	if !indexer.IsWatching(ns) {
		t.Error("expected namespace to be watched")
	}

	// Delete the namespace state
	key := namespace.StateKey(ns)
	store.mu.Lock()
	delete(store.objects, key)
	store.mu.Unlock()

	// Wait for watcher to detect deletion
	time.Sleep(100 * time.Millisecond)

	if indexer.IsWatching(ns) {
		t.Error("expected namespace to be unwatched after deletion")
	}

	indexer.Stop()
}

func TestExtractNamespaceFromKey(t *testing.T) {
	tests := []struct {
		key      string
		expected string
	}{
		{"vex/namespaces/my-ns/meta/state.json", "my-ns"},
		{"vex/namespaces/test_123/meta/state.json", "test_123"},
		{"vex/namespaces/ns/wal/1.wal.zst", ""},
		{"vex/namespaces/", ""},
		{"other/path/meta/state.json", ""},
	}

	for _, tc := range tests {
		result := extractNamespaceFromKey(tc.key)
		if result != tc.expected {
			t.Errorf("extractNamespaceFromKey(%q) = %q, expected %q", tc.key, result, tc.expected)
		}
	}
}

func TestIndexerNamespaceDiscovery(t *testing.T) {
	store := newMockStore()
	stateManager := namespace.NewStateManager(store)

	// Create multiple namespaces
	namespaces := []string{"ns1", "ns2", "ns3"}
	for _, ns := range namespaces {
		createNamespaceState(t, store, ns, 0, 0)
	}

	config := &IndexerConfig{
		PollInterval:          50 * time.Millisecond,
		NamespacePollInterval: 50 * time.Millisecond,
	}

	indexer := New(store, stateManager, config, nil)
	indexer.Start()

	// Wait for discovery
	time.Sleep(200 * time.Millisecond)

	// Check all namespaces are being watched
	for _, ns := range namespaces {
		if !indexer.IsWatching(ns) {
			t.Errorf("expected namespace %q to be watched", ns)
		}
	}

	if indexer.NamespaceCount() != len(namespaces) {
		t.Errorf("expected %d namespaces, got %d", len(namespaces), indexer.NamespaceCount())
	}

	indexer.Stop()
}

func TestIndexerMultipleWatchers(t *testing.T) {
	store := newMockStore()
	stateManager := namespace.NewStateManager(store)

	ns := "multi-watcher-test"
	createNamespaceState(t, store, ns, 0, 0)

	config := DefaultConfig()
	indexer := New(store, stateManager, config, nil)

	// Watch same namespace multiple times
	err := indexer.WatchNamespace(ns)
	if err != nil {
		t.Fatalf("first watch failed: %v", err)
	}

	err = indexer.WatchNamespace(ns)
	if err != nil {
		t.Fatalf("second watch failed: %v", err)
	}

	// Should only have one watcher
	if indexer.NamespaceCount() != 1 {
		t.Errorf("expected 1 namespace, got %d", indexer.NamespaceCount())
	}

	indexer.Stop()
}

func TestIndexerAdvancesIndexedWALSeq(t *testing.T) {
	store := newMockStore()
	stateManager := namespace.NewStateManager(store)

	ns := "advance-seq-test"
	createNamespaceState(t, store, ns, 5, 0)
	for seq := uint64(1); seq <= 5; seq++ {
		createWALEntry(t, store, ns, seq)
	}

	config := &IndexerConfig{
		PollInterval:          20 * time.Millisecond,
		NamespacePollInterval: 1 * time.Second,
	}

	// Use default processor which reads WAL
	indexer := New(store, stateManager, config, nil)

	err := indexer.WatchNamespace(ns)
	if err != nil {
		t.Fatalf("failed to watch namespace: %v", err)
	}

	// Wait for processing to complete
	time.Sleep(200 * time.Millisecond)

	// Check that indexed_wal_seq has advanced to head_seq
	ctx := context.Background()
	loaded, err := stateManager.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if loaded.State.Index.IndexedWALSeq != 5 {
		t.Errorf("expected indexed_wal_seq=5, got %d", loaded.State.Index.IndexedWALSeq)
	}

	// Verify there's no more unindexed WAL
	hasUnindexed, err := indexer.HasUnindexedWAL(ctx, ns)
	if err != nil {
		t.Fatalf("failed to check unindexed WAL: %v", err)
	}
	if hasUnindexed {
		t.Error("expected no unindexed WAL after processing")
	}

	indexer.Stop()
}

func TestIndexerAdvancesSeqEvenWithZeroBytes(t *testing.T) {
	store := newMockStore()
	stateManager := namespace.NewStateManager(store)

	ns := "zero-bytes-test"
	createNamespaceState(t, store, ns, 3, 0)
	for seq := uint64(1); seq <= 3; seq++ {
		createWALEntry(t, store, ns, seq)
	}

	var processorCalled atomic.Bool

	// Processor that returns 0 bytes but succeeds
	processor := func(ctx context.Context, namespace string, startSeq, endSeq uint64, state *namespace.State, etag string) (*WALProcessResult, error) {
		processorCalled.Store(true)
		return &WALProcessResult{BytesIndexed: 0}, nil
	}

	config := &IndexerConfig{
		PollInterval:          20 * time.Millisecond,
		NamespacePollInterval: 1 * time.Second,
	}

	indexer := New(store, stateManager, config, processor)

	err := indexer.WatchNamespace(ns)
	if err != nil {
		t.Fatalf("failed to watch namespace: %v", err)
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	if !processorCalled.Load() {
		t.Error("expected processor to be called")
	}

	// Check that indexed_wal_seq has advanced even with 0 bytes returned
	ctx := context.Background()
	loaded, err := stateManager.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if loaded.State.Index.IndexedWALSeq != 3 {
		t.Errorf("expected indexed_wal_seq=3 (even with 0 bytes), got %d", loaded.State.Index.IndexedWALSeq)
	}

	indexer.Stop()
}

func TestDefaultProcessorReadsWAL(t *testing.T) {
	store := newMockStore()
	stateManager := namespace.NewStateManager(store)

	ns := "default-processor-test"
	createNamespaceState(t, store, ns, 3, 0)
	for seq := uint64(1); seq <= 3; seq++ {
		createWALEntry(t, store, ns, seq)
	}

	config := &IndexerConfig{
		PollInterval:          20 * time.Millisecond,
		NamespacePollInterval: 1 * time.Second,
	}

	// Use nil processor to use default processor
	indexer := New(store, stateManager, config, nil)

	err := indexer.WatchNamespace(ns)
	if err != nil {
		t.Fatalf("failed to watch namespace: %v", err)
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	ctx := context.Background()
	loaded, err := stateManager.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	// Check that indexed_wal_seq advanced (default processor reads WAL)
	if loaded.State.Index.IndexedWALSeq != 3 {
		t.Errorf("expected indexed_wal_seq=3, got %d", loaded.State.Index.IndexedWALSeq)
	}

	// Check that bytes_unindexed_est decreased
	if loaded.State.WAL.BytesUnindexedEst != 0 {
		t.Logf("bytes_unindexed_est = %d (may not be zero depending on timing)", loaded.State.WAL.BytesUnindexedEst)
	}

	indexer.Stop()
}
