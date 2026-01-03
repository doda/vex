package warmer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestWarmer_NewAndClose(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateManager := namespace.NewStateManager(store)

	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: t.TempDir(),
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	w := New(store, stateManager, diskCache, nil, DefaultConfig())
	if w == nil {
		t.Fatal("expected warmer to be created")
	}

	// Close should not block
	err = w.Close()
	if err != nil {
		t.Errorf("expected no error on close, got %v", err)
	}
}

func TestWarmer_Enqueue(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateManager := namespace.NewStateManager(store)

	cfg := Config{
		Workers:   1,
		QueueSize: 10,
	}

	w := New(store, stateManager, nil, nil, cfg)
	defer w.Close()

	// Enqueue should return true for non-full queue
	if !w.Enqueue("test-ns") {
		t.Error("expected Enqueue to return true")
	}

	// Queue length should be at least 1 (may be 0 if worker already processed)
	// Can't reliably test this due to race with worker
}

func TestWarmer_EnqueueFullQueue(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateManager := namespace.NewStateManager(store)

	// Create manually without workers to test queue full behavior
	w := &Warmer{
		store:        store,
		stateManager: stateManager,
		tasks:        make(chan WarmTask, 2),
		workers:      0,
		done:         make(chan struct{}),
	}

	// Fill the queue
	w.Enqueue("ns1")
	w.Enqueue("ns2")

	// Third enqueue should fail (queue full)
	if w.Enqueue("ns3") {
		t.Error("expected Enqueue to return false when queue is full")
	}

	// Clean up
	close(w.done)
}

func TestWarmer_QueueLen(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateManager := namespace.NewStateManager(store)

	// Create warmer with no workers
	w := &Warmer{
		store:        store,
		stateManager: stateManager,
		tasks:        make(chan WarmTask, 10),
		workers:      0,
		done:         make(chan struct{}),
	}

	if w.QueueLen() != 0 {
		t.Errorf("expected queue length 0, got %d", w.QueueLen())
	}

	w.Enqueue("ns1")
	if w.QueueLen() != 1 {
		t.Errorf("expected queue length 1, got %d", w.QueueLen())
	}

	w.Enqueue("ns2")
	if w.QueueLen() != 2 {
		t.Errorf("expected queue length 2, got %d", w.QueueLen())
	}

	close(w.done)
}

func TestWarmer_DefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Workers != 2 {
		t.Errorf("expected default workers 2, got %d", cfg.Workers)
	}
	if cfg.QueueSize != 100 {
		t.Errorf("expected default queue size 100, got %d", cfg.QueueSize)
	}
}

func TestWarmer_WorkerProcessesTasks(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateManager := namespace.NewStateManager(store)

	// Create a namespace
	_, err := stateManager.Create(nil, "worker-test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: t.TempDir(),
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	cfg := Config{
		Workers:   1,
		QueueSize: 10,
	}

	w := New(store, stateManager, diskCache, nil, cfg)
	defer w.Close()

	// Enqueue a task
	w.Enqueue("worker-test-ns")

	// Wait a bit for the worker to process
	time.Sleep(50 * time.Millisecond)

	// Queue should be empty now
	if w.QueueLen() != 0 {
		t.Errorf("expected queue length 0 after processing, got %d", w.QueueLen())
	}
}

func TestWarmer_PrefetchBounds(t *testing.T) {
	ctx := context.Background()
	baseStore := objectstore.NewMemoryStore()
	store := newTrackingStore(baseStore)
	stateManager := namespace.NewStateManager(store)
	ns := "prefetch-bounds"

	loaded, err := stateManager.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	centroidsKey := "vex/namespaces/" + ns + "/index/segments/seg_1/vectors.centroids.bin"
	offsetsKey := "vex/namespaces/" + ns + "/index/segments/seg_1/vectors.cluster_offsets.bin"
	docsKey := "vex/namespaces/" + ns + "/index/segments/seg_1/docs.col.zst"
	filterKey1 := "vex/namespaces/" + ns + "/index/segments/seg_1/filters/category.bitmap"
	filterKey2 := "vex/namespaces/" + ns + "/index/segments/seg_1/filters/type.bitmap"

	putObject(t, store, centroidsKey, bytes.Repeat([]byte("c"), 128))
	putObject(t, store, offsetsKey, bytes.Repeat([]byte("o"), 128))
	docsData := bytes.Repeat([]byte("d"), 2048)
	putObject(t, store, docsKey, docsData)
	putObject(t, store, filterKey1, bytes.Repeat([]byte("f"), 1024))
	putObject(t, store, filterKey2, bytes.Repeat([]byte("g"), 1024))

	walKey := "vex/namespaces/" + ns + "/" + wal.KeyForSeq(1)
	loaded, err = stateManager.AdvanceWAL(ctx, ns, loaded.ETag, walKey, 0, nil)
	if err != nil {
		t.Fatalf("failed to advance wal: %v", err)
	}

	manifest := index.NewManifest(ns)
	manifest.IndexedWALSeq = 1
	manifest.AddSegment(index.Segment{
		ID:          "seg_1",
		Level:       index.L0,
		StartWALSeq: 1,
		EndWALSeq:   1,
		DocsKey:     docsKey,
		FilterKeys:  []string{filterKey1, filterKey2},
		IVFKeys: &index.IVFKeys{
			CentroidsKey:      centroidsKey,
			ClusterOffsetsKey: offsetsKey,
		},
	})

	manifestKey := index.ManifestKey(ns, 1)
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	putObject(t, store, manifestKey, manifestBytes)

	_, err = stateManager.UpdateIndexManifest(ctx, ns, loaded.ETag, manifestKey, 1, manifest.IndexedWALSeq)
	if err != nil {
		t.Fatalf("failed to update manifest: %v", err)
	}

	cfg := Config{
		Workers:             1,
		QueueSize:           1,
		DocsHeaderBytes:     64,
		HotFilterBitmapKeys: 1,
	}
	warmer := New(store, stateManager, nil, nil, cfg)
	defer warmer.Close()

	warmer.warmNamespace(ctx, ns)

	docRange := store.RangeFor(docsKey)
	if docRange == nil {
		t.Fatalf("expected range read for docs")
	}
	if docRange.Start != 0 || docRange.End != cfg.DocsHeaderBytes-1 {
		t.Fatalf("unexpected docs range: %+v", docRange)
	}
	if store.ReadBytes(docsKey) >= int64(len(docsData)) {
		t.Fatalf("expected partial docs read, got %d bytes of %d", store.ReadBytes(docsKey), len(docsData))
	}
	if store.ReadBytes(filterKey1) == 0 {
		t.Fatalf("expected hot filter bitmap to be prefetched")
	}
	if store.ReadBytes(filterKey2) != 0 {
		t.Fatalf("expected cold filter bitmap to be skipped")
	}
}

func TestExtractNamespace(t *testing.T) {
	tests := []struct {
		key      string
		expected string
	}{
		{"vex/namespaces/myns/index/segments/seg1/docs.col.zst", "myns"},
		{"vex/namespaces/test-ns/index/manifests/001.idx.json", "test-ns"},
		{"vex/namespaces/ns-with-dashes/filters/attr.bitmap", "ns-with-dashes"},
		{"vex/namespaces/", ""},
		{"vex/namespace/wrong", ""},
		{"other/path/file", ""},
		{"", ""},
	}

	for _, tt := range tests {
		got := extractNamespace(tt.key)
		if got != tt.expected {
			t.Errorf("extractNamespace(%q) = %q, want %q", tt.key, got, tt.expected)
		}
	}
}

type trackingStore struct {
	objectstore.Store
	mu         sync.Mutex
	readBytes  map[string]int64
	rangesSeen map[string]*objectstore.ByteRange
}

type countingReadCloser struct {
	io.ReadCloser
	key    string
	parent *trackingStore
}

func newTrackingStore(store objectstore.Store) *trackingStore {
	return &trackingStore{
		Store:      store,
		readBytes:  make(map[string]int64),
		rangesSeen: make(map[string]*objectstore.ByteRange),
	}
}

func (s *trackingStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	reader, info, err := s.Store.Get(ctx, key, opts)
	if err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	if opts != nil && opts.Range != nil {
		s.rangesSeen[key] = &objectstore.ByteRange{Start: opts.Range.Start, End: opts.Range.End}
	}
	s.mu.Unlock()
	return &countingReadCloser{ReadCloser: reader, key: key, parent: s}, info, nil
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if n > 0 {
		c.parent.mu.Lock()
		c.parent.readBytes[c.key] += int64(n)
		c.parent.mu.Unlock()
	}
	return n, err
}

func (s *trackingStore) ReadBytes(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readBytes[key]
}

func (s *trackingStore) RangeFor(key string) *objectstore.ByteRange {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rangesSeen[key]
}

func putObject(t *testing.T, store objectstore.Store, key string, data []byte) {
	t.Helper()
	_, err := store.Put(context.Background(), key, bytes.NewReader(data), int64(len(data)), nil)
	if err != nil {
		t.Fatalf("failed to put %s: %v", key, err)
	}
}
