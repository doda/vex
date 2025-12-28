package warmer

import (
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/namespace"
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
