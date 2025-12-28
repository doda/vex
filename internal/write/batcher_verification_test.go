package write

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestVerifyAtMostOneWALPerSecondPerNamespace verifies:
// "Verify at most 1 WAL entry committed per second per namespace"
func TestVerifyAtMostOneWALPerSecondPerNamespace(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Use a 500ms batch window for testing
	cfg := BatcherConfig{
		BatchWindow:  500 * time.Millisecond,
		MaxBatchSize: 100 * 1024 * 1024,
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "wal-rate-test"

	// Send 10 writes quickly - they should all batch into 1 WAL entry
	var wg sync.WaitGroup
	startCh := make(chan struct{})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startCh
			req := &WriteRequest{
				RequestID:  "req-" + string(rune('a'+idx)),
				UpsertRows: []map[string]any{{"id": uint64(idx + 1)}},
			}
			_, err := batcher.Submit(context.Background(), ns, req)
			if err != nil {
				t.Errorf("write %d failed: %v", idx, err)
			}
		}(i)
	}

	// Start all writes at once
	close(startCh)
	wg.Wait()

	// Load state and verify only 1 WAL entry was created
	state, err := stateMan.Load(context.Background(), ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if state.State.WAL.HeadSeq != 1 {
		t.Errorf("expected exactly 1 WAL entry (HeadSeq=1), got HeadSeq=%d", state.State.WAL.HeadSeq)
	}

	t.Logf("Verified: 10 concurrent writes -> 1 WAL entry (HeadSeq=%d)", state.State.WAL.HeadSeq)
}

// TestVerifyConcurrentWritesBatchIntoSameWALEntry verifies:
// "Test concurrent writes batch into same WAL entry"
func TestVerifyConcurrentWritesBatchIntoSameWALEntry(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	cfg := BatcherConfig{
		BatchWindow:  200 * time.Millisecond,
		MaxBatchSize: 100 * 1024 * 1024,
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "concurrent-batch-test"

	// Track response times to verify writes return at approximately the same time
	var wg sync.WaitGroup
	responses := make([]time.Time, 5)
	startCh := make(chan struct{})

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startCh
			req := &WriteRequest{
				RequestID:  "concurrent-" + string(rune('a'+idx)),
				UpsertRows: []map[string]any{{"id": uint64(idx + 1), "value": idx * 100}},
			}
			_, err := batcher.Submit(context.Background(), ns, req)
			responses[idx] = time.Now()
			if err != nil {
				t.Errorf("write %d failed: %v", idx, err)
			}
		}(i)
	}

	startTime := time.Now()
	close(startCh)
	wg.Wait()

	// Verify all responses came back at approximately the same time (within tolerance)
	// since they were batched into the same commit
	for i := 1; i < len(responses); i++ {
		diff := responses[i].Sub(responses[0]).Abs()
		if diff > 100*time.Millisecond {
			t.Errorf("response time difference too large between writes 0 and %d: %v", i, diff)
		}
	}

	// Verify single WAL entry
	state, err := stateMan.Load(context.Background(), ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if state.State.WAL.HeadSeq != 1 {
		t.Errorf("expected 1 WAL entry, got %d", state.State.WAL.HeadSeq)
	}

	// Verify WAL entry contains multiple sub-batches
	walKey := "vex/namespaces/" + ns + "/" + state.State.WAL.HeadKey
	reader, _, err := store.Get(context.Background(), walKey, nil)
	if err != nil {
		t.Fatalf("failed to read WAL entry: %v", err)
	}
	defer reader.Close()

	decoder, err := wal.NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	// Read all data
	var data []byte
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		data = append(data, buf[:n]...)
		if err != nil {
			break
		}
	}

	walEntry, err := decoder.Decode(data)
	if err != nil {
		t.Fatalf("failed to decode WAL entry: %v", err)
	}

	if len(walEntry.SubBatches) != 5 {
		t.Errorf("expected 5 sub-batches in WAL entry, got %d", len(walEntry.SubBatches))
	}

	t.Logf("Verified: 5 concurrent writes -> 1 WAL entry with %d sub-batches, elapsed %v",
		len(walEntry.SubBatches), time.Since(startTime))
}

// TestVerifyBatchWindowOrSizeThresholdTriggers verifies:
// "Verify batch window or size threshold triggers commit"
func TestVerifyBatchWindowOrSizeThresholdTriggers(t *testing.T) {
	t.Run("batch window trigger", func(t *testing.T) {
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)

		batchWindow := 100 * time.Millisecond
		cfg := BatcherConfig{
			BatchWindow:  batchWindow,
			MaxBatchSize: 100 * 1024 * 1024, // large size
		}

		batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
		if err != nil {
			t.Fatalf("failed to create batcher: %v", err)
		}
		defer batcher.Close()

		ns := "window-trigger-test"

		start := time.Now()
		req := &WriteRequest{
			RequestID:  "single-write",
			UpsertRows: []map[string]any{{"id": uint64(1)}},
		}
		_, err = batcher.Submit(context.Background(), ns, req)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("write failed: %v", err)
		}

		// Should take at least the batch window time
		if elapsed < batchWindow-20*time.Millisecond {
			t.Errorf("write completed too quickly (%v), batch window should delay", elapsed)
		}

		t.Logf("Verified: batch window trigger, elapsed=%v", elapsed)
	})

	t.Run("size threshold trigger", func(t *testing.T) {
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)

		cfg := BatcherConfig{
			BatchWindow:  10 * time.Second, // long window
			MaxBatchSize: 100,              // very small threshold
		}

		batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
		if err != nil {
			t.Fatalf("failed to create batcher: %v", err)
		}
		defer batcher.Close()

		ns := "size-trigger-test"

		// Create a write that exceeds the small size threshold
		largeData := make([]map[string]any, 5)
		for i := 0; i < 5; i++ {
			largeData[i] = map[string]any{
				"id":   uint64(i + 1),
				"data": "some data to push over threshold",
			}
		}

		start := time.Now()
		req := &WriteRequest{
			RequestID:  "large-write",
			UpsertRows: largeData,
		}
		_, err = batcher.Submit(context.Background(), ns, req)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("write failed: %v", err)
		}

		// Should complete much faster than the batch window due to size trigger
		if elapsed > 1*time.Second {
			t.Errorf("write took too long (%v), size threshold should trigger early", elapsed)
		}

		t.Logf("Verified: size threshold trigger, elapsed=%v", elapsed)
	})
}

// TestVerifyCommitLatencyUpToOneSecond verifies:
// "Test commit latency up to 1 second for window alignment"
func TestVerifyCommitLatencyUpToOneSecond(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Use 1 second batch window (as specified in the requirement)
	batchWindow := 1 * time.Second
	cfg := BatcherConfig{
		BatchWindow:  batchWindow,
		MaxBatchSize: 100 * 1024 * 1024,
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "latency-test"

	req := &WriteRequest{
		RequestID:  "latency-write",
		UpsertRows: []map[string]any{{"id": uint64(1)}},
	}

	start := time.Now()
	_, err = batcher.Submit(context.Background(), ns, req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Latency should be approximately the batch window (up to 1 second as spec says)
	minExpected := batchWindow - 100*time.Millisecond
	maxExpected := batchWindow + 200*time.Millisecond

	if elapsed < minExpected {
		t.Errorf("write completed too quickly (%v), expected at least %v", elapsed, minExpected)
	}
	if elapsed > maxExpected {
		t.Errorf("write took too long (%v), expected at most %v", elapsed, maxExpected)
	}

	t.Logf("Verified: commit latency with 1s window = %v (expected ~1s)", elapsed)
}
