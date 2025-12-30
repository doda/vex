package write

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestBatcherBatchesWritesPerSecond verifies that at most 1 WAL entry is committed per second per namespace.
func TestBatcherBatchesWritesPerSecond(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Use a short batch window for testing
	cfg := BatcherConfig{
		BatchWindow:  100 * time.Millisecond,
		MaxBatchSize: 100 * 1024 * 1024,
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "test-namespace"

	// Submit 3 writes quickly - they should all batch into 1 WAL entry
	var wg sync.WaitGroup
	results := make([]*WriteResponse, 3)
	errors := make([]error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := &WriteRequest{
				RequestID: "req-" + string(rune('a'+idx)),
				UpsertRows: []map[string]any{
					{"id": uint64(idx + 1), "name": "test-" + string(rune('a'+idx))},
				},
			}
			results[idx], errors[idx] = batcher.Submit(context.Background(), ns, req)
		}(i)
	}

	wg.Wait()

	// Check all writes succeeded
	for i := 0; i < 3; i++ {
		if errors[i] != nil {
			t.Fatalf("write %d failed: %v", i, errors[i])
		}
		if results[i] == nil {
			t.Fatalf("write %d returned nil response", i)
		}
		if results[i].RowsUpserted != 1 {
			t.Errorf("write %d: expected 1 row upserted, got %d", i, results[i].RowsUpserted)
		}
	}

	// Check that only 1 WAL entry was created
	state, err := stateMan.Load(context.Background(), ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}
	if state.State.WAL.HeadSeq != 1 {
		t.Errorf("expected WAL HeadSeq=1 (single batch), got %d", state.State.WAL.HeadSeq)
	}
}

// TestBatcherConcurrentWritesBatch verifies that concurrent writes to the same namespace batch together.
func TestBatcherConcurrentWritesBatch(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Use a short batch window for testing
	cfg := BatcherConfig{
		BatchWindow:  200 * time.Millisecond,
		MaxBatchSize: 100 * 1024 * 1024,
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "test-namespace"
	numWriters := 10

	var wg sync.WaitGroup
	var successCount atomic.Int32

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := &WriteRequest{
				RequestID: "concurrent-req-" + string(rune('a'+idx)),
				UpsertRows: []map[string]any{
					{"id": uint64(idx + 100), "value": idx * 10},
				},
			}
			resp, err := batcher.Submit(context.Background(), ns, req)
			if err != nil {
				t.Errorf("concurrent write %d failed: %v", idx, err)
				return
			}
			if resp.RowsUpserted == 1 {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if int(successCount.Load()) != numWriters {
		t.Errorf("expected %d successful writes, got %d", numWriters, successCount.Load())
	}

	// All concurrent writes should have batched into 1 WAL entry
	state, err := stateMan.Load(context.Background(), ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}
	if state.State.WAL.HeadSeq != 1 {
		t.Errorf("expected WAL HeadSeq=1 (single batch for concurrent writes), got %d", state.State.WAL.HeadSeq)
	}
}

// TestBatcherSizeThresholdTrigger verifies that size threshold triggers commit.
func TestBatcherSizeThresholdTrigger(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Use a very small size threshold to trigger early commit
	cfg := BatcherConfig{
		BatchWindow:  10 * time.Second, // long window, so size triggers first
		MaxBatchSize: 500,              // very small threshold
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "size-test-namespace"

	// Submit a large write that exceeds size threshold
	largeData := make([]map[string]any, 20)
	for i := 0; i < 20; i++ {
		largeData[i] = map[string]any{
			"id":          uint64(i + 1),
			"description": "This is a test document with some content to increase size",
			"metadata":    "Additional metadata field to push size over threshold",
		}
	}

	req := &WriteRequest{
		RequestID:  "large-write",
		UpsertRows: largeData,
	}

	start := time.Now()
	resp, err := batcher.Submit(context.Background(), ns, req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("large write failed: %v", err)
	}
	if resp.RowsUpserted != 20 {
		t.Errorf("expected 20 rows upserted, got %d", resp.RowsUpserted)
	}

	// Size threshold should have triggered immediate commit (not waiting for batch window)
	if elapsed > 1*time.Second {
		t.Errorf("write took too long (%v), size threshold should have triggered early commit", elapsed)
	}
}

// TestBatcherSizeThresholdRespectsRateLimit verifies size-triggered flushes do not exceed the per-window rate.
func TestBatcherSizeThresholdRespectsRateLimit(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	batchWindow := 100 * time.Millisecond
	cfg := BatcherConfig{
		BatchWindow:  batchWindow,
		MaxBatchSize: 200, // small threshold to force size flush
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "size-rate-limit-test"

	makeLargeWrite := func(requestID string, baseID uint64) *WriteRequest {
		rows := make([]map[string]any, 8)
		for i := 0; i < len(rows); i++ {
			rows[i] = map[string]any{
				"id":   baseID + uint64(i),
				"data": "some data to push over threshold",
			}
		}
		return &WriteRequest{
			RequestID:  requestID,
			UpsertRows: rows,
		}
	}

	if _, err := batcher.Submit(context.Background(), ns, makeLargeWrite("rate-limit-1", 1)); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	start := time.Now()
	if _, err := batcher.Submit(context.Background(), ns, makeLargeWrite("rate-limit-2", 100)); err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	elapsed := time.Since(start)

	minExpected := batchWindow - 20*time.Millisecond
	if elapsed < minExpected {
		t.Errorf("second write completed too quickly (%v), expected at least %v due to rate limit", elapsed, minExpected)
	}

	state, err := stateMan.Load(context.Background(), ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}
	if state.State.WAL.HeadSeq != 2 {
		t.Errorf("expected HeadSeq=2 (two batches), got %d", state.State.WAL.HeadSeq)
	}
}

// TestBatcherWindowAlignment verifies commit latency aligns with batch window.
func TestBatcherWindowAlignment(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Use 200ms batch window for testing
	batchWindow := 200 * time.Millisecond
	cfg := BatcherConfig{
		BatchWindow:  batchWindow,
		MaxBatchSize: 100 * 1024 * 1024,
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "window-test"

	// Submit a single write - it should wait for batch window
	req := &WriteRequest{
		RequestID: "single-write",
		UpsertRows: []map[string]any{
			{"id": uint64(1), "value": "test"},
		},
	}

	start := time.Now()
	resp, err := batcher.Submit(context.Background(), ns, req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}

	// The write should take approximately the batch window time
	// Allow some tolerance for scheduling variance
	minExpected := batchWindow - 50*time.Millisecond
	maxExpected := batchWindow + 100*time.Millisecond

	if elapsed < minExpected {
		t.Errorf("write completed too quickly (%v), expected at least %v", elapsed, minExpected)
	}
	if elapsed > maxExpected {
		t.Errorf("write took too long (%v), expected at most %v", elapsed, maxExpected)
	}
}

// TestBatcherMultipleNamespaces verifies batching is independent per namespace.
func TestBatcherMultipleNamespaces(t *testing.T) {
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

	// Submit concurrent writes to different namespaces, all at the same time
	namespaces := []string{"ns1", "ns2", "ns3"}
	var wg sync.WaitGroup

	// Use a channel to coordinate starting all writes simultaneously
	startCh := make(chan struct{})

	for _, ns := range namespaces {
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(namespace string, idx int) {
				defer wg.Done()
				// Wait for start signal to ensure all writes start together
				<-startCh
				req := &WriteRequest{
					RequestID: namespace + "-req-" + string(rune('0'+idx)),
					UpsertRows: []map[string]any{
						{"id": uint64(idx + 1), "namespace": namespace},
					},
				}
				_, err := batcher.Submit(context.Background(), namespace, req)
				if err != nil {
					t.Errorf("write to %s failed: %v", namespace, err)
				}
			}(ns, i)
		}
	}

	// Start all writes at once
	close(startCh)

	wg.Wait()

	// Each namespace should have batched its writes
	// Due to the concurrent nature, we may have 1 or a small number of batches per namespace
	// The important thing is that independent namespaces don't interfere with each other
	for _, ns := range namespaces {
		state, err := stateMan.Load(context.Background(), ns)
		if err != nil {
			t.Fatalf("failed to load state for %s: %v", ns, err)
		}
		// Verify some writes were batched (HeadSeq should be 3 or less for 3 writes)
		if state.State.WAL.HeadSeq > 3 {
			t.Errorf("namespace %s: expected HeadSeq <= 3, got %d", ns, state.State.WAL.HeadSeq)
		}
	}
}

// TestBatcherSequentialBatches verifies multiple batches committed in sequence.
func TestBatcherSequentialBatches(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	batchWindow := 50 * time.Millisecond
	cfg := BatcherConfig{
		BatchWindow:  batchWindow,
		MaxBatchSize: 100 * 1024 * 1024,
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "sequential-test"

	// Submit first batch
	req1 := &WriteRequest{
		RequestID:  "batch1-req",
		UpsertRows: []map[string]any{{"id": uint64(1), "batch": 1}},
	}
	_, err = batcher.Submit(context.Background(), ns, req1)
	if err != nil {
		t.Fatalf("first batch failed: %v", err)
	}

	// Wait for batch window to expire to ensure new batch starts
	time.Sleep(batchWindow + 50*time.Millisecond)

	// Submit second batch
	req2 := &WriteRequest{
		RequestID:  "batch2-req",
		UpsertRows: []map[string]any{{"id": uint64(2), "batch": 2}},
	}
	_, err = batcher.Submit(context.Background(), ns, req2)
	if err != nil {
		t.Fatalf("second batch failed: %v", err)
	}

	// Should have 2 WAL entries now
	state, err := stateMan.Load(context.Background(), ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if state.State.WAL.HeadSeq != 2 {
		t.Errorf("expected HeadSeq=2 (two batches), got %d", state.State.WAL.HeadSeq)
	}
}

// TestBatcherClose verifies that Close flushes pending batches.
func TestBatcherClose(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	cfg := BatcherConfig{
		BatchWindow:  10 * time.Second, // long window
		MaxBatchSize: 100 * 1024 * 1024,
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}

	ns := "close-test"

	// Submit a write in a goroutine
	done := make(chan struct{})
	go func() {
		req := &WriteRequest{
			RequestID:  "pending-write",
			UpsertRows: []map[string]any{{"id": uint64(1), "value": "pending"}},
		}
		_, err := batcher.Submit(context.Background(), ns, req)
		if err != nil {
			t.Errorf("write failed: %v", err)
		}
		close(done)
	}()

	// Give the write time to start
	time.Sleep(50 * time.Millisecond)

	// Close should flush the pending batch
	if err := batcher.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Wait for the write to complete
	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("write did not complete after Close")
	}

	// The write should have been committed
	state, err := stateMan.Load(context.Background(), ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if state.State.WAL.HeadSeq != 1 {
		t.Errorf("expected HeadSeq=1, got %d", state.State.WAL.HeadSeq)
	}
}

// TestBatcherClosedRejectsWrites verifies that closed batcher rejects new writes.
func TestBatcherClosedRejectsWrites(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	batcher, err := NewBatcher(store, stateMan, nil)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}

	// Close the batcher
	if err := batcher.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Try to submit a write - should fail
	req := &WriteRequest{
		RequestID:  "rejected-write",
		UpsertRows: []map[string]any{{"id": uint64(1)}},
	}
	_, err = batcher.Submit(context.Background(), "test-ns", req)
	if err != ErrBatcherClosed {
		t.Errorf("expected ErrBatcherClosed, got %v", err)
	}
}

// TestBatcherSubBatchesPreserved verifies each write becomes a separate sub-batch.
func TestBatcherSubBatchesPreserved(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	cfg := BatcherConfig{
		BatchWindow:  5 * time.Second,
		MaxBatchSize: 100 * 1024 * 1024,
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "subbatch-test"
	requests := []struct {
		id  string
		req *WriteRequest
	}{
		{
			id: "subbatch-1",
			req: &WriteRequest{
				RequestID: "subbatch-1",
				UpsertRows: []map[string]any{
					{"id": uint64(20), "name": "bravo"},
					{"id": uint64(10), "name": "alpha"},
				},
			},
		},
		{
			id: "subbatch-2",
			req: &WriteRequest{
				RequestID: "subbatch-2",
				PatchRows: []map[string]any{
					{"id": uint64(10), "tag": "patched"},
				},
			},
		},
		{
			id: "subbatch-3",
			req: &WriteRequest{
				RequestID: "subbatch-3",
				Deletes:   []any{uint64(20)},
			},
		},
	}

	results := make([]*WriteResponse, len(requests))
	errs := make([]error, len(requests))
	var wg sync.WaitGroup

	for i, item := range requests {
		wg.Add(1)
		go func(idx int, req *WriteRequest) {
			defer wg.Done()
			results[idx], errs[idx] = batcher.Submit(context.Background(), ns, req)
		}(i, item.req)
		waitForBatchSize(t, batcher, ns, i+1)
	}

	batcher.timerFlush(ns)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
		if results[i] == nil {
			t.Fatalf("write %d returned nil response", i)
		}
	}

	state, err := stateMan.Load(context.Background(), ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if state.State.WAL.HeadSeq != 1 {
		t.Errorf("expected HeadSeq=1, got %d", state.State.WAL.HeadSeq)
	}

	committer, err := wal.NewCommitter(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	entry, err := committer.ReadWAL(context.Background(), ns, 1)
	if err != nil {
		t.Fatalf("failed to read WAL entry: %v", err)
	}
	if len(entry.SubBatches) != len(requests) {
		t.Fatalf("expected %d sub-batches, got %d", len(requests), len(entry.SubBatches))
	}

	for i, subBatch := range entry.SubBatches {
		if subBatch.RequestId != requests[i].id {
			t.Errorf("sub-batch %d request_id=%q, want %q", i, subBatch.RequestId, requests[i].id)
		}
	}

	first := entry.SubBatches[0]
	if len(first.Mutations) != 2 {
		t.Fatalf("expected 2 mutations in first sub-batch, got %d", len(first.Mutations))
	}
	if first.Mutations[0].Type != wal.MutationType_MUTATION_TYPE_UPSERT ||
		first.Mutations[1].Type != wal.MutationType_MUTATION_TYPE_UPSERT {
		t.Fatalf("expected upsert mutations in first sub-batch")
	}
	if got := first.Mutations[0].GetId().GetU64(); got != 10 {
		t.Fatalf("expected first mutation id=10, got %d", got)
	}
	if got := first.Mutations[1].GetId().GetU64(); got != 20 {
		t.Fatalf("expected second mutation id=20, got %d", got)
	}
	if got := first.Mutations[0].Attributes["name"].GetStringVal(); got != "alpha" {
		t.Fatalf("expected id=10 name=alpha, got %q", got)
	}
	if got := first.Mutations[1].Attributes["name"].GetStringVal(); got != "bravo" {
		t.Fatalf("expected id=20 name=bravo, got %q", got)
	}

	second := entry.SubBatches[1]
	if len(second.Mutations) != 1 {
		t.Fatalf("expected 1 mutation in second sub-batch, got %d", len(second.Mutations))
	}
	if second.Mutations[0].Type != wal.MutationType_MUTATION_TYPE_PATCH {
		t.Fatalf("expected patch mutation in second sub-batch")
	}
	if got := second.Mutations[0].GetId().GetU64(); got != 10 {
		t.Fatalf("expected patch mutation id=10, got %d", got)
	}
	if got := second.Mutations[0].Attributes["tag"].GetStringVal(); got != "patched" {
		t.Fatalf("expected patch tag=patched, got %q", got)
	}

	third := entry.SubBatches[2]
	if len(third.Mutations) != 1 {
		t.Fatalf("expected 1 mutation in third sub-batch, got %d", len(third.Mutations))
	}
	if third.Mutations[0].Type != wal.MutationType_MUTATION_TYPE_DELETE {
		t.Fatalf("expected delete mutation in third sub-batch")
	}
	if got := third.Mutations[0].GetId().GetU64(); got != 20 {
		t.Fatalf("expected delete mutation id=20, got %d", got)
	}
}

// TestBatcherContextCancellation verifies context cancellation is handled properly.
func TestBatcherContextCancellation(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	cfg := BatcherConfig{
		BatchWindow:  5 * time.Second, // long window
		MaxBatchSize: 100 * 1024 * 1024,
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ns := "cancel-test"

	// Create a context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	req := &WriteRequest{
		RequestID:  "cancel-write",
		UpsertRows: []map[string]any{{"id": uint64(1)}},
	}

	_, err = batcher.Submit(ctx, ns, req)
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func waitForBatchSize(t *testing.T, batcher *Batcher, ns string, size int) {
	t.Helper()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		batcher.mu.Lock()
		batch := batcher.batches[ns]
		count := 0
		if batch != nil {
			count = len(batch.writes)
		}
		batcher.mu.Unlock()
		if count >= size {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for batch size %d", size)
}

// TestBatcherSizeThresholdTimerRace verifies no race between size-threshold and timer flush.
func TestBatcherSizeThresholdTimerRace(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Use very short window and small size to maximize chance of race
	cfg := BatcherConfig{
		BatchWindow:  10 * time.Millisecond,
		MaxBatchSize: 100, // very small
	}

	batcher, err := NewBatcherWithConfig(store, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	// Run multiple iterations to increase chance of hitting race
	for i := 0; i < 10; i++ {
		ns := "race-test-" + string(rune('a'+i))

		var wg sync.WaitGroup
		numWrites := 5

		for j := 0; j < numWrites; j++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				// Create writes that are just under the size threshold
				// so accumulation triggers size flush
				req := &WriteRequest{
					RequestID: "race-req-" + string(rune('0'+idx)),
					UpsertRows: []map[string]any{
						{"id": uint64(idx + 1), "data": "some test data to add size"},
					},
				}
				_, err := batcher.Submit(context.Background(), ns, req)
				if err != nil {
					t.Errorf("write %d failed: %v", idx, err)
				}
			}(j)
		}

		wg.Wait()

		// Verify namespace state exists and is valid
		state, err := stateMan.Load(context.Background(), ns)
		if err != nil {
			t.Fatalf("failed to load state for %s: %v", ns, err)
		}
		if state.State.WAL.HeadSeq == 0 {
			t.Errorf("namespace %s: HeadSeq should be > 0, got 0", ns)
		}
	}
}

// TestBatcherIndependentContexts verifies one write's context doesn't affect others.
func TestBatcherIndependentContexts(t *testing.T) {
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

	ns := "context-test"
	var wg sync.WaitGroup
	results := make([]error, 3)

	// Write 1: normal context
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := &WriteRequest{
			RequestID:  "normal-write",
			UpsertRows: []map[string]any{{"id": uint64(1)}},
		}
		_, results[0] = batcher.Submit(context.Background(), ns, req)
	}()

	// Write 2: cancelled context (but should still be included in batch)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately
		req := &WriteRequest{
			RequestID:  "cancelled-write",
			UpsertRows: []map[string]any{{"id": uint64(2)}},
		}
		_, results[1] = batcher.Submit(ctx, ns, req)
	}()

	// Write 3: normal context
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := &WriteRequest{
			RequestID:  "another-write",
			UpsertRows: []map[string]any{{"id": uint64(3)}},
		}
		_, results[2] = batcher.Submit(context.Background(), ns, req)
	}()

	wg.Wait()

	// Write 1 and 3 should succeed
	if results[0] != nil {
		t.Errorf("write 1 should succeed, got: %v", results[0])
	}
	if results[2] != nil {
		t.Errorf("write 3 should succeed, got: %v", results[2])
	}

	// Write 2 may fail with context cancelled - that's expected behavior
	// The key is that write 1 and 3 are not affected

	// Verify state was updated
	state, err := stateMan.Load(context.Background(), ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if state.State.WAL.HeadSeq == 0 {
		t.Error("expected HeadSeq > 0")
	}
}

// TestBatcher_CompatModeRejectsDotProduct verifies that dot_product is rejected in turbopuffer mode.
func TestBatcher_CompatModeRejectsDotProduct(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create batcher with turbopuffer compat mode (default)
	batcher, err := NewBatcherWithCompatMode(store, stateMan, nil, "turbopuffer")
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	// Try to submit with dot_product distance metric
	req := &WriteRequest{
		RequestID: "test-req-1",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "test"},
		},
		DistanceMetric: "dot_product",
	}

	_, err = batcher.Submit(ctx, "test-ns", req)
	if err == nil {
		t.Fatal("expected error for dot_product in turbopuffer mode, got nil")
	}
	if !errors.Is(err, ErrDotProductNotAllowed) {
		t.Errorf("expected ErrDotProductNotAllowed, got: %v", err)
	}
}

// TestBatcher_VexModeAllowsDotProduct verifies that dot_product is allowed in vex mode.
func TestBatcher_VexModeAllowsDotProduct(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create batcher with vex compat mode
	batcher, err := NewBatcherWithCompatMode(store, stateMan, nil, "vex")
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	// Try to submit with dot_product distance metric - should succeed
	req := &WriteRequest{
		RequestID: "test-req-1",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "test"},
		},
		DistanceMetric: "dot_product",
	}

	_, err = batcher.Submit(ctx, "test-ns", req)
	if err != nil {
		t.Errorf("expected no error for dot_product in vex mode, got: %v", err)
	}
}

// TestBatcher_CompatMode verifies the compat mode getter.
func TestBatcher_CompatMode(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	tests := []struct {
		compatMode string
	}{
		{"turbopuffer"},
		{"vex"},
	}

	for _, tt := range tests {
		t.Run(tt.compatMode, func(t *testing.T) {
			batcher, err := NewBatcherWithCompatMode(store, stateMan, nil, tt.compatMode)
			if err != nil {
				t.Fatalf("failed to create batcher: %v", err)
			}
			defer batcher.Close()

			if batcher.CompatMode() != tt.compatMode {
				t.Errorf("Batcher.CompatMode() = %q, want %q", batcher.CompatMode(), tt.compatMode)
			}
		})
	}
}
