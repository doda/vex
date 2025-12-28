package wal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestRepairerDetectHighestContiguousSeq(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	repairer := NewRepairer(store, stateMgr)

	ctx := context.Background()
	ns := "test-detect-contiguous"

	// Create namespace state
	_, err := stateMgr.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	// Create WAL entries 1, 2, 3 (contiguous)
	for seq := uint64(1); seq <= 3; seq++ {
		walKey := walKeyForNamespace(ns, seq)
		data := []byte(fmt.Sprintf("wal-entry-%d", seq))
		_, err := store.PutIfAbsent(ctx, walKey, bytes.NewReader(data), int64(len(data)), nil)
		if err != nil {
			t.Fatalf("failed to upload WAL %d: %v", seq, err)
		}
	}

	// Detect highest contiguous
	highestContiguous, orphaned, err := repairer.DetectHighestContiguousSeq(ctx, ns)
	if err != nil {
		t.Fatalf("failed to detect: %v", err)
	}

	if highestContiguous != 3 {
		t.Errorf("expected highest contiguous 3, got %d", highestContiguous)
	}

	if len(orphaned) != 0 {
		t.Errorf("expected no orphaned WALs, got %v", orphaned)
	}
}

func TestRepairerDetectWithGaps(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	repairer := NewRepairer(store, stateMgr)

	ctx := context.Background()
	ns := "test-detect-gaps"

	// Create namespace state
	_, err := stateMgr.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	// Create WAL entries 1, 2, 5 (gap at 3, 4)
	for _, seq := range []uint64{1, 2, 5} {
		walKey := walKeyForNamespace(ns, seq)
		data := []byte(fmt.Sprintf("wal-entry-%d", seq))
		_, err := store.PutIfAbsent(ctx, walKey, bytes.NewReader(data), int64(len(data)), nil)
		if err != nil {
			t.Fatalf("failed to upload WAL %d: %v", seq, err)
		}
	}

	// Detect highest contiguous
	highestContiguous, orphaned, err := repairer.DetectHighestContiguousSeq(ctx, ns)
	if err != nil {
		t.Fatalf("failed to detect: %v", err)
	}

	if highestContiguous != 2 {
		t.Errorf("expected highest contiguous 2, got %d", highestContiguous)
	}

	if len(orphaned) != 1 || orphaned[0] != 5 {
		t.Errorf("expected orphaned [5], got %v", orphaned)
	}
}

func TestRepairerRepairPartialCommit(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	repairer := NewRepairer(store, stateMgr)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-repair-partial"

	// Create namespace and commit first WAL through normal flow
	entry1 := NewWalEntry(ns, 0)
	batch1 := NewWriteSubBatch("req-1")
	batch1.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), nil, nil, 0)
	entry1.SubBatches = append(entry1.SubBatches, batch1)

	result1, err := committer.Commit(ctx, ns, entry1, nil)
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}

	if result1.Seq != 1 {
		t.Errorf("expected seq 1, got %d", result1.Seq)
	}

	// Simulate partial commit: upload WAL 2 but don't update state
	entry2 := NewWalEntry(ns, 2)
	entry2.Namespace = ns
	entry2.Seq = 2
	batch2 := NewWriteSubBatch("req-2")
	batch2.AddUpsert(DocumentIDFromID(document.NewU64ID(2)), nil, nil, 0)
	entry2.SubBatches = append(entry2.SubBatches, batch2)

	encoder, _ := NewEncoder()
	encResult, _ := encoder.Encode(entry2)
	encoder.Close()

	walKey2 := walKeyForNamespace(ns, 2)
	_, err = store.PutIfAbsent(ctx, walKey2, bytes.NewReader(encResult.Data), int64(len(encResult.Data)), nil)
	if err != nil {
		t.Fatalf("failed to upload WAL 2: %v", err)
	}

	// Verify state still shows head_seq = 1
	loaded, _ := stateMgr.Load(ctx, ns)
	if loaded.State.WAL.HeadSeq != 1 {
		t.Errorf("expected head_seq 1 before repair, got %d", loaded.State.WAL.HeadSeq)
	}

	// Run repair
	repairResult, err := repairer.Repair(ctx, ns)
	if err != nil {
		t.Fatalf("repair failed: %v", err)
	}

	if repairResult.OriginalHeadSeq != 1 {
		t.Errorf("expected original head_seq 1, got %d", repairResult.OriginalHeadSeq)
	}

	if repairResult.NewHeadSeq != 2 {
		t.Errorf("expected new head_seq 2, got %d", repairResult.NewHeadSeq)
	}

	if repairResult.WALsRepaired != 1 {
		t.Errorf("expected 1 WAL repaired, got %d", repairResult.WALsRepaired)
	}

	// Verify state is now updated
	loadedAfter, _ := stateMgr.Load(ctx, ns)
	if loadedAfter.State.WAL.HeadSeq != 2 {
		t.Errorf("expected head_seq 2 after repair, got %d", loadedAfter.State.WAL.HeadSeq)
	}
}

func TestRepairerNoRepairNeeded(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	repairer := NewRepairer(store, stateMgr)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-no-repair"

	// Commit WAL normally
	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	_, err = committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Run repair
	repairResult, err := repairer.Repair(ctx, ns)
	if err != nil {
		t.Fatalf("repair failed: %v", err)
	}

	if repairResult.WALsRepaired != 0 {
		t.Errorf("expected 0 WALs repaired, got %d", repairResult.WALsRepaired)
	}

	if repairResult.OriginalHeadSeq != repairResult.NewHeadSeq {
		t.Errorf("expected no change in head_seq")
	}
}

func TestWriterRetriesStateCASAfterWALUpload(t *testing.T) {
	store := objectstore.NewMemoryStore()

	// Wrap store to fail CAS a couple times
	failCount := 0
	wrapper := &stateCASFailingStore{
		Store:     store,
		failCount: &failCount,
		maxFails:  2,
	}

	stateMgr := namespace.NewStateManager(wrapper)
	committer, err := NewCommitter(wrapper, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-retry-cas"

	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	// Commit should succeed despite CAS failures (retries)
	result, err := committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("commit failed despite retries: %v", err)
	}

	if result.Seq != 1 {
		t.Errorf("expected seq 1, got %d", result.Seq)
	}

	// Verify retries happened
	if failCount < 2 {
		t.Errorf("expected at least 2 CAS failures, got %d", failCount)
	}

	// Verify final state is correct
	loaded, _ := stateMgr.Load(ctx, ns)
	if loaded.State.WAL.HeadSeq != 1 {
		t.Errorf("expected head_seq 1, got %d", loaded.State.WAL.HeadSeq)
	}
}

func TestRepairTaskStalenessBound(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	repairer := NewRepairer(store, stateMgr)

	// Use short intervals for testing
	stalenessBound := 100 * time.Millisecond
	task := NewRepairTask(repairer, 50*time.Millisecond, stalenessBound)

	if task.StalenessBound() != stalenessBound {
		t.Errorf("expected staleness bound %v, got %v", stalenessBound, task.StalenessBound())
	}

	ctx := context.Background()
	ns := "test-staleness"

	// Create namespace
	_, err := stateMgr.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	// First repair
	_, err = task.RepairOnce(ctx, ns)
	if err != nil {
		t.Fatalf("first repair failed: %v", err)
	}

	firstRepairTime := task.GetLastRepairTime(ns)
	if firstRepairTime.IsZero() {
		t.Error("expected non-zero repair time after first repair")
	}

	// Immediate second repair should use cached result (within staleness bound)
	time.Sleep(10 * time.Millisecond)
	_, err = task.RepairOnce(ctx, ns)
	if err != nil {
		t.Fatalf("second repair failed: %v", err)
	}

	// Wait past staleness bound
	time.Sleep(stalenessBound + 50*time.Millisecond)

	// Third repair should run fresh
	_, err = task.RepairOnce(ctx, ns)
	if err != nil {
		t.Fatalf("third repair failed: %v", err)
	}

	thirdRepairTime := task.GetLastRepairTime(ns)
	if !thirdRepairTime.After(firstRepairTime) {
		t.Error("expected third repair time to be after first")
	}
}

func TestRepairTaskDefault60sStalenessBound(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	repairer := NewRepairer(store, stateMgr)

	// Use zero to get default
	task := NewRepairTask(repairer, 0, 0)

	expectedBound := 60 * time.Second
	if task.StalenessBound() != expectedBound {
		t.Errorf("expected default staleness bound %v, got %v", expectedBound, task.StalenessBound())
	}
}

func TestRepairMultiplePartialCommits(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	repairer := NewRepairer(store, stateMgr)

	ctx := context.Background()
	ns := "test-multi-partial"

	// Create namespace
	_, err := stateMgr.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	// Upload WALs 1, 2, 3 without updating state (all partial commits)
	for seq := uint64(1); seq <= 3; seq++ {
		entry := NewWalEntry(ns, seq)
		entry.Namespace = ns
		entry.Seq = seq
		batch := NewWriteSubBatch(fmt.Sprintf("req-%d", seq))
		batch.AddUpsert(DocumentIDFromID(document.NewU64ID(seq)), nil, nil, 0)
		entry.SubBatches = append(entry.SubBatches, batch)

		encoder, _ := NewEncoder()
		encResult, _ := encoder.Encode(entry)
		encoder.Close()

		walKey := walKeyForNamespace(ns, seq)
		_, err = store.PutIfAbsent(ctx, walKey, bytes.NewReader(encResult.Data), int64(len(encResult.Data)), nil)
		if err != nil {
			t.Fatalf("failed to upload WAL %d: %v", seq, err)
		}
	}

	// Run repair
	repairResult, err := repairer.Repair(ctx, ns)
	if err != nil {
		t.Fatalf("repair failed: %v", err)
	}

	if repairResult.OriginalHeadSeq != 0 {
		t.Errorf("expected original head_seq 0, got %d", repairResult.OriginalHeadSeq)
	}

	if repairResult.NewHeadSeq != 3 {
		t.Errorf("expected new head_seq 3, got %d", repairResult.NewHeadSeq)
	}

	if repairResult.WALsRepaired != 3 {
		t.Errorf("expected 3 WALs repaired, got %d", repairResult.WALsRepaired)
	}

	// Verify state
	loaded, _ := stateMgr.Load(ctx, ns)
	if loaded.State.WAL.HeadSeq != 3 {
		t.Errorf("expected head_seq 3 after repair, got %d", loaded.State.WAL.HeadSeq)
	}
}

func TestRepairTaskStartStop(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	repairer := NewRepairer(store, stateMgr)

	ctx := context.Background()
	ns := "test-start-stop"

	// Create namespace
	_, err := stateMgr.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	// Upload a partial WAL
	entry := NewWalEntry(ns, 1)
	entry.Namespace = ns
	entry.Seq = 1
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	encoder, _ := NewEncoder()
	encResult, _ := encoder.Encode(entry)
	encoder.Close()

	walKey := walKeyForNamespace(ns, 1)
	_, err = store.PutIfAbsent(ctx, walKey, bytes.NewReader(encResult.Data), int64(len(encResult.Data)), nil)
	if err != nil {
		t.Fatalf("failed to upload WAL: %v", err)
	}

	// Create task with short intervals
	task := NewRepairTask(repairer, 50*time.Millisecond, 50*time.Millisecond)

	// Start task
	task.Start(ctx, []string{ns})

	// Wait for repair to happen
	time.Sleep(200 * time.Millisecond)

	// Stop task
	task.Stop()

	// Verify repair happened
	loaded, _ := stateMgr.Load(ctx, ns)
	if loaded.State.WAL.HeadSeq != 1 {
		t.Errorf("expected head_seq 1 after repair task, got %d", loaded.State.WAL.HeadSeq)
	}

	// Verify last repair was recorded
	lastSeq := task.GetLastRepairSeq(ns)
	if lastSeq != 1 {
		t.Errorf("expected last repair seq 1, got %d", lastSeq)
	}
}

func TestRepairAdvancesStateSafely(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	repairer := NewRepairer(store, stateMgr)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-safe-advance"

	// Create initial state via normal commit
	entry1 := NewWalEntry(ns, 0)
	batch1 := NewWriteSubBatch("req-1")
	batch1.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), nil, nil, 0)
	entry1.SubBatches = append(entry1.SubBatches, batch1)

	_, err = committer.Commit(ctx, ns, entry1, nil)
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}

	// Add partial WALs 2 and 3
	for seq := uint64(2); seq <= 3; seq++ {
		entry := NewWalEntry(ns, seq)
		entry.Namespace = ns
		entry.Seq = seq
		batch := NewWriteSubBatch(fmt.Sprintf("req-%d", seq))
		batch.AddUpsert(DocumentIDFromID(document.NewU64ID(seq)), nil, nil, 0)
		entry.SubBatches = append(entry.SubBatches, batch)

		encoder, _ := NewEncoder()
		encResult, _ := encoder.Encode(entry)
		encoder.Close()

		walKey := walKeyForNamespace(ns, seq)
		_, err = store.PutIfAbsent(ctx, walKey, bytes.NewReader(encResult.Data), int64(len(encResult.Data)), nil)
		if err != nil {
			t.Fatalf("failed to upload WAL %d: %v", seq, err)
		}
	}

	// Run repair
	result, err := repairer.Repair(ctx, ns)
	if err != nil {
		t.Fatalf("repair failed: %v", err)
	}

	// Verify state advanced by exactly +1 increments
	if result.WALsRepaired != 2 {
		t.Errorf("expected 2 WALs repaired, got %d", result.WALsRepaired)
	}

	// Verify final state
	loaded, _ := stateMgr.Load(ctx, ns)
	if loaded.State.WAL.HeadSeq != 3 {
		t.Errorf("expected head_seq 3, got %d", loaded.State.WAL.HeadSeq)
	}

	// HeadKey should point to the last WAL
	expectedKey := KeyForSeq(3)
	if loaded.State.WAL.HeadKey != expectedKey {
		t.Errorf("expected head_key %s, got %s", expectedKey, loaded.State.WAL.HeadKey)
	}
}

func TestRepairConcurrentWithWriter(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	repairer := NewRepairer(store, stateMgr)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-concurrent"

	// Create initial namespace
	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-init")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(0)), nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)
	_, err = committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("initial commit failed: %v", err)
	}

	var wg sync.WaitGroup
	errorsCh := make(chan error, 10)

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			entry := NewWalEntry(ns, 0)
			batch := NewWriteSubBatch(fmt.Sprintf("req-%d", i))
			batch.AddUpsert(DocumentIDFromID(document.NewU64ID(uint64(100+i))), nil, nil, 0)
			entry.SubBatches = append(entry.SubBatches, batch)

			_, err := committer.Commit(ctx, ns, entry, nil)
			if err != nil {
				errorsCh <- fmt.Errorf("commit %d failed: %w", i, err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Repair goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			_, err := repairer.Repair(ctx, ns)
			if err != nil {
				errorsCh <- fmt.Errorf("repair %d failed: %w", i, err)
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errorsCh)

	for err := range errorsCh {
		t.Errorf("concurrent error: %v", err)
	}

	// Verify final state is consistent
	loaded, _ := stateMgr.Load(ctx, ns)
	if loaded.State.WAL.HeadSeq < 1 {
		t.Errorf("expected head_seq >= 1, got %d", loaded.State.WAL.HeadSeq)
	}

	// Verify all WALs up to head_seq exist
	for seq := uint64(1); seq <= loaded.State.WAL.HeadSeq; seq++ {
		walKey := walKeyForNamespace(ns, seq)
		_, err := store.Head(ctx, walKey)
		if err != nil {
			t.Errorf("WAL seq %d should exist: %v", seq, err)
		}
	}
}

// Helper: store that fails PutIfMatch a configurable number of times
type stateCASFailingStore struct {
	objectstore.Store
	failCount *int
	maxFails  int
	mu        sync.Mutex
}

func (s *stateCASFailingStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	s.mu.Lock()
	if *s.failCount < s.maxFails {
		*s.failCount++
		s.mu.Unlock()
		return nil, objectstore.ErrPrecondition
	}
	s.mu.Unlock()

	return s.Store.PutIfMatch(ctx, key, body, size, etag, opts)
}
