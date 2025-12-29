package simulation

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// makeWalEntry creates a simple WAL entry for testing.
func makeWalEntry(requestID string, docID string) *wal.WalEntry {
	return &wal.WalEntry{
		FormatVersion: uint32(wal.FormatVersion),
		SubBatches: []*wal.WriteSubBatch{
			{
				RequestId: requestID,
				Mutations: []*wal.Mutation{
					{
						Type: wal.MutationType_MUTATION_TYPE_UPSERT,
						Id: &wal.DocumentID{
							Id: &wal.DocumentID_Str{Str: docID},
						},
					},
				},
			},
		},
	}
}

// TestConcurrentWritesWithCASRetries tests that concurrent writes
// correctly handle CAS retries when multiple writers compete.
func TestConcurrentWritesWithCASRetries(t *testing.T) {
	ctx := context.Background()
	clock := NewSimClock()
	memStore := NewMemoryStoreWithLog()
	store := NewDeterministicStore(memStore, clock)

	stateManager := namespace.NewStateManager(store)

	// Create initial namespace state
	_, err := stateManager.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Track successful commits
	var successCount atomic.Int32
	var casRetries atomic.Int32
	var errs []error
	var errsMu sync.Mutex

	// Number of concurrent writers
	numWriters := 5
	var wg sync.WaitGroup

	// Create committers for each writer
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			committer, cerr := wal.NewCommitter(store, stateManager)
			if cerr != nil {
				errsMu.Lock()
				errs = append(errs, fmt.Errorf("writer %d: failed to create committer: %w", writerID, cerr))
				errsMu.Unlock()
				return
			}
			defer committer.Close()

			entry := makeWalEntry(fmt.Sprintf("req-%d", writerID), fmt.Sprintf("doc-%d", writerID))

			result, cerr := committer.Commit(ctx, "test-ns", entry, nil)
			if cerr != nil {
				// CAS retry errors are expected for some writers
				if !stderrors.Is(cerr, wal.ErrWALSeqConflict) && !stderrors.Is(cerr, wal.ErrStateUpdateFailed) {
					errsMu.Lock()
					errs = append(errs, fmt.Errorf("writer %d: unexpected error: %w", writerID, cerr))
					errsMu.Unlock()
				}
				casRetries.Add(1)
				return
			}

			successCount.Add(1)
			t.Logf("Writer %d committed seq %d", writerID, result.Seq)
		}(i)
	}

	wg.Wait()

	// Verify results
	if successCount.Load() == 0 {
		t.Error("expected at least one successful commit")
	}
	if len(errs) > 0 {
		for _, e := range errs {
			t.Error(e)
		}
	}

	// Verify WAL monotonicity by loading final state
	loaded, err := stateManager.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to load final state: %v", err)
	}

	expectedSeq := uint64(successCount.Load())
	if loaded.State.WAL.HeadSeq != expectedSeq {
		t.Errorf("expected head_seq=%d, got %d", expectedSeq, loaded.State.WAL.HeadSeq)
	}

	t.Logf("Successful commits: %d, CAS retries: %d, Final head_seq: %d",
		successCount.Load(), casRetries.Load(), loaded.State.WAL.HeadSeq)
}

// TestNodeCrashBetweenWALUploadAndStateUpdate simulates a node crash
// that occurs after WAL upload succeeds but before state.json is updated.
func TestNodeCrashBetweenWALUploadAndStateUpdate(t *testing.T) {
	ctx := context.Background()
	clock := NewSimClock()
	memStore := NewMemoryStoreWithLog()
	store := NewDeterministicStore(memStore, clock)

	stateManager := namespace.NewStateManager(store)

	// Create initial namespace
	_, err := stateManager.Create(ctx, "crash-test")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Add hook to fail the state.json CAS update (simulating crash after WAL upload)
	crashErr := stderrors.New("simulated crash")
	store.AddHook(&OpHook{
		Op:      "PutIfMatch",
		Key:     "*state.json",
		OneShot: true, // Only trigger once
		Fault: &FaultConfig{
			Type:  FaultError,
			Error: crashErr,
		},
	})

	committer, err := wal.NewCommitter(store, stateManager)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	entry := makeWalEntry("crash-req", "doc-crash")

	// This commit should fail due to "crash"
	_, err = committer.Commit(ctx, "crash-test", entry, nil)
	if err == nil {
		t.Fatal("expected commit to fail due to crash")
	}
	t.Logf("Commit failed as expected: %v", err)

	// WAL entry should exist (upload succeeded before crash)
	walKey := fmt.Sprintf("vex/namespaces/crash-test/%s", wal.KeyForSeq(1))
	exists, err := verifyKeyExists(ctx, store, walKey)
	if err != nil {
		t.Fatalf("failed to check WAL key: %v", err)
	}
	if !exists {
		t.Error("WAL entry should exist after crash")
	}

	// But state.json should NOT be updated (crash was before state update)
	loaded, err := stateManager.Load(ctx, "crash-test")
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if loaded.State.WAL.HeadSeq != 0 {
		t.Errorf("expected head_seq=0 (state not updated), got %d", loaded.State.WAL.HeadSeq)
	}

	t.Log("Verified: WAL uploaded but state not updated due to crash")

	// Recovery process should detect and repair the inconsistency
	// by advancing state to match existing WAL entries
	repairer := wal.NewRepairer(store, stateManager)
	result, err := repairer.Repair(ctx, "crash-test")
	if err != nil {
		t.Fatalf("repair failed: %v", err)
	}

	t.Logf("Repair result: WALsRepaired=%d, highestSeq=%d", result.WALsRepaired, result.HighestContiguousSeq)

	// Verify state is now consistent
	loaded, err = stateManager.Load(ctx, "crash-test")
	if err != nil {
		t.Fatalf("failed to load repaired state: %v", err)
	}
	if loaded.State.WAL.HeadSeq != 1 {
		t.Errorf("expected repaired head_seq=1, got %d", loaded.State.WAL.HeadSeq)
	}
}

// TestIndexerCrashMidBuild simulates an indexer crash during index build.
func TestIndexerCrashMidBuild(t *testing.T) {
	ctx := context.Background()
	clock := NewSimClock()
	memStore := NewMemoryStoreWithLog()
	store := NewDeterministicStore(memStore, clock)

	stateManager := namespace.NewStateManager(store)

	// Create namespace with some WAL entries
	_, err := stateManager.Create(ctx, "indexer-crash-test")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Commit some WAL entries
	committer, err := wal.NewCommitter(store, stateManager)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}

	for i := 1; i <= 3; i++ {
		entry := makeWalEntry(fmt.Sprintf("req-%d", i), fmt.Sprintf("doc-%d", i))
		_, err = committer.Commit(ctx, "indexer-crash-test", entry, nil)
		if err != nil {
			t.Fatalf("failed to commit entry %d: %v", i, err)
		}
	}
	committer.Close()

	// Verify initial state
	loaded, err := stateManager.Load(ctx, "indexer-crash-test")
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if loaded.State.WAL.HeadSeq != 3 {
		t.Fatalf("expected head_seq=3, got %d", loaded.State.WAL.HeadSeq)
	}
	if loaded.State.Index.IndexedWALSeq != 0 {
		t.Fatalf("expected indexed_wal_seq=0, got %d", loaded.State.Index.IndexedWALSeq)
	}

	// Simulate indexer starting and crashing mid-build
	// It would write segment objects but crash before updating state

	// Write orphan segment object (simulating partial index build)
	orphanKey := "vex/namespaces/indexer-crash-test/index/segments/orphan-segment/docs.bin"
	_, err = memStore.Put(ctx, orphanKey, bytes.NewReader([]byte("orphan data")), 11, nil)
	if err != nil {
		t.Fatalf("failed to write orphan segment: %v", err)
	}

	// Verify orphan exists
	exists, err := verifyKeyExists(ctx, memStore, orphanKey)
	if err != nil {
		t.Fatalf("failed to check orphan: %v", err)
	}
	if !exists {
		t.Error("orphan segment should exist")
	}

	// State should still show indexed_wal_seq=0
	loaded, err = stateManager.Load(ctx, "indexer-crash-test")
	if err != nil {
		t.Fatalf("failed to reload state: %v", err)
	}
	if loaded.State.Index.IndexedWALSeq != 0 {
		t.Errorf("indexed_wal_seq should still be 0 after indexer crash, got %d",
			loaded.State.Index.IndexedWALSeq)
	}

	// Orphan objects should be safe to GC (not referenced by any manifest)
	t.Log("Indexer crash left orphan segment - GC can clean it up safely")
}

// TestMonotonicWALSeqInvariant verifies that WAL sequences are always monotonic.
func TestMonotonicWALSeqInvariant(t *testing.T) {
	ctx := context.Background()
	clock := NewSimClock()
	memStore := NewMemoryStoreWithLog()
	store := NewDeterministicStore(memStore, clock)

	stateManager := namespace.NewStateManager(store)

	_, err := stateManager.Create(ctx, "monotonic-test")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	committer, err := wal.NewCommitter(store, stateManager)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	// Track committed sequences
	var committedSeqs []uint64
	var seqMu sync.Mutex

	// Commit multiple entries sequentially
	for i := 1; i <= 10; i++ {
		entry := makeWalEntry(fmt.Sprintf("req-%d", i), fmt.Sprintf("doc-%d", i))

		result, err := committer.Commit(ctx, "monotonic-test", entry, nil)
		if err != nil {
			t.Fatalf("failed to commit entry %d: %v", i, err)
		}

		seqMu.Lock()
		committedSeqs = append(committedSeqs, result.Seq)
		seqMu.Unlock()
	}

	// Verify monotonicity
	for i := 1; i < len(committedSeqs); i++ {
		if committedSeqs[i] != committedSeqs[i-1]+1 {
			t.Errorf("WAL seq not monotonic: seq[%d]=%d, seq[%d]=%d",
				i-1, committedSeqs[i-1], i, committedSeqs[i])
		}
	}

	// Verify final state
	loaded, err := stateManager.Load(ctx, "monotonic-test")
	if err != nil {
		t.Fatalf("failed to load final state: %v", err)
	}
	if loaded.State.WAL.HeadSeq != 10 {
		t.Errorf("expected final head_seq=10, got %d", loaded.State.WAL.HeadSeq)
	}

	// Try to directly update state with non-monotonic seq (should fail)
	_, err = stateManager.Update(ctx, "monotonic-test", loaded.ETag, func(state *namespace.State) error {
		state.WAL.HeadSeq = 15 // Jump from 10 to 15 (not +1)
		return nil
	})
	if err == nil {
		t.Error("expected error for non-monotonic seq update")
	}
	if !stderrors.Is(err, namespace.ErrWALSeqNotMonotonic) {
		t.Errorf("expected ErrWALSeqNotMonotonic, got %v", err)
	}

	t.Log("Monotonic WAL seq invariant verified")
}

// TestSnapshotCorrectnessForStrongReads verifies that strong reads
// see a consistent snapshot of all committed WAL entries.
func TestSnapshotCorrectnessForStrongReads(t *testing.T) {
	ctx := context.Background()
	clock := NewSimClock()
	memStore := NewMemoryStoreWithLog()
	store := NewDeterministicStore(memStore, clock)

	stateManager := namespace.NewStateManager(store)

	_, err := stateManager.Create(ctx, "snapshot-test")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	committer, err := wal.NewCommitter(store, stateManager)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	// Commit some entries
	docIDs := []string{"doc-a", "doc-b", "doc-c"}
	for i, docID := range docIDs {
		entry := makeWalEntry(fmt.Sprintf("req-%d", i), docID)
		_, err = committer.Commit(ctx, "snapshot-test", entry, nil)
		if err != nil {
			t.Fatalf("failed to commit %s: %v", docID, err)
		}
	}

	// Load state to get current head_seq
	loaded, err := stateManager.Load(ctx, "snapshot-test")
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	snapshotSeq := loaded.State.WAL.HeadSeq

	// Verify all WAL entries up to snapshot_seq exist
	for seq := uint64(1); seq <= snapshotSeq; seq++ {
		walKey := fmt.Sprintf("vex/namespaces/snapshot-test/%s", wal.KeyForSeq(seq))
		exists, err := verifyKeyExists(ctx, store, walKey)
		if err != nil {
			t.Fatalf("failed to check WAL %d: %v", seq, err)
		}
		if !exists {
			t.Errorf("WAL entry %d missing from snapshot (head_seq=%d)", seq, snapshotSeq)
		}
	}

	// Read and decode all WAL entries to verify consistency
	decoder, err := wal.NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	var foundDocs []string
	for seq := uint64(1); seq <= snapshotSeq; seq++ {
		walKey := fmt.Sprintf("vex/namespaces/snapshot-test/%s", wal.KeyForSeq(seq))
		reader, _, err := store.Get(ctx, walKey, nil)
		if err != nil {
			t.Fatalf("failed to read WAL %d: %v", seq, err)
		}

		data, _ := readAll(reader)
		reader.Close()

		entry, err := decoder.Decode(data)
		if err != nil {
			t.Fatalf("failed to decode WAL %d: %v", seq, err)
		}

		for _, batch := range entry.SubBatches {
			for _, mut := range batch.Mutations {
				if strID, ok := mut.Id.Id.(*wal.DocumentID_Str); ok {
					foundDocs = append(foundDocs, strID.Str)
				}
			}
		}
	}

	// Verify all expected docs are in snapshot
	if len(foundDocs) != len(docIDs) {
		t.Errorf("expected %d docs in snapshot, found %d", len(docIDs), len(foundDocs))
	}

	t.Logf("Snapshot at seq=%d contains %d docs: %v", snapshotSeq, len(foundDocs), foundDocs)
}

// TestCASRetryWithInterleavedWriters tests interleaved CAS updates.
func TestCASRetryWithInterleavedWriters(t *testing.T) {
	ctx := context.Background()
	clock := NewSimClock()
	memStore := NewMemoryStoreWithLog()
	store := NewDeterministicStore(memStore, clock)
	scheduler := NewScheduler(clock)

	stateManager := namespace.NewStateManager(store)

	_, err := stateManager.Create(ctx, "interleaved-test")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Use scheduler to control interleaving
	scheduler.SetMode(ModeStep)

	results := make(chan *wal.CommitResult, 2)
	errors := make(chan error, 2)

	// Submit two writers
	task1 := scheduler.Submit("writer-1", func(ctx context.Context) (any, error) {
		committer, err := wal.NewCommitter(store, stateManager)
		if err != nil {
			return nil, err
		}
		defer committer.Close()

		entry := makeWalEntry("req-1", "doc-1")
		return committer.Commit(ctx, "interleaved-test", entry, nil)
	})

	task2 := scheduler.Submit("writer-2", func(ctx context.Context) (any, error) {
		committer, err := wal.NewCommitter(store, stateManager)
		if err != nil {
			return nil, err
		}
		defer committer.Close()

		entry := makeWalEntry("req-2", "doc-2")
		return committer.Commit(ctx, "interleaved-test", entry, nil)
	})

	// Let both tasks run
	scheduler.Step(task1.ID)
	scheduler.Step(task2.ID)

	// Wait for completion
	task1.Wait(10 * time.Second)
	task2.Wait(10 * time.Second)

	// Collect results
	if task1.Error() != nil {
		errors <- task1.Error()
	} else if result, ok := task1.Result().(*wal.CommitResult); ok {
		results <- result
	}

	if task2.Error() != nil {
		errors <- task2.Error()
	} else if result, ok := task2.Result().(*wal.CommitResult); ok {
		results <- result
	}

	close(results)
	close(errors)

	// Count successes and failures
	var successCount int
	for result := range results {
		t.Logf("Successful commit: seq=%d", result.Seq)
		successCount++
	}

	var errorCount int
	for err := range errors {
		t.Logf("Failed commit: %v", err)
		errorCount++
	}

	// At least one should succeed
	if successCount == 0 {
		t.Error("expected at least one successful commit")
	}

	// Verify final state is consistent
	loaded, err := stateManager.Load(ctx, "interleaved-test")
	if err != nil {
		t.Fatalf("failed to load final state: %v", err)
	}
	t.Logf("Final state: head_seq=%d, successes=%d, failures=%d",
		loaded.State.WAL.HeadSeq, successCount, errorCount)
}

// TestDeterministicStoreWithFaultInjection tests fault injection capabilities.
func TestDeterministicStoreWithFaultInjection(t *testing.T) {
	ctx := context.Background()
	clock := NewSimClock()
	memStore := NewMemoryStoreWithLog()
	store := NewDeterministicStore(memStore, clock)

	stateManager := namespace.NewStateManager(store)

	// Create namespace first
	_, err := stateManager.Create(ctx, "fault-test")
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Now add hook to inject error on PutIfMatch (for state updates)
	injectedErr := stderrors.New("injected error")
	store.AddHook(&OpHook{
		Op:      "PutIfMatch",
		Key:     "*state.json",
		OneShot: true,
		Fault: &FaultConfig{
			Type:  FaultError,
			Error: injectedErr,
		},
	})

	// Reload the state with fresh ETag
	loaded, err := stateManager.Load(ctx, "fault-test")
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	// Update should fail due to injected error
	_, err = stateManager.Update(ctx, "fault-test", loaded.ETag, func(state *namespace.State) error {
		state.WAL.HeadSeq = 1
		state.WAL.HeadKey = "wal/00000000000000000001.wal.zst"
		return nil
	})
	if err == nil {
		t.Error("expected error from fault injection")
	}

	// Verify operation was recorded
	history := store.GetHistory()
	var foundFaultedOp bool
	for _, op := range history {
		if op.Op == "PutIfMatch" && op.Error != nil {
			foundFaultedOp = true
			t.Logf("Found faulted operation: %s on %s", op.Op, op.Key)
			break
		}
	}
	if !foundFaultedOp {
		t.Error("expected to find faulted PutIfMatch operation in history")
	}

	t.Log("Fault injection working correctly")
}

// TestIndexedWALSeqNeverExceedsHeadSeq verifies the invariant that
// indexed_wal_seq can never exceed wal.head_seq.
func TestIndexedWALSeqNeverExceedsHeadSeq(t *testing.T) {
	ctx := context.Background()
	clock := NewSimClock()
	memStore := NewMemoryStoreWithLog()
	store := NewDeterministicStore(memStore, clock)

	stateManager := namespace.NewStateManager(store)

	loaded, err := stateManager.Create(ctx, "invariant-test")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Try to set indexed_wal_seq > head_seq (should fail)
	_, err = stateManager.Update(ctx, "invariant-test", loaded.ETag, func(state *namespace.State) error {
		state.Index.IndexedWALSeq = 10 // head_seq is still 0
		return nil
	})

	if err == nil {
		t.Error("expected error when setting indexed_wal_seq > head_seq")
	}
	if !stderrors.Is(err, namespace.ErrIndexSeqExceedsWAL) {
		t.Errorf("expected ErrIndexSeqExceedsWAL, got %v", err)
	}

	// Commit some WAL entries
	committer, err := wal.NewCommitter(store, stateManager)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	for i := 1; i <= 5; i++ {
		entry := makeWalEntry(fmt.Sprintf("req-%d", i), fmt.Sprintf("doc-%d", i))
		_, err = committer.Commit(ctx, "invariant-test", entry, nil)
		if err != nil {
			t.Fatalf("failed to commit entry %d: %v", i, err)
		}
	}

	// Now indexed_wal_seq can be advanced up to head_seq (5)
	loaded, err = stateManager.Load(ctx, "invariant-test")
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	// Advance to valid value (should work)
	_, err = stateManager.AdvanceIndex(ctx, "invariant-test", loaded.ETag,
		"manifest/00000001.json", 1, 3, 0)
	if err != nil {
		t.Errorf("expected successful index advance to seq=3, got %v", err)
	}

	// Try to advance beyond head_seq (should fail)
	loaded, err = stateManager.Load(ctx, "invariant-test")
	if err != nil {
		t.Fatalf("failed to reload state: %v", err)
	}

	_, err = stateManager.AdvanceIndex(ctx, "invariant-test", loaded.ETag,
		"manifest/00000002.json", 2, 10, 0) // 10 > 5
	if err == nil {
		t.Error("expected error when advancing indexed_wal_seq beyond head_seq")
	}

	t.Log("Invariant indexed_wal_seq <= head_seq verified")
}

// Helper functions

func verifyKeyExists(ctx context.Context, store objectstore.Store, key string) (bool, error) {
	_, err := store.Head(ctx, key)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func readAll(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r.(interface{ Read([]byte) (int, error) }))
	return buf.Bytes(), err
}
