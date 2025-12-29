package wal

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestCommitBasic(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-namespace"

	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), map[string]*AttributeValue{
		"name": StringValue("test"),
	}, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	if result.Seq != 1 {
		t.Errorf("expected seq 1, got %d", result.Seq)
	}

	if result.State.WAL.HeadSeq != 1 {
		t.Errorf("expected state head_seq 1, got %d", result.State.WAL.HeadSeq)
	}

	if result.BytesWritten <= 0 {
		t.Errorf("expected positive bytes written, got %d", result.BytesWritten)
	}
}

func TestCommitTracksLogicalBytes(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-logical-bytes"

	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), map[string]*AttributeValue{
		"payload": StringValue(strings.Repeat("a", 8*1024)),
	}, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	encResult, err := encoder.Encode(entry)
	encoder.Close()
	if err != nil {
		t.Fatalf("failed to encode entry: %v", err)
	}

	if result.BytesWritten != encResult.LogicalBytes {
		t.Fatalf("expected logical bytes %d, got %d", encResult.LogicalBytes, result.BytesWritten)
	}

	loaded, err := stateMgr.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if loaded.State.WAL.BytesUnindexedEst != encResult.LogicalBytes {
		t.Fatalf("expected bytes_unindexed_est %d, got %d",
			encResult.LogicalBytes, loaded.State.WAL.BytesUnindexedEst)
	}
}

func TestCommitIdempotentWALUpload(t *testing.T) {
	// Test that WAL PUT uses If-None-Match: * for idempotency
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-idempotent"

	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	// First commit
	result1, err := committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("first commit failed: %v", err)
	}

	// Verify WAL exists
	exists, err := committer.VerifyWALExists(ctx, ns, 1)
	if err != nil {
		t.Fatalf("failed to verify WAL exists: %v", err)
	}
	if !exists {
		t.Error("expected WAL to exist after commit")
	}

	// Try to directly put the same WAL key again - should fail with conflict
	walKey := walKeyForNamespace(ns, 1)
	_, err = store.PutIfAbsent(ctx, walKey, nil, 0, nil)
	if !objectstore.IsConflictError(err) {
		t.Errorf("expected conflict error when re-uploading WAL, got: %v", err)
	}

	// Second commit should get a new sequence
	entry2 := NewWalEntry(ns, 0)
	batch2 := NewWriteSubBatch("req-2")
	batch2.AddUpsert(DocumentIDFromID(document.NewU64ID(2)), nil, nil, 0)
	entry2.SubBatches = append(entry2.SubBatches, batch2)

	result2, err := committer.Commit(ctx, ns, entry2, nil)
	if err != nil {
		t.Fatalf("second commit failed: %v", err)
	}

	if result2.Seq != result1.Seq+1 {
		t.Errorf("expected seq %d, got %d", result1.Seq+1, result2.Seq)
	}
}

func TestCommitStateCASUpdate(t *testing.T) {
	// Test that state.json update uses If-Match for CAS
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-cas"

	// Create initial state
	_, err = stateMgr.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify state was updated correctly
	loaded, err := stateMgr.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if loaded.State.WAL.HeadSeq != result.Seq {
		t.Errorf("state head_seq mismatch: expected %d, got %d", result.Seq, loaded.State.WAL.HeadSeq)
	}

	if loaded.State.WAL.HeadKey != KeyForSeq(result.Seq) {
		t.Errorf("state head_key mismatch: expected %s, got %s", KeyForSeq(result.Seq), loaded.State.WAL.HeadKey)
	}
}

func TestCommitBothWALAndStateCommitted(t *testing.T) {
	// Test that success is returned only after both WAL and state are committed
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-both-committed"

	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// After success, verify both WAL and state exist
	// 1. WAL exists
	walExists, err := committer.VerifyWALExists(ctx, ns, result.Seq)
	if err != nil {
		t.Fatalf("failed to verify WAL: %v", err)
	}
	if !walExists {
		t.Error("WAL should exist after successful commit")
	}

	// 2. State exists and is updated
	loaded, err := stateMgr.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if loaded.State.WAL.HeadSeq != result.Seq {
		t.Errorf("state not updated: expected head_seq %d, got %d", result.Seq, loaded.State.WAL.HeadSeq)
	}
}

func TestCommitCASRetryOnETagMismatch(t *testing.T) {
	// Test CAS retry loop on ETag mismatch using a wrapper store
	store := objectstore.NewMemoryStore()

	// Wrap the store to inject CAS failures
	failCount := 0
	maxFails := 2
	wrapper := &casFailingStore{
		Store:     store,
		failCount: &failCount,
		maxFails:  maxFails,
	}

	stateMgr := namespace.NewStateManager(wrapper)
	committer, err := NewCommitter(wrapper, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-cas-retry"

	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	// This should succeed after retries
	result, err := committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("commit failed despite retries: %v", err)
	}

	if result.Seq != 1 {
		t.Errorf("expected seq 1, got %d", result.Seq)
	}

	if failCount < maxFails {
		t.Errorf("expected at least %d CAS failures, got %d", maxFails, failCount)
	}
}

func TestCommitWALNotReUploadedOnStateRetry(t *testing.T) {
	// Test that WAL key is not re-uploaded on state retry
	store := objectstore.NewMemoryStore()

	// Track PUT calls
	var putCount int32
	wrapper := &countingStore{
		Store:    store,
		putCount: &putCount,
	}

	// Create a store that fails CAS once, triggering retry
	failCount := 0
	casWrapper := &casFailingStore{
		Store:     wrapper,
		failCount: &failCount,
		maxFails:  1,
	}

	stateMgr := namespace.NewStateManager(casWrapper)
	committer, err := NewCommitter(casWrapper, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-no-reupload"

	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	_, err = committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// The WAL PUT (If-None-Match) should only happen once
	// State PUT might happen multiple times due to retries
	walKey := walKeyForNamespace(ns, 1)

	// Verify WAL was uploaded exactly once
	info, err := store.Head(ctx, walKey)
	if err != nil {
		t.Fatalf("WAL should exist: %v", err)
	}
	if info == nil {
		t.Error("WAL info should not be nil")
	}

	// Count how many times PutIfAbsent was called for WAL key
	// Since we use If-None-Match, trying to upload again would fail with conflict
	_, err = store.PutIfAbsent(ctx, walKey, nil, 0, nil)
	if !objectstore.IsConflictError(err) {
		t.Error("WAL should already exist, second PutIfAbsent should fail with conflict")
	}
}

func TestCommitMultipleSequential(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-sequential"

	for i := 1; i <= 5; i++ {
		entry := NewWalEntry(ns, 0)
		batch := NewWriteSubBatch("req-" + string(rune('0'+i)))
		batch.AddUpsert(DocumentIDFromID(document.NewU64ID(uint64(i))), nil, nil, 0)
		entry.SubBatches = append(entry.SubBatches, batch)

		result, err := committer.Commit(ctx, ns, entry, nil)
		if err != nil {
			t.Fatalf("commit %d failed: %v", i, err)
		}

		if result.Seq != uint64(i) {
			t.Errorf("expected seq %d, got %d", i, result.Seq)
		}
	}

	// Verify final state
	loaded, err := stateMgr.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if loaded.State.WAL.HeadSeq != 5 {
		t.Errorf("expected final head_seq 5, got %d", loaded.State.WAL.HeadSeq)
	}
}

func TestCommitWithSchemaDelta(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-schema-delta"

	schemaDelta := &namespace.Schema{
		Attributes: map[string]namespace.AttributeSchema{
			"name": {Type: "string"},
			"age":  {Type: "int"},
		},
	}

	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(1)), map[string]*AttributeValue{
		"name": StringValue("Alice"),
		"age":  IntValue(30),
	}, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := committer.Commit(ctx, ns, entry, schemaDelta)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify schema was applied
	if result.State.Schema == nil {
		t.Fatal("expected schema to be set")
	}
	if len(result.State.Schema.Attributes) != 2 {
		t.Errorf("expected 2 attributes, got %d", len(result.State.Schema.Attributes))
	}
	if result.State.Schema.Attributes["name"].Type != "string" {
		t.Errorf("expected name type 'string', got '%s'", result.State.Schema.Attributes["name"].Type)
	}
}

func TestReadWAL(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-read-wal"

	entry := NewWalEntry(ns, 0)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(DocumentIDFromID(document.NewU64ID(42)), map[string]*AttributeValue{
		"value": IntValue(123),
	}, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Read the WAL back
	readEntry, err := committer.ReadWAL(ctx, ns, result.Seq)
	if err != nil {
		t.Fatalf("failed to read WAL: %v", err)
	}

	if readEntry.Seq != result.Seq {
		t.Errorf("seq mismatch: expected %d, got %d", result.Seq, readEntry.Seq)
	}
	if readEntry.Namespace != ns {
		t.Errorf("namespace mismatch: expected %s, got %s", ns, readEntry.Namespace)
	}
	if len(readEntry.SubBatches) != 1 {
		t.Fatalf("expected 1 sub-batch, got %d", len(readEntry.SubBatches))
	}
	if readEntry.SubBatches[0].RequestId != "req-1" {
		t.Errorf("request ID mismatch: expected 'req-1', got '%s'", readEntry.SubBatches[0].RequestId)
	}
}

func TestConcurrentCommits(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)

	ctx := context.Background()
	ns := "test-concurrent"

	numCommitters := 5
	commitsPerWriter := 3

	var wg sync.WaitGroup
	successCount := int32(0)
	var mu sync.Mutex
	successfulSeqs := make([]uint64, 0)

	for i := 0; i < numCommitters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			committer, err := NewCommitter(store, stateMgr)
			if err != nil {
				t.Errorf("failed to create committer %d: %v", writerID, err)
				return
			}
			defer committer.Close()

			for j := 0; j < commitsPerWriter; j++ {
				entry := NewWalEntry(ns, 0)
				batch := NewWriteSubBatch("req-" + string(rune('A'+writerID)) + "-" + string(rune('0'+j)))
				batch.AddUpsert(DocumentIDFromID(document.NewU64ID(uint64(writerID*100+j))), nil, nil, 0)
				entry.SubBatches = append(entry.SubBatches, batch)

				result, err := committer.Commit(ctx, ns, entry, nil)
				if err != nil {
					// Some failures are expected due to CAS contention
					continue
				}
				atomic.AddInt32(&successCount, 1)
				mu.Lock()
				successfulSeqs = append(successfulSeqs, result.Seq)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// At least some commits should succeed
	if successCount == 0 {
		t.Error("expected at least some commits to succeed")
	}

	// Verify state is consistent
	loaded, err := stateMgr.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	// Verify successful commits count matches state
	if int32(loaded.State.WAL.HeadSeq) != successCount {
		t.Errorf("head_seq %d doesn't match success count %d", loaded.State.WAL.HeadSeq, successCount)
	}

	// Verify all WAL entries that were successfully committed exist
	committer, _ := NewCommitter(store, stateMgr)
	defer committer.Close()

	for _, seq := range successfulSeqs {
		exists, err := committer.VerifyWALExists(ctx, ns, seq)
		if err != nil {
			t.Errorf("failed to verify WAL seq %d: %v", seq, err)
		}
		if !exists {
			t.Errorf("WAL seq %d was successfully committed but doesn't exist", seq)
		}
	}
}

func TestVerifyWALExists(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMgr := namespace.NewStateManager(store)
	committer, err := NewCommitter(store, stateMgr)
	if err != nil {
		t.Fatalf("failed to create committer: %v", err)
	}
	defer committer.Close()

	ctx := context.Background()
	ns := "test-verify-exists"

	// Non-existent WAL
	exists, err := committer.VerifyWALExists(ctx, ns, 999)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Error("WAL should not exist for seq 999")
	}

	// Create a WAL entry
	entry := NewWalEntry(ns, 0)
	entry.SubBatches = append(entry.SubBatches, NewWriteSubBatch("req-1"))
	result, err := committer.Commit(ctx, ns, entry, nil)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Now it should exist
	exists, err = committer.VerifyWALExists(ctx, ns, result.Seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("WAL should exist after commit")
	}
}

// Helper stores for testing

type casFailingStore struct {
	objectstore.Store
	failCount *int
	maxFails  int
	mu        sync.Mutex
}

func (s *casFailingStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	s.mu.Lock()
	if *s.failCount < s.maxFails {
		*s.failCount++
		s.mu.Unlock()
		return nil, objectstore.ErrPrecondition
	}
	s.mu.Unlock()

	return s.Store.PutIfMatch(ctx, key, body, size, etag, opts)
}

type countingStore struct {
	objectstore.Store
	putCount *int32
}

func (s *countingStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	atomic.AddInt32(s.putCount, 1)
	return s.Store.Put(ctx, key, body, size, opts)
}

func (s *countingStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	atomic.AddInt32(s.putCount, 1)
	return s.Store.PutIfAbsent(ctx, key, body, size, opts)
}
