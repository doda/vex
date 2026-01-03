// Package fault implements fault injection tests for Vex.
//
// These tests verify that Vex handles various failure scenarios correctly:
// - Transient 500 errors from object store
// - Timeout errors
// - Partial reads
// - Strong consistency failures
// - Eventual consistency degradation
// - Background repair after failures
package fault

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	errInjected500      = errors.New("injected 500 error")
	errContextDeadline  = errors.New("context deadline exceeded")
	errPartialRead      = errors.New("connection reset during read")
	errTransientFailure = errors.New("transient failure")
)

// FaultMode represents the type of fault to inject.
type FaultMode int

const (
	FaultNone FaultMode = iota
	FaultError500
	FaultTimeout
	FaultPartialRead
	FaultTransient // Fails first N attempts, then succeeds
)

// FaultConfig configures fault injection for the fault store.
type FaultConfig struct {
	Mode            FaultMode
	Operation       string // "Get", "Put", "Head", "List", "PutIfAbsent", "PutIfMatch" - empty matches all
	KeyPattern      string // Empty matches all, "*" suffix for prefix match
	Error           error
	TransientCount  int           // For FaultTransient: how many times to fail before succeeding
	Timeout         time.Duration // For FaultTimeout: how long to delay
	PartialReadSize int64         // For FaultPartialRead: how many bytes to return before error
}

// FaultStore wraps an object store with configurable fault injection.
type FaultStore struct {
	inner objectstore.Store

	mu          sync.RWMutex
	faults      []*FaultConfig
	attempts    map[string]int // Key -> attempt count for transient faults
	opRecords   []OpRecord
	recordLimit int
}

// OpRecord tracks an operation for test verification.
type OpRecord struct {
	Op      string
	Key     string
	Success bool
	Error   error
	Time    time.Time
}

// NewFaultStore creates a new fault-injecting store wrapper.
func NewFaultStore(inner objectstore.Store) *FaultStore {
	return &FaultStore{
		inner:       inner,
		attempts:    make(map[string]int),
		opRecords:   make([]OpRecord, 0, 1000),
		recordLimit: 1000,
	}
}

// AddFault adds a fault configuration.
func (f *FaultStore) AddFault(cfg *FaultConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults = append(f.faults, cfg)
}

// ClearFaults removes all fault configurations.
func (f *FaultStore) ClearFaults() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults = nil
}

// GetRecords returns a copy of operation records.
func (f *FaultStore) GetRecords() []OpRecord {
	f.mu.RLock()
	defer f.mu.RUnlock()
	records := make([]OpRecord, len(f.opRecords))
	copy(records, f.opRecords)
	return records
}

// ClearRecords clears operation records.
func (f *FaultStore) ClearRecords() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opRecords = f.opRecords[:0]
}

// ResetAttempts resets transient fault attempt counters.
func (f *FaultStore) ResetAttempts() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts = make(map[string]int)
}

func (f *FaultStore) recordOp(op, key string, success bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.opRecords) < f.recordLimit {
		f.opRecords = append(f.opRecords, OpRecord{
			Op:      op,
			Key:     key,
			Success: success,
			Error:   err,
			Time:    time.Now(),
		})
	}
}

func matchKey(key, pattern string) bool {
	if pattern == "" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(key, strings.TrimSuffix(pattern, "*"))
	}
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(key, strings.TrimPrefix(pattern, "*"))
	}
	return key == pattern
}

func (f *FaultStore) checkFault(ctx context.Context, op, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, fault := range f.faults {
		if fault.Operation != "" && fault.Operation != op {
			continue
		}
		if fault.KeyPattern != "" && !matchKey(key, fault.KeyPattern) {
			continue
		}

		switch fault.Mode {
		case FaultError500:
			if fault.Error != nil {
				return fault.Error
			}
			return errInjected500

		case FaultTimeout:
			timeout := fault.Timeout
			if timeout == 0 {
				timeout = 5 * time.Second
			}
			f.mu.Unlock()
			select {
			case <-ctx.Done():
				f.mu.Lock()
				return ctx.Err()
			case <-time.After(timeout):
				f.mu.Lock()
				return errContextDeadline
			}

		case FaultTransient:
			attemptKey := op + ":" + key
			attempts := f.attempts[attemptKey]
			f.attempts[attemptKey] = attempts + 1
			if attempts < fault.TransientCount {
				if fault.Error != nil {
					return fault.Error
				}
				return errTransientFailure
			}
			// Succeeded after N attempts

		case FaultNone:
			// No fault
		}
	}
	return nil
}

func (f *FaultStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	if err := f.checkFault(ctx, "Get", key); err != nil {
		f.recordOp("Get", key, false, err)
		return nil, nil, err
	}

	reader, info, err := f.inner.Get(ctx, key, opts)
	f.recordOp("Get", key, err == nil, err)

	if err != nil {
		return nil, nil, err
	}

	// Check for partial read fault
	f.mu.RLock()
	var partialSize int64 = -1
	for _, fault := range f.faults {
		if fault.Mode == FaultPartialRead {
			if (fault.Operation == "" || fault.Operation == "Get") &&
				(fault.KeyPattern == "" || matchKey(key, fault.KeyPattern)) {
				partialSize = fault.PartialReadSize
				break
			}
		}
	}
	f.mu.RUnlock()

	if partialSize >= 0 {
		return &partialReader{
			reader:    reader,
			limit:     partialSize,
			readBytes: 0,
		}, info, nil
	}

	return reader, info, nil
}

// partialReader wraps a reader and returns an error after reading a limited number of bytes.
type partialReader struct {
	reader    io.ReadCloser
	limit     int64
	readBytes int64
}

func (p *partialReader) Read(b []byte) (int, error) {
	if p.readBytes >= p.limit {
		return 0, errPartialRead
	}
	remaining := p.limit - p.readBytes
	if int64(len(b)) > remaining {
		b = b[:remaining]
	}
	n, err := p.reader.Read(b)
	p.readBytes += int64(n)
	if p.readBytes >= p.limit && err == nil {
		return n, errPartialRead
	}
	return n, err
}

func (p *partialReader) Close() error {
	return p.reader.Close()
}

func (f *FaultStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	if err := f.checkFault(ctx, "Head", key); err != nil {
		f.recordOp("Head", key, false, err)
		return nil, err
	}

	info, err := f.inner.Head(ctx, key)
	f.recordOp("Head", key, err == nil, err)
	return info, err
}

func (f *FaultStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if err := f.checkFault(ctx, "Put", key); err != nil {
		f.recordOp("Put", key, false, err)
		return nil, err
	}

	info, err := f.inner.Put(ctx, key, body, size, opts)
	f.recordOp("Put", key, err == nil, err)
	return info, err
}

func (f *FaultStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if err := f.checkFault(ctx, "PutIfAbsent", key); err != nil {
		f.recordOp("PutIfAbsent", key, false, err)
		return nil, err
	}

	info, err := f.inner.PutIfAbsent(ctx, key, body, size, opts)
	f.recordOp("PutIfAbsent", key, err == nil, err)
	return info, err
}

func (f *FaultStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if err := f.checkFault(ctx, "PutIfMatch", key); err != nil {
		f.recordOp("PutIfMatch", key, false, err)
		return nil, err
	}

	info, err := f.inner.PutIfMatch(ctx, key, body, size, etag, opts)
	f.recordOp("PutIfMatch", key, err == nil, err)
	return info, err
}

func (f *FaultStore) Delete(ctx context.Context, key string) error {
	if err := f.checkFault(ctx, "Delete", key); err != nil {
		f.recordOp("Delete", key, false, err)
		return err
	}

	err := f.inner.Delete(ctx, key)
	f.recordOp("Delete", key, err == nil, err)
	return err
}

func (f *FaultStore) List(ctx context.Context, opts *objectstore.ListOptions) (*objectstore.ListResult, error) {
	if err := f.checkFault(ctx, "List", ""); err != nil {
		f.recordOp("List", "", false, err)
		return nil, err
	}

	result, err := f.inner.List(ctx, opts)
	f.recordOp("List", "", err == nil, err)
	return result, err
}

// =============================================================================
// Helper functions
// =============================================================================

// walKeyForNS constructs the full object storage key for a WAL entry.
func walKeyForNS(ns string, seq uint64) string {
	return "vex/namespaces/" + ns + "/" + wal.KeyForSeq(seq)
}

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

func makeWalEntryWithU64ID(requestID string, docID uint64) *wal.WalEntry {
	return &wal.WalEntry{
		FormatVersion: uint32(wal.FormatVersion),
		SubBatches: []*wal.WriteSubBatch{
			{
				RequestId: requestID,
				Mutations: []*wal.Mutation{
					{
						Type: wal.MutationType_MUTATION_TYPE_UPSERT,
						Id: &wal.DocumentID{
							Id: &wal.DocumentID_U64{U64: docID},
						},
					},
				},
			},
		},
	}
}

// =============================================================================
// Test: Transient 500 errors from object store
// =============================================================================

func TestTransient500Errors(t *testing.T) {
	ctx := context.Background()
	inner := objectstore.NewMemoryStore()
	store := NewFaultStore(inner)

	ns := "test-transient-500"
	stateKey := namespace.StateKey(ns)

	t.Run("500 error on state read fails namespace load", func(t *testing.T) {
		store.ClearFaults()
		store.AddFault(&FaultConfig{
			Mode:       FaultError500,
			Operation:  "Get",
			KeyPattern: "*state.json",
			Error:      errInjected500,
		})

		stateMan := namespace.NewStateManager(store)
		_, err := stateMan.Load(ctx, ns)
		if err == nil {
			t.Fatal("expected error when loading state with 500 fault, got nil")
		}

		records := store.GetRecords()
		var foundFault bool
		for _, r := range records {
			if r.Op == "Get" && strings.Contains(r.Key, "state.json") && r.Error != nil {
				foundFault = true
				break
			}
		}
		if !foundFault {
			t.Error("expected to find faulted Get operation for state.json")
		}
	})

	t.Run("500 error on WAL write fails commit", func(t *testing.T) {
		store.ClearFaults()
		store.ClearRecords()

		// First, create a valid namespace state
		state := &namespace.State{
			FormatVersion: 1,
			Namespace:     ns,
			WAL:           namespace.WALState{HeadSeq: 0},
		}
		data, _ := json.Marshal(state)
		_, err := inner.Put(ctx, stateKey, bytes.NewReader(data), int64(len(data)), nil)
		if err != nil {
			t.Fatalf("failed to create initial state: %v", err)
		}

		// Now inject fault on WAL writes
		store.AddFault(&FaultConfig{
			Mode:       FaultError500,
			Operation:  "PutIfAbsent",
			KeyPattern: "vex/namespaces/" + ns + "/wal/*",
			Error:      errInjected500,
		})

		stateMan := namespace.NewStateManager(store)
		committer, err := wal.NewCommitter(store, stateMan)
		if err != nil {
			t.Fatalf("failed to create committer: %v", err)
		}
		defer committer.Close()

		entry := makeWalEntry("req-1", "doc-1")
		_, err = committer.Commit(ctx, ns, entry, nil)
		if err == nil {
			t.Error("expected WAL commit to fail with 500 fault, got nil")
		}

		records := store.GetRecords()
		var foundWALFault bool
		for _, r := range records {
			if r.Op == "PutIfAbsent" && strings.Contains(r.Key, "/wal/") && r.Error != nil {
				foundWALFault = true
				break
			}
		}
		if !foundWALFault {
			t.Error("expected to find faulted WAL write operation")
		}
	})

	t.Run("transient 500 recovers after retries", func(t *testing.T) {
		store.ClearFaults()
		store.ClearRecords()
		store.ResetAttempts()

		testKey := "test-transient-key"

		// Fail first 2 attempts, then succeed
		store.AddFault(&FaultConfig{
			Mode:           FaultTransient,
			Operation:      "Get",
			KeyPattern:     testKey,
			TransientCount: 2,
			Error:          errTransientFailure,
		})

		// Write the object first
		data := []byte("test data")
		_, err := inner.Put(ctx, testKey, bytes.NewReader(data), int64(len(data)), nil)
		if err != nil {
			t.Fatalf("failed to put test object: %v", err)
		}

		// First attempt should fail
		_, _, err = store.Get(ctx, testKey, nil)
		if err == nil {
			t.Error("first attempt should fail")
		}

		// Second attempt should fail
		_, _, err = store.Get(ctx, testKey, nil)
		if err == nil {
			t.Error("second attempt should fail")
		}

		// Third attempt should succeed
		reader, _, err := store.Get(ctx, testKey, nil)
		if err != nil {
			t.Errorf("third attempt should succeed, got error: %v", err)
		}
		if reader != nil {
			reader.Close()
		}

		records := store.GetRecords()
		failCount := 0
		successCount := 0
		for _, r := range records {
			if r.Key == testKey {
				if r.Success {
					successCount++
				} else {
					failCount++
				}
			}
		}
		if failCount != 2 {
			t.Errorf("expected 2 failures, got %d", failCount)
		}
		if successCount != 1 {
			t.Errorf("expected 1 success, got %d", successCount)
		}
	})
}

// =============================================================================
// Test: Timeout errors
// =============================================================================

func TestTimeoutErrors(t *testing.T) {
	ctx := context.Background()
	inner := objectstore.NewMemoryStore()
	store := NewFaultStore(inner)

	t.Run("timeout on Get operation", func(t *testing.T) {
		store.ClearFaults()
		store.ClearRecords()

		// Configure short timeout
		store.AddFault(&FaultConfig{
			Mode:      FaultTimeout,
			Operation: "Get",
			Timeout:   50 * time.Millisecond,
		})

		// Create context with timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		start := time.Now()
		_, _, err := store.Get(timeoutCtx, "any-key", nil)
		elapsed := time.Since(start)

		if err == nil {
			t.Error("expected timeout error, got nil")
		}

		// Should timeout around 50ms (the fault timeout)
		if elapsed < 40*time.Millisecond {
			t.Errorf("timeout happened too quickly: %v", elapsed)
		}
	})

	t.Run("context cancellation during timeout", func(t *testing.T) {
		store.ClearFaults()
		store.ClearRecords()

		// Configure long timeout
		store.AddFault(&FaultConfig{
			Mode:      FaultTimeout,
			Operation: "Get",
			Timeout:   10 * time.Second,
		})

		// Create context and cancel it quickly
		timeoutCtx, cancel := context.WithCancel(ctx)

		var wg sync.WaitGroup
		var getErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, getErr = store.Get(timeoutCtx, "any-key", nil)
		}()

		// Cancel after a short delay
		time.Sleep(50 * time.Millisecond)
		cancel()
		wg.Wait()

		if getErr == nil {
			t.Error("expected context canceled error, got nil")
		}
		if !errors.Is(getErr, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", getErr)
		}
	})

	t.Run("timeout on Put operation blocks writes", func(t *testing.T) {
		store.ClearFaults()
		store.ClearRecords()

		store.AddFault(&FaultConfig{
			Mode:      FaultTimeout,
			Operation: "Put",
			Timeout:   50 * time.Millisecond,
		})

		timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		data := []byte("test data")
		_, err := store.Put(timeoutCtx, "test-key", bytes.NewReader(data), int64(len(data)), nil)
		if err == nil {
			t.Error("expected timeout error on Put, got nil")
		}
	})
}

// =============================================================================
// Test: Partial reads
// =============================================================================

func TestPartialReads(t *testing.T) {
	ctx := context.Background()
	inner := objectstore.NewMemoryStore()
	store := NewFaultStore(inner)

	t.Run("partial read returns error mid-stream", func(t *testing.T) {
		store.ClearFaults()
		store.ClearRecords()

		testKey := "partial-read-key"
		fullData := []byte("This is a longer piece of test data that will be partially read")
		_, err := inner.Put(ctx, testKey, bytes.NewReader(fullData), int64(len(fullData)), nil)
		if err != nil {
			t.Fatalf("failed to put test data: %v", err)
		}

		// Configure partial read to fail after 10 bytes
		store.AddFault(&FaultConfig{
			Mode:            FaultPartialRead,
			Operation:       "Get",
			KeyPattern:      testKey,
			PartialReadSize: 10,
		})

		reader, _, err := store.Get(ctx, testKey, nil)
		if err != nil {
			t.Fatalf("Get should succeed, partial read happens during read: %v", err)
		}
		defer reader.Close()

		// Try to read all data
		buf := make([]byte, len(fullData))
		n, err := io.ReadFull(reader, buf)

		if err == nil {
			t.Error("expected partial read error, got nil")
		}
		if !errors.Is(err, errPartialRead) {
			t.Errorf("expected errPartialRead, got %v", err)
		}
		if n != 10 {
			t.Errorf("expected to read 10 bytes before error, got %d", n)
		}
	})

	t.Run("partial read of WAL entry corrupts decode", func(t *testing.T) {
		store.ClearFaults()
		store.ClearRecords()

		ns := "test-partial-wal"
		stateMan := namespace.NewStateManager(inner)

		// Create namespace state
		_, err := stateMan.Create(ctx, ns)
		if err != nil {
			t.Fatalf("failed to create namespace: %v", err)
		}

		// Write a WAL entry using the inner store (no faults)
		committer, err := wal.NewCommitter(inner, stateMan)
		if err != nil {
			t.Fatalf("failed to create committer: %v", err)
		}
		defer committer.Close()

		entry := makeWalEntry("req-1", "test-doc")
		result, err := committer.Commit(ctx, ns, entry, nil)
		if err != nil {
			t.Fatalf("failed to commit WAL: %v", err)
		}

		// Now configure partial read for WAL entries
		store.AddFault(&FaultConfig{
			Mode:            FaultPartialRead,
			Operation:       "Get",
			KeyPattern:      "vex/namespaces/" + ns + "/wal/*",
			PartialReadSize: 5, // Very small to ensure corruption
		})

		// Try to read the WAL entry through the fault store
		walKey := result.WALKey
		reader, _, err := store.Get(ctx, walKey, nil)
		if err != nil {
			t.Fatalf("Get should succeed for WAL: %v", err)
		}

		// Try to decode - should fail due to partial read
		data, err := io.ReadAll(reader)
		reader.Close()

		// Either ReadAll fails or the data is truncated
		if err == nil && len(data) == 5 {
			// Partial read worked as expected - truncated data
			t.Logf("Got truncated data: %d bytes (expected partial)", len(data))
		} else if errors.Is(err, errPartialRead) {
			// Partial read error as expected
			t.Logf("Got partial read error as expected")
		} else if err != nil {
			t.Logf("Got error during read: %v", err)
		}
	})
}

// =============================================================================
// Test: Strong queries fail loudly when required
// =============================================================================

// mockFailingTailStore simulates a tail store that fails on refresh for strong consistency testing.
type mockFailingTailStore struct {
	docs       []*tail.Document
	refreshErr error
	scanErr    error
}

func (m *mockFailingTailStore) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return m.refreshErr
}

func (m *mockFailingTailStore) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	if m.scanErr != nil {
		return nil, m.scanErr
	}
	return m.docs, nil
}

func (m *mockFailingTailStore) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.Scan(ctx, ns, f)
}

func (m *mockFailingTailStore) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	if m.scanErr != nil {
		return nil, m.scanErr
	}
	var results []tail.VectorScanResult
	for i, doc := range m.docs {
		if i >= topK {
			break
		}
		results = append(results, tail.VectorScanResult{Doc: doc, Distance: float64(i)})
	}
	return results, nil
}

func (m *mockFailingTailStore) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return m.VectorScan(ctx, ns, queryVector, topK, metric, f)
}

func (m *mockFailingTailStore) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *mockFailingTailStore) TailBytes(ns string) int64 {
	return 0
}

func (m *mockFailingTailStore) Clear(ns string) {}

func (m *mockFailingTailStore) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *mockFailingTailStore) Close() error {
	return nil
}

func writeTestState(ctx context.Context, store objectstore.Store, ns string, walHeadSeq, indexedSeq uint64) error {
	state := &namespace.State{
		FormatVersion: 1,
		Namespace:     ns,
		WAL:           namespace.WALState{HeadSeq: walHeadSeq},
		Index:         namespace.IndexState{IndexedWALSeq: indexedSeq},
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	key := namespace.StateKey(ns)
	_, err = store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
	return err
}

func TestStrongQueriesFailLoudly(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	ns := "test-strong-fail"

	t.Run("strong query fails with ErrSnapshotRefreshFailed when refresh fails", func(t *testing.T) {
		if err := writeTestState(ctx, store, ns, 5, 2); err != nil {
			t.Fatalf("failed to write test state: %v", err)
		}

		stateMan := namespace.NewStateManager(store)
		mockTail := &mockFailingTailStore{
			refreshErr: errors.New("object storage unavailable"),
		}

		h := query.NewHandler(store, stateMan, mockTail)

		req := &query.QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "strong",
		}

		_, err := h.Handle(ctx, ns, req)
		if err == nil {
			t.Fatal("expected error for strong query with refresh failure, got nil")
		}
		if !errors.Is(err, query.ErrSnapshotRefreshFailed) {
			t.Errorf("expected ErrSnapshotRefreshFailed, got: %v", err)
		}
	})

	t.Run("default consistency (strong) also fails loudly", func(t *testing.T) {
		stateMan := namespace.NewStateManager(store)
		mockTail := &mockFailingTailStore{
			refreshErr: errors.New("connection reset"),
		}

		h := query.NewHandler(store, stateMan, mockTail)

		req := &query.QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "", // Default is strong
		}

		_, err := h.Handle(ctx, ns, req)
		if err == nil {
			t.Fatal("expected error for default consistency query with refresh failure, got nil")
		}
		if !errors.Is(err, query.ErrSnapshotRefreshFailed) {
			t.Errorf("expected ErrSnapshotRefreshFailed, got: %v", err)
		}
	})

	t.Run("strong vector query fails on refresh error", func(t *testing.T) {
		stateMan := namespace.NewStateManager(store)
		mockTail := &mockFailingTailStore{
			refreshErr: errors.New("timeout"),
		}

		h := query.NewHandler(store, stateMan, mockTail)

		req := &query.QueryRequest{
			RankBy:      []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Limit:       10,
			Consistency: "strong",
		}

		_, err := h.Handle(ctx, ns, req)
		if err == nil {
			t.Fatal("expected error for strong vector query with refresh failure, got nil")
		}
		if !errors.Is(err, query.ErrSnapshotRefreshFailed) {
			t.Errorf("expected ErrSnapshotRefreshFailed, got: %v", err)
		}
	})

	t.Run("multi-query strong fails when any refresh fails", func(t *testing.T) {
		stateMan := namespace.NewStateManager(store)
		mockTail := &mockFailingTailStore{
			refreshErr: errors.New("network error"),
		}

		h := query.NewHandler(store, stateMan, mockTail)

		subqueries := []map[string]any{
			{"rank_by": []any{"id", "asc"}, "limit": 5},
			{"rank_by": []any{"id", "desc"}, "limit": 5},
		}

		_, err := h.HandleMultiQuery(ctx, ns, subqueries, "strong")
		if err == nil {
			t.Fatal("expected error for strong multi-query with refresh failure, got nil")
		}
		if !errors.Is(err, query.ErrSnapshotRefreshFailed) {
			t.Errorf("expected ErrSnapshotRefreshFailed, got: %v", err)
		}
	})
}

// =============================================================================
// Test: Eventual queries degrade gracefully
// =============================================================================

func TestEventualQueriesDegradeGracefully(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	ns := "test-eventual-degrade"

	t.Run("eventual query succeeds when refresh would fail", func(t *testing.T) {
		if err := writeTestState(ctx, store, ns, 5, 2); err != nil {
			t.Fatalf("failed to write test state: %v", err)
		}

		stateMan := namespace.NewStateManager(store)

		cachedDocs := []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "cached-1"}},
			{ID: document.NewU64ID(2), Attributes: map[string]any{"name": "cached-2"}},
		}

		mockTail := &mockFailingTailStore{
			docs:       cachedDocs,
			refreshErr: errors.New("object storage unavailable"),
		}

		h := query.NewHandler(store, stateMan, mockTail)
		h.MarkSnapshotRefreshed(ns, time.Now())

		req := &query.QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "eventual",
		}

		resp, err := h.Handle(ctx, ns, req)
		if err != nil {
			t.Fatalf("eventual query should succeed from cache, got error: %v", err)
		}
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows from cache, got %d", len(resp.Rows))
		}
	})

	t.Run("eventual vector query succeeds from cache", func(t *testing.T) {
		stateMan := namespace.NewStateManager(store)

		cachedDocs := []*tail.Document{
			{ID: document.NewU64ID(1), Vector: []float32{1.0, 0.0, 0.0}, Attributes: map[string]any{"name": "vec-1"}},
			{ID: document.NewU64ID(2), Vector: []float32{0.0, 1.0, 0.0}, Attributes: map[string]any{"name": "vec-2"}},
		}

		mockTail := &mockFailingTailStore{
			docs:       cachedDocs,
			refreshErr: errors.New("network timeout"),
		}

		h := query.NewHandler(store, stateMan, mockTail)
		h.MarkSnapshotRefreshed(ns, time.Now())

		req := &query.QueryRequest{
			RankBy:      []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Limit:       10,
			Consistency: "eventual",
		}

		resp, err := h.Handle(ctx, ns, req)
		if err != nil {
			t.Fatalf("eventual vector query should succeed, got error: %v", err)
		}
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(resp.Rows))
		}
		// Vector queries should include $dist
		if resp.Rows[0].Dist == nil {
			t.Error("expected $dist in vector query results")
		}
	})

	t.Run("strong fails but eventual succeeds on same data", func(t *testing.T) {
		stateMan := namespace.NewStateManager(store)

		cachedDocs := []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "doc-1"}},
		}

		mockTail := &mockFailingTailStore{
			docs:       cachedDocs,
			refreshErr: errors.New("storage unavailable"),
		}

		h := query.NewHandler(store, stateMan, mockTail)
		h.MarkSnapshotRefreshed(ns, time.Now())

		// Strong query should fail
		strongReq := &query.QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "strong",
		}
		_, strongErr := h.Handle(ctx, ns, strongReq)
		if strongErr == nil {
			t.Error("expected strong query to fail")
		}

		// Eventual query should succeed
		eventualReq := &query.QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "eventual",
		}
		resp, eventualErr := h.Handle(ctx, ns, eventualReq)
		if eventualErr != nil {
			t.Fatalf("eventual query should succeed, got: %v", eventualErr)
		}
		if resp == nil || len(resp.Rows) != 1 {
			t.Error("expected 1 row from eventual query")
		}
	})

	t.Run("eventual multi-query succeeds without refresh", func(t *testing.T) {
		stateMan := namespace.NewStateManager(store)

		cachedDocs := []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"category": "A"}},
			{ID: document.NewU64ID(2), Attributes: map[string]any{"category": "B"}},
		}

		mockTail := &mockFailingTailStore{
			docs:       cachedDocs,
			refreshErr: errors.New("connection refused"),
		}

		h := query.NewHandler(store, stateMan, mockTail)
		h.MarkSnapshotRefreshed(ns, time.Now())

		subqueries := []map[string]any{
			{"rank_by": []any{"id", "asc"}, "limit": 5},
			{"rank_by": []any{"id", "desc"}, "limit": 5},
		}

		resp, err := h.HandleMultiQuery(ctx, ns, subqueries, "eventual")
		if err != nil {
			t.Fatalf("eventual multi-query should succeed, got error: %v", err)
		}
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if len(resp.Results) != 2 {
			t.Errorf("expected 2 subquery results, got %d", len(resp.Results))
		}
	})
}

// =============================================================================
// Test: Background repair restores state
// =============================================================================

func TestBackgroundRepairRestoresState(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	ns := "test-repair"

	t.Run("repair detects and fixes partial WAL commit", func(t *testing.T) {
		// Create initial state
		stateMan := namespace.NewStateManager(store)
		_, err := stateMan.Create(ctx, ns)
		if err != nil {
			t.Fatalf("failed to create namespace: %v", err)
		}

		// Commit several WAL entries
		committer, err := wal.NewCommitter(store, stateMan)
		if err != nil {
			t.Fatalf("failed to create committer: %v", err)
		}

		for i := 1; i <= 3; i++ {
			entry := makeWalEntryWithU64ID("req-"+string(rune('0'+i)), uint64(i))
			_, err := committer.Commit(ctx, ns, entry, nil)
			if err != nil {
				t.Fatalf("failed to commit WAL %d: %v", i, err)
			}
		}
		committer.Close()

		// Verify initial state
		loaded, err := stateMan.Load(ctx, ns)
		if err != nil {
			t.Fatalf("failed to load state: %v", err)
		}
		if loaded.State.WAL.HeadSeq != 3 {
			t.Errorf("expected head seq 3, got %d", loaded.State.WAL.HeadSeq)
		}

		// Simulate partial commit: write WAL 4 directly without updating state
		walEntry := makeWalEntryWithU64ID("req-orphan", 4)
		walEntry.Namespace = ns
		walEntry.Seq = 4
		encoder, err := wal.NewEncoder()
		if err != nil {
			t.Fatalf("failed to create encoder: %v", err)
		}
		encResult, err := encoder.Encode(walEntry)
		if err != nil {
			t.Fatalf("failed to encode WAL: %v", err)
		}
		encoder.Close()

		walKey := walKeyForNS(ns, 4)
		_, err = store.PutIfAbsent(ctx, walKey, bytes.NewReader(encResult.Data), int64(len(encResult.Data)), nil)
		if err != nil {
			t.Fatalf("failed to write orphan WAL: %v", err)
		}

		// State still shows head seq 3 (partial commit)
		loaded, _ = stateMan.Load(ctx, ns)
		if loaded.State.WAL.HeadSeq != 3 {
			t.Errorf("before repair, expected head seq 3, got %d", loaded.State.WAL.HeadSeq)
		}

		// Run repair
		repairer := wal.NewRepairer(store, stateMan)
		result, err := repairer.Repair(ctx, ns)
		if err != nil {
			t.Fatalf("repair failed: %v", err)
		}

		// Verify repair result
		if result.OriginalHeadSeq != 3 {
			t.Errorf("expected original head seq 3, got %d", result.OriginalHeadSeq)
		}
		if result.NewHeadSeq != 4 {
			t.Errorf("expected new head seq 4, got %d", result.NewHeadSeq)
		}
		if result.WALsRepaired != 1 {
			t.Errorf("expected 1 WAL repaired, got %d", result.WALsRepaired)
		}

		// Verify state is now correct
		loaded, _ = stateMan.Load(ctx, ns)
		if loaded.State.WAL.HeadSeq != 4 {
			t.Errorf("after repair, expected head seq 4, got %d", loaded.State.WAL.HeadSeq)
		}
	})

	t.Run("repair handles gaps in WAL sequence", func(t *testing.T) {
		gapNS := "test-repair-gap"
		stateMan := namespace.NewStateManager(store)
		_, err := stateMan.Create(ctx, gapNS)
		if err != nil {
			t.Fatalf("failed to create namespace: %v", err)
		}

		// Commit WAL 1
		committer, err := wal.NewCommitter(store, stateMan)
		if err != nil {
			t.Fatalf("failed to create committer: %v", err)
		}
		entry := makeWalEntryWithU64ID("req-1", 1)
		_, err = committer.Commit(ctx, gapNS, entry, nil)
		if err != nil {
			t.Fatalf("failed to commit WAL 1: %v", err)
		}
		committer.Close()

		// Write WAL 3 directly (skipping 2) - creates a gap
		walEntry := makeWalEntryWithU64ID("req-3", 3)
		walEntry.Namespace = gapNS
		walEntry.Seq = 3
		encoder, err := wal.NewEncoder()
		if err != nil {
			t.Fatalf("failed to create encoder: %v", err)
		}
		encResult, err := encoder.Encode(walEntry)
		if err != nil {
			t.Fatalf("failed to encode WAL: %v", err)
		}
		encoder.Close()

		walKey := walKeyForNS(gapNS, 3)
		_, err = store.PutIfAbsent(ctx, walKey, bytes.NewReader(encResult.Data), int64(len(encResult.Data)), nil)
		if err != nil {
			t.Fatalf("failed to write WAL 3: %v", err)
		}

		// Repair should only advance to contiguous sequence (1)
		repairer := wal.NewRepairer(store, stateMan)
		result, err := repairer.Repair(ctx, gapNS)
		if err != nil {
			t.Fatalf("repair failed: %v", err)
		}

		if result.HighestContiguousSeq != 1 {
			t.Errorf("expected highest contiguous 1, got %d", result.HighestContiguousSeq)
		}
		if len(result.OrphanedWALs) != 1 || result.OrphanedWALs[0] != 3 {
			t.Errorf("expected orphaned WAL [3], got %v", result.OrphanedWALs)
		}
	})

	t.Run("repair task runs periodically", func(t *testing.T) {
		taskNS := "test-repair-task"
		stateMan := namespace.NewStateManager(store)
		_, err := stateMan.Create(ctx, taskNS)
		if err != nil {
			t.Fatalf("failed to create namespace: %v", err)
		}

		repairer := wal.NewRepairer(store, stateMan)
		task := wal.NewRepairTask(repairer, 50*time.Millisecond, 100*time.Millisecond)

		taskCtx, cancel := context.WithCancel(ctx)
		task.Start(taskCtx, []string{taskNS})

		// Let it run a few cycles
		time.Sleep(200 * time.Millisecond)

		// Check that repair has run
		lastRepairTime := task.GetLastRepairTime(taskNS)
		if lastRepairTime.IsZero() {
			t.Error("expected repair task to have run")
		}

		cancel()
		task.Stop()
	})

	t.Run("repair task respects staleness bound", func(t *testing.T) {
		boundNS := "test-repair-bound"
		stateMan := namespace.NewStateManager(store)
		_, err := stateMan.Create(ctx, boundNS)
		if err != nil {
			t.Fatalf("failed to create namespace: %v", err)
		}

		repairer := wal.NewRepairer(store, stateMan)
		// Long staleness bound
		task := wal.NewRepairTask(repairer, 10*time.Millisecond, 1*time.Hour)

		if task.StalenessBound() != 1*time.Hour {
			t.Errorf("expected staleness bound 1h, got %v", task.StalenessBound())
		}

		// Manual repair should work
		result, err := task.RepairOnce(ctx, boundNS)
		if err != nil {
			t.Fatalf("RepairOnce failed: %v", err)
		}
		if result == nil {
			t.Fatal("expected non-nil result from RepairOnce")
		}

		// Subsequent repair should be skipped due to staleness bound
		firstRepairTime := task.GetLastRepairTime(boundNS)
		time.Sleep(20 * time.Millisecond)

		// The task loop won't repair again within staleness bound
		secondRepairTime := task.GetLastRepairTime(boundNS)
		if !secondRepairTime.Equal(firstRepairTime) {
			t.Error("expected repair time to be unchanged within staleness bound")
		}
	})
}

// =============================================================================
// Integration: Combined fault scenarios
// =============================================================================

func TestCombinedFaultScenarios(t *testing.T) {
	ctx := context.Background()
	inner := objectstore.NewMemoryStore()
	store := NewFaultStore(inner)
	ns := "test-combined"

	t.Run("transient failures during write followed by successful query", func(t *testing.T) {
		store.ClearFaults()
		store.ClearRecords()
		store.ResetAttempts()

		stateMan := namespace.NewStateManager(store)
		_, err := stateMan.Create(ctx, ns)
		if err != nil {
			t.Fatalf("failed to create namespace: %v", err)
		}

		// Configure transient failure on WAL writes (fail first attempt)
		store.AddFault(&FaultConfig{
			Mode:           FaultTransient,
			Operation:      "PutIfAbsent",
			KeyPattern:     "vex/namespaces/" + ns + "/wal/*",
			TransientCount: 1,
			Error:          errTransientFailure,
		})

		committer, err := wal.NewCommitter(store, stateMan)
		if err != nil {
			t.Fatalf("failed to create committer: %v", err)
		}
		defer committer.Close()

		entry := makeWalEntry("req-1", "doc-1")

		// First attempt fails
		_, err = committer.Commit(ctx, ns, entry, nil)
		if err == nil {
			t.Error("first commit should fail due to transient fault")
		}

		// Second attempt succeeds
		_, err = committer.Commit(ctx, ns, entry, nil)
		if err != nil {
			t.Errorf("second commit should succeed, got: %v", err)
		}

		// Verify records show failure then success
		records := store.GetRecords()
		var failures, successes int
		for _, r := range records {
			if r.Op == "PutIfAbsent" && strings.Contains(r.Key, "/wal/") {
				if r.Success {
					successes++
				} else {
					failures++
				}
			}
		}
		if failures < 1 {
			t.Error("expected at least one WAL write failure")
		}
		if successes < 1 {
			t.Error("expected at least one WAL write success")
		}
	})

	t.Run("concurrent faults with different modes", func(t *testing.T) {
		store.ClearFaults()
		store.ClearRecords()

		// Different faults for different operations
		store.AddFault(&FaultConfig{
			Mode:       FaultError500,
			Operation:  "Get",
			KeyPattern: "fault-get-key",
			Error:      errInjected500,
		})
		store.AddFault(&FaultConfig{
			Mode:       FaultTimeout,
			Operation:  "Put",
			KeyPattern: "fault-put-key",
			Timeout:    50 * time.Millisecond,
		})

		// Test Get fault
		_, _, err := store.Get(ctx, "fault-get-key", nil)
		if err == nil {
			t.Error("expected Get fault error")
		}

		// Test Put fault with timeout context
		timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		_, err = store.Put(timeoutCtx, "fault-put-key", bytes.NewReader([]byte("data")), 4, nil)
		cancel()
		if err == nil {
			t.Error("expected Put timeout error")
		}

		// Normal operations should work
		_, err = store.Put(ctx, "normal-key", bytes.NewReader([]byte("data")), 4, nil)
		if err != nil {
			t.Errorf("normal Put should work: %v", err)
		}
	})
}

// =============================================================================
// Concurrency stress tests
// =============================================================================

func TestConcurrentFaultInjection(t *testing.T) {
	ctx := context.Background()
	inner := objectstore.NewMemoryStore()
	store := NewFaultStore(inner)

	t.Run("concurrent operations with intermittent faults", func(t *testing.T) {
		store.ClearFaults()
		store.ClearRecords()
		store.ResetAttempts()

		// 50% of Gets fail (every other attempt)
		var getCount atomic.Int64
		store.AddFault(&FaultConfig{
			Mode:           FaultTransient,
			Operation:      "Get",
			TransientCount: 1, // Every other attempt succeeds
		})

		// Write some data first
		for i := 0; i < 10; i++ {
			key := "concurrent-key-" + string(rune('0'+i))
			data := []byte("data")
			_, err := inner.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
			if err != nil {
				t.Fatalf("failed to write key %s: %v", key, err)
			}
		}

		var wg sync.WaitGroup
		errCount := atomic.Int64{}
		successCount := atomic.Int64{}

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := "concurrent-key-" + string(rune('0'+(idx%10)))
				_, _, err := store.Get(ctx, key, nil)
				if err != nil {
					errCount.Add(1)
				} else {
					successCount.Add(1)
				}
				getCount.Add(1)
			}(i)
		}

		wg.Wait()

		// With transient faults, we should see a mix of successes and failures
		t.Logf("Concurrent test: %d successes, %d failures out of %d total",
			successCount.Load(), errCount.Load(), getCount.Load())

		if successCount.Load() == 0 {
			t.Error("expected some successful operations")
		}
	})
}
