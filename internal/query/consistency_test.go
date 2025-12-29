package query

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// mockTailStoreWithRefresh allows controlling Refresh behavior for testing.
type mockTailStoreWithRefresh struct {
	docs          []*tail.Document
	refreshCalled bool
	refreshAfter  uint64
	refreshUpTo   uint64
	refreshErr    error
}

func (m *mockTailStoreWithRefresh) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	m.refreshCalled = true
	m.refreshAfter = afterSeq
	m.refreshUpTo = upToSeq
	return m.refreshErr
}

func (m *mockTailStoreWithRefresh) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStoreWithRefresh) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStoreWithRefresh) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	var results []tail.VectorScanResult
	for i, doc := range m.docs {
		if i >= topK {
			break
		}
		results = append(results, tail.VectorScanResult{
			Doc:      doc,
			Distance: float64(i) * 0.1,
		})
	}
	return results, nil
}

func (m *mockTailStoreWithRefresh) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return m.VectorScan(ctx, ns, queryVector, topK, metric, f)
}

func (m *mockTailStoreWithRefresh) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *mockTailStoreWithRefresh) TailBytes(ns string) int64 {
	return 0
}

func (m *mockTailStoreWithRefresh) Clear(ns string) {}

func (m *mockTailStoreWithRefresh) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *mockTailStoreWithRefresh) Close() error {
	return nil
}

// writeTestState writes a test state directly to object storage, bypassing validation.
// This allows testing with arbitrary WAL sequences.
func writeTestState(ctx context.Context, store objectstore.Store, ns string, walHeadSeq, indexedSeq uint64) error {
	state := &namespace.State{
		FormatVersion: 1,
		Namespace:     ns,
		WAL: namespace.WALState{
			HeadSeq: walHeadSeq,
		},
		Index: namespace.IndexState{
			IndexedWALSeq: indexedSeq,
		},
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	key := namespace.StateKey(ns)
	_, err = store.PutIfAbsent(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
	if err != nil && !objectstore.IsConflictError(err) {
		return err
	}
	// If conflict, update the existing state
	if objectstore.IsConflictError(err) {
		_, err = store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestStrongConsistencyIsDefault(t *testing.T) {
	// Verify that consistency defaults to strong (empty string means strong).
	// This is verified by checking that the default value is empty string
	// and the handler treats empty as strong.

	t.Run("default consistency is empty string (treated as strong)", func(t *testing.T) {
		req, err := ParseQueryRequest(map[string]any{
			"rank_by": []any{"id", "asc"},
		})
		if err != nil {
			t.Fatalf("ParseQueryRequest failed: %v", err)
		}
		if req.Consistency != "" {
			t.Errorf("expected default consistency to be empty string, got %q", req.Consistency)
		}
	})

	t.Run("explicit strong is valid", func(t *testing.T) {
		req, err := ParseQueryRequest(map[string]any{
			"rank_by":     []any{"id", "asc"},
			"consistency": "strong",
		})
		if err != nil {
			t.Fatalf("ParseQueryRequest failed: %v", err)
		}
		if req.Consistency != "strong" {
			t.Errorf("expected consistency to be 'strong', got %q", req.Consistency)
		}
	})

	t.Run("eventual consistency is valid", func(t *testing.T) {
		req, err := ParseQueryRequest(map[string]any{
			"rank_by":     []any{"id", "asc"},
			"consistency": "eventual",
		})
		if err != nil {
			t.Fatalf("ParseQueryRequest failed: %v", err)
		}
		if req.Consistency != "eventual" {
			t.Errorf("expected consistency to be 'eventual', got %q", req.Consistency)
		}
	})
}

func TestInvalidConsistencyRejected(t *testing.T) {
	h := &Handler{}

	tests := []struct {
		name        string
		consistency string
		wantErr     bool
	}{
		{"empty (default/strong)", "", false},
		{"explicit strong", "strong", false},
		{"eventual", "eventual", false},
		{"invalid value", "invalid", true},
		{"typo in strong", "strng", true},
		{"typo in eventual", "eventuall", true},
		{"camelCase", "Strong", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &QueryRequest{
				RankBy:      []any{"id", "asc"},
				Consistency: tt.consistency,
			}
			err := h.validateRequest(req)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidConsistency) {
					t.Errorf("expected ErrInvalidConsistency, got %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestStrongQueryRefreshesCache(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()

	// Write state directly to simulate unindexed data (WAL at seq 5, indexed at seq 2)
	if err := writeTestState(ctx, store, "test-ns", 5, 2); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	mockTail := &mockTailStoreWithRefresh{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"name": "doc1"},
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	t.Run("strong query triggers refresh for unindexed data", func(t *testing.T) {
		mockTail.refreshCalled = false

		req := &QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "", // Default is strong
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if !mockTail.refreshCalled {
			t.Error("expected Refresh to be called for strong consistency query")
		}
		if mockTail.refreshAfter != 2 {
			t.Errorf("expected refreshAfter=2, got %d", mockTail.refreshAfter)
		}
		if mockTail.refreshUpTo != 5 {
			t.Errorf("expected refreshUpTo=5, got %d", mockTail.refreshUpTo)
		}
	})

	t.Run("explicit strong consistency triggers refresh", func(t *testing.T) {
		mockTail.refreshCalled = false

		req := &QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "strong",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if !mockTail.refreshCalled {
			t.Error("expected Refresh to be called for explicit strong consistency query")
		}
	})

	t.Run("eventual consistency does NOT trigger refresh", func(t *testing.T) {
		mockTail.refreshCalled = false

		req := &QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "eventual",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if mockTail.refreshCalled {
			t.Error("expected Refresh NOT to be called for eventual consistency query")
		}
	})
}

func TestStrongQueryIncludesAllCommittedWALEntries(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()

	// Write state directly to simulate unindexed data spanning multiple entries (WAL at seq 10, indexed at seq 3)
	if err := writeTestState(ctx, store, "test-ns", 10, 3); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	mockTail := &mockTailStoreWithRefresh{
		docs: []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "doc1"}},
			{ID: document.NewU64ID(2), Attributes: map[string]any{"name": "doc2"}},
			{ID: document.NewU64ID(3), Attributes: map[string]any{"name": "doc3"}},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy:      []any{"id", "asc"},
		Limit:       10,
		Consistency: "strong",
	}
	_, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// The refresh should cover all WAL entries from indexedSeq to headSeq
	if mockTail.refreshAfter != 3 {
		t.Errorf("expected refresh to start after seq 3, got %d", mockTail.refreshAfter)
	}
	if mockTail.refreshUpTo != 10 {
		t.Errorf("expected refresh to include up to seq 10, got %d", mockTail.refreshUpTo)
	}
}

func TestStrongQueryFailsIfRefreshFails(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()

	// Write state directly to simulate unindexed data (WAL at seq 5, indexed at seq 2)
	if err := writeTestState(ctx, store, "test-ns", 5, 2); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	refreshError := errors.New("object storage unavailable")
	mockTail := &mockTailStoreWithRefresh{
		refreshErr: refreshError,
	}

	h := NewHandler(store, stateMan, mockTail)

	t.Run("strong query fails when refresh fails", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "strong",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil {
			t.Fatal("expected error when refresh fails, got nil")
		}
		if !errors.Is(err, ErrSnapshotRefreshFailed) {
			t.Errorf("expected ErrSnapshotRefreshFailed, got %v", err)
		}
	})

	t.Run("eventual query succeeds even when refresh would fail", func(t *testing.T) {
		mockTail.docs = []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "doc1"}},
		}

		req := &QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "eventual",
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("expected eventual query to succeed, got error: %v", err)
		}
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
	})
}

func TestMissingWalEntryFailsStrongQueries(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	defer tailStore.Close()

	ctx := context.Background()

	if err := writeTestState(ctx, store, "test-ns", 2, 0); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req")
	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{"name": wal.StringValue("doc1")},
		nil, 0,
	)
	entry.SubBatches = append(entry.SubBatches, batch)
	tailStore.AddWALEntry("test-ns", entry)

	h := NewHandler(store, stateMan, tailStore)

	strongReq := &QueryRequest{
		RankBy:      []any{"id", "asc"},
		Limit:       10,
		Consistency: "strong",
	}
	_, err := h.Handle(ctx, "test-ns", strongReq)
	if err == nil {
		t.Fatal("expected strong query to fail when WAL is missing")
	}
	if !errors.Is(err, ErrSnapshotRefreshFailed) {
		t.Fatalf("expected ErrSnapshotRefreshFailed, got %v", err)
	}

	eventualReq := &QueryRequest{
		RankBy:      []any{"id", "asc"},
		Limit:       10,
		Consistency: "eventual",
	}
	resp, err := h.Handle(ctx, "test-ns", eventualReq)
	if err != nil {
		t.Fatalf("expected eventual query to succeed, got %v", err)
	}
	if resp == nil || len(resp.Rows) != 1 {
		t.Fatalf("expected 1 row from eventual query, got %+v", resp)
	}
}

func TestStrongQueryNoRefreshWhenFullyIndexed(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()

	// Write state directly - WAL head equals indexed seq (fully indexed, no unindexed data)
	if err := writeTestState(ctx, store, "test-ns", 5, 5); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	mockTail := &mockTailStoreWithRefresh{
		docs: []*tail.Document{},
	}

	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy:      []any{"id", "asc"},
		Limit:       10,
		Consistency: "strong",
	}
	_, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// No refresh needed when fully indexed
	if mockTail.refreshCalled {
		t.Error("expected Refresh NOT to be called when data is fully indexed")
	}
}

// mockTailStoreWithByteLimit is a mock that tracks byte limit parameters for eventual consistency testing.
type mockTailStoreWithByteLimit struct {
	docs                         []*tail.Document
	scanByteLimit                int64
	vectorByteLimit              int64
	scanIncludingDeletedLimit    int64
	scanWithLimitCall            bool
	vectorWithLimitCall          bool
	scanIncludingDeletedWithCall bool
}

func (m *mockTailStoreWithByteLimit) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *mockTailStoreWithByteLimit) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStoreWithByteLimit) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	m.scanWithLimitCall = true
	m.scanByteLimit = byteLimitBytes
	return m.docs, nil
}

func (m *mockTailStoreWithByteLimit) ScanIncludingDeletedWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	m.scanIncludingDeletedWithCall = true
	m.scanIncludingDeletedLimit = byteLimitBytes
	return m.docs, nil
}

func (m *mockTailStoreWithByteLimit) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	var results []tail.VectorScanResult
	for i, doc := range m.docs {
		if i >= topK {
			break
		}
		results = append(results, tail.VectorScanResult{
			Doc:      doc,
			Distance: float64(i) * 0.1,
		})
	}
	return results, nil
}

func (m *mockTailStoreWithByteLimit) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	m.vectorWithLimitCall = true
	m.vectorByteLimit = byteLimitBytes
	return m.VectorScan(ctx, ns, queryVector, topK, metric, f)
}

func (m *mockTailStoreWithByteLimit) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *mockTailStoreWithByteLimit) TailBytes(ns string) int64 {
	return 0
}

func (m *mockTailStoreWithByteLimit) Clear(ns string) {}

func (m *mockTailStoreWithByteLimit) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *mockTailStoreWithByteLimit) Close() error {
	return nil
}

func TestEventualConsistencyCanBeRequested(t *testing.T) {
	// This test verifies that consistency: "eventual" is a valid request option
	t.Run("eventual consistency is a valid value", func(t *testing.T) {
		req, err := ParseQueryRequest(map[string]any{
			"rank_by":     []any{"id", "asc"},
			"consistency": "eventual",
		})
		if err != nil {
			t.Fatalf("ParseQueryRequest failed: %v", err)
		}
		if req.Consistency != "eventual" {
			t.Errorf("expected consistency 'eventual', got %q", req.Consistency)
		}
	})

	t.Run("eventual query executes successfully", func(t *testing.T) {
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)
		ctx := context.Background()

		if err := writeTestState(ctx, store, "test-ns", 5, 2); err != nil {
			t.Fatalf("failed to write test state: %v", err)
		}

		mockTail := &mockTailStoreWithRefresh{
			docs: []*tail.Document{
				{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "doc1"}},
			},
		}

		h := NewHandler(store, stateMan, mockTail)

		req := &QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "eventual",
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if len(resp.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(resp.Rows))
		}
	})
}

func TestEventualConsistencyMayBeStale(t *testing.T) {
	// Eventual consistency queries skip refresh, allowing stale reads up to 60 seconds.
	// The staleness is implicit - we don't refresh on every query.
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	// Write state with unindexed data
	if err := writeTestState(ctx, store, "test-ns", 10, 5); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	mockTail := &mockTailStoreWithRefresh{
		docs: []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "doc1"}},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	t.Run("eventual query skips refresh allowing staleness", func(t *testing.T) {
		mockTail.refreshCalled = false

		req := &QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "eventual",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		// Eventual consistency should NOT trigger a refresh
		if mockTail.refreshCalled {
			t.Error("expected Refresh NOT to be called for eventual consistency (allows staleness)")
		}
	})

	t.Run("strong query does NOT allow staleness", func(t *testing.T) {
		mockTail.refreshCalled = false

		req := &QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "strong",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		// Strong consistency must refresh to not allow staleness
		if !mockTail.refreshCalled {
			t.Error("expected Refresh to be called for strong consistency (no staleness)")
		}
	})
}

func TestEventualSearchesOnly128MiBOfTail(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	if err := writeTestState(ctx, store, "test-ns", 5, 2); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	mockTail := &mockTailStoreWithByteLimit{
		docs: []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "doc1", "content": "hello world"}, Vector: []float32{1.0, 0.0}},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	t.Run("eventual order-by query uses byte-limited scan", func(t *testing.T) {
		mockTail.scanWithLimitCall = false
		mockTail.scanByteLimit = 0

		req := &QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "eventual",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if !mockTail.scanWithLimitCall {
			t.Error("expected ScanWithByteLimit to be called for eventual consistency")
		}
		if mockTail.scanByteLimit != EventualTailCapBytes {
			t.Errorf("expected byte limit %d, got %d", EventualTailCapBytes, mockTail.scanByteLimit)
		}
	})

	t.Run("eventual vector query uses byte-limited scan", func(t *testing.T) {
		mockTail.vectorWithLimitCall = false
		mockTail.vectorByteLimit = 0

		req := &QueryRequest{
			RankBy:      []any{"vector", "ANN", []any{1.0, 0.0}},
			Limit:       10,
			Consistency: "eventual",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if !mockTail.vectorWithLimitCall {
			t.Error("expected VectorScanWithByteLimit to be called for eventual consistency")
		}
		if mockTail.vectorByteLimit != EventualTailCapBytes {
			t.Errorf("expected byte limit %d, got %d", EventualTailCapBytes, mockTail.vectorByteLimit)
		}
	})

	t.Run("eventual BM25 query uses byte-limited scan including deletes", func(t *testing.T) {
		mockTail.scanIncludingDeletedWithCall = false
		mockTail.scanIncludingDeletedLimit = 0

		req := &QueryRequest{
			RankBy:      []any{"content", "BM25", "hello"},
			Limit:       10,
			Consistency: "eventual",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if !mockTail.scanIncludingDeletedWithCall {
			t.Error("expected ScanIncludingDeletedWithByteLimit to be called for eventual BM25")
		}
		if mockTail.scanIncludingDeletedLimit != EventualTailCapBytes {
			t.Errorf("expected byte limit %d, got %d", EventualTailCapBytes, mockTail.scanIncludingDeletedLimit)
		}
	})

	t.Run("eventual composite query uses byte-limited scan", func(t *testing.T) {
		mockTail.scanWithLimitCall = false
		mockTail.scanByteLimit = 0

		req := &QueryRequest{
			RankBy:      []any{"Sum", []any{[]any{"content", "BM25", "hello"}}},
			Limit:       10,
			Consistency: "eventual",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if !mockTail.scanWithLimitCall {
			t.Error("expected ScanWithByteLimit to be called for eventual composite queries")
		}
		if mockTail.scanByteLimit != EventualTailCapBytes {
			t.Errorf("expected byte limit %d, got %d", EventualTailCapBytes, mockTail.scanByteLimit)
		}
	})

	t.Run("strong query does NOT use byte-limited scan", func(t *testing.T) {
		mockTail.scanWithLimitCall = false
		mockTail.vectorWithLimitCall = false

		req := &QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "strong",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if mockTail.scanWithLimitCall {
			t.Error("expected Scan (not ScanWithByteLimit) to be called for strong consistency")
		}
	})
}

func TestEventualTailCapValue(t *testing.T) {
	// Verify the 128 MiB constant is correctly defined
	const expectedBytes = 128 * 1024 * 1024 // 128 MiB

	if EventualTailCapBytes != expectedBytes {
		t.Errorf("EventualTailCapBytes = %d, want %d", EventualTailCapBytes, expectedBytes)
	}
}
