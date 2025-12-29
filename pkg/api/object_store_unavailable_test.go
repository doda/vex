package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestWriteFailsWith503WhenObjectStoreUnavailable verifies that write operations
// fail with HTTP 503 Service Unavailable when object storage is unavailable.
// This is task step 1: "Test writes fail with 503 when object store unavailable"
func TestWriteFailsWith503WhenObjectStoreUnavailable(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Mark object store as unavailable via test state
	router.SetState(&ServerState{
		Namespaces:  map[string]*NamespaceState{},
		ObjectStore: ObjectStoreState{Available: false},
	})

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["status"] != "error" {
		t.Error("expected error status in response")
	}

	if !strings.Contains(resp["error"], "object storage") {
		t.Errorf("expected 'object storage' in error message, got %s", resp["error"])
	}
}

// mockTailStoreForRefreshFailure simulates a tail store that fails on refresh
// to test strong consistency behavior when object store becomes unavailable.
type mockTailStoreForRefreshFailure struct {
	docs       []*tail.Document
	refreshErr error
}

func (m *mockTailStoreForRefreshFailure) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return m.refreshErr
}

func (m *mockTailStoreForRefreshFailure) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStoreForRefreshFailure) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStoreForRefreshFailure) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
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

func (m *mockTailStoreForRefreshFailure) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return m.VectorScan(ctx, ns, queryVector, topK, metric, f)
}

func (m *mockTailStoreForRefreshFailure) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *mockTailStoreForRefreshFailure) TailBytes(ns string) int64 {
	return 0
}

func (m *mockTailStoreForRefreshFailure) Clear(ns string) {}

func (m *mockTailStoreForRefreshFailure) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *mockTailStoreForRefreshFailure) Close() error {
	return nil
}

// writeTestState writes a test state directly to object storage for testing.
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
	if objectstore.IsConflictError(err) {
		_, err = store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// TestStrongQueryFailsWhenSnapshotCannotBeRefreshed verifies that strong consistency
// queries fail with ErrSnapshotRefreshFailed (mapped to 503) when the object store
// becomes unavailable and snapshot cannot be refreshed.
// This is task step 2: "Verify strong queries fail when snapshot cannot be refreshed"
func TestStrongQueryFailsWhenSnapshotCannotBeRefreshed(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Write state directly - WAL at seq 5, indexed at seq 2 (unindexed data exists)
	if err := writeTestState(ctx, store, "test-ns", 5, 2); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	// Mock tail store that fails on refresh (simulates object store unavailable)
	mockTail := &mockTailStoreForRefreshFailure{
		refreshErr: errors.New("object storage unavailable"),
	}

	h := query.NewHandler(store, stateMan, mockTail)

	t.Run("strong query fails when refresh fails", func(t *testing.T) {
		req := &query.QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "strong",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil {
			t.Fatal("expected error when refresh fails, got nil")
		}
		if !errors.Is(err, query.ErrSnapshotRefreshFailed) {
			t.Errorf("expected ErrSnapshotRefreshFailed, got %v", err)
		}
	})

	t.Run("default consistency (strong) also fails when refresh fails", func(t *testing.T) {
		req := &query.QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "", // Default is strong
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil {
			t.Fatal("expected error when refresh fails with default consistency, got nil")
		}
		if !errors.Is(err, query.ErrSnapshotRefreshFailed) {
			t.Errorf("expected ErrSnapshotRefreshFailed, got %v", err)
		}
	})
}

// TestEventualQueryServesFromCacheWhenObjectStoreUnavailable verifies that eventual
// consistency queries can still serve data from cache when the object store becomes
// unavailable (as long as the data is not too stale).
// This is task step 3: "Test eventual queries may serve from cache if not too stale"
func TestEventualQueryServesFromCacheWhenObjectStoreUnavailable(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Write state directly - WAL at seq 5, indexed at seq 2 (unindexed data exists)
	if err := writeTestState(ctx, store, "test-ns", 5, 2); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	// Mock tail store that has cached documents but would fail refresh
	cachedDocs := []*tail.Document{
		{
			ID:         document.NewU64ID(1),
			Attributes: map[string]any{"name": "cached-doc-1"},
		},
		{
			ID:         document.NewU64ID(2),
			Attributes: map[string]any{"name": "cached-doc-2"},
		},
	}

	mockTail := &mockTailStoreForRefreshFailure{
		docs:       cachedDocs,
		refreshErr: errors.New("object storage unavailable"),
	}

	h := query.NewHandler(store, stateMan, mockTail)

	t.Run("eventual query succeeds from cache when refresh would fail", func(t *testing.T) {
		req := &query.QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "eventual",
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("expected eventual query to succeed from cache, got error: %v", err)
		}
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows from cache, got %d", len(resp.Rows))
		}
	})

	t.Run("strong query fails but eventual query succeeds", func(t *testing.T) {
		// First, verify strong query fails
		strongReq := &query.QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "strong",
		}
		_, strongErr := h.Handle(ctx, "test-ns", strongReq)
		if strongErr == nil {
			t.Error("expected strong query to fail when object store is unavailable")
		}

		// Then, verify eventual query still works
		eventualReq := &query.QueryRequest{
			RankBy:      []any{"id", "asc"},
			Limit:       10,
			Consistency: "eventual",
		}
		resp, eventualErr := h.Handle(ctx, "test-ns", eventualReq)
		if eventualErr != nil {
			t.Fatalf("expected eventual query to succeed, got error: %v", eventualErr)
		}
		if resp == nil {
			t.Fatal("expected non-nil response from eventual query")
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows from eventual query, got %d", len(resp.Rows))
		}
	})
}

// TestEventualQueryWithVectorSearch verifies that eventual consistency also works
// for vector ANN queries when object store is unavailable.
func TestEventualQueryWithVectorSearch(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Write state directly
	if err := writeTestState(ctx, store, "test-ns", 5, 2); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	// Mock tail store with vector data that would fail refresh
	cachedDocs := []*tail.Document{
		{
			ID:         document.NewU64ID(1),
			Vector:     []float32{1.0, 0.0, 0.0},
			Attributes: map[string]any{"name": "vec-doc-1"},
		},
		{
			ID:         document.NewU64ID(2),
			Vector:     []float32{0.0, 1.0, 0.0},
			Attributes: map[string]any{"name": "vec-doc-2"},
		},
	}

	mockTail := &mockTailStoreForRefreshFailure{
		docs:       cachedDocs,
		refreshErr: errors.New("object storage unavailable"),
	}

	h := query.NewHandler(store, stateMan, mockTail)

	t.Run("eventual vector query succeeds from cache", func(t *testing.T) {
		req := &query.QueryRequest{
			RankBy:      []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Limit:       10,
			Consistency: "eventual",
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("expected eventual vector query to succeed, got error: %v", err)
		}
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(resp.Rows))
		}
		// Verify distance is included in results
		if resp.Rows[0].Dist == nil {
			t.Error("expected $dist in vector query results")
		}
	})
}
