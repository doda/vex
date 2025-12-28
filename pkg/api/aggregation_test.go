package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/logging"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// mockAggTailStore is a mock tail store for aggregation tests
type mockAggTailStore struct {
	docs []*tail.Document
}

func (m *mockAggTailStore) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *mockAggTailStore) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	if f == nil {
		return m.docs, nil
	}
	var result []*tail.Document
	for _, doc := range m.docs {
		if f.Eval(doc.Attributes) {
			result = append(result, doc)
		}
	}
	return result, nil
}

func (m *mockAggTailStore) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.Scan(ctx, ns, f)
}

func (m *mockAggTailStore) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *mockAggTailStore) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *mockAggTailStore) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *mockAggTailStore) TailBytes(ns string) int64 { return 0 }
func (m *mockAggTailStore) Clear(ns string)           {}
func (m *mockAggTailStore) AddWALEntry(ns string, entry *wal.WalEntry) {}
func (m *mockAggTailStore) Close() error { return nil }

func TestAggregationAPIResponse(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Create mock tail store with test documents
	mockTail := &mockAggTailStore{
		docs: []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"category": "A", "price": float64(10.5), "qty": int64(2)}},
			{ID: document.NewU64ID(2), Attributes: map[string]any{"category": "B", "price": float64(20.0), "qty": int64(3)}},
			{ID: document.NewU64ID(3), Attributes: map[string]any{"category": "A", "price": float64(15.0), "qty": int64(1)}},
		},
	}

	// Create query handler with mock tail store
	queryHandler := query.NewHandler(store, stateMan, mockTail)

	cfg := &config.Config{
		AuthToken: "test-token",
	}

	// Create router directly with injected dependencies
	router := &Router{
		cfg: cfg,
		mux: http.NewServeMux(),
		state: &ServerState{
			Namespaces: map[string]*NamespaceState{
				"test-ns": {Exists: true},
			},
			ObjectStore: ObjectStoreState{Available: true},
		},
		logger:       logging.New(),
		store:        store,
		stateManager: stateMan,
		tailStore:    mockTail,
		queryHandler: queryHandler,
	}
	// Register routes
	router.mux.HandleFunc("POST /v2/namespaces/{namespace}/query", router.authMiddleware(router.validateNamespace(router.handleQuery)))

	t.Run("aggregate_by returns aggregations object", func(t *testing.T) {
		body := map[string]any{
			"aggregate_by": map[string]any{
				"total_count": []any{"Count"},
			},
		}
		jsonBody, _ := json.Marshal(body)

		req := httptest.NewRequest(http.MethodPost, "/v2/namespaces/test-ns/query", bytes.NewReader(jsonBody))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Check that aggregations object exists
		aggs, ok := resp["aggregations"].(map[string]any)
		if !ok {
			t.Fatal("expected aggregations object in response")
		}

		// Check the count value
		if aggs["total_count"] != float64(3) {
			t.Errorf("expected total_count 3, got %v", aggs["total_count"])
		}

		// Check that rows is NOT in the response
		if _, ok := resp["rows"]; ok {
			t.Error("expected no rows in aggregation response")
		}

		// Check billing and performance are present
		if _, ok := resp["billing"]; !ok {
			t.Error("expected billing in response")
		}
		if _, ok := resp["performance"]; !ok {
			t.Error("expected performance in response")
		}
	})

	t.Run("aggregations compute correctly over result set", func(t *testing.T) {
		body := map[string]any{
			"aggregate_by": map[string]any{
				"count":       []any{"Count"},
				"total_price": []any{"Sum", "price"},
				"total_qty":   []any{"Sum", "qty"},
			},
		}
		jsonBody, _ := json.Marshal(body)

		req := httptest.NewRequest(http.MethodPost, "/v2/namespaces/test-ns/query", bytes.NewReader(jsonBody))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		aggs, ok := resp["aggregations"].(map[string]any)
		if !ok {
			t.Fatal("expected aggregations object in response")
		}

		// Verify count
		if aggs["count"] != float64(3) {
			t.Errorf("expected count 3, got %v", aggs["count"])
		}

		// Verify sum of prices (10.5 + 20.0 + 15.0 = 45.5)
		expectedPriceSum := 45.5
		if aggs["total_price"] != expectedPriceSum {
			t.Errorf("expected total_price %v, got %v", expectedPriceSum, aggs["total_price"])
		}

		// Verify sum of quantities (2 + 3 + 1 = 6)
		expectedQtySum := float64(6)
		if aggs["total_qty"] != expectedQtySum {
			t.Errorf("expected total_qty %v, got %v", expectedQtySum, aggs["total_qty"])
		}
	})

	t.Run("aggregation with filter", func(t *testing.T) {
		body := map[string]any{
			"aggregate_by": map[string]any{
				"count": []any{"Count"},
			},
			"filters": []any{"category", "Eq", "A"},
		}
		jsonBody, _ := json.Marshal(body)

		req := httptest.NewRequest(http.MethodPost, "/v2/namespaces/test-ns/query", bytes.NewReader(jsonBody))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		aggs, ok := resp["aggregations"].(map[string]any)
		if !ok {
			t.Fatal("expected aggregations object in response")
		}

		// Should only count 2 docs with category "A"
		if aggs["count"] != float64(2) {
			t.Errorf("expected count 2 for category A, got %v", aggs["count"])
		}
	})

	t.Run("invalid aggregate_by returns 400", func(t *testing.T) {
		body := map[string]any{
			"aggregate_by": map[string]any{
				"bad": []any{"UnknownFunction"},
			},
		}
		jsonBody, _ := json.Marshal(body)

		req := httptest.NewRequest(http.MethodPost, "/v2/namespaces/test-ns/query", bytes.NewReader(jsonBody))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("group_by returns aggregation_groups", func(t *testing.T) {
		body := map[string]any{
			"aggregate_by": map[string]any{
				"count": []any{"Count"},
			},
			"group_by": []any{"category"},
		}
		jsonBody, _ := json.Marshal(body)

		req := httptest.NewRequest(http.MethodPost, "/v2/namespaces/test-ns/query", bytes.NewReader(jsonBody))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Check that aggregation_groups array exists
		aggGroups, ok := resp["aggregation_groups"].([]any)
		if !ok {
			t.Fatal("expected aggregation_groups array in response")
		}

		// Should have 2 groups (A and B)
		if len(aggGroups) != 2 {
			t.Errorf("expected 2 groups, got %d", len(aggGroups))
		}

		// Check that rows is NOT in the response
		if _, ok := resp["rows"]; ok {
			t.Error("expected no rows in grouped aggregation response")
		}

		// Check that aggregations is NOT in the response (it's groups, not aggregations)
		if _, ok := resp["aggregations"]; ok {
			t.Error("expected no aggregations in grouped aggregation response")
		}

		// Check billing and performance are present
		if _, ok := resp["billing"]; !ok {
			t.Error("expected billing in response")
		}
		if _, ok := resp["performance"]; !ok {
			t.Error("expected performance in response")
		}
	})

	t.Run("group_by groups are computed correctly", func(t *testing.T) {
		body := map[string]any{
			"aggregate_by": map[string]any{
				"count":       []any{"Count"},
				"total_price": []any{"Sum", "price"},
			},
			"group_by": []any{"category"},
		}
		jsonBody, _ := json.Marshal(body)

		req := httptest.NewRequest(http.MethodPost, "/v2/namespaces/test-ns/query", bytes.NewReader(jsonBody))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		aggGroups, ok := resp["aggregation_groups"].([]any)
		if !ok {
			t.Fatal("expected aggregation_groups array in response")
		}

		// Find groups and verify counts and sums
		for _, g := range aggGroups {
			group := g.(map[string]any)
			cat := group["category"]
			if cat == "A" {
				// Category A: 2 docs (10.5 + 15.0 = 25.5)
				if group["count"] != float64(2) {
					t.Errorf("expected count 2 for category A, got %v", group["count"])
				}
				if group["total_price"] != 25.5 {
					t.Errorf("expected total_price 25.5 for category A, got %v", group["total_price"])
				}
			} else if cat == "B" {
				// Category B: 1 doc (20.0)
				if group["count"] != float64(1) {
					t.Errorf("expected count 1 for category B, got %v", group["count"])
				}
				if group["total_price"] != 20.0 {
					t.Errorf("expected total_price 20.0 for category B, got %v", group["total_price"])
				}
			}
		}
	})

	t.Run("group_by without aggregate_by returns 400", func(t *testing.T) {
		body := map[string]any{
			"rank_by":  []any{"id", "asc"},
			"group_by": []any{"category"},
		}
		jsonBody, _ := json.Marshal(body)

		req := httptest.NewRequest(http.MethodPost, "/v2/namespaces/test-ns/query", bytes.NewReader(jsonBody))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("invalid group_by format returns 400", func(t *testing.T) {
		body := map[string]any{
			"aggregate_by": map[string]any{
				"count": []any{"Count"},
			},
			"group_by": "not_an_array",
		}
		jsonBody, _ := json.Marshal(body)

		req := httptest.NewRequest(http.MethodPost, "/v2/namespaces/test-ns/query", bytes.NewReader(jsonBody))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}
