package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/write"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestMultiQueryAPI(t *testing.T) {
	// Set up router with real object store
	cfg := &config.Config{}
	r := NewRouter(cfg)

	store := objectstore.NewMemoryStore()
	if err := r.SetStore(store); err != nil {
		t.Fatalf("Failed to set store: %v", err)
	}
	defer r.Close()

	// Create a namespace with some test data
	ctx := httptest.NewRequest("GET", "/", nil).Context()
	stateMan := namespace.NewStateManager(store)
	if _, err := stateMan.Create(ctx, "test-ns"); err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Write some test documents
	writeReq := &write.WriteRequest{
		UpsertRows: []map[string]any{
			{"id": uint64(1), "name": "doc1", "score": int64(100)},
			{"id": uint64(2), "name": "doc2", "score": int64(200)},
			{"id": uint64(3), "name": "doc3", "score": int64(150)},
		},
	}
	if _, err := r.writeHandler.Handle(ctx, "test-ns", writeReq); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	// Mark namespace as existing
	r.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("basic multi-query", func(t *testing.T) {
		body := map[string]any{
			"queries": []any{
				map[string]any{"rank_by": []any{"id", "asc"}, "limit": 10},
				map[string]any{"rank_by": []any{"score", "desc"}, "limit": 2},
			},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("Failed to parse response: %v", err)
		}

		// Should have results array
		results, ok := resp["results"].([]any)
		if !ok {
			t.Fatalf("expected results array in response, got %T", resp["results"])
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}

		// Should have billing and performance
		if _, ok := resp["billing"]; !ok {
			t.Error("expected billing in response")
		}
		if _, ok := resp["performance"]; !ok {
			t.Error("expected performance in response")
		}
	})

	t.Run("multi-query results in correct order", func(t *testing.T) {
		// Test that results array order matches queries array order
		// We use different query types to verify the ordering is preserved
		body := map[string]any{
			"queries": []any{
				map[string]any{"rank_by": []any{"id", "desc"}, "limit": 10},    // Regular query
				map[string]any{"aggregate_by": map[string]any{"c": []any{"Count"}}}, // Aggregation query
			},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
		}

		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)

		results := resp["results"].([]any)
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}

		// First result should have rows (rank_by query)
		firstResult := results[0].(map[string]any)
		if _, ok := firstResult["rows"]; !ok {
			t.Error("expected first result to have 'rows' key (rank_by query)")
		}
		if _, ok := firstResult["aggregations"]; ok {
			t.Error("first result should not have 'aggregations'")
		}

		// Second result should have aggregations (aggregate_by query)
		secondResult := results[1].(map[string]any)
		if _, ok := secondResult["aggregations"]; !ok {
			t.Error("expected second result to have 'aggregations' key (aggregate_by query)")
		}
		if _, ok := secondResult["rows"]; ok {
			t.Error("second result should not have 'rows'")
		}
	})

	t.Run("too many subqueries returns 400", func(t *testing.T) {
		queries := make([]any, 17) // More than MaxMultiQuery (16)
		for i := range queries {
			queries[i] = map[string]any{"rank_by": []any{"id", "asc"}, "limit": 1}
		}
		body := map[string]any{"queries": queries}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("16 subqueries is allowed", func(t *testing.T) {
		queries := make([]any, 16) // Exactly MaxMultiQuery
		for i := range queries {
			queries[i] = map[string]any{"rank_by": []any{"id", "asc"}, "limit": 1}
		}
		body := map[string]any{"queries": queries}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
		}

		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)

		results := resp["results"].([]any)
		if len(results) != 16 {
			t.Errorf("expected 16 results, got %d", len(results))
		}
	})

	t.Run("empty queries array returns 400", func(t *testing.T) {
		body := map[string]any{"queries": []any{}}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("invalid subquery returns 400", func(t *testing.T) {
		body := map[string]any{
			"queries": []any{
				map[string]any{"rank_by": []any{"id", "asc"}, "limit": 10},
				map[string]any{"limit": 10}, // Missing rank_by
			},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("queries not array returns 400", func(t *testing.T) {
		body := map[string]any{"queries": "not an array"}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("multi-query with aggregation subquery", func(t *testing.T) {
		body := map[string]any{
			"queries": []any{
				map[string]any{"rank_by": []any{"id", "asc"}, "limit": 10},
				map[string]any{"aggregate_by": map[string]any{"total": []any{"Count"}}},
			},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
		}

		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)

		results := resp["results"].([]any)
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}

		// First result should have rows
		firstResult := results[0].(map[string]any)
		if _, ok := firstResult["rows"]; !ok {
			t.Error("expected rows in first result")
		}

		// Second result should have aggregations
		secondResult := results[1].(map[string]any)
		if _, ok := secondResult["aggregations"]; !ok {
			t.Error("expected aggregations in second result")
		}
	})

	t.Run("multi-query with consistency", func(t *testing.T) {
		body := map[string]any{
			"queries": []any{
				map[string]any{"rank_by": []any{"id", "asc"}, "limit": 10},
			},
			"consistency": "eventual",
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
		}
	})

	t.Run("multi-query on non-existent namespace returns 404", func(t *testing.T) {
		r.SetState(&ServerState{
			Namespaces: map[string]*NamespaceState{
				"test-ns": {Exists: true},
				// no-ns is not set
			},
			ObjectStore: ObjectStoreState{Available: true},
		})

		body := map[string]any{
			"queries": []any{
				map[string]any{"rank_by": []any{"id", "asc"}, "limit": 10},
			},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/no-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
		}
	})
}

func TestMultiQueryFallbackMode(t *testing.T) {
	// Test fallback mode when queryHandler is not available
	cfg := &config.Config{}
	r := NewRouter(cfg)

	// Mark namespace as existing
	r.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("fallback returns empty results", func(t *testing.T) {
		body := map[string]any{
			"queries": []any{
				map[string]any{"rank_by": []any{"id", "asc"}, "limit": 10},
				map[string]any{"rank_by": []any{"id", "desc"}, "limit": 10},
			},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
		}

		var resp map[string]any
		json.Unmarshal(w.Body.Bytes(), &resp)

		results := resp["results"].([]any)
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}

		// Each result should have empty rows
		for i, res := range results {
			result := res.(map[string]any)
			rows := result["rows"].([]any)
			if len(rows) != 0 {
				t.Errorf("expected empty rows in result %d, got %d", i, len(rows))
			}
		}
	})

	t.Run("fallback validates too many subqueries", func(t *testing.T) {
		queries := make([]any, 17)
		for i := range queries {
			queries[i] = map[string]any{"rank_by": []any{"id", "asc"}, "limit": 1}
		}
		body := map[string]any{"queries": queries}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})
}
