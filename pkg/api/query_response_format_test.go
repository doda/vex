package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestQueryResponseFormatIntegration tests the complete query response format at the API level.
// This verifies the task "query-response-format" requirements:
// 1. Verify rows array format with id, $dist, attributes
// 2. Test billing object is present (can be zeros)
// 3. Test performance object with cache metrics and timing
func TestQueryResponseFormatIntegration(t *testing.T) {
	cfg := &config.Config{AuthToken: testAuthToken}
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace and write some test documents
	ctx := context.Background()
	if router.stateManager == nil {
		t.Skip("stateManager not available")
	}

	_, err := router.stateManager.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Set up namespace state (required for query handler path)
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	// Add documents to tail store for querying via WAL entry
	if ts, ok := router.tailStore.(*tail.TailStore); ok {
		entry := wal.NewWalEntry("test-ns", 1)
		batch := wal.NewWriteSubBatch("req1")
		batch.AddUpsert(
			&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
			map[string]*wal.AttributeValue{
				"name":  wal.StringValue("doc1"),
				"score": wal.IntValue(100),
			},
			nil, 0,
		)
		batch.AddUpsert(
			&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 2}},
			map[string]*wal.AttributeValue{
				"name":  wal.StringValue("doc2"),
				"score": wal.IntValue(200),
			},
			nil, 0,
		)
		entry.SubBatches = append(entry.SubBatches, batch)
		ts.AddWALEntry("test-ns", entry)
	}

	t.Run("rows array format with id and attributes", func(t *testing.T) {
		body := map[string]any{
			"rank_by": []any{"id", "asc"},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeAuthed(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		// Verify rows array exists
		rows, ok := resp["rows"].([]any)
		if !ok {
			t.Fatal("response must include 'rows' array")
		}

		// Verify each row has id and attributes
		for i, rowAny := range rows {
			row, ok := rowAny.(map[string]any)
			if !ok {
				t.Errorf("row[%d] must be an object", i)
				continue
			}

			// id must be present
			if _, ok := row["id"]; !ok {
				t.Errorf("row[%d] must have 'id'", i)
			}

			// For order-by queries, $dist should be absent
			if _, ok := row["$dist"]; ok {
				t.Errorf("row[%d] should not have '$dist' for order-by query", i)
			}
		}
	})

	t.Run("vector query rows include $dist", func(t *testing.T) {
		body := map[string]any{
			"rank_by": []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeAuthed(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		rows, ok := resp["rows"].([]any)
		if !ok {
			t.Fatal("response must include 'rows' array")
		}

		// For vector queries, each row must have $dist
		for i, rowAny := range rows {
			row, ok := rowAny.(map[string]any)
			if !ok {
				t.Errorf("row[%d] must be an object", i)
				continue
			}

			if _, ok := row["$dist"]; !ok {
				t.Errorf("row[%d] must have '$dist' for vector query", i)
			}
			if _, ok := row["id"]; !ok {
				t.Errorf("row[%d] must have 'id'", i)
			}
		}
	})

	t.Run("billing object is present", func(t *testing.T) {
		body := map[string]any{
			"rank_by": []any{"id", "asc"},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeAuthed(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		billing, ok := resp["billing"].(map[string]any)
		if !ok {
			t.Fatal("response must include 'billing' object")
		}

		// Verify billing fields exist (can be zeros)
		if _, ok := billing["billable_logical_bytes_queried"]; !ok {
			t.Error("billing must include 'billable_logical_bytes_queried'")
		}
		if _, ok := billing["billable_logical_bytes_returned"]; !ok {
			t.Error("billing must include 'billable_logical_bytes_returned'")
		}
	})

	t.Run("performance object with cache metrics and timing", func(t *testing.T) {
		body := map[string]any{
			"rank_by": []any{"id", "asc"},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeAuthed(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		perf, ok := resp["performance"].(map[string]any)
		if !ok {
			t.Fatal("response must include 'performance' object")
		}

		// Verify cache_temperature is present and a string
		cacheTemp, ok := perf["cache_temperature"]
		if !ok {
			t.Error("performance must include 'cache_temperature'")
		} else {
			if _, ok := cacheTemp.(string); !ok {
				t.Errorf("cache_temperature must be a string, got %T", cacheTemp)
			}
		}

		// Verify server_total_ms is present
		if _, ok := perf["server_total_ms"]; !ok {
			t.Error("performance must include 'server_total_ms'")
		}
	})

	t.Run("aggregation response omits rows", func(t *testing.T) {
		body := map[string]any{
			"aggregate_by": map[string]any{"count": []any{"Count"}},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeAuthed(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		// Should have aggregations, not rows
		if _, ok := resp["rows"]; ok {
			t.Error("aggregation response should not include 'rows'")
		}
		if _, ok := resp["aggregations"]; !ok {
			t.Error("aggregation response must include 'aggregations'")
		}

		// Should still have billing and performance
		if _, ok := resp["billing"]; !ok {
			t.Error("aggregation response must include 'billing'")
		}
		if _, ok := resp["performance"]; !ok {
			t.Error("aggregation response must include 'performance'")
		}
	})
}

// TestQueryResponseFormatFallback tests the fallback response format when query handler is not available.
func TestQueryResponseFormatFallback(t *testing.T) {
	cfg := &config.Config{AuthToken: testAuthToken}
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("fallback response includes billing", func(t *testing.T) {
		body := map[string]any{
			"rank_by": []any{"id", "asc"},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeAuthed(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		billing, ok := resp["billing"].(map[string]any)
		if !ok {
			t.Fatal("fallback response must include 'billing' object")
		}
		if _, ok := billing["billable_logical_bytes_queried"]; !ok {
			t.Error("billing must include 'billable_logical_bytes_queried'")
		}
		if _, ok := billing["billable_logical_bytes_returned"]; !ok {
			t.Error("billing must include 'billable_logical_bytes_returned'")
		}
	})

	t.Run("fallback response includes performance", func(t *testing.T) {
		body := map[string]any{
			"rank_by": []any{"id", "asc"},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeAuthed(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		perf, ok := resp["performance"].(map[string]any)
		if !ok {
			t.Fatal("fallback response must include 'performance' object")
		}
		if _, ok := perf["cache_temperature"]; !ok {
			t.Error("performance must include 'cache_temperature'")
		}
		if _, ok := perf["server_total_ms"]; !ok {
			t.Error("performance must include 'server_total_ms'")
		}
	})

	t.Run("fallback response includes empty rows array", func(t *testing.T) {
		body := map[string]any{
			"rank_by": []any{"id", "asc"},
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeAuthed(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}

		rows, ok := resp["rows"].([]any)
		if !ok {
			t.Fatal("fallback response must include 'rows' array")
		}
		if len(rows) != 0 {
			t.Errorf("fallback rows should be empty, got %d", len(rows))
		}
	})
}

// TestQueryResponse202IncludesBillingAndPerformance verifies that 202 responses also include
// billing and performance objects.
func TestQueryResponse202IncludesBillingAndPerformance(t *testing.T) {
	cfg := &config.Config{AuthToken: testAuthToken}
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"building-ns": {Exists: true, IndexBuilding: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	body := map[string]any{
		"rank_by": []any{"id", "asc"},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/building-ns/query", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeAuthed(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// Even 202 responses should have billing and performance
	if _, ok := resp["billing"]; !ok {
		t.Error("202 response must include 'billing' object")
	}
	if _, ok := resp["performance"]; !ok {
		t.Error("202 response must include 'performance' object")
	}
	if _, ok := resp["rows"]; !ok {
		t.Error("202 response must include 'rows' array")
	}
}
