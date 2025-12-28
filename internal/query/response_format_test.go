package query

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestQueryResponseFormat verifies the complete query response format
// including rows, billing, and performance objects.
func TestQueryResponseFormat(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	mockTail := &mockTailStore{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"name": "doc1", "score": int64(100)},
				Vector:     []float32{1.0, 0.0, 0.0},
			},
			{
				ID:         document.NewU64ID(2),
				Attributes: map[string]any{"name": "doc2", "score": int64(200)},
				Vector:     []float32{0.0, 1.0, 0.0},
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	t.Run("response has billing object", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"id", "asc"},
			Limit:  10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		// Billing must be present with the expected fields
		// The values can be zero, but the structure must be correct
		if resp.Billing.BillableLogicalBytesQueried < 0 {
			t.Error("BillableLogicalBytesQueried must be >= 0")
		}
		if resp.Billing.BillableLogicalBytesReturned < 0 {
			t.Error("BillableLogicalBytesReturned must be >= 0")
		}
	})

	t.Run("response has performance object", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"id", "asc"},
			Limit:  10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		// Performance must be present with cache_temperature and server_total_ms
		if resp.Performance.CacheTemperature == "" {
			t.Error("CacheTemperature must be set")
		}
		// server_total_ms can be 0 for fast queries
		if resp.Performance.ServerTotalMs < 0 {
			t.Error("ServerTotalMs must be >= 0")
		}
	})

	t.Run("vector query row has $dist", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Limit:  10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) == 0 {
			t.Fatal("expected rows in response")
		}

		for i, row := range resp.Rows {
			if row.Dist == nil {
				t.Errorf("row[%d]: $dist must be present for vector ANN queries", i)
			}
			if row.ID == nil {
				t.Errorf("row[%d]: id must be present", i)
			}
		}
	})

	t.Run("order-by query row omits $dist", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"score", "desc"},
			Limit:  10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) == 0 {
			t.Fatal("expected rows in response")
		}

		for i, row := range resp.Rows {
			if row.Dist != nil {
				t.Errorf("row[%d]: $dist should be omitted for order-by queries", i)
			}
			if row.ID == nil {
				t.Errorf("row[%d]: id must be present", i)
			}
		}
	})

	t.Run("row includes attributes", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"id", "asc"},
			Limit:  10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) == 0 {
			t.Fatal("expected rows in response")
		}

		// Check that attributes are included
		for i, row := range resp.Rows {
			if row.Attributes == nil {
				t.Errorf("row[%d]: attributes should be present", i)
			}
			if _, ok := row.Attributes["name"]; !ok {
				t.Errorf("row[%d]: expected 'name' attribute", i)
			}
		}
	})
}

// TestRowToJSONFormat verifies that RowToJSON produces the correct format
// with id, $dist, and merged attributes.
func TestRowToJSONFormat(t *testing.T) {
	t.Run("complete row with $dist and attributes", func(t *testing.T) {
		dist := 0.25
		row := Row{
			ID:   uint64(42),
			Dist: &dist,
			Attributes: map[string]any{
				"name":  "test",
				"count": int64(10),
			},
		}

		result := RowToJSON(row)

		// Verify id is present
		if result["id"] != uint64(42) {
			t.Errorf("expected id=42, got %v", result["id"])
		}

		// Verify $dist is present and correct
		if result["$dist"] != 0.25 {
			t.Errorf("expected $dist=0.25, got %v", result["$dist"])
		}

		// Verify attributes are merged
		if result["name"] != "test" {
			t.Errorf("expected name='test', got %v", result["name"])
		}
		if result["count"] != int64(10) {
			t.Errorf("expected count=10, got %v", result["count"])
		}
	})

	t.Run("row without $dist for order-by", func(t *testing.T) {
		row := Row{
			ID:   "string-id",
			Dist: nil,
			Attributes: map[string]any{
				"value": "abc",
			},
		}

		result := RowToJSON(row)

		// Verify id is present
		if result["id"] != "string-id" {
			t.Errorf("expected id='string-id', got %v", result["id"])
		}

		// Verify $dist is NOT present
		if _, ok := result["$dist"]; ok {
			t.Error("$dist should not be present for order-by queries")
		}

		// Verify attributes are merged
		if result["value"] != "abc" {
			t.Errorf("expected value='abc', got %v", result["value"])
		}
	})

	t.Run("row with UUID id", func(t *testing.T) {
		dist := 0.5
		row := Row{
			ID:         "550e8400-e29b-41d4-a716-446655440000",
			Dist:       &dist,
			Attributes: map[string]any{},
		}

		result := RowToJSON(row)

		if result["id"] != "550e8400-e29b-41d4-a716-446655440000" {
			t.Errorf("expected UUID id, got %v", result["id"])
		}
	})

	t.Run("row with empty attributes", func(t *testing.T) {
		dist := 0.1
		row := Row{
			ID:         uint64(1),
			Dist:       &dist,
			Attributes: map[string]any{},
		}

		result := RowToJSON(row)

		// Should have id and $dist, no other keys
		if len(result) != 2 {
			t.Errorf("expected 2 keys (id, $dist), got %d", len(result))
		}
	})
}

// TestBillingInfoSerialization verifies BillingInfo JSON serialization.
func TestBillingInfoSerialization(t *testing.T) {
	t.Run("zero values serialize correctly", func(t *testing.T) {
		billing := BillingInfo{
			BillableLogicalBytesQueried:  0,
			BillableLogicalBytesReturned: 0,
		}

		data, err := json.Marshal(billing)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded map[string]any
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if _, ok := decoded["billable_logical_bytes_queried"]; !ok {
			t.Error("billable_logical_bytes_queried must be present even when 0")
		}
		if _, ok := decoded["billable_logical_bytes_returned"]; !ok {
			t.Error("billable_logical_bytes_returned must be present even when 0")
		}
	})

	t.Run("non-zero values serialize correctly", func(t *testing.T) {
		billing := BillingInfo{
			BillableLogicalBytesQueried:  1000,
			BillableLogicalBytesReturned: 500,
		}

		data, err := json.Marshal(billing)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded map[string]any
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded["billable_logical_bytes_queried"] != float64(1000) {
			t.Errorf("expected 1000, got %v", decoded["billable_logical_bytes_queried"])
		}
		if decoded["billable_logical_bytes_returned"] != float64(500) {
			t.Errorf("expected 500, got %v", decoded["billable_logical_bytes_returned"])
		}
	})
}

// TestPerformanceInfoSerialization verifies PerformanceInfo JSON serialization.
func TestPerformanceInfoSerialization(t *testing.T) {
	t.Run("performance fields serialize correctly", func(t *testing.T) {
		perf := PerformanceInfo{
			CacheTemperature: "cold",
			ServerTotalMs:    0,
		}

		data, err := json.Marshal(perf)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded map[string]any
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded["cache_temperature"] != "cold" {
			t.Errorf("expected cache_temperature='cold', got %v", decoded["cache_temperature"])
		}
		if _, ok := decoded["server_total_ms"]; !ok {
			t.Error("server_total_ms must be present")
		}
	})

	t.Run("warm cache temperature", func(t *testing.T) {
		perf := PerformanceInfo{
			CacheTemperature: "warm",
			ServerTotalMs:    10.5,
		}

		data, err := json.Marshal(perf)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded map[string]any
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded["cache_temperature"] != "warm" {
			t.Errorf("expected cache_temperature='warm', got %v", decoded["cache_temperature"])
		}
		if decoded["server_total_ms"] != 10.5 {
			t.Errorf("expected server_total_ms=10.5, got %v", decoded["server_total_ms"])
		}
	})

	t.Run("hot cache temperature", func(t *testing.T) {
		perf := PerformanceInfo{
			CacheTemperature: "hot",
			ServerTotalMs:    2.1,
		}

		data, err := json.Marshal(perf)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded map[string]any
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded["cache_temperature"] != "hot" {
			t.Errorf("expected cache_temperature='hot', got %v", decoded["cache_temperature"])
		}
	})
}

// TestQueryResponseSerialization verifies the complete QueryResponse JSON format.
func TestQueryResponseSerialization(t *testing.T) {
	t.Run("complete response serialization", func(t *testing.T) {
		dist := 0.5
		resp := QueryResponse{
			Rows: []Row{
				{
					ID:   uint64(1),
					Dist: &dist,
					Attributes: map[string]any{
						"name": "test",
					},
				},
			},
			Billing: BillingInfo{
				BillableLogicalBytesQueried:  100,
				BillableLogicalBytesReturned: 50,
			},
			Performance: PerformanceInfo{
				CacheTemperature: "cold",
				ServerTotalMs:    5.2,
			},
		}

		data, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded map[string]any
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		// Verify all top-level keys present
		if _, ok := decoded["rows"]; !ok {
			t.Error("rows must be present")
		}
		if _, ok := decoded["billing"]; !ok {
			t.Error("billing must be present")
		}
		if _, ok := decoded["performance"]; !ok {
			t.Error("performance must be present")
		}
	})

	t.Run("aggregation response has no rows", func(t *testing.T) {
		resp := QueryResponse{
			Aggregations: map[string]any{
				"count": int64(10),
			},
			Billing: BillingInfo{
				BillableLogicalBytesQueried:  0,
				BillableLogicalBytesReturned: 0,
			},
			Performance: PerformanceInfo{
				CacheTemperature: "cold",
				ServerTotalMs:    0,
			},
		}

		data, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded map[string]any
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		// Rows should be omitted (omitempty), aggregations present
		if _, ok := decoded["rows"]; ok {
			t.Error("rows should be omitted for aggregation queries")
		}
		if _, ok := decoded["aggregations"]; !ok {
			t.Error("aggregations must be present")
		}
	})
}

// TestMultiQueryResponseFormat verifies multi-query response format.
func TestMultiQueryResponseFormat(t *testing.T) {
	t.Run("multi-query response has results and billing/performance", func(t *testing.T) {
		resp := MultiQueryResponse{
			Results: []SubQueryResult{
				{Rows: []Row{{ID: uint64(1)}}},
				{Aggregations: map[string]any{"count": int64(5)}},
			},
			Billing: BillingInfo{
				BillableLogicalBytesQueried:  0,
				BillableLogicalBytesReturned: 0,
			},
			Performance: PerformanceInfo{
				CacheTemperature: "cold",
				ServerTotalMs:    0,
			},
		}

		data, err := json.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded map[string]any
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		// Verify results array is present
		results, ok := decoded["results"].([]any)
		if !ok {
			t.Fatal("results must be an array")
		}
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}

		// Verify billing present
		if _, ok := decoded["billing"]; !ok {
			t.Error("billing must be present")
		}

		// Verify performance present
		if _, ok := decoded["performance"]; !ok {
			t.Error("performance must be present")
		}
	})
}
