package query

import (
	"context"
	"errors"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestHandleMultiQuery(t *testing.T) {
	// Create a handler with mock dependencies
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create namespace state
	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Create mock tail store with test documents
	mockTail := &mockTailStore{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"name": "doc1", "category": "a", "score": int64(100)},
				Vector:     []float32{1.0, 0.0, 0.0},
			},
			{
				ID:         document.NewU64ID(2),
				Attributes: map[string]any{"name": "doc2", "category": "b", "score": int64(200)},
				Vector:     []float32{0.0, 1.0, 0.0},
			},
			{
				ID:         document.NewU64ID(3),
				Attributes: map[string]any{"name": "doc3", "category": "a", "score": int64(150)},
				Vector:     []float32{0.0, 0.0, 1.0},
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	t.Run("basic multi-query with two subqueries", func(t *testing.T) {
		queries := []map[string]any{
			{"rank_by": []any{"id", "asc"}, "limit": 10},
			{"rank_by": []any{"score", "desc"}, "limit": 2},
		}
		resp, err := h.HandleMultiQuery(ctx, "test-ns", queries, "")
		if err != nil {
			t.Fatalf("HandleMultiQuery() error = %v", err)
		}
		if len(resp.Results) != 2 {
			t.Errorf("expected 2 results, got %d", len(resp.Results))
		}
		// First subquery should return 3 docs ordered by id asc
		if len(resp.Results[0].Rows) != 3 {
			t.Errorf("expected 3 rows in first result, got %d", len(resp.Results[0].Rows))
		}
		// Second subquery should return 2 docs ordered by score desc
		if len(resp.Results[1].Rows) != 2 {
			t.Errorf("expected 2 rows in second result, got %d", len(resp.Results[1].Rows))
		}
	})

	t.Run("results in same order as queries", func(t *testing.T) {
		queries := []map[string]any{
			{"rank_by": []any{"id", "desc"}, "limit": 1},
			{"rank_by": []any{"id", "asc"}, "limit": 1},
		}
		resp, err := h.HandleMultiQuery(ctx, "test-ns", queries, "")
		if err != nil {
			t.Fatalf("HandleMultiQuery() error = %v", err)
		}
		if len(resp.Results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(resp.Results))
		}
		// First subquery: id desc - should return doc3
		if len(resp.Results[0].Rows) != 1 {
			t.Errorf("expected 1 row in first result, got %d", len(resp.Results[0].Rows))
		}
		if resp.Results[0].Rows[0].ID != uint64(3) {
			t.Errorf("expected first result row id=3, got %v", resp.Results[0].Rows[0].ID)
		}
		// Second subquery: id asc - should return doc1
		if len(resp.Results[1].Rows) != 1 {
			t.Errorf("expected 1 row in second result, got %d", len(resp.Results[1].Rows))
		}
		if resp.Results[1].Rows[0].ID != uint64(1) {
			t.Errorf("expected second result row id=1, got %v", resp.Results[1].Rows[0].ID)
		}
	})

	t.Run("mixed query types (vector and attribute)", func(t *testing.T) {
		queries := []map[string]any{
			{"rank_by": []any{"vector", "ANN", []any{1.0, 0.0, 0.0}}, "limit": 2},
			{"rank_by": []any{"name", "asc"}, "limit": 3},
		}
		resp, err := h.HandleMultiQuery(ctx, "test-ns", queries, "")
		if err != nil {
			t.Fatalf("HandleMultiQuery() error = %v", err)
		}
		if len(resp.Results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(resp.Results))
		}
		// First result: vector query should have $dist
		if len(resp.Results[0].Rows) != 2 {
			t.Errorf("expected 2 rows in vector result, got %d", len(resp.Results[0].Rows))
		}
		if resp.Results[0].Rows[0].Dist == nil {
			t.Error("expected $dist for vector query result")
		}
		// Second result: attribute order-by should not have $dist
		if len(resp.Results[1].Rows) != 3 {
			t.Errorf("expected 3 rows in attribute result, got %d", len(resp.Results[1].Rows))
		}
		if resp.Results[1].Rows[0].Dist != nil {
			t.Error("expected no $dist for attribute order-by result")
		}
	})

	t.Run("mixed query types (rows and aggregations)", func(t *testing.T) {
		queries := []map[string]any{
			{"rank_by": []any{"id", "asc"}, "limit": 10},
			{"aggregate_by": map[string]any{"total": []any{"Count"}}},
		}
		resp, err := h.HandleMultiQuery(ctx, "test-ns", queries, "")
		if err != nil {
			t.Fatalf("HandleMultiQuery() error = %v", err)
		}
		if len(resp.Results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(resp.Results))
		}
		// First result: rows
		if len(resp.Results[0].Rows) == 0 {
			t.Error("expected rows in first result")
		}
		if resp.Results[0].Aggregations != nil {
			t.Error("first result should not have aggregations")
		}
		// Second result: aggregations
		if resp.Results[1].Rows != nil {
			t.Error("second result should not have rows")
		}
		if resp.Results[1].Aggregations == nil {
			t.Error("expected aggregations in second result")
		}
		if resp.Results[1].Aggregations["total"] != int64(3) {
			t.Errorf("expected aggregation total=3, got %v", resp.Results[1].Aggregations["total"])
		}
	})

	t.Run("too many subqueries returns error", func(t *testing.T) {
		queries := make([]map[string]any, MaxMultiQuery+1)
		for i := range queries {
			queries[i] = map[string]any{"rank_by": []any{"id", "asc"}, "limit": 1}
		}
		_, err := h.HandleMultiQuery(ctx, "test-ns", queries, "")
		if err != ErrTooManySubqueries {
			t.Errorf("expected ErrTooManySubqueries, got %v", err)
		}
	})

	t.Run("exactly 16 subqueries is allowed", func(t *testing.T) {
		queries := make([]map[string]any, MaxMultiQuery)
		for i := range queries {
			queries[i] = map[string]any{"rank_by": []any{"id", "asc"}, "limit": 1}
		}
		resp, err := h.HandleMultiQuery(ctx, "test-ns", queries, "")
		if err != nil {
			t.Fatalf("HandleMultiQuery() error = %v", err)
		}
		if len(resp.Results) != MaxMultiQuery {
			t.Errorf("expected %d results, got %d", MaxMultiQuery, len(resp.Results))
		}
	})

	t.Run("empty queries array returns error", func(t *testing.T) {
		queries := []map[string]any{}
		_, err := h.HandleMultiQuery(ctx, "test-ns", queries, "")
		if err != ErrInvalidMultiQuery {
			t.Errorf("expected ErrInvalidMultiQuery, got %v", err)
		}
	})

	t.Run("invalid subquery returns error", func(t *testing.T) {
		queries := []map[string]any{
			{"rank_by": []any{"id", "asc"}, "limit": 10},
			{"limit": 10}, // Missing rank_by
		}
		_, err := h.HandleMultiQuery(ctx, "test-ns", queries, "")
		if err != ErrRankByRequired {
			t.Errorf("expected ErrRankByRequired, got %v", err)
		}
	})

	t.Run("namespace not found", func(t *testing.T) {
		queries := []map[string]any{
			{"rank_by": []any{"id", "asc"}, "limit": 10},
		}
		_, err := h.HandleMultiQuery(ctx, "nonexistent-ns", queries, "")
		if err != ErrNamespaceNotFound {
			t.Errorf("expected ErrNamespaceNotFound, got %v", err)
		}
	})

	t.Run("eventual consistency mode", func(t *testing.T) {
		queries := []map[string]any{
			{"rank_by": []any{"id", "asc"}, "limit": 10},
		}
		resp, err := h.HandleMultiQuery(ctx, "test-ns", queries, "eventual")
		if err != nil {
			t.Fatalf("HandleMultiQuery() error = %v", err)
		}
		if len(resp.Results) != 1 {
			t.Errorf("expected 1 result, got %d", len(resp.Results))
		}
	})

	t.Run("response includes billing and performance", func(t *testing.T) {
		queries := []map[string]any{
			{"rank_by": []any{"id", "asc"}, "limit": 10},
		}
		resp, err := h.HandleMultiQuery(ctx, "test-ns", queries, "")
		if err != nil {
			t.Fatalf("HandleMultiQuery() error = %v", err)
		}
		// Billing should be present (even if zeros)
		if resp.Billing.BillableLogicalBytesQueried != 0 {
			t.Log("billing bytes queried is non-zero, this is acceptable")
		}
		// Performance should be present
		if resp.Performance.CacheTemperature == "" {
			t.Error("expected cache_temperature in performance")
		}
	})

	t.Run("grouped aggregation subquery", func(t *testing.T) {
		queries := []map[string]any{
			{
				"aggregate_by": map[string]any{"count": []any{"Count"}},
				"group_by":     []any{"category"},
			},
		}
		resp, err := h.HandleMultiQuery(ctx, "test-ns", queries, "")
		if err != nil {
			t.Fatalf("HandleMultiQuery() error = %v", err)
		}
		if len(resp.Results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(resp.Results))
		}
		if resp.Results[0].AggregationGroups == nil {
			t.Error("expected aggregation_groups in result")
		}
		// Should have 2 groups: category a and category b
		if len(resp.Results[0].AggregationGroups) != 2 {
			t.Errorf("expected 2 aggregation groups, got %d", len(resp.Results[0].AggregationGroups))
		}
	})

	t.Run("pending rebuild returns index rebuilding error", func(t *testing.T) {
		loaded, err := stateMan.Create(ctx, "rebuild-ns")
		if err != nil {
			t.Fatalf("failed to create rebuild namespace: %v", err)
		}
		if _, err := stateMan.AddPendingRebuild(ctx, "rebuild-ns", loaded.ETag, RebuildKindFilter, "category"); err != nil {
			t.Fatalf("failed to add pending rebuild: %v", err)
		}

		queries := []map[string]any{
			{"rank_by": []any{"id", "asc"}, "filters": []any{"category", "Eq", "a"}, "limit": 10},
		}
		_, err = h.HandleMultiQuery(ctx, "rebuild-ns", queries, "")
		if !errors.Is(err, ErrIndexRebuilding) {
			t.Errorf("expected ErrIndexRebuilding, got %v", err)
		}
	})
}

func TestMultiQuerySnapshotIsolation(t *testing.T) {
	// This test verifies that all subqueries use the same snapshot.
	// In practice, this means that the tail store refresh happens once
	// before any subquery execution.

	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Create a tracking tail store to verify refresh is called once
	refreshCount := 0
	mockTail := &trackingTailStore{
		mockTailStore: mockTailStore{
			docs: []*tail.Document{
				{
					ID:         document.NewU64ID(1),
					Attributes: map[string]any{"name": "doc1"},
				},
			},
		},
		onRefresh: func() {
			refreshCount++
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	// Execute multi-query with 3 subqueries
	queries := []map[string]any{
		{"rank_by": []any{"id", "asc"}, "limit": 10},
		{"rank_by": []any{"id", "desc"}, "limit": 10},
		{"aggregate_by": map[string]any{"count": []any{"Count"}}},
	}
	_, err = h.HandleMultiQuery(ctx, "test-ns", queries, "")
	if err != nil {
		t.Fatalf("HandleMultiQuery() error = %v", err)
	}

	// Refresh should be called at most once for snapshot isolation
	// (may be 0 if head_seq == indexed_seq)
	if refreshCount > 1 {
		t.Errorf("expected refresh to be called at most once, got %d", refreshCount)
	}
}

// trackingTailStore wraps mockTailStore to track method calls
type trackingTailStore struct {
	mockTailStore
	onRefresh func()
}

func (t *trackingTailStore) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	if t.onRefresh != nil {
		t.onRefresh()
	}
	return nil
}
