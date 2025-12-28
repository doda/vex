package query

import (
	"context"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestParseGroupBy(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		wantLen int
		wantErr bool
	}{
		{
			name:    "single attribute",
			input:   []any{"category"},
			wantLen: 1,
		},
		{
			name:    "multiple attributes",
			input:   []any{"category", "size", "color"},
			wantLen: 3,
		},
		{
			name:    "string slice",
			input:   []string{"category", "size"},
			wantLen: 2,
		},
		{
			name:    "empty array",
			input:   []any{},
			wantErr: true,
		},
		{
			name:    "not an array",
			input:   "category",
			wantErr: true,
		},
		{
			name:    "non-string element",
			input:   []any{"category", 123},
			wantErr: true,
		},
		{
			name:    "nil",
			input:   nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseGroupBy(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseGroupBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(result) != tt.wantLen {
				t.Errorf("parseGroupBy() returned %d attrs, want %d", len(result), tt.wantLen)
			}
		})
	}
}

func TestGroupedAggregationQuery(t *testing.T) {
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
			{ID: document.NewU64ID(1), Attributes: map[string]any{"category": "A", "size": "small", "price": float64(10.5), "qty": int64(2)}},
			{ID: document.NewU64ID(2), Attributes: map[string]any{"category": "B", "size": "small", "price": float64(20.0), "qty": int64(3)}},
			{ID: document.NewU64ID(3), Attributes: map[string]any{"category": "A", "size": "medium", "price": float64(15.0), "qty": int64(1)}},
			{ID: document.NewU64ID(4), Attributes: map[string]any{"category": "B", "size": "small", "price": float64(25.0), "qty": int64(4)}},
			{ID: document.NewU64ID(5), Attributes: map[string]any{"category": "A", "size": "small", "price": float64(30.0), "qty": int64(5)}},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	t.Run("group by single attribute", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count": []any{"Count"},
			},
			GroupBy: []any{"category"},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.AggregationGroups == nil {
			t.Fatal("expected aggregation_groups in response")
		}
		if len(resp.AggregationGroups) != 2 {
			t.Errorf("expected 2 groups (A and B), got %d", len(resp.AggregationGroups))
		}

		// Check groups are sorted by group key
		if resp.AggregationGroups[0]["category"] != "A" {
			t.Errorf("expected first group to be category A, got %v", resp.AggregationGroups[0]["category"])
		}
		if resp.AggregationGroups[1]["category"] != "B" {
			t.Errorf("expected second group to be category B, got %v", resp.AggregationGroups[1]["category"])
		}

		// Check count for category A (3 docs)
		if resp.AggregationGroups[0]["count"] != int64(3) {
			t.Errorf("expected count 3 for category A, got %v", resp.AggregationGroups[0]["count"])
		}
		// Check count for category B (2 docs)
		if resp.AggregationGroups[1]["count"] != int64(2) {
			t.Errorf("expected count 2 for category B, got %v", resp.AggregationGroups[1]["count"])
		}
	})

	t.Run("group by multiple attributes", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count": []any{"Count"},
			},
			GroupBy: []any{"category", "size"},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.AggregationGroups == nil {
			t.Fatal("expected aggregation_groups in response")
		}

		// Expected groups:
		// - A, small: 2 (docs 1, 5)
		// - A, medium: 1 (doc 3)
		// - B, small: 2 (docs 2, 4)
		if len(resp.AggregationGroups) != 3 {
			t.Errorf("expected 3 groups, got %d", len(resp.AggregationGroups))
		}

		// Verify each group has both group attributes
		for _, group := range resp.AggregationGroups {
			if _, ok := group["category"]; !ok {
				t.Error("group missing 'category' attribute")
			}
			if _, ok := group["size"]; !ok {
				t.Error("group missing 'size' attribute")
			}
			if _, ok := group["count"]; !ok {
				t.Error("group missing 'count' aggregation")
			}
		}
	})

	t.Run("group by with sum aggregation", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"total_price": []any{"Sum", "price"},
				"count":       []any{"Count"},
			},
			GroupBy: []any{"category"},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.AggregationGroups == nil {
			t.Fatal("expected aggregation_groups in response")
		}

		// Category A: 10.5 + 15.0 + 30.0 = 55.5
		// Category B: 20.0 + 25.0 = 45.0
		for _, group := range resp.AggregationGroups {
			cat := group["category"]
			if cat == "A" {
				if group["total_price"] != 55.5 {
					t.Errorf("expected total_price 55.5 for category A, got %v", group["total_price"])
				}
				if group["count"] != int64(3) {
					t.Errorf("expected count 3 for category A, got %v", group["count"])
				}
			} else if cat == "B" {
				if group["total_price"] != 45.0 {
					t.Errorf("expected total_price 45.0 for category B, got %v", group["total_price"])
				}
				if group["count"] != int64(2) {
					t.Errorf("expected count 2 for category B, got %v", group["count"])
				}
			}
		}
	})

	t.Run("group by with filter", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count": []any{"Count"},
			},
			GroupBy: []any{"category"},
			Filters: []any{"size", "Eq", "small"},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.AggregationGroups == nil {
			t.Fatal("expected aggregation_groups in response")
		}

		// Only small items:
		// Category A: 2 (docs 1, 5)
		// Category B: 2 (docs 2, 4)
		if len(resp.AggregationGroups) != 2 {
			t.Errorf("expected 2 groups, got %d", len(resp.AggregationGroups))
		}
	})

	t.Run("group by with limit", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count": []any{"Count"},
			},
			GroupBy: []any{"category"},
			Limit:   1,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.AggregationGroups == nil {
			t.Fatal("expected aggregation_groups in response")
		}
		if len(resp.AggregationGroups) != 1 {
			t.Errorf("expected 1 group (limited), got %d", len(resp.AggregationGroups))
		}
		// First group should be A (sorted alphabetically)
		if resp.AggregationGroups[0]["category"] != "A" {
			t.Errorf("expected first group to be category A, got %v", resp.AggregationGroups[0]["category"])
		}
	})

	t.Run("no rows in grouped response", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count": []any{"Count"},
			},
			GroupBy: []any{"category"},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 0 {
			t.Errorf("expected no rows in grouped aggregation response, got %d", len(resp.Rows))
		}
		if resp.Aggregations != nil {
			t.Error("expected no aggregations field in grouped response")
		}
	})
}

func TestGroupByValidation(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	h := NewHandler(store, stateMan, nil)

	t.Run("group_by without aggregate_by", func(t *testing.T) {
		req := &QueryRequest{
			GroupBy: []any{"category"},
			RankBy:  []any{"id", "asc"},
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil {
			t.Error("expected error for group_by without aggregate_by")
		}
		if err != ErrGroupByWithoutAgg {
			t.Errorf("expected ErrGroupByWithoutAgg, got %v", err)
		}
	})

	t.Run("invalid group_by format", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count": []any{"Count"},
			},
			GroupBy: "not_an_array",
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil {
			t.Error("expected error for invalid group_by format")
		}
	})

	t.Run("empty group_by array", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count": []any{"Count"},
			},
			GroupBy: []any{},
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil {
			t.Error("expected error for empty group_by array")
		}
	})
}

func TestGroupByWithNilValues(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Create mock tail store with docs that have missing attributes
	mockTail := &mockAggTailStore{
		docs: []*tail.Document{
			{ID: document.NewU64ID(1), Attributes: map[string]any{"category": "A", "price": float64(10)}},
			{ID: document.NewU64ID(2), Attributes: map[string]any{"category": "B", "price": float64(20)}},
			{ID: document.NewU64ID(3), Attributes: map[string]any{"price": float64(15)}}, // missing category
			{ID: document.NewU64ID(4), Attributes: nil},                                   // nil attributes
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	t.Run("group by with missing attribute values", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count": []any{"Count"},
			},
			GroupBy: []any{"category"},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.AggregationGroups == nil {
			t.Fatal("expected aggregation_groups in response")
		}

		// Should have 3 groups: A (1), B (1), nil (2)
		if len(resp.AggregationGroups) != 3 {
			t.Errorf("expected 3 groups, got %d", len(resp.AggregationGroups))
		}

		// Find and check the nil group
		for _, group := range resp.AggregationGroups {
			if group["category"] == nil {
				if group["count"] != int64(2) {
					t.Errorf("expected count 2 for nil category, got %v", group["count"])
				}
			}
		}
	})
}
