package query

import (
	"context"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestParseAggregateBy(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		wantErr bool
		check   func(t *testing.T, aggs map[string]*ParsedAggregation)
	}{
		{
			name: "single count",
			input: map[string]any{
				"total": []any{"Count"},
			},
			check: func(t *testing.T, aggs map[string]*ParsedAggregation) {
				if len(aggs) != 1 {
					t.Errorf("expected 1 aggregation, got %d", len(aggs))
				}
				if aggs["total"] == nil {
					t.Error("expected 'total' aggregation")
					return
				}
				if aggs["total"].Type != AggCount {
					t.Errorf("expected Count type, got %v", aggs["total"].Type)
				}
			},
		},
		{
			name: "single sum",
			input: map[string]any{
				"total_price": []any{"Sum", "price"},
			},
			check: func(t *testing.T, aggs map[string]*ParsedAggregation) {
				if len(aggs) != 1 {
					t.Errorf("expected 1 aggregation, got %d", len(aggs))
				}
				if aggs["total_price"] == nil {
					t.Error("expected 'total_price' aggregation")
					return
				}
				if aggs["total_price"].Type != AggSum {
					t.Errorf("expected Sum type, got %v", aggs["total_price"].Type)
				}
				if aggs["total_price"].Attribute != "price" {
					t.Errorf("expected attribute 'price', got %q", aggs["total_price"].Attribute)
				}
			},
		},
		{
			name: "multiple aggregations",
			input: map[string]any{
				"count":      []any{"Count"},
				"total_qty":  []any{"Sum", "quantity"},
				"total_cost": []any{"Sum", "cost"},
			},
			check: func(t *testing.T, aggs map[string]*ParsedAggregation) {
				if len(aggs) != 3 {
					t.Errorf("expected 3 aggregations, got %d", len(aggs))
				}
				if aggs["count"] == nil || aggs["count"].Type != AggCount {
					t.Error("expected count aggregation")
				}
				if aggs["total_qty"] == nil || aggs["total_qty"].Type != AggSum || aggs["total_qty"].Attribute != "quantity" {
					t.Error("expected total_qty aggregation with Sum of quantity")
				}
				if aggs["total_cost"] == nil || aggs["total_cost"].Type != AggSum || aggs["total_cost"].Attribute != "cost" {
					t.Error("expected total_cost aggregation with Sum of cost")
				}
			},
		},
		{
			name:    "not an object",
			input:   []any{"Count"},
			wantErr: true,
		},
		{
			name: "empty aggregation array",
			input: map[string]any{
				"bad": []any{},
			},
			wantErr: true,
		},
		{
			name: "unknown function",
			input: map[string]any{
				"bad": []any{"Unknown"},
			},
			wantErr: true,
		},
		{
			name: "count with extra args",
			input: map[string]any{
				"bad": []any{"Count", "extra"},
			},
			wantErr: true,
		},
		{
			name: "sum without attribute",
			input: map[string]any{
				"bad": []any{"Sum"},
			},
			wantErr: true,
		},
		{
			name: "sum with non-string attribute",
			input: map[string]any{
				"bad": []any{"Sum", 123},
			},
			wantErr: true,
		},
		{
			name: "aggregation not an array",
			input: map[string]any{
				"bad": "Count",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggs, err := parseAggregateBy(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseAggregateBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.check != nil {
				tt.check(t, aggs)
			}
		})
	}
}

func TestComputeSum(t *testing.T) {
	tests := []struct {
		name     string
		docs     []*tail.Document
		attr     string
		expected float64
	}{
		{
			name: "sum int64 values",
			docs: []*tail.Document{
				{ID: document.NewU64ID(1), Attributes: map[string]any{"qty": int64(10)}},
				{ID: document.NewU64ID(2), Attributes: map[string]any{"qty": int64(20)}},
				{ID: document.NewU64ID(3), Attributes: map[string]any{"qty": int64(30)}},
			},
			attr:     "qty",
			expected: 60,
		},
		{
			name: "sum float64 values",
			docs: []*tail.Document{
				{ID: document.NewU64ID(1), Attributes: map[string]any{"price": 10.5}},
				{ID: document.NewU64ID(2), Attributes: map[string]any{"price": 20.25}},
			},
			attr:     "price",
			expected: 30.75,
		},
		{
			name: "sum with missing values",
			docs: []*tail.Document{
				{ID: document.NewU64ID(1), Attributes: map[string]any{"qty": int64(10)}},
				{ID: document.NewU64ID(2), Attributes: map[string]any{"other": int64(5)}},
				{ID: document.NewU64ID(3), Attributes: map[string]any{"qty": int64(30)}},
			},
			attr:     "qty",
			expected: 40,
		},
		{
			name: "sum with nil attributes",
			docs: []*tail.Document{
				{ID: document.NewU64ID(1), Attributes: map[string]any{"qty": int64(10)}},
				{ID: document.NewU64ID(2), Attributes: nil},
			},
			attr:     "qty",
			expected: 10,
		},
		{
			name: "sum uint64 values",
			docs: []*tail.Document{
				{ID: document.NewU64ID(1), Attributes: map[string]any{"count": uint64(100)}},
				{ID: document.NewU64ID(2), Attributes: map[string]any{"count": uint64(200)}},
			},
			attr:     "count",
			expected: 300,
		},
		{
			name:     "empty docs",
			docs:     []*tail.Document{},
			attr:     "qty",
			expected: 0,
		},
		{
			name: "mixed numeric types",
			docs: []*tail.Document{
				{ID: document.NewU64ID(1), Attributes: map[string]any{"val": int64(10)}},
				{ID: document.NewU64ID(2), Attributes: map[string]any{"val": float64(5.5)}},
				{ID: document.NewU64ID(3), Attributes: map[string]any{"val": uint64(4)}},
			},
			attr:     "val",
			expected: 19.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeSum(tt.docs, tt.attr)
			if result != tt.expected {
				t.Errorf("computeSum() = %v, want %v", result, tt.expected)
			}
		})
	}
}

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
	// Apply filter
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

func (m *mockAggTailStore) TailBytes(ns string) int64 {
	return 0
}

func (m *mockAggTailStore) Clear(ns string) {}

func (m *mockAggTailStore) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *mockAggTailStore) Close() error {
	return nil
}

func TestAggregationQueryIntegration(t *testing.T) {
	// Set up test data
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
			{ID: document.NewU64ID(4), Attributes: map[string]any{"category": "B", "price": float64(25.0), "qty": int64(4)}},
			{ID: document.NewU64ID(5), Attributes: map[string]any{"category": "A", "price": float64(30.0), "qty": int64(5)}},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	t.Run("count all documents", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"total": []any{"Count"},
			},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.Aggregations == nil {
			t.Fatal("expected aggregations in response")
		}
		if resp.Aggregations["total"] != int64(5) {
			t.Errorf("expected count 5, got %v", resp.Aggregations["total"])
		}
	})

	t.Run("sum price", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"total_price": []any{"Sum", "price"},
			},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.Aggregations == nil {
			t.Fatal("expected aggregations in response")
		}
		expectedSum := 10.5 + 20.0 + 15.0 + 25.0 + 30.0
		if resp.Aggregations["total_price"] != expectedSum {
			t.Errorf("expected sum %v, got %v", expectedSum, resp.Aggregations["total_price"])
		}
	})

	t.Run("multiple aggregations", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count":       []any{"Count"},
				"total_price": []any{"Sum", "price"},
				"total_qty":   []any{"Sum", "qty"},
			},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.Aggregations == nil {
			t.Fatal("expected aggregations in response")
		}
		if resp.Aggregations["count"] != int64(5) {
			t.Errorf("expected count 5, got %v", resp.Aggregations["count"])
		}
		expectedPrice := 10.5 + 20.0 + 15.0 + 25.0 + 30.0
		if resp.Aggregations["total_price"] != expectedPrice {
			t.Errorf("expected price sum %v, got %v", expectedPrice, resp.Aggregations["total_price"])
		}
		expectedQty := float64(2 + 3 + 1 + 4 + 5) // Sum returns float64
		if resp.Aggregations["total_qty"] != expectedQty {
			t.Errorf("expected qty sum %v, got %v", expectedQty, resp.Aggregations["total_qty"])
		}
	})

	t.Run("aggregation with filter", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count": []any{"Count"},
			},
			Filters: []any{"category", "Eq", "A"},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.Aggregations == nil {
			t.Fatal("expected aggregations in response")
		}
		// Should only count docs with category "A" (3 docs)
		if resp.Aggregations["count"] != int64(3) {
			t.Errorf("expected count 3 for category A, got %v", resp.Aggregations["count"])
		}
	})

	t.Run("sum with filter", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"total_price": []any{"Sum", "price"},
			},
			Filters: []any{"category", "Eq", "B"},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp.Aggregations == nil {
			t.Fatal("expected aggregations in response")
		}
		// Should only sum prices for category "B" (20.0 + 25.0 = 45.0)
		expectedSum := 20.0 + 25.0
		if resp.Aggregations["total_price"] != expectedSum {
			t.Errorf("expected sum %v for category B, got %v", expectedSum, resp.Aggregations["total_price"])
		}
	})

	t.Run("response has no rows", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"count": []any{"Count"},
			},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 0 {
			t.Errorf("expected no rows in aggregation response, got %d", len(resp.Rows))
		}
	})
}

func TestAggregationValidation(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	h := NewHandler(store, stateMan, nil)

	t.Run("invalid aggregate_by not an object", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: []any{"Count"},
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil {
			t.Error("expected error for invalid aggregate_by")
		}
	})

	t.Run("invalid aggregation function", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{
				"bad": []any{"Invalid"},
			},
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil {
			t.Error("expected error for unknown aggregation function")
		}
	})
}
