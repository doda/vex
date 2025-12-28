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

// filteringTailStore implements tail.Store with actual filter evaluation.
type filteringTailStore struct {
	docs []*tail.Document
}

func (m *filteringTailStore) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *filteringTailStore) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	var results []*tail.Document
	for _, doc := range m.docs {
		if f != nil {
			filterDoc := make(filter.Document)
			for k, v := range doc.Attributes {
				filterDoc[k] = v
			}
			if !f.Eval(filterDoc) {
				continue
			}
		}
		results = append(results, doc)
	}
	return results, nil
}

func (m *filteringTailStore) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.Scan(ctx, ns, f)
}

func (m *filteringTailStore) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	var results []tail.VectorScanResult
	for _, doc := range m.docs {
		if doc.Vector == nil {
			continue
		}

		if f != nil {
			filterDoc := make(filter.Document)
			for k, v := range doc.Attributes {
				filterDoc[k] = v
			}
			if !f.Eval(filterDoc) {
				continue
			}
		}

		dist := computeVectorDistance(queryVector, doc.Vector, metric)
		results = append(results, tail.VectorScanResult{
			Doc:      doc,
			Distance: dist,
		})
	}

	sortVectorResults(results)
	if len(results) > topK {
		results = results[:topK]
	}
	return results, nil
}

func (m *filteringTailStore) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return m.VectorScan(ctx, ns, queryVector, topK, metric, f)
}

func (m *filteringTailStore) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *filteringTailStore) TailBytes(ns string) int64 {
	return 0
}

func (m *filteringTailStore) Clear(ns string) {}

func (m *filteringTailStore) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *filteringTailStore) Close() error {
	return nil
}

func computeVectorDistance(a, b []float32, metric tail.DistanceMetric) float64 {
	switch metric {
	case tail.MetricEuclideanSquared:
		sum := float64(0)
		for i := range a {
			if i < len(b) {
				diff := float64(a[i] - b[i])
				sum += diff * diff
			}
		}
		return sum
	case tail.MetricDotProduct:
		sum := float64(0)
		for i := range a {
			if i < len(b) {
				sum += float64(a[i]) * float64(b[i])
			}
		}
		return -sum
	default:
		var dotProd, normA, normB float64
		for i := range a {
			if i < len(b) {
				dotProd += float64(a[i]) * float64(b[i])
				normA += float64(a[i]) * float64(a[i])
				normB += float64(b[i]) * float64(b[i])
			}
		}
		if normA == 0 || normB == 0 {
			return 1.0
		}
		return 1.0 - (dotProd / (sqrt(normA) * sqrt(normB)))
	}
}

func sqrt(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 20; i++ {
		z = (z + x/z) / 2
	}
	return z
}

func sortVectorResults(results []tail.VectorScanResult) {
	for i := range results {
		for j := i + 1; j < len(results); j++ {
			if results[j].Distance < results[i].Distance {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}

func TestQueryWithFiltersRestrictsResults(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	if _, err := stateMan.Create(ctx, "test-ns"); err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	tailStore := &filteringTailStore{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"category": "A", "score": int64(100)},
				Vector:     []float32{1.0, 0.0, 0.0},
			},
			{
				ID:         document.NewU64ID(2),
				Attributes: map[string]any{"category": "B", "score": int64(200)},
				Vector:     []float32{0.0, 1.0, 0.0},
			},
			{
				ID:         document.NewU64ID(3),
				Attributes: map[string]any{"category": "A", "score": int64(300)},
				Vector:     []float32{0.0, 0.0, 1.0},
			},
			{
				ID:         document.NewU64ID(4),
				Attributes: map[string]any{"category": "C", "score": int64(50)},
				Vector:     []float32{0.5, 0.5, 0.0},
			},
		},
	}

	h := NewHandler(store, stateMan, tailStore)

	t.Run("filters restrict attribute query results", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"score", "desc"},
			Filters: []any{"category", "Eq", "A"},
			Limit:   10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows (only category A), got %d", len(resp.Rows))
		}
		for _, row := range resp.Rows {
			if row.Attributes["category"] != "A" {
				t.Errorf("expected category A, got %v", row.Attributes["category"])
			}
		}
	})

	t.Run("filters with NotEq operator", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"score", "asc"},
			Filters: []any{"category", "NotEq", "A"},
			Limit:   10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows (B and C), got %d", len(resp.Rows))
		}
		for _, row := range resp.Rows {
			if row.Attributes["category"] == "A" {
				t.Error("category A should be excluded")
			}
		}
	})

	t.Run("filters with In operator", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "asc"},
			Filters: []any{"category", "In", []any{"A", "B"}},
			Limit:   10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 3 {
			t.Errorf("expected 3 rows (A and B categories), got %d", len(resp.Rows))
		}
	})

	t.Run("filters with comparison operators", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"score", "asc"},
			Filters: []any{"score", "Gte", int64(100)},
			Limit:   10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 3 {
			t.Errorf("expected 3 rows (score >= 100), got %d", len(resp.Rows))
		}
		for _, row := range resp.Rows {
			score := row.Attributes["score"].(int64)
			if score < 100 {
				t.Errorf("expected score >= 100, got %d", score)
			}
		}
	})

	t.Run("compound filter with And", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"id", "asc"},
			Filters: []any{"And", []any{
				[]any{"category", "Eq", "A"},
				[]any{"score", "Gte", int64(200)},
			}},
			Limit: 10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 1 {
			t.Errorf("expected 1 row (category A and score >= 200), got %d", len(resp.Rows))
		}
		if len(resp.Rows) > 0 {
			if resp.Rows[0].ID != uint64(3) {
				t.Errorf("expected id 3, got %v", resp.Rows[0].ID)
			}
		}
	})

	t.Run("no results when filter matches nothing", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "asc"},
			Filters: []any{"category", "Eq", "Z"},
			Limit:   10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 0 {
			t.Errorf("expected 0 rows, got %d", len(resp.Rows))
		}
	})
}

func TestFiltersAppliedBeforeRanking(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	if _, err := stateMan.Create(ctx, "test-ns"); err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	tailStore := &filteringTailStore{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"active": true, "score": int64(100)},
				Vector:     []float32{1.0, 0.0, 0.0},
			},
			{
				ID:         document.NewU64ID(2),
				Attributes: map[string]any{"active": false, "score": int64(500)},
				Vector:     []float32{0.9, 0.1, 0.0},
			},
			{
				ID:         document.NewU64ID(3),
				Attributes: map[string]any{"active": true, "score": int64(200)},
				Vector:     []float32{0.8, 0.2, 0.0},
			},
			{
				ID:         document.NewU64ID(4),
				Attributes: map[string]any{"active": false, "score": int64(1000)},
				Vector:     []float32{0.7, 0.3, 0.0},
			},
			{
				ID:         document.NewU64ID(5),
				Attributes: map[string]any{"active": true, "score": int64(50)},
				Vector:     []float32{0.0, 1.0, 0.0},
			},
		},
	}

	h := NewHandler(store, stateMan, tailStore)

	t.Run("top_k applies after filter", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"score", "desc"},
			Filters: []any{"active", "Eq", true},
			Limit:   2,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(resp.Rows))
		}
		ids := make([]any, len(resp.Rows))
		for i, row := range resp.Rows {
			ids[i] = row.ID
		}
		if len(resp.Rows) >= 2 {
			if resp.Rows[0].ID != uint64(3) || resp.Rows[1].ID != uint64(1) {
				t.Errorf("expected top 2 active docs by score (3, 1), got %v", ids)
			}
		}
	})
}

func TestVectorSearchWithFilters(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	if _, err := stateMan.Create(ctx, "test-ns"); err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	tailStore := &filteringTailStore{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"type": "product", "name": "Widget"},
				Vector:     []float32{1.0, 0.0, 0.0},
			},
			{
				ID:         document.NewU64ID(2),
				Attributes: map[string]any{"type": "article", "name": "Guide"},
				Vector:     []float32{0.99, 0.1, 0.0},
			},
			{
				ID:         document.NewU64ID(3),
				Attributes: map[string]any{"type": "product", "name": "Gadget"},
				Vector:     []float32{0.9, 0.2, 0.0},
			},
			{
				ID:         document.NewU64ID(4),
				Attributes: map[string]any{"type": "article", "name": "Tutorial"},
				Vector:     []float32{0.8, 0.3, 0.0},
			},
			{
				ID:         document.NewU64ID(5),
				Attributes: map[string]any{"type": "product", "name": "Tool"},
				Vector:     []float32{0.0, 1.0, 0.0},
			},
		},
	}

	h := NewHandler(store, stateMan, tailStore)

	t.Run("vector search with type filter", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Filters: []any{"type", "Eq", "product"},
			Limit:   10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 3 {
			t.Errorf("expected 3 product rows, got %d", len(resp.Rows))
		}
		for _, row := range resp.Rows {
			if row.Attributes["type"] != "product" {
				t.Errorf("expected type 'product', got %v", row.Attributes["type"])
			}
			if row.Dist == nil {
				t.Error("expected $dist to be set for vector query")
			}
		}
	})

	t.Run("vector search results ordered by distance within filter", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Filters: []any{"type", "Eq", "product"},
			Limit:   3,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) < 2 {
			t.Fatalf("expected at least 2 rows, got %d", len(resp.Rows))
		}
		if *resp.Rows[0].Dist > *resp.Rows[1].Dist {
			t.Error("results should be ordered by distance ascending")
		}
		if resp.Rows[0].ID != uint64(1) {
			t.Errorf("expected closest product to be id 1, got %v", resp.Rows[0].ID)
		}
	})

	t.Run("vector search with filter excludes closer non-matching docs", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Filters: []any{"type", "Eq", "article"},
			Limit:   10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 article rows, got %d", len(resp.Rows))
		}
		if resp.Rows[0].ID != uint64(2) {
			t.Errorf("expected closest article to be id 2, got %v", resp.Rows[0].ID)
		}
	})

	t.Run("vector search with compound filter", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Filters: []any{"And", []any{
				[]any{"type", "Eq", "product"},
				[]any{"name", "NotEq", "Tool"},
			}},
			Limit: 10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows (product, not Tool), got %d", len(resp.Rows))
		}
		for _, row := range resp.Rows {
			if row.Attributes["name"] == "Tool" {
				t.Error("Tool should be excluded")
			}
		}
	})

	t.Run("vector search with Or filter", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Filters: []any{"Or", []any{
				[]any{"name", "Eq", "Widget"},
				[]any{"name", "Eq", "Guide"},
			}},
			Limit: 10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(resp.Rows))
		}
	})

	t.Run("vector search with top_k after filter", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Filters: []any{"type", "Eq", "product"},
			Limit:   1,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(resp.Rows))
		}
		if len(resp.Rows) > 0 && resp.Rows[0].ID != uint64(1) {
			t.Errorf("expected closest product (id 1), got %v", resp.Rows[0].ID)
		}
	})
}

func TestInvalidFilterReturnsError(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	if _, err := stateMan.Create(ctx, "test-ns"); err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	tailStore := &filteringTailStore{docs: nil}
	h := NewHandler(store, stateMan, tailStore)

	t.Run("invalid filter expression", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "asc"},
			Filters: "not-an-array",
			Limit:   10,
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil {
			t.Error("expected error for invalid filter")
		}
	})

	t.Run("unknown filter operator", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "asc"},
			Filters: []any{"name", "UnknownOp", "value"},
			Limit:   10,
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil {
			t.Error("expected error for unknown filter operator")
		}
	})
}
