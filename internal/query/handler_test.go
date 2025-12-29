package query

import (
	"context"
	"errors"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestParseQueryRequest(t *testing.T) {
	tests := []struct {
		name    string
		body    map[string]any
		wantErr bool
		check   func(t *testing.T, req *QueryRequest)
	}{
		{
			name: "basic rank_by vector ANN",
			body: map[string]any{
				"rank_by": []any{"vector", "ANN", []any{1.0, 2.0, 3.0}},
			},
			check: func(t *testing.T, req *QueryRequest) {
				if req.RankBy == nil {
					t.Error("expected rank_by to be set")
				}
				if req.Limit != DefaultLimit {
					t.Errorf("expected default limit %d, got %d", DefaultLimit, req.Limit)
				}
			},
		},
		{
			name: "rank_by with limit",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"limit":   100,
			},
			check: func(t *testing.T, req *QueryRequest) {
				if req.Limit != 100 {
					t.Errorf("expected limit 100, got %d", req.Limit)
				}
			},
		},
		{
			name: "rank_by with top_k alias",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"top_k":   50,
			},
			check: func(t *testing.T, req *QueryRequest) {
				if req.Limit != 50 {
					t.Errorf("expected limit 50, got %d", req.Limit)
				}
			},
		},
		{
			name: "include_attributes",
			body: map[string]any{
				"rank_by":            []any{"id", "asc"},
				"include_attributes": []any{"name", "age"},
			},
			check: func(t *testing.T, req *QueryRequest) {
				if len(req.IncludeAttributes) != 2 {
					t.Errorf("expected 2 include_attributes, got %d", len(req.IncludeAttributes))
				}
			},
		},
		{
			name: "exclude_attributes",
			body: map[string]any{
				"rank_by":            []any{"id", "asc"},
				"exclude_attributes": []any{"secret"},
			},
			check: func(t *testing.T, req *QueryRequest) {
				if len(req.ExcludeAttributes) != 1 {
					t.Errorf("expected 1 exclude_attributes, got %d", len(req.ExcludeAttributes))
				}
			},
		},
		{
			name: "filters",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"name", "Eq", "test"},
			},
			check: func(t *testing.T, req *QueryRequest) {
				if req.Filters == nil {
					t.Error("expected filters to be set")
				}
			},
		},
		{
			name: "aggregate_by without rank_by",
			body: map[string]any{
				"aggregate_by": map[string]any{"count": []any{"Count"}},
			},
			check: func(t *testing.T, req *QueryRequest) {
				if req.AggregateBy == nil {
					t.Error("expected aggregate_by to be set")
				}
			},
		},
		{
			name: "consistency eventual",
			body: map[string]any{
				"rank_by":     []any{"id", "asc"},
				"consistency": "eventual",
			},
			check: func(t *testing.T, req *QueryRequest) {
				if req.Consistency != "eventual" {
					t.Errorf("expected consistency 'eventual', got %q", req.Consistency)
				}
			},
		},
		{
			name: "vector_encoding base64",
			body: map[string]any{
				"rank_by":         []any{"vector", "ANN", []any{1.0}},
				"vector_encoding": "base64",
			},
			check: func(t *testing.T, req *QueryRequest) {
				if req.VectorEncoding != "base64" {
					t.Errorf("expected vector_encoding 'base64', got %q", req.VectorEncoding)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := ParseQueryRequest(tt.body)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQueryRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.check != nil {
				tt.check(t, req)
			}
		})
	}
}

func TestValidateRequest(t *testing.T) {
	h := &Handler{}

	tests := []struct {
		name    string
		req     *QueryRequest
		wantErr error
	}{
		{
			name:    "rank_by required",
			req:     &QueryRequest{},
			wantErr: ErrRankByRequired,
		},
		{
			name: "aggregate_by allows no rank_by",
			req: &QueryRequest{
				AggregateBy: map[string]any{"count": []any{"Count"}},
			},
			wantErr: nil,
		},
		{
			name: "limit too high",
			req: &QueryRequest{
				RankBy: []any{"id", "asc"},
				Limit:  MaxTopK + 1,
			},
			wantErr: ErrInvalidLimit,
		},
		{
			name: "negative limit",
			req: &QueryRequest{
				RankBy: []any{"id", "asc"},
				Limit:  -1,
			},
			wantErr: ErrInvalidLimit,
		},
		{
			name: "valid limit at max",
			req: &QueryRequest{
				RankBy: []any{"id", "asc"},
				Limit:  MaxTopK,
			},
			wantErr: nil,
		},
		{
			name: "include and exclude conflict",
			req: &QueryRequest{
				RankBy:            []any{"id", "asc"},
				IncludeAttributes: []string{"a"},
				ExcludeAttributes: []string{"b"},
			},
			wantErr: ErrAttributeConflict,
		},
		{
			name: "invalid vector_encoding",
			req: &QueryRequest{
				RankBy:         []any{"id", "asc"},
				VectorEncoding: "invalid",
			},
			wantErr: ErrInvalidVectorEncoding,
		},
		{
			name: "valid vector_encoding float",
			req: &QueryRequest{
				RankBy:         []any{"id", "asc"},
				VectorEncoding: "float",
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := h.validateRequest(tt.req)
			if err != tt.wantErr {
				t.Errorf("validateRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseRankBy(t *testing.T) {
	tests := []struct {
		name    string
		rankBy  any
		want    *ParsedRankBy
		wantErr bool
	}{
		{
			name:   "vector ANN",
			rankBy: []any{"vector", "ANN", []any{1.0, 2.0, 3.0}},
			want: &ParsedRankBy{
				Type:        RankByVector,
				QueryVector: []float32{1.0, 2.0, 3.0},
			},
		},
		{
			name:   "attribute asc",
			rankBy: []any{"id", "asc"},
			want: &ParsedRankBy{
				Type:      RankByAttr,
				Field:     "id",
				Direction: "asc",
			},
		},
		{
			name:   "attribute desc",
			rankBy: []any{"timestamp", "desc"},
			want: &ParsedRankBy{
				Type:      RankByAttr,
				Field:     "timestamp",
				Direction: "desc",
			},
		},
		{
			name:   "BM25",
			rankBy: []any{"content", "BM25", "search query"},
			want: &ParsedRankBy{
				Type:  RankByBM25,
				Field: "content",
			},
		},
		{
			name:    "invalid - not array",
			rankBy:  "invalid",
			wantErr: true,
		},
		{
			name:    "invalid - too short",
			rankBy:  []any{"id"},
			wantErr: true,
		},
		{
			name:    "invalid direction",
			rankBy:  []any{"id", "invalid"},
			wantErr: true,
		},
		{
			name:    "vector ANN missing vector",
			rankBy:  []any{"vector", "ANN"},
			wantErr: true,
		},
		{
			name:   "nil",
			rankBy: nil,
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRankBy(tt.rankBy, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRankBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.want == nil && got != nil {
				t.Errorf("expected nil, got %+v", got)
				return
			}
			if tt.want != nil {
				if got == nil {
					t.Errorf("expected %+v, got nil", tt.want)
					return
				}
				if got.Type != tt.want.Type {
					t.Errorf("Type = %v, want %v", got.Type, tt.want.Type)
				}
				if got.Field != tt.want.Field {
					t.Errorf("Field = %v, want %v", got.Field, tt.want.Field)
				}
				if got.Direction != tt.want.Direction {
					t.Errorf("Direction = %v, want %v", got.Direction, tt.want.Direction)
				}
			}
		})
	}
}

func TestFilterAttributes(t *testing.T) {
	rows := []Row{
		{
			ID: uint64(1),
			Attributes: map[string]any{
				"name": "test",
				"age":  25,
				"city": "NYC",
			},
		},
	}

	t.Run("include_attributes", func(t *testing.T) {
		result := filterAttributes(rows, []string{"name", "age"}, nil)
		if len(result[0].Attributes) != 2 {
			t.Errorf("expected 2 attributes, got %d", len(result[0].Attributes))
		}
		if _, ok := result[0].Attributes["city"]; ok {
			t.Error("city should be excluded")
		}
	})

	t.Run("exclude_attributes", func(t *testing.T) {
		// Reset rows
		rows := []Row{
			{
				ID: uint64(1),
				Attributes: map[string]any{
					"name": "test",
					"age":  25,
					"city": "NYC",
				},
			},
		}
		result := filterAttributes(rows, nil, []string{"city"})
		if len(result[0].Attributes) != 2 {
			t.Errorf("expected 2 attributes, got %d", len(result[0].Attributes))
		}
		if _, ok := result[0].Attributes["city"]; ok {
			t.Error("city should be excluded")
		}
	})

	t.Run("no filtering", func(t *testing.T) {
		rows := []Row{
			{
				ID: uint64(1),
				Attributes: map[string]any{
					"name": "test",
					"age":  25,
					"city": "NYC",
				},
			},
		}
		result := filterAttributes(rows, nil, nil)
		if len(result[0].Attributes) != 3 {
			t.Errorf("expected 3 attributes, got %d", len(result[0].Attributes))
		}
	})
}

func TestRowToJSON(t *testing.T) {
	dist := 0.5
	row := Row{
		ID:   uint64(123),
		Dist: &dist,
		Attributes: map[string]any{
			"name": "test",
		},
	}

	result := RowToJSON(row)

	if result["id"] != uint64(123) {
		t.Errorf("expected id 123, got %v", result["id"])
	}
	if result["$dist"] != 0.5 {
		t.Errorf("expected $dist 0.5, got %v", result["$dist"])
	}
	if result["name"] != "test" {
		t.Errorf("expected name 'test', got %v", result["name"])
	}
}

func TestRowToJSON_NoDist(t *testing.T) {
	row := Row{
		ID:   uint64(123),
		Dist: nil,
		Attributes: map[string]any{
			"name": "test",
		},
	}

	result := RowToJSON(row)

	if result["id"] != uint64(123) {
		t.Errorf("expected id 123, got %v", result["id"])
	}
	if _, ok := result["$dist"]; ok {
		t.Error("$dist should not be present for order-by queries")
	}
}

func TestDocIDToAny(t *testing.T) {
	t.Run("u64", func(t *testing.T) {
		id := document.NewU64ID(123)
		result := docIDToAny(id)
		if result != uint64(123) {
			t.Errorf("expected 123, got %v", result)
		}
	})

	t.Run("string", func(t *testing.T) {
		id, _ := document.NewStringID("test-id")
		result := docIDToAny(id)
		if result != "test-id" {
			t.Errorf("expected 'test-id', got %v", result)
		}
	})
}

// TestQueryHandlerIntegration tests the full query flow with mocked dependencies
type mockTailStore struct {
	docs []*tail.Document
}

func (m *mockTailStore) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *mockTailStore) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStore) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStore) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
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

func (m *mockTailStore) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return m.VectorScan(ctx, ns, queryVector, topK, metric, f)
}

func (m *mockTailStore) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *mockTailStore) TailBytes(ns string) int64 {
	return 0
}

func (m *mockTailStore) Clear(ns string) {}

func (m *mockTailStore) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *mockTailStore) Close() error {
	return nil
}

// mockStateManager is a minimal implementation for testing
type mockStateManager struct {
	state *namespace.LoadedState
}

func (m *mockStateManager) Load(ctx context.Context, ns string) (*namespace.LoadedState, error) {
	if m.state == nil {
		return nil, namespace.ErrStateNotFound
	}
	return m.state, nil
}

func TestQueryHandler_Handle(t *testing.T) {
	// Create a handler with mock dependencies
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create namespace state
	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}
	if _, err := stateMan.Update(ctx, "test-ns", "", func(state *namespace.State) error {
		state.Vector = &namespace.VectorConfig{
			Dims:           3,
			DType:          "f32",
			DistanceMetric: string(vector.MetricCosineDistance),
			ANN:            true,
		}
		return nil
	}); err != nil {
		t.Fatalf("failed to update vector config: %v", err)
	}

	// Create mock tail store with test documents
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

	// Create handler with the mock tail store
	h := NewHandler(store, stateMan, mockTail)

	t.Run("vector query", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
			Limit:  10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(resp.Rows))
		}
		if resp.Rows[0].Dist == nil {
			t.Error("expected $dist to be set for vector query")
		}
	})

	t.Run("vector query invalid dims", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"vector", "ANN", []any{1.0, 0.0}},
			Limit:  10,
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err == nil || !errors.Is(err, ErrInvalidVectorDims) {
			t.Errorf("expected ErrInvalidVectorDims, got %v", err)
		}
	})

	t.Run("order by attribute", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"score", "desc"},
			Limit:  10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if len(resp.Rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(resp.Rows))
		}
		if resp.Rows[0].Dist != nil {
			t.Error("$dist should be nil for order-by query")
		}
	})

	t.Run("missing rank_by", func(t *testing.T) {
		req := &QueryRequest{
			Limit: 10,
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != ErrRankByRequired {
			t.Errorf("expected ErrRankByRequired, got %v", err)
		}
	})

	t.Run("invalid limit", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"id", "asc"},
			Limit:  MaxTopK + 1,
		}
		_, err := h.Handle(ctx, "test-ns", req)
		if err != ErrInvalidLimit {
			t.Errorf("expected ErrInvalidLimit, got %v", err)
		}
	})

	t.Run("namespace not found", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"id", "asc"},
			Limit:  10,
		}
		_, err := h.Handle(ctx, "nonexistent-ns", req)
		if err != ErrNamespaceNotFound {
			t.Errorf("expected ErrNamespaceNotFound, got %v", err)
		}
	})

	t.Run("include_attributes filtering", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:            []any{"id", "asc"},
			Limit:             10,
			IncludeAttributes: []string{"name"},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		for _, row := range resp.Rows {
			if _, ok := row.Attributes["score"]; ok {
				t.Error("score should be excluded")
			}
			if _, ok := row.Attributes["name"]; !ok {
				t.Error("name should be included")
			}
		}
	})

	t.Run("exclude_attributes filtering", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:            []any{"id", "asc"},
			Limit:             10,
			ExcludeAttributes: []string{"score"},
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		for _, row := range resp.Rows {
			if _, ok := row.Attributes["score"]; ok {
				t.Error("score should be excluded")
			}
		}
	})

	t.Run("aggregate_by without rank_by returns aggregations", func(t *testing.T) {
		req := &QueryRequest{
			AggregateBy: map[string]any{"count": []any{"Count"}},
			Limit:       10,
		}
		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}
		if resp == nil {
			t.Error("expected non-nil response")
		}
		if resp.Aggregations == nil {
			t.Error("expected aggregations in response")
		}
	})
}
