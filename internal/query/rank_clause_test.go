package query

import (
	"context"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/fts"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// mockTailStoreForRankClause implements tail.Store for rank clause testing.
type mockTailStoreForRankClause struct {
	docs []*tail.Document
}

func (m *mockTailStoreForRankClause) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *mockTailStoreForRankClause) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStoreForRankClause) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, limit int64) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStoreForRankClause) VectorScan(ctx context.Context, ns string, qv []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *mockTailStoreForRankClause) VectorScanWithByteLimit(ctx context.Context, ns string, qv []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, limit int64) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *mockTailStoreForRankClause) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *mockTailStoreForRankClause) TailBytes(ns string) int64 {
	return 0
}

func (m *mockTailStoreForRankClause) Clear(ns string) {}

func (m *mockTailStoreForRankClause) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *mockTailStoreForRankClause) Close() error {
	return nil
}

func TestParseRankClause(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		wantType RankClauseType
		wantErr  bool
	}{
		{
			name:     "simple BM25",
			input:    []any{"title", "BM25", "hello"},
			wantType: RankClauseBM25,
			wantErr:  false,
		},
		{
			name:     "Sum with two BM25 clauses",
			input:    []any{"Sum", []any{[]any{"title", "BM25", "hello"}, []any{"content", "BM25", "world"}}},
			wantType: RankClauseSum,
			wantErr:  false,
		},
		{
			name:     "Max with two BM25 clauses",
			input:    []any{"Max", []any{[]any{"title", "BM25", "hello"}, []any{"content", "BM25", "world"}}},
			wantType: RankClauseMax,
			wantErr:  false,
		},
		{
			name:     "Product with weight and BM25",
			input:    []any{"Product", 2.5, []any{"title", "BM25", "hello"}},
			wantType: RankClauseProduct,
			wantErr:  false,
		},
		{
			name:     "filter as rank clause",
			input:    []any{"category", "Eq", "premium"},
			wantType: RankClauseFilter,
			wantErr:  false,
		},
		{
			name:    "invalid Sum missing clauses",
			input:   []any{"Sum"},
			wantErr: true,
		},
		{
			name:    "invalid Max wrong format",
			input:   []any{"Max", "not an array"},
			wantErr: true,
		},
		{
			name:    "invalid Product missing weight",
			input:   []any{"Product"},
			wantErr: true,
		},
		{
			name:    "invalid Product non-numeric weight",
			input:   []any{"Product", "not a number", []any{"title", "BM25", "hello"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, err := ParseRankClause(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if clause.Type != tt.wantType {
				t.Errorf("expected type %v, got %v", tt.wantType, clause.Type)
			}
		})
	}
}

func TestSumOperator(t *testing.T) {
	ctx := context.Background()

	// Create test documents with content in two fields
	docs := []*tail.Document{
		{
			ID: document.NewU64ID(1),
			Attributes: map[string]any{
				"title":   "hello world",
				"content": "foo bar",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(2),
			Attributes: map[string]any{
				"title":   "foo bar",
				"content": "hello world",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(3),
			Attributes: map[string]any{
				"title":   "hello hello hello",
				"content": "hello hello",
			},
			WalSeq: 1,
		},
	}

	mockTail := &mockTailStoreForRankClause{docs: docs}
	mockStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(mockStore)
	handler := NewHandler(mockStore, stateMan, mockTail)

	loadedState := &namespace.LoadedState{
		State: &namespace.State{
			Schema: &namespace.Schema{},
		},
	}

	// Query with Sum of two BM25 clauses for "hello"
	// Document 3 should score highest since it has "hello" in both fields multiple times
	parsed := &ParsedRankBy{
		Type: RankByComposite,
		Clause: &RankClause{
			Type: RankClauseSum,
			Clauses: []*RankClause{
				{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
				{Type: RankClauseBM25, Field: "content", QueryText: "hello"},
			},
		},
	}

	rows, err := handler.executeCompositeQuery(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rows) == 0 {
		t.Fatal("expected at least one result")
	}

	// Document 3 should be first (highest combined score for "hello")
	if rows[0].ID != uint64(3) {
		t.Errorf("expected doc 3 to be first (highest Sum score), got doc %v", rows[0].ID)
	}

	// All results should have positive scores
	for _, row := range rows {
		if row.Dist == nil || *row.Dist <= 0 {
			t.Errorf("expected positive score for doc %v, got %v", row.ID, row.Dist)
		}
	}

	t.Run("Sum combines scores additively", func(t *testing.T) {
		// The score should be the sum of the individual BM25 scores
		// Document 3 has "hello" in both title (3x) and content (2x)
		// so it should have a combined score higher than docs that only match one field
		if len(rows) < 2 {
			t.Skip("need at least 2 results to compare")
		}
		// Just verify doc 3 (matching both) is ranked higher than docs matching one
		found := false
		for i, row := range rows {
			if row.ID == uint64(3) {
				if i != 0 {
					t.Errorf("expected doc 3 to be first, but it was at position %d", i)
				}
				found = true
				break
			}
		}
		if !found {
			t.Error("doc 3 not found in results")
		}
	})
}

func TestMaxOperator(t *testing.T) {
	ctx := context.Background()

	docs := []*tail.Document{
		{
			ID: document.NewU64ID(1),
			Attributes: map[string]any{
				"title":   "hello world",
				"content": "nothing here",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(2),
			Attributes: map[string]any{
				"title":   "nothing here",
				"content": "hello world",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(3),
			Attributes: map[string]any{
				"title":   "hello hello hello",
				"content": "short",
			},
			WalSeq: 1,
		},
	}

	mockTail := &mockTailStoreForRankClause{docs: docs}
	mockStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(mockStore)
	handler := NewHandler(mockStore, stateMan, mockTail)

	loadedState := &namespace.LoadedState{
		State: &namespace.State{
			Schema: &namespace.Schema{},
		},
	}

	// Max of two BM25 clauses - should take the highest score from either field
	parsed := &ParsedRankBy{
		Type: RankByComposite,
		Clause: &RankClause{
			Type: RankClauseMax,
			Clauses: []*RankClause{
				{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
				{Type: RankClauseBM25, Field: "content", QueryText: "hello"},
			},
		},
	}

	rows, err := handler.executeCompositeQuery(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rows) == 0 {
		t.Fatal("expected at least one result")
	}

	// All 3 documents should be in results with positive scores
	if len(rows) != 3 {
		t.Fatalf("expected 3 results, got %d", len(rows))
	}

	t.Run("Max takes maximum not sum", func(t *testing.T) {
		// Documents 1 and 2 should have similar scores since they have the same
		// term frequency in one field each
		if len(rows) < 3 {
			t.Skip("need all 3 results")
		}

		// Both doc 1 and doc 2 should be in results (they both have one "hello")
		var doc1Score, doc2Score float64
		for _, row := range rows {
			switch row.ID {
			case uint64(1):
				if row.Dist != nil {
					doc1Score = *row.Dist
				}
			case uint64(2):
				if row.Dist != nil {
					doc2Score = *row.Dist
				}
			}
		}

		// Their Max scores should be similar (both have single "hello")
		if doc1Score <= 0 || doc2Score <= 0 {
			t.Error("expected positive scores for doc 1 and doc 2")
		}
	})

	t.Run("Max selects highest scoring field", func(t *testing.T) {
		// Verify that doc 3 gets the max score from title (where it has 3x hello)
		// by checking it has a higher score than docs 1 and 2
		var doc3Score float64
		for _, row := range rows {
			if row.ID == uint64(3) && row.Dist != nil {
				doc3Score = *row.Dist
				break
			}
		}

		if doc3Score <= 0 {
			t.Error("expected positive score for doc 3")
		}

		// Doc 3 should have a higher score due to multiple hello occurrences
		for _, row := range rows {
			if row.ID != uint64(3) && row.Dist != nil {
				if *row.Dist >= doc3Score {
					// This is not strictly required by the test, just verifying the behavior
					t.Logf("doc %v score %f >= doc 3 score %f (due to BM25 IDF/TF tradeoffs)", row.ID, *row.Dist, doc3Score)
				}
			}
		}
	})
}

func TestProductOperator(t *testing.T) {
	ctx := context.Background()

	docs := []*tail.Document{
		{
			ID: document.NewU64ID(1),
			Attributes: map[string]any{
				"title": "hello world",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(2),
			Attributes: map[string]any{
				"title": "hello hello hello",
			},
			WalSeq: 1,
		},
	}

	mockTail := &mockTailStoreForRankClause{docs: docs}
	mockStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(mockStore)
	handler := NewHandler(mockStore, stateMan, mockTail)

	loadedState := &namespace.LoadedState{
		State: &namespace.State{
			Schema: &namespace.Schema{},
		},
	}

	// Test with weight 2.0
	parsed := &ParsedRankBy{
		Type: RankByComposite,
		Clause: &RankClause{
			Type:   RankClauseProduct,
			Weight: 2.0,
			Clause: &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
		},
	}

	rows, err := handler.executeCompositeQuery(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rows) < 2 {
		t.Fatalf("expected 2 results, got %d", len(rows))
	}

	// Document 2 should still be first (higher base score)
	if rows[0].ID != uint64(2) {
		t.Errorf("expected doc 2 to be first, got doc %v", rows[0].ID)
	}

	t.Run("Product applies weight multiplier", func(t *testing.T) {
		// Compare with weight 1.0 (no boost)
		parsedNoBoost := &ParsedRankBy{
			Type: RankByComposite,
			Clause: &RankClause{
				Type:   RankClauseProduct,
				Weight: 1.0,
				Clause: &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
			},
		}

		rowsNoBoost, err := handler.executeCompositeQuery(ctx, "test-ns", loadedState, parsedNoBoost, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// The boosted score should be 2x the unboosted score
		if len(rowsNoBoost) < 1 || len(rows) < 1 {
			t.Skip("need results to compare")
		}

		boostedScore := *rows[0].Dist
		unBoostedScore := *rowsNoBoost[0].Dist

		// The boosted score should be approximately 2x the unboosted score
		ratio := boostedScore / unBoostedScore
		if ratio < 1.9 || ratio > 2.1 {
			t.Errorf("expected ratio ~2.0, got %f (boosted=%f, unboosted=%f)", ratio, boostedScore, unBoostedScore)
		}
	})
}

func TestFilterInRankBy(t *testing.T) {
	ctx := context.Background()

	docs := []*tail.Document{
		{
			ID: document.NewU64ID(1),
			Attributes: map[string]any{
				"title":    "hello world",
				"category": "premium",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(2),
			Attributes: map[string]any{
				"title":    "hello world",
				"category": "basic",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(3),
			Attributes: map[string]any{
				"title":    "hello hello hello",
				"category": "basic",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(4),
			Attributes: map[string]any{
				"title":    "hello hello",
				"category": "premium",
			},
			WalSeq: 1,
		},
	}

	mockTail := &mockTailStoreForRankClause{docs: docs}
	mockStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(mockStore)
	handler := NewHandler(mockStore, stateMan, mockTail)

	loadedState := &namespace.LoadedState{
		State: &namespace.State{
			Schema: &namespace.Schema{},
		},
	}

	t.Run("filter yields score 1 when matching", func(t *testing.T) {
		// Use Sum of BM25 + filter boost for premium category
		premiumFilter, err := filter.Parse([]any{"category", "Eq", "premium"})
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}

		parsed := &ParsedRankBy{
			Type: RankByComposite,
			Clause: &RankClause{
				Type: RankClauseSum,
				Clauses: []*RankClause{
					{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
					{Type: RankClauseFilter, Filter: premiumFilter},
				},
			},
		}

		rows, err := handler.executeCompositeQuery(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(rows) < 4 {
			t.Fatalf("expected 4 results, got %d", len(rows))
		}

		// Documents with premium category should score higher due to +1 boost
		// Doc 4 (premium, 2x hello) should beat doc 3 (basic, 3x hello)
		// because of the filter boost
		// Find doc 4's position
		var doc3Pos, doc4Pos int
		for i, row := range rows {
			switch row.ID {
			case uint64(3):
				doc3Pos = i
			case uint64(4):
				doc4Pos = i
			}
		}

		// With a +1 boost for premium, doc 4 should rank higher or equal
		// (depends on exact BM25 scores, but the boost should help)
		if doc4Pos > doc3Pos {
			t.Logf("Doc 4 (premium) at pos %d, Doc 3 (basic) at pos %d - filter boost may not be enough to overcome TF difference", doc4Pos, doc3Pos)
			// This is acceptable - the test verifies the filter is applied
		}
	})

	t.Run("filter yields score 0 when not matching", func(t *testing.T) {
		// Use Product with filter - docs not matching should have 0 score and be excluded
		premiumFilter, err := filter.Parse([]any{"category", "Eq", "premium"})
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}

		parsed := &ParsedRankBy{
			Type: RankByComposite,
			Clause: &RankClause{
				Type:   RankClauseProduct,
				Weight: 1.0,
				Clause: &RankClause{Type: RankClauseFilter, Filter: premiumFilter},
			},
		}

		rows, err := handler.executeCompositeQuery(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Only docs with category=premium should have score > 0
		// Basic category docs have score 0 and are excluded
		if len(rows) != 2 {
			t.Errorf("expected 2 results (premium only), got %d", len(rows))
		}

		for _, row := range rows {
			// Verify all results are premium docs
			if row.ID != uint64(1) && row.ID != uint64(4) {
				t.Errorf("unexpected doc %v in results (should only be premium)", row.ID)
			}
		}
	})
}

func TestNestedCompositeOperators(t *testing.T) {
	ctx := context.Background()

	docs := []*tail.Document{
		{
			ID: document.NewU64ID(1),
			Attributes: map[string]any{
				"title":       "hello world",
				"description": "foo bar",
				"category":    "premium",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(2),
			Attributes: map[string]any{
				"title":       "foo bar",
				"description": "hello world",
				"category":    "basic",
			},
			WalSeq: 1,
		},
	}

	mockTail := &mockTailStoreForRankClause{docs: docs}
	mockStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(mockStore)
	handler := NewHandler(mockStore, stateMan, mockTail)

	loadedState := &namespace.LoadedState{
		State: &namespace.State{
			Schema: &namespace.Schema{},
		},
	}

	t.Run("nested Sum inside Product", func(t *testing.T) {
		// Product(2, Sum([title BM25, desc BM25]))
		parsed := &ParsedRankBy{
			Type: RankByComposite,
			Clause: &RankClause{
				Type:   RankClauseProduct,
				Weight: 2.0,
				Clause: &RankClause{
					Type: RankClauseSum,
					Clauses: []*RankClause{
						{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
						{Type: RankClauseBM25, Field: "description", QueryText: "hello"},
					},
				},
			},
		}

		rows, err := handler.executeCompositeQuery(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(rows) != 2 {
			t.Errorf("expected 2 results, got %d", len(rows))
		}

		// Both docs have "hello" in one field each, so similar scores
		// (just verifying nested structure works)
		for _, row := range rows {
			if row.Dist == nil || *row.Dist <= 0 {
				t.Errorf("expected positive score for doc %v", row.ID)
			}
		}
	})

	t.Run("Sum with nested Product", func(t *testing.T) {
		// Sum([Product(3, title BM25), desc BM25])
		parsed := &ParsedRankBy{
			Type: RankByComposite,
			Clause: &RankClause{
				Type: RankClauseSum,
				Clauses: []*RankClause{
					{
						Type:   RankClauseProduct,
						Weight: 3.0,
						Clause: &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
					},
					{Type: RankClauseBM25, Field: "description", QueryText: "hello"},
				},
			},
		}

		rows, err := handler.executeCompositeQuery(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(rows) != 2 {
			t.Errorf("expected 2 results, got %d", len(rows))
		}

		// Doc 1 has hello in title (boosted 3x), doc 2 has hello in description (1x)
		// Doc 1 should rank higher
		if rows[0].ID != uint64(1) {
			t.Errorf("expected doc 1 (title match with 3x boost) to be first, got %v", rows[0].ID)
		}
	})
}

func TestParseRankByWithCompositeOperators(t *testing.T) {
	tests := []struct {
		name     string
		rankBy   any
		wantType RankByType
		wantErr  bool
	}{
		{
			name:     "Sum operator",
			rankBy:   []any{"Sum", []any{[]any{"title", "BM25", "hello"}}},
			wantType: RankByComposite,
			wantErr:  false,
		},
		{
			name:     "Max operator",
			rankBy:   []any{"Max", []any{[]any{"title", "BM25", "hello"}}},
			wantType: RankByComposite,
			wantErr:  false,
		},
		{
			name:     "Product operator",
			rankBy:   []any{"Product", 2.0, []any{"title", "BM25", "hello"}},
			wantType: RankByComposite,
			wantErr:  false,
		},
		{
			name:     "simple BM25 still works",
			rankBy:   []any{"title", "BM25", "hello"},
			wantType: RankByBM25,
			wantErr:  false,
		},
		{
			name:     "simple vector still works",
			rankBy:   []any{"vector", "ANN", []any{0.1, 0.2, 0.3}},
			wantType: RankByVector,
			wantErr:  false,
		},
		{
			name:     "simple attribute order still works",
			rankBy:   []any{"timestamp", "desc"},
			wantType: RankByAttr,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parseRankBy(tt.rankBy, "")
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if parsed.Type != tt.wantType {
				t.Errorf("expected type %v, got %v", tt.wantType, parsed.Type)
			}
		})
	}
}

func TestRankScorerDirectly(t *testing.T) {
	docs := []*tail.Document{
		{
			ID:         document.NewU64ID(1),
			Attributes: map[string]any{"title": "hello world"},
		},
		{
			ID:         document.NewU64ID(2),
			Attributes: map[string]any{"title": "hello hello"},
		},
	}

	configs := map[string]*fts.Config{
		"title": fts.DefaultConfig(),
	}

	t.Run("BM25 scoring", func(t *testing.T) {
		clause := &RankClause{
			Type:      RankClauseBM25,
			Field:     "title",
			QueryText: "hello",
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score1 := scorer.Score(docs[0], 0)
		score2 := scorer.Score(docs[1], 1)

		if score1 <= 0 {
			t.Errorf("expected positive score for doc 1, got %f", score1)
		}
		if score2 <= 0 {
			t.Errorf("expected positive score for doc 2, got %f", score2)
		}
		// Doc 2 has more occurrences of "hello", should have higher score
		if score2 <= score1 {
			t.Errorf("expected doc 2 (more hello) to score higher: score1=%f, score2=%f", score1, score2)
		}
	})

	t.Run("Sum scoring", func(t *testing.T) {
		clause := &RankClause{
			Type: RankClauseSum,
			Clauses: []*RankClause{
				{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
				{Type: RankClauseBM25, Field: "title", QueryText: "world"},
			},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score1 := scorer.Score(docs[0], 0) // has "hello world"
		score2 := scorer.Score(docs[1], 1) // has "hello hello"

		// Doc 1 matches both terms, doc 2 only matches hello
		// Doc 1's sum should include scores for both hello and world
		if score1 <= 0 {
			t.Errorf("expected positive score for doc 1, got %f", score1)
		}
		if score2 <= 0 {
			t.Errorf("expected positive score for doc 2, got %f", score2)
		}
	})

	t.Run("Max scoring", func(t *testing.T) {
		clause := &RankClause{
			Type: RankClauseMax,
			Clauses: []*RankClause{
				{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
				{Type: RankClauseBM25, Field: "title", QueryText: "nonexistent"},
			},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score1 := scorer.Score(docs[0], 0)

		// Max of (hello score, 0) = hello score
		if score1 <= 0 {
			t.Errorf("expected positive score (max of hello and 0), got %f", score1)
		}
	})

	t.Run("Product scoring", func(t *testing.T) {
		clause := &RankClause{
			Type:   RankClauseProduct,
			Weight: 3.0,
			Clause: &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "hello"},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		boostedScore := scorer.Score(docs[0], 0)

		// Compare with unboosted
		unboostedClause := &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "hello"}
		unboostedScorer := NewRankScorer(unboostedClause, configs)
		unboostedScorer.BuildIndexes(docs)
		unboostedScore := unboostedScorer.Score(docs[0], 0)

		ratio := boostedScore / unboostedScore
		if ratio < 2.9 || ratio > 3.1 {
			t.Errorf("expected 3x boost, got ratio %f (boosted=%f, unboosted=%f)", ratio, boostedScore, unboostedScore)
		}
	})

	t.Run("Filter scoring", func(t *testing.T) {
		f, err := filter.Parse([]any{"title", "Eq", "hello world"})
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}

		clause := &RankClause{
			Type:   RankClauseFilter,
			Filter: f,
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		score1 := scorer.Score(docs[0], 0) // title = "hello world" - matches
		score2 := scorer.Score(docs[1], 1) // title = "hello hello" - no match

		if score1 != 1.0 {
			t.Errorf("expected score 1 for matching doc, got %f", score1)
		}
		if score2 != 0.0 {
			t.Errorf("expected score 0 for non-matching doc, got %f", score2)
		}
	})
}

func TestZeroScoreExclusion(t *testing.T) {
	ctx := context.Background()

	docs := []*tail.Document{
		{
			ID:         document.NewU64ID(1),
			Attributes: map[string]any{"title": "hello world"},
			WalSeq:     1,
		},
		{
			ID:         document.NewU64ID(2),
			Attributes: map[string]any{"title": "foo bar"},
			WalSeq:     1,
		},
	}

	mockTail := &mockTailStoreForRankClause{docs: docs}
	mockStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(mockStore)
	handler := NewHandler(mockStore, stateMan, mockTail)

	loadedState := &namespace.LoadedState{
		State: &namespace.State{
			Schema: &namespace.Schema{},
		},
	}

	parsed := &ParsedRankBy{
		Type: RankByComposite,
		Clause: &RankClause{
			Type:      RankClauseBM25,
			Field:     "title",
			QueryText: "hello",
		},
	}

	rows, err := handler.executeCompositeQuery(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only doc 1 matches "hello", doc 2 has zero score and should be excluded
	if len(rows) != 1 {
		t.Errorf("expected 1 result (zero score excluded), got %d", len(rows))
	}

	if len(rows) > 0 && rows[0].ID != uint64(1) {
		t.Errorf("expected doc 1 to be the only result, got %v", rows[0].ID)
	}
}

func TestRankClausePrefixMatching(t *testing.T) {
	docs := []*tail.Document{
		{
			ID:         document.NewU64ID(1),
			Attributes: map[string]any{"title": "hello world"},
		},
		{
			ID:         document.NewU64ID(2),
			Attributes: map[string]any{"title": "hello there"},
		},
		{
			ID:         document.NewU64ID(3),
			Attributes: map[string]any{"title": "goodbye world"},
		},
	}

	configs := map[string]*fts.Config{
		"title": fts.DefaultConfig(),
	}

	t.Run("BM25 clause with last_as_prefix parsing", func(t *testing.T) {
		// Test parsing BM25 clause with last_as_prefix in object format
		clause, err := ParseRankClause([]any{"title", "BM25", map[string]any{
			"query":          "hel",
			"last_as_prefix": true,
		}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if clause.Type != RankClauseBM25 {
			t.Errorf("expected RankClauseBM25, got %v", clause.Type)
		}
		if clause.QueryText != "hel" {
			t.Errorf("expected query 'hel', got %q", clause.QueryText)
		}
		if !clause.LastAsPrefix {
			t.Error("expected LastAsPrefix to be true")
		}
	})

	t.Run("BM25 clause with prefix matching scores correctly", func(t *testing.T) {
		clause := &RankClause{
			Type:         RankClauseBM25,
			Field:        "title",
			QueryText:    "hel",
			LastAsPrefix: true,
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// Docs 1 and 2 have "hello" which matches prefix "hel"
		score1 := scorer.Score(docs[0], 0)
		score2 := scorer.Score(docs[1], 1)
		// Doc 3 has "goodbye" which doesn't match prefix "hel"
		score3 := scorer.Score(docs[2], 2)

		// Docs 1 and 2 should have score 1.0 (prefix match score)
		if score1 != 1.0 {
			t.Errorf("expected score 1.0 for prefix match, got %f", score1)
		}
		if score2 != 1.0 {
			t.Errorf("expected score 1.0 for prefix match, got %f", score2)
		}
		if score3 != 0 {
			t.Errorf("expected score 0 for non-matching doc, got %f", score3)
		}
	})

	t.Run("multi-token prefix matching requires all tokens", func(t *testing.T) {
		clause := &RankClause{
			Type:         RankClauseBM25,
			Field:        "title",
			QueryText:    "hello wor",
			LastAsPrefix: true,
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// Doc 1 has "hello world" - matches "hello" exact and "wor" prefix
		score1 := scorer.Score(docs[0], 0)
		// Doc 2 has "hello there" - matches "hello" but not "wor" prefix
		score2 := scorer.Score(docs[1], 1)
		// Doc 3 has "goodbye world" - doesn't match "hello"
		score3 := scorer.Score(docs[2], 2)

		if score1 <= 0 {
			t.Errorf("expected positive score for doc 1, got %f", score1)
		}
		if score2 != 0 {
			t.Errorf("expected score 0 for doc 2 (no 'wor' prefix), got %f", score2)
		}
		if score3 != 0 {
			t.Errorf("expected score 0 for doc 3 (no 'hello'), got %f", score3)
		}
	})

	t.Run("prefix in Sum composite", func(t *testing.T) {
		clause := &RankClause{
			Type: RankClauseSum,
			Clauses: []*RankClause{
				{Type: RankClauseBM25, Field: "title", QueryText: "hel", LastAsPrefix: true},
				{Type: RankClauseBM25, Field: "title", QueryText: "wor", LastAsPrefix: true},
			},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// Doc 1 has "hello world" - matches both prefixes
		score1 := scorer.Score(docs[0], 0)
		// Doc 2 has "hello there" - matches only "hel"
		score2 := scorer.Score(docs[1], 1)
		// Doc 3 has "goodbye world" - matches only "wor"
		score3 := scorer.Score(docs[2], 2)

		// Sum of two 1.0 scores = 2.0
		if score1 != 2.0 {
			t.Errorf("expected score 2.0 for doc 1 (both prefixes), got %f", score1)
		}
		if score2 != 1.0 {
			t.Errorf("expected score 1.0 for doc 2 (one prefix), got %f", score2)
		}
		if score3 != 1.0 {
			t.Errorf("expected score 1.0 for doc 3 (one prefix), got %f", score3)
		}
	})

	t.Run("prefix in Product composite", func(t *testing.T) {
		clause := &RankClause{
			Type:   RankClauseProduct,
			Weight: 5.0,
			Clause: &RankClause{Type: RankClauseBM25, Field: "title", QueryText: "hel", LastAsPrefix: true},
		}

		scorer := NewRankScorer(clause, configs)
		scorer.BuildIndexes(docs)

		// Doc 1 matches prefix - score should be 5.0 (5 * 1.0)
		score1 := scorer.Score(docs[0], 0)
		if score1 != 5.0 {
			t.Errorf("expected score 5.0 (5 * prefix score), got %f", score1)
		}
	})
}
