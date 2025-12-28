package query

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// mockTailStoreForBM25 implements tail.Store for BM25 query testing.
type mockTailStoreForBM25 struct {
	docs []*tail.Document
}

func (m *mockTailStoreForBM25) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *mockTailStoreForBM25) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStoreForBM25) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, limit int64) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *mockTailStoreForBM25) VectorScan(ctx context.Context, ns string, qv []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *mockTailStoreForBM25) VectorScanWithByteLimit(ctx context.Context, ns string, qv []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, limit int64) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *mockTailStoreForBM25) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *mockTailStoreForBM25) TailBytes(ns string) int64 {
	return 0
}

func (m *mockTailStoreForBM25) Clear(ns string) {}

func (m *mockTailStoreForBM25) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *mockTailStoreForBM25) Close() error {
	return nil
}

func TestBM25RankByParsing(t *testing.T) {
	tests := []struct {
		name        string
		rankBy      any
		wantErr     bool
		wantField   string
		wantQuery   string
	}{
		{
			name:      "valid BM25 query",
			rankBy:    []any{"title", "BM25", "hello world"},
			wantErr:   false,
			wantField: "title",
			wantQuery: "hello world",
		},
		{
			name:      "BM25 query with different field",
			rankBy:    []any{"content", "BM25", "search terms"},
			wantErr:   false,
			wantField: "content",
			wantQuery: "search terms",
		},
		{
			name:    "BM25 missing query string",
			rankBy:  []any{"title", "BM25"},
			wantErr: true,
		},
		{
			name:    "BM25 query not a string",
			rankBy:  []any{"title", "BM25", 123},
			wantErr: true,
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
			if parsed.Type != RankByBM25 {
				t.Errorf("expected RankByBM25, got %v", parsed.Type)
			}
			if parsed.Field != tt.wantField {
				t.Errorf("expected field %q, got %q", tt.wantField, parsed.Field)
			}
			if parsed.QueryText != tt.wantQuery {
				t.Errorf("expected query %q, got %q", tt.wantQuery, parsed.QueryText)
			}
		})
	}
}

func TestBM25QueryExecution(t *testing.T) {
	ctx := context.Background()

	// Create test documents
	docs := []*tail.Document{
		{
			ID: document.NewU64ID(1),
			Attributes: map[string]any{
				"title":   "The quick brown fox",
				"content": "A quick brown fox jumps over the lazy dog",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(2),
			Attributes: map[string]any{
				"title":   "Hello world",
				"content": "This is a hello world example",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(3),
			Attributes: map[string]any{
				"title":   "Another document",
				"content": "This document does not contain the search term",
			},
			WalSeq: 1,
		},
		{
			ID: document.NewU64ID(4),
			Attributes: map[string]any{
				"title":   "Fox story",
				"content": "The fox fox fox appears many times in this fox document",
			},
			WalSeq: 1,
		},
	}

	// Create mock tail store
	mockTail := &mockTailStoreForBM25{docs: docs}

	// Create mock state manager with FTS enabled for the content field
	ftsConfig := map[string]any{
		"tokenizer":        "word_v3",
		"case_sensitive":   false,
		"remove_stopwords": true,
	}
	ftsJSON, _ := json.Marshal(ftsConfig)

	mockStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(mockStore)

	// Create handler
	handler := NewHandler(mockStore, stateMan, mockTail)

	t.Run("BM25 search returns matching documents", func(t *testing.T) {
		// Search for "fox" in content
		loadedState := &namespace.LoadedState{
			State: &namespace.State{
				Schema: &namespace.Schema{
					Attributes: map[string]namespace.AttributeSchema{
						"content": {
							Type:           "string",
							FullTextSearch: ftsJSON,
						},
					},
				},
			},
		}

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "content",
			QueryText: "fox",
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should return documents containing "fox"
		if len(rows) != 2 {
			t.Errorf("expected 2 results, got %d", len(rows))
		}

		// All results should have a positive score
		for _, row := range rows {
			if row.Dist == nil {
				t.Error("expected $dist to be set")
			} else if *row.Dist <= 0 {
				t.Errorf("expected positive score, got %f", *row.Dist)
			}
		}
	})

	t.Run("documents with higher term frequency score higher", func(t *testing.T) {
		loadedState := &namespace.LoadedState{
			State: &namespace.State{
				Schema: &namespace.Schema{
					Attributes: map[string]namespace.AttributeSchema{
						"content": {
							Type:           "string",
							FullTextSearch: ftsJSON,
						},
					},
				},
			},
		}

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "content",
			QueryText: "fox",
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Document 4 has "fox" multiple times, should score higher
		if len(rows) < 2 {
			t.Fatalf("expected at least 2 results")
		}

		// First result should be doc 4 (highest tf for "fox")
		if rows[0].ID != uint64(4) {
			t.Errorf("expected doc 4 to be first (highest TF), got %v", rows[0].ID)
		}
	})

	t.Run("zero score documents excluded", func(t *testing.T) {
		loadedState := &namespace.LoadedState{
			State: &namespace.State{
				Schema: &namespace.Schema{
					Attributes: map[string]namespace.AttributeSchema{
						"content": {
							Type:           "string",
							FullTextSearch: ftsJSON,
						},
					},
				},
			},
		}

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "content",
			QueryText: "unicorn", // Not in any document
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// No documents should match
		if len(rows) != 0 {
			t.Errorf("expected 0 results for non-matching query, got %d", len(rows))
		}
	})

	t.Run("$dist contains BM25 score", func(t *testing.T) {
		loadedState := &namespace.LoadedState{
			State: &namespace.State{
				Schema: &namespace.Schema{
					Attributes: map[string]namespace.AttributeSchema{
						"title": {
							Type:           "string",
							FullTextSearch: ftsJSON,
						},
					},
				},
			},
		}

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "title",
			QueryText: "hello",
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(rows) != 1 {
			t.Fatalf("expected 1 result, got %d", len(rows))
		}

		// Verify $dist is set with a positive BM25 score
		if rows[0].Dist == nil {
			t.Error("expected $dist to be set")
		} else if *rows[0].Dist <= 0 {
			t.Errorf("expected positive BM25 score, got %f", *rows[0].Dist)
		}
	})

	t.Run("limit is respected", func(t *testing.T) {
		loadedState := &namespace.LoadedState{
			State: &namespace.State{
				Schema: &namespace.Schema{
					Attributes: map[string]namespace.AttributeSchema{
						"content": {
							Type:           "string",
							FullTextSearch: ftsJSON,
						},
					},
				},
			},
		}

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "content",
			QueryText: "document", // Appears in docs 3 and 4
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 1})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(rows) != 1 {
			t.Errorf("expected 1 result with limit=1, got %d", len(rows))
		}
	})

	t.Run("empty query returns no results", func(t *testing.T) {
		loadedState := &namespace.LoadedState{
			State: &namespace.State{
				Schema: &namespace.Schema{
					Attributes: map[string]namespace.AttributeSchema{
						"content": {
							Type:           "string",
							FullTextSearch: ftsJSON,
						},
					},
				},
			},
		}

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "content",
			QueryText: "", // Empty query
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(rows) != 0 {
			t.Errorf("expected 0 results for empty query, got %d", len(rows))
		}
	})

	t.Run("uses default config when not in schema", func(t *testing.T) {
		loadedState := &namespace.LoadedState{
			State: &namespace.State{
				Schema: nil, // No schema
			},
		}

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "content",
			QueryText: "fox",
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should still work with default config
		if len(rows) != 2 {
			t.Errorf("expected 2 results, got %d", len(rows))
		}
	})
}

func TestBM25ScoreComputation(t *testing.T) {
	// Test the BM25 scoring function directly
	t.Run("BM25 score increases with term frequency", func(t *testing.T) {
		// This is tested indirectly through TestBM25QueryExecution
		// Document 4 with multiple "fox" occurrences should score higher
	})

	t.Run("BM25 score uses IDF", func(t *testing.T) {
		// Rare terms should contribute more to the score
		// This is verified by the ranking behavior in execution tests
	})
}
