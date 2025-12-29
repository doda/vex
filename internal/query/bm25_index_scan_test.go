package query

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// mockTailStoreForBM25Index implements tail.Store for BM25 index scan testing.
type mockTailStoreForBM25Index struct {
	docs []*tail.Document
}

func (m *mockTailStoreForBM25Index) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *mockTailStoreForBM25Index) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	// Return all docs including deleted ones for proper deduplication
	// The deduplication logic needs deleted docs to shadow indexed versions
	if f == nil {
		return m.docs, nil
	}
	result := make([]*tail.Document, 0)
	for _, doc := range m.docs {
		// Deleted docs bypass filter (they need to shadow indexed docs)
		if doc.Deleted {
			result = append(result, doc)
			continue
		}
		filterDoc := make(map[string]any)
		filterDoc["id"] = doc.ID.String()
		for k, v := range doc.Attributes {
			filterDoc[k] = v
		}
		if f.Eval(filterDoc) {
			result = append(result, doc)
		}
	}
	return result, nil
}

func (m *mockTailStoreForBM25Index) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, limit int64) ([]*tail.Document, error) {
	return m.Scan(ctx, ns, f)
}

func (m *mockTailStoreForBM25Index) VectorScan(ctx context.Context, ns string, qv []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *mockTailStoreForBM25Index) VectorScanWithByteLimit(ctx context.Context, ns string, qv []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, limit int64) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *mockTailStoreForBM25Index) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *mockTailStoreForBM25Index) TailBytes(ns string) int64 {
	return 0
}

func (m *mockTailStoreForBM25Index) Clear(ns string) {}

func (m *mockTailStoreForBM25Index) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *mockTailStoreForBM25Index) Close() error {
	return nil
}

// setupIndexedDocsInStore creates indexed documents in the object store
// that can be read by the index.Reader.
func setupIndexedDocsInStore(t *testing.T, ctx context.Context, store objectstore.Store, nsName string, docs []index.IndexedDocument) string {
	t.Helper()

	// Create the segment docs file
	docsData, err := json.Marshal(docs)
	if err != nil {
		t.Fatalf("failed to marshal docs: %v", err)
	}
	docsKey := "vex/namespaces/" + nsName + "/index/segments/seg_001/docs.json"
	store.PutIfAbsent(ctx, docsKey, bytes.NewReader(docsData), int64(len(docsData)), nil)

	// Create the manifest
	manifest := &index.Manifest{
		FormatVersion: 1,
		Namespace:     nsName,
		IndexedWALSeq: 5,
		Segments: []index.Segment{
			{
				ID:          "seg_001",
				DocsKey:     docsKey,
				StartWALSeq: 1,
				EndWALSeq:   5,
			},
		},
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	manifestKey := "vex/namespaces/" + nsName + "/index/manifests/manifest_001.json"
	store.PutIfAbsent(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)

	return manifestKey
}

func TestBM25IndexScan(t *testing.T) {
	ctx := context.Background()

	// Create indexed documents (from segments)
	indexedDocs := []index.IndexedDocument{
		{
			ID:        "u64:100",
			NumericID: 100,
			WALSeq:    5,
			Deleted:   false,
			Attributes: map[string]any{
				"title":   "Indexed document about cats",
				"content": "Cats are popular pets that love to play",
			},
		},
		{
			ID:        "u64:101",
			NumericID: 101,
			WALSeq:    5,
			Deleted:   false,
			Attributes: map[string]any{
				"title":   "Dogs and their behavior",
				"content": "Dogs are loyal animals",
			},
		},
		{
			ID:        "u64:102",
			NumericID: 102,
			WALSeq:    5,
			Deleted:   false,
			Attributes: map[string]any{
				"title":   "About cats and dogs",
				"content": "Both cats and dogs make great pets",
			},
		},
	}

	// Create tail documents (unindexed)
	tailDocs := []*tail.Document{
		{
			ID: document.NewU64ID(200),
			Attributes: map[string]any{
				"title":   "New article about cats",
				"content": "Cats sleep a lot during the day",
			},
			WalSeq: 10, // Newer WAL seq than indexed
		},
	}

	// Create mock stores
	mockTail := &mockTailStoreForBM25Index{docs: tailDocs}
	mockStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(mockStore)

	// Setup indexed docs in object store
	manifestKey := setupIndexedDocsInStore(t, ctx, mockStore, "test-ns", indexedDocs)

	// Create handler with index reader
	handler := NewHandler(mockStore, stateMan, mockTail)

	ftsConfig := map[string]any{
		"tokenizer":        "word_v3",
		"case_sensitive":   false,
		"remove_stopwords": true,
	}
	ftsJSON, _ := json.Marshal(ftsConfig)

	loadedState := &namespace.LoadedState{
		State: &namespace.State{
			Schema: &namespace.Schema{
				Attributes: map[string]namespace.AttributeSchema{
					"title": {
						Type:           "string",
						FullTextSearch: ftsJSON,
					},
					"content": {
						Type:           "string",
						FullTextSearch: ftsJSON,
					},
				},
			},
			Index: namespace.IndexState{
				ManifestKey:   manifestKey,
				ManifestSeq:   1,
				IndexedWALSeq: 5, // Up to WAL seq 5 is indexed
			},
		},
	}

	t.Run("indexed docs are returned in BM25 query", func(t *testing.T) {
		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "title",
			QueryText: "cats",
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should return docs with "cats" in title:
		// - Doc 100: "Indexed document about cats"
		// - Doc 102: "About cats and dogs"
		// - Doc 200: "New article about cats" (from tail)
		if len(rows) != 3 {
			t.Errorf("expected 3 results (2 indexed + 1 tail), got %d", len(rows))
			for _, r := range rows {
				t.Logf("  got doc %v with score %v", r.ID, r.Dist)
			}
		}

		// Verify all results have positive BM25 scores
		for _, row := range rows {
			if row.Dist == nil {
				t.Error("expected $dist to be set")
			} else if *row.Dist <= 0 {
				t.Errorf("expected positive score, got %f for doc %v", *row.Dist, row.ID)
			}
		}
	})

	t.Run("tail docs are included with correct scoring", func(t *testing.T) {
		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "content",
			QueryText: "cats",
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should return docs with "cats" in content:
		// - Doc 100: "Cats are popular pets that love to play"
		// - Doc 102: "Both cats and dogs make great pets"
		// - Doc 200: "Cats sleep a lot during the day" (from tail)
		if len(rows) != 3 {
			t.Errorf("expected 3 results, got %d", len(rows))
		}

		// Check that doc 200 (from tail) is included
		found200 := false
		for _, row := range rows {
			if id, ok := row.ID.(uint64); ok && id == 200 {
				found200 = true
			}
		}
		if !found200 {
			t.Error("expected tail doc 200 to be included in results")
		}
	})

	t.Run("tail doc takes precedence when document updated", func(t *testing.T) {
		// Add a tail doc that updates an indexed doc
		updateTailDocs := []*tail.Document{
			{
				ID: document.NewU64ID(100), // Same ID as indexed doc
				Attributes: map[string]any{
					"title":   "Updated article about dogs", // Changed from cats to dogs
					"content": "This is all about dogs now",
				},
				WalSeq: 15, // Newer WAL seq
			},
		}
		mockTail.docs = updateTailDocs

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "title",
			QueryText: "cats",
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Doc 100 should NOT be returned since the tail update changed title from cats to dogs
		// Should only return:
		// - Doc 102: "About cats and dogs"
		// Note: Doc 200 is not in updateTailDocs
		for _, row := range rows {
			if id, ok := row.ID.(uint64); ok && id == 100 {
				t.Errorf("doc 100 should not be returned - it was updated to 'dogs' in tail")
			}
		}

		// Reset tail docs
		mockTail.docs = tailDocs
	})

	t.Run("deleted docs in tail are excluded", func(t *testing.T) {
		// Add a tail doc that deletes an indexed doc
		deleteTailDocs := []*tail.Document{
			{
				ID:      document.NewU64ID(100), // Same ID as indexed doc
				WalSeq:  15,                     // Newer WAL seq
				Deleted: true,                   // Mark as deleted
			},
		}
		mockTail.docs = deleteTailDocs

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "title",
			QueryText: "cats",
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Doc 100 should NOT be returned since it's deleted in tail
		for _, row := range rows {
			if id, ok := row.ID.(uint64); ok && id == 100 {
				t.Errorf("deleted doc 100 should not be returned")
			}
		}

		// Reset tail docs
		mockTail.docs = tailDocs
	})

	t.Run("search only indexed when no tail", func(t *testing.T) {
		// Create handler with no tail store
		handlerNoTail := NewHandler(mockStore, stateMan, nil)

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "title",
			QueryText: "cats",
		}

		rows, err := handlerNoTail.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should return indexed docs only:
		// - Doc 100: "Indexed document about cats"
		// - Doc 102: "About cats and dogs"
		if len(rows) != 2 {
			t.Errorf("expected 2 results (indexed only), got %d", len(rows))
		}
	})
}

func TestBM25IndexScanDeduplication(t *testing.T) {
	ctx := context.Background()

	// Create indexed doc
	indexedDocs := []index.IndexedDocument{
		{
			ID:        "u64:100",
			NumericID: 100,
			WALSeq:    5,
			Deleted:   false,
			Attributes: map[string]any{
				"title": "Original indexed version about cats",
			},
		},
	}

	mockStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(mockStore)
	manifestKey := setupIndexedDocsInStore(t, ctx, mockStore, "test-ns", indexedDocs)

	ftsConfig := map[string]any{
		"tokenizer":        "word_v3",
		"case_sensitive":   false,
		"remove_stopwords": true,
	}
	ftsJSON, _ := json.Marshal(ftsConfig)

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
			Index: namespace.IndexState{
				ManifestKey:   manifestKey,
				ManifestSeq:   1,
				IndexedWALSeq: 5,
			},
		},
	}

	t.Run("deduplication keeps higher WAL seq version", func(t *testing.T) {
		// Tail has newer version of doc 100
		tailDocs := []*tail.Document{
			{
				ID: document.NewU64ID(100),
				Attributes: map[string]any{
					"title": "Updated tail version about cats and more cats",
				},
				WalSeq: 10, // Newer than indexed (5)
			},
		}
		mockTail := &mockTailStoreForBM25Index{docs: tailDocs}
		handler := NewHandler(mockStore, stateMan, mockTail)

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "title",
			QueryText: "cats",
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should only have 1 result (deduplicated)
		if len(rows) != 1 {
			t.Errorf("expected 1 result (deduplicated), got %d", len(rows))
		}

		// Verify it's the tail version (has "more cats")
		if len(rows) > 0 {
			attrs := rows[0].Attributes
			if attrs != nil {
				title := attrs["title"].(string)
				if title != "Updated tail version about cats and more cats" {
					t.Errorf("expected tail version title, got %q", title)
				}
			}
		}
	})

	t.Run("deduplication removes deleted docs", func(t *testing.T) {
		// Tail has deleted version of doc 100
		tailDocs := []*tail.Document{
			{
				ID:      document.NewU64ID(100),
				WalSeq:  10,
				Deleted: true,
			},
		}
		mockTail := &mockTailStoreForBM25Index{docs: tailDocs}
		handler := NewHandler(mockStore, stateMan, mockTail)

		parsed := &ParsedRankBy{
			Type:      RankByBM25,
			Field:     "title",
			QueryText: "cats",
		}

		rows, err := handler.executeBM25Query(ctx, "test-ns", loadedState, parsed, nil, &QueryRequest{Limit: 10}, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should have 0 results (indexed doc deleted by tail)
		if len(rows) != 0 {
			t.Errorf("expected 0 results (deleted), got %d", len(rows))
		}
	})
}
