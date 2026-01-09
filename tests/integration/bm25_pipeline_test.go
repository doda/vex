package integration

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

type countingStore struct {
	objectstore.Store
	mu        sync.Mutex
	getCounts map[string]int
}

func newCountingStore(store objectstore.Store) *countingStore {
	return &countingStore{
		Store:     store,
		getCounts: make(map[string]int),
	}
}

func (s *countingStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	s.mu.Lock()
	s.getCounts[key]++
	s.mu.Unlock()
	return s.Store.Get(ctx, key, opts)
}

func (s *countingStore) ResetCounts() {
	s.mu.Lock()
	s.getCounts = make(map[string]int)
	s.mu.Unlock()
}

func (s *countingStore) GetCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCounts[key]
}

func hasFTSTerm(terms []string, term string) bool {
	if len(terms) == 0 {
		return false
	}
	idx := sort.SearchStrings(terms, term)
	return idx < len(terms) && terms[idx] == term
}

func TestObjectStoreBM25PipelineSkipsNonMatchingSegments(t *testing.T) {
	ns := fmt.Sprintf("objectstore-bm25-pipeline-%d", time.Now().UnixNano())
	store := newCountingStore(newS3Store(t))
	fixture := newS3FixtureWithStore(t, store)
	defer fixture.close(t)

	fixture.write(t, ns, map[string]any{
		"schema": map[string]any{
			"content": map[string]any{
				"type":             "string",
				"full_text_search": true,
			},
		},
		"upsert_rows": []map[string]any{
			{"id": "doc1", "content": "alpha bravo"},
			{"id": "doc2", "content": "alpha alpha charlie"},
		},
	})
	waitForStateHeadSeq(t, store, ns, 1)
	indexNamespaceRange(t, store, ns, 0, 1)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc3", "content": "beta beta"},
			{"id": "doc4", "content": "beta delta"},
		},
	})
	waitForStateHeadSeq(t, store, ns, 2)
	indexNamespaceRange(t, store, ns, 1, 2)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc5", "content": "alpha echo"},
			{"id": "doc6", "content": "echo foxtrot"},
		},
	})
	waitForStateHeadSeq(t, store, ns, 3)
	indexNamespaceRange(t, store, ns, 2, 3)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc4", "content": "alpha alpha alpha"},
		},
		"deletes": []any{"doc2"},
	})
	waitForStateHeadSeq(t, store, ns, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stateMan := namespace.NewStateManager(store)
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	manifestKey := loaded.State.Index.ManifestKey
	if manifestKey == "" {
		t.Fatalf("expected manifest key to be set after indexing")
	}
	reader := index.NewReader(store, nil, nil)
	manifest, err := reader.LoadManifest(ctx, manifestKey)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}
	if manifest == nil || len(manifest.Segments) != 3 {
		count := 0
		if manifest != nil {
			count = len(manifest.Segments)
		}
		t.Fatalf("expected 3 segments, got %d", count)
	}

	segmentsWithTerms, err := reader.LoadFTSTermsForField(ctx, manifestKey, "content")
	if err != nil {
		t.Fatalf("failed to load FTS terms: %v", err)
	}
	if len(segmentsWithTerms) != len(manifest.Segments) {
		t.Fatalf("expected terms for %d segments, got %d", len(manifest.Segments), len(segmentsWithTerms))
	}

	matchingKeys := make(map[string]bool)
	nonMatchingKeys := make(map[string]bool)
	for _, entry := range segmentsWithTerms {
		if len(entry.Segment.FTSKeys) == 0 || len(entry.Segment.FTSTermKeys) == 0 {
			t.Fatalf("expected FTS keys and term keys on segment %s", entry.Segment.ID)
		}
		hasAlpha := hasFTSTerm(entry.Terms, "alpha")
		for _, key := range entry.Segment.FTSKeys {
			if !strings.HasSuffix(key, "fts.content.bm25") {
				continue
			}
			if hasAlpha {
				matchingKeys[key] = true
			} else {
				nonMatchingKeys[key] = true
			}
		}
	}
	if len(matchingKeys) == 0 || len(nonMatchingKeys) == 0 {
		t.Fatalf("expected both matching and non-matching segments, got match=%d nonmatch=%d", len(matchingKeys), len(nonMatchingKeys))
	}

	store.ResetCounts()

	result := fixture.query(t, ns, map[string]any{
		"rank_by": []any{"content", "BM25", "alpha"},
		"limit":   4,
	})
	rows, ok := result["rows"].([]any)
	if !ok {
		t.Fatalf("expected rows to be an array, got %T", result["rows"])
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	ids := extractRowIDs(t, result)
	for _, id := range []string{"doc1", "doc4", "doc5"} {
		if !ids[id] {
			t.Fatalf("expected BM25 results to include %s, got %v", id, ids)
		}
	}
	if ids["doc2"] || ids["doc3"] {
		t.Fatalf("expected doc2/doc3 to be excluded, got %v", ids)
	}

	var doc4Attrs map[string]any
	for _, row := range rows {
		entry, ok := row.(map[string]any)
		if !ok {
			t.Fatalf("expected row to be object, got %T", row)
		}
		if entry["id"] == "doc4" {
			attrs, ok := entry["attributes"].(map[string]any)
			if !ok {
				t.Fatalf("expected attributes to be object for doc4, got %T", entry["attributes"])
			}
			doc4Attrs = attrs
			break
		}
	}
	if doc4Attrs == nil {
		t.Fatalf("expected doc4 to be present in results")
	}
	if doc4Attrs["content"] != "alpha alpha alpha" {
		t.Fatalf("expected doc4 content to come from tail update, got %v", doc4Attrs["content"])
	}

	for key := range matchingKeys {
		if store.GetCount(key) == 0 {
			t.Fatalf("expected FTS index to be loaded for key %s", key)
		}
	}
	for key := range nonMatchingKeys {
		if store.GetCount(key) != 0 {
			t.Fatalf("expected FTS index to be skipped for key %s", key)
		}
	}
}
