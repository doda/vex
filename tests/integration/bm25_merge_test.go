package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/indexer"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func indexNamespaceRange(t *testing.T, store objectstore.Store, ns string, startSeq, endSeq uint64) {
	t.Helper()
	stateMan := namespace.NewStateManager(store)
	idx := indexer.New(store, stateMan, nil, nil)
	processor := indexer.NewL0SegmentProcessor(store, stateMan, nil, idx)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state for indexing: %v", err)
	}
	result, err := processor.ProcessWAL(ctx, ns, startSeq, endSeq, loaded.State, loaded.ETag)
	if err != nil {
		t.Fatalf("failed to index WAL range: %v", err)
	}
	if result == nil || !result.ManifestWritten {
		t.Fatalf("expected manifest to be written during indexing")
	}
}

func TestObjectStoreBM25TopKMergeAcrossSegments(t *testing.T) {
	ns := fmt.Sprintf("objectstore-bm25-merge-%d", time.Now().UnixNano())
	fixture := newS3Fixture(t, ns)
	defer fixture.close(t)

	fixture.write(t, ns, map[string]any{
		"schema": map[string]any{
			"content": map[string]any{
				"type":             "string",
				"full_text_search": true,
			},
		},
		"upsert_rows": []map[string]any{
			{"id": "doc1", "content": "alpha alpha alpha alpha"},
			{"id": "doc2", "content": "alpha beta beta beta"},
		},
	})
	waitForStateHeadSeq(t, fixture.store, ns, 1)
	indexNamespaceRange(t, fixture.store, ns, 0, 1)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc3", "content": "alpha alpha alpha beta"},
			{"id": "doc4", "content": "alpha alpha beta beta"},
		},
	})
	waitForStateHeadSeq(t, fixture.store, ns, 2)
	indexNamespaceRange(t, fixture.store, ns, 1, 2)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc5", "content": "alpha alpha beta gamma"},
			{"id": "doc6", "content": "alpha beta gamma delta"},
		},
	})
	waitForStateHeadSeq(t, fixture.store, ns, 3)
	indexNamespaceRange(t, fixture.store, ns, 2, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stateMan := namespace.NewStateManager(fixture.store)
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if loaded.State.Index.ManifestKey == "" {
		t.Fatalf("expected manifest key to be set after indexing")
	}
	reader := index.NewReader(fixture.store, nil, nil)
	manifest, err := reader.LoadManifest(ctx, loaded.State.Index.ManifestKey)
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

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc4", "content": "alpha alpha alpha alpha"},
		},
		"deletes": []any{"doc2"},
	})
	waitForStateHeadSeq(t, fixture.store, ns, 4)

	result := fixture.query(t, ns, map[string]any{
		"rank_by": []any{"content", "BM25", "alpha"},
		"limit":   3,
	})
	rows, ok := result["rows"].([]any)
	if !ok {
		t.Fatalf("expected rows to be an array, got %T", result["rows"])
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	ids := extractRowIDs(t, result)
	for _, id := range []string{"doc1", "doc3", "doc4"} {
		if !ids[id] {
			t.Fatalf("expected BM25 results to include %s, got %v", id, ids)
		}
	}
	if ids["doc2"] {
		t.Fatalf("expected doc2 to be deleted, got %v", ids)
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
	if doc4Attrs["content"] != "alpha alpha alpha alpha" {
		t.Fatalf("expected doc4 content to come from tail update, got %v", doc4Attrs["content"])
	}
}
