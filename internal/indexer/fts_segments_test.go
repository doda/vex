package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
)

func TestIndexerPublishesFTSSegments(t *testing.T) {
	store := newMockStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()
	ns := "test-ns"

	loaded, err := stateMan.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	ftsConfig := map[string]any{"tokenizer": "word_v3"}
	ftsJSON, _ := json.Marshal(ftsConfig)
	loaded, err = stateMan.Update(ctx, ns, loaded.ETag, func(state *namespace.State) error {
		state.Schema = &namespace.Schema{
			Attributes: map[string]namespace.AttributeSchema{
				"title": {Type: "string", FullTextSearch: ftsJSON},
				"body":  {Type: "string", FullTextSearch: ftsJSON},
				"count": {Type: "int"},
			},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update schema: %v", err)
	}

	docs := []vectorTestDoc{
		{id: 1, attrs: map[string]any{"title": "Hello world", "body": "Welcome to vex"}},
		{id: 2, attrs: map[string]any{"title": "Search engine", "body": "FTS indexing"}},
	}
	_, data := createVectorWALEntry(ns, 1, docs)
	walKey := fmt.Sprintf("vex/namespaces/%s/%s", ns, wal.KeyForSeq(1))
	store.mu.Lock()
	store.objects[walKey] = mockObject{data: data, etag: "etag1"}
	store.mu.Unlock()

	loaded, err = stateMan.AdvanceWAL(ctx, ns, loaded.ETag, walKey, int64(len(data)), nil)
	if err != nil {
		t.Fatalf("failed to advance WAL: %v", err)
	}

	indexer := New(store, stateMan, DefaultConfig(), nil)
	processor := NewL0SegmentProcessor(store, stateMan, nil, indexer)
	result, err := processor.ProcessWAL(ctx, ns, 0, 1, loaded.State, loaded.ETag)
	if err != nil {
		t.Fatalf("ProcessWAL failed: %v", err)
	}
	if result == nil || result.ManifestKey == "" {
		t.Fatal("expected manifest key after indexing")
	}

	reader := index.NewReader(store, nil, nil)
	manifest, err := reader.LoadManifest(ctx, result.ManifestKey)
	if err != nil {
		t.Fatalf("LoadManifest failed: %v", err)
	}
	if manifest == nil || len(manifest.Segments) != 1 {
		t.Fatalf("expected 1 segment in manifest, got %v", manifest)
	}

	segment := manifest.Segments[0]
	if len(segment.FTSKeys) != 2 {
		t.Fatalf("expected 2 FTS keys, got %d", len(segment.FTSKeys))
	}

	expected := map[string]bool{
		"fts.title.bm25": false,
		"fts.body.bm25":  false,
	}
	for _, key := range segment.FTSKeys {
		if _, err := store.Head(ctx, key); err != nil {
			t.Fatalf("expected FTS object to exist: %v", err)
		}
		if !strings.Contains(key, "/fts.") || !strings.HasSuffix(key, ".bm25") {
			t.Fatalf("unexpected FTS key format: %s", key)
		}
		for suffix := range expected {
			if strings.Contains(key, suffix) {
				expected[suffix] = true
			}
		}
	}
	for suffix, seen := range expected {
		if !seen {
			t.Fatalf("expected FTS key with %s", suffix)
		}
	}
}
