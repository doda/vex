package indexer

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/internal/wal"
)

func TestIndexerPublishesFilterArtifacts(t *testing.T) {
	store := newMockStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()
	ns := "test-ns"

	loaded, err := stateMan.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	filterable := true
	loaded, err = stateMan.Update(ctx, ns, loaded.ETag, func(state *namespace.State) error {
		state.Schema = &namespace.Schema{
			Attributes: map[string]namespace.AttributeSchema{
				"category": {Type: "string", Filterable: &filterable},
			},
		}
		state.Vector = &namespace.VectorConfig{
			Dims:           4,
			DType:          "f32",
			DistanceMetric: "cosine_distance",
			ANN:            true,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update schema: %v", err)
	}

	docs := []vectorTestDoc{
		{id: 1, attrs: map[string]any{"category": "A"}, vector: []float32{1.0, 0.0, 0.0, 0.0}},
		{id: 2, attrs: map[string]any{"category": "B"}, vector: []float32{0.9, 0.1, 0.0, 0.0}},
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
	if len(segment.FilterKeys) != 1 {
		t.Fatalf("expected 1 filter key, got %d", len(segment.FilterKeys))
	}
	filterKey := segment.FilterKeys[0]
	if !strings.Contains(filterKey, "/filters/category.bitmap") {
		t.Fatalf("expected category filter key, got %s", filterKey)
	}
	if _, err := store.Head(ctx, filterKey); err != nil {
		t.Fatalf("expected filter object to exist: %v", err)
	}

	handler := query.NewHandler(store, stateMan, nil)
	req := &query.QueryRequest{
		RankBy:  []any{"vector", "ANN", []any{1.0, 0.0, 0.0, 0.0}},
		Filters: []any{"category", "Eq", "A"},
		Limit:   10,
	}
	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("filtered query failed: %v", err)
	}
	if len(resp.Rows) != 1 {
		t.Fatalf("expected 1 filtered result, got %d", len(resp.Rows))
	}
	if resp.Rows[0].ID != uint64(1) {
		t.Fatalf("expected result ID 1, got %v", resp.Rows[0].ID)
	}
}
