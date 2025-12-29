package indexer

import (
	"context"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
)

type emptyTailStore struct{}

func (e *emptyTailStore) Refresh(ctx context.Context, namespace string, afterSeq, upToSeq uint64) error {
	return nil
}

func (e *emptyTailStore) Scan(ctx context.Context, namespace string, f *filter.Filter) ([]*tail.Document, error) {
	return nil, nil
}

func (e *emptyTailStore) ScanWithByteLimit(ctx context.Context, namespace string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return nil, nil
}

func (e *emptyTailStore) VectorScan(ctx context.Context, namespace string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (e *emptyTailStore) VectorScanWithByteLimit(ctx context.Context, namespace string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (e *emptyTailStore) GetDocument(ctx context.Context, namespace string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (e *emptyTailStore) TailBytes(namespace string) int64 {
	return 0
}

func (e *emptyTailStore) Clear(namespace string) {}

func (e *emptyTailStore) AddWALEntry(namespace string, entry *wal.WalEntry) {}

func (e *emptyTailStore) Close() error {
	return nil
}

func TestIndexerIncludesNonVectorDocs(t *testing.T) {
	store := newMockStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	docs := []vectorTestDoc{
		{id: 1, attrs: map[string]any{"score": float64(10), "name": "alpha"}},
		{id: 2, attrs: map[string]any{"score": float64(20), "name": "beta"}},
	}
	_, data := createVectorWALEntry("test-ns", 1, docs)
	store.mu.Lock()
	store.objects["vex/namespaces/test-ns/wal/00000000000000000001.wal.zst"] = mockObject{data: data, etag: "etag1"}
	store.mu.Unlock()

	loaded, err := stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to load namespace: %v", err)
	}
	loaded, err = stateMan.AdvanceWAL(ctx, "test-ns", loaded.ETag, "vex/namespaces/test-ns/wal/00000000000000000001.wal.zst", int64(len(data)), nil)
	if err != nil {
		t.Fatalf("failed to advance WAL: %v", err)
	}

	idxer := New(store, stateMan, DefaultConfig(), nil)
	processor := NewL0SegmentProcessor(store, stateMan, nil, idxer)

	result, err := processor.ProcessWAL(ctx, "test-ns", 0, 1, loaded.State, loaded.ETag)
	if err != nil {
		t.Fatalf("ProcessWAL failed: %v", err)
	}
	if result == nil || !result.ManifestWritten {
		t.Fatalf("expected manifest to be written")
	}

	reader := index.NewReader(store, nil, nil)
	indexedDocs, err := reader.LoadSegmentDocs(ctx, result.ManifestKey)
	if err != nil {
		t.Fatalf("LoadSegmentDocs failed: %v", err)
	}
	if len(indexedDocs) != 2 {
		t.Fatalf("expected 2 indexed docs, got %d", len(indexedDocs))
	}

	docAttrs := make(map[string]map[string]any)
	for _, doc := range indexedDocs {
		docAttrs[doc.ID] = doc.Attributes
	}
	if attrs, ok := docAttrs["1"]; !ok || attrs["name"] != "alpha" {
		t.Errorf("expected doc 1 attributes to include name=alpha, got %v", attrs)
	}
	if attrs, ok := docAttrs["2"]; !ok || attrs["name"] != "beta" {
		t.Errorf("expected doc 2 attributes to include name=beta, got %v", attrs)
	}

	handler := query.NewHandler(store, stateMan, &emptyTailStore{})
	resp, err := handler.Handle(ctx, "test-ns", &query.QueryRequest{
		RankBy: []any{"score", "desc"},
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("query Handle failed: %v", err)
	}
	if len(resp.Rows) != 2 {
		t.Fatalf("expected 2 query rows, got %d", len(resp.Rows))
	}
	if resp.Rows[0].ID != uint64(2) || resp.Rows[1].ID != uint64(1) {
		t.Errorf("unexpected row order: %+v", resp.Rows)
	}
}
