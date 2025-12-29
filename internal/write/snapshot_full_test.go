package write

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func setupIndexedNamespace(t *testing.T, ctx context.Context, store objectstore.Store, stateMan *namespace.StateManager, ns string, docs []index.IndexedDocument, indexedSeq uint64) {
	t.Helper()

	for i := range docs {
		if docs[i].WALSeq == 0 {
			docs[i].WALSeq = indexedSeq
		}
	}

	docsKey := fmt.Sprintf("vex/namespaces/%s/index/segments/seg_1/docs.json", ns)
	docsData, err := json.Marshal(docs)
	if err != nil {
		t.Fatalf("failed to marshal docs: %v", err)
	}
	if _, err := store.Put(ctx, docsKey, bytes.NewReader(docsData), int64(len(docsData)), nil); err != nil {
		t.Fatalf("failed to store docs: %v", err)
	}

	manifestKey := fmt.Sprintf("vex/namespaces/%s/index/manifests/00000000000000000001.idx.json", ns)
	manifest := &index.Manifest{
		FormatVersion: index.CurrentManifestVersion,
		Namespace:     ns,
		IndexedWALSeq: indexedSeq,
		Segments: []index.Segment{
			{
				ID:          "seg_1",
				Level:       0,
				StartWALSeq:  indexedSeq,
				EndWALSeq:    indexedSeq,
				DocsKey:     docsKey,
				Stats: index.SegmentStats{
					RowCount: int64(len(docs)),
				},
			},
		},
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	if _, err := store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil); err != nil {
		t.Fatalf("failed to store manifest: %v", err)
	}

	loaded, err := stateMan.LoadOrCreate(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if _, err := stateMan.Update(ctx, ns, loaded.ETag, func(state *namespace.State) error {
		state.WAL.HeadSeq = indexedSeq
		state.WAL.HeadKey = fmt.Sprintf("vex/namespaces/%s/wal/%020d.wal.zst", ns, indexedSeq)
		state.Index.IndexedWALSeq = indexedSeq
		state.Index.ManifestKey = manifestKey
		state.Index.ManifestSeq = 1
		return nil
	}); err != nil {
		t.Fatalf("failed to update state: %v", err)
	}
}

func TestDeleteByFilter_IndexedSnapshot(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "indexed-delete-by-filter"
	setupIndexedNamespace(t, ctx, store, stateMan, ns, []index.IndexedDocument{
		{NumericID: 1, WALSeq: 1, Attributes: map[string]any{"status": "active"}},
		{NumericID: 2, WALSeq: 1, Attributes: map[string]any{"status": "inactive"}},
		{NumericID: 3, WALSeq: 1, Attributes: map[string]any{"status": "active"}},
	}, 1)

	req := &WriteRequest{
		RequestID: "delete-indexed",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"status", "Eq", "active"},
		},
	}
	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("delete_by_filter failed: %v", err)
	}
	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted, got %d", resp.RowsDeleted)
	}
}

func TestPatchByFilter_IndexedSnapshot(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "indexed-patch-by-filter"
	setupIndexedNamespace(t, ctx, store, stateMan, ns, []index.IndexedDocument{
		{NumericID: 10, WALSeq: 1, Attributes: map[string]any{"tier": "gold"}},
		{NumericID: 11, WALSeq: 1, Attributes: map[string]any{"tier": "silver"}},
	}, 1)

	req := &WriteRequest{
		RequestID: "patch-indexed",
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"tier", "Eq", "gold"},
			Updates: map[string]any{"tier": "platinum"},
		},
	}
	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("patch_by_filter failed: %v", err)
	}
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}
}

func TestConditionalUpsert_IndexedSnapshot(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "indexed-conditional-upsert"
	setupIndexedNamespace(t, ctx, store, stateMan, ns, []index.IndexedDocument{
		{NumericID: 1, WALSeq: 1, Attributes: map[string]any{"version": int64(1)}},
	}, 1)

	req := &WriteRequest{
		RequestID:       "upsert-indexed",
		UpsertCondition: []any{"version", "Eq", int64(2)},
		UpsertRows: []map[string]any{
			{"id": 1, "version": int64(2)},
		},
	}
	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("conditional upsert failed: %v", err)
	}
	if resp.RowsUpserted != 0 {
		t.Errorf("expected 0 rows upserted, got %d", resp.RowsUpserted)
	}
}

func TestConditionalPatch_IndexedSnapshot(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "indexed-conditional-patch"
	setupIndexedNamespace(t, ctx, store, stateMan, ns, []index.IndexedDocument{
		{NumericID: 5, WALSeq: 1, Attributes: map[string]any{"version": int64(1)}},
	}, 1)

	req := &WriteRequest{
		RequestID:      "patch-indexed",
		PatchCondition: []any{"version", "Eq", int64(1)},
		PatchRows: []map[string]any{
			{"id": 5, "note": "patched"},
		},
	}
	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("conditional patch failed: %v", err)
	}
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}
}

func TestConditionalDelete_IndexedSnapshot(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "indexed-conditional-delete"
	setupIndexedNamespace(t, ctx, store, stateMan, ns, []index.IndexedDocument{
		{NumericID: 9, WALSeq: 1, Attributes: map[string]any{"version": int64(1)}},
	}, 1)

	req := &WriteRequest{
		RequestID:       "delete-indexed",
		DeleteCondition: []any{"version", "Eq", int64(1)},
		Deletes:         []any{uint64(9)},
	}
	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("conditional delete failed: %v", err)
	}
	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted, got %d", resp.RowsDeleted)
	}
}

func TestCopyFromNamespace_IndexedSnapshot(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	sourceNs := "indexed-copy-source"
	destNs := "indexed-copy-dest"

	setupIndexedNamespace(t, ctx, store, stateMan, sourceNs, []index.IndexedDocument{
		{NumericID: 100, WALSeq: 1, Attributes: map[string]any{"name": "alpha"}},
		{NumericID: 200, WALSeq: 1, Attributes: map[string]any{"name": "beta"}},
	}, 1)

	req := &WriteRequest{
		RequestID:         "copy-indexed",
		CopyFromNamespace: sourceNs,
	}
	resp, err := handler.Handle(ctx, destNs, req)
	if err != nil {
		t.Fatalf("copy_from_namespace failed: %v", err)
	}
	if resp.RowsUpserted != 2 {
		t.Errorf("expected 2 rows upserted, got %d", resp.RowsUpserted)
	}
}
