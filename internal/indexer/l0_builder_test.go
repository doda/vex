package indexer

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestL0PublishSegment_ManifestConflict(t *testing.T) {
	store := newMockStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()
	ns := "test-ns"

	loaded, err := stateMan.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	segID := "seg_1"
	docsKey := index.DocsObjectKey(ns, segID)
	_, _ = store.Put(ctx, docsKey, bytes.NewReader([]byte("docs")), int64(len("docs")), nil)

	segment := &index.Segment{
		ID:          segID,
		Level:       index.L0,
		StartWALSeq: 1,
		EndWALSeq:   1,
		DocsKey:     docsKey,
		Stats:       index.SegmentStats{RowCount: 1, LogicalBytes: 4},
	}

	existing := index.NewManifest(ns)
	existingData, err := existing.MarshalJSON()
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	conflictKey := index.ManifestKey(ns, loaded.State.Index.ManifestSeq+1)
	_, _ = store.Put(ctx, conflictKey, bytes.NewReader(existingData), int64(len(existingData)), nil)

	processor := NewL0SegmentProcessor(store, stateMan, nil, nil)
	_, err = processor.publishSegment(ctx, ns, segment, loaded.State, loaded.ETag)
	if err == nil {
		t.Fatal("expected manifest conflict error")
	}
	if !objectstore.IsConflictError(err) {
		t.Fatalf("expected conflict error, got: %v", err)
	}

	updated, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to reload namespace: %v", err)
	}
	if updated.State.Index.ManifestSeq != 0 {
		t.Errorf("expected manifest seq to remain 0, got %d", updated.State.Index.ManifestSeq)
	}
}

func TestL0PublishSegment_VerifiesObjects(t *testing.T) {
	store := newMockStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()
	ns := "test-ns"

	loaded, err := stateMan.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	segment := &index.Segment{
		ID:          "seg_missing",
		Level:       index.L0,
		StartWALSeq: 1,
		EndWALSeq:   1,
		DocsKey:     index.DocsObjectKey(ns, "seg_missing"),
		Stats:       index.SegmentStats{RowCount: 1, LogicalBytes: 4},
	}

	processor := NewL0SegmentProcessor(store, stateMan, nil, nil)
	_, err = processor.publishSegment(ctx, ns, segment, loaded.State, loaded.ETag)
	if err == nil {
		t.Fatal("expected missing object error")
	}
	if !errors.Is(err, index.ErrObjectMissing) {
		t.Fatalf("expected ErrObjectMissing, got: %v", err)
	}

	manifestKey := index.ManifestKey(ns, loaded.State.Index.ManifestSeq+1)
	_, err = store.Head(ctx, manifestKey)
	if !objectstore.IsNotFoundError(err) {
		t.Fatalf("expected manifest to be absent, got: %v", err)
	}
}
