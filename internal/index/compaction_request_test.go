package index

import (
	"context"
	"testing"
	"time"

	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestApplyCompactionRequest_AppliesManifest(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	ns := "test-ns"

	t.Setenv("VEX_VERIFY_MANIFEST_REFERENCES", "0")

	seg1 := Segment{ID: "seg_1", Level: L0, StartWALSeq: 1, EndWALSeq: 1}
	seg2 := Segment{ID: "seg_2", Level: L0, StartWALSeq: 2, EndWALSeq: 2}
	manifest := NewManifest(ns)
	manifest.AddSegment(seg1)
	manifest.AddSegment(seg2)
	manifest.UpdateIndexedWALSeq()

	stateMan := seedStateWithManifest(t, ctx, store, ns, manifest)
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	newSeg := Segment{
		ID:          "seg_compacted",
		Level:       L1,
		StartWALSeq: 1,
		EndWALSeq:   2,
		CreatedAt:   time.Now().UTC(),
	}
	req := NewCompactionRequest(ns, loaded.State.Index.ManifestSeq, loaded.State.Index.ManifestKey, []Segment{seg1, seg2}, newSeg)
	if req.ID == "" {
		t.Fatal("expected request ID")
	}

	if _, _, err := WriteCompactionRequest(ctx, store, req); err != nil {
		t.Fatalf("failed to write compaction request: %v", err)
	}

	result, err := ApplyCompactionRequest(ctx, store, stateMan, req, CompactionApplyOptions{RetentionTime: 0, MaxAttempts: 3})
	if err != nil {
		t.Fatalf("ApplyCompactionRequest failed: %v", err)
	}
	if result == nil || !result.Applied {
		t.Fatalf("expected compaction to apply, got %+v", result)
	}

	loaded, err = stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to reload state: %v", err)
	}
	if loaded.State.Index.ManifestSeq != 2 {
		t.Fatalf("expected manifest seq 2, got %d", loaded.State.Index.ManifestSeq)
	}

	updated, err := LoadManifest(ctx, store, ns, loaded.State.Index.ManifestSeq)
	if err != nil {
		t.Fatalf("failed to load updated manifest: %v", err)
	}
	if updated.GetSegment("seg_1") != nil || updated.GetSegment("seg_2") != nil {
		t.Fatalf("expected source segments removed after apply")
	}
	if updated.GetSegment("seg_compacted") == nil {
		t.Fatalf("expected new segment to be present after apply")
	}

	if _, err := store.Head(ctx, CompactionRequestKey(ns, req.ID)); !objectstore.IsNotFoundError(err) {
		t.Fatalf("expected request to be deleted, got %v", err)
	}

}
