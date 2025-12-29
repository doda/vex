package write

import (
	"context"
	"errors"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestConditionalPatch_DocMissing_Skip(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	// Patch with condition on non-existent document should be SKIPPED
	// (different from upsert which applies unconditionally for missing docs)
	req := &WriteRequest{
		RequestID: "test-cond-patch-1",
		PatchRows: []map[string]any{
			{"id": 1, "name": "patch-test", "status": "updated"},
		},
		PatchCondition: []any{"version", "Lt", 100},
	}

	resp, err := handler.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be 0 because patch on missing doc is skipped
	if resp.RowsPatched != 0 {
		t.Errorf("expected 0 rows patched for missing doc, got %d", resp.RowsPatched)
	}
}

func TestConditionalPatch_DocExists_ConditionMet(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document and add to tail
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 1},
	}, 1)

	// Now patch with a condition that should be met (version < 10)
	req2 := &WriteRequest{
		RequestID: "test-cond-patch-2b",
		PatchRows: []map[string]any{
			{"id": 1, "name": "updated", "status": "active"},
		},
		PatchCondition: []any{"version", "Lt", 10}, // Existing version (1) < 10 => true
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched when condition met, got %d", resp.RowsPatched)
	}
}

func TestConditionalPatch_DocExists_ConditionNotMet(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document with version 10
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 10},
	}, 1)

	// Now patch with a condition that should NOT be met (version < 10, but version = 10)
	req2 := &WriteRequest{
		RequestID: "test-cond-patch-3b",
		PatchRows: []map[string]any{
			{"id": 1, "name": "updated", "status": "active"},
		},
		PatchCondition: []any{"version", "Lt", 10}, // Existing version (10) < 10 => false
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsPatched != 0 {
		t.Errorf("expected 0 rows patched when condition not met, got %d", resp.RowsPatched)
	}
}

func TestConditionalPatch_RefNew_VersionCheck(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document with version 5
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 5},
	}, 1)

	// Now patch with a condition using $ref_new: existing version < new version
	// The condition ["version", "Lt", "$ref_new.version"] checks if
	// existing version < new version. With existing=5 and new=10, this is true.
	req2 := &WriteRequest{
		RequestID: "test-cond-patch-4b",
		PatchRows: []map[string]any{
			{"id": 1, "version": 10, "status": "updated"},
		},
		PatchCondition: []any{"version", "Lt", "$ref_new.version"},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched when $ref_new condition met, got %d", resp.RowsPatched)
	}
}

func TestConditionalPatch_RefNew_ConditionNotMet(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document with version 10
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 10},
	}, 1)

	// Now try to patch with new version 5, but condition is existing < new
	// existing_version (10) < new_version (5) => false, should skip
	req2 := &WriteRequest{
		RequestID: "test-cond-patch-5b",
		PatchRows: []map[string]any{
			{"id": 1, "version": 5, "status": "downgraded"},
		},
		PatchCondition: []any{"version", "Lt", "$ref_new.version"},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsPatched != 0 {
		t.Errorf("expected 0 rows patched when $ref_new condition not met, got %d", resp.RowsPatched)
	}
}

func TestConditionalPatch_MultipleRows_MixedConditions(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// Create two documents with different versions
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "doc1", "version": 5},
		{"id": 2, "name": "doc2", "version": 15},
	}, 1)

	// Now patch three docs with condition version < 10
	// Doc 1 (version 5 < 10) should be patched
	// Doc 2 (version 15 < 10) should NOT be patched
	// Doc 3 (missing) should be SKIPPED (unlike upsert which would apply)
	req2 := &WriteRequest{
		RequestID: "test-cond-patch-6b",
		PatchRows: []map[string]any{
			{"id": 1, "status": "doc1-patched"},
			{"id": 2, "status": "doc2-patched"},
			{"id": 3, "status": "doc3-patched"},
		},
		PatchCondition: []any{"version", "Lt", 10},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Doc 1: condition met (5 < 10), patched
	// Doc 2: condition not met (15 < 10 is false), skipped
	// Doc 3: missing, skipped (unlike upsert which would apply unconditionally)
	// Total patched: 1
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}
}

func TestConditionalPatch_AtomicWithWriting(t *testing.T) {
	ctx := context.Background()
	handler, store, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// Create a document and add to tail
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 1},
	}, 1)

	// Verify WAL entry exists
	walKey1 := "vex/namespaces/" + ns + "/wal/00000000000000000001.wal.zst"
	_, _, err := store.Get(ctx, walKey1, nil)
	if err != nil {
		t.Fatalf("WAL entry 1 not found: %v", err)
	}

	// Conditional patch that should succeed
	req2 := &WriteRequest{
		RequestID: "test-cond-patch-7b",
		PatchRows: []map[string]any{
			{"id": 1, "status": "patched"},
		},
		PatchCondition: []any{"version", "Eq", 1},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the patch was applied atomically
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}

	// Verify WAL entry 2 exists (write was committed)
	walKey2 := "vex/namespaces/" + ns + "/wal/00000000000000000002.wal.zst"
	_, _, err = store.Get(ctx, walKey2, nil)
	if err != nil {
		t.Fatalf("WAL entry 2 not found: %v", err)
	}
}

func TestConditionalPatch_ComplexCondition_And(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// Create a document
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 5, "status": "active"},
	}, 1)

	// Complex condition: version < 10 AND status = "active"
	req2 := &WriteRequest{
		RequestID: "test-cond-patch-8b",
		PatchRows: []map[string]any{
			{"id": 1, "description": "updated via conditional patch"},
		},
		PatchCondition: []any{"And", []any{
			[]any{"version", "Lt", 10},
			[]any{"status", "Eq", "active"},
		}},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched with And condition, got %d", resp.RowsPatched)
	}
}

func TestConditionalPatch_NullCondition(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// Create a document without "deleted" attribute
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 1},
	}, 1)

	// Condition: deleted = null (i.e., attribute missing or null)
	req2 := &WriteRequest{
		RequestID: "test-cond-patch-9b",
		PatchRows: []map[string]any{
			{"id": 1, "status": "patched when not deleted"},
		},
		PatchCondition: []any{"deleted", "Eq", nil},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched when checking null attribute, got %d", resp.RowsPatched)
	}
}

func TestConditionalPatch_NotEq_Condition(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// Create a document with status "pending"
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "status": "pending"},
	}, 1)

	// Condition: status != "completed"
	req2 := &WriteRequest{
		RequestID: "test-cond-patch-10",
		PatchRows: []map[string]any{
			{"id": 1, "processed": true},
		},
		PatchCondition: []any{"status", "NotEq", "completed"},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched with NotEq condition, got %d", resp.RowsPatched)
	}
}

func TestConditionalPatch_DeletedDoc_Skip(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create a document
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 1},
	}, 1)

	// Now delete the document
	delEntry := wal.NewWalEntry(ns, 2)
	delBatch := wal.NewWriteSubBatch("delete-doc")
	id, _ := document.ParseID(1)
	protoID := wal.DocumentIDFromID(id)
	delBatch.AddDelete(protoID)
	delEntry.SubBatches = append(delEntry.SubBatches, delBatch)
	tailStore.AddWALEntry(ns, delEntry)

	// Try to patch the deleted document
	req := &WriteRequest{
		RequestID: "test-cond-patch-deleted",
		PatchRows: []map[string]any{
			{"id": 1, "status": "attempt-patch-deleted"},
		},
		PatchCondition: []any{"version", "Eq", 1},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be 0 because the document is deleted
	if resp.RowsPatched != 0 {
		t.Errorf("expected 0 rows patched for deleted doc, got %d", resp.RowsPatched)
	}
}

func TestConditionalPatch_InvalidCondition_Error(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// Create a document
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 1},
	}, 1)

	// Invalid condition (unknown operator)
	req := &WriteRequest{
		RequestID: "test-cond-patch-invalid",
		PatchRows: []map[string]any{
			{"id": 1, "status": "attempt"},
		},
		PatchCondition: []any{"version", "InvalidOp", 1},
	}

	_, err := handler.Handle(ctx, ns, req)
	if err == nil {
		t.Error("expected error for invalid condition, got nil")
	}
}

func TestConditionalPatch_WithoutTailStore_Error(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	req := &WriteRequest{
		RequestID: "cond-patch",
		PatchRows: []map[string]any{
			{"id": 1, "status": "updated"},
		},
		PatchCondition: []any{"status", "Eq", "active"},
	}

	_, err = handler.Handle(context.Background(), "test-ns", req)
	if err == nil {
		t.Fatal("expected error without tail store, got nil")
	}
	if !errors.Is(err, ErrConditionalRequiresTail) {
		t.Fatalf("expected ErrConditionalRequiresTail, got %v", err)
	}
}
