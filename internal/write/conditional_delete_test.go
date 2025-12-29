package write

import (
	"context"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestConditionalDelete_DocMissing_Skip(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	// Delete with condition on non-existent document should be SKIPPED
	req := &WriteRequest{
		RequestID:       "test-cond-delete-1",
		Deletes:         []any{1},
		DeleteCondition: []any{"version", "Lt", 100},
	}

	resp, err := handler.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be 0 because delete on missing doc is skipped
	if resp.RowsDeleted != 0 {
		t.Errorf("expected 0 rows deleted for missing doc, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_DocExists_ConditionMet(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document and add to tail
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 1},
	}, 1)

	// Now delete with a condition that should be met (version < 10)
	req2 := &WriteRequest{
		RequestID:       "test-cond-delete-2b",
		Deletes:         []any{1},
		DeleteCondition: []any{"version", "Lt", 10}, // Existing version (1) < 10 => true
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted when condition met, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_DocExists_ConditionNotMet(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document with version 10
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 10},
	}, 1)

	// Now delete with a condition that should NOT be met (version < 10, but version = 10)
	req2 := &WriteRequest{
		RequestID:       "test-cond-delete-3b",
		Deletes:         []any{1},
		DeleteCondition: []any{"version", "Lt", 10}, // Existing version (10) < 10 => false
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsDeleted != 0 {
		t.Errorf("expected 0 rows deleted when condition not met, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_RefNew_AllAttributesNull(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document with version 5
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 5},
	}, 1)

	// Delete with condition using $ref_new: $ref_new.version should be null for deletes
	// The condition checks if the existing version equals $ref_new.version (which is null)
	// Since version (5) != null, this should NOT delete
	req2 := &WriteRequest{
		RequestID:       "test-cond-delete-4b",
		Deletes:         []any{1},
		DeleteCondition: []any{"version", "Eq", "$ref_new.version"},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// version (5) != null, so condition is not met
	if resp.RowsDeleted != 0 {
		t.Errorf("expected 0 rows deleted when $ref_new.version is null and doesn't match, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_RefNew_NullAttribute_MatchesMissing(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// Create a document without "deletable" attribute (it will be null/missing)
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 5},
	}, 1)

	// Delete with condition: deletable = $ref_new.version
	// Both are null/missing, so they match
	req2 := &WriteRequest{
		RequestID:       "test-cond-delete-5b",
		Deletes:         []any{1},
		DeleteCondition: []any{"deletable", "Eq", "$ref_new.version"}, // both null
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// deletable (missing/null) = $ref_new.version (null) => true
	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted when both attributes are null, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_MultipleRows_MixedConditions(t *testing.T) {
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

	// Now delete three docs with condition version < 10
	// Doc 1 (version 5 < 10) should be deleted
	// Doc 2 (version 15 < 10) should NOT be deleted
	// Doc 3 (missing) should be SKIPPED
	req2 := &WriteRequest{
		RequestID:       "test-cond-delete-6b",
		Deletes:         []any{1, 2, 3},
		DeleteCondition: []any{"version", "Lt", 10},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Doc 1: condition met (5 < 10), deleted
	// Doc 2: condition not met (15 < 10 is false), skipped
	// Doc 3: missing, skipped
	// Total deleted: 1
	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_AtomicWithWriting(t *testing.T) {
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

	// Conditional delete that should succeed
	req2 := &WriteRequest{
		RequestID:       "test-cond-delete-7b",
		Deletes:         []any{1},
		DeleteCondition: []any{"version", "Eq", 1},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the delete was applied atomically
	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted, got %d", resp.RowsDeleted)
	}

	// Verify WAL entry 2 exists (write was committed)
	walKey2 := "vex/namespaces/" + ns + "/wal/00000000000000000002.wal.zst"
	_, _, err = store.Get(ctx, walKey2, nil)
	if err != nil {
		t.Fatalf("WAL entry 2 not found: %v", err)
	}
}

func TestConditionalDelete_ComplexCondition_And(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// Create a document
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 5, "status": "expired"},
	}, 1)

	// Complex condition: version < 10 AND status = "expired"
	req2 := &WriteRequest{
		RequestID: "test-cond-delete-8b",
		Deletes:   []any{1},
		DeleteCondition: []any{"And", []any{
			[]any{"version", "Lt", 10},
			[]any{"status", "Eq", "expired"},
		}},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted with And condition, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_NullCondition(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// Create a document without "lock_holder" attribute
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 1},
	}, 1)

	// Condition: lock_holder = null (i.e., attribute missing or null)
	// Only delete if no one holds the lock
	req2 := &WriteRequest{
		RequestID:       "test-cond-delete-9b",
		Deletes:         []any{1},
		DeleteCondition: []any{"lock_holder", "Eq", nil},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted when checking null attribute, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_NotEq_Condition(t *testing.T) {
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
	// Only delete if not completed
	req2 := &WriteRequest{
		RequestID:       "test-cond-delete-10",
		Deletes:         []any{1},
		DeleteCondition: []any{"status", "NotEq", "completed"},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted with NotEq condition, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_AlreadyDeletedDoc_Skip(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create a document
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 1},
	}, 1)

	// Now delete the document (add tombstone to tail)
	delEntry := wal.NewWalEntry(ns, 2)
	delBatch := wal.NewWriteSubBatch("delete-doc")
	id, _ := document.ParseID(1)
	protoID := wal.DocumentIDFromID(id)
	delBatch.AddDelete(protoID)
	delEntry.SubBatches = append(delEntry.SubBatches, delBatch)
	tailStore.AddWALEntry(ns, delEntry)

	// Try to conditionally delete the already deleted document
	req := &WriteRequest{
		RequestID:       "test-cond-delete-already-deleted",
		Deletes:         []any{1},
		DeleteCondition: []any{"version", "Eq", 1},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be 0 because the document is already deleted
	if resp.RowsDeleted != 0 {
		t.Errorf("expected 0 rows deleted for already deleted doc, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_InvalidCondition_Error(t *testing.T) {
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
		RequestID:       "test-cond-delete-invalid",
		Deletes:         []any{1},
		DeleteCondition: []any{"version", "InvalidOp", 1},
	}

	_, err := handler.Handle(ctx, ns, req)
	if err == nil {
		t.Error("expected error for invalid condition, got nil")
	}
}

func TestConditionalDelete_OrCondition(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// Create a document with expired status
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "test", "status": "expired", "version": 100},
	}, 1)

	// Condition: status = "expired" OR version > 50
	// Either condition should trigger the delete
	req := &WriteRequest{
		RequestID: "test-cond-delete-or",
		Deletes:   []any{1},
		DeleteCondition: []any{"Or", []any{
			[]any{"status", "Eq", "expired"},
			[]any{"version", "Gt", 50},
		}},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted with Or condition, got %d", resp.RowsDeleted)
	}
}

func TestConditionalDelete_ParseWriteRequest(t *testing.T) {
	// Test that delete_condition is parsed correctly from request body
	body := map[string]any{
		"deletes":          []any{1, 2, 3},
		"delete_condition": []any{"status", "Eq", "archived"},
	}

	req, err := ParseWriteRequest("test-request-id", body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.DeleteCondition == nil {
		t.Error("expected DeleteCondition to be set")
	}

	condArr, ok := req.DeleteCondition.([]any)
	if !ok {
		t.Fatalf("expected DeleteCondition to be []any, got %T", req.DeleteCondition)
	}

	if len(condArr) != 3 {
		t.Errorf("expected condition with 3 elements, got %d", len(condArr))
	}

	if condArr[0] != "status" {
		t.Errorf("expected first element to be 'status', got %v", condArr[0])
	}

	if condArr[1] != "Eq" {
		t.Errorf("expected second element to be 'Eq', got %v", condArr[1])
	}

	if condArr[2] != "archived" {
		t.Errorf("expected third element to be 'archived', got %v", condArr[2])
	}
}

func TestConditionalDelete_WithoutTailStore_NoOp(t *testing.T) {
	// Test that conditional deletes are skipped when no tail store is available
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create handler WITHOUT tail store
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ctx := context.Background()

	// First insert a document normally (without condition)
	insertReq := &WriteRequest{
		RequestID: "insert-doc",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "test", "version": 1},
		},
	}

	_, err = handler.Handle(ctx, "test-ns", insertReq)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Now try conditional delete - should be skipped (no tail store to evaluate)
	deleteReq := &WriteRequest{
		RequestID:       "cond-delete",
		Deletes:         []any{1},
		DeleteCondition: []any{"version", "Eq", 1},
	}

	resp, err := handler.Handle(ctx, "test-ns", deleteReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be 0 because conditional deletes require tail store
	if resp.RowsDeleted != 0 {
		t.Errorf("expected 0 rows deleted without tail store, got %d", resp.RowsDeleted)
	}
}
