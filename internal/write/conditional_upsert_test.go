package write

import (
	"context"
	"testing"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// setupConditionalHandler creates a handler with a tail store for conditional tests.
func setupConditionalHandler(t *testing.T) (*Handler, objectstore.Store, *tail.TailStore) {
	t.Helper()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create memory cache and tail store
	memCache := cache.NewMemoryCache(cache.MemoryCacheConfig{MaxBytes: 64 * 1024 * 1024})
	tailStore := tail.New(tail.DefaultConfig(), store, nil, memCache)

	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	return handler, store, tailStore
}

// insertDocsWithTail inserts test documents and updates the tail store
func insertDocsWithTail(t *testing.T, ctx context.Context, handler *Handler, tailStore *tail.TailStore, ns string, docs []map[string]any, seq uint64) {
	t.Helper()

	req := &WriteRequest{
		RequestID:  "setup-docs",
		UpsertRows: docs,
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("failed to insert test documents: %v", err)
	}

	if resp.RowsUpserted != int64(len(docs)) {
		t.Fatalf("expected %d rows upserted, got %d", len(docs), resp.RowsUpserted)
	}

	// Add the WAL entry to the tail store so conditional upserts can find the documents
	walEntry := wal.NewWalEntry(ns, seq)
	subBatch := wal.NewWriteSubBatch("setup-docs")
	for _, doc := range docs {
		id, err := document.ParseID(doc["id"])
		if err != nil {
			t.Fatalf("failed to parse ID: %v", err)
		}
		protoID := wal.DocumentIDFromID(id)

		attrs := make(map[string]*wal.AttributeValue)
		for k, v := range doc {
			if k == "id" || k == "vector" {
				continue
			}
			switch val := v.(type) {
			case string:
				attrs[k] = wal.StringValue(val)
			case int:
				attrs[k] = wal.IntValue(int64(val))
			case int64:
				attrs[k] = wal.IntValue(val)
			case float64:
				attrs[k] = wal.FloatValue(val)
			case bool:
				attrs[k] = wal.BoolValue(val)
			}
		}
		subBatch.AddUpsert(protoID, attrs, nil, 0)
	}
	walEntry.SubBatches = append(walEntry.SubBatches, subBatch)
	tailStore.AddWALEntry(ns, walEntry)
}

func TestConditionalUpsert_DocMissing_ApplyUnconditionally(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	// Upsert with condition on non-existent document should apply unconditionally
	req := &WriteRequest{
		RequestID: "test-cond-1",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "test1", "version": 1},
		},
		UpsertCondition: []any{"version", "Lt", 100}, // Condition on existing doc
	}

	resp, err := handler.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted for missing doc, got %d", resp.RowsUpserted)
	}
}

func TestConditionalUpsert_DocExists_ConditionMet(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document and add to tail
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 1},
	}, 1)

	// Now update with a condition that should be met (version < 10)
	req2 := &WriteRequest{
		RequestID: "test-cond-2b",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "updated", "version": 2},
		},
		UpsertCondition: []any{"version", "Lt", 10}, // Existing version (1) < 10 => true
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted when condition met, got %d", resp.RowsUpserted)
	}
}

func TestConditionalUpsert_DocExists_ConditionNotMet(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document with version 10
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 10},
	}, 1)

	// Now update with a condition that should NOT be met (version < 10, but version = 10)
	req2 := &WriteRequest{
		RequestID: "test-cond-3b",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "updated", "version": 11},
		},
		UpsertCondition: []any{"version", "Lt", 10}, // Existing version (10) < 10 => false
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 0 {
		t.Errorf("expected 0 rows upserted when condition not met, got %d", resp.RowsUpserted)
	}
}

func TestConditionalUpsert_RefNew_VersionCheck(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document with version 5
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 5},
	}, 1)

	// Now update with a condition using $ref_new: existing version < new version
	// The condition ["version", "Lt", "$ref_new.version"] checks if
	// existing version < new version. With existing=5 and new=10, this is true.
	req2 := &WriteRequest{
		RequestID: "test-cond-4b",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "updated", "version": 10},
		},
		UpsertCondition: []any{"version", "Lt", "$ref_new.version"},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted when $ref_new condition met, got %d", resp.RowsUpserted)
	}
}

func TestConditionalUpsert_RefNew_ConditionNotMet(t *testing.T) {
	ctx := context.Background()
	handler, _, tailStore := setupConditionalHandler(t)
	defer handler.Close()
	defer tailStore.Close()

	ns := "test-ns"

	// First, create the document with version 10
	insertDocsWithTail(t, ctx, handler, tailStore, ns, []map[string]any{
		{"id": 1, "name": "original", "version": 10},
	}, 1)

	// Now try to update with new version 5, but condition is existing < new
	// existing_version (10) < new_version (5) => false, should skip
	req2 := &WriteRequest{
		RequestID: "test-cond-5b",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "updated", "version": 5},
		},
		UpsertCondition: []any{"version", "Lt", "$ref_new.version"},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 0 {
		t.Errorf("expected 0 rows upserted when $ref_new condition not met, got %d", resp.RowsUpserted)
	}
}

func TestConditionalUpsert_MultipleRows_MixedConditions(t *testing.T) {
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

	// Now update both with condition version < 10
	// Doc 1 (version 5 < 10) should be updated
	// Doc 2 (version 15 < 10) should NOT be updated
	// Doc 3 (missing) should be created unconditionally
	req2 := &WriteRequest{
		RequestID: "test-cond-6b",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "doc1-updated", "version": 6},
			{"id": 2, "name": "doc2-updated", "version": 16},
			{"id": 3, "name": "doc3-new", "version": 1},
		},
		UpsertCondition: []any{"version", "Lt", 10},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Doc 1: condition met (5 < 10), upserted
	// Doc 2: condition not met (15 < 10 is false), skipped
	// Doc 3: missing, applied unconditionally
	// Total upserted: 2
	if resp.RowsUpserted != 2 {
		t.Errorf("expected 2 rows upserted, got %d", resp.RowsUpserted)
	}
}

func TestConditionalUpsert_AtomicWithWriting(t *testing.T) {
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

	// Conditional upsert that should succeed
	req2 := &WriteRequest{
		RequestID: "test-cond-7b",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "updated", "version": 2},
		},
		UpsertCondition: []any{"version", "Eq", 1},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the upsert was applied atomically
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}

	// Verify WAL entry 2 exists (write was committed)
	walKey2 := "vex/namespaces/" + ns + "/wal/00000000000000000002.wal.zst"
	_, _, err = store.Get(ctx, walKey2, nil)
	if err != nil {
		t.Fatalf("WAL entry 2 not found: %v", err)
	}
}

func TestConditionalUpsert_ComplexCondition_And(t *testing.T) {
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
		RequestID: "test-cond-8b",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "updated", "version": 6, "status": "active"},
		},
		UpsertCondition: []any{"And", []any{
			[]any{"version", "Lt", 10},
			[]any{"status", "Eq", "active"},
		}},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted with And condition, got %d", resp.RowsUpserted)
	}
}

func TestConditionalUpsert_NullCondition(t *testing.T) {
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
		RequestID: "test-cond-9b",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "updated", "version": 2},
		},
		UpsertCondition: []any{"deleted", "Eq", nil},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted when checking null attribute, got %d", resp.RowsUpserted)
	}
}

func TestConditionalUpsert_NotEq_Condition(t *testing.T) {
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
		RequestID: "test-cond-10",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "updated", "status": "active"},
		},
		UpsertCondition: []any{"status", "NotEq", "completed"},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted with NotEq condition, got %d", resp.RowsUpserted)
	}
}
