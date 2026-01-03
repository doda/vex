package write

import (
	"context"
	"errors"
	"testing"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// --- Test: copy_from_namespace bulk upserts from source namespace ---

func TestHandler_CopyFromNamespace_BulkUpserts(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	sourceNs := "source-namespace"
	destNs := "dest-namespace"

	// Insert documents into source namespace
	sourceDocs := []map[string]any{
		{"id": 1, "name": "doc1", "value": 100},
		{"id": 2, "name": "doc2", "value": 200},
		{"id": 3, "name": "doc3", "value": 300},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, sourceNs, sourceDocs)

	// Now copy from source to destination
	copyReq := &WriteRequest{
		RequestID:         "copy-test",
		CopyFromNamespace: sourceNs,
	}

	resp, err := handler.Handle(ctx, destNs, copyReq)
	if err != nil {
		t.Fatalf("copy_from_namespace failed: %v", err)
	}

	// Should have upserted 3 documents
	if resp.RowsUpserted != 3 {
		t.Errorf("expected 3 rows upserted, got %d", resp.RowsUpserted)
	}
	if resp.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", resp.RowsAffected)
	}
}

// --- Test: copy_from_namespace runs AFTER patch_by_filter, BEFORE explicit upserts ---

func TestHandler_CopyFromNamespace_Ordering(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	sourceNs := "source-ns-ordering"
	destNs := "dest-ns-ordering"

	// Insert documents into source namespace
	sourceDocs := []map[string]any{
		{"id": 1, "category": "A", "status": "old"},
		{"id": 2, "category": "A", "status": "old"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, sourceNs, sourceDocs)

	// Insert a document in destination that will be patched by filter
	destDocs := []map[string]any{
		{"id": 10, "category": "B", "status": "initial"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, destNs, destDocs)

	// Combined request:
	// 1. patch_by_filter (category="B") - patches ID 10
	// 2. copy_from_namespace - copies IDs 1,2 from source
	// 3. upsert_rows - adds ID 20
	combinedReq := &WriteRequest{
		RequestID: "ordering-test",
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"category", "Eq", "B"},
			Updates: map[string]any{"status": "patched"},
		},
		CopyFromNamespace: sourceNs,
		UpsertRows: []map[string]any{
			{"id": 20, "category": "new", "status": "inserted"},
		},
	}

	resp, err := handler.Handle(ctx, destNs, combinedReq)
	if err != nil {
		t.Fatalf("combined operation failed: %v", err)
	}

	// patch_by_filter should have patched 1 doc
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}

	// copy should have upserted 2 docs + explicit upsert 1 doc
	if resp.RowsUpserted != 3 {
		t.Errorf("expected 3 rows upserted (2 from copy + 1 explicit), got %d", resp.RowsUpserted)
	}
}

// --- Test: copy_from_namespace can be combined with other write operations ---

func TestHandler_CopyFromNamespace_Combined(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	sourceNs := "source-combined"
	destNs := "dest-combined"

	// Insert documents into source namespace
	sourceDocs := []map[string]any{
		{"id": 1, "name": "source-doc1"},
		{"id": 2, "name": "source-doc2"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, sourceNs, sourceDocs)

	// Combined request with copy + explicit operations
	combinedReq := &WriteRequest{
		RequestID:         "combined-copy",
		CopyFromNamespace: sourceNs,
		UpsertRows: []map[string]any{
			{"id": 10, "name": "explicit-upsert"},
		},
		PatchRows: []map[string]any{
			{"id": 1, "name": "patched-name"}, // Patches the document copied from source
		},
		Deletes: []any{2}, // Deletes one of the copied documents
	}

	resp, err := handler.Handle(ctx, destNs, combinedReq)
	if err != nil {
		t.Fatalf("combined copy operation failed: %v", err)
	}

	// copy upserts 2 docs + explicit upsert 1 doc
	if resp.RowsUpserted != 3 {
		t.Errorf("expected 3 rows upserted, got %d", resp.RowsUpserted)
	}
	// patch 1 doc
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}
	// delete 1 doc
	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted, got %d", resp.RowsDeleted)
	}
}

// --- Test: source namespace read at consistent snapshot ---

func TestHandler_CopyFromNamespace_ConsistentSnapshot(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	sourceNs := "source-snapshot"
	destNs := "dest-snapshot"

	// Insert documents into source namespace
	sourceDocs := []map[string]any{
		{"id": 1, "version": 1},
		{"id": 2, "version": 1},
		{"id": 3, "version": 1},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, sourceNs, sourceDocs)

	// Copy from source to destination
	copyReq := &WriteRequest{
		RequestID:         "snapshot-test",
		CopyFromNamespace: sourceNs,
	}

	resp, err := handler.Handle(ctx, destNs, copyReq)
	if err != nil {
		t.Fatalf("copy_from_namespace failed: %v", err)
	}

	// Should have upserted 3 documents from the snapshot
	if resp.RowsUpserted != 3 {
		t.Errorf("expected 3 rows upserted from snapshot, got %d", resp.RowsUpserted)
	}
}

// --- Test: source namespace not found returns error ---

func TestHandler_CopyFromNamespace_SourceNotFound(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()
	_ = tailStore // Silence unused variable warning

	destNs := "dest-not-found"

	// Try to copy from a non-existent source namespace
	copyReq := &WriteRequest{
		RequestID:         "not-found-test",
		CopyFromNamespace: "non-existent-namespace",
	}

	_, err := handler.Handle(ctx, destNs, copyReq)
	if err == nil {
		t.Error("expected error when source namespace not found, got nil")
	}

	if !errors.Is(err, ErrSourceNamespaceNotFound) {
		t.Errorf("expected ErrSourceNamespaceNotFound, got: %v", err)
	}
}

// --- Test: copy_from_namespace with no tail store returns error ---

func TestHandler_CopyFromNamespace_NoTailStore(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create handler WITHOUT tail store
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	destNs := "dest-no-tail"

	// copy_from_namespace should fail when no tail store is configured
	copyReq := &WriteRequest{
		RequestID:         "no-tail-test",
		CopyFromNamespace: "some-source",
	}

	_, err = handler.Handle(ctx, destNs, copyReq)
	if err == nil {
		t.Error("expected error when no tail store configured, got nil")
	}
}

// --- Test: copy_from_namespace with empty source namespace ---

func TestHandler_CopyFromNamespace_EmptySource(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()
	_ = tailStore // Silence unused variable warning

	sourceNs := "empty-source"
	destNs := "dest-from-empty"

	// Create an empty source namespace
	emptyReq := &WriteRequest{
		RequestID:  "create-empty",
		UpsertRows: []map[string]any{},
	}
	_, err := handler.Handle(ctx, sourceNs, emptyReq)
	if err != nil {
		t.Fatalf("failed to create empty source namespace: %v", err)
	}

	// Copy from empty source
	copyReq := &WriteRequest{
		RequestID:         "copy-empty",
		CopyFromNamespace: sourceNs,
	}

	resp, err := handler.Handle(ctx, destNs, copyReq)
	if err != nil {
		t.Fatalf("copy_from_namespace from empty source failed: %v", err)
	}

	// Should upsert 0 documents
	if resp.RowsUpserted != 0 {
		t.Errorf("expected 0 rows upserted from empty source, got %d", resp.RowsUpserted)
	}
}

// --- Test: copy_from_namespace with vectors ---

func TestHandler_CopyFromNamespace_WithVectors(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	tailStore := tail.New(tail.DefaultConfig(), store, nil, cache.NewMemoryCache(cache.MemoryCacheConfig{
		MaxBytes: 64 * 1024 * 1024,
	}))

	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	sourceNs := "source-vectors"
	destNs := "dest-vectors"

	// Create a WAL entry with vectors manually
	walEntry := wal.NewWalEntry(sourceNs, 1)
	subBatch := wal.NewWriteSubBatch("setup-vectors")

	id, _ := document.ParseID(1)
	protoID := wal.DocumentIDFromID(id)
	attrs := map[string]*wal.AttributeValue{
		"name": wal.StringValue("doc-with-vector"),
	}
	// Add a 3-dimensional vector
	vectorBytes := vectorToBytes([]float32{1.0, 2.0, 3.0})
	subBatch.AddUpsert(protoID, attrs, vectorBytes, 3)

	walEntry.SubBatches = append(walEntry.SubBatches, subBatch)
	tailStore.AddWALEntry(sourceNs, walEntry)

	// Create the namespace state so it can be loaded
	sourceLoaded, err := stateMan.LoadOrCreate(ctx, sourceNs)
	if err != nil {
		t.Fatalf("failed to create source namespace: %v", err)
	}
	if _, err := stateMan.AdvanceWAL(ctx, sourceNs, sourceLoaded.ETag, wal.KeyForSeq(1), 0, nil); err != nil {
		t.Fatalf("failed to advance WAL for source namespace: %v", err)
	}

	// Copy from source with vectors
	copyReq := &WriteRequest{
		RequestID:         "copy-vectors",
		CopyFromNamespace: sourceNs,
	}

	resp, err := handler.Handle(ctx, destNs, copyReq)
	if err != nil {
		t.Fatalf("copy_from_namespace with vectors failed: %v", err)
	}

	// Should have upserted 1 document with vector
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
}

// --- Test: ParseWriteRequest correctly parses copy_from_namespace ---

func TestParseWriteRequest_CopyFromNamespace(t *testing.T) {
	tests := []struct {
		name            string
		body            map[string]any
		expectCopy      bool
		expectCopyValue string
		wantErr         bool
	}{
		{
			name: "basic copy_from_namespace",
			body: map[string]any{
				"copy_from_namespace": "source-ns",
			},
			expectCopy:      true,
			expectCopyValue: "source-ns",
			wantErr:         false,
		},
		{
			name: "copy_from_namespace combined with upserts",
			body: map[string]any{
				"copy_from_namespace": "source-ns",
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "new"},
				},
			},
			expectCopy:      true,
			expectCopyValue: "source-ns",
			wantErr:         false,
		},
		{
			name:       "no copy_from_namespace",
			body:       map[string]any{},
			expectCopy: false,
			wantErr:    false,
		},
		{
			name: "copy_from_namespace invalid type",
			body: map[string]any{
				"copy_from_namespace": 123, // Should be string
			},
			wantErr: true,
		},
		{
			name: "copy_from_namespace with all operations",
			body: map[string]any{
				"copy_from_namespace": "source-ns",
				"delete_by_filter":    []any{"status", "Eq", "old"},
				"patch_by_filter": map[string]any{
					"filter":  []any{"category", "Eq", "A"},
					"updates": map[string]any{"processed": true},
				},
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "new"},
				},
				"patch_rows": []any{
					map[string]any{"id": 2, "status": "patched"},
				},
				"deletes": []any{3},
			},
			expectCopy:      true,
			expectCopyValue: "source-ns",
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := ParseWriteRequest("test-req", tt.body)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.expectCopy {
				if req.CopyFromNamespace != tt.expectCopyValue {
					t.Errorf("expected CopyFromNamespace=%q, got %q", tt.expectCopyValue, req.CopyFromNamespace)
				}
			} else {
				if req.CopyFromNamespace != "" {
					t.Errorf("expected CopyFromNamespace to be empty, got %q", req.CopyFromNamespace)
				}
			}
		})
	}
}

// --- Test: copy_from_namespace combined with delete_by_filter and patch_by_filter ---

func TestHandler_CopyFromNamespace_FullOrdering(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	sourceNs := "source-full-ordering"
	destNs := "dest-full-ordering"

	// Insert documents into source namespace
	sourceDocs := []map[string]any{
		{"id": 1, "name": "source1"},
		{"id": 2, "name": "source2"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, sourceNs, sourceDocs)

	// Insert documents into destination namespace
	destDocs := []map[string]any{
		{"id": 100, "type": "delete-me"},
		{"id": 101, "type": "patch-me"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, destNs, destDocs)

	// Full ordering test:
	// 1. delete_by_filter (type="delete-me") - deletes ID 100
	// 2. patch_by_filter (type="patch-me") - patches ID 101
	// 3. copy_from_namespace - copies IDs 1,2 from source
	// 4. upsert_rows - adds ID 200
	// 5. patch_rows - patches ID 1 (copied from source)
	// 6. deletes - deletes ID 2 (copied from source)
	fullReq := &WriteRequest{
		RequestID: "full-ordering",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"type", "Eq", "delete-me"},
		},
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"type", "Eq", "patch-me"},
			Updates: map[string]any{"status": "filter-patched"},
		},
		CopyFromNamespace: sourceNs,
		UpsertRows: []map[string]any{
			{"id": 200, "name": "explicit-upsert"},
		},
		PatchRows: []map[string]any{
			{"id": 1, "name": "explicit-patch"},
		},
		Deletes: []any{2},
	}

	resp, err := handler.Handle(ctx, destNs, fullReq)
	if err != nil {
		t.Fatalf("full ordering operation failed: %v", err)
	}

	// delete_by_filter: 1 doc + explicit delete: 1 doc = 2 deletes
	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted, got %d", resp.RowsDeleted)
	}

	// patch_by_filter: 1 doc + explicit patch: 1 doc = 2 patches
	if resp.RowsPatched != 2 {
		t.Errorf("expected 2 rows patched, got %d", resp.RowsPatched)
	}

	// copy: 2 docs + explicit upsert: 1 doc = 3 upserts
	if resp.RowsUpserted != 3 {
		t.Errorf("expected 3 rows upserted, got %d", resp.RowsUpserted)
	}
}
