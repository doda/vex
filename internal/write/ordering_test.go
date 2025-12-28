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

// TestHandler_WriteOrderingCanonical verifies the canonical ordering of write operations:
// delete_by_filter -> patch_by_filter -> copy_from_namespace -> upserts -> patches -> deletes
func TestHandler_WriteOrderingCanonical(t *testing.T) {
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

	ns := "test-write-ordering"

	// Step 1: Set up initial documents
	initialDocs := []map[string]any{
		{"id": 1, "category": "toDelete", "status": "active", "value": 100},
		{"id": 2, "category": "toPatch", "status": "active", "value": 200},
		{"id": 3, "category": "unchanged", "status": "active", "value": 300},
	}
	setupReq := &WriteRequest{
		RequestID:  "setup",
		UpsertRows: initialDocs,
	}
	if _, err := handler.Handle(ctx, ns, setupReq); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Add documents to tail store for filter operations
	walEntry := wal.NewWalEntry(ns, 1)
	subBatch := wal.NewWriteSubBatch("setup")
	for _, doc := range initialDocs {
		id, _ := document.ParseID(doc["id"])
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
			}
		}
		subBatch.AddUpsert(protoID, attrs, nil, 0)
	}
	walEntry.SubBatches = append(walEntry.SubBatches, subBatch)
	tailStore.AddWALEntry(ns, walEntry)

	// Step 2: Execute a combined request with all operations
	// The ordering should be:
	// 1. delete_by_filter (deletes id=1 with category=toDelete)
	// 2. patch_by_filter (patches id=2 with category=toPatch)
	// 3. upserts (adds id=10)
	// 4. patches (patches id=3)
	// 5. deletes (deletes id=10)
	//
	// NOTE: The explicit delete of id=10 comes AFTER the upsert of id=10,
	// demonstrating that deletes run after upserts in the ordering.
	combinedReq := &WriteRequest{
		RequestID: "combined-ordering",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"category", "Eq", "toDelete"},
		},
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"category", "Eq", "toPatch"},
			Updates: map[string]any{"patchedBy": "filter"},
		},
		UpsertRows: []map[string]any{
			{"id": 10, "category": "new", "value": 1000},
		},
		PatchRows: []map[string]any{
			{"id": 3, "patchedBy": "explicit"},
		},
		Deletes: []any{10}, // This deletes the document upserted above
	}

	resp, err := handler.Handle(ctx, ns, combinedReq)
	if err != nil {
		t.Fatalf("combined request failed: %v", err)
	}

	// Verify the counts
	// - delete_by_filter: deletes 1 doc (id=1)
	// - patch_by_filter: patches 1 doc (id=2)
	// - upsert: adds 1 doc (id=10)
	// - patch: patches 1 doc (id=3)
	// - delete: deletes 1 doc (id=10)
	// Total: deleted=2, upserted=1, patched=2
	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted (1 from filter + 1 explicit), got %d", resp.RowsDeleted)
	}
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
	if resp.RowsPatched != 2 {
		t.Errorf("expected 2 rows patched (1 from filter + 1 explicit), got %d", resp.RowsPatched)
	}
}

// TestHandler_DeleteByFilter_Resurrection verifies that delete_by_filter can be
// "resurrected" by a later upsert in the same request.
// Per spec: "If multiple operations in the same request affect the same ID, later
// phases in the ordering above MUST win (e.g., delete_by_filter can be 'resurrected'
// by a later upsert in the same request)."
func TestHandler_DeleteByFilter_Resurrection(t *testing.T) {
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

	ns := "test-resurrection"

	// Step 1: Set up initial documents
	initialDocs := []map[string]any{
		{"id": 1, "category": "A", "value": 100},
		{"id": 2, "category": "A", "value": 200},
		{"id": 3, "category": "B", "value": 300},
	}
	setupReq := &WriteRequest{
		RequestID:  "setup",
		UpsertRows: initialDocs,
	}
	if _, err := handler.Handle(ctx, ns, setupReq); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Add documents to tail store
	walEntry := wal.NewWalEntry(ns, 1)
	subBatch := wal.NewWriteSubBatch("setup")
	for _, doc := range initialDocs {
		id, _ := document.ParseID(doc["id"])
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
			}
		}
		subBatch.AddUpsert(protoID, attrs, nil, 0)
	}
	walEntry.SubBatches = append(walEntry.SubBatches, subBatch)
	tailStore.AddWALEntry(ns, walEntry)

	// Step 2: Execute resurrection scenario
	// - delete_by_filter deletes all docs with category="A" (ids 1 and 2)
	// - upsert adds back id=1 with new values (resurrection!)
	resurrectionReq := &WriteRequest{
		RequestID: "resurrection",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"category", "Eq", "A"},
		},
		UpsertRows: []map[string]any{
			{"id": 1, "category": "resurrected", "value": 999}, // Same ID, new data
		},
	}

	resp, err := handler.Handle(ctx, ns, resurrectionReq)
	if err != nil {
		t.Fatalf("resurrection request failed: %v", err)
	}

	// The delete_by_filter should delete 2 documents (ids 1 and 2)
	// The upsert should add back 1 document (id=1)
	// Both operations are recorded in the WAL, but the net effect depends on
	// how the WAL is applied - the upsert for id=1 comes after the delete.
	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted by filter (ids 1 and 2), got %d", resp.RowsDeleted)
	}
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted (resurrected id=1), got %d", resp.RowsUpserted)
	}

	// Verify the WAL contains both the delete and the upsert
	// When applied, the upsert should win since it comes after the delete in ordering
	walKey := "vex/namespaces/" + ns + "/wal/2.wal.zst"
	_, info, err := store.Get(ctx, walKey, nil)
	if err != nil {
		t.Fatalf("failed to get WAL entry: %v", err)
	}
	if info.Size == 0 {
		t.Error("WAL entry should not be empty")
	}
}

// TestHandler_WriteOrdering_PatchAfterUpsert verifies that patches run after upserts
// and can modify documents that were just upserted in the same request.
func TestHandler_WriteOrdering_PatchAfterUpsert(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-after-upsert"

	// Upsert a doc and patch the same doc in one request
	// The patch runs AFTER the upsert in ordering, so both are recorded
	req := &WriteRequest{
		RequestID: "patch-after-upsert",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "original", "value": 100},
		},
		PatchRows: []map[string]any{
			{"id": 1, "name": "patched"}, // Patches the doc just upserted
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	// Both operations should be recorded
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}
}

// TestHandler_WriteOrdering_DeleteAfterUpsert verifies that deletes run after upserts
// and can delete documents that were just upserted in the same request.
func TestHandler_WriteOrdering_DeleteAfterUpsert(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-delete-after-upsert"

	// Upsert a doc and delete the same doc in one request
	// The delete runs AFTER the upsert in ordering, so both are recorded
	req := &WriteRequest{
		RequestID: "delete-after-upsert",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "will be deleted", "value": 100},
		},
		Deletes: []any{1}, // Deletes the doc just upserted
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	// Both operations should be recorded
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted, got %d", resp.RowsDeleted)
	}
}

// TestHandler_WriteOrdering_DeleteAfterPatch verifies that deletes run after patches
// and can delete documents that were just patched in the same request.
func TestHandler_WriteOrdering_DeleteAfterPatch(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-delete-after-patch"

	// First set up a document
	setupReq := &WriteRequest{
		RequestID:  "setup",
		UpsertRows: []map[string]any{{"id": 1, "name": "original"}},
	}
	if _, err := handler.Handle(ctx, ns, setupReq); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Patch and delete the same doc in one request
	// The delete runs AFTER the patch in ordering
	req := &WriteRequest{
		RequestID: "delete-after-patch",
		PatchRows: []map[string]any{
			{"id": 1, "name": "patched"},
		},
		Deletes: []any{1}, // Deletes after patching
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	// Both operations should be recorded
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}
	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted, got %d", resp.RowsDeleted)
	}
}

// TestHandler_WriteOrdering_Atomicity verifies that all operations in a single
// request are atomic - they all end up in the same WAL entry.
func TestHandler_WriteOrdering_Atomicity(t *testing.T) {
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

	ns := "test-atomicity"

	// First set up some documents
	setupDocs := []map[string]any{
		{"id": 1, "category": "A", "value": 100},
		{"id": 2, "category": "A", "value": 200},
	}
	setupReq := &WriteRequest{
		RequestID:  "setup",
		UpsertRows: setupDocs,
	}
	if _, err := handler.Handle(ctx, ns, setupReq); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Add to tail store
	walEntry := wal.NewWalEntry(ns, 1)
	subBatch := wal.NewWriteSubBatch("setup")
	for _, doc := range setupDocs {
		id, _ := document.ParseID(doc["id"])
		protoID := wal.DocumentIDFromID(id)
		attrs := make(map[string]*wal.AttributeValue)
		for k, v := range doc {
			if k == "id" {
				continue
			}
			switch val := v.(type) {
			case string:
				attrs[k] = wal.StringValue(val)
			case int:
				attrs[k] = wal.IntValue(int64(val))
			}
		}
		subBatch.AddUpsert(protoID, attrs, nil, 0)
	}
	walEntry.SubBatches = append(walEntry.SubBatches, subBatch)
	tailStore.AddWALEntry(ns, walEntry)

	// Execute a complex multi-operation request
	complexReq := &WriteRequest{
		RequestID: "atomic-ops",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"category", "Eq", "A"},
		},
		UpsertRows: []map[string]any{
			{"id": 10, "category": "new", "value": 1000},
			{"id": 11, "category": "new", "value": 1100},
		},
		PatchRows: []map[string]any{
			{"id": 10, "patched": true},
		},
		Deletes: []any{11},
	}

	resp, err := handler.Handle(ctx, ns, complexReq)
	if err != nil {
		t.Fatalf("complex request failed: %v", err)
	}

	// Verify the response contains all operations
	if resp.RowsDeleted != 3 { // 2 from filter + 1 explicit
		t.Errorf("expected 3 rows deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsUpserted != 2 {
		t.Errorf("expected 2 rows upserted, got %d", resp.RowsUpserted)
	}
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}

	// Verify only ONE WAL entry was created for this complex request
	// (the setup created WAL entry 1, so this should be entry 2)
	walKey := "vex/namespaces/" + ns + "/wal/2.wal.zst"
	_, info, err := store.Get(ctx, walKey, nil)
	if err != nil {
		t.Fatalf("failed to get WAL entry: %v", err)
	}
	if info.Size == 0 {
		t.Error("WAL entry should not be empty")
	}

	// Verify that WAL entry 3 does NOT exist (no partial commits)
	_, _, err = store.Get(ctx, "vex/namespaces/"+ns+"/wal/3.wal.zst", nil)
	if err == nil {
		t.Error("WAL entry 3 should not exist - all ops should be in one entry")
	}

	// Verify namespace state advanced by exactly 1 for the complex request
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if loaded.State.WAL.HeadSeq != 2 {
		t.Errorf("expected WAL head_seq 2, got %d", loaded.State.WAL.HeadSeq)
	}
}

// TestHandler_WriteOrdering_PatchByFilter_BeforeUpserts verifies that patch_by_filter
// runs before explicit upserts, so patches to documents are based on their pre-request state.
func TestHandler_WriteOrdering_PatchByFilter_BeforeUpserts(t *testing.T) {
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

	ns := "test-patch-by-filter-order"

	// Set up documents
	setupDocs := []map[string]any{
		{"id": 1, "category": "patchable", "value": 100},
	}
	setupReq := &WriteRequest{
		RequestID:  "setup",
		UpsertRows: setupDocs,
	}
	if _, err := handler.Handle(ctx, ns, setupReq); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Add to tail store
	walEntry := wal.NewWalEntry(ns, 1)
	subBatch := wal.NewWriteSubBatch("setup")
	for _, doc := range setupDocs {
		id, _ := document.ParseID(doc["id"])
		protoID := wal.DocumentIDFromID(id)
		attrs := make(map[string]*wal.AttributeValue)
		for k, v := range doc {
			if k == "id" {
				continue
			}
			switch val := v.(type) {
			case string:
				attrs[k] = wal.StringValue(val)
			case int:
				attrs[k] = wal.IntValue(int64(val))
			}
		}
		subBatch.AddUpsert(protoID, attrs, nil, 0)
	}
	walEntry.SubBatches = append(walEntry.SubBatches, subBatch)
	tailStore.AddWALEntry(ns, walEntry)

	// Execute request with patch_by_filter and upsert for same category
	req := &WriteRequest{
		RequestID: "patch-filter-order",
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"category", "Eq", "patchable"},
			Updates: map[string]any{"patchedByFilter": true},
		},
		UpsertRows: []map[string]any{
			// This new doc is NOT visible to patch_by_filter since it runs after
			{"id": 2, "category": "patchable", "value": 200},
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	// patch_by_filter should only patch existing doc (id=1), not the new upsert (id=2)
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched (only existing doc), got %d", resp.RowsPatched)
	}
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
}

// TestHandler_WriteOrdering_DeleteByFilter_BeforePatchByFilter verifies that
// delete_by_filter runs before patch_by_filter in the canonical ordering.
func TestHandler_WriteOrdering_DeleteByFilter_BeforePatchByFilter(t *testing.T) {
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

	ns := "test-delete-before-patch-filter"

	// Set up documents with overlapping filter criteria
	setupDocs := []map[string]any{
		{"id": 1, "status": "active", "category": "A", "value": 100},
		{"id": 2, "status": "active", "category": "B", "value": 200},
		{"id": 3, "status": "inactive", "category": "A", "value": 300},
	}
	setupReq := &WriteRequest{
		RequestID:  "setup",
		UpsertRows: setupDocs,
	}
	if _, err := handler.Handle(ctx, ns, setupReq); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Add to tail store
	walEntry := wal.NewWalEntry(ns, 1)
	subBatch := wal.NewWriteSubBatch("setup")
	for _, doc := range setupDocs {
		id, _ := document.ParseID(doc["id"])
		protoID := wal.DocumentIDFromID(id)
		attrs := make(map[string]*wal.AttributeValue)
		for k, v := range doc {
			if k == "id" {
				continue
			}
			switch val := v.(type) {
			case string:
				attrs[k] = wal.StringValue(val)
			case int:
				attrs[k] = wal.IntValue(int64(val))
			}
		}
		subBatch.AddUpsert(protoID, attrs, nil, 0)
	}
	walEntry.SubBatches = append(walEntry.SubBatches, subBatch)
	tailStore.AddWALEntry(ns, walEntry)

	// Execute request with overlapping filters
	// delete_by_filter removes status=active (ids 1, 2)
	// patch_by_filter tries to patch category=A (ids 1, 3)
	// Since delete runs first, id=1 should be deleted and NOT patched
	req := &WriteRequest{
		RequestID: "delete-before-patch",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"status", "Eq", "active"},
		},
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"category", "Eq", "A"},
			Updates: map[string]any{"patched": true},
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	// delete_by_filter should delete 2 docs (ids 1, 2 with status=active)
	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted, got %d", resp.RowsDeleted)
	}

	// patch_by_filter sees the documents AFTER delete_by_filter,
	// so id=1 is deleted and won't be patched. Only id=3 remains with category=A
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched (id=3 after id=1 deleted), got %d", resp.RowsPatched)
	}
}
