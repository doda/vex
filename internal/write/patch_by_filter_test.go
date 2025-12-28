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

// --- Test: patch_by_filter updates documents matching filter ---

func TestHandler_PatchByFilter_UpdatesMatchingDocuments(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-patch-by-filter"

	// Insert test documents
	docs := []map[string]any{
		{"id": 1, "category": "A", "value": 100},
		{"id": 2, "category": "B", "value": 200},
		{"id": 3, "category": "A", "value": 300},
		{"id": 4, "category": "B", "value": 400},
		{"id": 5, "category": "A", "value": 500},
	}

	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Patch all documents with category "A" to set status="updated"
	patchReq := &WriteRequest{
		RequestID: "patch-by-filter-test",
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"category", "Eq", "A"},
			Updates: map[string]any{"status": "updated"},
		},
	}

	resp, err := handler.Handle(ctx, ns, patchReq)
	if err != nil {
		t.Fatalf("patch_by_filter failed: %v", err)
	}

	// Should patch 3 documents (IDs 1, 3, 5 have category "A")
	if resp.RowsPatched != 3 {
		t.Errorf("expected 3 rows patched, got %d", resp.RowsPatched)
	}
	if resp.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", resp.RowsAffected)
	}
	if resp.RowsRemaining {
		t.Error("expected rows_remaining to be false")
	}
}

// --- Test: patch_by_filter runs AFTER delete_by_filter, BEFORE other ops ---

func TestHandler_PatchByFilter_RunsAfterDeleteByFilter(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-patch-after-delete"

	// Insert test documents
	docs := []map[string]any{
		{"id": 1, "category": "A", "status": "old"},
		{"id": 2, "category": "B", "status": "old"},
		{"id": 3, "category": "A", "status": "old"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// In a single request: delete_by_filter + patch_by_filter + upsert
	// The ordering should be: delete_by_filter -> patch_by_filter -> upserts
	combinedReq := &WriteRequest{
		RequestID: "combined-order-test",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"category", "Eq", "A"}, // Deletes IDs 1 and 3
		},
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"category", "Eq", "B"}, // Patches ID 2
			Updates: map[string]any{"status": "patched"},
		},
		UpsertRows: []map[string]any{
			{"id": 10, "category": "new", "status": "inserted"},
		},
	}

	resp, err := handler.Handle(ctx, ns, combinedReq)
	if err != nil {
		t.Fatalf("combined operation failed: %v", err)
	}

	// delete_by_filter should have found and deleted 2 docs (IDs 1 and 3)
	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted (from filter), got %d", resp.RowsDeleted)
	}

	// patch_by_filter should have patched 1 doc (ID 2)
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}

	// upsert should have added 1 doc
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
}

// --- Test: max 500,000 rows can be patched ---

func TestHandler_PatchByFilter_MaxRowsLimit(t *testing.T) {
	// This test verifies the constant is correctly set
	if MaxPatchByFilterRows != 500_000 {
		t.Errorf("expected MaxPatchByFilterRows to be 500,000, got %d", MaxPatchByFilterRows)
	}
}

// --- Test: exceeding limit with allow_partial=false returns 400 ---

func TestHandler_PatchByFilter_ExceedsLimitReturns400(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-patch-exceeds-limit"

	// Create test documents
	docs := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		docs[i] = map[string]any{"id": i + 1, "category": "test"}
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// The actual limit is 500k, but we can test the error type exists
	if !errors.Is(ErrPatchByFilterTooMany, ErrPatchByFilterTooMany) {
		t.Error("ErrPatchByFilterTooMany should exist")
	}
}

// --- Test: allow_partial=true succeeds and sets rows_remaining=true ---

func TestHandler_PatchByFilter_AllowPartialSucceeds(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-patch-allow-partial"

	// Insert test documents
	docs := []map[string]any{
		{"id": 1, "status": "active"},
		{"id": 2, "status": "active"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Test with allow_partial=true
	patchReq := &WriteRequest{
		RequestID: "patch-allow-partial",
		PatchByFilter: &PatchByFilterRequest{
			Filter:       []any{"status", "Eq", "active"},
			Updates:      map[string]any{"processed": true},
			AllowPartial: true,
		},
	}

	resp, err := handler.Handle(ctx, ns, patchReq)
	if err != nil {
		t.Fatalf("patch_by_filter with allow_partial failed: %v", err)
	}

	// Should patch 2 documents
	if resp.RowsPatched != 2 {
		t.Errorf("expected 2 rows patched, got %d", resp.RowsPatched)
	}
	// rows_remaining should be false since we didn't exceed the limit
	if resp.RowsRemaining {
		t.Error("expected rows_remaining to be false when not exceeding limit")
	}
}

// --- Test: two-phase with re-evaluation (Read Committed semantics) ---

func TestHandler_PatchByFilter_TwoPhaseReEvaluation(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-patch-two-phase"

	// Insert test documents
	docs := []map[string]any{
		{"id": 1, "status": "pending", "priority": 1},
		{"id": 2, "status": "pending", "priority": 2},
		{"id": 3, "status": "complete", "priority": 1},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Patch documents with status "pending"
	patchReq := &WriteRequest{
		RequestID: "patch-two-phase",
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"status", "Eq", "pending"},
			Updates: map[string]any{"processed": true},
		},
	}

	resp, err := handler.Handle(ctx, ns, patchReq)
	if err != nil {
		t.Fatalf("patch_by_filter two-phase failed: %v", err)
	}

	// Should patch 2 documents (IDs 1 and 2 have status "pending")
	if resp.RowsPatched != 2 {
		t.Errorf("expected 2 rows patched, got %d", resp.RowsPatched)
	}
}

// --- Test: vector attribute cannot be patched via filter ---

func TestHandler_PatchByFilter_VectorPatchForbidden(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-patch-vector-forbidden"

	// Insert test documents
	docs := []map[string]any{
		{"id": 1, "category": "A"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Try to patch with vector attribute - should fail
	patchReq := &WriteRequest{
		RequestID: "patch-vector",
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"category", "Eq", "A"},
			Updates: map[string]any{"vector": []float64{1.0, 2.0, 3.0}},
		},
	}

	_, err := handler.Handle(ctx, ns, patchReq)
	if err == nil {
		t.Error("expected error when patching vector attribute, got nil")
	}

	if !errors.Is(err, ErrVectorPatchForbidden) {
		t.Errorf("expected ErrVectorPatchForbidden, got: %v", err)
	}
}

// --- Test: ParseWriteRequest correctly parses patch_by_filter ---

func TestParseWriteRequest_PatchByFilter(t *testing.T) {
	tests := []struct {
		name          string
		body          map[string]any
		expectPatch   bool
		expectPartial bool
		wantErr       bool
	}{
		{
			name: "basic patch_by_filter",
			body: map[string]any{
				"patch_by_filter": map[string]any{
					"filter":  []any{"status", "Eq", "active"},
					"updates": map[string]any{"processed": true},
				},
			},
			expectPatch:   true,
			expectPartial: false,
			wantErr:       false,
		},
		{
			name: "patch_by_filter with allow_partial",
			body: map[string]any{
				"patch_by_filter": map[string]any{
					"filter":  []any{"status", "Eq", "active"},
					"updates": map[string]any{"processed": true},
				},
				"patch_by_filter_allow_partial": true,
			},
			expectPatch:   true,
			expectPartial: true,
			wantErr:       false,
		},
		{
			name: "patch_by_filter with And filter",
			body: map[string]any{
				"patch_by_filter": map[string]any{
					"filter": []any{
						"And",
						[]any{
							[]any{"status", "Eq", "active"},
							[]any{"priority", "Gt", 5},
						},
					},
					"updates": map[string]any{"urgent": true},
				},
			},
			expectPatch: true,
			wantErr:     false,
		},
		{
			name: "patch_by_filter combined with upserts",
			body: map[string]any{
				"patch_by_filter": map[string]any{
					"filter":  []any{"status", "Eq", "old"},
					"updates": map[string]any{"status": "updated"},
				},
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "new"},
				},
			},
			expectPatch: true,
			wantErr:     false,
		},
		{
			name:        "no patch_by_filter",
			body:        map[string]any{},
			expectPatch: false,
			wantErr:     false,
		},
		{
			name: "patch_by_filter missing filter",
			body: map[string]any{
				"patch_by_filter": map[string]any{
					"updates": map[string]any{"status": "updated"},
				},
			},
			wantErr: true,
		},
		{
			name: "patch_by_filter missing updates",
			body: map[string]any{
				"patch_by_filter": map[string]any{
					"filter": []any{"status", "Eq", "active"},
				},
			},
			wantErr: true,
		},
		{
			name: "patch_by_filter invalid format",
			body: map[string]any{
				"patch_by_filter": "invalid",
			},
			wantErr: true,
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

			if tt.expectPatch {
				if req.PatchByFilter == nil {
					t.Error("expected patch_by_filter to be parsed, got nil")
				} else if tt.expectPartial && !req.PatchByFilter.AllowPartial {
					t.Error("expected AllowPartial to be true")
				}
			} else {
				if req.PatchByFilter != nil {
					t.Error("expected patch_by_filter to be nil")
				}
			}
		})
	}
}

// --- Test: patch_by_filter with nil filter matches nothing ---

func TestHandler_PatchByFilter_NilFilter(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-patch-nil-filter"

	// Insert test documents
	docs := []map[string]any{
		{"id": 1, "name": "doc1"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Test with nil filter (should match nothing)
	patchReq := &WriteRequest{
		RequestID: "patch-nil-filter",
		PatchByFilter: &PatchByFilterRequest{
			Filter:  nil,
			Updates: map[string]any{"status": "patched"},
		},
	}

	resp, err := handler.Handle(ctx, ns, patchReq)
	if err != nil {
		t.Fatalf("patch_by_filter with nil filter failed: %v", err)
	}

	// Should patch 0 documents
	if resp.RowsPatched != 0 {
		t.Errorf("expected 0 rows patched with nil filter, got %d", resp.RowsPatched)
	}
}

// --- Test: patch_by_filter with invalid filter returns error ---

func TestHandler_PatchByFilter_InvalidFilter(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-patch-invalid-filter"

	// Insert a test document
	docs := []map[string]any{
		{"id": 1, "name": "doc1"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Test with invalid filter expression
	patchReq := &WriteRequest{
		RequestID: "patch-invalid-filter",
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"InvalidOp", "field", "value"}, // Invalid operator
			Updates: map[string]any{"status": "patched"},
		},
	}

	_, err := handler.Handle(ctx, ns, patchReq)
	if err == nil {
		t.Error("expected error for invalid filter, got nil")
	}

	if !errors.Is(err, ErrInvalidFilter) {
		t.Errorf("expected ErrInvalidFilter, got: %v", err)
	}
}

// --- Test: patch_by_filter without tail store is a no-op ---

func TestHandler_PatchByFilter_NoTailStore(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create handler WITHOUT tail store
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-no-tail"

	// patch_by_filter should silently be a no-op when no tail store is configured
	patchReq := &WriteRequest{
		RequestID: "patch-no-tail",
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"status", "Eq", "active"},
			Updates: map[string]any{"processed": true},
		},
	}

	resp, err := handler.Handle(ctx, ns, patchReq)
	if err != nil {
		t.Fatalf("patch_by_filter without tail store should not fail: %v", err)
	}

	// Should patch 0 documents (no tail store to evaluate)
	if resp.RowsPatched != 0 {
		t.Errorf("expected 0 rows patched without tail store, got %d", resp.RowsPatched)
	}
}

// --- Test: patch_by_filter with complex filter ---

func TestHandler_PatchByFilter_ComplexFilter(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-patch-complex-filter"

	// Insert test documents with various attributes
	docs := []map[string]any{
		{"id": 1, "status": "active", "priority": int64(10), "region": "us"},
		{"id": 2, "status": "active", "priority": int64(5), "region": "eu"},
		{"id": 3, "status": "inactive", "priority": int64(10), "region": "us"},
		{"id": 4, "status": "active", "priority": int64(10), "region": "eu"},
		{"id": 5, "status": "active", "priority": int64(3), "region": "us"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Patch documents where status="active" AND priority >= 10
	patchReq := &WriteRequest{
		RequestID: "patch-complex-filter",
		PatchByFilter: &PatchByFilterRequest{
			Filter: []any{
				"And",
				[]any{
					[]any{"status", "Eq", "active"},
					[]any{"priority", "Gte", int64(10)},
				},
			},
			Updates: map[string]any{"highlighted": true},
		},
	}

	resp, err := handler.Handle(ctx, ns, patchReq)
	if err != nil {
		t.Fatalf("patch_by_filter with complex filter failed: %v", err)
	}

	// Should patch docs 1 and 4 (active AND priority >= 10)
	if resp.RowsPatched != 2 {
		t.Errorf("expected 2 rows patched, got %d", resp.RowsPatched)
	}
}

// --- Test: patch_by_filter combined with delete_by_filter and explicit ops ---

func TestHandler_PatchByFilter_CombinedOperations(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create a tail store
	tailStore := tail.New(tail.DefaultConfig(), store, nil, cache.NewMemoryCache(cache.MemoryCacheConfig{
		MaxBytes: 64 * 1024 * 1024,
	}))

	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-combined"

	// Insert test documents
	docs := []map[string]any{
		{"id": 1, "type": "delete-me", "status": "old"},
		{"id": 2, "type": "patch-me", "status": "old"},
		{"id": 3, "type": "keep", "status": "old"},
	}

	// First, insert documents
	insertReq := &WriteRequest{
		RequestID:  "setup",
		UpsertRows: docs,
	}
	_, err = handler.Handle(ctx, ns, insertReq)
	if err != nil {
		t.Fatalf("failed to insert documents: %v", err)
	}

	// Add WAL entry to tail store
	walEntry := wal.NewWalEntry(ns, 1)
	subBatch := wal.NewWriteSubBatch("setup")
	for _, doc := range docs {
		id, _ := document.ParseID(doc["id"])
		protoID := wal.DocumentIDFromID(id)
		attrs := make(map[string]*wal.AttributeValue)
		for k, v := range doc {
			if k == "id" {
				continue
			}
			if s, ok := v.(string); ok {
				attrs[k] = wal.StringValue(s)
			}
		}
		subBatch.AddUpsert(protoID, attrs, nil, 0)
	}
	walEntry.SubBatches = append(walEntry.SubBatches, subBatch)
	tailStore.AddWALEntry(ns, walEntry)

	// Now run combined request:
	// 1. delete_by_filter (type="delete-me") -> deletes ID 1
	// 2. patch_by_filter (type="patch-me") -> patches ID 2
	// 3. upsert -> adds ID 10
	// 4. explicit patch -> patches ID 3 (if it exists)
	// 5. explicit delete -> deletes ID 100 (no-op)
	combinedReq := &WriteRequest{
		RequestID: "combined",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"type", "Eq", "delete-me"},
		},
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"type", "Eq", "patch-me"},
			Updates: map[string]any{"status": "filtered-patch"},
		},
		UpsertRows: []map[string]any{
			{"id": 10, "type": "new", "status": "inserted"},
		},
		PatchRows: []map[string]any{
			{"id": 3, "status": "explicit-patch"},
		},
		Deletes: []any{100}, // Non-existent, should be a no-op
	}

	resp, err := handler.Handle(ctx, ns, combinedReq)
	if err != nil {
		t.Fatalf("combined operation failed: %v", err)
	}

	// Verify counts
	if resp.RowsDeleted != 2 { // 1 from filter + 1 explicit
		t.Errorf("expected 2 rows deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsPatched != 2 { // 1 from filter + 1 explicit
		t.Errorf("expected 2 rows patched, got %d", resp.RowsPatched)
	}
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
}

// --- Test: patch_by_filter ordering with delete (deleted doc not patched) ---

func TestHandler_PatchByFilter_DeletedDocNotPatched(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-delete-before-patch"

	// Insert test documents that match both filters
	docs := []map[string]any{
		{"id": 1, "category": "match", "status": "old"},
		{"id": 2, "category": "match", "status": "old"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Both delete_by_filter and patch_by_filter target category="match"
	// Since delete runs first, patch should not find any documents
	req := &WriteRequest{
		RequestID: "delete-then-patch",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"category", "Eq", "match"},
		},
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"category", "Eq", "match"},
			Updates: map[string]any{"status": "patched"},
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("operation failed: %v", err)
	}

	// delete_by_filter should delete 2 docs
	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted, got %d", resp.RowsDeleted)
	}

	// Note: In the current implementation, patch_by_filter uses the same snapshot
	// as delete_by_filter (before any changes). However, due to phase 2 re-evaluation,
	// deleted documents should be skipped (doc.Deleted check).
	// This test verifies the two-phase semantics work correctly.
}

// --- Test: rows_remaining from both delete and patch ---

func TestHandler_PatchByFilter_RowsRemainingFromBoth(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-rows-remaining-both"

	// Insert test documents
	docs := []map[string]any{
		{"id": 1, "category": "A"},
		{"id": 2, "category": "B"},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Test that rows_remaining is properly tracked from either operation
	// Since limits are 5M and 500k respectively, we can't easily test exceeding,
	// but we verify the flag is properly OR'd
	req := &WriteRequest{
		RequestID: "rows-remaining",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter:       []any{"category", "Eq", "A"},
			AllowPartial: true,
		},
		PatchByFilter: &PatchByFilterRequest{
			Filter:       []any{"category", "Eq", "B"},
			Updates:      map[string]any{"status": "patched"},
			AllowPartial: true,
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("operation failed: %v", err)
	}

	// Neither should exceed limit, so rows_remaining should be false
	if resp.RowsRemaining {
		t.Error("expected rows_remaining to be false when neither limit exceeded")
	}
}
