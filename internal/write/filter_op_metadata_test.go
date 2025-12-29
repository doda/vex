package write

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestDeleteByFilter_WALMetadata verifies that delete_by_filter persists
// the required metadata in the WAL entry for deterministic replay.
func TestDeleteByFilter_WALMetadata(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-delete-by-filter-wal"

	// Insert test documents
	docs := []map[string]any{
		{"id": uint64(1), "status": "inactive", "value": 100},
		{"id": uint64(2), "status": "active", "value": 200},
		{"id": uint64(3), "status": "inactive", "value": 300},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Perform delete_by_filter
	deleteReq := &WriteRequest{
		RequestID: "delete-wal-metadata-test",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter:       []any{"status", "Eq", "inactive"},
			AllowPartial: true,
		},
	}

	resp, err := handler.Handle(ctx, ns, deleteReq)
	if err != nil {
		t.Fatalf("delete_by_filter failed: %v", err)
	}

	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted, got %d", resp.RowsDeleted)
	}

	// Read the WAL entry back from object storage
	walKey := "vex/namespaces/" + ns + "/" + wal.KeyForSeq(2) // seq 2 after initial insert
	data, _, err := store.Get(ctx, walKey, nil)
	if err != nil {
		t.Fatalf("failed to read WAL entry: %v", err)
	}
	walData, err := readAllBytes(data)
	if err != nil {
		t.Fatalf("failed to read WAL data: %v", err)
	}

	decoder, err := wal.NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry, err := decoder.Decode(walData)
	if err != nil {
		t.Fatalf("failed to decode WAL entry: %v", err)
	}

	// Verify filter operation metadata
	if len(entry.SubBatches) != 1 {
		t.Fatalf("expected 1 sub-batch, got %d", len(entry.SubBatches))
	}

	filterOps := entry.SubBatches[0].GetAllFilterOps()
	if len(filterOps) == 0 {
		t.Fatal("expected filter_ops to be set, got empty")
	}

	// Verify operation type (first filter op should be delete)
	filterOp := filterOps[0]
	if filterOp.Type != wal.FilterOperationType_FILTER_OPERATION_TYPE_DELETE {
		t.Errorf("expected DELETE operation type, got %v", filterOp.Type)
	}

	// Verify phase1_snapshot_seq
	if filterOp.Phase1SnapshotSeq != 1 {
		t.Errorf("expected phase1_snapshot_seq=1, got %d", filterOp.Phase1SnapshotSeq)
	}

	// Verify candidate_ids contains the matching IDs
	if len(filterOp.CandidateIds) != 2 {
		t.Errorf("expected 2 candidate IDs, got %d", len(filterOp.CandidateIds))
	}
	candidateKeys := candidateIDKeys(t, filterOp.CandidateIds)
	for _, id := range []document.ID{document.NewU64ID(1), document.NewU64ID(3)} {
		if _, ok := candidateKeys[idKey(id)]; !ok {
			t.Errorf("expected candidate ID %s", id.String())
		}
	}

	// Verify filter_json is set
	if filterOp.FilterJson == "" {
		t.Error("expected filter_json to be set")
	}

	// Verify the filter JSON can be parsed back
	var parsedFilter []any
	if err := json.Unmarshal([]byte(filterOp.FilterJson), &parsedFilter); err != nil {
		t.Errorf("filter_json is not valid JSON: %v", err)
	}
	if !reflect.DeepEqual(parsedFilter, deleteReq.DeleteByFilter.Filter) {
		t.Errorf("filter_json mismatch: got %v want %v", parsedFilter, deleteReq.DeleteByFilter.Filter)
	}

	// Verify allow_partial
	if !filterOp.AllowPartial {
		t.Error("expected allow_partial=true")
	}

	// Verify patch_json is empty for delete operations
	if filterOp.PatchJson != "" {
		t.Error("expected patch_json to be empty for delete_by_filter")
	}
}

// TestPatchByFilter_WALMetadata verifies that patch_by_filter persists
// the required metadata in the WAL entry for deterministic replay.
func TestPatchByFilter_WALMetadata(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-patch-by-filter-wal"

	// Insert test documents
	docs := []map[string]any{
		{"id": uint64(1), "status": "pending", "value": 100},
		{"id": uint64(2), "status": "active", "value": 200},
		{"id": uint64(3), "status": "pending", "value": 300},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Perform patch_by_filter
	patchReq := &WriteRequest{
		RequestID: "patch-wal-metadata-test",
		PatchByFilter: &PatchByFilterRequest{
			Filter:       []any{"status", "Eq", "pending"},
			Updates:      map[string]any{"status": "processed", "flag": true},
			AllowPartial: false,
		},
	}

	resp, err := handler.Handle(ctx, ns, patchReq)
	if err != nil {
		t.Fatalf("patch_by_filter failed: %v", err)
	}

	if resp.RowsPatched != 2 {
		t.Errorf("expected 2 rows patched, got %d", resp.RowsPatched)
	}

	// Read the WAL entry back from object storage
	walKey := "vex/namespaces/" + ns + "/" + wal.KeyForSeq(2)
	data, _, err := store.Get(ctx, walKey, nil)
	if err != nil {
		t.Fatalf("failed to read WAL entry: %v", err)
	}
	walData, err := readAllBytes(data)
	if err != nil {
		t.Fatalf("failed to read WAL data: %v", err)
	}

	decoder, err := wal.NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry, err := decoder.Decode(walData)
	if err != nil {
		t.Fatalf("failed to decode WAL entry: %v", err)
	}

	// Verify filter operation metadata
	if len(entry.SubBatches) != 1 {
		t.Fatalf("expected 1 sub-batch, got %d", len(entry.SubBatches))
	}

	filterOps := entry.SubBatches[0].GetAllFilterOps()
	if len(filterOps) == 0 {
		t.Fatal("expected filter_ops to be set, got empty")
	}

	// Verify operation type (first filter op should be patch)
	filterOp := filterOps[0]
	if filterOp.Type != wal.FilterOperationType_FILTER_OPERATION_TYPE_PATCH {
		t.Errorf("expected PATCH operation type, got %v", filterOp.Type)
	}

	// Verify phase1_snapshot_seq
	if filterOp.Phase1SnapshotSeq != 1 {
		t.Errorf("expected phase1_snapshot_seq=1, got %d", filterOp.Phase1SnapshotSeq)
	}

	// Verify candidate_ids
	if len(filterOp.CandidateIds) != 2 {
		t.Errorf("expected 2 candidate IDs, got %d", len(filterOp.CandidateIds))
	}
	candidateKeys := candidateIDKeys(t, filterOp.CandidateIds)
	for _, id := range []document.ID{document.NewU64ID(1), document.NewU64ID(3)} {
		if _, ok := candidateKeys[idKey(id)]; !ok {
			t.Errorf("expected candidate ID %s", id.String())
		}
	}

	// Verify filter_json
	if filterOp.FilterJson == "" {
		t.Error("expected filter_json to be set")
	}
	var parsedFilter []any
	if err := json.Unmarshal([]byte(filterOp.FilterJson), &parsedFilter); err != nil {
		t.Errorf("filter_json is not valid JSON: %v", err)
	}
	if !reflect.DeepEqual(parsedFilter, patchReq.PatchByFilter.Filter) {
		t.Errorf("filter_json mismatch: got %v want %v", parsedFilter, patchReq.PatchByFilter.Filter)
	}

	// Verify patch_json is set for patch operations
	if filterOp.PatchJson == "" {
		t.Error("expected patch_json to be set for patch_by_filter")
	}

	// Verify the patch JSON can be parsed back
	var parsedPatch map[string]any
	if err := json.Unmarshal([]byte(filterOp.PatchJson), &parsedPatch); err != nil {
		t.Errorf("patch_json is not valid JSON: %v", err)
	}
	expectedPatch := map[string]any{
		"status": "processed",
		"flag":   true,
	}
	if !reflect.DeepEqual(parsedPatch, expectedPatch) {
		t.Errorf("patch_json mismatch: got %v want %v", parsedPatch, expectedPatch)
	}

	// Verify patch content
	if parsedPatch["status"] != "processed" {
		t.Errorf("expected status='processed' in patch, got %v", parsedPatch["status"])
	}

	// Verify allow_partial (should be false)
	if filterOp.AllowPartial {
		t.Error("expected allow_partial=false")
	}
}

// TestFilterOp_DeterministicReplay verifies that filter-based operations
// can be deterministically replayed from WAL metadata.
func TestFilterOp_DeterministicReplay(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-deterministic-replay"

	// Insert test documents
	docs := []map[string]any{
		{"id": uint64(1), "category": "A", "count": int64(10)},
		{"id": uint64(2), "category": "B", "count": int64(20)},
		{"id": uint64(3), "category": "A", "count": int64(30)},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Perform delete_by_filter
	deleteReq := &WriteRequest{
		RequestID: "deterministic-replay-test",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"category", "Eq", "A"},
		},
	}

	_, err := handler.Handle(ctx, ns, deleteReq)
	if err != nil {
		t.Fatalf("delete_by_filter failed: %v", err)
	}

	// Read the WAL entry
	walKey := "vex/namespaces/" + ns + "/" + wal.KeyForSeq(2)
	data, _, err := store.Get(ctx, walKey, nil)
	if err != nil {
		t.Fatalf("failed to read WAL entry: %v", err)
	}
	walData, err := readAllBytes(data)
	if err != nil {
		t.Fatalf("failed to read WAL data: %v", err)
	}

	decoder, err := wal.NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry, err := decoder.Decode(walData)
	if err != nil {
		t.Fatalf("failed to decode WAL entry: %v", err)
	}

	filterOps := entry.SubBatches[0].GetAllFilterOps()
	if len(filterOps) == 0 {
		t.Fatal("expected filter_ops to be set, got empty")
	}
	filterOp := filterOps[0]

	// Simulate replay: re-parse the filter and verify candidate IDs
	var filterExpr any
	if err := json.Unmarshal([]byte(filterOp.FilterJson), &filterExpr); err != nil {
		t.Fatalf("failed to parse filter_json: %v", err)
	}

	parsedFilter, err := filter.Parse(filterExpr)
	if err != nil {
		t.Fatalf("failed to parse filter from WAL: %v", err)
	}

	// Create a fresh tail store and verify the filter still matches
	freshTailStore := tail.New(tail.DefaultConfig(), store, nil, cache.NewMemoryCache(cache.MemoryCacheConfig{
		MaxBytes: 64 * 1024 * 1024,
	}))

	// Re-add the initial documents to simulate replay state
	initialEntry := wal.NewWalEntry(ns, 1)
	subBatch := wal.NewWriteSubBatch("setup")
	for _, doc := range docs {
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
			case int64:
				attrs[k] = wal.IntValue(val)
			}
		}
		subBatch.AddUpsert(protoID, attrs, nil, 0)
	}
	initialEntry.SubBatches = append(initialEntry.SubBatches, subBatch)
	freshTailStore.AddWALEntry(ns, initialEntry)

	// Verify each candidate ID from WAL would still be processed deterministically
	for _, candidateProtoID := range filterOp.CandidateIds {
		candidateID, err := wal.DocumentIDToID(candidateProtoID)
		if err != nil {
			t.Fatalf("failed to convert candidate ID: %v", err)
		}

		// Get document from fresh tail store
		doc, err := freshTailStore.GetDocument(ctx, ns, candidateID)
		if err != nil {
			t.Fatalf("failed to get document: %v", err)
		}

		if doc == nil {
			t.Errorf("expected document %v to exist in replay state", candidateID)
			continue
		}

		// Build filter document and verify it matches
		filterDoc := make(filter.Document)
		for k, v := range doc.Attributes {
			filterDoc[k] = v
		}

		if !parsedFilter.Eval(filterDoc) {
			t.Errorf("document %v should match filter during replay", candidateID)
		}
	}
}

// TestCandidateIDs_BoundedByLimit verifies that candidate IDs respect the max limit.
func TestCandidateIDs_BoundedByLimit(t *testing.T) {
	// This is a unit test verifying the constant is set correctly
	if MaxDeleteByFilterRows != 5_000_000 {
		t.Errorf("MaxDeleteByFilterRows should be 5,000,000, got %d", MaxDeleteByFilterRows)
	}
	if MaxPatchByFilterRows != 500_000 {
		t.Errorf("MaxPatchByFilterRows should be 500,000, got %d", MaxPatchByFilterRows)
	}
}

// TestMultiFilterOps_BothInSameRequest verifies that a request containing both
// delete_by_filter and patch_by_filter records both operations in the WAL.
func TestMultiFilterOps_BothInSameRequest(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-multi-filter-ops"

	// Insert test documents
	docs := []map[string]any{
		{"id": uint64(1), "status": "to_delete", "value": 100},
		{"id": uint64(2), "status": "to_patch", "value": 200},
		{"id": uint64(3), "status": "to_delete", "value": 300},
		{"id": uint64(4), "status": "to_patch", "value": 400},
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Perform both delete_by_filter and patch_by_filter in same request
	multiReq := &WriteRequest{
		RequestID: "multi-filter-ops-test",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter:       []any{"status", "Eq", "to_delete"},
			AllowPartial: true,
		},
		PatchByFilter: &PatchByFilterRequest{
			Filter:       []any{"status", "Eq", "to_patch"},
			Updates:      map[string]any{"status": "patched"},
			AllowPartial: true,
		},
	}

	resp, err := handler.Handle(ctx, ns, multiReq)
	if err != nil {
		t.Fatalf("multi-filter-ops request failed: %v", err)
	}

	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsPatched != 2 {
		t.Errorf("expected 2 rows patched, got %d", resp.RowsPatched)
	}

	// Read the WAL entry back from object storage
	walKey := "vex/namespaces/" + ns + "/" + wal.KeyForSeq(2) // seq 2 after initial insert
	data, _, err := store.Get(ctx, walKey, nil)
	if err != nil {
		t.Fatalf("failed to read WAL entry: %v", err)
	}
	walData, err := readAllBytes(data)
	if err != nil {
		t.Fatalf("failed to read WAL data: %v", err)
	}

	decoder, err := wal.NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry, err := decoder.Decode(walData)
	if err != nil {
		t.Fatalf("failed to decode WAL entry: %v", err)
	}

	// Verify both filter operations are in the WAL
	if len(entry.SubBatches) != 1 {
		t.Fatalf("expected 1 sub-batch, got %d", len(entry.SubBatches))
	}

	filterOps := entry.SubBatches[0].GetAllFilterOps()
	if len(filterOps) != 2 {
		t.Fatalf("expected 2 filter ops, got %d", len(filterOps))
	}

	// Verify ordering: delete before patch
	if filterOps[0].Type != wal.FilterOperationType_FILTER_OPERATION_TYPE_DELETE {
		t.Errorf("first filter op should be DELETE, got %v", filterOps[0].Type)
	}
	if filterOps[1].Type != wal.FilterOperationType_FILTER_OPERATION_TYPE_PATCH {
		t.Errorf("second filter op should be PATCH, got %v", filterOps[1].Type)
	}

	// Verify delete operation metadata
	deleteOp := filterOps[0]
	if len(deleteOp.CandidateIds) != 2 {
		t.Errorf("delete op expected 2 candidate IDs, got %d", len(deleteOp.CandidateIds))
	}
	if deleteOp.FilterJson == "" {
		t.Error("delete op filter_json should be set")
	}

	// Verify patch operation metadata
	patchOp := filterOps[1]
	if len(patchOp.CandidateIds) != 2 {
		t.Errorf("patch op expected 2 candidate IDs, got %d", len(patchOp.CandidateIds))
	}
	if patchOp.FilterJson == "" {
		t.Error("patch op filter_json should be set")
	}
	if patchOp.PatchJson == "" {
		t.Error("patch op patch_json should be set")
	}
}

// TestMultiFilterOps_ReplayPreservesOrdering verifies that replaying a WAL entry
// with both filter ops preserves the ordering (delete before patch).
func TestMultiFilterOps_ReplayPreservesOrdering(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	handler, tailStore := createHandlerWithTail(t, store)
	defer handler.Close()

	ns := "test-replay-ordering"

	// Insert test documents - doc 3 matches both filters
	docs := []map[string]any{
		{"id": uint64(1), "type": "A", "status": "pending"},
		{"id": uint64(2), "type": "B", "status": "pending"},
		{"id": uint64(3), "type": "A", "status": "pending"}, // Matches delete, so should NOT be patched
	}
	insertDocumentsForTest(t, ctx, handler, tailStore, ns, docs)

	// Delete type=A, then patch status=pending
	multiReq := &WriteRequest{
		RequestID: "replay-ordering-test",
		DeleteByFilter: &DeleteByFilterRequest{
			Filter: []any{"type", "Eq", "A"},
		},
		PatchByFilter: &PatchByFilterRequest{
			Filter:  []any{"status", "Eq", "pending"},
			Updates: map[string]any{"status": "done"},
		},
	}

	resp, err := handler.Handle(ctx, ns, multiReq)
	if err != nil {
		t.Fatalf("multi-filter-ops request failed: %v", err)
	}

	// Should delete 2 docs (id 1 and 3 with type=A)
	// Should patch 1 doc (id 2 with status=pending, id 3 was already deleted)
	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched (other was deleted), got %d", resp.RowsPatched)
	}

	// Read and verify the WAL entry
	walKey := "vex/namespaces/" + ns + "/" + wal.KeyForSeq(2)
	data, _, err := store.Get(ctx, walKey, nil)
	if err != nil {
		t.Fatalf("failed to read WAL entry: %v", err)
	}
	walData, err := readAllBytes(data)
	if err != nil {
		t.Fatalf("failed to read WAL data: %v", err)
	}

	decoder, err := wal.NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry, err := decoder.Decode(walData)
	if err != nil {
		t.Fatalf("failed to decode WAL entry: %v", err)
	}

	filterOps := entry.SubBatches[0].GetAllFilterOps()

	// Verify ordering is preserved in WAL
	if len(filterOps) != 2 {
		t.Fatalf("expected 2 filter ops, got %d", len(filterOps))
	}
	if filterOps[0].Type != wal.FilterOperationType_FILTER_OPERATION_TYPE_DELETE {
		t.Error("first filter op should be DELETE")
	}
	if filterOps[1].Type != wal.FilterOperationType_FILTER_OPERATION_TYPE_PATCH {
		t.Error("second filter op should be PATCH")
	}

	// The delete op should have 2 candidates (id 1 and 3)
	if len(filterOps[0].CandidateIds) != 2 {
		t.Errorf("delete op expected 2 candidates, got %d", len(filterOps[0].CandidateIds))
	}

	// The patch op should have candidates for all pending docs at snapshot time
	// (id 1, 2, 3), but only doc 2 was actually patched due to delete_by_filter
	// The WAL records the phase 1 candidates, which were evaluated before deletes
}

// readAllBytes reads all bytes from an io.ReadCloser
func readAllBytes(rc io.ReadCloser) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		rc.Close()
		return nil, err
	}
	rc.Close()
	return buf.Bytes(), nil
}

func candidateIDKeys(t *testing.T, ids []*wal.DocumentID) map[string]struct{} {
	t.Helper()
	keys := make(map[string]struct{}, len(ids))
	for _, protoID := range ids {
		id, err := wal.DocumentIDToID(protoID)
		if err != nil {
			t.Fatalf("failed to convert candidate ID: %v", err)
		}
		keys[idKey(id)] = struct{}{}
	}
	return keys
}

func idKey(id document.ID) string {
	return id.Type().String() + ":" + id.String()
}
