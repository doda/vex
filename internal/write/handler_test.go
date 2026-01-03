package write

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestHandler_HandleBasicUpsert(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	req := &WriteRequest{
		RequestID: "test-req-1",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "test1", "value": 100},
			{"id": 2, "name": "test2", "value": 200},
		},
	}

	resp, err := handler.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 2 {
		t.Errorf("expected 2 rows upserted, got %d", resp.RowsUpserted)
	}
	if resp.RowsAffected != 2 {
		t.Errorf("expected 2 rows affected, got %d", resp.RowsAffected)
	}
	if resp.RowsDeleted != 0 {
		t.Errorf("expected 0 rows deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsPatched != 0 {
		t.Errorf("expected 0 rows patched, got %d", resp.RowsPatched)
	}
}

func TestHandlerWALConflictDoesNotAdvanceState(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-wal-conflict"
	walKey := "vex/namespaces/" + ns + "/wal/00000000000000000001.wal.zst"

	encoder, err := wal.NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	conflictEntry := wal.NewWalEntry(ns, 1)
	conflictBatch := wal.NewWriteSubBatch("conflict")
	conflictBatch.AddUpsert(wal.DocumentIDFromID(document.NewU64ID(99)), map[string]*wal.AttributeValue{
		"name": wal.StringValue("conflict"),
	}, nil, 0)
	conflictEntry.SubBatches = append(conflictEntry.SubBatches, conflictBatch)

	conflictResult, err := encoder.Encode(conflictEntry)
	if err != nil {
		encoder.Close()
		t.Fatalf("failed to encode conflict WAL: %v", err)
	}
	encoder.Close()

	_, err = store.PutIfAbsent(ctx, walKey, bytes.NewReader(conflictResult.Data), int64(len(conflictResult.Data)), &objectstore.PutOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("failed to precreate WAL: %v", err)
	}

	req := &WriteRequest{
		RequestID: "test-conflict",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "winner"},
		},
	}

	_, err = handler.Handle(ctx, ns, req)
	if err == nil {
		t.Fatal("expected WAL conflict error")
	}
	if !errors.Is(err, wal.ErrWALSeqConflict) {
		t.Fatalf("expected WAL seq conflict error, got %v", err)
	}

	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if loaded.State.WAL.HeadSeq != 0 {
		t.Errorf("expected head_seq 0, got %d", loaded.State.WAL.HeadSeq)
	}
}

func TestHandler_HandleDeletes(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	req := &WriteRequest{
		RequestID: "test-req-2",
		Deletes:   []any{1, 2, 3},
	}

	resp, err := handler.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsDeleted != 3 {
		t.Errorf("expected 3 rows deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", resp.RowsAffected)
	}
}

func TestHandler_HandleMixedOperations(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	req := &WriteRequest{
		RequestID: "test-req-3",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "test1"},
			{"id": 2, "name": "test2"},
		},
		Deletes: []any{3, 4},
	}

	resp, err := handler.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 2 {
		t.Errorf("expected 2 rows upserted, got %d", resp.RowsUpserted)
	}
	if resp.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsAffected != 4 {
		t.Errorf("expected 4 rows affected, got %d", resp.RowsAffected)
	}
}

func TestHandler_ImplicitNamespaceCreation(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "new-namespace"

	// Verify namespace doesn't exist yet
	_, err = stateMan.Load(ctx, ns)
	if err != namespace.ErrStateNotFound {
		t.Errorf("expected namespace not found, got: %v", err)
	}

	req := &WriteRequest{
		RequestID:  "test-req-4",
		UpsertRows: []map[string]any{{"id": 1, "name": "test"}},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}

	// Verify namespace was created
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}

	if loaded.State.Namespace != ns {
		t.Errorf("expected namespace %q, got %q", ns, loaded.State.Namespace)
	}
	if loaded.State.WAL.HeadSeq != 1 {
		t.Errorf("expected WAL head_seq 1, got %d", loaded.State.WAL.HeadSeq)
	}
}

func TestHandler_WALCommitToObjectStorage(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "wal-test"

	req := &WriteRequest{
		RequestID:  "test-req-5",
		UpsertRows: []map[string]any{{"id": 1, "name": "test"}},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}

	// Verify WAL entry exists in object storage
	walKey := "vex/namespaces/" + ns + "/wal/00000000000000000001.wal.zst"
	_, info, err := store.Get(ctx, walKey, nil)
	if err != nil {
		t.Fatalf("WAL entry not found in object storage: %v", err)
	}
	if info.Size == 0 {
		t.Error("WAL entry has zero size")
	}

	// Verify namespace state was updated
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}

	if loaded.State.WAL.HeadSeq != 1 {
		t.Errorf("expected WAL head_seq 1, got %d", loaded.State.WAL.HeadSeq)
	}
	walKeyRelative := "wal/00000000000000000001.wal.zst"
	if loaded.State.WAL.HeadKey != walKeyRelative {
		t.Errorf("expected WAL head_key %q, got %q", walKeyRelative, loaded.State.WAL.HeadKey)
	}
}

func TestHandler_SequentialWrites(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "sequential-test"

	for i := 1; i <= 5; i++ {
		req := &WriteRequest{
			RequestID:  "test-req-seq-" + string(rune('0'+i)),
			UpsertRows: []map[string]any{{"id": i, "name": "test"}},
		}

		resp, err := handler.Handle(ctx, ns, req)
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}

		if resp.RowsUpserted != 1 {
			t.Errorf("write %d: expected 1 row upserted, got %d", i, resp.RowsUpserted)
		}

		// Verify WAL sequence progresses
		loaded, err := stateMan.Load(ctx, ns)
		if err != nil {
			t.Fatalf("failed to load namespace state: %v", err)
		}

		if loaded.State.WAL.HeadSeq != uint64(i) {
			t.Errorf("write %d: expected WAL head_seq %d, got %d", i, i, loaded.State.WAL.HeadSeq)
		}
	}
}

func TestHandler_InvalidIDRejected(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	req := &WriteRequest{
		RequestID:  "test-req-invalid",
		UpsertRows: []map[string]any{{"name": "missing-id"}}, // No id field
	}

	_, err = handler.Handle(ctx, "test-ns", req)
	if err == nil {
		t.Error("expected error for missing ID, got nil")
	}
}

func TestHandler_VectorUpsert(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	req := &WriteRequest{
		RequestID: "test-req-vector",
		UpsertRows: []map[string]any{
			{
				"id":     1,
				"name":   "test",
				"vector": []any{0.1, 0.2, 0.3, 0.4},
			},
		},
	}

	resp, err := handler.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
}

func TestParseWriteRequest(t *testing.T) {
	tests := []struct {
		name    string
		body    map[string]any
		wantErr bool
		check   func(*testing.T, *WriteRequest)
	}{
		{
			name: "upsert_rows",
			body: map[string]any{
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "test1"},
					map[string]any{"id": 2, "name": "test2"},
				},
			},
			check: func(t *testing.T, req *WriteRequest) {
				if len(req.UpsertRows) != 2 {
					t.Errorf("expected 2 upsert rows, got %d", len(req.UpsertRows))
				}
			},
		},
		{
			name: "deletes",
			body: map[string]any{
				"deletes": []any{1, 2, 3},
			},
			check: func(t *testing.T, req *WriteRequest) {
				if len(req.Deletes) != 3 {
					t.Errorf("expected 3 deletes, got %d", len(req.Deletes))
				}
			},
		},
		{
			name: "combined",
			body: map[string]any{
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "test"},
				},
				"deletes": []any{2},
			},
			check: func(t *testing.T, req *WriteRequest) {
				if len(req.UpsertRows) != 1 {
					t.Errorf("expected 1 upsert row, got %d", len(req.UpsertRows))
				}
				if len(req.Deletes) != 1 {
					t.Errorf("expected 1 delete, got %d", len(req.Deletes))
				}
			},
		},
		{
			name: "invalid upsert_rows type",
			body: map[string]any{
				"upsert_rows": "not an array",
			},
			wantErr: true,
		},
		{
			name: "invalid upsert_rows element",
			body: map[string]any{
				"upsert_rows": []any{"not an object"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := ParseWriteRequest("req-id", tt.body)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, req)
			}
		})
	}
}

func TestValidateColumnarIDs(t *testing.T) {
	tests := []struct {
		name    string
		ids     []any
		wantErr bool
	}{
		{
			name:    "unique integers",
			ids:     []any{1, 2, 3},
			wantErr: false,
		},
		{
			name:    "unique strings",
			ids:     []any{"a", "b", "c"},
			wantErr: false,
		},
		{
			name:    "unique mixed",
			ids:     []any{1, "a", 2, "b"},
			wantErr: false,
		},
		{
			name:    "duplicate integers",
			ids:     []any{1, 2, 1},
			wantErr: true,
		},
		{
			name:    "duplicate strings",
			ids:     []any{"a", "b", "a"},
			wantErr: true,
		},
		{
			name:    "numeric string duplicates normalized integer",
			ids:     []any{1, "1"}, // Both normalize to u64(1)
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateColumnarIDs(tt.ids)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestHandler_DuplicateIDsLastWriteWins(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Create request with duplicate IDs - the later one should win
	req := &WriteRequest{
		RequestID: "test-req-dupe",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "first", "value": 100},
			{"id": 2, "name": "unique", "value": 200},
			{"id": 1, "name": "second", "value": 300}, // Duplicate - should override
		},
	}

	resp, err := handler.Handle(ctx, "test-ns-dupe", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only 2 unique IDs should be upserted (deduplication happens before WAL commit)
	if resp.RowsUpserted != 2 {
		t.Errorf("expected 2 rows upserted (deduplicated), got %d", resp.RowsUpserted)
	}
	if resp.RowsAffected != 2 {
		t.Errorf("expected 2 rows affected (deduplicated), got %d", resp.RowsAffected)
	}
}

func TestHandler_DuplicateIDsNormalizedComparison(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Create request where numeric ID and string ID normalize to same value
	// Integer 1 and string "1" both normalize to u64(1)
	req := &WriteRequest{
		RequestID: "test-req-normalized",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "integer-id", "value": 100},
			{"id": "1", "name": "string-id", "value": 200}, // Should override, same normalized ID
		},
	}

	resp, err := handler.Handle(ctx, "test-ns-normalized", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only 1 unique ID (normalized) should be upserted
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted (normalized dedup), got %d", resp.RowsUpserted)
	}
}

func TestHandler_DuplicateIDsWithVector(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Create request with duplicate IDs where vectors differ - last one should win
	req := &WriteRequest{
		RequestID: "test-req-vector-dupe",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "first", "vector": []any{1.0, 0.0, 0.0}},
			{"id": 1, "name": "second", "vector": []any{0.0, 1.0, 0.0}}, // Should override
		},
	}

	resp, err := handler.Handle(ctx, "test-ns-vector-dupe", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only 1 unique ID should be upserted
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
}

func TestHandler_SchemaInferenceFromFirstWrite(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-ns-schema"

	// First write includes id and various attribute types
	req := &WriteRequest{
		RequestID: "test-req-schema",
		UpsertRows: []map[string]any{
			{
				"id":     1,
				"name":   "test",          // string
				"count":  100,             // int
				"price":  19.99,           // float
				"active": true,            // bool
				"tags":   []any{"a", "b"}, // string array
				"vector": []any{0.1, 0.2}, // vector
			},
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}

	// Second write with a new document, same attributes
	req2 := &WriteRequest{
		RequestID: "test-req-schema-2",
		UpsertRows: []map[string]any{
			{
				"id":     2,
				"name":   "test2",
				"count":  200,
				"price":  29.99,
				"active": false,
				"tags":   []any{"c", "d"},
				"vector": []any{0.3, 0.4},
			},
		},
	}

	resp2, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	if resp2.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp2.RowsUpserted)
	}

	// Verify namespace was created and state updated
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}
	if loaded.State.WAL.HeadSeq != 2 {
		t.Errorf("expected WAL head_seq 2, got %d", loaded.State.WAL.HeadSeq)
	}
}

func TestHandler_DocumentArbitraryAttributes(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Test that documents can have arbitrary attributes
	req := &WriteRequest{
		RequestID: "test-req-attrs",
		UpsertRows: []map[string]any{
			{
				"id":           1,
				"custom_field": "value1",
				"another_attr": 123,
				"metadata":     "some info",
			},
			{
				"id":            2,
				"different_set": "yes", // Different attributes than doc 1
				"rating":        4.5,
			},
		},
	}

	resp, err := handler.Handle(ctx, "test-ns-attrs", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.RowsUpserted != 2 {
		t.Errorf("expected 2 rows upserted, got %d", resp.RowsUpserted)
	}
}

func TestParseColumnarToRows(t *testing.T) {
	tests := []struct {
		name       string
		input      map[string]any
		wantCount  int
		wantErr    bool
		errContain string
	}{
		{
			name: "basic columnar format",
			input: map[string]any{
				"ids":   []any{1, 2, 3},
				"name":  []any{"a", "b", "c"},
				"value": []any{100, 200, 300},
			},
			wantCount: 3,
			wantErr:   false,
		},
		{
			name: "columnar with vectors",
			input: map[string]any{
				"ids":    []any{1, 2},
				"name":   []any{"vec1", "vec2"},
				"vector": []any{[]any{0.1, 0.2}, []any{0.3, 0.4}},
			},
			wantCount: 2,
			wantErr:   false,
		},
		{
			name:       "missing ids field",
			input:      map[string]any{"name": []any{"a", "b"}},
			wantErr:    true,
			errContain: "missing 'ids' field",
		},
		{
			name:       "ids not an array",
			input:      map[string]any{"ids": "not-an-array"},
			wantErr:    true,
			errContain: "'ids' must be an array",
		},
		{
			name: "attribute not an array",
			input: map[string]any{
				"ids":  []any{1, 2},
				"name": "not-an-array",
			},
			wantErr:    true,
			errContain: "must be an array",
		},
		{
			name: "mismatched array lengths",
			input: map[string]any{
				"ids":  []any{1, 2, 3},
				"name": []any{"a", "b"}, // Missing one element
			},
			wantErr:    true,
			errContain: "has 2 elements, expected 3",
		},
		{
			name: "duplicate IDs",
			input: map[string]any{
				"ids":  []any{1, 2, 1}, // Duplicate
				"name": []any{"a", "b", "c"},
			},
			wantErr:    true,
			errContain: "duplicate",
		},
		{
			name: "empty ids array",
			input: map[string]any{
				"ids": []any{},
			},
			wantCount: 0,
			wantErr:   false,
		},
		{
			name: "single element",
			input: map[string]any{
				"ids":   []any{42},
				"name":  []any{"single"},
				"value": []any{999},
			},
			wantCount: 1,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := ParseColumnarToRows(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContain != "" && !contains(err.Error(), tt.errContain) {
					t.Errorf("expected error containing %q, got %q", tt.errContain, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(rows) != tt.wantCount {
				t.Errorf("expected %d rows, got %d", tt.wantCount, len(rows))
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestParseColumnarToRows_ContentVerification(t *testing.T) {
	input := map[string]any{
		"ids":   []any{1, 2, 3},
		"name":  []any{"alice", "bob", "charlie"},
		"score": []any{100, 200, 300},
	}

	rows, err := ParseColumnarToRows(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Verify first row
	if rows[0]["id"] != 1 || rows[0]["name"] != "alice" || rows[0]["score"] != 100 {
		t.Errorf("row 0 mismatch: %v", rows[0])
	}

	// Verify second row
	if rows[1]["id"] != 2 || rows[1]["name"] != "bob" || rows[1]["score"] != 200 {
		t.Errorf("row 1 mismatch: %v", rows[1])
	}

	// Verify third row
	if rows[2]["id"] != 3 || rows[2]["name"] != "charlie" || rows[2]["score"] != 300 {
		t.Errorf("row 2 mismatch: %v", rows[2])
	}
}

func TestParseWriteRequest_UpsertColumns(t *testing.T) {
	tests := []struct {
		name       string
		body       map[string]any
		wantRows   int
		wantErr    bool
		errContain string
	}{
		{
			name: "upsert_columns only",
			body: map[string]any{
				"upsert_columns": map[string]any{
					"ids":  []any{1, 2},
					"name": []any{"a", "b"},
				},
			},
			wantRows: 2,
			wantErr:  false,
		},
		{
			name: "upsert_columns with upsert_rows",
			body: map[string]any{
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "row1"},
				},
				"upsert_columns": map[string]any{
					"ids":  []any{2, 3},
					"name": []any{"col1", "col2"},
				},
			},
			wantRows: 3, // 1 from rows + 2 from columns
			wantErr:  false,
		},
		{
			name: "upsert_columns not an object",
			body: map[string]any{
				"upsert_columns": []any{"not", "an", "object"},
			},
			wantErr:    true,
			errContain: "must be an object",
		},
		{
			name: "upsert_columns with duplicate IDs",
			body: map[string]any{
				"upsert_columns": map[string]any{
					"ids":  []any{1, 1}, // Duplicate
					"name": []any{"a", "b"},
				},
			},
			wantErr:    true,
			errContain: "duplicate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := ParseWriteRequest("test-req", tt.body)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContain != "" && !contains(err.Error(), tt.errContain) {
					t.Errorf("expected error containing %q, got %q", tt.errContain, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(req.UpsertRows) != tt.wantRows {
				t.Errorf("expected %d rows, got %d", tt.wantRows, len(req.UpsertRows))
			}
		})
	}
}

func TestHandler_UpsertColumnsIntegration(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Parse columnar request
	body := map[string]any{
		"upsert_columns": map[string]any{
			"ids":   []any{1, 2, 3},
			"name":  []any{"alice", "bob", "charlie"},
			"value": []any{100, 200, 300},
		},
	}

	writeReq, err := ParseWriteRequest("test-columnar", body)
	if err != nil {
		t.Fatalf("failed to parse request: %v", err)
	}

	resp, err := handler.Handle(ctx, "test-ns-columnar", writeReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 3 {
		t.Errorf("expected 3 rows upserted, got %d", resp.RowsUpserted)
	}
	if resp.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", resp.RowsAffected)
	}
}

func TestHandler_UpsertColumnsWithVectors(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	body := map[string]any{
		"upsert_columns": map[string]any{
			"ids":    []any{1, 2},
			"name":   []any{"vec1", "vec2"},
			"vector": []any{[]any{0.1, 0.2, 0.3}, []any{0.4, 0.5, 0.6}},
		},
	}

	writeReq, err := ParseWriteRequest("test-columnar-vectors", body)
	if err != nil {
		t.Fatalf("failed to parse request: %v", err)
	}

	resp, err := handler.Handle(ctx, "test-ns-columnar-vectors", writeReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 2 {
		t.Errorf("expected 2 rows upserted, got %d", resp.RowsUpserted)
	}
}

// --- patch_rows tests ---

func TestHandler_PatchRowsUpdatesOnlySpecifiedAttributes(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-ns"

	seedReq := &WriteRequest{
		RequestID: "test-patch-seed",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "original-name"},
			{"id": 2, "value": 123},
		},
	}
	if _, err := handler.Handle(ctx, ns, seedReq); err != nil {
		t.Fatalf("unexpected seed error: %v", err)
	}

	// patch_rows should update only specified attributes
	req := &WriteRequest{
		RequestID: "test-patch-1",
		PatchRows: []map[string]any{
			{"id": 1, "name": "patched-name"},
			{"id": 2, "value": 999},
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Patches should be recorded
	if resp.RowsPatched != 2 {
		t.Errorf("expected 2 rows patched, got %d", resp.RowsPatched)
	}
	if resp.RowsAffected != 2 {
		t.Errorf("expected 2 rows affected, got %d", resp.RowsAffected)
	}
	// Patches are not upserts
	if resp.RowsUpserted != 0 {
		t.Errorf("expected 0 rows upserted, got %d", resp.RowsUpserted)
	}
	if resp.RowsDeleted != 0 {
		t.Errorf("expected 0 rows deleted, got %d", resp.RowsDeleted)
	}
}

func TestHandler_PatchToMissingIDSilentlyIgnored(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-missing-ns"

	// Patch to a document that doesn't exist should be silently ignored at apply time
	// At write time, the patch is recorded in the WAL (the actual no-op happens during query/apply)
	req := &WriteRequest{
		RequestID: "test-patch-missing",
		PatchRows: []map[string]any{
			{"id": 999, "name": "patched-nonexistent"},
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error (patch to missing ID should not fail): %v", err)
	}

	if resp.RowsPatched != 0 {
		t.Errorf("expected 0 rows patched for missing ID, got %d", resp.RowsPatched)
	}
	if resp.RowsAffected != 0 {
		t.Errorf("expected 0 rows affected for missing ID, got %d", resp.RowsAffected)
	}
}

func TestHandler_PatchMissingIDWithoutSnapshotFails(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-missing-no-snapshot-ns"

	req := &WriteRequest{
		RequestID: "test-patch-missing-no-snapshot",
		PatchRows: []map[string]any{
			{"id": 999, "name": "patched-nonexistent"},
		},
	}

	_, err = handler.Handle(ctx, ns, req)
	if err == nil {
		t.Fatal("expected error when patching without snapshot, got nil")
	}
	if !errors.Is(err, ErrPatchRequiresTail) {
		t.Fatalf("expected ErrPatchRequiresTail, got %v", err)
	}
}

func TestHandler_VectorCannotBePatched(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-vector-ns"

	// Attempting to patch a vector attribute should fail with 400
	req := &WriteRequest{
		RequestID: "test-patch-vector",
		PatchRows: []map[string]any{
			{"id": 1, "vector": []any{0.1, 0.2, 0.3}},
		},
	}

	_, err = handler.Handle(ctx, ns, req)
	if err == nil {
		t.Fatal("expected error when patching vector, got nil")
	}

	if !errors.Is(err, ErrVectorPatchForbidden) {
		t.Errorf("expected ErrVectorPatchForbidden, got: %v", err)
	}
}

func TestHandler_PatchRowsDuplicateIDsLastWriteWins(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-dupe-ns"

	seedReq := &WriteRequest{
		RequestID: "test-patch-dupe-seed",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "first"},
			{"id": 2, "name": "unique"},
		},
	}
	if _, err := handler.Handle(ctx, ns, seedReq); err != nil {
		t.Fatalf("unexpected seed error: %v", err)
	}

	// Multiple patches to the same ID - only the last one should be applied
	req := &WriteRequest{
		RequestID: "test-patch-dupe",
		PatchRows: []map[string]any{
			{"id": 1, "name": "first", "value": 100},
			{"id": 2, "name": "unique"},
			{"id": 1, "name": "second", "value": 300}, // Duplicate - should override
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only 2 unique IDs should be patched (last-write-wins deduplication)
	if resp.RowsPatched != 2 {
		t.Errorf("expected 2 rows patched (deduplicated), got %d", resp.RowsPatched)
	}
	if resp.RowsAffected != 2 {
		t.Errorf("expected 2 rows affected (deduplicated), got %d", resp.RowsAffected)
	}
}

func TestHandler_PatchRowsNormalizedIDDeduplication(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-normalized-ns"

	seedReq := &WriteRequest{
		RequestID: "test-patch-normalized-seed",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "original"},
		},
	}
	if _, err := handler.Handle(ctx, ns, seedReq); err != nil {
		t.Fatalf("unexpected seed error: %v", err)
	}

	// Integer 1 and string "1" both normalize to u64(1) - should dedupe
	req := &WriteRequest{
		RequestID: "test-patch-normalized",
		PatchRows: []map[string]any{
			{"id": 1, "name": "integer-id"},
			{"id": "1", "name": "string-id"}, // Should override, same normalized ID
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only 1 unique ID (normalized) should be patched
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched (normalized dedup), got %d", resp.RowsPatched)
	}
}

func TestHandler_PatchRowsMissingID(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-no-id-ns"

	// Patch without ID should fail
	req := &WriteRequest{
		RequestID: "test-patch-no-id",
		PatchRows: []map[string]any{
			{"name": "missing-id"}, // No id field
		},
	}

	_, err = handler.Handle(ctx, ns, req)
	if err == nil {
		t.Fatal("expected error for missing ID, got nil")
	}

	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("expected ErrInvalidRequest, got: %v", err)
	}
}

func TestHandler_PatchWithUpsertAndDelete(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-mixed-ns"

	// Combine upsert, patch, and delete in one request
	// Write ordering: upserts → patches → deletes
	req := &WriteRequest{
		RequestID: "test-patch-mixed",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "upserted"},
		},
		PatchRows: []map[string]any{
			{"id": 1, "name": "patched"},
		},
		Deletes: []any{3},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched, got %d", resp.RowsPatched)
	}
	if resp.RowsDeleted != 1 {
		t.Errorf("expected 1 row deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", resp.RowsAffected)
	}
}

// --- patch_columns tests ---

func TestParseColumnarToPatchRows(t *testing.T) {
	tests := []struct {
		name       string
		input      map[string]any
		wantCount  int
		wantErr    bool
		errContain string
	}{
		{
			name: "basic columnar patch format",
			input: map[string]any{
				"ids":   []any{1, 2, 3},
				"name":  []any{"a", "b", "c"},
				"value": []any{100, 200, 300},
			},
			wantCount: 3,
			wantErr:   false,
		},
		{
			name:       "missing ids field",
			input:      map[string]any{"name": []any{"a", "b"}},
			wantErr:    true,
			errContain: "missing 'ids' field",
		},
		{
			name:       "ids not an array",
			input:      map[string]any{"ids": "not-an-array"},
			wantErr:    true,
			errContain: "'ids' must be an array",
		},
		{
			name: "attribute not an array",
			input: map[string]any{
				"ids":  []any{1, 2},
				"name": "not-an-array",
			},
			wantErr:    true,
			errContain: "must be an array",
		},
		{
			name: "mismatched array lengths",
			input: map[string]any{
				"ids":  []any{1, 2, 3},
				"name": []any{"a", "b"},
			},
			wantErr:    true,
			errContain: "has 2 elements, expected 3",
		},
		{
			name: "duplicate IDs returns 400",
			input: map[string]any{
				"ids":  []any{1, 2, 1},
				"name": []any{"a", "b", "c"},
			},
			wantErr:    true,
			errContain: "duplicate",
		},
		{
			name: "vector attribute forbidden in patch",
			input: map[string]any{
				"ids":    []any{1, 2},
				"name":   []any{"a", "b"},
				"vector": []any{[]any{0.1, 0.2}, []any{0.3, 0.4}},
			},
			wantErr:    true,
			errContain: "vector attribute cannot be patched",
		},
		{
			name: "empty ids array",
			input: map[string]any{
				"ids": []any{},
			},
			wantCount: 0,
			wantErr:   false,
		},
		{
			name: "single element",
			input: map[string]any{
				"ids":   []any{42},
				"name":  []any{"single"},
				"value": []any{999},
			},
			wantCount: 1,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := ParseColumnarToPatchRows(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContain != "" && !contains(err.Error(), tt.errContain) {
					t.Errorf("expected error containing %q, got %q", tt.errContain, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(rows) != tt.wantCount {
				t.Errorf("expected %d rows, got %d", tt.wantCount, len(rows))
			}
		})
	}
}

func TestParseColumnarToPatchRows_ContentVerification(t *testing.T) {
	input := map[string]any{
		"ids":   []any{1, 2, 3},
		"name":  []any{"alice", "bob", "charlie"},
		"score": []any{100, 200, 300},
	}

	rows, err := ParseColumnarToPatchRows(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Verify first row
	if rows[0]["id"] != 1 || rows[0]["name"] != "alice" || rows[0]["score"] != 100 {
		t.Errorf("row 0 mismatch: %v", rows[0])
	}

	// Verify second row
	if rows[1]["id"] != 2 || rows[1]["name"] != "bob" || rows[1]["score"] != 200 {
		t.Errorf("row 1 mismatch: %v", rows[1])
	}

	// Verify third row
	if rows[2]["id"] != 3 || rows[2]["name"] != "charlie" || rows[2]["score"] != 300 {
		t.Errorf("row 2 mismatch: %v", rows[2])
	}
}

func TestParseWriteRequest_PatchColumns(t *testing.T) {
	tests := []struct {
		name       string
		body       map[string]any
		wantRows   int
		wantErr    bool
		errContain string
	}{
		{
			name: "patch_columns only",
			body: map[string]any{
				"patch_columns": map[string]any{
					"ids":  []any{1, 2},
					"name": []any{"a", "b"},
				},
			},
			wantRows: 2,
			wantErr:  false,
		},
		{
			name: "patch_columns with patch_rows",
			body: map[string]any{
				"patch_rows": []any{
					map[string]any{"id": 1, "name": "row1"},
				},
				"patch_columns": map[string]any{
					"ids":  []any{2, 3},
					"name": []any{"col1", "col2"},
				},
			},
			wantRows: 3, // 1 from rows + 2 from columns
			wantErr:  false,
		},
		{
			name: "patch_columns not an object",
			body: map[string]any{
				"patch_columns": []any{"not", "an", "object"},
			},
			wantErr:    true,
			errContain: "must be an object",
		},
		{
			name: "patch_columns with duplicate IDs",
			body: map[string]any{
				"patch_columns": map[string]any{
					"ids":  []any{1, 1},
					"name": []any{"a", "b"},
				},
			},
			wantErr:    true,
			errContain: "duplicate",
		},
		{
			name: "patch_columns with vector forbidden",
			body: map[string]any{
				"patch_columns": map[string]any{
					"ids":    []any{1, 2},
					"name":   []any{"a", "b"},
					"vector": []any{[]any{0.1, 0.2}, []any{0.3, 0.4}},
				},
			},
			wantErr:    true,
			errContain: "vector attribute cannot be patched",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := ParseWriteRequest("test-req", tt.body)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContain != "" && !contains(err.Error(), tt.errContain) {
					t.Errorf("expected error containing %q, got %q", tt.errContain, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(req.PatchRows) != tt.wantRows {
				t.Errorf("expected %d patch rows, got %d", tt.wantRows, len(req.PatchRows))
			}
		})
	}
}

func TestHandler_PatchColumnsIntegration(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	seedReq := &WriteRequest{
		RequestID: "test-columnar-patch-seed",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "seed1", "value": 10},
			{"id": 2, "name": "seed2", "value": 20},
			{"id": 3, "name": "seed3", "value": 30},
		},
	}
	if _, err := handler.Handle(ctx, "test-ns-columnar-patch", seedReq); err != nil {
		t.Fatalf("unexpected seed error: %v", err)
	}

	// Parse columnar patch request
	body := map[string]any{
		"patch_columns": map[string]any{
			"ids":   []any{1, 2, 3},
			"name":  []any{"alice", "bob", "charlie"},
			"value": []any{100, 200, 300},
		},
	}

	writeReq, err := ParseWriteRequest("test-columnar-patch", body)
	if err != nil {
		t.Fatalf("failed to parse request: %v", err)
	}

	resp, err := handler.Handle(ctx, "test-ns-columnar-patch", writeReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsPatched != 3 {
		t.Errorf("expected 3 rows patched, got %d", resp.RowsPatched)
	}
	if resp.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", resp.RowsAffected)
	}
	if resp.RowsUpserted != 0 {
		t.Errorf("expected 0 rows upserted, got %d", resp.RowsUpserted)
	}
}

func TestHandler_PatchColumnsVectorForbidden(t *testing.T) {
	// Try to patch with vectors in columnar format - should fail
	body := map[string]any{
		"patch_columns": map[string]any{
			"ids":    []any{1, 2},
			"name":   []any{"vec1", "vec2"},
			"vector": []any{[]any{0.1, 0.2, 0.3}, []any{0.4, 0.5, 0.6}},
		},
	}

	_, err := ParseWriteRequest("test-columnar-patch-vector", body)
	if err == nil {
		t.Fatal("expected error when patching vectors in columnar format, got nil")
	}

	if !errors.Is(err, ErrVectorPatchForbidden) {
		t.Errorf("expected ErrVectorPatchForbidden, got: %v", err)
	}
}

func TestHandler_PatchColumnsDuplicateIDsReturns400(t *testing.T) {
	// Duplicate IDs in columnar patch should return 400 (unlike row format which deduplicates)
	body := map[string]any{
		"patch_columns": map[string]any{
			"ids":  []any{1, 2, 1}, // Duplicate ID
			"name": []any{"a", "b", "c"},
		},
	}

	_, err := ParseWriteRequest("test-columnar-patch-dupe", body)
	if err == nil {
		t.Fatal("expected error for duplicate IDs in columnar patch, got nil")
	}

	if !errors.Is(err, ErrDuplicateIDColumn) {
		t.Errorf("expected ErrDuplicateIDColumn, got: %v", err)
	}
}

func TestHandler_PatchColumnsWithPatchRows(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	handler, err := NewHandlerWithTail(store, stateMan, tailStore)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	seedReq := &WriteRequest{
		RequestID: "test-mixed-patch-seed",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "seed1"},
			{"id": 2, "name": "seed2"},
			{"id": 3, "name": "seed3"},
		},
	}
	if _, err := handler.Handle(ctx, "test-ns-mixed-patch", seedReq); err != nil {
		t.Fatalf("unexpected seed error: %v", err)
	}

	// Combine patch_rows and patch_columns in one request
	body := map[string]any{
		"patch_rows": []any{
			map[string]any{"id": 1, "name": "row-patch"},
		},
		"patch_columns": map[string]any{
			"ids":  []any{2, 3},
			"name": []any{"col-patch1", "col-patch2"},
		},
	}

	writeReq, err := ParseWriteRequest("test-mixed-patch", body)
	if err != nil {
		t.Fatalf("failed to parse request: %v", err)
	}

	resp, err := handler.Handle(ctx, "test-ns-mixed-patch", writeReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 1 from patch_rows + 2 from patch_columns = 3 total
	if resp.RowsPatched != 3 {
		t.Errorf("expected 3 rows patched, got %d", resp.RowsPatched)
	}
	if resp.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", resp.RowsAffected)
	}
}

func TestParseWriteRequest_PatchRows(t *testing.T) {
	tests := []struct {
		name       string
		body       map[string]any
		wantErr    bool
		errContain string
		check      func(*testing.T, *WriteRequest)
	}{
		{
			name: "patch_rows only",
			body: map[string]any{
				"patch_rows": []any{
					map[string]any{"id": 1, "name": "patched1"},
					map[string]any{"id": 2, "name": "patched2"},
				},
			},
			check: func(t *testing.T, req *WriteRequest) {
				if len(req.PatchRows) != 2 {
					t.Errorf("expected 2 patch rows, got %d", len(req.PatchRows))
				}
			},
		},
		{
			name: "patch_rows with upsert_rows",
			body: map[string]any{
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "upserted"},
				},
				"patch_rows": []any{
					map[string]any{"id": 2, "name": "patched"},
				},
			},
			check: func(t *testing.T, req *WriteRequest) {
				if len(req.UpsertRows) != 1 {
					t.Errorf("expected 1 upsert row, got %d", len(req.UpsertRows))
				}
				if len(req.PatchRows) != 1 {
					t.Errorf("expected 1 patch row, got %d", len(req.PatchRows))
				}
			},
		},
		{
			name: "patch_rows not an array",
			body: map[string]any{
				"patch_rows": "not an array",
			},
			wantErr:    true,
			errContain: "patch_rows must be an array",
		},
		{
			name: "patch_rows element not an object",
			body: map[string]any{
				"patch_rows": []any{"not an object"},
			},
			wantErr:    true,
			errContain: "patch_rows[0] must be an object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := ParseWriteRequest("req-id", tt.body)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContain != "" && !contains(err.Error(), tt.errContain) {
					t.Errorf("expected error containing %q, got %q", tt.errContain, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, req)
			}
		})
	}
}

// --- Delete-specific tests (write-deletes task) ---

// TestHandler_DeleteNonExistentIDSucceedsSilently verifies that deleting
// a document ID that doesn't exist succeeds without error (silent no-op).
func TestHandler_DeleteNonExistentIDSucceedsSilently(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-delete-nonexistent"

	// Delete IDs that have never been created - should succeed silently
	req := &WriteRequest{
		RequestID: "test-delete-nonexistent",
		Deletes:   []any{999, 998, 997},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("deleting non-existent IDs should not fail: %v", err)
	}

	// The deletes should still be recorded in the WAL
	if resp.RowsDeleted != 3 {
		t.Errorf("expected 3 rows deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", resp.RowsAffected)
	}
}

// TestHandler_DeleteMixedExistentAndNonExistent verifies that a request
// with both existing and non-existing document IDs succeeds.
func TestHandler_DeleteMixedExistentAndNonExistent(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-delete-mixed"

	// First, create some documents
	createReq := &WriteRequest{
		RequestID: "create-docs",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "doc1"},
			{"id": 2, "name": "doc2"},
		},
	}

	_, err = handler.Handle(ctx, ns, createReq)
	if err != nil {
		t.Fatalf("failed to create initial documents: %v", err)
	}

	// Now delete a mix of existing (1, 2) and non-existent (999) IDs
	deleteReq := &WriteRequest{
		RequestID: "delete-mixed",
		Deletes:   []any{1, 999, 2},
	}

	resp, err := handler.Handle(ctx, ns, deleteReq)
	if err != nil {
		t.Fatalf("mixed delete should not fail: %v", err)
	}

	// All 3 deletes should be recorded (even the non-existent one)
	if resp.RowsDeleted != 3 {
		t.Errorf("expected 3 rows deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", resp.RowsAffected)
	}
}

// TestHandler_DeleteWithVariousIDTypes verifies deletes work with different ID types.
func TestHandler_DeleteWithVariousIDTypes(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Delete using various ID types: integer, string, UUID
	req := &WriteRequest{
		RequestID: "delete-various-types",
		Deletes: []any{
			1,
			"string-id",
			"550e8400-e29b-41d4-a716-446655440000",
		},
	}

	resp, err := handler.Handle(ctx, "test-delete-types", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsDeleted != 3 {
		t.Errorf("expected 3 rows deleted, got %d", resp.RowsDeleted)
	}
}

// TestHandler_DeleteEmptyArray verifies empty deletes array is allowed.
func TestHandler_DeleteEmptyArray(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	req := &WriteRequest{
		RequestID: "delete-empty",
		Deletes:   []any{},
	}

	resp, err := handler.Handle(ctx, "test-delete-empty", req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsDeleted != 0 {
		t.Errorf("expected 0 rows deleted, got %d", resp.RowsDeleted)
	}
	if resp.RowsAffected != 0 {
		t.Errorf("expected 0 rows affected, got %d", resp.RowsAffected)
	}
}

// TestHandler_DeleteRecordedInWAL verifies that deletes are committed to the WAL.
func TestHandler_DeleteRecordedInWAL(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-delete-wal"

	req := &WriteRequest{
		RequestID: "delete-wal-test",
		Deletes:   []any{1, 2, 3},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsDeleted != 3 {
		t.Errorf("expected 3 rows deleted, got %d", resp.RowsDeleted)
	}

	// Verify WAL entry exists in object storage
	walKey := "vex/namespaces/" + ns + "/wal/00000000000000000001.wal.zst"
	_, info, err := store.Get(ctx, walKey, nil)
	if err != nil {
		t.Fatalf("WAL entry not found in object storage: %v", err)
	}
	if info.Size == 0 {
		t.Error("WAL entry has zero size")
	}

	// Verify namespace state was updated
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}

	if loaded.State.WAL.HeadSeq != 1 {
		t.Errorf("expected WAL head_seq 1, got %d", loaded.State.WAL.HeadSeq)
	}
}

// --- Schema Updates Tests (write-schema-updates task) ---

// TestParseSchemaUpdate verifies schema parsing from write request body.
func TestParseSchemaUpdate(t *testing.T) {
	tests := []struct {
		name       string
		input      any
		wantErr    bool
		errContain string
		check      func(*testing.T, *SchemaUpdate)
	}{
		{
			name:  "nil returns nil",
			input: nil,
			check: func(t *testing.T, s *SchemaUpdate) {
				if s != nil {
					t.Error("expected nil schema update")
				}
			},
		},
		{
			name: "basic schema with type only",
			input: map[string]any{
				"name": map[string]any{
					"type": "string",
				},
			},
			check: func(t *testing.T, s *SchemaUpdate) {
				if s == nil {
					t.Fatal("expected non-nil schema update")
				}
				if attr, ok := s.Attributes["name"]; !ok {
					t.Error("expected 'name' attribute")
				} else if attr.Type != "string" {
					t.Errorf("expected type 'string', got %q", attr.Type)
				}
			},
		},
		{
			name: "schema with filterable",
			input: map[string]any{
				"category": map[string]any{
					"type":       "string",
					"filterable": true,
				},
			},
			check: func(t *testing.T, s *SchemaUpdate) {
				attr := s.Attributes["category"]
				if attr.Filterable == nil || !*attr.Filterable {
					t.Error("expected filterable to be true")
				}
			},
		},
		{
			name: "schema with full_text_search boolean",
			input: map[string]any{
				"description": map[string]any{
					"type":             "string",
					"full_text_search": true,
				},
			},
			check: func(t *testing.T, s *SchemaUpdate) {
				attr := s.Attributes["description"]
				if attr.FullTextSearch != true {
					t.Error("expected full_text_search to be true")
				}
			},
		},
		{
			name: "schema with full_text_search config object",
			input: map[string]any{
				"content": map[string]any{
					"type": "string",
					"full_text_search": map[string]any{
						"tokenizer":      "word_v3",
						"case_sensitive": false,
					},
				},
			},
			check: func(t *testing.T, s *SchemaUpdate) {
				attr := s.Attributes["content"]
				if attr.FullTextSearch == nil {
					t.Error("expected full_text_search to be set")
				}
			},
		},
		{
			name: "schema with regex",
			input: map[string]any{
				"pattern": map[string]any{
					"type":  "string",
					"regex": true,
				},
			},
			check: func(t *testing.T, s *SchemaUpdate) {
				attr := s.Attributes["pattern"]
				if attr.Regex == nil || !*attr.Regex {
					t.Error("expected regex to be true")
				}
			},
		},
		{
			name:       "schema not an object",
			input:      "not an object",
			wantErr:    true,
			errContain: "schema must be an object",
		},
		{
			name: "schema attribute not an object",
			input: map[string]any{
				"name": "not an object",
			},
			wantErr:    true,
			errContain: "must be an object",
		},
		{
			name: "invalid attribute name (starts with $)",
			input: map[string]any{
				"$invalid": map[string]any{
					"type": "string",
				},
			},
			wantErr:    true,
			errContain: "cannot start with '$'",
		},
		{
			name: "filterable not a boolean",
			input: map[string]any{
				"name": map[string]any{
					"type":       "string",
					"filterable": "not-a-bool",
				},
			},
			wantErr:    true,
			errContain: "filterable must be a boolean",
		},
		{
			name: "type not a string",
			input: map[string]any{
				"name": map[string]any{
					"type": 123,
				},
			},
			wantErr:    true,
			errContain: "type must be a string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseSchemaUpdate(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContain != "" && !contains(err.Error(), tt.errContain) {
					t.Errorf("expected error containing %q, got %q", tt.errContain, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

// TestParseWriteRequest_Schema verifies schema field in write request parsing.
func TestParseWriteRequest_Schema(t *testing.T) {
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
		"schema": map[string]any{
			"name": map[string]any{
				"type":       "string",
				"filterable": true,
			},
		},
	}

	req, err := ParseWriteRequest("test-req", body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.Schema == nil {
		t.Fatal("expected non-nil schema")
	}
	if len(req.Schema.Attributes) != 1 {
		t.Errorf("expected 1 schema attribute, got %d", len(req.Schema.Attributes))
	}
	if attr, ok := req.Schema.Attributes["name"]; !ok {
		t.Error("expected 'name' in schema")
	} else {
		if attr.Type != "string" {
			t.Errorf("expected type 'string', got %q", attr.Type)
		}
		if attr.Filterable == nil || !*attr.Filterable {
			t.Error("expected filterable to be true")
		}
	}
}

// TestValidateAndConvertSchemaUpdate verifies schema validation and conversion.
func TestValidateAndConvertSchemaUpdate(t *testing.T) {
	trueVal := true
	falseVal := false

	tests := []struct {
		name           string
		update         *SchemaUpdate
		existingSchema *namespace.Schema
		wantErr        bool
		errContain     string
		check          func(*testing.T, *namespace.Schema)
	}{
		{
			name:   "nil update returns nil",
			update: nil,
			check: func(t *testing.T, s *namespace.Schema) {
				if s != nil {
					t.Error("expected nil result")
				}
			},
		},
		{
			name: "new attribute with type",
			update: &SchemaUpdate{
				Attributes: map[string]AttributeSchemaUpdate{
					"name": {Type: "string", Filterable: &trueVal},
				},
			},
			check: func(t *testing.T, s *namespace.Schema) {
				if s == nil {
					t.Fatal("expected non-nil result")
				}
				attr := s.Attributes["name"]
				if attr.Type != "string" {
					t.Errorf("expected type 'string', got %q", attr.Type)
				}
				if attr.Filterable == nil || !*attr.Filterable {
					t.Error("expected filterable true")
				}
			},
		},
		{
			name: "update filterable on existing attribute",
			update: &SchemaUpdate{
				Attributes: map[string]AttributeSchemaUpdate{
					"category": {Filterable: &trueVal},
				},
			},
			existingSchema: &namespace.Schema{
				Attributes: map[string]namespace.AttributeSchema{
					"category": {Type: "string", Filterable: &falseVal},
				},
			},
			check: func(t *testing.T, s *namespace.Schema) {
				attr := s.Attributes["category"]
				if attr.Type != "string" {
					t.Errorf("expected existing type 'string', got %q", attr.Type)
				}
				if attr.Filterable == nil || !*attr.Filterable {
					t.Error("expected updated filterable to be true")
				}
			},
		},
		{
			name: "update full_text_search on existing attribute",
			update: &SchemaUpdate{
				Attributes: map[string]AttributeSchemaUpdate{
					"description": {FullTextSearch: true},
				},
			},
			existingSchema: &namespace.Schema{
				Attributes: map[string]namespace.AttributeSchema{
					"description": {Type: "string"},
				},
			},
			check: func(t *testing.T, s *namespace.Schema) {
				attr := s.Attributes["description"]
				if len(attr.FullTextSearch) == 0 {
					t.Error("expected full_text_search to be set")
				}
			},
		},
		{
			name: "type change rejected",
			update: &SchemaUpdate{
				Attributes: map[string]AttributeSchemaUpdate{
					"age": {Type: "string"}, // Was int
				},
			},
			existingSchema: &namespace.Schema{
				Attributes: map[string]namespace.AttributeSchema{
					"age": {Type: "int"},
				},
			},
			wantErr:    true,
			errContain: "changing attribute type is not allowed",
		},
		{
			name: "same type allowed",
			update: &SchemaUpdate{
				Attributes: map[string]AttributeSchemaUpdate{
					"age": {Type: "int", Filterable: &trueVal},
				},
			},
			existingSchema: &namespace.Schema{
				Attributes: map[string]namespace.AttributeSchema{
					"age": {Type: "int"},
				},
			},
			check: func(t *testing.T, s *namespace.Schema) {
				attr := s.Attributes["age"]
				if attr.Type != "int" {
					t.Errorf("expected type 'int', got %q", attr.Type)
				}
			},
		},
		{
			name: "new attribute requires type",
			update: &SchemaUpdate{
				Attributes: map[string]AttributeSchemaUpdate{
					"newfield": {Filterable: &trueVal}, // No type
				},
			},
			wantErr:    true,
			errContain: "requires type",
		},
		{
			name: "invalid type rejected",
			update: &SchemaUpdate{
				Attributes: map[string]AttributeSchemaUpdate{
					"field": {Type: "invalid_type"},
				},
			},
			wantErr:    true,
			errContain: "invalid type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ValidateAndConvertSchemaUpdate(tt.update, tt.existingSchema)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContain != "" && !contains(err.Error(), tt.errContain) {
					t.Errorf("expected error containing %q, got %q", tt.errContain, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

// TestHandler_SchemaSpecifiedInWriteRequest verifies schema can be specified in write request.
func TestHandler_SchemaSpecifiedInWriteRequest(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-schema-write"

	// Write with explicit schema
	filterableTrue := true
	req := &WriteRequest{
		RequestID: "test-schema-1",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "test", "category": "books"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"name":     {Type: "string", Filterable: &filterableTrue},
				"category": {Type: "string", Filterable: &filterableTrue},
			},
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}

	// Verify schema was applied
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}

	if loaded.State.Schema == nil {
		t.Fatal("expected schema to be set")
	}

	nameAttr, ok := loaded.State.Schema.Attributes["name"]
	if !ok {
		t.Fatal("expected 'name' attribute in schema")
	}
	if nameAttr.Type != "string" {
		t.Errorf("expected name type 'string', got %q", nameAttr.Type)
	}
	if nameAttr.Filterable == nil || !*nameAttr.Filterable {
		t.Error("expected name to be filterable")
	}

	catAttr, ok := loaded.State.Schema.Attributes["category"]
	if !ok {
		t.Fatal("expected 'category' attribute in schema")
	}
	if catAttr.Filterable == nil || !*catAttr.Filterable {
		t.Error("expected category to be filterable")
	}
}

// TestHandler_SchemaChangesAppliedAtomicallyWithWrite verifies schema changes
// are applied atomically with the write.
func TestHandler_SchemaChangesAppliedAtomicallyWithWrite(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-schema-atomic"

	// First write: create initial schema
	filterableTrue := true
	req1 := &WriteRequest{
		RequestID: "test-schema-atomic-1",
		UpsertRows: []map[string]any{
			{"id": 1, "name": "first"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"name": {Type: "string"},
			},
		},
	}

	_, err = handler.Handle(ctx, ns, req1)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Verify first write state
	loaded1, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state after first write: %v", err)
	}

	if loaded1.State.WAL.HeadSeq != 1 {
		t.Errorf("expected WAL seq 1, got %d", loaded1.State.WAL.HeadSeq)
	}

	// Second write: update schema to add filterable
	req2 := &WriteRequest{
		RequestID: "test-schema-atomic-2",
		UpsertRows: []map[string]any{
			{"id": 2, "name": "second"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"name": {Filterable: &filterableTrue},
			},
		},
	}

	_, err = handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	// Verify second write state - both WAL seq and schema should be updated together
	loaded2, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state after second write: %v", err)
	}

	if loaded2.State.WAL.HeadSeq != 2 {
		t.Errorf("expected WAL seq 2, got %d", loaded2.State.WAL.HeadSeq)
	}

	nameAttr := loaded2.State.Schema.Attributes["name"]
	if nameAttr.Type != "string" {
		t.Errorf("expected type 'string', got %q", nameAttr.Type)
	}
	if nameAttr.Filterable == nil || !*nameAttr.Filterable {
		t.Error("expected filterable to be updated to true")
	}
}

// TestHandler_TypeChangeRejectedWith400 verifies that changing attribute type returns 400.
func TestHandler_TypeChangeRejectedWith400(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-type-change"

	// First write: create schema with 'age' as int
	req1 := &WriteRequest{
		RequestID: "type-change-1",
		UpsertRows: []map[string]any{
			{"id": 1, "age": 25},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"age": {Type: "int"},
			},
		},
	}

	_, err = handler.Handle(ctx, ns, req1)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Second write: try to change 'age' type to string
	req2 := &WriteRequest{
		RequestID: "type-change-2",
		UpsertRows: []map[string]any{
			{"id": 2, "age": "thirty"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"age": {Type: "string"}, // Type change should be rejected
			},
		},
	}

	_, err = handler.Handle(ctx, ns, req2)
	if err == nil {
		t.Fatal("expected error for type change, got nil")
	}

	if !errors.Is(err, ErrSchemaTypeChange) {
		t.Errorf("expected ErrSchemaTypeChange, got: %v", err)
	}

	// Verify state wasn't modified by failed write
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	// WAL should still be at seq 1
	if loaded.State.WAL.HeadSeq != 1 {
		t.Errorf("expected WAL seq 1, got %d", loaded.State.WAL.HeadSeq)
	}

	// Age should still be int
	ageAttr := loaded.State.Schema.Attributes["age"]
	if ageAttr.Type != "int" {
		t.Errorf("expected type 'int' unchanged, got %q", ageAttr.Type)
	}
}

// TestHandler_FilterableCanBeUpdated verifies that filterable can be updated on existing attributes.
func TestHandler_FilterableCanBeUpdated(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-filterable-update"

	// First write: create schema with filterable = false
	falseVal := false
	req1 := &WriteRequest{
		RequestID: "filterable-1",
		UpsertRows: []map[string]any{
			{"id": 1, "status": "active"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"status": {Type: "string", Filterable: &falseVal},
			},
		},
	}

	_, err = handler.Handle(ctx, ns, req1)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Verify filterable is false
	loaded1, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	statusAttr := loaded1.State.Schema.Attributes["status"]
	if statusAttr.Filterable == nil || *statusAttr.Filterable {
		t.Error("expected filterable to be false initially")
	}

	// Second write: update filterable to true
	trueVal := true
	req2 := &WriteRequest{
		RequestID: "filterable-2",
		UpsertRows: []map[string]any{
			{"id": 2, "status": "inactive"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"status": {Filterable: &trueVal}, // Just update filterable, keep type
			},
		},
	}

	_, err = handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	// Verify filterable is now true
	loaded2, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	statusAttr = loaded2.State.Schema.Attributes["status"]
	if statusAttr.Filterable == nil || !*statusAttr.Filterable {
		t.Error("expected filterable to be updated to true")
	}
	if statusAttr.Type != "string" {
		t.Errorf("expected type 'string' preserved, got %q", statusAttr.Type)
	}
}

// TestHandler_FullTextSearchCanBeUpdated verifies that full_text_search can be updated.
func TestHandler_FullTextSearchCanBeUpdated(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-fts-update"

	// First write: create schema without FTS
	req1 := &WriteRequest{
		RequestID: "fts-1",
		UpsertRows: []map[string]any{
			{"id": 1, "content": "Hello world"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"content": {Type: "string"},
			},
		},
	}

	_, err = handler.Handle(ctx, ns, req1)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Verify FTS is not set
	loaded1, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	contentAttr := loaded1.State.Schema.Attributes["content"]
	if len(contentAttr.FullTextSearch) > 0 {
		t.Error("expected full_text_search to be empty initially")
	}

	// Second write: enable FTS
	req2 := &WriteRequest{
		RequestID: "fts-2",
		UpsertRows: []map[string]any{
			{"id": 2, "content": "Goodbye world"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"content": {FullTextSearch: true},
			},
		},
	}

	_, err = handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	// Verify FTS is now enabled
	loaded2, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	contentAttr = loaded2.State.Schema.Attributes["content"]
	if len(contentAttr.FullTextSearch) == 0 {
		t.Error("expected full_text_search to be enabled")
	}
}

// TestHandler_SchemaUpdateWithNoData verifies schema can be updated without data changes.
func TestHandler_SchemaUpdateWithNoData(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-schema-only"

	// First write: create initial data and schema
	req1 := &WriteRequest{
		RequestID: "schema-only-1",
		UpsertRows: []map[string]any{
			{"id": 1, "title": "Test"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"title": {Type: "string"},
			},
		},
	}

	_, err = handler.Handle(ctx, ns, req1)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Second write: only schema update, no data
	trueVal := true
	req2 := &WriteRequest{
		RequestID: "schema-only-2",
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"title": {Filterable: &trueVal},
			},
		},
	}

	resp, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("schema-only write failed: %v", err)
	}

	// Should succeed with 0 rows affected
	if resp.RowsAffected != 0 {
		t.Errorf("expected 0 rows affected, got %d", resp.RowsAffected)
	}

	// Verify schema was updated
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	titleAttr := loaded.State.Schema.Attributes["title"]
	if titleAttr.Filterable == nil || !*titleAttr.Filterable {
		t.Error("expected filterable to be updated")
	}
}

// TestValidateDistanceMetric tests the distance metric validation against compat mode.
func TestValidateDistanceMetric(t *testing.T) {
	tests := []struct {
		name       string
		metric     string
		compatMode string
		wantErr    bool
		errType    error
	}{
		// Valid cases
		{"cosine_distance in turbopuffer", "cosine_distance", "turbopuffer", false, nil},
		{"cosine_distance in vex", "cosine_distance", "vex", false, nil},
		{"euclidean_squared in turbopuffer", "euclidean_squared", "turbopuffer", false, nil},
		{"euclidean_squared in vex", "euclidean_squared", "vex", false, nil},
		{"dot_product in vex", "dot_product", "vex", false, nil},

		// Invalid: dot_product in turbopuffer mode
		{"dot_product in turbopuffer", "dot_product", "turbopuffer", true, ErrDotProductNotAllowed},
		{"dot_product with empty compat", "dot_product", "", true, ErrDotProductNotAllowed},

		// Invalid: unknown metric
		{"unknown metric", "unknown", "vex", true, ErrInvalidDistanceMetric},
		{"empty metric", "", "vex", true, ErrInvalidDistanceMetric},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDistanceMetric(tt.metric, tt.compatMode)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateDistanceMetric(%q, %q) error = %v, wantErr %v",
					tt.metric, tt.compatMode, err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errType != nil {
				if !errors.Is(err, tt.errType) {
					t.Errorf("ValidateDistanceMetric(%q, %q) error = %v, want %v",
						tt.metric, tt.compatMode, err, tt.errType)
				}
			}
		})
	}
}

// TestHandler_CompatModeRejectsDotProduct verifies that dot_product is rejected in turbopuffer mode.
func TestHandler_CompatModeRejectsDotProduct(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create handler with turbopuffer compat mode (default)
	handler, err := NewHandlerWithOptions(store, stateMan, nil, "turbopuffer")
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Try to write with dot_product distance metric
	req := &WriteRequest{
		RequestID: "test-req-1",
		UpsertRows: []map[string]any{
			{"id": 1, "vector": []float64{1.0, 2.0, 3.0}},
		},
		DistanceMetric: "dot_product",
	}

	_, err = handler.Handle(ctx, "test-ns", req)
	if err == nil {
		t.Fatal("expected error for dot_product in turbopuffer mode, got nil")
	}
	if !errors.Is(err, ErrDotProductNotAllowed) {
		t.Errorf("expected ErrDotProductNotAllowed, got: %v", err)
	}
}

// TestHandler_VexModeAllowsDotProduct verifies that dot_product is allowed in vex mode.
func TestHandler_VexModeAllowsDotProduct(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	// Create handler with vex compat mode
	handler, err := NewHandlerWithOptions(store, stateMan, nil, "vex")
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Try to write with dot_product distance metric - should succeed
	req := &WriteRequest{
		RequestID: "test-req-1",
		UpsertRows: []map[string]any{
			{"id": 1, "vector": []float64{1.0, 2.0, 3.0}},
		},
		DistanceMetric: "dot_product",
	}

	_, err = handler.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Errorf("expected no error for dot_product in vex mode, got: %v", err)
	}
}

// TestHandler_CompatMode verifies the compat mode getter.
func TestHandler_CompatMode(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	tests := []struct {
		compatMode string
	}{
		{"turbopuffer"},
		{"vex"},
	}

	for _, tt := range tests {
		t.Run(tt.compatMode, func(t *testing.T) {
			handler, err := NewHandlerWithOptions(store, stateMan, nil, tt.compatMode)
			if err != nil {
				t.Fatalf("failed to create handler: %v", err)
			}
			defer handler.Close()

			if handler.CompatMode() != tt.compatMode {
				t.Errorf("Handler.CompatMode() = %q, want %q", handler.CompatMode(), tt.compatMode)
			}
		})
	}
}
