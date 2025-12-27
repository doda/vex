package write

import (
	"context"
	"errors"
	"testing"

	"github.com/vexsearch/vex/internal/namespace"
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
	walKey := "wal/1.wal.zst"
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
	if loaded.State.WAL.HeadKey != walKey {
		t.Errorf("expected WAL head_key %q, got %q", walKey, loaded.State.WAL.HeadKey)
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
				"id":       1,
				"name":     "test",           // string
				"count":    100,              // int
				"price":    19.99,            // float
				"active":   true,             // bool
				"tags":     []any{"a", "b"},  // string array
				"vector":   []any{0.1, 0.2},  // vector
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
				"id":       2,
				"name":     "test2",
				"count":    200,
				"price":    29.99,
				"active":   false,
				"tags":     []any{"c", "d"},
				"vector":   []any{0.3, 0.4},
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
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-ns"

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
	handler, err := NewHandler(store, stateMan)
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

	// The patch is recorded in the WAL, even if the doc doesn't exist
	// (at apply/query time it will be a no-op)
	if resp.RowsPatched != 1 {
		t.Errorf("expected 1 row patched (recorded), got %d", resp.RowsPatched)
	}
	if resp.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", resp.RowsAffected)
	}
}

func TestHandler_VectorCannotBePatched(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
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
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-dupe-ns"

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
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-patch-normalized-ns"

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
	handler, err := NewHandler(store, stateMan)
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
			{"id": 2, "name": "patched"},
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
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

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
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

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
