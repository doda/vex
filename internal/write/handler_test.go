package write

import (
	"context"
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
