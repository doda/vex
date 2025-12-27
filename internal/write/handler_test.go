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
