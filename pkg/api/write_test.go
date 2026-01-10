package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestWriteAPI_BasicUpsert(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1", "value": 100},
			map[string]any{"id": 2, "name": "test2", "value": 200},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["rows_upserted"] != float64(2) {
		t.Errorf("expected rows_upserted 2, got %v", result["rows_upserted"])
	}
	if result["rows_affected"] != float64(2) {
		t.Errorf("expected rows_affected 2, got %v", result["rows_affected"])
	}
	if result["rows_deleted"] != float64(0) {
		t.Errorf("expected rows_deleted 0, got %v", result["rows_deleted"])
	}
	if result["rows_patched"] != float64(0) {
		t.Errorf("expected rows_patched 0, got %v", result["rows_patched"])
	}
}

func TestWriteAPI_WALConflictRetries(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "wal-conflict-ns"
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

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "winner"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	stateMan := namespace.NewStateManager(store)
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if loaded.State.WAL.HeadSeq != 2 {
		t.Errorf("expected head_seq 2 after retry, got %d", loaded.State.WAL.HeadSeq)
	}
}

func TestWriteAPI_Deletes(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"deletes": []any{1, 2, 3},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["rows_deleted"] != float64(3) {
		t.Errorf("expected rows_deleted 3, got %v", result["rows_deleted"])
	}
	if result["rows_affected"] != float64(3) {
		t.Errorf("expected rows_affected 3, got %v", result["rows_affected"])
	}
}

func TestWriteAPI_ImplicitNamespaceCreation(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "new-namespace"
	stateMan := namespace.NewStateManager(store)

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Verify namespace was created
	loaded, err := stateMan.Load(req.Context(), ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}

	if loaded.State.Namespace != ns {
		t.Errorf("expected namespace %q, got %q", ns, loaded.State.Namespace)
	}
}

func TestWriteAPI_WALCommitToObjectStorage(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "wal-test"

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Verify WAL entry exists in object storage
	walKeyRelative := "wal/00000000000000000001.wal.zst"
	walKeyFull := "vex/namespaces/" + ns + "/" + walKeyRelative
	_, info, err := store.Get(req.Context(), walKeyFull, nil)
	if err != nil {
		t.Fatalf("WAL entry not found in object storage: %v", err)
	}
	if info.Size == 0 {
		t.Error("WAL entry has zero size")
	}

	// Verify namespace state was updated
	stateMan := namespace.NewStateManager(store)
	loaded, err := stateMan.Load(req.Context(), ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}

	if loaded.State.WAL.HeadSeq != 1 {
		t.Errorf("expected WAL head_seq 1, got %d", loaded.State.WAL.HeadSeq)
	}
	if loaded.State.WAL.HeadKey != walKeyRelative {
		t.Errorf("expected WAL head_key %q, got %q", walKeyRelative, loaded.State.WAL.HeadKey)
	}
}

func TestWriteAPI_ResponseFormat(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Verify all required fields are present
	requiredFields := []string{"rows_affected", "rows_upserted", "rows_patched", "rows_deleted"}
	for _, field := range requiredFields {
		if _, ok := result[field]; !ok {
			t.Errorf("missing required field: %s", field)
		}
	}
}

func TestWriteAPI_DuplicateColumnarIDs(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_columns": map[string]any{
			"ids":   []any{1, 2, 1}, // Duplicate ID
			"names": []any{"a", "b", "c"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_InvalidJSON(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader([]byte("not valid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_InvalidNamespace(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	// Test with invalid namespace name (contains $)
	req := httptest.NewRequest("POST", "/v2/namespaces/$invalid", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_VectorSupport(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{
				"id":     1,
				"name":   "test",
				"vector": []any{0.1, 0.2, 0.3, 0.4},
			},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["rows_upserted"] != float64(1) {
		t.Errorf("expected rows_upserted 1, got %v", result["rows_upserted"])
	}
}

func TestWriteAPI_DeletedNamespace(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "deleted-ns"

	// Mark namespace as deleted via test state
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			ns: {Deleted: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_ObjectStoreUnavailable(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Mark object store as unavailable via test state
	router.SetState(&ServerState{
		Namespaces:  map[string]*NamespaceState{},
		ObjectStore: ObjectStoreState{Available: false},
	})

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_Backpressure(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "backpressure-ns"

	// Set high unindexed bytes to trigger backpressure
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			ns: {
				Exists:          true,
				UnindexedBytes:  3 * 1024 * 1024 * 1024, // 3GB > 2GB threshold
				BackpressureOff: false,
			},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected status 429, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_MultipleIDTypes(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "numeric"},
			map[string]any{"id": "550e8400-e29b-41d4-a716-446655440000", "name": "uuid"},
			map[string]any{"id": "string-id", "name": "string"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["rows_upserted"] != float64(3) {
		t.Errorf("expected rows_upserted 3, got %v", result["rows_upserted"])
	}
}

func TestWriteAPI_UpsertColumns(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_columns": map[string]any{
			"ids":   []any{1, 2, 3},
			"name":  []any{"alice", "bob", "charlie"},
			"value": []any{100, 200, 300},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["rows_upserted"] != float64(3) {
		t.Errorf("expected rows_upserted 3, got %v", result["rows_upserted"])
	}
	if result["rows_affected"] != float64(3) {
		t.Errorf("expected rows_affected 3, got %v", result["rows_affected"])
	}
}

func TestWriteAPI_UpsertColumnsWithVectors(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_columns": map[string]any{
			"ids":    []any{1, 2},
			"name":   []any{"vec1", "vec2"},
			"vector": []any{[]any{0.1, 0.2, 0.3}, []any{0.4, 0.5, 0.6}},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["rows_upserted"] != float64(2) {
		t.Errorf("expected rows_upserted 2, got %v", result["rows_upserted"])
	}
}

func TestWriteAPI_UpsertColumnsCombinedWithRows(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "row1"},
		},
		"upsert_columns": map[string]any{
			"ids":  []any{2, 3},
			"name": []any{"col1", "col2"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// 1 from rows + 2 from columns = 3
	if result["rows_upserted"] != float64(3) {
		t.Errorf("expected rows_upserted 3, got %v", result["rows_upserted"])
	}
}

func TestWriteAPI_UpsertColumnsMismatchedLengths(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_columns": map[string]any{
			"ids":  []any{1, 2, 3},
			"name": []any{"a", "b"}, // Only 2 elements, should fail
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_UpsertColumnsMissingIds(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_columns": map[string]any{
			"name":  []any{"a", "b"},
			"value": []any{1, 2},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

// --- Delete-specific tests (write-deletes task) ---

// TestWriteAPI_DeleteNonExistentIDSucceedsSilently verifies that deleting
// a document ID that doesn't exist succeeds without error (silent no-op).
func TestWriteAPI_DeleteNonExistentIDSucceedsSilently(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Delete IDs that have never been created - should succeed silently
	body := map[string]any{
		"deletes": []any{999, 998, 997},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 (silent success for non-existent IDs), got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// The deletes should still be recorded in the WAL
	if result["rows_deleted"] != float64(3) {
		t.Errorf("expected rows_deleted 3, got %v", result["rows_deleted"])
	}
	if result["rows_affected"] != float64(3) {
		t.Errorf("expected rows_affected 3, got %v", result["rows_affected"])
	}
}

// TestWriteAPI_DeleteMixedExistentAndNonExistent verifies that a request
// with both existing and non-existing document IDs succeeds.
func TestWriteAPI_DeleteMixedExistentAndNonExistent(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "delete-mixed-ns"

	// First, create some documents
	createBody := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "doc1"},
			map[string]any{"id": 2, "name": "doc2"},
		},
	}
	createBytes, _ := json.Marshal(createBody)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(createBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create initial documents: %d", w.Result().StatusCode)
	}

	// Now delete a mix of existing (1, 2) and non-existent (999) IDs
	deleteBody := map[string]any{
		"deletes": []any{1, 999, 2},
	}
	deleteBytes, _ := json.Marshal(deleteBody)

	req = httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(deleteBytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// All 3 deletes should be recorded (even the non-existent one)
	if result["rows_deleted"] != float64(3) {
		t.Errorf("expected rows_deleted 3, got %v", result["rows_deleted"])
	}
	if result["rows_affected"] != float64(3) {
		t.Errorf("expected rows_affected 3, got %v", result["rows_affected"])
	}
}

// TestWriteAPI_DeleteWithVariousIDTypes verifies deletes work with different ID types.
func TestWriteAPI_DeleteWithVariousIDTypes(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Delete using various ID types: integer, string, UUID
	body := map[string]any{
		"deletes": []any{
			1,
			"string-id",
			"550e8400-e29b-41d4-a716-446655440000",
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["rows_deleted"] != float64(3) {
		t.Errorf("expected rows_deleted 3, got %v", result["rows_deleted"])
	}
}

// TestWriteAPI_DeleteEmptyArray verifies empty deletes array is allowed.
func TestWriteAPI_DeleteEmptyArray(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"deletes": []any{},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["rows_deleted"] != float64(0) {
		t.Errorf("expected rows_deleted 0, got %v", result["rows_deleted"])
	}
	if result["rows_affected"] != float64(0) {
		t.Errorf("expected rows_affected 0, got %v", result["rows_affected"])
	}
}

// --- Schema Updates Tests (write-schema-updates task) ---

// TestWriteAPI_SchemaSpecifiedInWriteRequest verifies schema can be specified in write request.
func TestWriteAPI_SchemaSpecifiedInWriteRequest(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "schema-write-ns"

	// Write with explicit schema
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test", "category": "books"},
		},
		"schema": map[string]any{
			"name":     map[string]any{"type": "string", "filterable": true},
			"category": map[string]any{"type": "string", "filterable": true},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 200, got %d: %s", resp.StatusCode, string(respBody))
	}

	// Verify schema was applied
	stateMan := namespace.NewStateManager(store)
	loaded, err := stateMan.Load(req.Context(), ns)
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
}

// TestWriteAPI_SchemaChangesAppliedAtomically verifies schema changes are applied atomically.
func TestWriteAPI_SchemaChangesAppliedAtomically(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "schema-atomic-ns"

	// First write: create initial schema
	body1 := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "first"},
		},
		"schema": map[string]any{
			"name": map[string]any{"type": "string"},
		},
	}
	body1Bytes, _ := json.Marshal(body1)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(body1Bytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("first write failed: %d", w.Result().StatusCode)
	}

	// Second write: update schema to add filterable
	body2 := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 2, "name": "second"},
		},
		"schema": map[string]any{
			"name": map[string]any{"filterable": true},
		},
	}
	body2Bytes, _ := json.Marshal(body2)

	req = httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(body2Bytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("second write failed: %d", w.Result().StatusCode)
	}

	// Verify both WAL and schema were updated atomically
	stateMan := namespace.NewStateManager(store)
	loaded, err := stateMan.Load(req.Context(), ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if loaded.State.WAL.HeadSeq != 2 {
		t.Errorf("expected WAL seq 2, got %d", loaded.State.WAL.HeadSeq)
	}

	nameAttr := loaded.State.Schema.Attributes["name"]
	if nameAttr.Filterable == nil || !*nameAttr.Filterable {
		t.Error("expected filterable to be updated to true")
	}
}

// TestWriteAPI_TypeChangeReturns400 verifies that changing attribute type returns 400.
func TestWriteAPI_TypeChangeReturns400(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "type-change-ns"

	// First write: create schema with 'age' as int
	body1 := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "age": 25},
		},
		"schema": map[string]any{
			"age": map[string]any{"type": "int"},
		},
	}
	body1Bytes, _ := json.Marshal(body1)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(body1Bytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("first write failed: %d", w.Result().StatusCode)
	}

	// Second write: try to change 'age' type to string
	body2 := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 2, "age": "thirty"},
		},
		"schema": map[string]any{
			"age": map[string]any{"type": "string"}, // Type change should be rejected
		},
	}
	body2Bytes, _ := json.Marshal(body2)

	req = httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(body2Bytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400 for type change, got %d", resp.StatusCode)
	}

	// Verify error message mentions type change
	respBody, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(respBody, []byte("type")) && !bytes.Contains(respBody, []byte("changing")) {
		t.Errorf("expected error message about type change, got: %s", string(respBody))
	}
}

// TestWriteAPI_FilterableCanBeUpdated verifies filterable can be updated on existing attributes.
func TestWriteAPI_FilterableCanBeUpdated(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "filterable-update-ns"

	// First write: create schema with filterable = false
	body1 := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "status": "active"},
		},
		"schema": map[string]any{
			"status": map[string]any{"type": "string", "filterable": false},
		},
	}
	body1Bytes, _ := json.Marshal(body1)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(body1Bytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("first write failed: %d", w.Result().StatusCode)
	}

	// Second write: update filterable to true
	body2 := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 2, "status": "inactive"},
		},
		"schema": map[string]any{
			"status": map[string]any{"filterable": true}, // Just update filterable
		},
	}
	body2Bytes, _ := json.Marshal(body2)

	req = httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(body2Bytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Verify filterable was updated
	stateMan := namespace.NewStateManager(store)
	loaded, err := stateMan.Load(req.Context(), ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	statusAttr := loaded.State.Schema.Attributes["status"]
	if statusAttr.Filterable == nil || !*statusAttr.Filterable {
		t.Error("expected filterable to be updated to true")
	}
	if statusAttr.Type != "string" {
		t.Errorf("expected type 'string' preserved, got %q", statusAttr.Type)
	}
}

// TestWriteAPI_FullTextSearchCanBeUpdated verifies full_text_search can be updated.
func TestWriteAPI_FullTextSearchCanBeUpdated(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	ns := "fts-update-ns"

	// First write: create schema without FTS
	body1 := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "content": "Hello world"},
		},
		"schema": map[string]any{
			"content": map[string]any{"type": "string"},
		},
	}
	body1Bytes, _ := json.Marshal(body1)

	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(body1Bytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("first write failed: %d", w.Result().StatusCode)
	}

	// Second write: enable FTS
	body2 := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 2, "content": "Goodbye world"},
		},
		"schema": map[string]any{
			"content": map[string]any{"full_text_search": true},
		},
	}
	body2Bytes, _ := json.Marshal(body2)

	req = httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(body2Bytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Verify FTS was enabled
	stateMan := namespace.NewStateManager(store)
	loaded, err := stateMan.Load(req.Context(), ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	contentAttr := loaded.State.Schema.Attributes["content"]
	if len(contentAttr.FullTextSearch) == 0 {
		t.Error("expected full_text_search to be enabled")
	}
}
