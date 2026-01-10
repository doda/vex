package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestDeleteNamespace_Returns200OnSuccess tests that DELETE endpoint returns 200 on success.
func TestDeleteNamespace_Returns200OnSuccess(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// First create a namespace by writing to it
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Now delete the namespace
	req = httptest.NewRequest("DELETE", "/v2/namespaces/test-ns", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Errorf("expected status 200, got %d: %s", resp.StatusCode, string(respBody))
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["status"] != "ok" {
		t.Errorf("expected status 'ok', got %v", result["status"])
	}
}

// TestDeleteNamespace_WritesTombstone tests that tombstone.json is written with deletion timestamp.
func TestDeleteNamespace_WritesTombstone(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/tombstone-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Delete the namespace
	req = httptest.NewRequest("DELETE", "/v2/namespaces/tombstone-ns", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("delete failed: %d", w.Result().StatusCode)
	}

	// Verify tombstone.json exists
	tombstoneKey := namespace.TombstoneKey("tombstone-ns")
	reader, _, err := store.Get(context.Background(), tombstoneKey, nil)
	if err != nil {
		t.Fatalf("expected tombstone.json to exist: %v", err)
	}
	defer reader.Close()

	// Parse tombstone and verify deleted_at is set
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read tombstone: %v", err)
	}

	var tombstone namespace.Tombstone
	if err := json.Unmarshal(data, &tombstone); err != nil {
		t.Fatalf("failed to parse tombstone: %v", err)
	}

	if tombstone.DeletedAt.IsZero() {
		t.Error("expected deleted_at to be set in tombstone")
	}
}

func TestDeleteNamespace_PurgeRemovesObjects(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/purge-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	ctx := context.Background()
	extraKey := "vex/namespaces/purge-ns/index/segments/seg_extra/docs.col"
	_, _ = store.Put(ctx, extraKey, bytes.NewReader([]byte("data")), int64(len("data")), nil)

	req = httptest.NewRequest("DELETE", "/v2/namespaces/purge-ns?purge=1", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("purge failed: %d", w.Result().StatusCode)
	}

	if _, err := store.Head(ctx, extraKey); !objectstore.IsNotFoundError(err) {
		t.Fatalf("expected extra object to be deleted, got %v", err)
	}

	if _, err := store.Head(ctx, namespace.StateKey("purge-ns")); err != nil {
		t.Fatalf("expected state.json to remain, got %v", err)
	}
	if _, err := store.Head(ctx, namespace.TombstoneKey("purge-ns")); err != nil {
		t.Fatalf("expected tombstone.json to remain, got %v", err)
	}

	if _, err := store.Head(ctx, namespace.CatalogKey("purge-ns")); !objectstore.IsNotFoundError(err) {
		t.Fatalf("expected catalog entry to be deleted, got %v", err)
	}
}

// TestDeleteNamespace_UpdatesStateWithTombstoned tests that state.json is updated with tombstoned=true.
func TestDeleteNamespace_UpdatesStateWithTombstoned(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/state-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Delete the namespace
	req = httptest.NewRequest("DELETE", "/v2/namespaces/state-ns", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("delete failed: %d", w.Result().StatusCode)
	}

	// Read state.json directly from store (bypassing manager which would reject tombstoned)
	stateKey := namespace.StateKey("state-ns")
	reader, _, err := store.Get(context.Background(), stateKey, nil)
	if err != nil {
		t.Fatalf("expected state.json to exist: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read state: %v", err)
	}

	var state namespace.State
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("failed to parse state: %v", err)
	}

	if !state.Deletion.Tombstoned {
		t.Error("expected state.deletion.tombstoned to be true")
	}

	if state.Deletion.TombstonedAt == nil {
		t.Error("expected state.deletion.tombstoned_at to be set")
	}
}

// TestDeleteNamespace_SubsequentReadsReturn404 tests that reads return 404 after deletion.
func TestDeleteNamespace_SubsequentReadsReturn404(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/read-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Delete the namespace
	req = httptest.NewRequest("DELETE", "/v2/namespaces/read-ns", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("delete failed: %d", w.Result().StatusCode)
	}

	// Try to get metadata - should return 404
	req = httptest.NewRequest("GET", "/v1/namespaces/read-ns/metadata", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404 for metadata, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Verify error message indicates namespace was deleted
	errMsg, ok := result["error"].(string)
	if !ok || errMsg == "" {
		t.Errorf("expected error message for deleted namespace")
	}
}

// TestDeleteNamespace_SubsequentWritesReturn404 tests that writes return 404 after deletion.
func TestDeleteNamespace_SubsequentWritesReturn404(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/write-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Delete the namespace
	req = httptest.NewRequest("DELETE", "/v2/namespaces/write-ns", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("delete failed: %d", w.Result().StatusCode)
	}

	// Try to write to the deleted namespace - should return 404
	body = map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 2, "name": "test2"},
		},
	}
	bodyBytes, _ = json.Marshal(body)

	req = httptest.NewRequest("POST", "/v2/namespaces/write-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		respBody, _ := io.ReadAll(resp.Body)
		t.Errorf("expected status 404 for write, got %d: %s", resp.StatusCode, string(respBody))
	}
}

// TestDeleteNamespace_SubsequentQueriesReturn404 tests that queries return 404 after deletion.
func TestDeleteNamespace_SubsequentQueriesReturn404(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/query-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Delete the namespace
	req = httptest.NewRequest("DELETE", "/v2/namespaces/query-ns", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("delete failed: %d", w.Result().StatusCode)
	}

	// Try to query the deleted namespace - should return 404
	queryBody := map[string]any{
		"rank_by": []any{"id", "asc"},
	}
	queryBytes, _ := json.Marshal(queryBody)

	req = httptest.NewRequest("POST", "/v2/namespaces/query-ns/query", bytes.NewReader(queryBytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		respBody, _ := io.ReadAll(resp.Body)
		t.Errorf("expected status 404 for query, got %d: %s", resp.StatusCode, string(respBody))
	}
}

// TestDeleteNamespace_DeletionIsIrreversible tests that a deleted namespace cannot be un-deleted.
func TestDeleteNamespace_DeletionIsIrreversible(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/irrev-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Delete the namespace
	req = httptest.NewRequest("DELETE", "/v2/namespaces/irrev-ns", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("delete failed: %d", w.Result().StatusCode)
	}

	// Try to delete again - should return 404 (already deleted)
	req = httptest.NewRequest("DELETE", "/v2/namespaces/irrev-ns", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404 for second delete, got %d", resp.StatusCode)
	}

	// The namespace should remain deleted even after attempting to write to it
	body = map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 2, "name": "test2"},
		},
	}
	bodyBytes, _ = json.Marshal(body)

	req = httptest.NewRequest("POST", "/v2/namespaces/irrev-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp = w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404 for write after delete, got %d", resp.StatusCode)
	}
}

// TestDeleteNamespace_NonExistentNamespace tests that deleting a non-existent namespace returns 404.
func TestDeleteNamespace_NonExistentNamespace(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Try to delete a namespace that doesn't exist
	req := httptest.NewRequest("DELETE", "/v2/namespaces/nonexistent-ns", nil)
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

// TestDeleteNamespace_InvalidNamespace tests that invalid namespace names return 400.
func TestDeleteNamespace_InvalidNamespace(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Try to delete a namespace with invalid name (contains invalid characters)
	req := httptest.NewRequest("DELETE", "/v2/namespaces/invalid!namespace", nil)
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

// TestDeleteNamespace_ObjectStoreUnavailable tests 503 when object store is unavailable.
func TestDeleteNamespace_ObjectStoreUnavailable(t *testing.T) {
	cfg := testConfig()
	router := NewRouter(cfg)

	// Set object store as unavailable
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: false},
	})

	req := httptest.NewRequest("DELETE", "/v2/namespaces/test-ns", nil)
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", resp.StatusCode)
	}
}

// TestDeleteNamespace_ObjectStoreFailure tests 503 when object store delete fails.
func TestDeleteNamespace_ObjectStoreFailure(t *testing.T) {
	cfg := testConfig()
	store := newFailingStore(objectstore.NewMemoryStore(), errors.New("object store failure"))
	store.failGet = true

	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	req := httptest.NewRequest("DELETE", "/v2/namespaces/test-ns", nil)
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	msg := parseErrorResponse(t, w.Result(), http.StatusServiceUnavailable)
	if !strings.Contains(msg, "object store") {
		t.Errorf("expected error to mention object store, got %q", msg)
	}
}

// TestDeleteNamespace_FallbackMode tests fallback behavior when no store is configured.
func TestDeleteNamespace_FallbackMode(t *testing.T) {
	cfg := testConfig()
	router := NewRouter(cfg)

	// Set up a namespace that exists (fallback mode)
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"fallback-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	// Delete the namespace
	req := httptest.NewRequest("DELETE", "/v2/namespaces/fallback-ns", nil)
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Verify that subsequent operations return 404
	req = httptest.NewRequest("DELETE", "/v2/namespaces/fallback-ns", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp = w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404 for second delete, got %d", resp.StatusCode)
	}
}

// TestDeleteNamespace_HintCacheWarmReturns404 tests that hint_cache_warm returns 404 after deletion.
func TestDeleteNamespace_HintCacheWarmReturns404(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/warm-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Delete the namespace
	req = httptest.NewRequest("DELETE", "/v2/namespaces/warm-ns", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("delete failed: %d", w.Result().StatusCode)
	}

	// Try to warm cache for deleted namespace - should return 404
	req = httptest.NewRequest("GET", "/v1/namespaces/warm-ns/hint_cache_warm", nil)
	w = httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404 for hint_cache_warm, got %d", resp.StatusCode)
	}
}
