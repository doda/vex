package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestWriteAPI_BasicUpsert(t *testing.T) {
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

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

func TestWriteAPI_Deletes(t *testing.T) {
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

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
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

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
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Verify WAL entry exists in object storage
	walKey := "wal/1.wal.zst"
	_, info, err := store.Get(req.Context(), walKey, nil)
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
	if loaded.State.WAL.HeadKey != walKey {
		t.Errorf("expected WAL head_key %q, got %q", walKey, loaded.State.WAL.HeadKey)
	}
}

func TestWriteAPI_ResponseFormat(t *testing.T) {
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

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
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_InvalidJSON(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader([]byte("not valid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_InvalidNamespace(t *testing.T) {
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_VectorSupport(t *testing.T) {
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

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
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_ObjectStoreUnavailable(t *testing.T) {
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_Backpressure(t *testing.T) {
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected status 429, got %d", resp.StatusCode)
	}
}

func TestWriteAPI_MultipleIDTypes(t *testing.T) {
	cfg := config.Default()
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

	router.ServeHTTP(w, req)

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
