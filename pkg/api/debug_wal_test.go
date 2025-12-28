package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestDebugWalEndpoint_ReturnsWalEntries tests that the endpoint returns WAL entries from from_seq.
func TestDebugWalEndpoint_ReturnsWalEntries(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// First create a namespace by writing to it
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1"},
			map[string]any{"id": 2, "name": "test2"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-wal-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(w.Result().Body)
		t.Fatalf("failed to create namespace: %d, body: %s", w.Result().StatusCode, string(respBody))
	}

	// Now call the debug WAL endpoint with admin auth
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-ns", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 200, got %d, body: %s", resp.StatusCode, string(respBody))
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Verify key fields are present
	if _, ok := result["namespace"]; !ok {
		t.Errorf("expected namespace field in response")
	}
	if _, ok := result["entries"]; !ok {
		t.Errorf("expected entries field in response")
	}
	if _, ok := result["head_seq"]; !ok {
		t.Errorf("expected head_seq field in response")
	}

	// Verify entries is an array with at least one entry
	entries, ok := result["entries"].([]interface{})
	if !ok {
		t.Fatalf("expected entries to be an array")
	}
	if len(entries) < 1 {
		t.Errorf("expected at least 1 WAL entry, got %d", len(entries))
	}

	// Verify the entry has expected fields
	entry, ok := entries[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected entry to be an object")
	}
	if _, ok := entry["seq"]; !ok {
		t.Errorf("expected seq field in WAL entry")
	}
	if _, ok := entry["namespace"]; !ok {
		t.Errorf("expected namespace field in WAL entry")
	}
	if _, ok := entry["sub_batches"]; !ok {
		t.Errorf("expected sub_batches field in WAL entry")
	}
}

// TestDebugWalEndpoint_FromSeq tests that from_seq parameter works correctly.
func TestDebugWalEndpoint_FromSeq(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace with initial data
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "first"},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-from-seq", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace")
	}

	// Get WAL entries with from_seq=1 (should get entry)
	req = httptest.NewRequest("GET", "/_debug/wal/test-from-seq?from_seq=1", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	json.Unmarshal(respBody, &result)

	entries, _ := result["entries"].([]interface{})
	if len(entries) != 1 {
		t.Errorf("expected 1 entry from from_seq=1, got %d", len(entries))
	}

	// Get WAL entries with from_seq=999 (should get empty)
	req = httptest.NewRequest("GET", "/_debug/wal/test-from-seq?from_seq=999", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp = w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	respBody, _ = io.ReadAll(resp.Body)
	json.Unmarshal(respBody, &result)

	entries, _ = result["entries"].([]interface{})
	if len(entries) != 0 {
		t.Errorf("expected 0 entries from from_seq=999, got %d", len(entries))
	}
}

// TestDebugWalEndpoint_RequiresAdminAuth tests that the endpoint is gated behind admin auth.
func TestDebugWalEndpoint_RequiresAdminAuth(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace first
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-wal-auth", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Test without any auth header
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-auth", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 without auth header, got %d", w.Result().StatusCode)
	}

	// Test with wrong token
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-auth", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 with wrong token, got %d", w.Result().StatusCode)
	}

	// Test with regular auth token (not admin token)
	cfg.AuthToken = "regular-token"
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-auth", nil)
	req.Header.Set("Authorization", "Bearer regular-token")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 with regular token on admin endpoint, got %d", w.Result().StatusCode)
	}

	// Test with correct admin token
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-auth", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(w.Result().Body)
		t.Errorf("expected 200 with admin token, got %d, body: %s", w.Result().StatusCode, string(respBody))
	}
}

// TestDebugWalEndpoint_NoAdminTokenConfigured tests that the endpoint returns 403 when no admin token is configured.
func TestDebugWalEndpoint_NoAdminTokenConfigured(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "" // No admin token configured
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace first
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-wal-notoken", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Try to access debug endpoint
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-notoken", nil)
	req.Header.Set("Authorization", "Bearer any-token")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 when no admin token configured, got %d", w.Result().StatusCode)
	}
}

// TestDebugWalEndpoint_NamespaceNotFound tests that the endpoint returns 404 for nonexistent namespace.
func TestDebugWalEndpoint_NamespaceNotFound(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	req := httptest.NewRequest("GET", "/_debug/wal/nonexistent-ns", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for nonexistent namespace, got %d", w.Result().StatusCode)
	}
}

// TestDebugWalEndpoint_DeletedNamespace tests that the endpoint returns 404 for deleted namespace.
func TestDebugWalEndpoint_DeletedNamespace(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-wal-delete", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Delete namespace
	req = httptest.NewRequest("DELETE", "/v2/namespaces/test-wal-delete", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to delete namespace: %d", w.Result().StatusCode)
	}

	// Try to get debug WAL for deleted namespace
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-delete", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for deleted namespace, got %d", w.Result().StatusCode)
	}
}

// TestDebugWalEndpoint_InvalidFromSeq tests validation of from_seq parameter.
func TestDebugWalEndpoint_InvalidFromSeq(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-wal-invalid", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Test with non-numeric from_seq
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-invalid?from_seq=abc", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for non-numeric from_seq, got %d", w.Result().StatusCode)
	}

	// Test with from_seq=0 (must be at least 1)
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-invalid?from_seq=0", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for from_seq=0, got %d", w.Result().StatusCode)
	}
}

// TestDebugWalEndpoint_LimitParameter tests the limit parameter.
func TestDebugWalEndpoint_LimitParameter(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace with data
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-wal-limit", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Test with valid limit
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-limit?limit=10", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("expected 200 with valid limit, got %d", w.Result().StatusCode)
	}

	// Test with invalid limit (too large)
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-limit?limit=10000", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for limit > 1000, got %d", w.Result().StatusCode)
	}

	// Test with invalid limit (zero)
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-limit?limit=0", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for limit=0, got %d", w.Result().StatusCode)
	}
}

// TestDebugWalEndpoint_TestMode tests fallback mode when no object store is configured.
func TestDebugWalEndpoint_TestMode(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	router := NewRouter(cfg)
	defer router.Close()

	// Set up test state with an existing namespace
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	req := httptest.NewRequest("GET", "/_debug/wal/test-ns", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// In test mode, should indicate it's in test mode
	if testMode, ok := result["test_mode"].(bool); !ok || !testMode {
		t.Errorf("expected test_mode=true in response")
	}
	if ns, ok := result["namespace"].(string); !ok || ns != "test-ns" {
		t.Errorf("expected namespace=test-ns in response")
	}
	// Test mode should return empty entries
	entries, _ := result["entries"].([]interface{})
	if len(entries) != 0 {
		t.Errorf("expected empty entries in test mode, got %d", len(entries))
	}
}

// TestDebugWalEndpoint_InvalidNamespace tests validation of namespace name.
func TestDebugWalEndpoint_InvalidNamespace(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Test with invalid namespace name (too long)
	longName := ""
	for i := 0; i < 150; i++ {
		longName += "a"
	}
	req := httptest.NewRequest("GET", "/_debug/wal/"+longName, nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid namespace name, got %d", w.Result().StatusCode)
	}
}

// TestDebugWalEndpoint_WalEntryStructure tests that WAL entries have the expected structure.
func TestDebugWalEndpoint_WalEntryStructure(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace with specific data
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "Alice", "age": 30},
			map[string]any{"id": 2, "name": "Bob", "age": 25},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-wal-structure", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace")
	}

	// Get WAL entries
	req = httptest.NewRequest("GET", "/_debug/wal/test-wal-structure", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	json.Unmarshal(respBody, &result)

	entries, _ := result["entries"].([]interface{})
	if len(entries) < 1 {
		t.Fatalf("expected at least 1 entry")
	}

	entry := entries[0].(map[string]interface{})

	// Verify seq is present and correct
	if seq, ok := entry["seq"].(float64); !ok || seq != 1 {
		t.Errorf("expected seq=1, got %v", entry["seq"])
	}

	// Verify namespace matches
	if ns, ok := entry["namespace"].(string); !ok || ns != "test-wal-structure" {
		t.Errorf("expected namespace=test-wal-structure, got %v", entry["namespace"])
	}

	// Verify sub_batches are present
	subBatches, ok := entry["sub_batches"].([]interface{})
	if !ok || len(subBatches) < 1 {
		t.Fatalf("expected sub_batches with at least 1 batch")
	}

	batch := subBatches[0].(map[string]interface{})

	// Verify mutations are present
	mutations, ok := batch["mutations"].([]interface{})
	if !ok || len(mutations) < 2 {
		t.Fatalf("expected at least 2 mutations")
	}

	// Verify first mutation
	mut := mutations[0].(map[string]interface{})
	if mutType, ok := mut["type"].(string); !ok || mutType != "MUTATION_TYPE_UPSERT" {
		t.Errorf("expected mutation type MUTATION_TYPE_UPSERT, got %v", mut["type"])
	}

	// Verify stats are present
	stats, ok := batch["stats"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected stats in batch")
	}
	if upserted, ok := stats["rows_upserted"].(float64); !ok || upserted < 2 {
		t.Errorf("expected rows_upserted >= 2, got %v", stats["rows_upserted"])
	}
}
