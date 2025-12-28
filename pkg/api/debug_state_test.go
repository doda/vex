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

// TestDebugStateEndpoint_ReturnsNamespaceState tests that the endpoint returns namespace state.
func TestDebugStateEndpoint_ReturnsNamespaceState(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// First create a namespace by writing to it
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-debug-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(w.Result().Body)
		t.Fatalf("failed to create namespace: %d, body: %s", w.Result().StatusCode, string(respBody))
	}

	// Now call the debug state endpoint with admin auth
	req = httptest.NewRequest("GET", "/_debug/state/test-debug-ns", nil)
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

	// Verify key state fields are present
	if _, ok := result["namespace"]; !ok {
		t.Errorf("expected namespace field in state")
	}
	if _, ok := result["wal"]; !ok {
		t.Errorf("expected wal field in state")
	}
	if _, ok := result["index"]; !ok {
		t.Errorf("expected index field in state")
	}
	if _, ok := result["format_version"]; !ok {
		t.Errorf("expected format_version field in state")
	}
}

// TestDebugStateEndpoint_RequiresAdminAuth tests that the endpoint is gated behind admin auth.
func TestDebugStateEndpoint_RequiresAdminAuth(t *testing.T) {
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
	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns-auth", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Test without any auth header
	req = httptest.NewRequest("GET", "/_debug/state/test-ns-auth", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 without auth header, got %d", w.Result().StatusCode)
	}

	// Test with wrong token
	req = httptest.NewRequest("GET", "/_debug/state/test-ns-auth", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 with wrong token, got %d", w.Result().StatusCode)
	}

	// Test with regular auth token (not admin token)
	cfg.AuthToken = "regular-token"
	req = httptest.NewRequest("GET", "/_debug/state/test-ns-auth", nil)
	req.Header.Set("Authorization", "Bearer regular-token")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 with regular token on admin endpoint, got %d", w.Result().StatusCode)
	}

	// Test with correct admin token
	req = httptest.NewRequest("GET", "/_debug/state/test-ns-auth", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(w.Result().Body)
		t.Errorf("expected 200 with admin token, got %d, body: %s", w.Result().StatusCode, string(respBody))
	}
}

// TestDebugStateEndpoint_NoAdminTokenConfigured tests that the endpoint returns 403 when no admin token is configured.
func TestDebugStateEndpoint_NoAdminTokenConfigured(t *testing.T) {
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
	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns-notoken", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Try to access debug endpoint
	req = httptest.NewRequest("GET", "/_debug/state/test-ns-notoken", nil)
	req.Header.Set("Authorization", "Bearer any-token")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 when no admin token configured, got %d", w.Result().StatusCode)
	}

	respBody, _ := io.ReadAll(w.Result().Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if errMsg, ok := result["error"].(string); !ok || errMsg == "" {
		t.Errorf("expected error message when admin token not configured")
	}
}

// TestDebugStateEndpoint_NamespaceNotFound tests that the endpoint returns 404 for nonexistent namespace.
func TestDebugStateEndpoint_NamespaceNotFound(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	req := httptest.NewRequest("GET", "/_debug/state/nonexistent-ns", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for nonexistent namespace, got %d", w.Result().StatusCode)
	}
}

// TestDebugStateEndpoint_DeletedNamespace tests that the endpoint returns 404 for deleted namespace.
func TestDebugStateEndpoint_DeletedNamespace(t *testing.T) {
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
	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns-delete", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Delete namespace
	req = httptest.NewRequest("DELETE", "/v2/namespaces/test-ns-delete", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to delete namespace: %d", w.Result().StatusCode)
	}

	// Try to get debug state for deleted namespace
	req = httptest.NewRequest("GET", "/_debug/state/test-ns-delete", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for deleted namespace, got %d", w.Result().StatusCode)
	}
}

// TestDebugStateEndpoint_TestMode tests fallback mode when no object store is configured.
func TestDebugStateEndpoint_TestMode(t *testing.T) {
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

	req := httptest.NewRequest("GET", "/_debug/state/test-ns", nil)
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
}

// TestDebugStateEndpoint_InvalidNamespace tests validation of namespace name.
func TestDebugStateEndpoint_InvalidNamespace(t *testing.T) {
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
	req := httptest.NewRequest("GET", "/_debug/state/"+longName, nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid namespace name, got %d", w.Result().StatusCode)
	}
}

// TestDebugStateEndpoint_ReturnsWALState tests that WAL state fields are returned.
func TestDebugStateEndpoint_ReturnsWALState(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace with some data
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
			map[string]any{"id": 2, "name": "test2"},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-wal-state", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Get debug state
	req = httptest.NewRequest("GET", "/_debug/state/test-wal-state", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
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

	// Verify WAL state structure
	walState, ok := result["wal"].(map[string]any)
	if !ok {
		t.Fatalf("expected wal field to be an object")
	}

	// WAL head_seq should be 1 after first write
	if headSeq, ok := walState["head_seq"].(float64); !ok || headSeq < 1 {
		t.Errorf("expected head_seq >= 1, got %v", walState["head_seq"])
	}
}
