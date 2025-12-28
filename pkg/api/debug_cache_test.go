package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestDebugCacheEndpoint_ReturnsCacheStatus tests that the endpoint returns cache status for namespace.
func TestDebugCacheEndpoint_ReturnsCacheStatus(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Set up disk cache
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: t.TempDir(),
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}
	router.SetDiskCache(diskCache)

	// Set up RAM cache
	ramCache := cache.NewMemoryCache(cache.MemoryCacheConfig{
		MaxBytes: 1024 * 1024,
	})
	router.SetRAMCache(ramCache)

	// First create a namespace by writing to it
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-cache-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(w.Result().Body)
		t.Fatalf("failed to create namespace: %d, body: %s", w.Result().StatusCode, string(respBody))
	}

	// Now call the debug cache endpoint with admin auth
	req = httptest.NewRequest("GET", "/_debug/cache/test-cache-ns", nil)
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

	// Verify key cache fields are present
	if _, ok := result["namespace"]; !ok {
		t.Errorf("expected namespace field in response")
	}
	if result["namespace"] != "test-cache-ns" {
		t.Errorf("expected namespace=test-cache-ns, got %v", result["namespace"])
	}
	if _, ok := result["disk_cache"]; !ok {
		t.Errorf("expected disk_cache field in response")
	}
	if _, ok := result["ram_cache"]; !ok {
		t.Errorf("expected ram_cache field in response")
	}

	// Verify disk_cache structure
	diskCacheStatus, ok := result["disk_cache"].(map[string]any)
	if !ok {
		t.Fatalf("expected disk_cache to be an object")
	}
	if _, ok := diskCacheStatus["used_bytes"]; !ok {
		t.Errorf("expected used_bytes in disk_cache")
	}
	if _, ok := diskCacheStatus["max_bytes"]; !ok {
		t.Errorf("expected max_bytes in disk_cache")
	}
	if _, ok := diskCacheStatus["entry_count"]; !ok {
		t.Errorf("expected entry_count in disk_cache")
	}

	// Verify ram_cache structure
	ramCacheStatus, ok := result["ram_cache"].(map[string]any)
	if !ok {
		t.Fatalf("expected ram_cache to be an object")
	}
	if _, ok := ramCacheStatus["namespace_bytes"]; !ok {
		t.Errorf("expected namespace_bytes in ram_cache")
	}
	if _, ok := ramCacheStatus["hit_ratio"]; !ok {
		t.Errorf("expected hit_ratio in ram_cache")
	}
}

// TestDebugCacheEndpoint_RequiresAdminAuth tests that the endpoint is gated behind admin auth.
func TestDebugCacheEndpoint_RequiresAdminAuth(t *testing.T) {
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
	req = httptest.NewRequest("GET", "/_debug/cache/test-ns-auth", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 without auth header, got %d", w.Result().StatusCode)
	}

	// Test with wrong token
	req = httptest.NewRequest("GET", "/_debug/cache/test-ns-auth", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 with wrong token, got %d", w.Result().StatusCode)
	}

	// Test with regular auth token (not admin token)
	cfg.AuthToken = "regular-token"
	req = httptest.NewRequest("GET", "/_debug/cache/test-ns-auth", nil)
	req.Header.Set("Authorization", "Bearer regular-token")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 with regular token on admin endpoint, got %d", w.Result().StatusCode)
	}

	// Test with correct admin token
	req = httptest.NewRequest("GET", "/_debug/cache/test-ns-auth", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(w.Result().Body)
		t.Errorf("expected 200 with admin token, got %d, body: %s", w.Result().StatusCode, string(respBody))
	}
}

// TestDebugCacheEndpoint_NoAdminTokenConfigured tests that the endpoint returns 403 when no admin token is configured.
func TestDebugCacheEndpoint_NoAdminTokenConfigured(t *testing.T) {
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
	req = httptest.NewRequest("GET", "/_debug/cache/test-ns-notoken", nil)
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

// TestDebugCacheEndpoint_NamespaceNotFound tests that the endpoint returns 404 for nonexistent namespace.
func TestDebugCacheEndpoint_NamespaceNotFound(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	req := httptest.NewRequest("GET", "/_debug/cache/nonexistent-ns", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for nonexistent namespace, got %d", w.Result().StatusCode)
	}
}

// TestDebugCacheEndpoint_DeletedNamespace tests that the endpoint returns 404 for deleted namespace.
func TestDebugCacheEndpoint_DeletedNamespace(t *testing.T) {
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

	// Try to get cache status for deleted namespace
	req = httptest.NewRequest("GET", "/_debug/cache/test-ns-delete", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for deleted namespace, got %d", w.Result().StatusCode)
	}
}

// TestDebugCacheEndpoint_TestMode tests fallback mode when no caches are configured.
func TestDebugCacheEndpoint_TestMode(t *testing.T) {
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

	req := httptest.NewRequest("GET", "/_debug/cache/test-ns", nil)
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

	// In test mode (no caches configured), should indicate it's in test mode
	if testMode, ok := result["test_mode"].(bool); !ok || !testMode {
		t.Errorf("expected test_mode=true in response when no caches configured")
	}
	if ns, ok := result["namespace"].(string); !ok || ns != "test-ns" {
		t.Errorf("expected namespace=test-ns in response")
	}
}

// TestDebugCacheEndpoint_InvalidNamespace tests validation of namespace name.
func TestDebugCacheEndpoint_InvalidNamespace(t *testing.T) {
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
	req := httptest.NewRequest("GET", "/_debug/cache/"+longName, nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid namespace name, got %d", w.Result().StatusCode)
	}
}

// TestDebugCacheEndpoint_WithCachedData tests cache stats with actual cached data.
func TestDebugCacheEndpoint_WithCachedData(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Set up disk cache
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: t.TempDir(),
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}
	router.SetDiskCache(diskCache)

	// Set up RAM cache
	ramCache := cache.NewMemoryCache(cache.MemoryCacheConfig{
		MaxBytes: 1024 * 1024,
	})
	router.SetRAMCache(ramCache)

	// Create namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-cached-ns", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Put some data in the RAM cache for this namespace
	testData := []byte("test cache data")
	ramCache.Put(cache.MemoryCacheKey{
		Namespace: "test-cached-ns",
		ShardID:   "test-shard",
		ItemID:    "test-item",
		ItemType:  cache.TypeCentroid,
	}, testData)

	// Get cache status
	req = httptest.NewRequest("GET", "/_debug/cache/test-cached-ns", nil)
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

	// Verify RAM cache shows the namespace bytes
	ramCacheStatus, ok := result["ram_cache"].(map[string]any)
	if !ok {
		t.Fatalf("expected ram_cache to be an object")
	}
	nsBytes, ok := ramCacheStatus["namespace_bytes"].(float64)
	if !ok {
		t.Fatalf("expected namespace_bytes to be a number")
	}
	if nsBytes != float64(len(testData)) {
		t.Errorf("expected namespace_bytes=%d, got %v", len(testData), nsBytes)
	}
}

// TestDebugCacheEndpoint_PinnedNamespace tests that pinned status is returned.
func TestDebugCacheEndpoint_PinnedNamespace(t *testing.T) {
	cfg := config.Default()
	cfg.AdminToken = "admin-secret"
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Set up disk cache
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: t.TempDir(),
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}
	router.SetDiskCache(diskCache)

	// Create namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/pinned-ns", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Pin the namespace
	diskCache.Pin("vex/namespaces/pinned-ns/")

	// Get cache status
	req = httptest.NewRequest("GET", "/_debug/cache/pinned-ns", nil)
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

	diskCacheStatus, ok := result["disk_cache"].(map[string]any)
	if !ok {
		t.Fatalf("expected disk_cache to be an object")
	}

	isPinned, ok := diskCacheStatus["is_pinned"].(bool)
	if !ok {
		t.Fatalf("expected is_pinned to be a bool")
	}
	if !isPinned {
		t.Errorf("expected is_pinned=true for pinned namespace")
	}
}
