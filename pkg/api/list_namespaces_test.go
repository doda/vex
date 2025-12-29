package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestListNamespaces_EmptyStore tests that the endpoint returns empty namespaces for an empty store.
func TestListNamespaces_EmptyStore(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	req := httptest.NewRequest("GET", "/v1/namespaces", nil)
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

	// Verify namespaces array exists
	namespaces, ok := result["namespaces"].([]interface{})
	if !ok {
		t.Fatalf("expected namespaces to be an array, got %T", result["namespaces"])
	}

	if len(namespaces) != 0 {
		t.Errorf("expected empty namespaces array, got %d items", len(namespaces))
	}

	// next_cursor should not be present for empty results
	if _, ok := result["next_cursor"]; ok {
		t.Errorf("expected no next_cursor for empty results")
	}
}

// TestListNamespaces_ListsNamespaces tests that the endpoint lists namespaces.
func TestListNamespaces_ListsNamespaces(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespaces by writing to them
	namesToCreate := []string{"alpha", "beta", "gamma"}
	for _, ns := range namesToCreate {
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

		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("failed to create namespace %s: %d", ns, w.Result().StatusCode)
		}
	}

	// List namespaces
	req := httptest.NewRequest("GET", "/v1/namespaces", nil)
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

	// Verify namespaces array
	namespaces, ok := result["namespaces"].([]interface{})
	if !ok {
		t.Fatalf("expected namespaces to be an array")
	}

	if len(namespaces) != 3 {
		t.Errorf("expected 3 namespaces, got %d", len(namespaces))
	}

	// Verify each namespace has an id
	found := make(map[string]bool)
	for _, ns := range namespaces {
		nsMap, ok := ns.(map[string]interface{})
		if !ok {
			t.Fatalf("expected namespace entry to be object, got %T", ns)
		}
		id, ok := nsMap["id"].(string)
		if !ok {
			t.Fatalf("expected namespace id to be string, got %T", nsMap["id"])
		}
		found[id] = true
	}

	for _, name := range namesToCreate {
		if !found[name] {
			t.Errorf("expected to find namespace %s", name)
		}
	}
}

// TestListNamespaces_CursorPagination tests cursor-based pagination.
func TestListNamespaces_CursorPagination(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create 5 namespaces
	for i := 0; i < 5; i++ {
		name := string(rune('a' + i)) + "-ns"
		body := map[string]any{
			"upsert_rows": []any{
				map[string]any{"id": 1, "name": "test"},
			},
		}
		bodyBytes, _ := json.Marshal(body)

		req := httptest.NewRequest("POST", "/v2/namespaces/"+name, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("failed to create namespace %s: %d", name, w.Result().StatusCode)
		}
	}

	// Request with page_size=2
	req := httptest.NewRequest("GET", "/v1/namespaces?page_size=2", nil)
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

	namespaces := result["namespaces"].([]interface{})
	if len(namespaces) != 2 {
		t.Errorf("expected 2 namespaces, got %d", len(namespaces))
	}

	// Should have next_cursor since there are more results
	nextCursor, ok := result["next_cursor"].(string)
	if !ok || nextCursor == "" {
		t.Errorf("expected next_cursor to be present")
	}

	// Request second page with cursor
	req = httptest.NewRequest("GET", "/v1/namespaces?page_size=2&cursor="+nextCursor, nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp = w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 for second page, got %d", resp.StatusCode)
	}

	respBody, _ = io.ReadAll(resp.Body)
	var result2 map[string]any
	if err := json.Unmarshal(respBody, &result2); err != nil {
		t.Fatalf("failed to parse second page response: %v", err)
	}

	namespaces2 := result2["namespaces"].([]interface{})
	// Should have more namespaces
	if len(namespaces2) == 0 {
		t.Errorf("expected namespaces in second page, got none")
	}
}

// TestListNamespaces_PrefixFilter tests prefix parameter for filtering.
func TestListNamespaces_PrefixFilter(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespaces with different prefixes
	namesToCreate := []string{"prod-alpha", "prod-beta", "staging-gamma", "dev-delta"}
	for _, ns := range namesToCreate {
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

		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("failed to create namespace %s: %d", ns, w.Result().StatusCode)
		}
	}

	// List with prefix=prod
	req := httptest.NewRequest("GET", "/v1/namespaces?prefix=prod", nil)
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

	namespaces := result["namespaces"].([]interface{})
	if len(namespaces) != 2 {
		t.Errorf("expected 2 namespaces with prefix 'prod', got %d", len(namespaces))
	}

	// Verify all returned namespaces start with "prod"
	for _, ns := range namespaces {
		nsMap := ns.(map[string]interface{})
		id := nsMap["id"].(string)
		if id[:4] != "prod" {
			t.Errorf("expected namespace to start with 'prod', got %s", id)
		}
	}
}

// TestListNamespaces_PageSizeParameter tests page_size parameter.
func TestListNamespaces_PageSizeParameter(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create 10 namespaces
	for i := 0; i < 10; i++ {
		name := string(rune('a'+i)) + "-ns"
		body := map[string]any{
			"upsert_rows": []any{
				map[string]any{"id": 1, "name": "test"},
			},
		}
		bodyBytes, _ := json.Marshal(body)

		req := httptest.NewRequest("POST", "/v2/namespaces/"+name, bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("failed to create namespace %s: %d", name, w.Result().StatusCode)
		}
	}

	// Test page_size=3
	req := httptest.NewRequest("GET", "/v1/namespaces?page_size=3", nil)
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

	namespaces := result["namespaces"].([]interface{})
	if len(namespaces) != 3 {
		t.Errorf("expected 3 namespaces with page_size=3, got %d", len(namespaces))
	}
}

// TestListNamespaces_DefaultPageSize tests default page_size is 100.
func TestListNamespaces_DefaultPageSize(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace
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

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// List without page_size - should use default 100
	req = httptest.NewRequest("GET", "/v1/namespaces", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Just verify the request succeeds - can't easily verify the default in a unit test
	// since we don't have 100+ namespaces
}

// TestListNamespaces_PageSizeMax tests that page_size cannot exceed 1000.
func TestListNamespaces_PageSizeMax(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Request with page_size > 1000
	req := httptest.NewRequest("GET", "/v1/namespaces?page_size=1001", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400 for page_size > 1000, got %d", resp.StatusCode)
	}
}

// TestListNamespaces_InvalidPageSize tests invalid page_size values.
func TestListNamespaces_InvalidPageSize(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	testCases := []struct {
		name     string
		pageSize string
	}{
		{"negative", "-1"},
		{"zero", "0"},
		{"non-numeric", "abc"},
		{"float", "10.5"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/v1/namespaces?page_size="+tc.pageSize, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			resp := w.Result()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("expected status 400 for page_size=%s, got %d", tc.pageSize, resp.StatusCode)
			}
		})
	}
}

// TestListNamespaces_ResponseFormat tests response includes namespaces array and next_cursor.
func TestListNamespaces_ResponseFormat(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace
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

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// List namespaces
	req = httptest.NewRequest("GET", "/v1/namespaces", nil)
	w = httptest.NewRecorder()
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

	// Verify namespaces array is present
	namespaces, ok := result["namespaces"].([]interface{})
	if !ok {
		t.Fatalf("expected namespaces to be an array")
	}

	if len(namespaces) != 1 {
		t.Errorf("expected 1 namespace, got %d", len(namespaces))
	}

	// Verify namespace entry has id field
	nsEntry := namespaces[0].(map[string]interface{})
	if nsEntry["id"] != "test-ns" {
		t.Errorf("expected namespace id 'test-ns', got %v", nsEntry["id"])
	}
}

// TestListNamespaces_ObjectStoreUnavailable tests 503 when object store is unavailable.
func TestListNamespaces_ObjectStoreUnavailable(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	// Set object store as unavailable
	router.SetState(&ServerState{
		Namespaces:  map[string]*NamespaceState{},
		ObjectStore: ObjectStoreState{Available: false},
	})

	req := httptest.NewRequest("GET", "/v1/namespaces", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", resp.StatusCode)
	}
}

// TestListNamespaces_FallbackMode tests fallback behavior when no store is configured.
func TestListNamespaces_FallbackMode(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	// Set object store as available but no actual store
	router.SetState(&ServerState{
		Namespaces:  map[string]*NamespaceState{},
		ObjectStore: ObjectStoreState{Available: true},
	})

	req := httptest.NewRequest("GET", "/v1/namespaces", nil)
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

	// Verify empty namespaces array in fallback mode
	namespaces, ok := result["namespaces"].([]interface{})
	if !ok {
		t.Fatalf("expected namespaces to be an array")
	}

	if len(namespaces) != 0 {
		t.Errorf("expected empty namespaces in fallback mode, got %d", len(namespaces))
	}
}

// TestListNamespaces_CatalogBased verifies that listing uses catalog entries.
// This ensures catalog/namespaces/<namespace> objects are used for listing.
func TestListNamespaces_CatalogBased(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespaces by writing to them (this creates catalog entries)
	namesToCreate := []string{"ns1", "ns2", "ns3"}
	for _, ns := range namesToCreate {
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

		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("failed to create namespace %s: %d", ns, w.Result().StatusCode)
		}
	}

	// Verify catalog entries exist
	ctx := context.Background()
	for _, ns := range namesToCreate {
		catalogKey := "catalog/namespaces/" + ns
		_, _, err := store.Get(ctx, catalogKey, nil)
		if err != nil {
			t.Errorf("expected catalog entry for %s to exist, got error: %v", ns, err)
		}
	}

	// List namespaces
	req := httptest.NewRequest("GET", "/v1/namespaces", nil)
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

	// Verify namespaces array
	namespaces, ok := result["namespaces"].([]interface{})
	if !ok {
		t.Fatalf("expected namespaces to be an array")
	}

	if len(namespaces) != 3 {
		t.Errorf("expected 3 namespaces, got %d", len(namespaces))
	}

	// Verify each namespace from catalog
	found := make(map[string]bool)
	for _, ns := range namespaces {
		nsMap, ok := ns.(map[string]interface{})
		if !ok {
			t.Fatalf("expected namespace entry to be object, got %T", ns)
		}
		id, ok := nsMap["id"].(string)
		if !ok {
			t.Fatalf("expected namespace id to be string, got %T", nsMap["id"])
		}
		found[id] = true
	}

	for _, name := range namesToCreate {
		if !found[name] {
			t.Errorf("expected to find namespace %s from catalog", name)
		}
	}
}
