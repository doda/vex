package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestMetadataEndpoint_ReturnsNamespaceMetadata tests that the metadata endpoint returns namespace metadata.
func TestMetadataEndpoint_ReturnsNamespaceMetadata(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// First create a namespace by writing to it
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1", "value": 100},
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

	// Now get metadata
	req = httptest.NewRequest("GET", "/v1/namespaces/test-ns/metadata", nil)
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

	// Verify namespace field
	if result["namespace"] != "test-ns" {
		t.Errorf("expected namespace 'test-ns', got %v", result["namespace"])
	}

	// Verify approx_row_count is present
	if _, ok := result["approx_row_count"]; !ok {
		t.Errorf("expected approx_row_count field")
	}

	// Verify approx_logical_bytes is present
	if _, ok := result["approx_logical_bytes"]; !ok {
		t.Errorf("expected approx_logical_bytes field")
	}
}

// TestMetadataEndpoint_IncludesSchema tests that the metadata endpoint includes the schema.
func TestMetadataEndpoint_IncludesSchema(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace with explicit schema in the write request
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test1", "count": 100, "active": true},
		},
		"schema": map[string]any{
			"name":   map[string]any{"type": "string"},
			"count":  map[string]any{"type": "uint"},
			"active": map[string]any{"type": "bool"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/schema-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(w.Result().Body)
		t.Fatalf("failed to create namespace: %d, body: %s", w.Result().StatusCode, string(respBody))
	}

	// Get metadata
	req = httptest.NewRequest("GET", "/v1/namespaces/schema-ns/metadata", nil)
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

	// Verify schema is present
	schema, ok := result["schema"].(map[string]any)
	if !ok {
		t.Fatalf("expected schema field to be present as object")
	}

	// Verify schema has attributes
	attrs, ok := schema["attributes"].(map[string]any)
	if !ok {
		t.Fatalf("expected schema.attributes to be present")
	}

	// Check that name, count, and active are in the schema
	if _, ok := attrs["name"]; !ok {
		t.Errorf("expected 'name' in schema attributes")
	}
	if _, ok := attrs["count"]; !ok {
		t.Errorf("expected 'count' in schema attributes")
	}
	if _, ok := attrs["active"]; !ok {
		t.Errorf("expected 'active' in schema attributes")
	}
}

// TestMetadataEndpoint_CreatedAtUpdatedAt tests that timestamps are returned.
func TestMetadataEndpoint_CreatedAtUpdatedAt(t *testing.T) {
	cfg := testConfig()
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

	req := httptest.NewRequest("POST", "/v2/namespaces/time-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Get metadata
	req = httptest.NewRequest("GET", "/v1/namespaces/time-ns/metadata", nil)
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

	// Verify created_at is present and parseable
	createdAt, ok := result["created_at"].(string)
	if !ok {
		t.Fatalf("expected created_at to be string, got %T", result["created_at"])
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", createdAt); err != nil {
		t.Errorf("failed to parse created_at: %v", err)
	}

	// Verify updated_at is present and parseable
	updatedAt, ok := result["updated_at"].(string)
	if !ok {
		t.Fatalf("expected updated_at to be string, got %T", result["updated_at"])
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", updatedAt); err != nil {
		t.Errorf("failed to parse updated_at: %v", err)
	}
}

// TestMetadataEndpoint_EncryptionField tests that encryption field is present with SSE default true.
func TestMetadataEndpoint_EncryptionField(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/enc-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Get metadata
	req = httptest.NewRequest("GET", "/v1/namespaces/enc-ns/metadata", nil)
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

	// Verify encryption field is present
	encryption, ok := result["encryption"].(map[string]any)
	if !ok {
		t.Fatalf("expected encryption field to be present as object")
	}

	// Verify SSE is true
	if encryption["sse"] != true {
		t.Errorf("expected encryption.sse to be true, got %v", encryption["sse"])
	}
}

// TestMetadataEndpoint_IndexStatus tests index.status field.
func TestMetadataEndpoint_IndexStatus(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/idx-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Get metadata
	req = httptest.NewRequest("GET", "/v1/namespaces/idx-ns/metadata", nil)
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

	// Verify index field is present
	index, ok := result["index"].(map[string]any)
	if !ok {
		t.Fatalf("expected index field to be present as object")
	}

	// Verify status is "updating" or "up-to-date"
	status, ok := index["status"].(string)
	if !ok {
		t.Fatalf("expected index.status to be string")
	}
	if status != "updating" && status != "up-to-date" {
		t.Errorf("expected index.status to be 'updating' or 'up-to-date', got %v", status)
	}
}

// TestMetadataEndpoint_UnindexedBytes tests that unindexed_bytes is shown when index is updating.
func TestMetadataEndpoint_UnindexedBytes(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace with data (WAL head > indexed WAL seq = updating)
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "data": "some test data to add bytes"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/unidx-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Get metadata
	req = httptest.NewRequest("GET", "/v1/namespaces/unidx-ns/metadata", nil)
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

	// Verify index field is present
	index, ok := result["index"].(map[string]any)
	if !ok {
		t.Fatalf("expected index field to be present as object")
	}

	// If status is updating, unindexed_bytes should be present
	status := index["status"].(string)
	if status == "updating" {
		if _, ok := index["unindexed_bytes"]; !ok {
			t.Errorf("expected unindexed_bytes when index status is 'updating'")
		}
	}
}

// TestMetadataEndpoint_NamespaceNotFound tests that 404 is returned for non-existent namespace.
func TestMetadataEndpoint_NamespaceNotFound(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	req := httptest.NewRequest("GET", "/v1/namespaces/nonexistent-ns/metadata", nil)
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["status"] != "error" {
		t.Errorf("expected status 'error', got %v", result["status"])
	}
}

// TestMetadataEndpoint_DeletedNamespace tests that deleted namespace returns appropriate error.
func TestMetadataEndpoint_DeletedNamespace(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create and delete a namespace
	stateManager := namespace.NewStateManager(store)

	// Create namespace first
	loaded, err := stateManager.Create(nil, "deleted-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Mark it as tombstoned
	_, err = stateManager.SetTombstoned(nil, "deleted-ns", loaded.ETag)
	if err != nil {
		t.Fatalf("failed to tombstone namespace: %v", err)
	}

	// Get metadata
	req := httptest.NewRequest("GET", "/v1/namespaces/deleted-ns/metadata", nil)
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

// TestMetadataEndpoint_ObjectStoreUnavailable tests 503 when object store is unavailable.
func TestMetadataEndpoint_ObjectStoreUnavailable(t *testing.T) {
	cfg := testConfig()
	router := NewRouter(cfg)

	// Set object store as unavailable
	router.SetState(&ServerState{
		Namespaces:  map[string]*NamespaceState{},
		ObjectStore: ObjectStoreState{Available: false},
	})

	req := httptest.NewRequest("GET", "/v1/namespaces/test-ns/metadata", nil)
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", resp.StatusCode)
	}
}

// TestMetadataEndpoint_ObjectStoreFailure tests 503 when object store state load fails.
func TestMetadataEndpoint_ObjectStoreFailure(t *testing.T) {
	cfg := testConfig()
	store := newFailingStore(objectstore.NewMemoryStore(), errors.New("object store failure"))
	store.failGet = true

	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	req := httptest.NewRequest("GET", "/v1/namespaces/test-ns/metadata", nil)
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	msg := parseErrorResponse(t, w.Result(), http.StatusServiceUnavailable)
	if !strings.Contains(msg, "object store") {
		t.Errorf("expected error to mention object store, got %q", msg)
	}
}

// TestMetadataEndpoint_FallbackMode tests fallback behavior when no store is configured.
func TestMetadataEndpoint_FallbackMode(t *testing.T) {
	cfg := testConfig()
	router := NewRouter(cfg)

	// Set up a namespace that exists (fallback mode)
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"fallback-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	req := httptest.NewRequest("GET", "/v1/namespaces/fallback-ns/metadata", nil)
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

	// Verify basic fields are present in fallback mode
	if result["namespace"] != "fallback-ns" {
		t.Errorf("expected namespace 'fallback-ns', got %v", result["namespace"])
	}
	if _, ok := result["approx_row_count"]; !ok {
		t.Errorf("expected approx_row_count field")
	}
	if _, ok := result["approx_logical_bytes"]; !ok {
		t.Errorf("expected approx_logical_bytes field")
	}
	if _, ok := result["encryption"]; !ok {
		t.Errorf("expected encryption field")
	}
	if _, ok := result["index"]; !ok {
		t.Errorf("expected index field")
	}
}

// TestMetadataEndpoint_FallbackModeUpdating tests fallback mode when index is updating.
func TestMetadataEndpoint_FallbackModeUpdating(t *testing.T) {
	cfg := testConfig()
	router := NewRouter(cfg)

	// Set up a namespace with index building
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"updating-ns": {
				Exists:         true,
				IndexBuilding:  true,
				UnindexedBytes: 5000,
			},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	req := httptest.NewRequest("GET", "/v1/namespaces/updating-ns/metadata", nil)
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

	// Verify index status
	index, ok := result["index"].(map[string]any)
	if !ok {
		t.Fatalf("expected index field")
	}

	if index["status"] != "updating" {
		t.Errorf("expected index.status to be 'updating', got %v", index["status"])
	}

	// Verify unindexed_bytes is present
	if index["unindexed_bytes"] != float64(5000) {
		t.Errorf("expected unindexed_bytes 5000, got %v", index["unindexed_bytes"])
	}
}
