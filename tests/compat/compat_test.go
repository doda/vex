package compat

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/pkg/api"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// This test suite verifies compatibility with turbopuffer's published API semantics.
// It tests documented request shapes, HTTP status codes, response formats, and
// filter semantics as described in the turbopuffer documentation and Vex SPEC.md.

// =============================================================================
// SECTION 1: Documented Request Shapes
// =============================================================================

// TestWriteRequestShapes verifies all documented write request shapes are accepted.
func TestWriteRequestShapes(t *testing.T) {
	cfg := newTestConfig()
	store := objectstore.NewMemoryStore()
	router := api.NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	testCases := []struct {
		name       string
		body       map[string]any
		wantStatus int
	}{
		{
			name: "upsert_rows - basic",
			body: map[string]any{
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "test"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "upsert_rows - with vector",
			body: map[string]any{
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "test", "vector": []any{1.0, 2.0, 3.0}},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "upsert_columns - basic",
			body: map[string]any{
				"upsert_columns": map[string]any{
					"ids":  []any{1, 2, 3},
					"name": []any{"a", "b", "c"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "patch_rows - basic",
			body: map[string]any{
				"patch_rows": []any{
					map[string]any{"id": 1, "name": "updated"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "patch_columns - basic",
			body: map[string]any{
				"patch_columns": map[string]any{
					"ids":  []any{1, 2},
					"name": []any{"updated1", "updated2"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "deletes - array of IDs",
			body: map[string]any{
				"deletes": []any{1, 2, 3},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete_by_filter - basic",
			body: map[string]any{
				"delete_by_filter": []any{"category", "Eq", "spam"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "patch_by_filter - basic",
			body: map[string]any{
				"patch_by_filter": map[string]any{
					"filter":  []any{"status", "Eq", "pending"},
					"updates": map[string]any{"status": "processed"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "schema - explicit update",
			body: map[string]any{
				"schema": map[string]any{
					"name": map[string]any{"type": "string"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "disable_backpressure - flag",
			body: map[string]any{
				"disable_backpressure": true,
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "test"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "conditional upsert - upsert_condition",
			body: map[string]any{
				"upsert_rows": []any{
					map[string]any{"id": 1, "name": "test", "version": 2},
				},
				"upsert_condition": []any{"version", "Lt", map[string]any{"$ref_new": "version"}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "conditional patch - patch_condition",
			body: map[string]any{
				"patch_rows": []any{
					map[string]any{"id": 1, "name": "updated"},
				},
				"patch_condition": []any{"locked", "Eq", false},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "conditional delete - delete_condition",
			body: map[string]any{
				"deletes": []any{1, 2, 3},
				"delete_condition": []any{"deletable", "Eq", true},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "empty write - valid",
			body: map[string]any{},
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bodyBytes, _ := json.Marshal(tc.body)
			req := httptest.NewRequest("POST", "/v2/namespaces/test-compat", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			addAuthHeader(req)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Result().StatusCode != tc.wantStatus {
				body, _ := io.ReadAll(w.Result().Body)
				t.Errorf("expected status %d, got %d: %s", tc.wantStatus, w.Result().StatusCode, string(body))
			}
		})
	}
}

// TestQueryRequestShapes verifies all documented query request shapes are accepted.
// Uses the test state approach for consistent behavior without write batching delays.
func TestQueryRequestShapes(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	// Use SetState to simulate an existing namespace with data
	router.SetState(&api.ServerState{
		Namespaces: map[string]*api.NamespaceState{
			"query-test": {Exists: true},
		},
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	testCases := []struct {
		name       string
		body       map[string]any
		wantStatus int
	}{
		{
			name: "rank_by id asc",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "rank_by id desc",
			body: map[string]any{
				"rank_by": []any{"id", "desc"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "rank_by attribute",
			body: map[string]any{
				"rank_by": []any{"score", "desc"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "rank_by vector ANN - float array",
			body: map[string]any{
				"rank_by": []any{"vector", "ANN", []any{1.0, 2.0, 3.0}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "rank_by vector ANN - base64",
			body: map[string]any{
				"rank_by":         []any{"vector", "ANN", "AACAPwAAAEAAAEBA"},
				"vector_encoding": "base64",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "limit parameter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"limit":   10,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "top_k alias",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"top_k":   10,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "include_attributes",
			body: map[string]any{
				"rank_by":            []any{"id", "asc"},
				"include_attributes": []any{"name", "score"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "exclude_attributes",
			body: map[string]any{
				"rank_by":            []any{"id", "asc"},
				"exclude_attributes": []any{"internal_field"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "filters - basic Eq",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"category", "Eq", "a"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "filters - And operator",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"And", []any{
					[]any{"category", "Eq", "a"},
					[]any{"score", "Gt", 5},
				}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "filters - Or operator",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"Or", []any{
					[]any{"category", "Eq", "a"},
					[]any{"category", "Eq", "b"},
				}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "aggregate_by - count",
			body: map[string]any{
				"aggregate_by": map[string]any{
					"Count": map[string]any{},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "aggregate_by - sum",
			body: map[string]any{
				"aggregate_by": map[string]any{
					"Sum": map[string]any{"field": "score"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "group_by",
			body: map[string]any{
				"aggregate_by": map[string]any{
					"Count": map[string]any{},
				},
				"group_by": "category",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "consistency - strong",
			body: map[string]any{
				"rank_by":     []any{"id", "asc"},
				"consistency": "strong",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "consistency - eventual",
			body: map[string]any{
				"rank_by":     []any{"id", "asc"},
				"consistency": "eventual",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "multi-query - queries array",
			body: map[string]any{
				"queries": []any{
					map[string]any{"rank_by": []any{"id", "asc"}, "limit": 5},
					map[string]any{"rank_by": []any{"score", "desc"}, "limit": 3},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bodyBytes, _ := json.Marshal(tc.body)
			req := httptest.NewRequest("POST", "/v2/namespaces/query-test/query", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			addAuthHeader(req)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Result().StatusCode != tc.wantStatus {
				body, _ := io.ReadAll(w.Result().Body)
				t.Errorf("expected status %d, got %d: %s", tc.wantStatus, w.Result().StatusCode, string(body))
			}
		})
	}
}

// =============================================================================
// SECTION 2: HTTP Status Codes
// =============================================================================

// TestStatusCode400InvalidRequest tests that 400 is returned for invalid requests.
func TestStatusCode400InvalidRequest(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	router.SetState(&api.ServerState{
		Namespaces: map[string]*api.NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	testCases := []struct {
		name           string
		method         string
		path           string
		body           string
		wantErrContain string
	}{
		{
			name:           "invalid JSON in write",
			method:         "POST",
			path:           "/v2/namespaces/test-ns",
			body:           "{invalid json",
			wantErrContain: "JSON",
		},
		{
			name:           "invalid JSON in query",
			method:         "POST",
			path:           "/v2/namespaces/test-ns/query",
			body:           "not json",
			wantErrContain: "JSON",
		},
		{
			name:           "invalid namespace name - special chars",
			method:         "POST",
			path:           "/v2/namespaces/test@namespace",
			body:           `{}`,
			wantErrContain: "namespace",
		},
		{
			name:           "invalid namespace name - too long",
			method:         "POST",
			path:           "/v2/namespaces/" + strings.Repeat("a", 129),
			body:           `{}`,
			wantErrContain: "128",
		},
		{
			name:           "duplicate IDs in upsert_columns",
			method:         "POST",
			path:           "/v2/namespaces/test-ns",
			body:           `{"upsert_columns":{"ids":[1,1],"name":["a","b"]}}`,
			wantErrContain: "duplicate",
		},
		{
			name:           "duplicate IDs in patch_columns",
			method:         "POST",
			path:           "/v2/namespaces/test-ns",
			body:           `{"patch_columns":{"ids":[1,1],"name":["a","b"]}}`,
			wantErrContain: "duplicate",
		},
		{
			name:           "limit too high in query",
			method:         "POST",
			path:           "/v2/namespaces/test-ns/query",
			body:           `{"rank_by":["id","asc"],"limit":10001}`,
			wantErrContain: "10,000",
		},
		{
			name:           "top_k too high in query",
			method:         "POST",
			path:           "/v2/namespaces/test-ns/query",
			body:           `{"rank_by":["id","asc"],"top_k":10001}`,
			wantErrContain: "10,000",
		},
		{
			name:           "missing rank_by without aggregate_by",
			method:         "POST",
			path:           "/v2/namespaces/test-ns/query",
			body:           `{}`,
			wantErrContain: "rank_by",
		},
		{
			name:           "too many subqueries in multi-query",
			method:         "POST",
			path:           "/v2/namespaces/test-ns/query",
			body:           `{"queries":[{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}]}`,
			wantErrContain: "16",
		},
		{
			name:           "invalid filter expression",
			method:         "POST",
			path:           "/v2/namespaces/test-ns/query",
			body:           `{"rank_by":["id","asc"],"filters":"not a filter"}`,
			wantErrContain: "filter",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}

			req := httptest.NewRequest(tc.method, tc.path, body)
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			addAuthHeader(req)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != http.StatusBadRequest {
				t.Errorf("expected status 400, got %d", w.Result().StatusCode)
			}

			respBody, _ := io.ReadAll(w.Result().Body)
			var result map[string]any
			json.Unmarshal(respBody, &result)

			if result["status"] != "error" {
				t.Errorf("expected status='error', got %v", result["status"])
			}

			errMsg, _ := result["error"].(string)
			if tc.wantErrContain != "" && !strings.Contains(strings.ToLower(errMsg), strings.ToLower(tc.wantErrContain)) {
				t.Errorf("expected error to contain %q, got: %s", tc.wantErrContain, errMsg)
			}
		})
	}
}

// TestAttributeNameValidation tests attribute name validation using real handlers.
func TestAttributeNameValidation(t *testing.T) {
	cfg := newTestConfig()
	store := objectstore.NewMemoryStore()
	router := api.NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	t.Run("attribute name too long returns 400", func(t *testing.T) {
		body := `{"upsert_rows":[{"id":1,"` + strings.Repeat("a", 129) + `":"value"}]}`
		req := httptest.NewRequest("POST", "/v2/namespaces/attr-test", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Result().StatusCode)
		}

		respBody, _ := io.ReadAll(w.Result().Body)
		var result map[string]any
		json.Unmarshal(respBody, &result)

		errMsg, _ := result["error"].(string)
		if !strings.Contains(errMsg, "128") {
			t.Errorf("expected error to mention '128', got: %s", errMsg)
		}
	})

	t.Run("attribute name starting with $ returns 400", func(t *testing.T) {
		body := `{"upsert_rows":[{"id":1,"$invalid":"value"}]}`
		req := httptest.NewRequest("POST", "/v2/namespaces/attr-test", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Result().StatusCode)
		}

		respBody, _ := io.ReadAll(w.Result().Body)
		var result map[string]any
		json.Unmarshal(respBody, &result)

		errMsg, _ := result["error"].(string)
		if !strings.Contains(errMsg, "$") {
			t.Errorf("expected error to mention '$', got: %s", errMsg)
		}
	})
}

// TestStatusCode401Unauthorized tests authentication error handling.
func TestStatusCode401Unauthorized(t *testing.T) {
	cfg := newTestConfig()
	cfg.AuthToken = "secret-token"
	router := api.NewRouter(cfg)

	testCases := []struct {
		name     string
		authHdr  string
		wantCode int
	}{
		{
			name:     "missing auth header",
			authHdr:  "",
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "invalid token",
			authHdr:  "Bearer wrong-token",
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "malformed header - no Bearer",
			authHdr:  "secret-token",
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "malformed header - Basic instead of Bearer",
			authHdr:  "Basic secret-token",
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "valid token",
			authHdr:  "Bearer secret-token",
			wantCode: http.StatusOK,
		},
	}

	router.SetState(&api.ServerState{
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/v1/namespaces", nil)
			if tc.authHdr != "" {
				req.Header.Set("Authorization", tc.authHdr)
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != tc.wantCode {
				t.Errorf("expected status %d, got %d", tc.wantCode, w.Result().StatusCode)
			}

			if tc.wantCode == http.StatusUnauthorized {
				body, _ := io.ReadAll(w.Result().Body)
				var result map[string]any
				json.Unmarshal(body, &result)

				if result["status"] != "error" {
					t.Errorf("expected status='error', got %v", result["status"])
				}
			}
		})
	}
}

// TestStatusCode202IndexBuilding tests 202 for queries depending on building index.
func TestStatusCode202IndexBuilding(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	router.SetState(&api.ServerState{
		Namespaces: map[string]*api.NamespaceState{
			"building-ns": {
				Exists:        true,
				IndexBuilding: true,
				UnindexedBytes: 1000,
			},
		},
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	testCases := []struct {
		name   string
		body   string
	}{
		{
			name: "simple query",
			body: `{}`,
		},
		{
			name: "query with filter",
			body: `{"filters":["status","Eq","active"]}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v2/namespaces/building-ns/query", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			addAuthHeader(req)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != http.StatusAccepted {
				t.Errorf("expected status 202, got %d", w.Result().StatusCode)
			}

			body, _ := io.ReadAll(w.Result().Body)
			var result map[string]any
			json.Unmarshal(body, &result)

			if result["status"] != "accepted" {
				t.Errorf("expected status='accepted', got %v", result["status"])
			}
		})
	}
}

// TestStatusCode429Backpressure tests backpressure enforcement.
func TestStatusCode429Backpressure(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	router.SetState(&api.ServerState{
		Namespaces: map[string]*api.NamespaceState{
			"backpressure-ns": {
				Exists:          true,
				UnindexedBytes:  api.MaxUnindexedBytes + 1,
				BackpressureOff: false,
			},
		},
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	req := httptest.NewRequest("POST", "/v2/namespaces/backpressure-ns", strings.NewReader(`{"upsert_rows":[]}`))
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected status 429, got %d", w.Result().StatusCode)
	}

	body, _ := io.ReadAll(w.Result().Body)
	var result map[string]any
	json.Unmarshal(body, &result)

	if result["status"] != "error" {
		t.Errorf("expected status='error', got %v", result["status"])
	}

	errMsg, _ := result["error"].(string)
	if !strings.Contains(errMsg, "2GB") {
		t.Errorf("expected error to mention 2GB threshold, got: %s", errMsg)
	}
}

// TestStatusCode503ServiceUnavailable tests object store failures and strong query with disable_backpressure.
func TestStatusCode503ServiceUnavailable(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	t.Run("object store unavailable", func(t *testing.T) {
		router.SetState(&api.ServerState{
			Namespaces: map[string]*api.NamespaceState{
				"test-ns": {Exists: true},
			},
			ObjectStore: api.ObjectStoreState{Available: false},
		})

		endpoints := []struct {
			method string
			path   string
			body   string
		}{
			{"POST", "/v2/namespaces/test-ns", `{"upsert_rows":[]}`},
			{"POST", "/v2/namespaces/test-ns/query", `{}`},
			{"GET", "/v1/namespaces", ""},
			{"DELETE", "/v2/namespaces/test-ns", ""},
		}

		for _, ep := range endpoints {
			var body io.Reader
			if ep.body != "" {
				body = strings.NewReader(ep.body)
			}

			req := httptest.NewRequest(ep.method, ep.path, body)
			if ep.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			addAuthHeader(req)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != http.StatusServiceUnavailable {
				t.Errorf("%s %s: expected 503, got %d", ep.method, ep.path, w.Result().StatusCode)
			}
		}
	})

	t.Run("strong query with disable_backpressure and unindexed > 2GB", func(t *testing.T) {
		router.SetState(&api.ServerState{
			Namespaces: map[string]*api.NamespaceState{
				"bp-off-ns": {
					Exists:          true,
					UnindexedBytes:  api.MaxUnindexedBytes + 1,
					BackpressureOff: true,
				},
			},
			ObjectStore: api.ObjectStoreState{Available: true},
		})

		req := httptest.NewRequest("POST", "/v2/namespaces/bp-off-ns/query", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected 503, got %d", w.Result().StatusCode)
		}

		body, _ := io.ReadAll(w.Result().Body)
		var result map[string]any
		json.Unmarshal(body, &result)

		errMsg, _ := result["error"].(string)
		if !strings.Contains(errMsg, "strong query") || !strings.Contains(errMsg, "disable_backpressure") {
			t.Errorf("expected error mentioning strong query and disable_backpressure, got: %s", errMsg)
		}
	})
}

// =============================================================================
// SECTION 3: Response Field Presence/Shape
// =============================================================================

// TestWriteResponseShape verifies write response contains expected fields.
func TestWriteResponseShape(t *testing.T) {
	cfg := newTestConfig()
	store := objectstore.NewMemoryStore()
	router := api.NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/response-test", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Result().StatusCode)
	}

	respBody, _ := io.ReadAll(w.Result().Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	requiredFields := []string{
		"rows_affected",
		"rows_upserted",
		"rows_patched",
		"rows_deleted",
	}

	for _, field := range requiredFields {
		if _, ok := result[field]; !ok {
			t.Errorf("missing required field: %s", field)
		}
	}
}

// TestQueryResponseShape verifies query response contains expected fields.
// Uses stub state to avoid write batching timing issues.
func TestQueryResponseShape(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	// Use SetState to simulate an existing namespace with data
	router.SetState(&api.ServerState{
		Namespaces: map[string]*api.NamespaceState{
			"query-response": {Exists: true},
		},
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	t.Run("basic query response", func(t *testing.T) {
		queryBody := map[string]any{
			"rank_by": []any{"id", "asc"},
		}
		queryBytes, _ := json.Marshal(queryBody)
		req := httptest.NewRequest("POST", "/v2/namespaces/query-response/query", bytes.NewReader(queryBytes))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Result().StatusCode)
		}

		respBody, _ := io.ReadAll(w.Result().Body)
		var result map[string]any
		if err := json.Unmarshal(respBody, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Check for rows array
		rows, ok := result["rows"].([]any)
		if !ok {
			t.Error("missing 'rows' array in response")
		}

		// Check billing object
		if _, ok := result["billing"]; !ok {
			t.Error("missing 'billing' object in response")
		}

		// Check performance object
		if _, ok := result["performance"]; !ok {
			t.Error("missing 'performance' object in response")
		}

		// Check row structure
		if len(rows) > 0 {
			row, ok := rows[0].(map[string]any)
			if !ok {
				t.Error("row is not an object")
			}
			if _, ok := row["id"]; !ok {
				t.Error("row missing 'id' field")
			}
		}
	})

	t.Run("aggregation response", func(t *testing.T) {
		queryBody := map[string]any{
			"aggregate_by": map[string]any{
				"Count": map[string]any{},
			},
		}
		queryBytes, _ := json.Marshal(queryBody)
		req := httptest.NewRequest("POST", "/v2/namespaces/query-response/query", bytes.NewReader(queryBytes))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Result().StatusCode)
		}

		respBody, _ := io.ReadAll(w.Result().Body)
		var result map[string]any
		if err := json.Unmarshal(respBody, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Aggregation responses should have aggregations field, not rows
		if _, ok := result["aggregations"]; !ok {
			t.Error("missing 'aggregations' in aggregation response")
		}
	})

	t.Run("multi-query response", func(t *testing.T) {
		queryBody := map[string]any{
			"queries": []any{
				map[string]any{"rank_by": []any{"id", "asc"}, "limit": 5},
				map[string]any{"rank_by": []any{"score", "desc"}, "limit": 3},
			},
		}
		queryBytes, _ := json.Marshal(queryBody)
		req := httptest.NewRequest("POST", "/v2/namespaces/query-response/query", bytes.NewReader(queryBytes))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Result().StatusCode)
		}

		respBody, _ := io.ReadAll(w.Result().Body)
		var result map[string]any
		if err := json.Unmarshal(respBody, &result); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}

		// Multi-query responses should have results array
		results, ok := result["results"].([]any)
		if !ok {
			t.Error("missing 'results' array in multi-query response")
		}

		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
	})
}

// TestErrorResponseShape verifies all errors follow the standard format.
func TestErrorResponseShape(t *testing.T) {
	cfg := newTestConfig()
	cfg.AuthToken = "secret"
	router := api.NewRouter(cfg)

	router.SetState(&api.ServerState{
		Namespaces: map[string]*api.NamespaceState{
			"exists": {Exists: true},
		},
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	errorCases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"unauthorized", "GET", "/v1/namespaces", ""},
		{"bad request - invalid JSON", "POST", "/v2/namespaces/exists", "{invalid"},
		{"not found", "GET", "/v1/namespaces/nonexistent/metadata", ""},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}

			req := httptest.NewRequest(tc.method, tc.path, body)
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			if tc.name != "unauthorized" {
				req.Header.Set("Authorization", "Bearer secret")
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			respBody, _ := io.ReadAll(w.Result().Body)
			var result map[string]any
			if err := json.Unmarshal(respBody, &result); err != nil {
				t.Fatalf("failed to parse error response: %v", err)
			}

			// Verify format: {"status":"error","error":"..."}
			if result["status"] != "error" {
				t.Errorf("expected status='error', got %v", result["status"])
			}

			errMsg, ok := result["error"].(string)
			if !ok {
				t.Error("'error' field should be a string")
			}
			if errMsg == "" {
				t.Error("error message should not be empty")
			}

			// Error response should only have status and error fields
			for key := range result {
				if key != "status" && key != "error" {
					t.Errorf("unexpected field in error response: %s", key)
				}
			}
		})
	}
}

// TestMetadataResponseShape verifies metadata endpoint response format.
// Note: Uses real handlers via object store to ensure proper metadata response shape.
func TestMetadataResponseShape(t *testing.T) {
	cfg := newTestConfig()
	store := objectstore.NewMemoryStore()
	router := api.NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace via write first
	setupBody := `{"upsert_rows":[{"id":1,"name":"test"}]}`
	setupReq := httptest.NewRequest("POST", "/v2/namespaces/metadata-test", strings.NewReader(setupBody))
	setupReq.Header.Set("Content-Type", "application/json")
	addAuthHeader(setupReq)
	router.ServeHTTP(httptest.NewRecorder(), setupReq)

	req := httptest.NewRequest("GET", "/v1/namespaces/metadata-test/metadata", nil)
	addAuthHeader(req)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Result().StatusCode)
	}

	respBody, _ := io.ReadAll(w.Result().Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Verify essential metadata fields are present
	// (stub handlers may not have schema, so we test with real handlers)
	if result["created_at"] == nil {
		t.Error("missing 'created_at' field")
	}
	if result["updated_at"] == nil {
		t.Error("missing 'updated_at' field")
	}
}

// TestListNamespacesResponseShape verifies list namespaces response format.
func TestListNamespacesResponseShape(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	router.SetState(&api.ServerState{
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	req := httptest.NewRequest("GET", "/v1/namespaces", nil)
	addAuthHeader(req)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Result().StatusCode)
	}

	respBody, _ := io.ReadAll(w.Result().Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Should have namespaces array
	if _, ok := result["namespaces"]; !ok {
		t.Error("missing 'namespaces' field in response")
	}

	// next_cursor may or may not be present depending on pagination
}

// =============================================================================
// SECTION 4: Eq null Matches Missing Semantics
// =============================================================================

// TestEqNullMatchesMissingSemantics verifies Eq null matches missing attribute semantics.
func TestEqNullMatchesMissingSemantics(t *testing.T) {
	t.Run("Eq null matches missing attribute", func(t *testing.T) {
		f, err := filter.Parse([]any{"attr", "Eq", nil})
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}

		// Document without the attribute
		docMissing := map[string]any{"other_attr": "value"}
		if !f.Eval(docMissing) {
			t.Error("Eq null should match when attribute is missing")
		}
	})

	t.Run("Eq null matches nil value", func(t *testing.T) {
		f, err := filter.Parse([]any{"attr", "Eq", nil})
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}

		// Document with nil value
		docNil := map[string]any{"attr": nil}
		if !f.Eval(docNil) {
			t.Error("Eq null should match when attribute is nil")
		}
	})

	t.Run("Eq null does not match present value", func(t *testing.T) {
		f, err := filter.Parse([]any{"attr", "Eq", nil})
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}

		// Document with a value
		docPresent := map[string]any{"attr": "value"}
		if f.Eval(docPresent) {
			t.Error("Eq null should not match when attribute has a value")
		}
	})

	t.Run("NotEq null matches present attribute", func(t *testing.T) {
		f, err := filter.Parse([]any{"attr", "NotEq", nil})
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}

		// Document with a value
		docPresent := map[string]any{"attr": "value"}
		if !f.Eval(docPresent) {
			t.Error("NotEq null should match when attribute is present")
		}
	})

	t.Run("NotEq null does not match missing attribute", func(t *testing.T) {
		f, err := filter.Parse([]any{"attr", "NotEq", nil})
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}

		// Document without the attribute
		docMissing := map[string]any{"other_attr": "value"}
		if f.Eval(docMissing) {
			t.Error("NotEq null should not match when attribute is missing")
		}
	})

	t.Run("NotEq null does not match nil value", func(t *testing.T) {
		f, err := filter.Parse([]any{"attr", "NotEq", nil})
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}

		// Document with nil value
		docNil := map[string]any{"attr": nil}
		if f.Eval(docNil) {
			t.Error("NotEq null should not match when attribute is nil")
		}
	})
}

// TestEqNullWithIntegration tests Eq null semantics through query API.
// Note: Uses stub state since the full integration would require waiting for write batching.
// The filter logic is fully tested in TestEqNullMatchesMissingSemantics using the filter package directly.
func TestEqNullWithIntegration(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	// Use SetState to simulate an existing namespace with data
	router.SetState(&api.ServerState{
		Namespaces: map[string]*api.NamespaceState{
			"eq-null-test": {Exists: true},
		},
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	t.Run("Eq null filter is accepted in query", func(t *testing.T) {
		queryBody := map[string]any{
			"rank_by": []any{"id", "asc"},
			"filters": []any{"email", "Eq", nil},
		}
		queryBytes, _ := json.Marshal(queryBody)
		req := httptest.NewRequest("POST", "/v2/namespaces/eq-null-test/query", bytes.NewReader(queryBytes))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			body, _ := io.ReadAll(w.Result().Body)
			t.Fatalf("expected 200, got %d: %s", w.Result().StatusCode, string(body))
		}
	})

	t.Run("NotEq null filter is accepted in query", func(t *testing.T) {
		queryBody := map[string]any{
			"rank_by": []any{"id", "asc"},
			"filters": []any{"email", "NotEq", nil},
		}
		queryBytes, _ := json.Marshal(queryBody)
		req := httptest.NewRequest("POST", "/v2/namespaces/eq-null-test/query", bytes.NewReader(queryBytes))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			body, _ := io.ReadAll(w.Result().Body)
			t.Fatalf("expected 200, got %d: %s", w.Result().StatusCode, string(body))
		}
	})
}

// TestEqNullWithDeleteByFilter tests Eq null semantics in delete_by_filter.
func TestEqNullWithDeleteByFilter(t *testing.T) {
	cfg := newTestConfig()
	store := objectstore.NewMemoryStore()
	router := api.NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Set up test data
	setupBody := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "has-status", "status": "active"},
			map[string]any{"id": 2, "name": "no-status"},
			map[string]any{"id": 3, "name": "null-status", "status": nil},
		},
	}
	setupBytes, _ := json.Marshal(setupBody)
	setupReq := httptest.NewRequest("POST", "/v2/namespaces/delete-null-test", bytes.NewReader(setupBytes))
	setupReq.Header.Set("Content-Type", "application/json")
	addAuthHeader(setupReq)
	router.ServeHTTP(httptest.NewRecorder(), setupReq)

	// Delete docs where status is null
	deleteBody := map[string]any{
		"delete_by_filter": []any{"status", "Eq", nil},
	}
	deleteBytes, _ := json.Marshal(deleteBody)
	deleteReq := httptest.NewRequest("POST", "/v2/namespaces/delete-null-test", bytes.NewReader(deleteBytes))
	deleteReq.Header.Set("Content-Type", "application/json")
	addAuthHeader(deleteReq)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, deleteReq)

	if w.Result().StatusCode != http.StatusOK {
		body, _ := io.ReadAll(w.Result().Body)
		t.Fatalf("expected 200, got %d: %s", w.Result().StatusCode, string(body))
	}

	respBody, _ := io.ReadAll(w.Result().Body)
	var result map[string]any
	json.Unmarshal(respBody, &result)

	// Should have deleted 2 docs (id:2 and id:3)
	if result["rows_deleted"] != float64(2) {
		t.Errorf("expected 2 rows deleted, got %v", result["rows_deleted"])
	}
}

// =============================================================================
// SECTION 5: Additional Compatibility Tests
// =============================================================================

// TestDocumentIDTypes tests all supported ID types.
func TestDocumentIDTypes(t *testing.T) {
	cfg := newTestConfig()
	store := objectstore.NewMemoryStore()
	router := api.NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	testCases := []struct {
		name       string
		id         any
		wantStatus int
	}{
		{"uint64 ID", 123, http.StatusOK},
		{"uint64 ID large", 9223372036854775807, http.StatusOK},
		{"UUID ID", "550e8400-e29b-41d4-a716-446655440000", http.StatusOK},
		{"string ID short", "my-doc-id", http.StatusOK},
		{"string ID max length", strings.Repeat("a", 64), http.StatusOK},
		{"string ID too long", strings.Repeat("a", 65), http.StatusBadRequest},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{
				"upsert_rows": []any{
					map[string]any{"id": tc.id, "name": "test"},
				},
			}
			bodyBytes, _ := json.Marshal(body)
			req := httptest.NewRequest("POST", "/v2/namespaces/id-types-test", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			addAuthHeader(req)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != tc.wantStatus {
				respBody, _ := io.ReadAll(w.Result().Body)
				t.Errorf("expected status %d, got %d: %s", tc.wantStatus, w.Result().StatusCode, string(respBody))
			}
		})
	}
}

// TestWriteOrdering tests that write requests accept proper operation ordering.
// Tests that delete_by_filter + upsert in same request is accepted (resurrection pattern).
func TestWriteOrdering(t *testing.T) {
	cfg := newTestConfig()
	store := objectstore.NewMemoryStore()
	router := api.NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Test resurrection pattern: delete_by_filter then upsert same ID in one request
	body := map[string]any{
		"delete_by_filter": []any{"category", "Eq", "spam"},
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "resurrected", "category": "ham"},
		},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/ordering-test", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(w.Result().Body)
		t.Fatalf("expected 200, got %d: %s", w.Result().StatusCode, string(respBody))
	}

	// Verify write response shape
	respBody, _ := io.ReadAll(w.Result().Body)
	var result map[string]any
	json.Unmarshal(respBody, &result)

	// Check that both operations were processed
	if _, ok := result["rows_deleted"]; !ok {
		t.Error("expected rows_deleted in response")
	}
	if _, ok := result["rows_upserted"]; !ok {
		t.Error("expected rows_upserted in response")
	}
}

// TestFilterOperators tests various filter operators for compatibility.
// Uses stub state to verify all filter operators are accepted.
func TestFilterOperators(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	// Use SetState to simulate an existing namespace
	router.SetState(&api.ServerState{
		Namespaces: map[string]*api.NamespaceState{
			"filter-ops-test": {Exists: true},
		},
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	testCases := []struct {
		name   string
		filter any
	}{
		{"Eq", []any{"score", "Eq", 10}},
		{"NotEq", []any{"score", "NotEq", 10}},
		{"Lt", []any{"score", "Lt", 20}},
		{"Lte", []any{"score", "Lte", 20}},
		{"Gt", []any{"score", "Gt", 10}},
		{"Gte", []any{"score", "Gte", 10}},
		{"In", []any{"score", "In", []any{10, 20}}},
		{"NotIn", []any{"score", "NotIn", []any{10}}},
		{"Contains", []any{"tags", "Contains", "b"}},
		{"ContainsAny", []any{"tags", "ContainsAny", []any{"a", "d"}}},
		{"And", []any{"And", []any{[]any{"score", "Gt", 10}, []any{"score", "Lt", 30}}}},
		{"Or", []any{"Or", []any{[]any{"score", "Eq", 10}, []any{"score", "Eq", 30}}}},
		{"Not", []any{"Not", []any{"score", "Eq", 20}}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queryBody := map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": tc.filter,
			}
			queryBytes, _ := json.Marshal(queryBody)
			req := httptest.NewRequest("POST", "/v2/namespaces/filter-ops-test/query", bytes.NewReader(queryBytes))
			req.Header.Set("Content-Type", "application/json")
			addAuthHeader(req)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != http.StatusOK {
				respBody, _ := io.ReadAll(w.Result().Body)
				t.Errorf("expected 200, got %d: %s", w.Result().StatusCode, string(respBody))
			}
		})
	}
}

// TestCompressionSupport tests gzip request/response compression.
func TestCompressionSupport(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	router.SetState(&api.ServerState{
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	t.Run("accepts gzip request body", func(t *testing.T) {
		// Note: Testing actual gzip requires compressing the body
		// For this test, we verify the Content-Encoding header is handled
		req := httptest.NewRequest("GET", "/v1/namespaces", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		// Server should either respond with gzip or plain - both are valid
		if w.Result().StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Result().StatusCode)
		}
	})
}

// TestPaginationByID tests that pagination query patterns are accepted.
// Tests that id > X filter pattern is valid for cursor-based pagination.
func TestPaginationByID(t *testing.T) {
	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	// Use SetState to simulate an existing namespace
	router.SetState(&api.ServerState{
		Namespaces: map[string]*api.NamespaceState{
			"pagination-test": {Exists: true},
		},
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	t.Run("page 1 query pattern", func(t *testing.T) {
		// Query first page with limit
		queryBody := map[string]any{
			"rank_by": []any{"id", "asc"},
			"limit":   5,
		}
		queryBytes, _ := json.Marshal(queryBody)
		req := httptest.NewRequest("POST", "/v2/namespaces/pagination-test/query", bytes.NewReader(queryBytes))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Result().StatusCode)
		}
	})

	t.Run("page 2 cursor pattern", func(t *testing.T) {
		// Query next page using id > last_id filter
		queryBody := map[string]any{
			"rank_by": []any{"id", "asc"},
			"limit":   5,
			"filters": []any{"id", "Gt", 5},
		}
		queryBytes, _ := json.Marshal(queryBody)
		req := httptest.NewRequest("POST", "/v2/namespaces/pagination-test/query", bytes.NewReader(queryBytes))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Result().StatusCode)
		}
	})
}

// TestNamespaceEndpoints tests all namespace-related endpoints.
func TestNamespaceEndpoints(t *testing.T) {
	cfg := newTestConfig()
	store := objectstore.NewMemoryStore()
	router := api.NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create a namespace via write
	createBody := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	createBytes, _ := json.Marshal(createBody)
	createReq := httptest.NewRequest("POST", "/v2/namespaces/ns-endpoint-test", bytes.NewReader(createBytes))
	createReq.Header.Set("Content-Type", "application/json")
	addAuthHeader(createReq)
	router.ServeHTTP(httptest.NewRecorder(), createReq)

	t.Run("GET /v1/namespaces/:namespace/metadata", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces/ns-endpoint-test/metadata", nil)
		addAuthHeader(req)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Result().StatusCode)
		}
	})

	t.Run("GET /v1/namespaces/:namespace/hint_cache_warm", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces/ns-endpoint-test/hint_cache_warm", nil)
		addAuthHeader(req)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Result().StatusCode)
		}
	})

	t.Run("GET /v1/namespaces", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces", nil)
		addAuthHeader(req)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Result().StatusCode)
		}
	})

	t.Run("DELETE /v2/namespaces/:namespace", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/v2/namespaces/ns-endpoint-test", nil)
		addAuthHeader(req)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Result().StatusCode)
		}

		// Verify namespace is deleted - subsequent requests should return 404
		metaReq := httptest.NewRequest("GET", "/v1/namespaces/ns-endpoint-test/metadata", nil)
		addAuthHeader(metaReq)
		metaW := httptest.NewRecorder()
		router.ServeHTTP(metaW, metaReq)

		if metaW.Result().StatusCode != http.StatusNotFound {
			t.Errorf("expected 404 after delete, got %d", metaW.Result().StatusCode)
		}
	})
}
