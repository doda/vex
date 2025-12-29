package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

)

// TestAllErrorsReturnCorrectFormat verifies all errors return {"status":"error","error":"..."}.
func TestAllErrorsReturnCorrectFormat(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = "secret"
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"exists":       {Exists: true},
			"deleted":      {Exists: true, Deleted: true},
			"backpressure": {Exists: true, UnindexedBytes: MaxUnindexedBytes + 1},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	testCases := []struct {
		name           string
		method         string
		path           string
		body           string
		auth           string
		expectedStatus int
	}{
		{
			name:           "unauthorized - missing auth",
			method:         "GET",
			path:           "/v1/namespaces",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "unauthorized - invalid token",
			method:         "GET",
			path:           "/v1/namespaces",
			auth:           "Bearer wrong",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "unauthorized - malformed header",
			method:         "GET",
			path:           "/v1/namespaces",
			auth:           "Basic secret",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "bad request - invalid namespace",
			method:         "POST",
			path:           "/v2/namespaces/invalid@name",
			auth:           "Bearer secret",
			body:           "{}",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "bad request - namespace too long",
			method:         "POST",
			path:           "/v2/namespaces/" + strings.Repeat("a", 129),
			auth:           "Bearer secret",
			body:           "{}",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "bad request - invalid JSON",
			method:         "POST",
			path:           "/v2/namespaces/exists",
			auth:           "Bearer secret",
			body:           "{invalid",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "bad request - duplicate IDs",
			method:         "POST",
			path:           "/v2/namespaces/exists",
			auth:           "Bearer secret",
			body:           `{"upsert_columns":{"ids":[1,1]}}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "not found - namespace does not exist",
			method:         "GET",
			path:           "/v1/namespaces/nonexistent/metadata",
			auth:           "Bearer secret",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "not found - namespace deleted",
			method:         "GET",
			path:           "/v1/namespaces/deleted/metadata",
			auth:           "Bearer secret",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "too many requests - backpressure",
			method:         "POST",
			path:           "/v2/namespaces/backpressure",
			auth:           "Bearer secret",
			body:           `{"upsert_rows":[]}`,
			expectedStatus: http.StatusTooManyRequests,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}

			req := httptest.NewRequest(tc.method, tc.path, body)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			resp := w.Result()
			if resp.StatusCode != tc.expectedStatus {
				t.Errorf("expected status %d, got %d", tc.expectedStatus, resp.StatusCode)
			}

			respBody, _ := io.ReadAll(resp.Body)
			var result map[string]interface{}
			if err := json.Unmarshal(respBody, &result); err != nil {
				t.Fatalf("failed to parse response as JSON: %v, body: %s", err, string(respBody))
			}

			// Verify format: {"status":"error","error":"..."}
			if result["status"] != "error" {
				t.Errorf("expected status='error', got '%v'", result["status"])
			}

			errMsg, ok := result["error"].(string)
			if !ok {
				t.Errorf("expected error to be a string, got %T", result["error"])
			}
			if errMsg == "" {
				t.Error("expected non-empty error message")
			}
		})
	}
}

// TestErrorMessagesAreDescriptive verifies that error messages provide useful information.
func TestErrorMessagesAreDescriptive(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = "secret"
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"exists":            {Exists: true},
			"deleted":           {Exists: true, Deleted: true},
			"backpressure":      {Exists: true, UnindexedBytes: MaxUnindexedBytes + 1},
			"backpressure-off": {Exists: true, UnindexedBytes: MaxUnindexedBytes + 1, BackpressureOff: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	testCases := []struct {
		name              string
		method            string
		path              string
		body              string
		auth              string
		mustContain       []string // error message must contain all of these
	}{
		{
			name:        "missing auth describes the issue",
			method:      "GET",
			path:        "/v1/namespaces",
			mustContain: []string{"Authorization"},
		},
		{
			name:        "invalid token is clear",
			method:      "GET",
			path:        "/v1/namespaces",
			auth:        "Bearer wrong",
			mustContain: []string{"invalid", "token"},
		},
		{
			name:        "invalid namespace explains the pattern",
			method:      "POST",
			path:        "/v2/namespaces/bad@name",
			auth:        "Bearer secret",
			body:        "{}",
			mustContain: []string{"namespace"},
		},
		{
			name:        "namespace too long mentions limit",
			method:      "POST",
			path:        "/v2/namespaces/" + strings.Repeat("x", 129),
			auth:        "Bearer secret",
			body:        "{}",
			mustContain: []string{"128"},
		},
		{
			name:        "invalid JSON is explicit",
			method:      "POST",
			path:        "/v2/namespaces/exists",
			auth:        "Bearer secret",
			body:        "not json",
			mustContain: []string{"invalid", "JSON"},
		},
		{
			name:        "duplicate IDs mentions duplicates",
			method:      "POST",
			path:        "/v2/namespaces/exists",
			auth:        "Bearer secret",
			body:        `{"upsert_columns":{"ids":[1,1]}}`,
			mustContain: []string{"duplicate"},
		},
		{
			name:        "namespace not found includes namespace name",
			method:      "GET",
			path:        "/v1/namespaces/my-missing-ns/metadata",
			auth:        "Bearer secret",
			mustContain: []string{"my-missing-ns", "not found"},
		},
		{
			name:        "deleted namespace indicates deletion",
			method:      "GET",
			path:        "/v1/namespaces/deleted/metadata",
			auth:        "Bearer secret",
			mustContain: []string{"deleted"},
		},
		{
			name:        "backpressure mentions threshold",
			method:      "POST",
			path:        "/v2/namespaces/backpressure",
			auth:        "Bearer secret",
			body:        `{}`,
			mustContain: []string{"2GB"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}

			req := httptest.NewRequest(tc.method, tc.path, body)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			respBody, _ := io.ReadAll(w.Result().Body)
			var result map[string]interface{}
			json.Unmarshal(respBody, &result)

			errMsg, _ := result["error"].(string)
			errLower := strings.ToLower(errMsg)

			for _, keyword := range tc.mustContain {
				if !strings.Contains(errLower, strings.ToLower(keyword)) {
					t.Errorf("expected error to contain '%s', got: %s", keyword, errMsg)
				}
			}
		})
	}
}

// TestServiceUnavailableErrors tests 503 error format.
func TestServiceUnavailableErrors(t *testing.T) {
	cfg := testConfig()
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"exists": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: false},
	})

	testCases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"list namespaces", "GET", "/v1/namespaces", ""},
		{"get metadata", "GET", "/v1/namespaces/exists/metadata", ""},
		{"write", "POST", "/v2/namespaces/exists", "{}"},
		{"query", "POST", "/v2/namespaces/exists/query", "{}"},
		{"delete", "DELETE", "/v2/namespaces/exists", ""},
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

			w := httptest.NewRecorder()
			router.ServeAuthed(w, req)

			if w.Result().StatusCode != http.StatusServiceUnavailable {
				t.Errorf("expected 503, got %d", w.Result().StatusCode)
			}

			respBody, _ := io.ReadAll(w.Result().Body)
			var result map[string]interface{}
			json.Unmarshal(respBody, &result)

			if result["status"] != "error" {
				t.Errorf("expected status='error', got '%v'", result["status"])
			}

			errMsg, _ := result["error"].(string)
			if !strings.Contains(strings.ToLower(errMsg), "object storage") {
				t.Errorf("expected error to mention 'object storage', got: %s", errMsg)
			}
		})
	}
}

// TestStrongQueryBackpressureError tests 503 for strong query with backpressure disabled.
func TestStrongQueryBackpressureError(t *testing.T) {
	cfg := testConfig()
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"bp-off": {
				Exists:          true,
				UnindexedBytes:  MaxUnindexedBytes + 1,
				BackpressureOff: true,
			},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	req := httptest.NewRequest("POST", "/v2/namespaces/bp-off/query", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Result().StatusCode)
	}

	body, _ := io.ReadAll(w.Result().Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	if result["status"] != "error" {
		t.Errorf("expected status='error', got '%v'", result["status"])
	}

	errMsg, _ := result["error"].(string)
	if !strings.Contains(errMsg, "strong query") {
		t.Errorf("expected error to mention 'strong query', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "disable_backpressure") {
		t.Errorf("expected error to mention 'disable_backpressure', got: %s", errMsg)
	}
}

// TestPayloadTooLargeError tests 413 error format.
func TestPayloadTooLargeError(t *testing.T) {
	cfg := testConfig()
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"exists": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	req := httptest.NewRequest("POST", "/v2/namespaces/exists", nil)
	req.ContentLength = MaxRequestBodySize + 1
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	if w.Result().StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413, got %d", w.Result().StatusCode)
	}

	body, _ := io.ReadAll(w.Result().Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	if result["status"] != "error" {
		t.Errorf("expected status='error', got '%v'", result["status"])
	}

	errMsg, _ := result["error"].(string)
	if !strings.Contains(errMsg, "256MB") {
		t.Errorf("expected error to mention '256MB', got: %s", errMsg)
	}
}

// TestErrorResponseHasOnlyExpectedFields verifies no extra fields in error responses.
func TestErrorResponseHasOnlyExpectedFields(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = "test"
	router := NewRouter(cfg)

	req := httptest.NewRequest("GET", "/v1/namespaces", nil)
	w := httptest.NewRecorder()
	router.ServeAuthed(w, req)

	body, _ := io.ReadAll(w.Result().Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	expectedFields := map[string]bool{"status": true, "error": true}
	for key := range result {
		if !expectedFields[key] {
			t.Errorf("unexpected field in error response: %s", key)
		}
	}

	if len(result) != 2 {
		t.Errorf("expected exactly 2 fields (status, error), got %d", len(result))
	}
}

// TestAllEndpointsHaveProperErrorFormat ensures every endpoint returns proper error format.
func TestAllEndpointsHaveProperErrorFormat(t *testing.T) {
	cfg := testConfig()
	cfg.AuthToken = "secret"
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		ObjectStore: ObjectStoreState{Available: true},
	})

	endpoints := []struct {
		method string
		path   string
		body   string
	}{
		{"GET", "/v1/namespaces", ""},
		{"GET", "/v1/namespaces/test/metadata", ""},
		{"GET", "/v1/namespaces/test/hint_cache_warm", ""},
		{"POST", "/v2/namespaces/test", "{}"},
		{"POST", "/v2/namespaces/test/query", "{}"},
		{"DELETE", "/v2/namespaces/test", ""},
		{"POST", "/v1/namespaces/test/_debug/recall", ""},
	}

	for _, ep := range endpoints {
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			var body io.Reader
			if ep.body != "" {
				body = strings.NewReader(ep.body)
			}

			// Test without auth (should return 401)
			req := httptest.NewRequest(ep.method, ep.path, body)
			if ep.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}

			w := httptest.NewRecorder()
			router.ServeAuthed(w, req)

			if w.Result().StatusCode != http.StatusUnauthorized {
				t.Errorf("expected 401 without auth, got %d", w.Result().StatusCode)
				return
			}

			respBody, _ := io.ReadAll(w.Result().Body)
			var result map[string]interface{}
			if err := json.Unmarshal(respBody, &result); err != nil {
				t.Fatalf("failed to parse response: %v", err)
			}

			if result["status"] != "error" {
				t.Errorf("expected status='error', got '%v'", result["status"])
			}
			if _, ok := result["error"].(string); !ok {
				t.Error("expected error to be a string")
			}
		})
	}
}
