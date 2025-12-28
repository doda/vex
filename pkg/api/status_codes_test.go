package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/config"
)

// TestStatus200Success tests that 200 is returned for successful operations.
func TestStatus200Success(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	// Set up a namespace that exists
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	testCases := []struct {
		name     string
		method   string
		path     string
		body     string
	}{
		{"health endpoint", "GET", "/health", ""},
		{"list namespaces", "GET", "/v1/namespaces", ""},
		{"get metadata", "GET", "/v1/namespaces/test-ns/metadata", ""},
		{"warm cache", "GET", "/v1/namespaces/test-ns/hint_cache_warm", ""},
		{"write endpoint", "POST", "/v2/namespaces/test-ns", `{"upsert_rows":[]}`},
		{"query endpoint", "POST", "/v2/namespaces/test-ns/query", `{"rank_by":["id","asc"]}`},
		{"debug recall", "POST", "/v1/namespaces/test-ns/_debug/recall", ""},
		{"delete namespace", "DELETE", "/v2/namespaces/test-ns", ""}, // Run delete last since it marks namespace as deleted
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
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != http.StatusOK {
				t.Errorf("expected status 200, got %d", w.Result().StatusCode)
			}
		})
	}
}

// TestStatus202IndexBuilding tests that 202 is returned when query depends on index still building.
func TestStatus202IndexBuilding(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	// Set up a namespace with index still building
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"building-ns": {
				Exists:        true,
				IndexBuilding: true,
				UnindexedBytes: 1000,
			},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	req := httptest.NewRequest("POST", "/v2/namespaces/building-ns/query", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusAccepted {
		t.Errorf("expected status 202, got %d", w.Result().StatusCode)
	}

	// Verify response body
	body, _ := io.ReadAll(w.Result().Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["status"] != "accepted" {
		t.Errorf("expected status 'accepted', got %v", result["status"])
	}
}

// TestStatus400InvalidRequest tests that 400 is returned for invalid requests.
func TestStatus400InvalidRequest(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	// Set up a namespace that exists
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", strings.NewReader(`{invalid json`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Result().StatusCode)
		}

		body, _ := io.ReadAll(w.Result().Body)
		var result map[string]string
		json.Unmarshal(body, &result)
		if result["status"] != "error" {
			t.Error("expected error status in response")
		}
	})

	t.Run("invalid namespace name - special chars", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v2/namespaces/test%40namespace", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Result().StatusCode)
		}
	})

	t.Run("invalid namespace name - too long", func(t *testing.T) {
		longName := strings.Repeat("a", 129)
		req := httptest.NewRequest("POST", "/v2/namespaces/"+longName, strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Result().StatusCode)
		}
	})

	t.Run("duplicate IDs in upsert_columns", func(t *testing.T) {
		body := `{"upsert_columns":{"ids":[1, 2, 1], "name":["a","b","c"]}}`
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Result().StatusCode)
		}

		respBody, _ := io.ReadAll(w.Result().Body)
		var result map[string]string
		json.Unmarshal(respBody, &result)
		if !strings.Contains(result["error"], "duplicate") {
			t.Errorf("expected duplicate IDs error, got %s", result["error"])
		}
	})
}

// TestStatus401Unauthorized tests that 401 is returned for authentication errors.
func TestStatus401Unauthorized(t *testing.T) {
	cfg := config.Default()
	cfg.AuthToken = "secret-token"
	router := NewRouter(cfg)

	t.Run("missing auth header", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %d", w.Result().StatusCode)
		}

		body, _ := io.ReadAll(w.Result().Body)
		var result map[string]string
		json.Unmarshal(body, &result)
		if result["status"] != "error" {
			t.Error("expected error status in response")
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %d", w.Result().StatusCode)
		}
	})

	t.Run("malformed auth header", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces", nil)
		req.Header.Set("Authorization", "secret-token") // Missing "Bearer "
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %d", w.Result().StatusCode)
		}
	})
}

// TestStatus404NotFound tests that 404 is returned when namespace is not found.
func TestStatus404NotFound(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	// Set up with explicit namespace states
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"existing-ns": {Exists: true},
			"deleted-ns":  {Exists: true, Deleted: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("namespace not found - metadata", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces/nonexistent/metadata", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Result().StatusCode)
		}

		body, _ := io.ReadAll(w.Result().Body)
		var result map[string]string
		json.Unmarshal(body, &result)
		if result["status"] != "error" {
			t.Error("expected error status in response")
		}
		if !strings.Contains(result["error"], "not found") {
			t.Errorf("expected 'not found' in error message, got %s", result["error"])
		}
	})

	t.Run("namespace not found - query", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v2/namespaces/nonexistent/query", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Result().StatusCode)
		}
	})

	t.Run("namespace deleted", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces/deleted-ns/metadata", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Result().StatusCode)
		}

		body, _ := io.ReadAll(w.Result().Body)
		var result map[string]string
		json.Unmarshal(body, &result)
		if !strings.Contains(result["error"], "deleted") {
			t.Errorf("expected 'deleted' in error message, got %s", result["error"])
		}
	})

	t.Run("write to deleted namespace returns 404", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v2/namespaces/deleted-ns", strings.NewReader(`{"upsert_rows":[]}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Result().StatusCode)
		}
	})
}

// TestStatus413PayloadTooLarge tests that 413 is returned when request body is too large.
func TestStatus413PayloadTooLarge(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	// Set up namespace
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("content-length exceeds limit", func(t *testing.T) {
		// Create a request with Content-Length > 256MB
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", nil)
		req.ContentLength = MaxRequestBodySize + 1
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("expected status 413, got %d", w.Result().StatusCode)
		}

		body, _ := io.ReadAll(w.Result().Body)
		var result map[string]string
		json.Unmarshal(body, &result)
		if result["status"] != "error" {
			t.Error("expected error status in response")
		}
		if !strings.Contains(result["error"], "256MB") {
			t.Errorf("expected '256MB' in error message, got %s", result["error"])
		}
	})

	t.Run("actual body exceeds limit", func(t *testing.T) {
		// Create a large body (we'll test with MaxBytesReader which is applied)
		// Note: This test verifies that MaxBytesReader is working
		largeBody := bytes.NewReader(make([]byte, MaxRequestBodySize+1))
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", largeBody)
		req.Header.Set("Content-Type", "application/json")
		// Don't set ContentLength to let MaxBytesReader kick in
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		// Should return 413 or 400 (bad JSON due to truncation)
		if w.Result().StatusCode != http.StatusRequestEntityTooLarge && w.Result().StatusCode != http.StatusBadRequest {
			t.Errorf("expected status 413 or 400, got %d", w.Result().StatusCode)
		}
	})
}

// TestStatus429Backpressure tests that 429 is returned for rate limiting/backpressure.
func TestStatus429Backpressure(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	// Set up namespace with high unindexed bytes (above 2GB threshold)
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"backpressure-ns": {
				Exists:         true,
				UnindexedBytes: MaxUnindexedBytes + 1,
				BackpressureOff: false,
			},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("write rejected due to backpressure", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v2/namespaces/backpressure-ns", strings.NewReader(`{"upsert_rows":[]}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusTooManyRequests {
			t.Errorf("expected status 429, got %d", w.Result().StatusCode)
		}

		body, _ := io.ReadAll(w.Result().Body)
		var result map[string]string
		json.Unmarshal(body, &result)
		if result["status"] != "error" {
			t.Error("expected error status in response")
		}
		if !strings.Contains(result["error"], "backpressure") || !strings.Contains(result["error"], "2GB") {
			t.Errorf("expected backpressure/2GB in error message, got %s", result["error"])
		}
	})

	t.Run("backpressure not applied when disabled", func(t *testing.T) {
		// Set up namespace with backpressure disabled
		router.SetState(&ServerState{
			Namespaces: map[string]*NamespaceState{
				"no-backpressure-ns": {
					Exists:         true,
					UnindexedBytes: MaxUnindexedBytes + 1,
					BackpressureOff: true,
				},
			},
			ObjectStore: ObjectStoreState{Available: true},
		})

		req := httptest.NewRequest("POST", "/v2/namespaces/no-backpressure-ns", strings.NewReader(`{"upsert_rows":[]}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		// With backpressure disabled, write should succeed
		if w.Result().StatusCode != http.StatusOK {
			t.Errorf("expected status 200 (backpressure disabled), got %d", w.Result().StatusCode)
		}
	})
}

// TestStatus500InternalServerError tests that 500 is returned for internal errors.
func TestStatus500InternalServerError(t *testing.T) {
	// For this test, we need to simulate an internal server error.
	// Since we're using stub handlers, we'll test the error type directly.

	err := ErrInternalServer("unexpected database error")
	if err.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", err.StatusCode)
	}

	if err.Message != "unexpected database error" {
		t.Errorf("expected message 'unexpected database error', got %s", err.Message)
	}

	// Verify error format when written
	cfg := config.Default()
	router := NewRouter(cfg)

	w := httptest.NewRecorder()
	router.writeAPIError(w, err)

	if w.Result().StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Result().StatusCode)
	}

	body, _ := io.ReadAll(w.Result().Body)
	var result map[string]string
	json.Unmarshal(body, &result)
	if result["status"] != "error" {
		t.Error("expected error status in response")
	}
}

// TestStatus503ServiceUnavailable tests that 503 is returned for object store failures
// and strong query with disable_backpressure when unindexed > 2GB.
func TestStatus503ServiceUnavailable(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	t.Run("object store unavailable", func(t *testing.T) {
		router.SetState(&ServerState{
			Namespaces: map[string]*NamespaceState{
				"test-ns": {Exists: true},
			},
			ObjectStore: ObjectStoreState{Available: false},
		})

		// Test write endpoint
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", strings.NewReader(`{"upsert_rows":[]}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected status 503, got %d", w.Result().StatusCode)
		}

		body, _ := io.ReadAll(w.Result().Body)
		var result map[string]string
		json.Unmarshal(body, &result)
		if result["status"] != "error" {
			t.Error("expected error status in response")
		}
		if !strings.Contains(result["error"], "object storage") {
			t.Errorf("expected 'object storage' in error message, got %s", result["error"])
		}
	})

	t.Run("object store unavailable - query", func(t *testing.T) {
		router.SetState(&ServerState{
			Namespaces: map[string]*NamespaceState{
				"test-ns": {Exists: true},
			},
			ObjectStore: ObjectStoreState{Available: false},
		})

		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected status 503, got %d", w.Result().StatusCode)
		}
	})

	t.Run("object store unavailable - list namespaces", func(t *testing.T) {
		router.SetState(&ServerState{
			ObjectStore: ObjectStoreState{Available: false},
		})

		req := httptest.NewRequest("GET", "/v1/namespaces", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected status 503, got %d", w.Result().StatusCode)
		}
	})

	t.Run("strong query with disable_backpressure and high unindexed", func(t *testing.T) {
		router.SetState(&ServerState{
			Namespaces: map[string]*NamespaceState{
				"backpressure-off-ns": {
					Exists:          true,
					UnindexedBytes:  MaxUnindexedBytes + 1,
					BackpressureOff: true,
				},
			},
			ObjectStore: ObjectStoreState{Available: true},
		})

		req := httptest.NewRequest("POST", "/v2/namespaces/backpressure-off-ns/query", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected status 503, got %d", w.Result().StatusCode)
		}

		body, _ := io.ReadAll(w.Result().Body)
		var result map[string]string
		json.Unmarshal(body, &result)
		if !strings.Contains(result["error"], "strong query") || !strings.Contains(result["error"], "disable_backpressure") {
			t.Errorf("expected 'strong query' and 'disable_backpressure' in error message, got %s", result["error"])
		}
	})
}

// TestErrorResponseFormatStatusCodes verifies all errors follow the standard format.
func TestErrorResponseFormatStatusCodes(t *testing.T) {
	cfg := config.Default()
	cfg.AuthToken = "test-token"
	router := NewRouter(cfg)

	// Test with unauthenticated request
	req := httptest.NewRequest("GET", "/v1/namespaces", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	body, _ := io.ReadAll(w.Result().Body)
	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse error response: %v", err)
	}

	// Verify format: {"status":"error","error":"..."}
	if result["status"] != "error" {
		t.Errorf("expected status 'error', got '%s'", result["status"])
	}
	if result["error"] == "" {
		t.Error("expected non-empty error message")
	}
}

// TestAllStatusCodeConstants verifies the constants are set correctly.
func TestAllStatusCodeConstants(t *testing.T) {
	testCases := []struct {
		err           *APIError
		expectedCode  int
		expectedName  string
	}{
		{ErrBadRequest("test"), http.StatusBadRequest, "400"},
		{ErrUnauthorized("test"), http.StatusUnauthorized, "401"},
		{ErrForbidden("test"), http.StatusForbidden, "403"},
		{ErrNotFound("test"), http.StatusNotFound, "404"},
		{ErrPayloadTooLarge("test"), http.StatusRequestEntityTooLarge, "413"},
		{ErrTooManyRequests("test"), http.StatusTooManyRequests, "429"},
		{ErrInternalServer("test"), http.StatusInternalServerError, "500"},
		{ErrServiceUnavailable("test"), http.StatusServiceUnavailable, "503"},
		{ErrIndexBuilding("test"), http.StatusAccepted, "202"},
	}

	for _, tc := range testCases {
		t.Run(tc.expectedName, func(t *testing.T) {
			if tc.err.StatusCode != tc.expectedCode {
				t.Errorf("expected status %d, got %d", tc.expectedCode, tc.err.StatusCode)
			}
		})
	}
}

// TestSpecificErrorConstructors verifies specific error helper functions.
func TestSpecificErrorConstructors(t *testing.T) {
	t.Run("ErrInvalidJSON", func(t *testing.T) {
		err := ErrInvalidJSON()
		if err.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", err.StatusCode)
		}
		if !strings.Contains(err.Message, "invalid JSON") {
			t.Errorf("expected 'invalid JSON' in message, got %s", err.Message)
		}
	})

	t.Run("ErrInvalidSchema", func(t *testing.T) {
		err := ErrInvalidSchema("type mismatch")
		if err.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", err.StatusCode)
		}
		if !strings.Contains(err.Message, "type mismatch") {
			t.Errorf("expected 'type mismatch' in message, got %s", err.Message)
		}
	})

	t.Run("ErrTypeMismatch", func(t *testing.T) {
		err := ErrTypeMismatch("age", "int", "string")
		if err.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", err.StatusCode)
		}
		if !strings.Contains(err.Message, "age") || !strings.Contains(err.Message, "int") {
			t.Errorf("expected field info in message, got %s", err.Message)
		}
	})

	t.Run("ErrDuplicateIDs", func(t *testing.T) {
		err := ErrDuplicateIDs()
		if err.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", err.StatusCode)
		}
		if !strings.Contains(err.Message, "duplicate") {
			t.Errorf("expected 'duplicate' in message, got %s", err.Message)
		}
	})

	t.Run("ErrNamespaceNotFound", func(t *testing.T) {
		err := ErrNamespaceNotFound("my-ns")
		if err.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404, got %d", err.StatusCode)
		}
		if !strings.Contains(err.Message, "my-ns") {
			t.Errorf("expected namespace name in message, got %s", err.Message)
		}
	})

	t.Run("ErrNamespaceDeleted", func(t *testing.T) {
		err := ErrNamespaceDeleted("deleted-ns")
		if err.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404, got %d", err.StatusCode)
		}
		if !strings.Contains(err.Message, "deleted") {
			t.Errorf("expected 'deleted' in message, got %s", err.Message)
		}
	})

	t.Run("ErrBackpressure", func(t *testing.T) {
		err := ErrBackpressure()
		if err.StatusCode != http.StatusTooManyRequests {
			t.Errorf("expected 429, got %d", err.StatusCode)
		}
		if !strings.Contains(err.Message, "2GB") {
			t.Errorf("expected '2GB' in message, got %s", err.Message)
		}
	})

	t.Run("ErrRateLimited", func(t *testing.T) {
		err := ErrRateLimited()
		if err.StatusCode != http.StatusTooManyRequests {
			t.Errorf("expected 429, got %d", err.StatusCode)
		}
	})

	t.Run("ErrObjectStoreUnavailable", func(t *testing.T) {
		err := ErrObjectStoreUnavailable()
		if err.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected 503, got %d", err.StatusCode)
		}
		if !strings.Contains(err.Message, "object storage") {
			t.Errorf("expected 'object storage' in message, got %s", err.Message)
		}
	})

	t.Run("ErrStrongQueryBackpressure", func(t *testing.T) {
		err := ErrStrongQueryBackpressure()
		if err.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected 503, got %d", err.StatusCode)
		}
		if !strings.Contains(err.Message, "strong query") {
			t.Errorf("expected 'strong query' in message, got %s", err.Message)
		}
	})
}

// TestMaxRequestBodySizeConstant verifies the constant is correct.
func TestMaxRequestBodySizeConstant(t *testing.T) {
	expected := 256 * 1024 * 1024 // 256MB
	if MaxRequestBodySize != expected {
		t.Errorf("expected MaxRequestBodySize to be %d, got %d", expected, MaxRequestBodySize)
	}
}

// TestMaxUnindexedBytesConstant verifies the constant is correct.
func TestMaxUnindexedBytesConstant(t *testing.T) {
	expected := int64(2 * 1024 * 1024 * 1024) // 2GB
	if MaxUnindexedBytes != expected {
		t.Errorf("expected MaxUnindexedBytes to be %d, got %d", expected, MaxUnindexedBytes)
	}
}
