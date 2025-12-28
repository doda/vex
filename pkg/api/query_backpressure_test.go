package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/config"
)

// TestStrongQueryReturns503WhenDisableBackpressureAndHighUnindexed tests that
// strong queries return 503 when disable_backpressure=true and unindexed > 2GB.
func TestStrongQueryReturns503WhenDisableBackpressureAndHighUnindexed(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {
				Exists:          true,
				UnindexedBytes:  MaxUnindexedBytes + 1,
				BackpressureOff: true,
			},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "default consistency (strong) returns 503",
			body:       `{"rank_by": ["id", "asc"]}`,
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "explicit strong consistency returns 503",
			body:       `{"rank_by": ["id", "asc"], "consistency": "strong"}`,
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "empty body (default strong) returns 503",
			body:       `{}`,
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, w.Result().StatusCode)
			}

			// Verify error message content
			body, _ := io.ReadAll(w.Result().Body)
			var result map[string]string
			json.Unmarshal(body, &result)

			if !strings.Contains(result["error"], "strong query") {
				t.Errorf("expected error to mention 'strong query', got %s", result["error"])
			}
			if !strings.Contains(result["error"], "disable_backpressure") {
				t.Errorf("expected error to mention 'disable_backpressure', got %s", result["error"])
			}
		})
	}
}

// TestEventualQueryWorksWhenDisableBackpressureAndHighUnindexed tests that
// eventual queries continue to work when disable_backpressure=true and unindexed > 2GB.
func TestEventualQueryWorksWhenDisableBackpressureAndHighUnindexed(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {
				Exists:          true,
				UnindexedBytes:  MaxUnindexedBytes + 1,
				BackpressureOff: true,
			},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("eventual consistency query works with high unindexed", func(t *testing.T) {
		body := `{"rank_by": ["id", "asc"], "consistency": "eventual"}`
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		// Should NOT get 503 - eventual queries should work
		if w.Result().StatusCode == http.StatusServiceUnavailable {
			t.Errorf("eventual query should not return 503, got %d", w.Result().StatusCode)
		}
	})

	t.Run("eventual vector query works with high unindexed", func(t *testing.T) {
		body := `{"rank_by": ["vector", "ANN", [1.0, 0.0]], "consistency": "eventual"}`
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		// Should NOT get 503 - eventual queries should work
		if w.Result().StatusCode == http.StatusServiceUnavailable {
			t.Errorf("eventual query should not return 503, got %d", w.Result().StatusCode)
		}
	})
}

// TestNoBackpressureErrorWhenThresholdNotExceeded tests that queries work normally
// when the unindexed bytes threshold is not exceeded.
func TestNoBackpressureErrorWhenThresholdNotExceeded(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {
				Exists:          true,
				UnindexedBytes:  MaxUnindexedBytes - 1, // Just under threshold
				BackpressureOff: true,
			},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("strong query works when below threshold", func(t *testing.T) {
		body := `{"rank_by": ["id", "asc"]}`
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		// Should NOT get 503 since we're below threshold
		if w.Result().StatusCode == http.StatusServiceUnavailable {
			t.Errorf("strong query should work when below threshold, got 503")
		}
	})
}

// TestNoBackpressureErrorWhenBackpressureEnabled tests that queries work normally
// when disable_backpressure is not set (backpressure is enabled).
func TestNoBackpressureErrorWhenBackpressureEnabled(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {
				Exists:          true,
				UnindexedBytes:  MaxUnindexedBytes + 1, // Above threshold
				BackpressureOff: false,                  // But backpressure is enabled
			},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("strong query works when backpressure enabled", func(t *testing.T) {
		body := `{"rank_by": ["id", "asc"]}`
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		// Should NOT get 503 since backpressure is not disabled
		// (normal behavior - writes would be rejected, but queries should work)
		if w.Result().StatusCode == http.StatusServiceUnavailable {
			t.Errorf("strong query should work when backpressure is enabled, got 503")
		}
	})
}

// TestQueryBackpressureCheckHappensAfterParsing tests that the backpressure check
// correctly interprets the consistency mode from the request body.
func TestQueryBackpressureCheckHappensAfterParsing(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {
				Exists:          true,
				UnindexedBytes:  MaxUnindexedBytes + 1,
				BackpressureOff: true,
			},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("invalid JSON still returns 400 not 503", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		// Should return 400 for invalid JSON, not 503
		if w.Result().StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 for invalid JSON, got %d", w.Result().StatusCode)
		}
	})
}
