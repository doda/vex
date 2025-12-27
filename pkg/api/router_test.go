package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/config"
)

func TestHealthEndpoint(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["status"] != "ok" {
		t.Errorf("expected status ok, got %s", result["status"])
	}
}

func TestAuthMiddleware(t *testing.T) {
	cfg := config.Default()
	cfg.AuthToken = "test-token"
	router := NewRouter(cfg)

	t.Run("missing auth returns 401", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %d", w.Result().StatusCode)
		}
	})

	t.Run("invalid token returns 401", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %d", w.Result().StatusCode)
		}
	})

	t.Run("valid token returns 200", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/namespaces", nil)
		req.Header.Set("Authorization", "Bearer test-token")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Result().StatusCode)
		}
	})
}

func TestListNamespaces(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	req := httptest.NewRequest("GET", "/v1/namespaces", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if _, ok := result["namespaces"]; !ok {
		t.Error("expected namespaces field in response")
	}
}

func TestErrorResponseFormat(t *testing.T) {
	cfg := config.Default()
	cfg.AuthToken = "test-token"
	router := NewRouter(cfg)

	req := httptest.NewRequest("GET", "/v1/namespaces", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	body, _ := io.ReadAll(w.Result().Body)
	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["status"] != "error" {
		t.Errorf("expected status error, got %s", result["status"])
	}
	if result["error"] == "" {
		t.Error("expected error message in response")
	}
}

func TestGzipRequestDecompression(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	body := []byte(`{"upsert_rows":[{"id":1,"name":"test"}]}`)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(body)
	gz.Close()

	req := httptest.NewRequest("POST", "/v2/namespaces/test", &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Result().StatusCode)
	}
}

func TestGzipResponseCompression(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Error("expected Content-Encoding: gzip header")
	}

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer gz.Close()

	body, _ := io.ReadAll(gz)
	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse decompressed response: %v", err)
	}

	if result["status"] != "ok" {
		t.Errorf("expected status ok, got %s", result["status"])
	}
}

func TestNamespaceValidation(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)

	t.Run("valid namespace names are accepted", func(t *testing.T) {
		validNames := []string{
			"test",
			"test123",
			"test-namespace",
			"test_namespace",
			"test.namespace",
			"Test-123_foo.bar",
			"a",
		}

		for _, name := range validNames {
			// Write to non-existent namespace should succeed (implicitly creates)
			req := httptest.NewRequest("POST", "/v2/namespaces/"+name, strings.NewReader(`{"upsert_rows":[]}`))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != http.StatusOK {
				t.Errorf("namespace %q should be valid, got status %d", name, w.Result().StatusCode)
			}
		}
	})

	t.Run("names longer than 128 chars are rejected with 400", func(t *testing.T) {
		longName := strings.Repeat("a", 129)
		req := httptest.NewRequest("POST", "/v2/namespaces/"+longName, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode != http.StatusBadRequest {
			t.Errorf("expected status 400 for name >128 chars, got %d", w.Result().StatusCode)
		}

		body, _ := io.ReadAll(w.Result().Body)
		var result map[string]string
		json.Unmarshal(body, &result)
		if result["status"] != "error" {
			t.Error("expected error status in response")
		}
	})

	t.Run("empty names are rejected", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v2/namespaces/", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Result().StatusCode == http.StatusOK {
			t.Error("empty namespace should not be accepted")
		}
	})

	t.Run("names with invalid characters are rejected", func(t *testing.T) {
		invalidNames := []string{
			"test namespace",
			"test/namespace",
			"test:namespace",
			"test*namespace",
			"test@namespace",
			"test#namespace",
		}

		for _, name := range invalidNames {
			req := httptest.NewRequest("POST", "/v2/namespaces/"+url.PathEscape(name), nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != http.StatusBadRequest {
				t.Errorf("namespace %q should be invalid, got status %d", name, w.Result().StatusCode)
			}
		}
	})

	t.Run("validation applies to all namespace endpoints", func(t *testing.T) {
		invalidName := url.PathEscape("invalid@name")
		endpoints := []struct {
			method string
			path   string
		}{
			{"GET", "/v1/namespaces/" + invalidName + "/metadata"},
			{"GET", "/v1/namespaces/" + invalidName + "/hint_cache_warm"},
			{"POST", "/v2/namespaces/" + invalidName},
			{"POST", "/v2/namespaces/" + invalidName + "/query"},
			{"DELETE", "/v2/namespaces/" + invalidName},
			{"POST", "/v1/namespaces/" + invalidName + "/_debug/recall"},
		}

		for _, ep := range endpoints {
			req := httptest.NewRequest(ep.method, ep.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Result().StatusCode != http.StatusBadRequest {
				t.Errorf("%s %s should return 400, got %d", ep.method, ep.path, w.Result().StatusCode)
			}
		}
	})
}
