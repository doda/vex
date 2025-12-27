package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
