package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/config"
)

func TestMetricsEndpoint(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)
	defer router.Close()

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("expected Content-Type to contain text/plain, got %s", contentType)
	}

	body := w.Body.String()

	// Check that standard Go metrics are present
	if !strings.Contains(body, "go_") {
		t.Error("expected go_ metrics to be present")
	}

	// Check that vex metrics are declared (promauto registers them automatically)
	if !strings.Contains(body, "vex_") {
		t.Error("expected vex_ metrics to be present")
	}
}

func TestMetricsEndpointNoAuth(t *testing.T) {
	cfg := config.Default()
	cfg.AuthToken = "secret-token" // Require auth for regular endpoints
	router := NewRouter(cfg)
	defer router.Close()

	// Metrics endpoint should NOT require auth
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("metrics endpoint should not require auth, got status %d", resp.StatusCode)
	}
}
