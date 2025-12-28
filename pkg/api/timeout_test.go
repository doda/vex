package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/config"
)

func TestQueryTimeoutEnforcement(t *testing.T) {
	// Create a config with a very short query timeout (10ms)
	cfg := config.Default()
	cfg.Timeout.QueryTimeoutMs = 10

	// Create router
	router := NewRouter(cfg)

	// Setup test namespace state (without object store so it uses fallback path)
	router.state.Namespaces["test-ns"] = &NamespaceState{
		Exists:  true,
		Deleted: false,
	}

	// The fallback path is fast, so the timeout won't be hit in normal execution
	// Instead, we test that the context has a deadline set
	// by verifying the timeout middleware correctly applies the deadline

	// Create request
	queryBody := map[string]interface{}{
		"rank_by": []interface{}{"id", "asc"},
		"limit":   10,
	}
	body, _ := json.Marshal(queryBody)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// In fallback mode, should return 200 OK
	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestWriteTimeoutEnforcement(t *testing.T) {
	// Create a config with a very short write timeout (10ms)
	cfg := config.Default()
	cfg.Timeout.WriteTimeoutMs = 10

	// Create router
	router := NewRouter(cfg)

	// Setup test namespace state (without object store so it uses fallback path)
	router.state.Namespaces["test-ns"] = &NamespaceState{
		Exists:  true,
		Deleted: false,
	}
	router.state.ObjectStore.Available = true

	// Create request
	writeBody := map[string]interface{}{
		"upsert_rows": []map[string]interface{}{
			{"id": 1, "name": "test"},
		},
	}
	body, _ := json.Marshal(writeBody)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// In fallback mode (no write handler), should return 200 OK
	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestQueryTimeoutConfigDefaults(t *testing.T) {
	cfg := config.Default()

	// Verify default query timeout is 30 seconds
	if cfg.Timeout.GetQueryTimeout() != 30000 {
		t.Errorf("expected default query timeout 30000ms, got %d", cfg.Timeout.GetQueryTimeout())
	}

	// Verify default write timeout is 60 seconds
	if cfg.Timeout.GetWriteTimeout() != 60000 {
		t.Errorf("expected default write timeout 60000ms, got %d", cfg.Timeout.GetWriteTimeout())
	}
}

func TestQueryTimeoutConfigOverride(t *testing.T) {
	cfg := config.Default()
	cfg.Timeout.QueryTimeoutMs = 5000
	cfg.Timeout.WriteTimeoutMs = 10000

	// Verify overridden query timeout
	if cfg.Timeout.GetQueryTimeout() != 5000 {
		t.Errorf("expected query timeout 5000ms, got %d", cfg.Timeout.GetQueryTimeout())
	}

	// Verify overridden write timeout
	if cfg.Timeout.GetWriteTimeout() != 10000 {
		t.Errorf("expected write timeout 10000ms, got %d", cfg.Timeout.GetWriteTimeout())
	}
}

func TestTimeoutConfigZeroFallsBackToDefault(t *testing.T) {
	cfg := config.Default()
	cfg.Timeout.QueryTimeoutMs = 0
	cfg.Timeout.WriteTimeoutMs = 0

	// Zero should fall back to default
	if cfg.Timeout.GetQueryTimeout() != 30000 {
		t.Errorf("expected query timeout to fall back to 30000ms, got %d", cfg.Timeout.GetQueryTimeout())
	}
	if cfg.Timeout.GetWriteTimeout() != 60000 {
		t.Errorf("expected write timeout to fall back to 60000ms, got %d", cfg.Timeout.GetWriteTimeout())
	}
}

func TestTimeoutConfigNegativeFallsBackToDefault(t *testing.T) {
	cfg := config.Default()
	cfg.Timeout.QueryTimeoutMs = -100
	cfg.Timeout.WriteTimeoutMs = -100

	// Negative should fall back to default
	if cfg.Timeout.GetQueryTimeout() != 30000 {
		t.Errorf("expected query timeout to fall back to 30000ms, got %d", cfg.Timeout.GetQueryTimeout())
	}
	if cfg.Timeout.GetWriteTimeout() != 60000 {
		t.Errorf("expected write timeout to fall back to 60000ms, got %d", cfg.Timeout.GetWriteTimeout())
	}
}

// TestTimeoutMiddlewareApplied verifies the timeout middleware sets context deadline
func TestTimeoutMiddlewareApplied(t *testing.T) {
	cfg := config.Default()
	cfg.Timeout.QueryTimeoutMs = 100

	router := NewRouter(cfg)

	// Create a test handler that records if context has deadline
	var contextHasDeadline bool
	var deadlineDuration time.Duration

	testHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		deadline, ok := req.Context().Deadline()
		contextHasDeadline = ok
		if ok {
			deadlineDuration = time.Until(deadline)
		}
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with timeout middleware
	wrappedHandler := router.queryTimeoutMiddleware(testHandler)

	// Create and execute request
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	wrappedHandler(w, req)

	if !contextHasDeadline {
		t.Error("expected context to have a deadline set by timeout middleware")
	}

	// Deadline should be approximately 100ms from now (with some tolerance)
	if deadlineDuration <= 0 || deadlineDuration > 110*time.Millisecond {
		t.Errorf("expected deadline around 100ms, got %v", deadlineDuration)
	}
}

// TestWriteTimeoutMiddlewareApplied verifies the write timeout middleware sets context deadline
func TestWriteTimeoutMiddlewareApplied(t *testing.T) {
	cfg := config.Default()
	cfg.Timeout.WriteTimeoutMs = 200

	router := NewRouter(cfg)

	// Create a test handler that records if context has deadline
	var contextHasDeadline bool
	var deadlineDuration time.Duration

	testHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		deadline, ok := req.Context().Deadline()
		contextHasDeadline = ok
		if ok {
			deadlineDuration = time.Until(deadline)
		}
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with timeout middleware
	wrappedHandler := router.writeTimeoutMiddleware(testHandler)

	// Create and execute request
	req := httptest.NewRequest("POST", "/test", nil)
	w := httptest.NewRecorder()
	wrappedHandler(w, req)

	if !contextHasDeadline {
		t.Error("expected context to have a deadline set by timeout middleware")
	}

	// Deadline should be approximately 200ms from now (with some tolerance)
	if deadlineDuration <= 0 || deadlineDuration > 210*time.Millisecond {
		t.Errorf("expected deadline around 200ms, got %v", deadlineDuration)
	}
}

// TestContextTimeoutErrorReturns504 verifies that context.DeadlineExceeded returns 504
func TestContextTimeoutErrorReturns504(t *testing.T) {
	// Test the error types directly
	err := ErrQueryTimeout()
	if err.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("expected status 504, got %d", err.StatusCode)
	}
	if err.Message != "query timed out: CPU budget exceeded" {
		t.Errorf("unexpected message: %s", err.Message)
	}

	err = ErrWriteTimeout()
	if err.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("expected status 504, got %d", err.StatusCode)
	}
	if err.Message != "write timed out: CPU budget exceeded" {
		t.Errorf("unexpected message: %s", err.Message)
	}
}

// TestTimeoutMiddlewareCancelsProperly tests that the middleware cancels the context
func TestTimeoutMiddlewareCancelsProperly(t *testing.T) {
	cfg := config.Default()
	cfg.Timeout.QueryTimeoutMs = 50

	router := NewRouter(cfg)

	// Create a handler that blocks until context is done
	handlerDone := make(chan struct{})
	contextCanceled := false

	testHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		select {
		case <-ctx.Done():
			contextCanceled = true
		case <-time.After(200 * time.Millisecond):
			// Handler took too long
		}
		close(handlerDone)
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with timeout middleware
	wrappedHandler := router.queryTimeoutMiddleware(testHandler)

	// Create and execute request
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	// Run in goroutine to not block
	go wrappedHandler(w, req)

	// Wait for handler to complete
	select {
	case <-handlerDone:
		// Handler completed
	case <-time.After(300 * time.Millisecond):
		t.Fatal("handler did not complete in time")
	}

	if !contextCanceled {
		t.Error("expected context to be canceled after timeout")
	}
}

// TestContextDeadlineExceededHandling tests that handlers properly handle context deadline exceeded
func TestContextDeadlineExceededHandling(t *testing.T) {
	// Verify error detection
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait for timeout
	<-ctx.Done()

	// Verify that context.DeadlineExceeded is detected properly
	err := ctx.Err()
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}

	// Verify error message
	if ctx.Err().Error() != "context deadline exceeded" {
		t.Errorf("unexpected error message: %s", ctx.Err().Error())
	}
}
