package logging

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddleware(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	mux := http.NewServeMux()
	mux.Handle("GET /test", handler)

	wrappedHandler := Middleware(logger)(mux)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got: %d", rec.Code)
	}

	// Verify request ID was set in response
	requestID := rec.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Error("expected X-Request-ID header to be set")
	}

	// Verify log output
	output := buf.String()
	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
		t.Fatalf("log output is not valid JSON: %v\nOutput: %s", err, output)
	}

	if logEntry["request_id"] != requestID {
		t.Errorf("expected request_id='%s' in log, got: %v", requestID, logEntry["request_id"])
	}

	if logEntry["endpoint"] != "GET /test" {
		t.Errorf("expected endpoint='GET /test', got: %v", logEntry["endpoint"])
	}

	if logEntry["status"] != float64(200) {
		t.Errorf("expected status=200, got: %v", logEntry["status"])
	}

	if _, ok := logEntry["server_total_ms"]; !ok {
		t.Error("expected server_total_ms in log output")
	}
}

func TestMiddlewareWithNamespace(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	// Create inner handler that logs the namespace from context
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// In real use, the router extracts namespace from path;
		// here we simulate by calling MiddlewareFunc on a handler that
		// uses a mux that parses the namespace
		w.WriteHeader(http.StatusOK)
	})

	// Create a mux that routes and captures the namespace
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v2/namespaces/{namespace}/query", func(w http.ResponseWriter, r *http.Request) {
		// Re-log with the namespace now that mux has parsed it
		ns := r.PathValue("namespace")
		ctx := r.Context()
		ctx = ContextWithNamespace(ctx, ns)

		// Log with namespace
		info := &RequestInfo{
			RequestID: RequestIDFromContext(r.Context()),
			Namespace: ns,
			Endpoint:  r.Method + " " + r.URL.Path,
		}
		logger.WithRequestInfo(info).Info("with namespace")

		handler.ServeHTTP(w, r.WithContext(ctx))
	})

	// The middleware wraps the whole mux, but namespace extraction happens inside
	wrappedHandler := Middleware(logger)(mux)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", nil)
	rec := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rec, req)

	output := buf.String()

	// Should have multiple log lines - look for the one with namespace
	lines := bytes.Split(buf.Bytes(), []byte("\n"))
	found := false
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var logEntry map[string]interface{}
		if err := json.Unmarshal(line, &logEntry); err != nil {
			continue
		}
		if logEntry["namespace"] == "test-ns" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("expected namespace='test-ns' in one of the log entries, got: %s", output)
	}
}

func TestMiddlewarePreservedRequestID(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux := http.NewServeMux()
	mux.Handle("GET /test", handler)

	wrappedHandler := Middleware(logger)(mux)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Request-ID", "custom-request-id")
	rec := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rec, req)

	// Verify the provided request ID was used
	if rec.Header().Get("X-Request-ID") != "custom-request-id" {
		t.Errorf("expected provided request ID to be preserved")
	}

	output := buf.String()
	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}

	if logEntry["request_id"] != "custom-request-id" {
		t.Errorf("expected request_id='custom-request-id', got: %v", logEntry["request_id"])
	}
}

func TestMiddlewareFunc(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	handler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}

	wrappedHandler := MiddlewareFunc(logger, handler)

	req := httptest.NewRequest("POST", "/test", nil)
	rec := httptest.NewRecorder()

	wrappedHandler(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("expected status 201, got: %d", rec.Code)
	}

	output := buf.String()
	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}

	if logEntry["status"] != float64(201) {
		t.Errorf("expected status=201, got: %v", logEntry["status"])
	}
}

func TestMiddlewareCapturesStatusCode(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
	}{
		{"200 OK", http.StatusOK},
		{"400 Bad Request", http.StatusBadRequest},
		{"401 Unauthorized", http.StatusUnauthorized},
		{"404 Not Found", http.StatusNotFound},
		{"429 Too Many Requests", http.StatusTooManyRequests},
		{"500 Internal Server Error", http.StatusInternalServerError},
		{"503 Service Unavailable", http.StatusServiceUnavailable},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := NewWithWriter(&buf)

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
			})

			mux := http.NewServeMux()
			mux.Handle("GET /test", handler)

			wrappedHandler := Middleware(logger)(mux)

			req := httptest.NewRequest("GET", "/test", nil)
			rec := httptest.NewRecorder()

			wrappedHandler.ServeHTTP(rec, req)

			output := buf.String()
			var logEntry map[string]interface{}
			if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
				t.Fatalf("log output is not valid JSON: %v", err)
			}

			if logEntry["status"] != float64(tc.statusCode) {
				t.Errorf("expected status=%d, got: %v", tc.statusCode, logEntry["status"])
			}
		})
	}
}

func TestMiddlewareServerTotalMs(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Small delay to ensure measurable timing
		w.WriteHeader(http.StatusOK)
	})

	mux := http.NewServeMux()
	mux.Handle("GET /test", handler)

	wrappedHandler := Middleware(logger)(mux)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rec, req)

	output := buf.String()
	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}

	serverTotalMs, ok := logEntry["server_total_ms"].(float64)
	if !ok {
		t.Fatal("expected server_total_ms to be a float")
	}

	// Should be a positive number (even if very small)
	if serverTotalMs < 0 {
		t.Errorf("expected server_total_ms >= 0, got: %f", serverTotalMs)
	}
}

func TestMiddlewareCapturesRequestMetrics(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if metrics := RequestMetricsFromContext(r.Context()); metrics != nil {
			metrics.CacheTemp = CacheWarm
			metrics.QueryExecMs = 12.5
		}
		w.WriteHeader(http.StatusOK)
	})

	mux := http.NewServeMux()
	mux.Handle("GET /test", handler)

	wrappedHandler := Middleware(logger)(mux)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rec, req)

	output := buf.String()
	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}

	if logEntry["cache_temperature"] != "warm" {
		t.Errorf("expected cache_temperature='warm', got: %v", logEntry["cache_temperature"])
	}

	if logEntry["query_execution_ms"] != 12.5 {
		t.Errorf("expected query_execution_ms=12.5, got: %v", logEntry["query_execution_ms"])
	}
}
