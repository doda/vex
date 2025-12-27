package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNewLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	logger.Info("test message")

	output := buf.String()
	if !strings.Contains(output, `"msg":"test message"`) {
		t.Errorf("expected log message in output, got: %s", output)
	}

	// Verify it's valid JSON
	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}

	if logEntry["msg"] != "test message" {
		t.Errorf("expected msg='test message', got: %v", logEntry["msg"])
	}
}

func TestLoggerWithRequestInfo(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	info := &RequestInfo{
		RequestID:     "test-req-123",
		Namespace:     "my-namespace",
		Endpoint:      "POST /v2/namespaces/my-namespace/query",
		CacheTemp:     CacheHot,
		ServerTotalMs: 42.5,
		QueryExecMs:   38.2,
	}

	logger.WithRequestInfo(info).Info("request completed")

	output := buf.String()

	// Verify it's valid JSON
	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}

	tests := []struct {
		key      string
		expected interface{}
	}{
		{"request_id", "test-req-123"},
		{"namespace", "my-namespace"},
		{"endpoint", "POST /v2/namespaces/my-namespace/query"},
		{"cache_temperature", "hot"},
		{"server_total_ms", 42.5},
		{"query_execution_ms", 38.2},
	}

	for _, tc := range tests {
		got := logEntry[tc.key]
		if got != tc.expected {
			t.Errorf("%s: expected %v, got %v", tc.key, tc.expected, got)
		}
	}
}

func TestLoggerWithContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	ctx := context.Background()
	ctx = ContextWithRequestID(ctx, "ctx-req-456")
	ctx = ContextWithNamespace(ctx, "ctx-namespace")
	ctx = ContextWithEndpoint(ctx, "GET /v1/namespaces")

	logger.WithContext(ctx).Info("context test")

	output := buf.String()

	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}

	if logEntry["request_id"] != "ctx-req-456" {
		t.Errorf("expected request_id='ctx-req-456', got: %v", logEntry["request_id"])
	}
	if logEntry["namespace"] != "ctx-namespace" {
		t.Errorf("expected namespace='ctx-namespace', got: %v", logEntry["namespace"])
	}
	if logEntry["endpoint"] != "GET /v1/namespaces" {
		t.Errorf("expected endpoint='GET /v1/namespaces', got: %v", logEntry["endpoint"])
	}
}

func TestContextHelpers(t *testing.T) {
	ctx := context.Background()

	// Test empty context returns empty strings
	if got := RequestIDFromContext(ctx); got != "" {
		t.Errorf("expected empty request_id, got: %s", got)
	}
	if got := NamespaceFromContext(ctx); got != "" {
		t.Errorf("expected empty namespace, got: %s", got)
	}
	if got := EndpointFromContext(ctx); got != "" {
		t.Errorf("expected empty endpoint, got: %s", got)
	}
	if got := RequestTimeFromContext(ctx); !got.IsZero() {
		t.Errorf("expected zero time, got: %v", got)
	}

	// Add values
	ctx = ContextWithRequestID(ctx, "req-123")
	ctx = ContextWithNamespace(ctx, "test-ns")
	ctx = ContextWithEndpoint(ctx, "POST /query")
	now := time.Now()
	ctx = ContextWithRequestTime(ctx, now)

	if got := RequestIDFromContext(ctx); got != "req-123" {
		t.Errorf("expected request_id='req-123', got: %s", got)
	}
	if got := NamespaceFromContext(ctx); got != "test-ns" {
		t.Errorf("expected namespace='test-ns', got: %s", got)
	}
	if got := EndpointFromContext(ctx); got != "POST /query" {
		t.Errorf("expected endpoint='POST /query', got: %s", got)
	}
	if got := RequestTimeFromContext(ctx); !got.Equal(now) {
		t.Errorf("expected time=%v, got: %v", now, got)
	}
}

func TestCacheTemperature(t *testing.T) {
	tests := []struct {
		temp     CacheTemperature
		expected string
	}{
		{CacheHot, "hot"},
		{CacheWarm, "warm"},
		{CacheCold, "cold"},
	}

	for _, tc := range tests {
		var buf bytes.Buffer
		logger := NewWithWriter(&buf)

		info := &RequestInfo{CacheTemp: tc.temp}
		logger.WithRequestInfo(info).Info("cache test")

		var logEntry map[string]interface{}
		if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
			t.Fatalf("log output is not valid JSON: %v", err)
		}

		if logEntry["cache_temperature"] != tc.expected {
			t.Errorf("expected cache_temperature='%s', got: %v", tc.expected, logEntry["cache_temperature"])
		}
	}
}

func TestElapsedMs(t *testing.T) {
	ctx := context.Background()

	// Empty context returns 0
	if got := ElapsedMs(ctx); got != 0 {
		t.Errorf("expected 0 for empty context, got: %f", got)
	}

	// With request time
	ctx = ContextWithRequestTime(ctx, time.Now().Add(-100*time.Millisecond))
	elapsed := ElapsedMs(ctx)

	// Should be approximately 100ms (allow some tolerance)
	if elapsed < 90 || elapsed > 200 {
		t.Errorf("expected elapsed ~100ms, got: %f", elapsed)
	}
}

func TestLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	logger.With("custom_field", "custom_value").Info("with test")

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}

	if logEntry["custom_field"] != "custom_value" {
		t.Errorf("expected custom_field='custom_value', got: %v", logEntry["custom_field"])
	}
}

func TestStructuredJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewWithWriter(&buf)

	info := &RequestInfo{
		RequestID:     "structured-test",
		Namespace:     "test-namespace",
		Endpoint:      "POST /v2/namespaces/test-namespace",
		CacheTemp:     CacheWarm,
		ServerTotalMs: 123.456,
		QueryExecMs:   100.0,
	}

	logger.WithRequestInfo(info).Info("request completed", "status", 200)

	output := buf.String()

	// Verify the output is valid JSON
	var logEntry map[string]interface{}
	if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
		t.Fatalf("expected valid JSON output, got error: %v\nOutput: %s", err, output)
	}

	// Check all expected fields are present
	expectedFields := []string{
		"time", "level", "msg",
		"request_id", "namespace", "endpoint",
		"cache_temperature", "server_total_ms", "query_execution_ms",
		"status",
	}

	for _, field := range expectedFields {
		if _, ok := logEntry[field]; !ok {
			t.Errorf("expected field '%s' in log output, got: %v", field, logEntry)
		}
	}
}
