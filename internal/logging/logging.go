// Package logging provides structured JSON logging for Vex.
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"time"
)

// Logger wraps slog.Logger with additional context fields.
type Logger struct {
	*slog.Logger
}

type contextKey string

const (
	requestIDKey      contextKey = "request_id"
	namespaceKey      contextKey = "namespace"
	endpointKey       contextKey = "endpoint"
	requestTimeKey    contextKey = "request_time"
	requestMetricsKey contextKey = "request_metrics"
)

// CacheTemperature represents cache warmth status.
type CacheTemperature string

const (
	CacheHot  CacheTemperature = "hot"
	CacheWarm CacheTemperature = "warm"
	CacheCold CacheTemperature = "cold"
)

// RequestInfo contains contextual information about the request.
type RequestInfo struct {
	RequestID     string
	Namespace     string
	Endpoint      string
	CacheTemp     CacheTemperature
	ServerTotalMs float64
	QueryExecMs   float64
	RequestTime   time.Time
}

// RequestMetrics holds mutable per-request measurements for logging.
type RequestMetrics struct {
	CacheTemp   CacheTemperature
	QueryExecMs float64
}

// New creates a new Logger with JSON output.
func New() *Logger {
	return NewWithWriter(os.Stdout)
}

// NewWithWriter creates a new Logger with JSON output to the provided writer.
func NewWithWriter(w io.Writer) *Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return &Logger{Logger: slog.New(handler)}
}

// WithContext returns a logger with context values attached.
func (l *Logger) WithContext(ctx context.Context) *Logger {
	logger := l.Logger

	if requestID, ok := ctx.Value(requestIDKey).(string); ok && requestID != "" {
		logger = logger.With(slog.String("request_id", requestID))
	}
	if namespace, ok := ctx.Value(namespaceKey).(string); ok && namespace != "" {
		logger = logger.With(slog.String("namespace", namespace))
	}
	if endpoint, ok := ctx.Value(endpointKey).(string); ok && endpoint != "" {
		logger = logger.With(slog.String("endpoint", endpoint))
	}

	return &Logger{Logger: logger}
}

// WithRequestInfo returns a logger with request information attached.
func (l *Logger) WithRequestInfo(info *RequestInfo) *Logger {
	logger := l.Logger

	if info.RequestID != "" {
		logger = logger.With(slog.String("request_id", info.RequestID))
	}
	if info.Namespace != "" {
		logger = logger.With(slog.String("namespace", info.Namespace))
	}
	if info.Endpoint != "" {
		logger = logger.With(slog.String("endpoint", info.Endpoint))
	}
	if info.CacheTemp != "" {
		logger = logger.With(slog.String("cache_temperature", string(info.CacheTemp)))
	}
	if info.ServerTotalMs > 0 {
		logger = logger.With(slog.Float64("server_total_ms", info.ServerTotalMs))
	}
	if info.QueryExecMs > 0 {
		logger = logger.With(slog.Float64("query_execution_ms", info.QueryExecMs))
	}

	return &Logger{Logger: logger}
}

// With returns a new logger with additional attributes.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{Logger: l.Logger.With(args...)}
}

// ContextWithRequestID adds a request ID to the context.
func ContextWithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

// ContextWithNamespace adds a namespace to the context.
func ContextWithNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, namespaceKey, namespace)
}

// ContextWithEndpoint adds an endpoint to the context.
func ContextWithEndpoint(ctx context.Context, endpoint string) context.Context {
	return context.WithValue(ctx, endpointKey, endpoint)
}

// ContextWithRequestTime adds a request start time to the context.
func ContextWithRequestTime(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, requestTimeKey, t)
}

// ContextWithRequestMetrics adds mutable request metrics to the context.
func ContextWithRequestMetrics(ctx context.Context, metrics *RequestMetrics) context.Context {
	return context.WithValue(ctx, requestMetricsKey, metrics)
}

// RequestIDFromContext extracts the request ID from the context.
func RequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

// NamespaceFromContext extracts the namespace from the context.
func NamespaceFromContext(ctx context.Context) string {
	if ns, ok := ctx.Value(namespaceKey).(string); ok {
		return ns
	}
	return ""
}

// EndpointFromContext extracts the endpoint from the context.
func EndpointFromContext(ctx context.Context) string {
	if ep, ok := ctx.Value(endpointKey).(string); ok {
		return ep
	}
	return ""
}

// RequestTimeFromContext extracts the request start time from the context.
func RequestTimeFromContext(ctx context.Context) time.Time {
	if t, ok := ctx.Value(requestTimeKey).(time.Time); ok {
		return t
	}
	return time.Time{}
}

// RequestMetricsFromContext extracts request metrics from the context.
func RequestMetricsFromContext(ctx context.Context) *RequestMetrics {
	if metrics, ok := ctx.Value(requestMetricsKey).(*RequestMetrics); ok {
		return metrics
	}
	return nil
}

// ElapsedMs returns the milliseconds elapsed since the request time.
func ElapsedMs(ctx context.Context) float64 {
	start := RequestTimeFromContext(ctx)
	if start.IsZero() {
		return 0
	}
	return float64(time.Since(start).Microseconds()) / 1000.0
}
