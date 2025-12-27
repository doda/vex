package logging

import (
	"net/http"
	"time"

	"github.com/google/uuid"
)

// responseWriter wraps http.ResponseWriter to capture status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Middleware creates an HTTP middleware that logs requests.
func Middleware(logger *Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Generate request ID
			requestID := r.Header.Get("X-Request-ID")
			if requestID == "" {
				requestID = uuid.New().String()
			}

			// Extract endpoint and namespace
			endpoint := r.Method + " " + r.URL.Path
			namespace := r.PathValue("namespace")

			// Build context with request info
			ctx := r.Context()
			ctx = ContextWithRequestID(ctx, requestID)
			ctx = ContextWithRequestTime(ctx, start)
			ctx = ContextWithEndpoint(ctx, endpoint)
			if namespace != "" {
				ctx = ContextWithNamespace(ctx, namespace)
			}

			// Wrap response writer to capture status
			rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			// Set request ID in response header
			w.Header().Set("X-Request-ID", requestID)

			// Serve the request
			next.ServeHTTP(rw, r.WithContext(ctx))

			// Log completion
			elapsed := float64(time.Since(start).Microseconds()) / 1000.0

			info := &RequestInfo{
				RequestID:     requestID,
				Namespace:     namespace,
				Endpoint:      endpoint,
				ServerTotalMs: elapsed,
			}

			logger.WithRequestInfo(info).Info("request completed",
				"status", rw.statusCode,
				"method", r.Method,
				"path", r.URL.Path,
			)
		})
	}
}

// MiddlewareFunc creates an HTTP middleware function for use with http.HandlerFunc.
func MiddlewareFunc(logger *Logger, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Generate request ID
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}

		// Extract endpoint and namespace
		endpoint := r.Method + " " + r.URL.Path
		namespace := r.PathValue("namespace")

		// Build context with request info
		ctx := r.Context()
		ctx = ContextWithRequestID(ctx, requestID)
		ctx = ContextWithRequestTime(ctx, start)
		ctx = ContextWithEndpoint(ctx, endpoint)
		if namespace != "" {
			ctx = ContextWithNamespace(ctx, namespace)
		}

		// Wrap response writer to capture status
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Set request ID in response header
		w.Header().Set("X-Request-ID", requestID)

		// Serve the request
		next(rw, r.WithContext(ctx))

		// Log completion
		elapsed := float64(time.Since(start).Microseconds()) / 1000.0

		info := &RequestInfo{
			RequestID:     requestID,
			Namespace:     namespace,
			Endpoint:      endpoint,
			ServerTotalMs: elapsed,
		}

		logger.WithRequestInfo(info).Info("request completed",
			"status", rw.statusCode,
			"method", r.Method,
			"path", r.URL.Path,
		)
	}
}
