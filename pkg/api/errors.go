package api

import "net/http"

// APIError represents an error with an associated HTTP status code.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return e.Message
}

// NewAPIError creates a new APIError with the given status code and message.
func NewAPIError(statusCode int, message string) *APIError {
	return &APIError{
		StatusCode: statusCode,
		Message:    message,
	}
}

// Common error constructors

// ErrBadRequest returns a 400 Bad Request error.
func ErrBadRequest(message string) *APIError {
	return NewAPIError(http.StatusBadRequest, message)
}

// ErrUnauthorized returns a 401 Unauthorized error.
func ErrUnauthorized(message string) *APIError {
	return NewAPIError(http.StatusUnauthorized, message)
}

// ErrForbidden returns a 403 Forbidden error.
func ErrForbidden(message string) *APIError {
	return NewAPIError(http.StatusForbidden, message)
}

// ErrNotFound returns a 404 Not Found error.
func ErrNotFound(message string) *APIError {
	return NewAPIError(http.StatusNotFound, message)
}

// ErrPayloadTooLarge returns a 413 Payload Too Large error.
func ErrPayloadTooLarge(message string) *APIError {
	return NewAPIError(http.StatusRequestEntityTooLarge, message)
}

// ErrTooManyRequests returns a 429 Too Many Requests error.
func ErrTooManyRequests(message string) *APIError {
	return NewAPIError(http.StatusTooManyRequests, message)
}

// ErrInternalServer returns a 500 Internal Server Error.
func ErrInternalServer(message string) *APIError {
	return NewAPIError(http.StatusInternalServerError, message)
}

// ErrServiceUnavailable returns a 503 Service Unavailable error.
func ErrServiceUnavailable(message string) *APIError {
	return NewAPIError(http.StatusServiceUnavailable, message)
}

// Specific error messages for common cases

// ErrInvalidJSON returns a 400 error for invalid JSON.
func ErrInvalidJSON() *APIError {
	return ErrBadRequest("invalid JSON in request body")
}

// ErrInvalidSchema returns a 400 error for schema violations.
func ErrInvalidSchema(detail string) *APIError {
	return ErrBadRequest("schema error: " + detail)
}

// ErrTypeMismatch returns a 400 error for type mismatches.
func ErrTypeMismatch(field, expected, got string) *APIError {
	return ErrBadRequest("type mismatch for field '" + field + "': expected " + expected + ", got " + got)
}

// ErrDuplicateIDs returns a 400 error for duplicate IDs in columnar format.
func ErrDuplicateIDs() *APIError {
	return ErrBadRequest("duplicate IDs in columnar format are not allowed")
}

// ErrNamespaceNotFound returns a 404 error for missing namespace.
func ErrNamespaceNotFound(namespace string) *APIError {
	return ErrNotFound("namespace '" + namespace + "' not found")
}

// ErrNamespaceDeleted returns a 404 error for deleted namespace.
func ErrNamespaceDeleted(namespace string) *APIError {
	return ErrNotFound("namespace '" + namespace + "' has been deleted")
}

// ErrBackpressure returns a 429 error when unindexed data exceeds threshold.
func ErrBackpressure() *APIError {
	return ErrTooManyRequests("write rejected: unindexed data exceeds 2GB threshold; try again later or use disable_backpressure=true")
}

// ErrRateLimited returns a 429 error for rate limiting.
func ErrRateLimited() *APIError {
	return ErrTooManyRequests("rate limit exceeded; try again later")
}

// ErrObjectStoreUnavailable returns a 503 error when object store is unavailable.
func ErrObjectStoreUnavailable() *APIError {
	return ErrServiceUnavailable("object storage is temporarily unavailable")
}

// ErrStrongQueryBackpressure returns a 503 error for strong query when disable_backpressure is set
// and unindexed data exceeds threshold.
func ErrStrongQueryBackpressure() *APIError {
	return ErrServiceUnavailable("strong query unavailable: unindexed data exceeds 2GB with disable_backpressure=true; use eventual consistency or wait for indexing")
}

// ErrIndexBuilding returns an *APIError for 202 status when query depends on index still building.
// This is technically not an error but a pending state.
func ErrIndexBuilding(message string) *APIError {
	return NewAPIError(http.StatusAccepted, message)
}

// MaxRequestBodySize is the maximum allowed request body size (256MB).
const MaxRequestBodySize = 256 * 1024 * 1024

// MaxUnindexedBytes is the threshold for backpressure (2GB).
const MaxUnindexedBytes = 2 * 1024 * 1024 * 1024
