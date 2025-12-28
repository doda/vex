package guardrails

import "errors"

var (
	// ErrNamespaceNotLoaded is returned when an operation requires a loaded namespace.
	ErrNamespaceNotLoaded = errors.New("namespace not loaded")

	// ErrTailBytesExceeded is returned when tail bytes would exceed the per-namespace cap.
	ErrTailBytesExceeded = errors.New("tail bytes exceeded per-namespace cap")

	// ErrColdFillLimitReached is returned when no cold fill slots are available.
	ErrColdFillLimitReached = errors.New("cold fill limit reached")
)
