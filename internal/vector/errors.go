package vector

import "errors"

var (
	// ErrInvalidDType is returned when an invalid data type is specified.
	ErrInvalidDType = errors.New("invalid vector dtype")

	// ErrInvalidMetric is returned when an invalid distance metric is specified.
	ErrInvalidMetric = errors.New("invalid distance metric")

	// ErrInvalidDimensions is returned when vector dimensions are invalid.
	ErrInvalidDimensions = errors.New("invalid vector dimensions")

	// ErrDimensionMismatch is returned when vector dimensions don't match the config.
	ErrDimensionMismatch = errors.New("vector dimension mismatch")

	// ErrInvalidEncoding is returned when vector encoding is invalid.
	ErrInvalidEncoding = errors.New("invalid vector encoding")

	// ErrMetricNotAllowed is returned when a metric is not allowed in the current mode.
	ErrMetricNotAllowed = errors.New("distance metric not allowed in compatibility mode")

	// ErrConfigImmutable is returned when trying to change an immutable config.
	ErrConfigImmutable = errors.New("vector configuration is immutable after first write")
)
