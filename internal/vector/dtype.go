// Package vector provides vector configuration and operations for Vex.
package vector

import (
	"fmt"
	"regexp"
	"strconv"
)

// DType represents the data type of vector elements.
type DType string

const (
	// DTypeF16 represents 16-bit floating point vectors.
	DTypeF16 DType = "f16"
	// DTypeF32 represents 32-bit floating point vectors.
	DTypeF32 DType = "f32"
)

// IsValid returns true if the dtype is a recognized vector element type.
func (d DType) IsValid() bool {
	switch d {
	case DTypeF16, DTypeF32:
		return true
	default:
		return false
	}
}

// BytesPerElement returns the number of bytes per vector element.
func (d DType) BytesPerElement() int {
	switch d {
	case DTypeF16:
		return 2
	case DTypeF32:
		return 4
	default:
		return 0
	}
}

// String returns the string representation of the dtype.
func (d DType) String() string {
	return string(d)
}

// vectorTypeRegex matches vector type strings like "[1536]f32" or "[768]f16".
var vectorTypeRegex = regexp.MustCompile(`^\[(\d+)\](f16|f32)$`)

// ParseVectorType parses a vector type string like "[1536]f32" into dimensions and dtype.
// Returns dimensions, dtype, and an error if the format is invalid.
func ParseVectorType(s string) (int, DType, error) {
	matches := vectorTypeRegex.FindStringSubmatch(s)
	if matches == nil {
		return 0, "", fmt.Errorf("invalid vector type format: %q (expected format like [1536]f32)", s)
	}

	dims, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, "", fmt.Errorf("invalid dimensions: %w", err)
	}
	if dims <= 0 {
		return 0, "", fmt.Errorf("dimensions must be positive, got %d", dims)
	}

	dtype := DType(matches[2])
	if !dtype.IsValid() {
		return 0, "", fmt.Errorf("invalid dtype: %s", dtype)
	}

	return dims, dtype, nil
}

// FormatVectorType formats dimensions and dtype into a vector type string.
func FormatVectorType(dims int, dtype DType) string {
	return fmt.Sprintf("[%d]%s", dims, dtype)
}
