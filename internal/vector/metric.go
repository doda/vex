package vector

import (
	"fmt"
)

// DistanceMetric represents the distance metric used for vector similarity.
type DistanceMetric string

const (
	// MetricCosineDistance is the default distance metric using cosine similarity.
	// Distance = 1 - cosine_similarity(a, b)
	MetricCosineDistance DistanceMetric = "cosine_distance"

	// MetricEuclideanSquared uses squared Euclidean distance.
	// Distance = sum((a[i] - b[i])^2)
	MetricEuclideanSquared DistanceMetric = "euclidean_squared"

	// MetricDotProduct uses negative dot product as distance.
	// Distance = -dot(a, b)
	// NOTE: This is a Vex-only extension, not supported in turbopuffer compat mode.
	MetricDotProduct DistanceMetric = "dot_product"
)

// DefaultMetric is the default distance metric when not specified.
const DefaultMetric = MetricCosineDistance

// IsValid returns true if the metric is a recognized distance metric.
// When compatMode is "turbopuffer", dot_product is not valid.
func (m DistanceMetric) IsValid(compatMode string) bool {
	switch m {
	case MetricCosineDistance, MetricEuclideanSquared:
		return true
	case MetricDotProduct:
		// dot_product is only valid when not in turbopuffer compat mode
		return compatMode != "turbopuffer"
	default:
		return false
	}
}

// IsValidStrict returns true if the metric is a recognized distance metric,
// ignoring compat mode restrictions.
func (m DistanceMetric) IsValidStrict() bool {
	switch m {
	case MetricCosineDistance, MetricEuclideanSquared, MetricDotProduct:
		return true
	default:
		return false
	}
}

// String returns the string representation of the metric.
func (m DistanceMetric) String() string {
	return string(m)
}

// ValidateMetric validates a distance metric against the compatibility mode.
// Returns an error if the metric is invalid or not allowed.
func ValidateMetric(metric DistanceMetric, compatMode string) error {
	if !metric.IsValidStrict() {
		return fmt.Errorf("unknown distance metric: %q", metric)
	}

	if metric == MetricDotProduct && compatMode == "turbopuffer" {
		return fmt.Errorf("distance metric %q is not supported in turbopuffer compatibility mode", metric)
	}

	return nil
}

// ParseMetric parses a string into a DistanceMetric.
// Returns an error if the string is not a valid metric.
func ParseMetric(s string) (DistanceMetric, error) {
	m := DistanceMetric(s)
	if !m.IsValidStrict() {
		return "", fmt.Errorf("unknown distance metric: %q", s)
	}
	return m, nil
}
