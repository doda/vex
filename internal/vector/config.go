package vector

import (
	"fmt"
)

// Config represents the vector configuration for a namespace.
// This is set on the first write that includes vectors and cannot be changed afterwards.
type Config struct {
	// Dims is the number of dimensions in each vector.
	Dims int `json:"dims"`

	// DType is the data type of vector elements (f16 or f32).
	DType DType `json:"dtype"`

	// DistanceMetric is the distance metric used for similarity calculations.
	DistanceMetric DistanceMetric `json:"distance_metric"`

	// ANN indicates whether Approximate Nearest Neighbor search is enabled.
	ANN bool `json:"ann"`
}

// NewConfig creates a new vector configuration with the given parameters.
func NewConfig(dims int, dtype DType, metric DistanceMetric) *Config {
	return &Config{
		Dims:           dims,
		DType:          dtype,
		DistanceMetric: metric,
		ANN:            true,
	}
}

// NewConfigFromType creates a config from a vector type string like "[1536]f32".
func NewConfigFromType(vectorType string, metric DistanceMetric) (*Config, error) {
	dims, dtype, err := ParseVectorType(vectorType)
	if err != nil {
		return nil, err
	}
	return &Config{
		Dims:           dims,
		DType:          dtype,
		DistanceMetric: metric,
		ANN:            true,
	}, nil
}

// Validate validates the vector configuration.
// compatMode should be "turbopuffer" for strict compatibility mode.
func (c *Config) Validate(compatMode string) error {
	if c.Dims <= 0 {
		return fmt.Errorf("vector dimensions must be positive, got %d", c.Dims)
	}

	if !c.DType.IsValid() {
		return fmt.Errorf("invalid vector dtype: %q (must be f16 or f32)", c.DType)
	}

	if err := ValidateMetric(c.DistanceMetric, compatMode); err != nil {
		return err
	}

	return nil
}

// Type returns the vector type string (e.g., "[1536]f32").
func (c *Config) Type() string {
	return FormatVectorType(c.Dims, c.DType)
}

// BytesPerVector returns the number of bytes required to store one vector.
func (c *Config) BytesPerVector() int {
	return c.Dims * c.DType.BytesPerElement()
}

// InferConfigFromVector infers a Config from a vector value.
// The vector must be provided as a float array or base64 string.
// If no metric is provided, cosine_distance is used as default.
func InferConfigFromVector(v any, metric DistanceMetric) (*Config, error) {
	if metric == "" {
		metric = DefaultMetric
	}

	vec, err := DecodeVector(v, 0)
	if err != nil {
		return nil, fmt.Errorf("cannot infer config: %w", err)
	}

	if len(vec) == 0 {
		return nil, fmt.Errorf("cannot infer config from empty vector")
	}

	return &Config{
		Dims:           len(vec),
		DType:          DTypeF32, // Default to f32 when inferred
		DistanceMetric: metric,
		ANN:            true,
	}, nil
}

// ValidateVector validates that a vector matches the expected configuration.
func (c *Config) ValidateVector(v any) error {
	vec, err := DecodeVector(v, 0)
	if err != nil {
		return err
	}

	if len(vec) != c.Dims {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", c.Dims, len(vec))
	}

	return nil
}

// Compatible checks if this config is compatible with another config.
// Configs are compatible if they have the same dimensions and distance metric.
// This is used to validate that vectors in a namespace are consistent.
func (c *Config) Compatible(other *Config) bool {
	if other == nil {
		return false
	}
	return c.Dims == other.Dims &&
		c.DistanceMetric == other.DistanceMetric &&
		c.DType == other.DType
}
