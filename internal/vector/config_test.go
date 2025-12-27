package vector

import (
	"testing"
)

func TestNewConfig(t *testing.T) {
	cfg := NewConfig(1536, DTypeF32, MetricCosineDistance)

	if cfg.Dims != 1536 {
		t.Errorf("Dims = %d, want 1536", cfg.Dims)
	}
	if cfg.DType != DTypeF32 {
		t.Errorf("DType = %q, want %q", cfg.DType, DTypeF32)
	}
	if cfg.DistanceMetric != MetricCosineDistance {
		t.Errorf("DistanceMetric = %q, want %q", cfg.DistanceMetric, MetricCosineDistance)
	}
	if !cfg.ANN {
		t.Error("ANN should be true by default")
	}
}

func TestNewConfigFromType(t *testing.T) {
	tests := []struct {
		name       string
		vectorType string
		metric     DistanceMetric
		wantDims   int
		wantDType  DType
		wantErr    bool
	}{
		{
			name:       "valid f32 config",
			vectorType: "[1536]f32",
			metric:     MetricCosineDistance,
			wantDims:   1536,
			wantDType:  DTypeF32,
		},
		{
			name:       "valid f16 config",
			vectorType: "[768]f16",
			metric:     MetricEuclideanSquared,
			wantDims:   768,
			wantDType:  DTypeF16,
		},
		{
			name:       "invalid format",
			vectorType: "invalid",
			metric:     MetricCosineDistance,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := NewConfigFromType(tt.vectorType, tt.metric)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConfigFromType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if cfg.Dims != tt.wantDims {
					t.Errorf("Dims = %d, want %d", cfg.Dims, tt.wantDims)
				}
				if cfg.DType != tt.wantDType {
					t.Errorf("DType = %q, want %q", cfg.DType, tt.wantDType)
				}
				if cfg.DistanceMetric != tt.metric {
					t.Errorf("DistanceMetric = %q, want %q", cfg.DistanceMetric, tt.metric)
				}
			}
		})
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name       string
		config     *Config
		compatMode string
		wantErr    bool
	}{
		{
			name:       "valid config in turbopuffer mode",
			config:     NewConfig(1536, DTypeF32, MetricCosineDistance),
			compatMode: "turbopuffer",
			wantErr:    false,
		},
		{
			name:       "valid config with euclidean_squared",
			config:     NewConfig(768, DTypeF16, MetricEuclideanSquared),
			compatMode: "turbopuffer",
			wantErr:    false,
		},
		{
			name:       "dot_product in vex mode",
			config:     NewConfig(1536, DTypeF32, MetricDotProduct),
			compatMode: "vex",
			wantErr:    false,
		},
		{
			name:       "dot_product in turbopuffer mode",
			config:     NewConfig(1536, DTypeF32, MetricDotProduct),
			compatMode: "turbopuffer",
			wantErr:    true,
		},
		{
			name: "zero dimensions",
			config: &Config{
				Dims:           0,
				DType:          DTypeF32,
				DistanceMetric: MetricCosineDistance,
			},
			compatMode: "turbopuffer",
			wantErr:    true,
		},
		{
			name: "negative dimensions",
			config: &Config{
				Dims:           -1,
				DType:          DTypeF32,
				DistanceMetric: MetricCosineDistance,
			},
			compatMode: "turbopuffer",
			wantErr:    true,
		},
		{
			name: "invalid dtype",
			config: &Config{
				Dims:           1536,
				DType:          DType("f64"),
				DistanceMetric: MetricCosineDistance,
			},
			compatMode: "turbopuffer",
			wantErr:    true,
		},
		{
			name: "unknown metric",
			config: &Config{
				Dims:           1536,
				DType:          DTypeF32,
				DistanceMetric: DistanceMetric("unknown"),
			},
			compatMode: "vex",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate(tt.compatMode)
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfig_Type(t *testing.T) {
	cfg := NewConfig(1536, DTypeF32, MetricCosineDistance)
	if got := cfg.Type(); got != "[1536]f32" {
		t.Errorf("Config.Type() = %q, want %q", got, "[1536]f32")
	}

	cfg = NewConfig(768, DTypeF16, MetricEuclideanSquared)
	if got := cfg.Type(); got != "[768]f16" {
		t.Errorf("Config.Type() = %q, want %q", got, "[768]f16")
	}
}

func TestConfig_BytesPerVector(t *testing.T) {
	tests := []struct {
		dims   int
		dtype  DType
		expect int
	}{
		{1536, DTypeF32, 1536 * 4},
		{768, DTypeF16, 768 * 2},
		{3, DTypeF32, 12},
	}

	for _, tt := range tests {
		cfg := NewConfig(tt.dims, tt.dtype, MetricCosineDistance)
		if got := cfg.BytesPerVector(); got != tt.expect {
			t.Errorf("Config{%d, %q}.BytesPerVector() = %d, want %d",
				tt.dims, tt.dtype, got, tt.expect)
		}
	}
}

func TestInferConfigFromVector(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		metric   DistanceMetric
		wantDims int
		wantErr  bool
	}{
		{
			name:     "infer from float64 array",
			input:    []float64{1.0, 2.0, 3.0},
			metric:   MetricCosineDistance,
			wantDims: 3,
		},
		{
			name:     "infer from any array",
			input:    []any{1.0, 2.0, 3.0, 4.0, 5.0},
			metric:   MetricEuclideanSquared,
			wantDims: 5,
		},
		{
			name:     "infer with default metric",
			input:    []float64{1.0, 2.0},
			metric:   "",
			wantDims: 2,
		},
		{
			name:    "empty vector",
			input:   []float64{},
			metric:  MetricCosineDistance,
			wantErr: true,
		},
		{
			name:    "invalid input type",
			input:   "not a vector",
			metric:  MetricCosineDistance,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := InferConfigFromVector(tt.input, tt.metric)
			if (err != nil) != tt.wantErr {
				t.Errorf("InferConfigFromVector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if cfg.Dims != tt.wantDims {
					t.Errorf("Dims = %d, want %d", cfg.Dims, tt.wantDims)
				}
				if cfg.DType != DTypeF32 {
					t.Errorf("DType = %q, want %q", cfg.DType, DTypeF32)
				}
				expectedMetric := tt.metric
				if expectedMetric == "" {
					expectedMetric = DefaultMetric
				}
				if cfg.DistanceMetric != expectedMetric {
					t.Errorf("DistanceMetric = %q, want %q", cfg.DistanceMetric, expectedMetric)
				}
			}
		})
	}
}

func TestConfig_ValidateVector(t *testing.T) {
	cfg := NewConfig(3, DTypeF32, MetricCosineDistance)

	tests := []struct {
		name    string
		input   any
		wantErr bool
	}{
		{"valid vector", []float64{1.0, 2.0, 3.0}, false},
		{"valid any array", []any{1.0, 2.0, 3.0}, false},
		{"dimension mismatch", []float64{1.0, 2.0}, true},
		{"too many dimensions", []float64{1.0, 2.0, 3.0, 4.0}, true},
		{"invalid type", "not a vector", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cfg.ValidateVector(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.ValidateVector() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfig_Compatible(t *testing.T) {
	cfg1 := NewConfig(1536, DTypeF32, MetricCosineDistance)
	cfg2 := NewConfig(1536, DTypeF32, MetricCosineDistance)
	cfg3 := NewConfig(768, DTypeF32, MetricCosineDistance)
	cfg4 := NewConfig(1536, DTypeF16, MetricCosineDistance)
	cfg5 := NewConfig(1536, DTypeF32, MetricEuclideanSquared)

	if !cfg1.Compatible(cfg2) {
		t.Error("identical configs should be compatible")
	}
	if cfg1.Compatible(cfg3) {
		t.Error("different dimensions should not be compatible")
	}
	if cfg1.Compatible(cfg4) {
		t.Error("different dtypes should not be compatible")
	}
	if cfg1.Compatible(cfg5) {
		t.Error("different metrics should not be compatible")
	}
	if cfg1.Compatible(nil) {
		t.Error("nil config should not be compatible")
	}
}

// Test distance_metric is set on first write with vectors (simulating behavior)
func TestDistanceMetricSetOnFirstWrite(t *testing.T) {
	// Simulate: first write infers config, subsequent writes must match
	firstVector := []float64{1.0, 2.0, 3.0}

	// First write - metric is set
	cfg, err := InferConfigFromVector(firstVector, MetricCosineDistance)
	if err != nil {
		t.Fatalf("failed to infer config: %v", err)
	}

	if cfg.DistanceMetric != MetricCosineDistance {
		t.Errorf("metric should be cosine_distance on first write, got %q", cfg.DistanceMetric)
	}

	// Simulate second write - must use same metric
	secondCfg, _ := InferConfigFromVector(firstVector, MetricCosineDistance)
	if !cfg.Compatible(secondCfg) {
		t.Error("second write config should be compatible with first")
	}

	// Simulate write with different metric - should fail compatibility
	differentMetricCfg, _ := InferConfigFromVector(firstVector, MetricEuclideanSquared)
	if cfg.Compatible(differentMetricCfg) {
		t.Error("write with different metric should not be compatible")
	}
}
