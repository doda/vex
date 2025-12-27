package vector

import (
	"testing"
)

func TestDistanceMetric_IsValid(t *testing.T) {
	tests := []struct {
		name       string
		metric     DistanceMetric
		compatMode string
		expect     bool
	}{
		// Cosine distance is always valid
		{"cosine_distance in turbopuffer mode", MetricCosineDistance, "turbopuffer", true},
		{"cosine_distance in vex mode", MetricCosineDistance, "vex", true},
		{"cosine_distance with empty compat", MetricCosineDistance, "", true},

		// Euclidean squared is always valid
		{"euclidean_squared in turbopuffer mode", MetricEuclideanSquared, "turbopuffer", true},
		{"euclidean_squared in vex mode", MetricEuclideanSquared, "vex", true},
		{"euclidean_squared with empty compat", MetricEuclideanSquared, "", true},

		// Dot product is only valid when not in turbopuffer mode
		{"dot_product in turbopuffer mode", MetricDotProduct, "turbopuffer", false},
		{"dot_product in vex mode", MetricDotProduct, "vex", true},
		{"dot_product with empty compat", MetricDotProduct, "", true},

		// Unknown metrics are never valid
		{"unknown metric", DistanceMetric("unknown"), "vex", false},
		{"empty metric", DistanceMetric(""), "vex", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.metric.IsValid(tt.compatMode); got != tt.expect {
				t.Errorf("DistanceMetric(%q).IsValid(%q) = %v, want %v",
					tt.metric, tt.compatMode, got, tt.expect)
			}
		})
	}
}

func TestDistanceMetric_IsValidStrict(t *testing.T) {
	tests := []struct {
		metric DistanceMetric
		expect bool
	}{
		{MetricCosineDistance, true},
		{MetricEuclideanSquared, true},
		{MetricDotProduct, true},
		{DistanceMetric("unknown"), false},
		{DistanceMetric(""), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.metric), func(t *testing.T) {
			if got := tt.metric.IsValidStrict(); got != tt.expect {
				t.Errorf("DistanceMetric(%q).IsValidStrict() = %v, want %v",
					tt.metric, got, tt.expect)
			}
		})
	}
}

func TestValidateMetric(t *testing.T) {
	tests := []struct {
		name       string
		metric     DistanceMetric
		compatMode string
		wantErr    bool
	}{
		// Valid metrics
		{"cosine in turbopuffer", MetricCosineDistance, "turbopuffer", false},
		{"euclidean in turbopuffer", MetricEuclideanSquared, "turbopuffer", false},
		{"cosine in vex", MetricCosineDistance, "vex", false},
		{"euclidean in vex", MetricEuclideanSquared, "vex", false},
		{"dot_product in vex", MetricDotProduct, "vex", false},

		// Invalid: dot_product in turbopuffer mode
		{"dot_product in turbopuffer", MetricDotProduct, "turbopuffer", true},

		// Invalid: unknown metrics
		{"unknown metric", DistanceMetric("unknown"), "vex", true},
		{"empty metric", DistanceMetric(""), "vex", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMetric(tt.metric, tt.compatMode)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateMetric(%q, %q) error = %v, wantErr %v",
					tt.metric, tt.compatMode, err, tt.wantErr)
			}
		})
	}
}

func TestParseMetric(t *testing.T) {
	tests := []struct {
		input   string
		expect  DistanceMetric
		wantErr bool
	}{
		{"cosine_distance", MetricCosineDistance, false},
		{"euclidean_squared", MetricEuclideanSquared, false},
		{"dot_product", MetricDotProduct, false},
		{"unknown", DistanceMetric(""), true},
		{"", DistanceMetric(""), true},
		{"COSINE_DISTANCE", DistanceMetric(""), true}, // Case sensitive
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseMetric(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMetric(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if err == nil && got != tt.expect {
				t.Errorf("ParseMetric(%q) = %q, want %q", tt.input, got, tt.expect)
			}
		})
	}
}

func TestDefaultMetric(t *testing.T) {
	if DefaultMetric != MetricCosineDistance {
		t.Errorf("DefaultMetric = %q, want %q", DefaultMetric, MetricCosineDistance)
	}
}
