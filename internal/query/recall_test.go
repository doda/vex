package query

import (
	"testing"
)

func TestComputeRecall(t *testing.T) {
	tests := []struct {
		name       string
		ann        []annResult
		exhaustive []exhaustiveResult
		expected   float64
	}{
		{
			name:       "empty_exhaustive",
			ann:        []annResult{{DocID: 1}},
			exhaustive: []exhaustiveResult{},
			expected:   1.0,
		},
		{
			name:       "perfect_recall",
			ann:        []annResult{{DocID: 1}, {DocID: 2}, {DocID: 3}},
			exhaustive: []exhaustiveResult{{DocID: 1}, {DocID: 2}, {DocID: 3}},
			expected:   1.0,
		},
		{
			name:       "half_recall",
			ann:        []annResult{{DocID: 1}, {DocID: 4}},
			exhaustive: []exhaustiveResult{{DocID: 1}, {DocID: 2}, {DocID: 3}, {DocID: 5}},
			expected:   0.25,
		},
		{
			name:       "no_recall",
			ann:        []annResult{{DocID: 10}, {DocID: 20}},
			exhaustive: []exhaustiveResult{{DocID: 1}, {DocID: 2}},
			expected:   0.0,
		},
		{
			name:       "partial_recall",
			ann:        []annResult{{DocID: 1}, {DocID: 2}},
			exhaustive: []exhaustiveResult{{DocID: 1}, {DocID: 2}, {DocID: 3}},
			expected:   2.0 / 3.0,
		},
		{
			name:       "empty_ann",
			ann:        []annResult{},
			exhaustive: []exhaustiveResult{{DocID: 1}, {DocID: 2}},
			expected:   0.0,
		},
		{
			name:       "both_empty",
			ann:        []annResult{},
			exhaustive: []exhaustiveResult{},
			expected:   1.0,
		},
		{
			name:       "single_match",
			ann:        []annResult{{DocID: 1}},
			exhaustive: []exhaustiveResult{{DocID: 1}},
			expected:   1.0,
		},
		{
			name:       "single_mismatch",
			ann:        []annResult{{DocID: 2}},
			exhaustive: []exhaustiveResult{{DocID: 1}},
			expected:   0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeRecall(tt.ann, tt.exhaustive)
			if !floatEqual(got, tt.expected) {
				t.Errorf("computeRecall() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestExhaustiveToANN(t *testing.T) {
	exhaustive := []exhaustiveResult{
		{DocID: 1, Distance: 0.1},
		{DocID: 2, Distance: 0.2},
		{DocID: 3, Distance: 0.3},
	}

	ann := exhaustiveToANN(exhaustive)

	if len(ann) != len(exhaustive) {
		t.Errorf("Expected %d results, got %d", len(exhaustive), len(ann))
	}

	for i, a := range ann {
		if a.DocID != exhaustive[i].DocID {
			t.Errorf("DocID mismatch at %d: got %d, want %d", i, a.DocID, exhaustive[i].DocID)
		}
		if a.Distance != exhaustive[i].Distance {
			t.Errorf("Distance mismatch at %d: got %v, want %v", i, a.Distance, exhaustive[i].Distance)
		}
	}
}

func TestRecallRequest_Defaults(t *testing.T) {
	// Test that default values are applied
	req := &RecallRequest{}

	// Verify defaults
	if DefaultRecallNum != 10 {
		t.Errorf("Expected DefaultRecallNum=10, got %d", DefaultRecallNum)
	}
	if DefaultRecallTopK != 10 {
		t.Errorf("Expected DefaultRecallTopK=10, got %d", DefaultRecallTopK)
	}

	// The actual default application happens in HandleRecall
	// Here we just verify the constants are set correctly
	if req.Num != 0 {
		t.Errorf("Expected initial Num=0, got %d", req.Num)
	}
	if req.TopK != 0 {
		t.Errorf("Expected initial TopK=0, got %d", req.TopK)
	}
}

func floatEqual(a, b float64) bool {
	const epsilon = 1e-9
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < epsilon
}
