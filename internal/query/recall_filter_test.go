package query

import (
	"context"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
)

// recallTestTailStore implements tail.Store for recall-aware filtering tests.
type recallTestTailStore struct {
	documents []*tail.Document
	tailBytes int64 // Approximate byte size for doc count estimation
}

func (m *recallTestTailStore) Refresh(ctx context.Context, namespace string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *recallTestTailStore) Scan(ctx context.Context, namespace string, f *filter.Filter) ([]*tail.Document, error) {
	return m.documents, nil
}

func (m *recallTestTailStore) ScanWithByteLimit(ctx context.Context, namespace string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.documents, nil
}

func (m *recallTestTailStore) VectorScan(ctx context.Context, namespace string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *recallTestTailStore) VectorScanWithByteLimit(ctx context.Context, namespace string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *recallTestTailStore) GetDocument(ctx context.Context, namespace string, id document.ID) (*tail.Document, error) {
	for _, doc := range m.documents {
		if doc.ID.String() == id.String() {
			return doc, nil
		}
	}
	return nil, nil
}

func (m *recallTestTailStore) TailBytes(namespace string) int64 {
	return m.tailBytes
}

func (m *recallTestTailStore) Clear(namespace string) {}

func (m *recallTestTailStore) AddWALEntry(namespace string, entry *wal.WalEntry) {
	// Not used in tests
}

func (m *recallTestTailStore) Close() error {
	return nil
}

func TestEstimateFilterSelectivity(t *testing.T) {
	tests := []struct {
		name           string
		documents      []*tail.Document
		filter         any
		expectedMin    float64
		expectedMax    float64
		sampleSize     int
	}{
		{
			name:        "nil filter - all docs pass",
			documents:   makeDocs(100, "category", []any{"A", "B", "C"}),
			filter:      nil,
			expectedMin: 1.0,
			expectedMax: 1.0,
			sampleSize:  100,
		},
		{
			name:        "high selectivity filter - ~33% pass",
			documents:   makeDocs(300, "category", []any{"A", "B", "C"}),
			filter:      []any{"category", "Eq", "A"},
			expectedMin: 0.28,
			expectedMax: 0.38,
			sampleSize:  300,
		},
		{
			name:        "low selectivity filter - ~10% pass",
			documents:   makeDocs(100, "category", []any{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}),
			filter:      []any{"category", "Eq", "A"},
			expectedMin: 0.05,
			expectedMax: 0.15,
			sampleSize:  100,
		},
		{
			name:        "very selective filter - 1% pass",
			documents:   makeDocs(100, "category", makeCategories(100)),
			filter:      []any{"category", "Eq", "cat_0"},
			expectedMin: 0.005,
			expectedMax: 0.025,
			sampleSize:  100,
		},
		{
			name:        "empty documents",
			documents:   []*tail.Document{},
			filter:      []any{"category", "Eq", "A"},
			expectedMin: 1.0,
			expectedMax: 1.0,
			sampleSize:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set tailBytes based on document count (estimate 1KB per doc)
			tailBytes := int64(len(tt.documents)) * 1024
			store := &recallTestTailStore{documents: tt.documents, tailBytes: tailBytes}
			var f *filter.Filter
			if tt.filter != nil {
				var err error
				f, err = filter.Parse(tt.filter)
				if err != nil {
					t.Fatalf("failed to parse filter: %v", err)
				}
			}

			sel, err := EstimateFilterSelectivity(context.Background(), store, "test-ns", f, tt.sampleSize)
			if err != nil {
				t.Fatalf("EstimateFilterSelectivity failed: %v", err)
			}

			if sel.Selectivity < tt.expectedMin || sel.Selectivity > tt.expectedMax {
				t.Errorf("selectivity = %v, want [%v, %v]", sel.Selectivity, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

func TestPlanRecallAwareSearch(t *testing.T) {
	cfg := DefaultFilterSelectivityConfig()

	tests := []struct {
		name               string
		selectivity        float64
		topK               int
		totalDocs          int
		expectExactSearch  bool
		expectNProbeMin    int
		expectNProbeMax    int
		expectCandidatesMin int
		expectCandidatesMax int
	}{
		{
			name:               "high selectivity (80%) - minimal oversampling",
			selectivity:        0.8,
			topK:               10,
			totalDocs:          1000,
			expectExactSearch:  false,
			expectNProbeMin:    cfg.MinNProbe,
			expectNProbeMax:    cfg.MinNProbe + 10,
			expectCandidatesMin: 10,
			expectCandidatesMax: 25,
		},
		{
			name:               "medium selectivity (20%) - moderate oversampling",
			selectivity:        0.2,
			topK:               10,
			totalDocs:          1000,
			expectExactSearch:  false,
			expectNProbeMin:    cfg.MinNProbe,
			expectNProbeMax:    cfg.MaxNProbe,
			expectCandidatesMin: 40,
			expectCandidatesMax: 60,
		},
		{
			name:               "low selectivity (5%) - high oversampling",
			selectivity:        0.05,
			topK:               10,
			totalDocs:          1000,
			expectExactSearch:  false,
			expectNProbeMin:    cfg.MaxNProbe - 20,
			expectNProbeMax:    cfg.MaxNProbe,
			expectCandidatesMin: 80,
			expectCandidatesMax: 200,
		},
		{
			name:               "very low selectivity (0.5%) - exact search fallback",
			selectivity:        0.005,
			topK:               10,
			totalDocs:          1000,
			expectExactSearch:  true,
			expectNProbeMin:    0,
			expectNProbeMax:    0,
			expectCandidatesMin: 0,
			expectCandidatesMax: 0,
		},
		{
			name:               "no filter (100%) - standard ANN",
			selectivity:        1.0,
			topK:               10,
			totalDocs:          1000,
			expectExactSearch:  false,
			expectNProbeMin:    cfg.MinNProbe,
			expectNProbeMax:    cfg.MinNProbe,
			expectCandidatesMin: 10,
			expectCandidatesMax: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel := &FilterSelectivity{
				Selectivity: tt.selectivity,
				SampleSize:  tt.totalDocs,
				PassCount:   int(tt.selectivity * float64(tt.totalDocs)),
			}

			plan := PlanRecallAwareSearch(sel, tt.topK, tt.totalDocs, cfg)

			if plan.UseExactSearch != tt.expectExactSearch {
				t.Errorf("UseExactSearch = %v, want %v", plan.UseExactSearch, tt.expectExactSearch)
			}

			if !tt.expectExactSearch {
				if plan.NProbe < tt.expectNProbeMin || plan.NProbe > tt.expectNProbeMax {
					t.Errorf("NProbe = %d, want [%d, %d]", plan.NProbe, tt.expectNProbeMin, tt.expectNProbeMax)
				}

				if plan.Candidates < tt.expectCandidatesMin || plan.Candidates > tt.expectCandidatesMax {
					t.Errorf("Candidates = %d, want [%d, %d]", plan.Candidates, tt.expectCandidatesMin, tt.expectCandidatesMax)
				}
			}
		})
	}
}

func TestShouldRetryWithMoreCandidates(t *testing.T) {
	cfg := DefaultFilterSelectivityConfig()

	tests := []struct {
		name          string
		plan          *RecallAwarePlan
		filteredCount int
		topK          int
		expectRetry   bool
	}{
		{
			name: "enough results - no retry",
			plan: &RecallAwarePlan{
				UseExactSearch: false,
				NProbe:         16,
				Candidates:     100,
				Selectivity:    0.5,
			},
			filteredCount: 10,
			topK:          10,
			expectRetry:   false,
		},
		{
			name: "too few results - should retry",
			plan: &RecallAwarePlan{
				UseExactSearch: false,
				NProbe:         16,
				Candidates:     50,
				Selectivity:    0.1,
			},
			filteredCount: 5,
			topK:          10,
			expectRetry:   true,
		},
		{
			name: "exact search - no retry",
			plan: &RecallAwarePlan{
				UseExactSearch: true,
				NProbe:         0,
				Candidates:     0,
				Selectivity:    0.001,
			},
			filteredCount: 3,
			topK:          10,
			expectRetry:   false,
		},
		{
			name: "already at max budget - no retry",
			plan: &RecallAwarePlan{
				UseExactSearch: false,
				NProbe:         cfg.MaxNProbe,
				Candidates:     int(float64(10) * cfg.MaxOversampleFactor),
				Selectivity:    0.1,
			},
			filteredCount: 5,
			topK:          10,
			expectRetry:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &AdaptiveSearchResult{
				Plan:                tt.plan,
				FilteredCount:       tt.filteredCount,
				CandidatesRetrieved: tt.plan.Candidates,
			}

			shouldRetry, _, _ := ShouldRetryWithMoreCandidates(result, tt.topK, cfg)
			if shouldRetry != tt.expectRetry {
				t.Errorf("shouldRetry = %v, want %v", shouldRetry, tt.expectRetry)
			}
		})
	}
}

func TestDefaultFilterSelectivityConfig(t *testing.T) {
	cfg := DefaultFilterSelectivityConfig()

	if cfg.ExactSearchThreshold <= 0 || cfg.ExactSearchThreshold >= 1 {
		t.Errorf("ExactSearchThreshold = %v, want (0, 1)", cfg.ExactSearchThreshold)
	}

	if cfg.MinOversampleFactor < 1 {
		t.Errorf("MinOversampleFactor = %v, want >= 1", cfg.MinOversampleFactor)
	}

	if cfg.MaxOversampleFactor < cfg.MinOversampleFactor {
		t.Errorf("MaxOversampleFactor = %v < MinOversampleFactor = %v", cfg.MaxOversampleFactor, cfg.MinOversampleFactor)
	}

	if cfg.MinNProbe < 1 {
		t.Errorf("MinNProbe = %v, want >= 1", cfg.MinNProbe)
	}

	if cfg.MaxNProbe < cfg.MinNProbe {
		t.Errorf("MaxNProbe = %v < MinNProbe = %v", cfg.MaxNProbe, cfg.MinNProbe)
	}

	if cfg.SampleSize <= 0 {
		t.Errorf("SampleSize = %v, want > 0", cfg.SampleSize)
	}
}

// Helper functions

func makeDocs(count int, attrName string, values []any) []*tail.Document {
	docs := make([]*tail.Document, count)
	for i := 0; i < count; i++ {
		docs[i] = &tail.Document{
			ID: document.NewU64ID(uint64(i)),
			Attributes: map[string]any{
				attrName: values[i%len(values)],
			},
			WalSeq: 1,
		}
	}
	return docs
}

func makeCategories(count int) []any {
	cats := make([]any, count)
	for i := 0; i < count; i++ {
		cats[i] = "cat_" + intToStr(i)
	}
	return cats
}

func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	result := ""
	for n > 0 {
		result = string(rune('0'+n%10)) + result
		n /= 10
	}
	return result
}
