package query

import (
	"context"

	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/tail"
)

// FilterSelectivityConfig holds configuration for filter selectivity estimation.
type FilterSelectivityConfig struct {
	// ExactSearchThreshold is the selectivity threshold below which exact search is used.
	// If estimated selectivity < threshold, we skip ANN and do exact search.
	// Default: 0.01 (1% of documents passing filter triggers exact search)
	ExactSearchThreshold float64

	// MinOversampleFactor is the minimum oversampling factor for filtered ANN.
	// Candidates = topK * MinOversampleFactor.
	// Default: 2.0
	MinOversampleFactor float64

	// MaxOversampleFactor is the maximum oversampling factor for filtered ANN.
	// Candidates = min(topK * MaxOversampleFactor, total_docs).
	// Default: 10.0
	MaxOversampleFactor float64

	// MinNProbe is the minimum number of clusters to probe.
	// Default: 16
	MinNProbe int

	// MaxNProbe is the maximum number of clusters to probe.
	// Default: 64
	MaxNProbe int

	// SampleSize is the number of documents to sample for selectivity estimation.
	// Default: 1000
	SampleSize int
}

// DefaultFilterSelectivityConfig returns the default configuration.
func DefaultFilterSelectivityConfig() FilterSelectivityConfig {
	return FilterSelectivityConfig{
		ExactSearchThreshold: 0.01,  // 1% selectivity -> exact search
		MinOversampleFactor:  2.0,   // At least 2x candidates
		MaxOversampleFactor:  10.0,  // At most 10x candidates
		MinNProbe:            16,    // Minimum probes
		MaxNProbe:            64,    // Maximum probes
		SampleSize:           1000,  // Sample 1000 docs for estimation
	}
}

// FilterSelectivity holds the estimated selectivity of a filter.
type FilterSelectivity struct {
	// Selectivity is the estimated fraction of documents that pass the filter.
	// Range: [0.0, 1.0] where 1.0 means all documents pass.
	Selectivity float64

	// SampleSize is the number of documents sampled for estimation.
	SampleSize int

	// PassCount is the number of sampled documents that passed the filter.
	PassCount int

	// EstimatedTotalDocs is an estimate of the total document count in the namespace.
	// This is calculated from TailBytes / average doc size from sample.
	EstimatedTotalDocs int
}

// EstimateFilterSelectivity estimates the selectivity of a filter by sampling documents.
// It returns the fraction of documents that are expected to pass the filter.
//
// Selectivity = (docs passing filter) / (total docs)
// Range: [0.0, 1.0] where 1.0 means all documents pass.
//
// This function uses bounded sampling via ScanWithByteLimit to avoid loading
// all documents into memory. The byte limit is calculated to fetch approximately
// sampleSize documents while keeping memory usage bounded.
//
// The function also estimates total document count using TailBytes and the
// average size per document from the sample.
func EstimateFilterSelectivity(ctx context.Context, ts tail.Store, ns string, f *filter.Filter, sampleSize int) (*FilterSelectivity, error) {
	if f == nil {
		// No filter means all documents pass
		return &FilterSelectivity{
			Selectivity:        1.0,
			SampleSize:         0,
			PassCount:          0,
			EstimatedTotalDocs: 0,
		}, nil
	}

	if sampleSize <= 0 {
		sampleSize = DefaultFilterSelectivityConfig().SampleSize
	}

	// Use byte-limited scan to avoid loading all documents.
	// Estimate ~1KB per document average (conservative estimate).
	// This bounds memory usage while still getting a reasonable sample.
	byteLimitPerDoc := int64(1024) // 1KB per doc estimate
	byteLimit := int64(sampleSize) * byteLimitPerDoc

	docs, err := ts.ScanWithByteLimit(ctx, ns, nil, byteLimit)
	if err != nil {
		return nil, err
	}

	if len(docs) == 0 {
		return &FilterSelectivity{
			Selectivity:        1.0,
			SampleSize:         0,
			PassCount:          0,
			EstimatedTotalDocs: 0,
		}, nil
	}

	// Limit to requested sample size if we got more
	sampled := docs
	if len(docs) > sampleSize {
		sampled = docs[:sampleSize]
	}

	// Count how many sampled documents pass the filter
	passCount := 0
	for _, doc := range sampled {
		filterDoc := make(filter.Document)
		for k, v := range doc.Attributes {
			filterDoc[k] = v
		}
		if f.Eval(filterDoc) {
			passCount++
		}
	}

	selectivity := float64(passCount) / float64(len(sampled))

	// Estimate total document count using TailBytes and sample size.
	// If we used the full byteLimit and got fewer docs, that's our total.
	// Otherwise, extrapolate from byte ratio.
	estimatedTotalDocs := len(sampled)
	tailBytes := ts.TailBytes(ns)
	if tailBytes > 0 && len(sampled) > 0 {
		// Estimate bytes per doc from what we sampled
		// Use the byteLimit we requested divided by docs we got
		avgBytesPerDoc := float64(byteLimit) / float64(len(docs))
		if avgBytesPerDoc > 0 {
			estimatedTotalDocs = int(float64(tailBytes) / avgBytesPerDoc)
			if estimatedTotalDocs < len(sampled) {
				estimatedTotalDocs = len(sampled)
			}
		}
	}

	return &FilterSelectivity{
		Selectivity:        selectivity,
		SampleSize:         len(sampled),
		PassCount:          passCount,
		EstimatedTotalDocs: estimatedTotalDocs,
	}, nil
}

// RecallAwarePlan holds the search plan for recall-aware filtering.
type RecallAwarePlan struct {
	// UseExactSearch indicates whether to use exhaustive exact search.
	UseExactSearch bool

	// NProbe is the number of clusters to probe in ANN search.
	NProbe int

	// Candidates is the number of candidates to retrieve from ANN.
	Candidates int

	// Selectivity is the estimated filter selectivity.
	Selectivity float64
}

// PlanRecallAwareSearch creates a search plan based on filter selectivity.
// It determines whether to use exact search or ANN, and if ANN, how many
// candidates and probes to use for good recall with the filter.
func PlanRecallAwareSearch(selectivity *FilterSelectivity, topK int, totalDocs int, cfg FilterSelectivityConfig) *RecallAwarePlan {
	if selectivity == nil || selectivity.Selectivity >= 1.0 {
		// No filter or all docs pass - use standard ANN
		return &RecallAwarePlan{
			UseExactSearch: false,
			NProbe:         cfg.MinNProbe,
			Candidates:     topK,
			Selectivity:    1.0,
		}
	}

	sel := selectivity.Selectivity

	// Very selective filter - use exact search for guaranteed recall
	if sel < cfg.ExactSearchThreshold {
		return &RecallAwarePlan{
			UseExactSearch: true,
			NProbe:         0,
			Candidates:     0,
			Selectivity:    sel,
		}
	}

	// Calculate oversampling factor based on selectivity
	// The lower the selectivity, the more we need to oversample
	// oversampleFactor = 1 / selectivity, clamped to [min, max]
	oversampleFactor := 1.0 / sel
	if oversampleFactor < cfg.MinOversampleFactor {
		oversampleFactor = cfg.MinOversampleFactor
	}
	if oversampleFactor > cfg.MaxOversampleFactor {
		oversampleFactor = cfg.MaxOversampleFactor
	}

	// Calculate candidates
	candidates := int(float64(topK) * oversampleFactor)
	if candidates > totalDocs {
		candidates = totalDocs
	}
	if candidates < topK {
		candidates = topK
	}

	// Calculate nProbe based on selectivity
	// Lower selectivity -> more probes for better recall
	// Linear interpolation between MinNProbe and MaxNProbe based on selectivity
	nProbeRange := float64(cfg.MaxNProbe - cfg.MinNProbe)
	// When selectivity is 1.0, use MinNProbe
	// When selectivity is ExactSearchThreshold, use MaxNProbe
	selRange := 1.0 - cfg.ExactSearchThreshold
	if selRange <= 0 {
		selRange = 1.0
	}
	selNormalized := (sel - cfg.ExactSearchThreshold) / selRange
	if selNormalized < 0 {
		selNormalized = 0
	}
	if selNormalized > 1 {
		selNormalized = 1
	}

	// Invert: low selectivity -> high nProbe
	nProbe := cfg.MaxNProbe - int(selNormalized*nProbeRange)
	if nProbe < cfg.MinNProbe {
		nProbe = cfg.MinNProbe
	}
	if nProbe > cfg.MaxNProbe {
		nProbe = cfg.MaxNProbe
	}

	return &RecallAwarePlan{
		UseExactSearch: false,
		NProbe:         nProbe,
		Candidates:     candidates,
		Selectivity:    sel,
	}
}

// AdaptiveSearchResult holds results from recall-aware search.
type AdaptiveSearchResult struct {
	// Plan is the search plan that was used.
	Plan *RecallAwarePlan

	// FilteredCount is the number of results after filtering.
	FilteredCount int

	// CandidatesRetrieved is the number of candidates retrieved from ANN.
	CandidatesRetrieved int

	// RecallEstimate is an estimate of recall (filtered_count / expected).
	// This is only meaningful when we have selectivity estimates.
	RecallEstimate float64
}

// ShouldRetryWithMoreCandidates checks if we should retry with more candidates.
// This implements adaptive budget increase.
func ShouldRetryWithMoreCandidates(result *AdaptiveSearchResult, topK int, cfg FilterSelectivityConfig) (bool, int, int) {
	if result.Plan.UseExactSearch {
		// Already using exact search, no retry needed
		return false, 0, 0
	}

	// Check if we got enough results
	if result.FilteredCount >= topK {
		return false, 0, 0
	}

	// Check if we can increase budget
	currentCandidates := result.CandidatesRetrieved
	currentNProbe := result.Plan.NProbe

	// Calculate new budget
	newCandidates := currentCandidates * 2
	newNProbe := currentNProbe + (cfg.MaxNProbe-cfg.MinNProbe)/4

	if newNProbe > cfg.MaxNProbe {
		newNProbe = cfg.MaxNProbe
	}

	// If we're already at max, fall back to exact search
	if currentNProbe >= cfg.MaxNProbe && currentCandidates >= int(float64(topK)*cfg.MaxOversampleFactor) {
		return false, 0, 0
	}

	return true, newNProbe, newCandidates
}
