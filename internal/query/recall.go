// Package query implements the query execution engine for Vex.
package query

import (
	"context"
	"errors"
	"math/rand"
	"sort"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/vector"
)

// RecallRequest represents a recall debug request.
type RecallRequest struct {
	Num   int `json:"num"`    // Number of vectors to sample
	TopK  int `json:"top_k"`  // Number of results to compare
}

// RecallResponse represents the response from a recall debug operation.
type RecallResponse struct {
	AvgRecall           float64 `json:"avg_recall"`
	AvgANNCount         float64 `json:"avg_ann_count"`
	AvgExhaustiveCount  float64 `json:"avg_exhaustive_count"`
}

// DefaultRecallNum is the default number of vectors to sample.
const DefaultRecallNum = 10

// DefaultRecallTopK is the default top_k for recall comparison.
const DefaultRecallTopK = 10

// HandleRecall executes a recall debug operation against the given namespace.
// It samples vectors, runs ANN and exhaustive searches, and computes recall metrics.
func (h *Handler) HandleRecall(ctx context.Context, ns string, req *RecallRequest) (*RecallResponse, error) {
	// Apply defaults
	if req.Num <= 0 {
		req.Num = DefaultRecallNum
	}
	if req.TopK <= 0 {
		req.TopK = DefaultRecallTopK
	}

	// Load namespace state
	loaded, err := h.stateMan.Load(ctx, ns)
	if err != nil {
		if errors.Is(err, namespace.ErrStateNotFound) {
			return nil, ErrNamespaceNotFound
		}
		return nil, err
	}

	// Determine distance metric from namespace config
	metric := tail.MetricCosineDistance
	vectorMetric := vector.MetricCosineDistance
	if loaded.State.Vector != nil && loaded.State.Vector.DistanceMetric != "" {
		switch loaded.State.Vector.DistanceMetric {
		case string(vector.MetricCosineDistance):
			metric = tail.MetricCosineDistance
			vectorMetric = vector.MetricCosineDistance
		case string(vector.MetricEuclideanSquared):
			metric = tail.MetricEuclideanSquared
			vectorMetric = vector.MetricEuclideanSquared
		case string(vector.MetricDotProduct):
			metric = tail.MetricDotProduct
			vectorMetric = vector.MetricDotProduct
		}
	}

	// Refresh tail for strong consistency
	if h.tailStore != nil {
		headSeq := loaded.State.WAL.HeadSeq
		indexedSeq := loaded.State.Index.IndexedWALSeq
		if headSeq > indexedSeq {
			if err := h.tailStore.Refresh(ctx, ns, indexedSeq, headSeq); err != nil {
				return nil, err
			}
		}
	}

	// Get all documents with vectors from tail
	var docs []*tail.Document
	if h.tailStore != nil {
		docs, err = h.tailStore.Scan(ctx, ns, nil)
		if err != nil {
			return nil, err
		}
	}

	// Filter to documents with vectors only
	var docsWithVectors []*tail.Document
	for _, doc := range docs {
		if doc.Vector != nil && len(doc.Vector) > 0 && !doc.Deleted {
			docsWithVectors = append(docsWithVectors, doc)
		}
	}

	// If no documents with vectors, return perfect recall (trivially)
	if len(docsWithVectors) == 0 {
		return &RecallResponse{
			AvgRecall:          1.0,
			AvgANNCount:        0,
			AvgExhaustiveCount: 0,
		}, nil
	}

	// Sample vectors
	sampleSize := req.Num
	if sampleSize > len(docsWithVectors) {
		sampleSize = len(docsWithVectors)
	}

	// Random sampling without replacement
	indices := make([]int, len(docsWithVectors))
	for i := range indices {
		indices[i] = i
	}
	rand.Shuffle(len(indices), func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})
	sampleIndices := indices[:sampleSize]

	// Check if we have an IVF index
	useIndex := loaded.State.Index.ManifestKey != "" && h.indexReader != nil
	var ivfReader *vector.IVFReader
	var clusterDataKey string
	if useIndex {
		ivfReader, clusterDataKey, err = h.indexReader.GetIVFReader(ctx, ns, loaded.State.Index.ManifestKey, loaded.State.Index.ManifestSeq)
		if err != nil {
			// Fall back to exhaustive only
			useIndex = false
		}
	}

	// For each sampled vector, compare ANN vs exhaustive results
	var totalRecall float64
	var totalANNCount, totalExhaustiveCount int64

	for _, idx := range sampleIndices {
		queryDoc := docsWithVectors[idx]
		queryVector := queryDoc.Vector

		// Run exhaustive search
		exhaustiveResults, err := h.runExhaustiveSearch(ctx, ns, queryVector, req.TopK, metric)
		if err != nil {
			return nil, err
		}
		exhaustiveCount := len(exhaustiveResults)

		// Run ANN search
		var annResults []annResult
		if useIndex && ivfReader != nil {
			annResults, err = h.runANNSearch(ctx, ns, ivfReader, clusterDataKey, queryVector, req.TopK, vectorMetric, loaded)
			if err != nil {
				// Fall back to exhaustive as ANN result
				annResults = exhaustiveToANN(exhaustiveResults)
			}
		} else {
			// No index, ANN is same as exhaustive
			annResults = exhaustiveToANN(exhaustiveResults)
		}
		annCount := len(annResults)

		// Compute recall: intersection of ANN results with exhaustive results / exhaustive count
		recall := computeRecall(annResults, exhaustiveResults)

		totalRecall += recall
		totalANNCount += int64(annCount)
		totalExhaustiveCount += int64(exhaustiveCount)
	}

	// Compute averages
	avgRecall := totalRecall / float64(sampleSize)
	avgANNCount := float64(totalANNCount) / float64(sampleSize)
	avgExhaustiveCount := float64(totalExhaustiveCount) / float64(sampleSize)

	return &RecallResponse{
		AvgRecall:          avgRecall,
		AvgANNCount:        avgANNCount,
		AvgExhaustiveCount: avgExhaustiveCount,
	}, nil
}

// annResult represents an ANN search result for recall computation.
type annResult struct {
	DocID    uint64
	Distance float64
}

// exhaustiveResult represents an exhaustive search result.
type exhaustiveResult struct {
	DocID    uint64
	Distance float64
}

// runExhaustiveSearch runs an exhaustive vector search over all documents.
func (h *Handler) runExhaustiveSearch(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric) ([]exhaustiveResult, error) {
	if h.tailStore == nil {
		return nil, nil
	}

	results, err := h.tailStore.VectorScan(ctx, ns, queryVector, topK, metric, nil)
	if err != nil {
		return nil, err
	}

	exhaustive := make([]exhaustiveResult, 0, len(results))
	for _, r := range results {
		exhaustive = append(exhaustive, exhaustiveResult{
			DocID:    r.Doc.ID.U64(),
			Distance: r.Distance,
		})
	}
	return exhaustive, nil
}

// runANNSearch runs an ANN search using the IVF index.
func (h *Handler) runANNSearch(ctx context.Context, ns string, ivfReader *vector.IVFReader, clusterDataKey string, queryVector []float32, topK int, metric vector.DistanceMetric, loaded *namespace.LoadedState) ([]annResult, error) {
	if ivfReader == nil {
		return nil, nil
	}

	// Use default nProbe
	nProbe := DefaultNProbe

	results, err := h.indexReader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVector, topK, nProbe)
	if err != nil {
		return nil, err
	}

	ann := make([]annResult, 0, len(results))
	for _, r := range results {
		ann = append(ann, annResult{
			DocID:    r.DocID,
			Distance: float64(r.Distance),
		})
	}

	// Sort by distance
	sort.Slice(ann, func(i, j int) bool {
		return ann[i].Distance < ann[j].Distance
	})

	return ann, nil
}

// exhaustiveToANN converts exhaustive results to ANN results.
func exhaustiveToANN(exhaustive []exhaustiveResult) []annResult {
	ann := make([]annResult, len(exhaustive))
	for i, e := range exhaustive {
		ann[i] = annResult{
			DocID:    e.DocID,
			Distance: e.Distance,
		}
	}
	return ann
}

// computeRecall computes recall as the fraction of exhaustive results found in ANN results.
func computeRecall(ann []annResult, exhaustive []exhaustiveResult) float64 {
	if len(exhaustive) == 0 {
		return 1.0 // Perfect recall if nothing to find
	}

	// Build a set of ANN doc IDs
	annSet := make(map[uint64]bool)
	for _, a := range ann {
		annSet[a.DocID] = true
	}

	// Count how many exhaustive results are in ANN results
	matches := 0
	for _, e := range exhaustive {
		if annSet[e.DocID] {
			matches++
		}
	}

	return float64(matches) / float64(len(exhaustive))
}
