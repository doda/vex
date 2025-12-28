package query

import (
	"context"
	"math"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// vectorTestTailStore is a mock tail store for vector ANN tests
type vectorTestTailStore struct {
	docs   []*tail.Document
	metric tail.DistanceMetric
}

func (m *vectorTestTailStore) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *vectorTestTailStore) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	return m.docs, nil
}

func (m *vectorTestTailStore) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	m.metric = metric // record which metric was used

	var results []tail.VectorScanResult
	for _, doc := range m.docs {
		if doc.Vector == nil || doc.Deleted {
			continue
		}

		if f != nil {
			filterDoc := make(filter.Document)
			for k, v := range doc.Attributes {
				filterDoc[k] = v
			}
			if !f.Eval(filterDoc) {
				continue
			}
		}

		dist := computeDistance(queryVector, doc.Vector, metric)
		results = append(results, tail.VectorScanResult{
			Doc:      doc,
			Distance: dist,
		})
	}

	// Sort by distance
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Distance < results[i].Distance {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	if len(results) > topK {
		results = results[:topK]
	}

	return results, nil
}

func computeDistance(a, b []float32, metric tail.DistanceMetric) float64 {
	if len(a) != len(b) {
		return math.MaxFloat64
	}

	switch metric {
	case tail.MetricCosineDistance:
		var dot, normA, normB float64
		for i := 0; i < len(a); i++ {
			ai := float64(a[i])
			bi := float64(b[i])
			dot += ai * bi
			normA += ai * ai
			normB += bi * bi
		}
		if normA == 0 || normB == 0 {
			return 1.0
		}
		return 1.0 - dot/(math.Sqrt(normA)*math.Sqrt(normB))
	case tail.MetricEuclideanSquared:
		var sum float64
		for i := 0; i < len(a); i++ {
			diff := float64(a[i]) - float64(b[i])
			sum += diff * diff
		}
		return sum
	default:
		return 0
	}
}

func (m *vectorTestTailStore) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *vectorTestTailStore) TailBytes(ns string) int64 {
	return 0
}

func (m *vectorTestTailStore) Clear(ns string) {}

func (m *vectorTestTailStore) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *vectorTestTailStore) Close() error {
	return nil
}

// TestExhaustiveVectorSearch tests that exact exhaustive vector search works before ANN index
func TestExhaustiveVectorSearch(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Create documents with vectors
	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"name": "doc1"},
				Vector:     []float32{1.0, 0.0, 0.0}, // Unit vector along x-axis
			},
			{
				ID:         document.NewU64ID(2),
				Attributes: map[string]any{"name": "doc2"},
				Vector:     []float32{0.0, 1.0, 0.0}, // Unit vector along y-axis
			},
			{
				ID:         document.NewU64ID(3),
				Attributes: map[string]any{"name": "doc3"},
				Vector:     []float32{0.707, 0.707, 0.0}, // 45 degrees between x and y
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	// Query with a vector along the x-axis
	req := &QueryRequest{
		RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should return all 3 documents
	if len(resp.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(resp.Rows))
	}

	// First result should be doc1 (exact match, distance ~0)
	if resp.Rows[0].ID != uint64(1) {
		t.Errorf("expected first result to be doc1, got %v", resp.Rows[0].ID)
	}
	if resp.Rows[0].Dist == nil || *resp.Rows[0].Dist > 0.001 {
		t.Errorf("expected distance ~0 for exact match, got %v", resp.Rows[0].Dist)
	}

	// Second result should be doc3 (45 degrees, distance ~0.293)
	if resp.Rows[1].ID != uint64(3) {
		t.Errorf("expected second result to be doc3, got %v", resp.Rows[1].ID)
	}

	// Third result should be doc2 (90 degrees, distance ~1)
	if resp.Rows[2].ID != uint64(2) {
		t.Errorf("expected third result to be doc2, got %v", resp.Rows[2].ID)
	}
}

// TestDistContainsDistanceFromQueryVector verifies $dist contains distance from query vector
func TestDistContainsDistanceFromQueryVector(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"name": "doc1"},
				Vector:     []float32{1.0, 0.0, 0.0},
			},
			{
				ID:         document.NewU64ID(2),
				Attributes: map[string]any{"name": "doc2"},
				Vector:     []float32{0.0, 1.0, 0.0},
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// All rows should have $dist set
	for i, row := range resp.Rows {
		if row.Dist == nil {
			t.Errorf("row %d: expected $dist to be set", i)
			continue
		}
		// Verify the $dist value is a valid distance (non-negative)
		if *row.Dist < 0 {
			t.Errorf("row %d: expected non-negative $dist, got %f", i, *row.Dist)
		}
	}

	// Verify RowToJSON includes $dist
	for i, row := range resp.Rows {
		json := RowToJSON(row)
		if _, ok := json["$dist"]; !ok {
			t.Errorf("row %d: RowToJSON should include $dist", i)
		}
	}
}

// TestVectorEncodingFloat tests that vector_encoding: float accepts float array
func TestVectorEncodingFloat(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:     document.NewU64ID(1),
				Vector: []float32{1.0, 2.0, 3.0},
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	// Test with float array (default encoding)
	req := &QueryRequest{
		RankBy:         []any{"vector", "ANN", []any{1.0, 2.0, 3.0}},
		VectorEncoding: "float",
		Limit:          10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() with float encoding error = %v", err)
	}

	if len(resp.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(resp.Rows))
	}
}

// TestVectorEncodingBase64 tests that vector_encoding: base64 accepts base64 encoded vector
func TestVectorEncodingBase64(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:     document.NewU64ID(1),
				Vector: []float32{1.0, 2.0, 3.0},
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	// Base64 encoding of [1.0, 2.0, 3.0] as little-endian float32
	// 1.0 = 0x3f800000, 2.0 = 0x40000000, 3.0 = 0x40400000
	base64Vec := vector.EncodeVectorBase64([]float32{1.0, 2.0, 3.0})

	req := &QueryRequest{
		RankBy:         []any{"vector", "ANN", base64Vec},
		VectorEncoding: "base64",
		Limit:          10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() with base64 encoding error = %v", err)
	}

	if len(resp.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(resp.Rows))
	}

	// Distance should be 0 (exact match)
	if resp.Rows[0].Dist == nil || *resp.Rows[0].Dist > 0.001 {
		t.Errorf("expected distance ~0 for exact match, got %v", resp.Rows[0].Dist)
	}
}

// TestCosineDistanceMetric verifies cosine_distance metric works correctly
func TestCosineDistanceMetric(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	loaded, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Set up namespace with cosine_distance metric
	_, err = stateMan.Update(ctx, "test-ns", loaded.ETag, func(state *namespace.State) error {
		state.Vector = &namespace.VectorConfig{
			Dims:           3,
			DType:          "f32",
			DistanceMetric: "cosine_distance",
			ANN:            true,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:     document.NewU64ID(1),
				Vector: []float32{1.0, 0.0, 0.0},
			},
			{
				ID:     document.NewU64ID(2),
				Vector: []float32{0.0, 1.0, 0.0},
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Verify that cosine distance was used
	if mockTail.metric != tail.MetricCosineDistance {
		t.Errorf("expected MetricCosineDistance, got %v", mockTail.metric)
	}

	// First result should have distance ~0 (same direction)
	if resp.Rows[0].Dist == nil || *resp.Rows[0].Dist > 0.001 {
		t.Errorf("expected distance ~0 for identical vectors, got %f", *resp.Rows[0].Dist)
	}

	// Second result should have distance ~1 (orthogonal)
	if resp.Rows[1].Dist == nil || math.Abs(*resp.Rows[1].Dist-1.0) > 0.001 {
		t.Errorf("expected distance ~1 for orthogonal vectors, got %f", *resp.Rows[1].Dist)
	}
}

// TestEuclideanSquaredMetric verifies euclidean_squared metric works correctly
func TestEuclideanSquaredMetric(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	loaded, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Set up namespace with euclidean_squared metric
	_, err = stateMan.Update(ctx, "test-ns", loaded.ETag, func(state *namespace.State) error {
		state.Vector = &namespace.VectorConfig{
			Dims:           3,
			DType:          "f32",
			DistanceMetric: "euclidean_squared",
			ANN:            true,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:     document.NewU64ID(1),
				Vector: []float32{1.0, 0.0, 0.0},
			},
			{
				ID:     document.NewU64ID(2),
				Vector: []float32{2.0, 0.0, 0.0},
			},
			{
				ID:     document.NewU64ID(3),
				Vector: []float32{4.0, 0.0, 0.0},
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Verify that euclidean squared was used
	if mockTail.metric != tail.MetricEuclideanSquared {
		t.Errorf("expected MetricEuclideanSquared, got %v", mockTail.metric)
	}

	// First result should have distance 0 (same point)
	if resp.Rows[0].ID != uint64(1) {
		t.Errorf("expected first result to be doc1, got %v", resp.Rows[0].ID)
	}
	if resp.Rows[0].Dist == nil || *resp.Rows[0].Dist > 0.001 {
		t.Errorf("expected distance 0 for identical vectors, got %f", *resp.Rows[0].Dist)
	}

	// Second result should have distance 1 (1^2 = 1)
	if resp.Rows[1].ID != uint64(2) {
		t.Errorf("expected second result to be doc2, got %v", resp.Rows[1].ID)
	}
	if resp.Rows[1].Dist == nil || math.Abs(*resp.Rows[1].Dist-1.0) > 0.001 {
		t.Errorf("expected distance 1 for doc2, got %f", *resp.Rows[1].Dist)
	}

	// Third result should have distance 9 (3^2 = 9)
	if resp.Rows[2].ID != uint64(3) {
		t.Errorf("expected third result to be doc3, got %v", resp.Rows[2].ID)
	}
	if resp.Rows[2].Dist == nil || math.Abs(*resp.Rows[2].Dist-9.0) > 0.001 {
		t.Errorf("expected distance 9 for doc3, got %f", *resp.Rows[2].Dist)
	}
}

// TestParseVectorBase64 tests that parseVector handles base64 strings
func TestParseVectorBase64(t *testing.T) {
	// Encode a known vector
	original := []float32{1.0, 2.0, 3.0, 4.0}
	base64Str := vector.EncodeVectorBase64(original)

	// Parse it back
	parsed, err := parseVector(base64Str, "base64")
	if err != nil {
		t.Fatalf("parseVector(base64) error = %v", err)
	}

	if len(parsed) != len(original) {
		t.Errorf("expected length %d, got %d", len(original), len(parsed))
	}

	for i, v := range parsed {
		if math.Abs(float64(v-original[i])) > 0.0001 {
			t.Errorf("element %d: expected %f, got %f", i, original[i], v)
		}
	}
}

// TestVectorQueryWithLimit tests that limit is respected
func TestVectorQueryWithLimit(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Create many documents
	docs := make([]*tail.Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = &tail.Document{
			ID:     document.NewU64ID(uint64(i + 1)),
			Vector: []float32{float32(i), 0.0, 0.0},
		}
	}

	mockTail := &vectorTestTailStore{docs: docs}
	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy: []any{"vector", "ANN", []any{0.0, 0.0, 0.0}},
		Limit:  5,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if len(resp.Rows) != 5 {
		t.Errorf("expected 5 rows (limit), got %d", len(resp.Rows))
	}
}

// TestVectorQueryWithFilter tests that filters work with vector search
func TestVectorQueryWithFilter(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Attributes: map[string]any{"category": "A"},
				Vector:     []float32{1.0, 0.0, 0.0},
			},
			{
				ID:         document.NewU64ID(2),
				Attributes: map[string]any{"category": "B"},
				Vector:     []float32{0.9, 0.1, 0.0},
			},
			{
				ID:         document.NewU64ID(3),
				Attributes: map[string]any{"category": "A"},
				Vector:     []float32{0.8, 0.2, 0.0},
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy:  []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
		Filters: []any{"category", "Eq", "A"},
		Limit:   10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should only return documents with category "A"
	if len(resp.Rows) != 2 {
		t.Errorf("expected 2 rows (filtered), got %d", len(resp.Rows))
	}

	for _, row := range resp.Rows {
		id := row.ID.(uint64)
		if id != 1 && id != 3 {
			t.Errorf("unexpected document id: %v", id)
		}
	}
}

// TestDefaultDistanceMetric tests that cosine_distance is the default when no metric is configured
func TestDefaultDistanceMetric(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:     document.NewU64ID(1),
				Vector: []float32{1.0, 0.0, 0.0},
			},
		},
	}

	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
		Limit:  10,
	}

	_, err = h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Verify that cosine distance is the default
	if mockTail.metric != tail.MetricCosineDistance {
		t.Errorf("expected default MetricCosineDistance, got %v", mockTail.metric)
	}
}
