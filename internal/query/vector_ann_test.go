package query

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/indexer"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/schema"
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

func (m *vectorTestTailStore) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.Scan(ctx, ns, f)
}

func (m *vectorTestTailStore) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	m.metric = metric // record which metric was used

	var results []tail.VectorScanResult
	for _, doc := range m.docs {
		if doc.Vector == nil || doc.Deleted {
			continue
		}

		if f != nil {
			filterDoc := buildVectorTestFilterDoc(doc)
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

// buildVectorTestFilterDoc creates a filter.Document including the "id" field.
func buildVectorTestFilterDoc(doc *tail.Document) filter.Document {
	filterDoc := make(filter.Document)
	for k, v := range doc.Attributes {
		filterDoc[k] = v
	}
	// Add "id" field for filtering by document ID
	switch doc.ID.Type() {
	case document.IDTypeU64:
		filterDoc["id"] = doc.ID.U64()
	case document.IDTypeUUID:
		filterDoc["id"] = doc.ID.UUID().String()
	case document.IDTypeString:
		filterDoc["id"] = doc.ID.String()
	default:
		filterDoc["id"] = doc.ID.String()
	}
	return filterDoc
}

func (m *vectorTestTailStore) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return m.VectorScan(ctx, ns, queryVector, topK, metric, f)
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

func encodeTestVector(vec []float32) []byte {
	buf := make([]byte, len(vec)*4)
	for i, v := range vec {
		bits := math.Float32bits(v)
		buf[i*4] = byte(bits)
		buf[i*4+1] = byte(bits >> 8)
		buf[i*4+2] = byte(bits >> 16)
		buf[i*4+3] = byte(bits >> 24)
	}
	return buf
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

func TestVectorANNRespectsSegmentTombstones(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req1")
	vec := []float32{1.0, 0.0, 0.0, 0.0}
	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		nil,
		encodeTestVector(vec),
		uint32(len(vec)),
	)
	entry.SubBatches = append(entry.SubBatches, batch)

	encoder, _ := wal.NewEncoder()
	encoded, err := encoder.Encode(entry)
	encoder.Close()
	if err != nil {
		t.Fatalf("failed to encode WAL entry: %v", err)
	}

	key1 := "vex/namespaces/test-ns/wal/00000000000000000001.wal.zst"
	if _, err := store.Put(ctx, key1, bytes.NewReader(encoded.Data), int64(len(encoded.Data)), nil); err != nil {
		t.Fatalf("failed to write WAL entry: %v", err)
	}

	loaded, err := stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to load namespace: %v", err)
	}
	loaded, err = stateMan.AdvanceWAL(ctx, "test-ns", loaded.ETag, key1, int64(len(encoded.Data)), nil)
	if err != nil {
		t.Fatalf("failed to advance WAL: %v", err)
	}

	idxer := indexer.New(store, stateMan, indexer.DefaultConfig(), nil)
	processor := indexer.NewL0SegmentProcessor(store, stateMan, nil, idxer)
	if _, err := processor.ProcessWAL(ctx, "test-ns", 0, 1, loaded.State, loaded.ETag); err != nil {
		t.Fatalf("failed to build initial segment: %v", err)
	}

	deleteEntry := wal.NewWalEntry("test-ns", 2)
	deleteBatch := wal.NewWriteSubBatch("req2")
	deleteBatch.AddDelete(&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}})
	deleteEntry.SubBatches = append(deleteEntry.SubBatches, deleteBatch)

	encoder, _ = wal.NewEncoder()
	deleteEncoded, err := encoder.Encode(deleteEntry)
	encoder.Close()
	if err != nil {
		t.Fatalf("failed to encode delete WAL: %v", err)
	}

	key2 := "vex/namespaces/test-ns/wal/00000000000000000002.wal.zst"
	if _, err := store.Put(ctx, key2, bytes.NewReader(deleteEncoded.Data), int64(len(deleteEncoded.Data)), nil); err != nil {
		t.Fatalf("failed to write delete WAL entry: %v", err)
	}

	loaded, err = stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to reload namespace: %v", err)
	}
	loaded, err = stateMan.AdvanceWAL(ctx, "test-ns", loaded.ETag, key2, int64(len(deleteEncoded.Data)), nil)
	if err != nil {
		t.Fatalf("failed to advance WAL for delete: %v", err)
	}
	if _, err := processor.ProcessWAL(ctx, "test-ns", 1, 2, loaded.State, loaded.ETag); err != nil {
		t.Fatalf("failed to build delete segment: %v", err)
	}

	h := NewHandler(store, stateMan, nil)
	req := &QueryRequest{
		RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0, 0.0}},
		Limit:  10,
	}
	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(resp.Rows) != 0 {
		t.Fatalf("expected 0 rows after delete, got %d", len(resp.Rows))
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

// TestANNSearchUsesIndexWhenAvailable tests that ANN search uses the IVF index when available
func TestANNSearchUsesIndexWhenAvailable(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()

	// Create namespace with an IVF index
	loaded, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Build an IVF index with test vectors
	dims := 4
	builder := vector.NewIVFBuilder(dims, vector.MetricCosineDistance, 2) // 2 clusters

	// Add vectors - cluster 0 near [1,0,0,0], cluster 1 near [0,1,0,0]
	builder.AddVector(1, []float32{0.95, 0.05, 0, 0})
	builder.AddVector(2, []float32{0.9, 0.1, 0, 0})
	builder.AddVector(3, []float32{0.1, 0.9, 0, 0})
	builder.AddVector(4, []float32{0.05, 0.95, 0, 0})

	ivfIndex, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build IVF index: %v", err)
	}

	// Write IVF files to object store
	centroidsKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin"
	offsetsKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin"
	clusterDataKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack"

	centroidsData := ivfIndex.GetCentroidsBytes()
	offsetsData := ivfIndex.GetClusterOffsetsBytes()
	clusterData := ivfIndex.GetClusterDataBytes()

	store.Put(ctx, centroidsKey, bytes.NewReader(centroidsData), int64(len(centroidsData)), nil)
	store.Put(ctx, offsetsKey, bytes.NewReader(offsetsData), int64(len(offsetsData)), nil)
	store.Put(ctx, clusterDataKey, bytes.NewReader(clusterData), int64(len(clusterData)), nil)

	// Create and store manifest
	manifestKey := "vex/namespaces/test-ns/index/manifests/000000000000000001.idx.json"
	manifest := map[string]any{
		"format_version":  1,
		"namespace":       "test-ns",
		"generated_at":    "2024-01-01T00:00:00Z",
		"indexed_wal_seq": uint64(10),
		"segments": []map[string]any{
			{
				"id":            "seg_001",
				"level":         0,
				"start_wal_seq": uint64(1),
				"end_wal_seq":   uint64(10),
				"ivf_keys": map[string]any{
					"centroids_key":       centroidsKey,
					"cluster_offsets_key": offsetsKey,
					"cluster_data_key":    clusterDataKey,
					"n_clusters":          2,
					"vector_count":        4,
				},
			},
		},
	}
	manifestData, _ := json.Marshal(manifest)
	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)

	// Update namespace state to point to the manifest
	// Need to update WAL head seq first (it must increase by exactly 1)
	currentETag := loaded.ETag
	for i := 1; i <= 10; i++ {
		updated, err := stateMan.AdvanceWAL(ctx, "test-ns", currentETag, wal.KeyForSeq(uint64(i)), 100, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL %d: %v", i, err)
		}
		currentETag = updated.ETag
	}

	_, err = stateMan.Update(ctx, "test-ns", currentETag, func(state *namespace.State) error {
		state.Index.ManifestKey = manifestKey
		state.Index.ManifestSeq = 1
		state.Index.IndexedWALSeq = 10
		state.Vector = &namespace.VectorConfig{
			Dims:           4,
			DType:          "f32",
			DistanceMetric: "cosine_distance",
			ANN:            true,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	// Create handler with empty tail (all data is indexed)
	mockTail := &vectorTestTailStore{docs: []*tail.Document{}}
	h := NewHandler(store, stateMan, mockTail)

	// Query for vectors near [1, 0, 0, 0]
	req := &QueryRequest{
		RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0, 0.0}},
		Limit:  4,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should return 4 results from the index
	if len(resp.Rows) != 4 {
		t.Errorf("expected 4 rows from index, got %d", len(resp.Rows))
	}

	// Results should be sorted by distance (ascending)
	for i := 1; i < len(resp.Rows); i++ {
		if resp.Rows[i].Dist == nil || resp.Rows[i-1].Dist == nil {
			t.Errorf("row %d: expected $dist to be set", i)
			continue
		}
		if *resp.Rows[i].Dist < *resp.Rows[i-1].Dist {
			t.Errorf("results not sorted: row[%d].Dist (%f) < row[%d].Dist (%f)",
				i, *resp.Rows[i].Dist, i-1, *resp.Rows[i-1].Dist)
		}
	}

	// First result should be doc 1 or 2 (closest to [1,0,0,0])
	firstID, ok := resp.Rows[0].ID.(uint64)
	if !ok {
		t.Errorf("expected first result ID to be uint64, got %T", resp.Rows[0].ID)
	}
	if firstID != 1 && firstID != 2 {
		t.Errorf("expected first result to be doc 1 or 2, got %v", firstID)
	}
}

// TestANNSearchMergesTailResults tests that ANN search merges index and tail results
func TestANNSearchMergesTailResults(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()

	// Create namespace
	loaded, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Build IVF index with indexed data (doc 1, 2)
	dims := 4
	builder := vector.NewIVFBuilder(dims, vector.MetricCosineDistance, 1) // 1 cluster for simplicity
	builder.AddVector(1, []float32{0.9, 0.1, 0, 0})
	builder.AddVector(2, []float32{0.8, 0.2, 0, 0})

	ivfIndex, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build IVF index: %v", err)
	}

	// Write IVF files to object store
	centroidsKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin"
	offsetsKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin"
	clusterDataKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack"

	store.Put(ctx, centroidsKey, bytes.NewReader(ivfIndex.GetCentroidsBytes()), int64(len(ivfIndex.GetCentroidsBytes())), nil)
	store.Put(ctx, offsetsKey, bytes.NewReader(ivfIndex.GetClusterOffsetsBytes()), int64(len(ivfIndex.GetClusterOffsetsBytes())), nil)
	store.Put(ctx, clusterDataKey, bytes.NewReader(ivfIndex.GetClusterDataBytes()), int64(len(ivfIndex.GetClusterDataBytes())), nil)

	// Create and store manifest
	manifestKey := "vex/namespaces/test-ns/index/manifests/000000000000000001.idx.json"
	manifest := map[string]any{
		"format_version":  1,
		"namespace":       "test-ns",
		"generated_at":    "2024-01-01T00:00:00Z",
		"indexed_wal_seq": uint64(10),
		"segments": []map[string]any{
			{
				"id":            "seg_001",
				"level":         0,
				"start_wal_seq": uint64(1),
				"end_wal_seq":   uint64(10),
				"ivf_keys": map[string]any{
					"centroids_key":       centroidsKey,
					"cluster_offsets_key": offsetsKey,
					"cluster_data_key":    clusterDataKey,
					"n_clusters":          1,
					"vector_count":        2,
				},
			},
		},
	}
	manifestData, _ := json.Marshal(manifest)
	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)

	// Update namespace state
	// Need to update WAL head seq first (it must increase by exactly 1)
	currentETag := loaded.ETag
	for i := 1; i <= 12; i++ {
		updated, err := stateMan.AdvanceWAL(ctx, "test-ns", currentETag, wal.KeyForSeq(uint64(i)), 100, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL %d: %v", i, err)
		}
		currentETag = updated.ETag
	}

	_, err = stateMan.Update(ctx, "test-ns", currentETag, func(state *namespace.State) error {
		state.Index.ManifestKey = manifestKey
		state.Index.ManifestSeq = 1
		state.Index.IndexedWALSeq = 10
		// WAL.HeadSeq = 12 (from AdvanceWAL)
		state.Vector = &namespace.VectorConfig{
			Dims:           4,
			DType:          "f32",
			DistanceMetric: "cosine_distance",
			ANN:            true,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	// Tail contains newer data (doc 3 with better distance than indexed docs)
	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:     document.NewU64ID(3),
				Vector: []float32{1.0, 0.0, 0, 0}, // Exact match, distance = 0
				WalSeq: 11,
			},
		},
	}
	h := NewHandler(store, stateMan, mockTail)

	// Query for vectors near [1, 0, 0, 0]
	req := &QueryRequest{
		RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0, 0.0}},
		Limit:  3,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should return 3 results (1 from tail + 2 from index)
	if len(resp.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(resp.Rows))
	}

	// First result should be doc 3 (exact match from tail, distance ~0)
	if len(resp.Rows) > 0 {
		firstID, ok := resp.Rows[0].ID.(uint64)
		if !ok {
			// It might be returned as uint64 directly from tail
			t.Logf("first result ID type: %T, value: %v", resp.Rows[0].ID, resp.Rows[0].ID)
		} else if firstID != 3 {
			t.Errorf("expected first result to be doc 3 (from tail), got %v", firstID)
		}

		if resp.Rows[0].Dist != nil && *resp.Rows[0].Dist > 0.01 {
			t.Errorf("expected first result (tail doc) to have distance ~0, got %f", *resp.Rows[0].Dist)
		}
	}
}

// TestANNSearchWithFilterSkipsIndex tests that filtered ANN queries skip the IVF index
// and use exhaustive search to ensure correct filter application.
func TestANNSearchWithFilterSkipsIndex(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()

	// Create namespace
	loaded, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Build IVF index with indexed data (docs 1, 2 without attributes)
	dims := 4
	builder := vector.NewIVFBuilder(dims, vector.MetricCosineDistance, 1)
	builder.AddVector(1, []float32{0.95, 0.05, 0, 0}) // Would match if category=A
	builder.AddVector(2, []float32{0.9, 0.1, 0, 0})   // Would NOT match (no category)

	ivfIndex, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build IVF index: %v", err)
	}

	// Write IVF files to object store
	centroidsKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin"
	offsetsKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin"
	clusterDataKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack"

	store.Put(ctx, centroidsKey, bytes.NewReader(ivfIndex.GetCentroidsBytes()), int64(len(ivfIndex.GetCentroidsBytes())), nil)
	store.Put(ctx, offsetsKey, bytes.NewReader(ivfIndex.GetClusterOffsetsBytes()), int64(len(ivfIndex.GetClusterOffsetsBytes())), nil)
	store.Put(ctx, clusterDataKey, bytes.NewReader(ivfIndex.GetClusterDataBytes()), int64(len(ivfIndex.GetClusterDataBytes())), nil)

	// Create and store manifest
	manifestKey := "vex/namespaces/test-ns/index/manifests/000000000000000001.idx.json"
	manifest := map[string]any{
		"format_version":  1,
		"namespace":       "test-ns",
		"generated_at":    "2024-01-01T00:00:00Z",
		"indexed_wal_seq": uint64(10),
		"segments": []map[string]any{
			{
				"id":            "seg_001",
				"level":         0,
				"start_wal_seq": uint64(1),
				"end_wal_seq":   uint64(10),
				"ivf_keys": map[string]any{
					"centroids_key":       centroidsKey,
					"cluster_offsets_key": offsetsKey,
					"cluster_data_key":    clusterDataKey,
					"n_clusters":          1,
					"vector_count":        2,
				},
			},
		},
	}
	manifestData, _ := json.Marshal(manifest)
	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)

	// Update namespace state
	currentETag := loaded.ETag
	for i := 1; i <= 12; i++ {
		updated, err := stateMan.AdvanceWAL(ctx, "test-ns", currentETag, wal.KeyForSeq(uint64(i)), 100, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL %d: %v", i, err)
		}
		currentETag = updated.ETag
	}

	_, err = stateMan.Update(ctx, "test-ns", currentETag, func(state *namespace.State) error {
		state.Index.ManifestKey = manifestKey
		state.Index.ManifestSeq = 1
		state.Index.IndexedWALSeq = 10
		state.Vector = &namespace.VectorConfig{
			Dims:           4,
			DType:          "f32",
			DistanceMetric: "cosine_distance",
			ANN:            true,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	// Tail contains docs with attributes that can be filtered
	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(3),
				Vector:     []float32{0.85, 0.15, 0, 0},
				Attributes: map[string]any{"category": "A"},
				WalSeq:     11,
			},
			{
				ID:         document.NewU64ID(4),
				Vector:     []float32{0.8, 0.2, 0, 0},
				Attributes: map[string]any{"category": "B"},
				WalSeq:     12,
			},
		},
	}
	h := NewHandler(store, stateMan, mockTail)

	// Query with filter - should only use tail (which has attributes)
	req := &QueryRequest{
		RankBy:  []any{"vector", "ANN", []any{1.0, 0.0, 0.0, 0.0}},
		Filters: []any{"category", "Eq", "A"},
		Limit:   10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should return only doc 3 (from tail, matches filter)
	// Docs 1 and 2 from index should NOT be returned (no filter support for index)
	// Doc 4 from tail should NOT be returned (category=B doesn't match filter)
	if len(resp.Rows) != 1 {
		t.Errorf("expected 1 row (only filtered tail results), got %d", len(resp.Rows))
	}

	if len(resp.Rows) > 0 {
		firstID, ok := resp.Rows[0].ID.(uint64)
		if !ok {
			t.Logf("first result ID type: %T, value: %v", resp.Rows[0].ID, resp.Rows[0].ID)
		} else if firstID != 3 {
			t.Errorf("expected result to be doc 3 (filtered tail doc), got %v", firstID)
		}
	}
}

// TestANNSearchWithFilterUsesIndexWhenBitmapsAvailable tests that filtered ANN queries
// use the IVF index when filter bitmap indexes are available in the segment.
func TestANNSearchWithFilterUsesIndexWhenBitmapsAvailable(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()

	// Create namespace
	loaded, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Build IVF index with indexed data
	dims := 4
	builder := vector.NewIVFBuilder(dims, vector.MetricCosineDistance, 1)
	builder.AddVector(1, []float32{0.95, 0.05, 0, 0}) // category=A, close to query
	builder.AddVector(2, []float32{0.9, 0.1, 0, 0})   // category=B, won't match filter
	builder.AddVector(3, []float32{0.85, 0.15, 0, 0}) // category=A, further from query

	ivfIndex, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build IVF index: %v", err)
	}

	// Write IVF files to object store
	centroidsKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin"
	offsetsKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin"
	clusterDataKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack"

	store.Put(ctx, centroidsKey, bytes.NewReader(ivfIndex.GetCentroidsBytes()), int64(len(ivfIndex.GetCentroidsBytes())), nil)
	store.Put(ctx, offsetsKey, bytes.NewReader(ivfIndex.GetClusterOffsetsBytes()), int64(len(ivfIndex.GetClusterOffsetsBytes())), nil)
	store.Put(ctx, clusterDataKey, bytes.NewReader(ivfIndex.GetClusterDataBytes()), int64(len(ivfIndex.GetClusterDataBytes())), nil)

	// Build filter bitmap index for "category" attribute
	// Note: rowID in filter bitmap must match docID in IVF index
	filterIdx := filter.NewFilterIndex("category", schema.TypeString)
	filterIdx.AddValue(1, "A") // docID 1 -> category A
	filterIdx.AddValue(2, "B") // docID 2 -> category B
	filterIdx.AddValue(3, "A") // docID 3 -> category A
	filterData, err := filterIdx.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize filter index: %v", err)
	}

	filterKey := "vex/namespaces/test-ns/index/segments/seg_001/filters/category.bitmap"
	store.Put(ctx, filterKey, bytes.NewReader(filterData), int64(len(filterData)), nil)

	// Create and store manifest with filter keys
	manifestKey := "vex/namespaces/test-ns/index/manifests/000000000000000001.idx.json"
	manifest := map[string]any{
		"format_version":  1,
		"namespace":       "test-ns",
		"generated_at":    "2024-01-01T00:00:00Z",
		"indexed_wal_seq": uint64(10),
		"segments": []map[string]any{
			{
				"id":            "seg_001",
				"level":         0,
				"start_wal_seq": uint64(1),
				"end_wal_seq":   uint64(10),
				"ivf_keys": map[string]any{
					"centroids_key":       centroidsKey,
					"cluster_offsets_key": offsetsKey,
					"cluster_data_key":    clusterDataKey,
					"n_clusters":          1,
					"vector_count":        3,
				},
				"filter_keys": []string{filterKey},
				"stats": map[string]any{
					"row_count": 3,
				},
			},
		},
	}
	manifestData, _ := json.Marshal(manifest)
	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)

	// Update namespace state
	currentETag := loaded.ETag
	for i := 1; i <= 10; i++ {
		updated, err := stateMan.AdvanceWAL(ctx, "test-ns", currentETag, wal.KeyForSeq(uint64(i)), 100, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL %d: %v", i, err)
		}
		currentETag = updated.ETag
	}

	_, err = stateMan.Update(ctx, "test-ns", currentETag, func(state *namespace.State) error {
		state.Index.ManifestKey = manifestKey
		state.Index.ManifestSeq = 1
		state.Index.IndexedWALSeq = 10
		state.Vector = &namespace.VectorConfig{
			Dims:           4,
			DType:          "f32",
			DistanceMetric: "cosine_distance",
			ANN:            true,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	// Empty tail - all data is in the index
	mockTail := &vectorTestTailStore{docs: []*tail.Document{}}
	h := NewHandler(store, stateMan, mockTail)

	// Query with filter - should use index with filter bitmaps
	req := &QueryRequest{
		RankBy:  []any{"vector", "ANN", []any{1.0, 0.0, 0.0, 0.0}},
		Filters: []any{"category", "Eq", "A"},
		Limit:   10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should return docs 1 and 3 (category=A) from index, not doc 2 (category=B)
	if len(resp.Rows) != 2 {
		t.Errorf("expected 2 rows (filtered index results), got %d", len(resp.Rows))
		for i, row := range resp.Rows {
			t.Logf("row %d: ID=%v, dist=%v", i, row.ID, row.Dist)
		}
	}

	// First result should be doc 1 (closest to query vector)
	if len(resp.Rows) > 0 {
		firstID, ok := resp.Rows[0].ID.(uint64)
		if !ok {
			t.Logf("first result ID type: %T, value: %v", resp.Rows[0].ID, resp.Rows[0].ID)
		} else if firstID != 1 {
			t.Errorf("expected first result to be doc 1, got %v", firstID)
		}
	}

	// Second result should be doc 3
	if len(resp.Rows) > 1 {
		secondID, ok := resp.Rows[1].ID.(uint64)
		if !ok {
			t.Logf("second result ID type: %T, value: %v", resp.Rows[1].ID, resp.Rows[1].ID)
		} else if secondID != 3 {
			t.Errorf("expected second result to be doc 3, got %v", secondID)
		}
	}
}

// TestANNSearchWithFilterMergesTailAndIndex tests that filtered ANN queries
// merge index results with tail results, with proper deduplication.
func TestANNSearchWithFilterMergesTailAndIndex(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()

	// Create namespace
	loaded, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Build IVF index with indexed data
	dims := 4
	builder := vector.NewIVFBuilder(dims, vector.MetricCosineDistance, 1)
	builder.AddVector(1, []float32{0.95, 0.05, 0, 0}) // category=A, indexed version
	builder.AddVector(2, []float32{0.9, 0.1, 0, 0})   // category=A, indexed version

	ivfIndex, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build IVF index: %v", err)
	}

	// Write IVF files to object store
	centroidsKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.centroids.bin"
	offsetsKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.cluster_offsets.bin"
	clusterDataKey := "vex/namespaces/test-ns/index/segments/seg_001/vectors.clusters.pack"

	store.Put(ctx, centroidsKey, bytes.NewReader(ivfIndex.GetCentroidsBytes()), int64(len(ivfIndex.GetCentroidsBytes())), nil)
	store.Put(ctx, offsetsKey, bytes.NewReader(ivfIndex.GetClusterOffsetsBytes()), int64(len(ivfIndex.GetClusterOffsetsBytes())), nil)
	store.Put(ctx, clusterDataKey, bytes.NewReader(ivfIndex.GetClusterDataBytes()), int64(len(ivfIndex.GetClusterDataBytes())), nil)

	// Build filter bitmap index for "category" attribute
	// Note: rowID in filter bitmap must match docID in IVF index
	filterIdx := filter.NewFilterIndex("category", schema.TypeString)
	filterIdx.AddValue(1, "A") // docID 1 -> category A
	filterIdx.AddValue(2, "A") // docID 2 -> category A
	filterData, err := filterIdx.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize filter index: %v", err)
	}

	filterKey := "vex/namespaces/test-ns/index/segments/seg_001/filters/category.bitmap"
	store.Put(ctx, filterKey, bytes.NewReader(filterData), int64(len(filterData)), nil)

	// Create and store manifest with filter keys
	manifestKey := "vex/namespaces/test-ns/index/manifests/000000000000000001.idx.json"
	manifest := map[string]any{
		"format_version":  1,
		"namespace":       "test-ns",
		"generated_at":    "2024-01-01T00:00:00Z",
		"indexed_wal_seq": uint64(10),
		"segments": []map[string]any{
			{
				"id":            "seg_001",
				"level":         0,
				"start_wal_seq": uint64(1),
				"end_wal_seq":   uint64(10),
				"ivf_keys": map[string]any{
					"centroids_key":       centroidsKey,
					"cluster_offsets_key": offsetsKey,
					"cluster_data_key":    clusterDataKey,
					"n_clusters":          1,
					"vector_count":        2,
				},
				"filter_keys": []string{filterKey},
				"stats": map[string]any{
					"row_count": 2,
				},
			},
		},
	}
	manifestData, _ := json.Marshal(manifest)
	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)

	// Update namespace state
	currentETag := loaded.ETag
	for i := 1; i <= 12; i++ {
		updated, err := stateMan.AdvanceWAL(ctx, "test-ns", currentETag, wal.KeyForSeq(uint64(i)), 100, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL %d: %v", i, err)
		}
		currentETag = updated.ETag
	}

	_, err = stateMan.Update(ctx, "test-ns", currentETag, func(state *namespace.State) error {
		state.Index.ManifestKey = manifestKey
		state.Index.ManifestSeq = 1
		state.Index.IndexedWALSeq = 10
		state.Vector = &namespace.VectorConfig{
			Dims:           4,
			DType:          "f32",
			DistanceMetric: "cosine_distance",
			ANN:            true,
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	// Tail contains:
	// - doc 1 updated (should override index version)
	// - doc 3 new (not in index, also category=A)
	mockTail := &vectorTestTailStore{
		docs: []*tail.Document{
			{
				ID:         document.NewU64ID(1),
				Vector:     []float32{1.0, 0.0, 0, 0}, // Updated to exact match
				Attributes: map[string]any{"category": "A"},
				WalSeq:     11,
			},
			{
				ID:         document.NewU64ID(3),
				Vector:     []float32{0.8, 0.2, 0, 0}, // New document
				Attributes: map[string]any{"category": "A"},
				WalSeq:     12,
			},
		},
	}
	h := NewHandler(store, stateMan, mockTail)

	// Query with filter
	req := &QueryRequest{
		RankBy:  []any{"vector", "ANN", []any{1.0, 0.0, 0.0, 0.0}},
		Filters: []any{"category", "Eq", "A"},
		Limit:   10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should return 3 documents:
	// - doc 1 from tail (updated version, distance ~0)
	// - doc 2 from index (category=A)
	// - doc 3 from tail (new, category=A)
	if len(resp.Rows) != 3 {
		t.Errorf("expected 3 rows (merged tail + index), got %d", len(resp.Rows))
		for i, row := range resp.Rows {
			t.Logf("row %d: ID=%v, dist=%v", i, row.ID, row.Dist)
		}
	}

	// First result should be doc 1 from tail (distance ~0)
	if len(resp.Rows) > 0 {
		firstID, ok := resp.Rows[0].ID.(uint64)
		if !ok {
			t.Logf("first result ID type: %T, value: %v", resp.Rows[0].ID, resp.Rows[0].ID)
		} else if firstID != 1 {
			t.Errorf("expected first result to be doc 1 (tail), got %v", firstID)
		}
		if resp.Rows[0].Dist == nil || *resp.Rows[0].Dist > 0.01 {
			t.Errorf("expected first result to have distance ~0, got %v", resp.Rows[0].Dist)
		}
	}
}
