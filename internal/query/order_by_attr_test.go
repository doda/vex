package query

import (
	"context"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// orderByMockTailStore is a mock tail store for order-by-attribute tests.
type orderByMockTailStore struct {
	docs []*tail.Document
}

func (m *orderByMockTailStore) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *orderByMockTailStore) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	if f == nil {
		return m.docs, nil
	}
	// Apply filter
	var result []*tail.Document
	for _, doc := range m.docs {
		filterDoc := make(filter.Document)
		for k, v := range doc.Attributes {
			filterDoc[k] = v
		}
		// Add "id" field for filtering by document ID
		filterDoc["id"] = docIDToFilterValue(doc.ID)
		if f.Eval(filterDoc) {
			result = append(result, doc)
		}
	}
	return result, nil
}

// docIDToFilterValue converts a document.ID to a value suitable for filter evaluation.
func docIDToFilterValue(id document.ID) any {
	switch id.Type() {
	case document.IDTypeU64:
		return id.U64()
	case document.IDTypeUUID:
		return id.UUID().String()
	case document.IDTypeString:
		return id.String()
	default:
		return id.String()
	}
}

func (m *orderByMockTailStore) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.Scan(ctx, ns, f)
}

func (m *orderByMockTailStore) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *orderByMockTailStore) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return m.VectorScan(ctx, ns, queryVector, topK, metric, f)
}

func (m *orderByMockTailStore) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *orderByMockTailStore) TailBytes(ns string) int64 {
	return 0
}

func (m *orderByMockTailStore) Clear(ns string) {}

func (m *orderByMockTailStore) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *orderByMockTailStore) Close() error {
	return nil
}

func setupOrderByTest(t *testing.T, docs []*tail.Document) (*Handler, context.Context) {
	t.Helper()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	mockTail := &orderByMockTailStore{docs: docs}
	h := NewHandler(store, stateMan, mockTail)
	return h, ctx
}

// Test rank_by ["id", "asc"] orders by id ascending
func TestOrderByID_Asc(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(3), Attributes: map[string]any{"name": "three"}},
		{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "one"}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"name": "two"}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"id", "asc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if len(resp.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(resp.Rows))
	}

	// Check order: should be 1, 2, 3
	expectedIDs := []uint64{1, 2, 3}
	for i, expected := range expectedIDs {
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			t.Errorf("row %d: expected uint64 id, got %T", i, resp.Rows[i].ID)
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, got)
		}
	}

	// Verify $dist is omitted
	for i, row := range resp.Rows {
		if row.Dist != nil {
			t.Errorf("row %d: expected $dist to be nil for order-by query, got %v", i, *row.Dist)
		}
	}
}

// Test rank_by ["id", "desc"] orders by id descending
func TestOrderByID_Desc(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(3), Attributes: map[string]any{"name": "three"}},
		{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "one"}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"name": "two"}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"id", "desc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Check order: should be 3, 2, 1
	expectedIDs := []uint64{3, 2, 1}
	for i, expected := range expectedIDs {
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			t.Errorf("row %d: expected uint64 id, got %T", i, resp.Rows[i].ID)
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, got)
		}
	}
}

// Test rank_by ["timestamp", "desc"] orders by timestamp descending
func TestOrderByTimestamp_Desc(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"timestamp": int64(1000)}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"timestamp": int64(3000)}},
		{ID: document.NewU64ID(3), Attributes: map[string]any{"timestamp": int64(2000)}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"timestamp", "desc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Check order: should be id=2 (3000), id=3 (2000), id=1 (1000)
	expectedIDs := []uint64{2, 3, 1}
	for i, expected := range expectedIDs {
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			t.Errorf("row %d: expected uint64 id, got %T", i, resp.Rows[i].ID)
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, got)
		}
	}

	// Verify $dist is omitted
	for i, row := range resp.Rows {
		if row.Dist != nil {
			t.Errorf("row %d: expected $dist to be nil, got %v", i, *row.Dist)
		}
	}
}

// Test rank_by ["timestamp", "asc"] orders by timestamp ascending
func TestOrderByTimestamp_Asc(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"timestamp": int64(1000)}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"timestamp": int64(3000)}},
		{ID: document.NewU64ID(3), Attributes: map[string]any{"timestamp": int64(2000)}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"timestamp", "asc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Check order: should be id=1 (1000), id=3 (2000), id=2 (3000)
	expectedIDs := []uint64{1, 3, 2}
	for i, expected := range expectedIDs {
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			t.Errorf("row %d: expected uint64 id, got %T", i, resp.Rows[i].ID)
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, got)
		}
	}
}

// Test $dist is omitted for order-by queries
func TestOrderByNoDistField(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"score": int64(100)}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"score": int64(200)}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"score", "desc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	for i, row := range resp.Rows {
		if row.Dist != nil {
			t.Errorf("row %d: $dist should be nil for order-by query", i)
		}
	}

	// Also verify serialization doesn't include $dist
	for i, row := range resp.Rows {
		serialized := RowToJSON(row)
		if _, ok := serialized["$dist"]; ok {
			t.Errorf("row %d: serialized JSON should not contain $dist", i)
		}
	}
}

// Test 'per' diversification option
func TestOrderByWithPerDiversification(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"category": "A", "score": int64(100)}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"category": "A", "score": int64(90)}},
		{ID: document.NewU64ID(3), Attributes: map[string]any{"category": "A", "score": int64(80)}},
		{ID: document.NewU64ID(4), Attributes: map[string]any{"category": "B", "score": int64(95)}},
		{ID: document.NewU64ID(5), Attributes: map[string]any{"category": "B", "score": int64(85)}},
		{ID: document.NewU64ID(6), Attributes: map[string]any{"category": "C", "score": int64(92)}},
	}

	h, ctx := setupOrderByTest(t, docs)

	// Request 2 results per category
	req := &QueryRequest{
		RankBy: []any{"score", "desc"},
		Limit:  2,
		Per:    []string{"category"},
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should get 2 from A (highest scores), 2 from B, 1 from C (only has 1)
	// Total: 5 documents
	// With limit=2 per category, sorted by score desc:
	// A: id=1 (100), id=2 (90) [id=3 (80) excluded]
	// B: id=4 (95), id=5 (85)
	// C: id=6 (92)

	if len(resp.Rows) != 5 {
		t.Errorf("expected 5 rows (2 per category), got %d", len(resp.Rows))
	}

	// Check that we have at most 2 per category
	categoryCounts := make(map[string]int)
	for _, row := range resp.Rows {
		cat := row.Attributes["category"].(string)
		categoryCounts[cat]++
	}

	for cat, count := range categoryCounts {
		if count > 2 {
			t.Errorf("category %s: expected at most 2 results, got %d", cat, count)
		}
	}
}

// Test per diversification with multiple attributes
func TestOrderByWithMultiplePerAttributes(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"category": "A", "region": "east", "score": int64(100)}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"category": "A", "region": "east", "score": int64(90)}},
		{ID: document.NewU64ID(3), Attributes: map[string]any{"category": "A", "region": "west", "score": int64(95)}},
		{ID: document.NewU64ID(4), Attributes: map[string]any{"category": "B", "region": "east", "score": int64(88)}},
		{ID: document.NewU64ID(5), Attributes: map[string]any{"category": "B", "region": "west", "score": int64(92)}},
	}

	h, ctx := setupOrderByTest(t, docs)

	// Request 1 result per (category, region) combination
	req := &QueryRequest{
		RankBy: []any{"score", "desc"},
		Limit:  1,
		Per:    []string{"category", "region"},
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should get 4 results: one per (category, region) combination
	// (A, east), (A, west), (B, east), (B, west)
	if len(resp.Rows) != 4 {
		t.Errorf("expected 4 rows (1 per category+region), got %d", len(resp.Rows))
	}

	// Check that we have at most 1 per combination
	keyCounts := make(map[string]int)
	for _, row := range resp.Rows {
		cat := row.Attributes["category"].(string)
		region := row.Attributes["region"].(string)
		key := cat + "|" + region
		keyCounts[key]++
	}

	for key, count := range keyCounts {
		if count > 1 {
			t.Errorf("key %s: expected at most 1 result, got %d", key, count)
		}
	}
}

// Test ordering by string attributes (lexicographic order)
func TestOrderByStringAttribute(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "zebra"}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"name": "apple"}},
		{ID: document.NewU64ID(3), Attributes: map[string]any{"name": "mango"}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"name", "asc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Check order: should be apple, mango, zebra
	expectedOrder := []string{"apple", "mango", "zebra"}
	for i, expected := range expectedOrder {
		got := resp.Rows[i].Attributes["name"].(string)
		if got != expected {
			t.Errorf("row %d: expected name %q, got %q", i, expected, got)
		}
	}
}

// Test ordering with nil/missing attributes
func TestOrderByWithMissingAttributes(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"score": int64(100)}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{}},                // no score
		{ID: document.NewU64ID(3), Attributes: map[string]any{"score": int64(50)}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"score", "desc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// nil values should sort last in desc order
	// Order: id=1 (100), id=3 (50), id=2 (nil)
	if len(resp.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(resp.Rows))
	}

	expectedIDs := []uint64{1, 3, 2}
	for i, expected := range expectedIDs {
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			t.Errorf("row %d: expected uint64 id, got %T", i, resp.Rows[i].ID)
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, got)
		}
	}
}

// Test ordering with float64 attribute
func TestOrderByFloatAttribute(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"price": float64(19.99)}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"price": float64(9.99)}},
		{ID: document.NewU64ID(3), Attributes: map[string]any{"price": float64(29.99)}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"price", "asc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Check order: should be 9.99, 19.99, 29.99
	expectedIDs := []uint64{2, 1, 3}
	for i, expected := range expectedIDs {
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			t.Errorf("row %d: expected uint64 id, got %T", i, resp.Rows[i].ID)
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, got)
		}
	}
}

// Test ordering with limit
func TestOrderByWithLimit(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"score": int64(100)}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"score": int64(200)}},
		{ID: document.NewU64ID(3), Attributes: map[string]any{"score": int64(300)}},
		{ID: document.NewU64ID(4), Attributes: map[string]any{"score": int64(400)}},
		{ID: document.NewU64ID(5), Attributes: map[string]any{"score": int64(500)}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"score", "desc"},
		Limit:  3,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if len(resp.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(resp.Rows))
	}

	// Should get top 3 by score desc: 5, 4, 3
	expectedIDs := []uint64{5, 4, 3}
	for i, expected := range expectedIDs {
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			t.Errorf("row %d: expected uint64 id, got %T", i, resp.Rows[i].ID)
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, got)
		}
	}
}

// Test parsing per field
func TestParseQueryRequest_Per(t *testing.T) {
	body := map[string]any{
		"rank_by": []any{"id", "asc"},
		"limit":   10,
		"per":     []any{"category"},
	}

	req, err := ParseQueryRequest(body)
	if err != nil {
		t.Fatalf("ParseQueryRequest() error = %v", err)
	}

	if len(req.Per) != 1 {
		t.Errorf("expected 1 per attribute, got %d", len(req.Per))
	}

	if req.Per[0] != "category" {
		t.Errorf("expected per[0] = 'category', got %q", req.Per[0])
	}
}

// Test parsing per field with multiple attributes
func TestParseQueryRequest_PerMultiple(t *testing.T) {
	body := map[string]any{
		"rank_by": []any{"id", "asc"},
		"limit":   10,
		"per":     []any{"category", "region"},
	}

	req, err := ParseQueryRequest(body)
	if err != nil {
		t.Fatalf("ParseQueryRequest() error = %v", err)
	}

	if len(req.Per) != 2 {
		t.Errorf("expected 2 per attributes, got %d", len(req.Per))
	}
}

// Test per diversification with per on id
func TestOrderByWithPerOnID(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"type": "A"}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"type": "A"}},
		{ID: document.NewU64ID(3), Attributes: map[string]any{"type": "B"}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"type", "asc"},
		Limit:  1,
		Per:    []string{"id"}, // per id means 1 per document, which is all of them
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Each document has a unique id, so we should get all of them
	if len(resp.Rows) != 3 {
		t.Errorf("expected 3 rows (1 per unique id), got %d", len(resp.Rows))
	}
}

// Test that string IDs are handled correctly in order-by
func TestOrderByStringID(t *testing.T) {
	id1, _ := document.NewStringID("charlie")
	id2, _ := document.NewStringID("alpha")
	id3, _ := document.NewStringID("bravo")

	docs := []*tail.Document{
		{ID: id1, Attributes: map[string]any{"name": "Charlie"}},
		{ID: id2, Attributes: map[string]any{"name": "Alpha"}},
		{ID: id3, Attributes: map[string]any{"name": "Bravo"}},
	}

	h, ctx := setupOrderByTest(t, docs)

	req := &QueryRequest{
		RankBy: []any{"id", "asc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// String IDs should be sorted lexicographically: alpha, bravo, charlie
	expectedIDs := []string{"alpha", "bravo", "charlie"}
	for i, expected := range expectedIDs {
		got, ok := resp.Rows[i].ID.(string)
		if !ok {
			t.Errorf("row %d: expected string id, got %T", i, resp.Rows[i].ID)
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %q, got %q", i, expected, got)
		}
	}
}
