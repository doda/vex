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

// exportPaginationTailStore is a mock tail store for export pagination tests.
type exportPaginationTailStore struct {
	docs []*tail.Document
}

func (m *exportPaginationTailStore) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *exportPaginationTailStore) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	if f == nil {
		return m.docs, nil
	}
	var result []*tail.Document
	for _, doc := range m.docs {
		filterDoc := buildExportFilterDoc(doc)
		if f.Eval(filterDoc) {
			result = append(result, doc)
		}
	}
	return result, nil
}

// buildExportFilterDoc creates a filter.Document including the "id" field.
func buildExportFilterDoc(doc *tail.Document) filter.Document {
	filterDoc := make(filter.Document)
	for k, v := range doc.Attributes {
		filterDoc[k] = v
	}
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

func (m *exportPaginationTailStore) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.Scan(ctx, ns, f)
}

func (m *exportPaginationTailStore) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *exportPaginationTailStore) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return m.VectorScan(ctx, ns, queryVector, topK, metric, f)
}

func (m *exportPaginationTailStore) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *exportPaginationTailStore) TailBytes(ns string) int64 {
	return 0
}

func (m *exportPaginationTailStore) Clear(ns string) {}

func (m *exportPaginationTailStore) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *exportPaginationTailStore) Close() error {
	return nil
}

func setupExportPaginationTest(t *testing.T, docs []*tail.Document) (*Handler, context.Context) {
	t.Helper()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	mockTail := &exportPaginationTailStore{docs: docs}
	h := NewHandler(store, stateMan, mockTail)
	return h, ctx
}

// TestExportPaginationByID tests export via paginated query by id.
// This verifies:
// 1. Query with rank_by ["id", "asc"] and limit works
// 2. Filter ["id", "Gt", last_id] for next page works
// 3. Pagination continues until fewer than limit results
func TestExportPaginationByID(t *testing.T) {
	// Create 10 documents with IDs 1-10
	docs := []*tail.Document{
		{ID: document.NewU64ID(5), Attributes: map[string]any{"name": "doc5"}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"name": "doc2"}},
		{ID: document.NewU64ID(8), Attributes: map[string]any{"name": "doc8"}},
		{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "doc1"}},
		{ID: document.NewU64ID(9), Attributes: map[string]any{"name": "doc9"}},
		{ID: document.NewU64ID(3), Attributes: map[string]any{"name": "doc3"}},
		{ID: document.NewU64ID(10), Attributes: map[string]any{"name": "doc10"}},
		{ID: document.NewU64ID(7), Attributes: map[string]any{"name": "doc7"}},
		{ID: document.NewU64ID(4), Attributes: map[string]any{"name": "doc4"}},
		{ID: document.NewU64ID(6), Attributes: map[string]any{"name": "doc6"}},
	}

	h, ctx := setupExportPaginationTest(t, docs)

	// Test 1: First page - rank_by ["id", "asc"] with limit 3
	t.Run("first page", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"id", "asc"},
			Limit:  3,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(resp.Rows))
		}

		// Verify IDs are 1, 2, 3 in order
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
	})

	// Test 2: Second page - filter ["id", "Gt", 3]
	t.Run("second page with id filter", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "asc"},
			Filters: []any{"id", "Gt", uint64(3)},
			Limit:   3,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(resp.Rows))
		}

		// Verify IDs are 4, 5, 6 in order
		expectedIDs := []uint64{4, 5, 6}
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
	})

	// Test 3: Third page - filter ["id", "Gt", 6]
	t.Run("third page with id filter", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "asc"},
			Filters: []any{"id", "Gt", uint64(6)},
			Limit:   3,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(resp.Rows))
		}

		// Verify IDs are 7, 8, 9 in order
		expectedIDs := []uint64{7, 8, 9}
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
	})

	// Test 4: Last page - filter ["id", "Gt", 9] - fewer than limit results
	t.Run("last page fewer than limit", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "asc"},
			Filters: []any{"id", "Gt", uint64(9)},
			Limit:   3,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 1 {
			t.Fatalf("expected 1 row (last page), got %d", len(resp.Rows))
		}

		got, ok := resp.Rows[0].ID.(uint64)
		if !ok {
			t.Errorf("expected uint64 id, got %T", resp.Rows[0].ID)
		} else if got != 10 {
			t.Errorf("expected id 10, got %d", got)
		}
	})

	// Test 5: Empty page - filter ["id", "Gt", 10]
	t.Run("empty page after last", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "asc"},
			Filters: []any{"id", "Gt", uint64(10)},
			Limit:   3,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 0 {
			t.Fatalf("expected 0 rows (past last page), got %d", len(resp.Rows))
		}
	})
}

// TestExportPaginationFullExport simulates a complete export loop.
func TestExportPaginationFullExport(t *testing.T) {
	// Create 25 documents with IDs 1-25
	docs := make([]*tail.Document, 25)
	for i := 0; i < 25; i++ {
		docs[i] = &tail.Document{
			ID:         document.NewU64ID(uint64(i + 1)),
			Attributes: map[string]any{"index": int64(i + 1)},
		}
	}

	h, ctx := setupExportPaginationTest(t, docs)

	// Export all documents with page size 10
	pageSize := 10
	var allExported []uint64
	var lastID uint64 = 0

	for {
		var req *QueryRequest
		if lastID == 0 {
			// First page
			req = &QueryRequest{
				RankBy: []any{"id", "asc"},
				Limit:  pageSize,
			}
		} else {
			// Subsequent pages
			req = &QueryRequest{
				RankBy:  []any{"id", "asc"},
				Filters: []any{"id", "Gt", lastID},
				Limit:   pageSize,
			}
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		for _, row := range resp.Rows {
			id, ok := row.ID.(uint64)
			if !ok {
				t.Fatalf("expected uint64 id, got %T", row.ID)
			}
			allExported = append(allExported, id)
			lastID = id
		}

		// Exit when fewer than limit results returned
		if len(resp.Rows) < pageSize {
			break
		}
	}

	// Verify all 25 documents were exported
	if len(allExported) != 25 {
		t.Errorf("expected 25 exported documents, got %d", len(allExported))
	}

	// Verify order
	for i, id := range allExported {
		expected := uint64(i + 1)
		if id != expected {
			t.Errorf("export position %d: expected id %d, got %d", i, expected, id)
		}
	}
}

// TestExportPaginationWithStringIDs tests pagination with string IDs.
func TestExportPaginationWithStringIDs(t *testing.T) {
	id1, _ := document.NewStringID("apple")
	id2, _ := document.NewStringID("banana")
	id3, _ := document.NewStringID("cherry")
	id4, _ := document.NewStringID("date")
	id5, _ := document.NewStringID("elderberry")

	docs := []*tail.Document{
		{ID: id3, Attributes: map[string]any{"name": "cherry"}},
		{ID: id1, Attributes: map[string]any{"name": "apple"}},
		{ID: id5, Attributes: map[string]any{"name": "elderberry"}},
		{ID: id2, Attributes: map[string]any{"name": "banana"}},
		{ID: id4, Attributes: map[string]any{"name": "date"}},
	}

	h, ctx := setupExportPaginationTest(t, docs)

	t.Run("first page string ids", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"id", "asc"},
			Limit:  2,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(resp.Rows))
		}

		// Should be "apple", "banana" (lexicographic order)
		expectedIDs := []string{"apple", "banana"}
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
	})

	t.Run("second page string ids with filter", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "asc"},
			Filters: []any{"id", "Gt", "banana"},
			Limit:   2,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(resp.Rows))
		}

		// Should be "cherry", "date" (lexicographic order after "banana")
		expectedIDs := []string{"cherry", "date"}
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
	})

	t.Run("last page string ids", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "asc"},
			Filters: []any{"id", "Gt", "date"},
			Limit:   2,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 1 {
			t.Fatalf("expected 1 row (last page), got %d", len(resp.Rows))
		}

		got, ok := resp.Rows[0].ID.(string)
		if !ok {
			t.Errorf("expected string id, got %T", resp.Rows[0].ID)
		} else if got != "elderberry" {
			t.Errorf("expected id 'elderberry', got %q", got)
		}
	})
}

// TestExportPaginationWithCombinedFilters tests pagination with additional filters.
func TestExportPaginationWithCombinedFilters(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), Attributes: map[string]any{"category": "A"}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"category": "B"}},
		{ID: document.NewU64ID(3), Attributes: map[string]any{"category": "A"}},
		{ID: document.NewU64ID(4), Attributes: map[string]any{"category": "B"}},
		{ID: document.NewU64ID(5), Attributes: map[string]any{"category": "A"}},
		{ID: document.NewU64ID(6), Attributes: map[string]any{"category": "B"}},
	}

	h, ctx := setupExportPaginationTest(t, docs)

	t.Run("export only category A with pagination", func(t *testing.T) {
		// First page of category A
		req := &QueryRequest{
			RankBy: []any{"id", "asc"},
			Filters: []any{"And", []any{
				[]any{"category", "Eq", "A"},
			}},
			Limit: 2,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(resp.Rows))
		}

		// Should be ids 1, 3 (category A)
		expectedIDs := []uint64{1, 3}
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

		// Second page with combined filter
		lastID := resp.Rows[len(resp.Rows)-1].ID.(uint64)
		req2 := &QueryRequest{
			RankBy: []any{"id", "asc"},
			Filters: []any{"And", []any{
				[]any{"category", "Eq", "A"},
				[]any{"id", "Gt", lastID},
			}},
			Limit: 2,
		}

		resp2, err := h.Handle(ctx, "test-ns", req2)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp2.Rows) != 1 {
			t.Fatalf("expected 1 row (last category A), got %d", len(resp2.Rows))
		}

		got, ok := resp2.Rows[0].ID.(uint64)
		if !ok {
			t.Errorf("expected uint64 id, got %T", resp2.Rows[0].ID)
		} else if got != 5 {
			t.Errorf("expected id 5, got %d", got)
		}
	})
}

// TestExportPaginationDescending tests export with descending order.
func TestExportPaginationDescending(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(3), Attributes: map[string]any{"name": "three"}},
		{ID: document.NewU64ID(1), Attributes: map[string]any{"name": "one"}},
		{ID: document.NewU64ID(5), Attributes: map[string]any{"name": "five"}},
		{ID: document.NewU64ID(2), Attributes: map[string]any{"name": "two"}},
		{ID: document.NewU64ID(4), Attributes: map[string]any{"name": "four"}},
	}

	h, ctx := setupExportPaginationTest(t, docs)

	t.Run("first page desc", func(t *testing.T) {
		req := &QueryRequest{
			RankBy: []any{"id", "desc"},
			Limit:  2,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(resp.Rows))
		}

		// Should be 5, 4 in descending order
		expectedIDs := []uint64{5, 4}
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
	})

	t.Run("second page desc with Lt filter", func(t *testing.T) {
		req := &QueryRequest{
			RankBy:  []any{"id", "desc"},
			Filters: []any{"id", "Lt", uint64(4)},
			Limit:   2,
		}

		resp, err := h.Handle(ctx, "test-ns", req)
		if err != nil {
			t.Fatalf("Handle() error = %v", err)
		}

		if len(resp.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(resp.Rows))
		}

		// Should be 3, 2 in descending order (all < 4)
		expectedIDs := []uint64{3, 2}
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
	})
}
