package query

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// indexOrderByMockTailStore is a mock tail store for order-by-index tests.
type indexOrderByMockTailStore struct {
	docs []*tail.Document
}

func (m *indexOrderByMockTailStore) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

func (m *indexOrderByMockTailStore) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	if f == nil {
		return m.docs, nil
	}
	var result []*tail.Document
	for _, doc := range m.docs {
		filterDoc := make(filter.Document)
		for k, v := range doc.Attributes {
			filterDoc[k] = v
		}
		filterDoc["id"] = docIDToFilterValue(doc.ID)
		if f.Eval(filterDoc) {
			result = append(result, doc)
		}
	}
	return result, nil
}

func (m *indexOrderByMockTailStore) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.Scan(ctx, ns, f)
}

func (m *indexOrderByMockTailStore) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *indexOrderByMockTailStore) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return nil, nil
}

func (m *indexOrderByMockTailStore) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *indexOrderByMockTailStore) TailBytes(ns string) int64 {
	return 0
}

func (m *indexOrderByMockTailStore) Clear(ns string) {}

func (m *indexOrderByMockTailStore) AddWALEntry(ns string, entry *wal.WalEntry) {}

func (m *indexOrderByMockTailStore) Close() error {
	return nil
}

// writeIndexDocs writes documents to the object store in the segment format.
func writeIndexDocs(store objectstore.Store, manifestKey, docsKey string, docs []index.IndexedDocument) error {
	// Write docs data
	docsData, err := json.Marshal(docs)
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Upload docs data
	_, err = store.Put(ctx, docsKey, bytes.NewReader(docsData), int64(len(docsData)), nil)
	if err != nil {
		return err
	}

	// Create manifest with segment
	manifest := &index.Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		IndexedWALSeq: 10,
		Segments: []index.Segment{
			{
				ID:          "seg_1",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
				DocsKey:     docsKey,
				Stats: index.SegmentStats{
					RowCount: int64(len(docs)),
				},
			},
		},
	}

	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return err
	}

	_, err = store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	return err
}

// TestOrderByWithIndexedDocuments verifies that order-by queries include
// documents from indexed segments, not just tail data.
func TestOrderByWithIndexedDocuments(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	// Create namespace with index state
	created, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Write indexed documents to object store
	manifestKey := "vex/namespaces/test-ns/index/manifests/00000000000000000010.idx.json"
	docsKey := "vex/namespaces/test-ns/index/segments/seg_1/docs.col.zst"

	indexedDocs := []index.IndexedDocument{
		{ID: "1", NumericID: 1, WALSeq: 5, Attributes: map[string]any{"score": float64(100), "name": "indexed-doc-1"}},
		{ID: "2", NumericID: 2, WALSeq: 6, Attributes: map[string]any{"score": float64(300), "name": "indexed-doc-2"}},
		{ID: "3", NumericID: 3, WALSeq: 7, Attributes: map[string]any{"score": float64(200), "name": "indexed-doc-3"}},
	}

	if err := writeIndexDocs(store, manifestKey, docsKey, indexedDocs); err != nil {
		t.Fatalf("failed to write index docs: %v", err)
	}

	// Advance WAL to seq 10 (must advance one at a time)
	currentETag := created.ETag
	for i := 1; i <= 10; i++ {
		updated, err := stateMan.AdvanceWAL(ctx, "test-ns", currentETag, wal.KeyForSeq(uint64(i)), 100, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL %d: %v", i, err)
		}
		currentETag = updated.ETag
	}

	// Update namespace state to point to manifest
	_, err = stateMan.Update(ctx, "test-ns", currentETag, func(state *namespace.State) error {
		state.Index.ManifestKey = manifestKey
		state.Index.IndexedWALSeq = 10
		state.Index.ManifestSeq = 10
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update namespace state: %v", err)
	}

	// Create tail store with no documents (simulating all indexed)
	mockTail := &indexOrderByMockTailStore{docs: nil}

	// Create handler with index reader
	h := NewHandler(store, stateMan, mockTail)

	// Execute order-by query
	req := &QueryRequest{
		RankBy: []any{"score", "desc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should get all 3 indexed documents, sorted by score desc
	if len(resp.Rows) != 3 {
		t.Errorf("expected 3 rows from indexed segments, got %d", len(resp.Rows))
	}

	// Check order: should be id=2 (300), id=3 (200), id=1 (100)
	expectedIDs := []uint64{2, 3, 1}
	for i, expected := range expectedIDs {
		if i >= len(resp.Rows) {
			break
		}
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			t.Errorf("row %d: expected uint64 id, got %T: %v", i, resp.Rows[i].ID, resp.Rows[i].ID)
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

// TestOrderByMergesIndexAndTail verifies that order-by queries merge
// documents from both indexed segments and tail data.
func TestOrderByMergesIndexAndTail(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	// Create namespace
	created, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Write indexed documents (older, WAL seq 5-7)
	manifestKey := "vex/namespaces/test-ns/index/manifests/00000000000000000010.idx.json"
	docsKey := "vex/namespaces/test-ns/index/segments/seg_1/docs.col.zst"

	indexedDocs := []index.IndexedDocument{
		{ID: "1", NumericID: 1, WALSeq: 5, Attributes: map[string]any{"score": float64(100), "source": "index"}},
		{ID: "2", NumericID: 2, WALSeq: 6, Attributes: map[string]any{"score": float64(300), "source": "index"}},
	}

	if err := writeIndexDocs(store, manifestKey, docsKey, indexedDocs); err != nil {
		t.Fatalf("failed to write index docs: %v", err)
	}

	// Advance WAL to seq 10
	currentETag := created.ETag
	for i := 1; i <= 10; i++ {
		updated, err := stateMan.AdvanceWAL(ctx, "test-ns", currentETag, wal.KeyForSeq(uint64(i)), 100, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL %d: %v", i, err)
		}
		currentETag = updated.ETag
	}

	// Update namespace state
	_, err = stateMan.Update(ctx, "test-ns", currentETag, func(state *namespace.State) error {
		state.Index.ManifestKey = manifestKey
		state.Index.IndexedWALSeq = 10
		state.Index.ManifestSeq = 10
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update namespace state: %v", err)
	}

	// Create tail store with newer documents (WAL seq 15-17)
	tailDocs := []*tail.Document{
		{ID: document.NewU64ID(3), WalSeq: 15, Attributes: map[string]any{"score": float64(250), "source": "tail"}},
		{ID: document.NewU64ID(4), WalSeq: 16, Attributes: map[string]any{"score": float64(150), "source": "tail"}},
	}
	mockTail := &indexOrderByMockTailStore{docs: tailDocs}

	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy: []any{"score", "desc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should get all 4 documents (2 from index + 2 from tail)
	if len(resp.Rows) != 4 {
		t.Errorf("expected 4 rows (2 index + 2 tail), got %d", len(resp.Rows))
	}

	// Check order by score desc: id=2 (300), id=3 (250), id=4 (150), id=1 (100)
	expectedIDs := []uint64{2, 3, 4, 1}
	for i, expected := range expectedIDs {
		if i >= len(resp.Rows) {
			break
		}
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

// TestOrderByTailTakesPrecedence verifies that tail data takes precedence
// over indexed data for the same document ID (deduplication).
func TestOrderByTailTakesPrecedence(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	created, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Write indexed documents - doc ID 1 has score 100
	manifestKey := "vex/namespaces/test-ns/index/manifests/00000000000000000010.idx.json"
	docsKey := "vex/namespaces/test-ns/index/segments/seg_1/docs.col.zst"

	indexedDocs := []index.IndexedDocument{
		{ID: "1", NumericID: 1, WALSeq: 5, Attributes: map[string]any{"score": float64(100), "source": "index"}},
		{ID: "2", NumericID: 2, WALSeq: 6, Attributes: map[string]any{"score": float64(50), "source": "index"}},
	}

	if err := writeIndexDocs(store, manifestKey, docsKey, indexedDocs); err != nil {
		t.Fatalf("failed to write index docs: %v", err)
	}

	// Advance WAL to seq 10
	currentETag := created.ETag
	for i := 1; i <= 10; i++ {
		updated, err := stateMan.AdvanceWAL(ctx, "test-ns", currentETag, wal.KeyForSeq(uint64(i)), 100, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL %d: %v", i, err)
		}
		currentETag = updated.ETag
	}

	_, err = stateMan.Update(ctx, "test-ns", currentETag, func(state *namespace.State) error {
		state.Index.ManifestKey = manifestKey
		state.Index.IndexedWALSeq = 10
		state.Index.ManifestSeq = 10
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update namespace state: %v", err)
	}

	// Tail has updated version of doc ID 1 with different score
	tailDocs := []*tail.Document{
		{ID: document.NewU64ID(1), WalSeq: 15, Attributes: map[string]any{"score": float64(999), "source": "tail"}},
	}
	mockTail := &indexOrderByMockTailStore{docs: tailDocs}

	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy: []any{"score", "desc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should get 2 documents (doc 1 from tail, doc 2 from index)
	if len(resp.Rows) != 2 {
		t.Errorf("expected 2 rows (deduplicated), got %d", len(resp.Rows))
	}

	// Check that doc 1's source is "tail" (not "index")
	for _, row := range resp.Rows {
		id, ok := row.ID.(uint64)
		if !ok {
			continue
		}
		if id == 1 {
			source, _ := row.Attributes["source"].(string)
			if source != "tail" {
				t.Errorf("doc 1: expected source 'tail', got %q", source)
			}
			score, _ := row.Attributes["score"].(float64)
			if score != 999 {
				t.Errorf("doc 1: expected score 999, got %v", score)
			}
		}
	}

	// Order should be: id=1 (999), id=2 (50)
	expectedIDs := []uint64{1, 2}
	for i, expected := range expectedIDs {
		if i >= len(resp.Rows) {
			break
		}
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, got)
		}
	}
}

// TestOrderByDeletedDocExcluded verifies that documents deleted in tail
// are excluded even if they exist in the index.
func TestOrderByDeletedDocExcluded(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	created, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Write indexed documents
	manifestKey := "vex/namespaces/test-ns/index/manifests/00000000000000000010.idx.json"
	docsKey := "vex/namespaces/test-ns/index/segments/seg_1/docs.col.zst"

	indexedDocs := []index.IndexedDocument{
		{ID: "1", NumericID: 1, WALSeq: 5, Attributes: map[string]any{"score": float64(100)}},
		{ID: "2", NumericID: 2, WALSeq: 6, Attributes: map[string]any{"score": float64(200)}},
		{ID: "3", NumericID: 3, WALSeq: 7, Attributes: map[string]any{"score": float64(300)}},
	}

	if err := writeIndexDocs(store, manifestKey, docsKey, indexedDocs); err != nil {
		t.Fatalf("failed to write index docs: %v", err)
	}

	// Advance WAL to seq 10
	currentETag := created.ETag
	for i := 1; i <= 10; i++ {
		updated, err := stateMan.AdvanceWAL(ctx, "test-ns", currentETag, wal.KeyForSeq(uint64(i)), 100, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL %d: %v", i, err)
		}
		currentETag = updated.ETag
	}

	_, err = stateMan.Update(ctx, "test-ns", currentETag, func(state *namespace.State) error {
		state.Index.ManifestKey = manifestKey
		state.Index.IndexedWALSeq = 10
		state.Index.ManifestSeq = 10
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update namespace state: %v", err)
	}

	// Tail contains delete (tombstone) for doc ID 2
	tailDocs := []*tail.Document{
		{ID: document.NewU64ID(2), WalSeq: 15, Deleted: true},
	}
	mockTail := &indexOrderByMockTailStore{docs: tailDocs}

	h := NewHandler(store, stateMan, mockTail)

	req := &QueryRequest{
		RankBy: []any{"score", "desc"},
		Limit:  10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should get 2 documents (doc 2 was deleted)
	if len(resp.Rows) != 2 {
		t.Errorf("expected 2 rows (1 deleted), got %d", len(resp.Rows))
	}

	// Verify doc 2 is not in results
	for _, row := range resp.Rows {
		id, ok := row.ID.(uint64)
		if ok && id == 2 {
			t.Error("doc 2 should be excluded (deleted)")
		}
	}

	// Order should be: id=3 (300), id=1 (100)
	expectedIDs := []uint64{3, 1}
	for i, expected := range expectedIDs {
		if i >= len(resp.Rows) {
			break
		}
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, got)
		}
	}
}

// TestOrderByWithFilterOnIndexedDocs verifies that filters work correctly
// on documents from indexed segments.
func TestOrderByWithFilterOnIndexedDocs(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	created, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Write indexed documents
	manifestKey := "vex/namespaces/test-ns/index/manifests/00000000000000000010.idx.json"
	docsKey := "vex/namespaces/test-ns/index/segments/seg_1/docs.col.zst"

	indexedDocs := []index.IndexedDocument{
		{ID: "1", NumericID: 1, WALSeq: 5, Attributes: map[string]any{"category": "A", "score": float64(100)}},
		{ID: "2", NumericID: 2, WALSeq: 6, Attributes: map[string]any{"category": "B", "score": float64(200)}},
		{ID: "3", NumericID: 3, WALSeq: 7, Attributes: map[string]any{"category": "A", "score": float64(300)}},
	}

	if err := writeIndexDocs(store, manifestKey, docsKey, indexedDocs); err != nil {
		t.Fatalf("failed to write index docs: %v", err)
	}

	// Advance WAL to seq 10
	currentETag := created.ETag
	for i := 1; i <= 10; i++ {
		updated, err := stateMan.AdvanceWAL(ctx, "test-ns", currentETag, wal.KeyForSeq(uint64(i)), 100, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL %d: %v", i, err)
		}
		currentETag = updated.ETag
	}

	_, err = stateMan.Update(ctx, "test-ns", currentETag, func(state *namespace.State) error {
		state.Index.ManifestKey = manifestKey
		state.Index.IndexedWALSeq = 10
		state.Index.ManifestSeq = 10
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update namespace state: %v", err)
	}

	mockTail := &indexOrderByMockTailStore{docs: nil}
	h := NewHandler(store, stateMan, mockTail)

	// Filter for category = "A"
	req := &QueryRequest{
		RankBy:  []any{"score", "desc"},
		Filters: []any{"category", "Eq", "A"},
		Limit:   10,
	}

	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Should get 2 documents (only category A)
	if len(resp.Rows) != 2 {
		t.Errorf("expected 2 rows (category A only), got %d", len(resp.Rows))
	}

	// Order should be: id=3 (300), id=1 (100)
	expectedIDs := []uint64{3, 1}
	for i, expected := range expectedIDs {
		if i >= len(resp.Rows) {
			break
		}
		got, ok := resp.Rows[i].ID.(uint64)
		if !ok {
			continue
		}
		if got != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, got)
		}
	}
}
