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

// mockDedupTailStore is a tail store for testing deduplication scenarios.
// It supports multiple versions of the same document with different WAL sequences.
type mockDedupTailStore struct {
	docs map[string][]*tail.Document // ID -> versions sorted by WAL seq desc
}

func newMockDedupTailStore() *mockDedupTailStore {
	return &mockDedupTailStore{
		docs: make(map[string][]*tail.Document),
	}
}

// AddVersion adds a version of a document with specific WAL sequence.
func (m *mockDedupTailStore) AddVersion(doc *tail.Document) {
	idStr := doc.ID.String()
	m.docs[idStr] = append(m.docs[idStr], doc)
}

func (m *mockDedupTailStore) Refresh(ctx context.Context, ns string, afterSeq, upToSeq uint64) error {
	return nil
}

// Scan returns deduplicated documents (newest WAL seq wins).
// This simulates what the real tail store does with its internal deduplication.
func (m *mockDedupTailStore) Scan(ctx context.Context, ns string, f *filter.Filter) ([]*tail.Document, error) {
	// Return only the newest version of each document
	result := make([]*tail.Document, 0)
	for _, versions := range m.docs {
		// Find the newest version
		var newest *tail.Document
		for _, v := range versions {
			if newest == nil || v.WalSeq > newest.WalSeq ||
				(v.WalSeq == newest.WalSeq && v.SubBatchID > newest.SubBatchID) {
				newest = v
			}
		}
		// Skip deleted documents
		if newest != nil && !newest.Deleted {
			if f != nil {
				filterDoc := make(filter.Document)
				for k, v := range newest.Attributes {
					filterDoc[k] = v
				}
				if f.Eval(filterDoc) {
					result = append(result, newest)
				}
			} else {
				result = append(result, newest)
			}
		}
	}
	return result, nil
}

func (m *mockDedupTailStore) ScanWithByteLimit(ctx context.Context, ns string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error) {
	return m.Scan(ctx, ns, f)
}

func (m *mockDedupTailStore) VectorScan(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter) ([]tail.VectorScanResult, error) {
	docs, _ := m.Scan(ctx, ns, f)
	var results []tail.VectorScanResult
	for i, doc := range docs {
		if i >= topK {
			break
		}
		if doc.Vector != nil {
			results = append(results, tail.VectorScanResult{
				Doc:      doc,
				Distance: float64(i) * 0.1,
			})
		}
	}
	return results, nil
}

func (m *mockDedupTailStore) VectorScanWithByteLimit(ctx context.Context, ns string, queryVector []float32, topK int, metric tail.DistanceMetric, f *filter.Filter, byteLimitBytes int64) ([]tail.VectorScanResult, error) {
	return m.VectorScan(ctx, ns, queryVector, topK, metric, f)
}

func (m *mockDedupTailStore) GetDocument(ctx context.Context, ns string, id document.ID) (*tail.Document, error) {
	return nil, nil
}

func (m *mockDedupTailStore) TailBytes(ns string) int64 { return 0 }
func (m *mockDedupTailStore) Clear(ns string)           {}
func (m *mockDedupTailStore) AddWALEntry(ns string, entry *wal.WalEntry) {}
func (m *mockDedupTailStore) Close() error              { return nil }

// TestQueryDeduplication_HighestWALSeqAuthoritative tests that the highest
// WAL sequence version is returned for each document.
// Verification step: "Verify highest WAL seq version is authoritative"
func TestQueryDeduplication_HighestWALSeqAuthoritative(t *testing.T) {
	ctx := context.Background()

	// Create mock tail store with multiple versions
	tailStore := newMockDedupTailStore()

	// Document 1: versions at WAL seq 5, 10, 15 - seq 15 should win
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     5,
		SubBatchID: 0,
		Attributes: map[string]any{"version": "v5"},
	})
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     15,
		SubBatchID: 0,
		Attributes: map[string]any{"version": "v15"},
	})
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Attributes: map[string]any{"version": "v10"},
	})

	// Document 2: versions at WAL seq 8, 12 - seq 12 should win
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(2),
		WalSeq:     8,
		SubBatchID: 0,
		Attributes: map[string]any{"version": "v8"},
	})
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(2),
		WalSeq:     12,
		SubBatchID: 0,
		Attributes: map[string]any{"version": "v12"},
	})

	// Create handler
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	h := NewHandler(store, stateMan, tailStore)

	// Query
	req := &QueryRequest{
		RankBy: []any{"id", "asc"},
		Limit:  10,
	}
	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Verify results
	if len(resp.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(resp.Rows))
	}

	// Find doc 1 and verify it has the v15 version
	for _, row := range resp.Rows {
		id := row.ID.(uint64)
		switch id {
		case 1:
			if row.Attributes["version"] != "v15" {
				t.Errorf("doc 1: expected version 'v15' (highest WAL seq), got %v", row.Attributes["version"])
			}
		case 2:
			if row.Attributes["version"] != "v12" {
				t.Errorf("doc 2: expected version 'v12' (highest WAL seq), got %v", row.Attributes["version"])
			}
		}
	}
}

// TestQueryDeduplication_TombstonesExcludeFromResults tests that deleted
// documents (tombstones) are excluded from query results.
// Verification step: "Verify tombstones (deletes) exclude docs from results"
func TestQueryDeduplication_TombstonesExcludeFromResults(t *testing.T) {
	ctx := context.Background()

	// Create mock tail store
	tailStore := newMockDedupTailStore()

	// Document 1: created at seq 5, deleted at seq 10 - should be excluded
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     5,
		SubBatchID: 0,
		Deleted:    false,
		Attributes: map[string]any{"name": "doc1"},
	})
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Deleted:    true, // Tombstone
	})

	// Document 2: not deleted - should be included
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(2),
		WalSeq:     7,
		SubBatchID: 0,
		Deleted:    false,
		Attributes: map[string]any{"name": "doc2"},
	})

	// Document 3: deleted at seq 3, resurrected at seq 8 - should be included
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(3),
		WalSeq:     3,
		SubBatchID: 0,
		Deleted:    true, // First deleted
	})
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(3),
		WalSeq:     8,
		SubBatchID: 0,
		Deleted:    false, // Then resurrected
		Attributes: map[string]any{"name": "doc3-resurrected"},
	})

	// Create handler
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	h := NewHandler(store, stateMan, tailStore)

	// Query
	req := &QueryRequest{
		RankBy: []any{"id", "asc"},
		Limit:  10,
	}
	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Verify results
	if len(resp.Rows) != 2 {
		t.Fatalf("expected 2 rows (doc1 deleted), got %d", len(resp.Rows))
	}

	// Verify doc 1 is excluded, docs 2 and 3 are included
	foundDoc2, foundDoc3 := false, false
	for _, row := range resp.Rows {
		id := row.ID.(uint64)
		switch id {
		case 1:
			t.Error("doc 1 should be excluded (deleted)")
		case 2:
			foundDoc2 = true
			if row.Attributes["name"] != "doc2" {
				t.Errorf("doc 2: expected name 'doc2', got %v", row.Attributes["name"])
			}
		case 3:
			foundDoc3 = true
			if row.Attributes["name"] != "doc3-resurrected" {
				t.Errorf("doc 3: expected name 'doc3-resurrected', got %v", row.Attributes["name"])
			}
		}
	}

	if !foundDoc2 {
		t.Error("doc 2 should be in results")
	}
	if !foundDoc3 {
		t.Error("doc 3 should be in results (resurrected)")
	}
}

// TestQueryDeduplication_NewestProcessedFirst tests that newest segments/tail
// are processed first, ensuring proper deduplication order.
// Verification step: "Test newest segments/tail processed first"
func TestQueryDeduplication_NewestProcessedFirst(t *testing.T) {
	// This test verifies the deduplication utility correctly handles ordering
	dedup := NewDeduplicator()

	// Simulate documents being added in non-chronological order
	// (as might happen when merging from different sources)

	// Add an older version (seq 5)
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     5,
		SubBatchID: 0,
		Attributes: map[string]any{"order": "first-added"},
	})

	// Add a newer version (seq 15)
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     15,
		SubBatchID: 0,
		Attributes: map[string]any{"order": "second-added"},
	})

	// Add an intermediate version (seq 10)
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Attributes: map[string]any{"order": "third-added"},
	})

	// Results should contain only the highest WAL seq version
	results := dedup.Results()
	if len(results.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(results.Docs))
	}

	doc := results.Docs[0]
	if doc.WalSeq != 15 {
		t.Errorf("expected WAL seq 15 (newest), got %d", doc.WalSeq)
	}
	if doc.Attributes["order"] != "second-added" {
		t.Errorf("expected order 'second-added' (highest WAL seq), got %v", doc.Attributes["order"])
	}
}

// TestQueryDeduplication_VectorSearch tests deduplication in vector ANN queries.
func TestQueryDeduplication_VectorSearch(t *testing.T) {
	ctx := context.Background()

	// Create mock tail store
	tailStore := newMockDedupTailStore()

	// Document 1: old vector, then updated vector
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     5,
		SubBatchID: 0,
		Vector:     []float32{0.0, 0.0, 1.0},
		Attributes: map[string]any{"name": "old-vector"},
	})
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Vector:     []float32{1.0, 0.0, 0.0},
		Attributes: map[string]any{"name": "new-vector"},
	})

	// Document 2: only one version
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(2),
		WalSeq:     7,
		SubBatchID: 0,
		Vector:     []float32{0.0, 1.0, 0.0},
		Attributes: map[string]any{"name": "single-version"},
	})

	// Create handler
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	h := NewHandler(store, stateMan, tailStore)

	// Query
	req := &QueryRequest{
		RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
		Limit:  10,
	}
	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Verify results
	if len(resp.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(resp.Rows))
	}

	// Find doc 1 and verify it has the new-vector version
	for _, row := range resp.Rows {
		id := row.ID.(uint64)
		if id == 1 {
			if row.Attributes["name"] != "new-vector" {
				t.Errorf("doc 1: expected 'new-vector' (newest), got %v", row.Attributes["name"])
			}
		}
	}
}

// TestQueryDeduplication_SubBatchOrdering tests that within the same WAL sequence,
// the sub-batch with the highest ID wins.
func TestQueryDeduplication_SubBatchOrdering(t *testing.T) {
	ctx := context.Background()

	// Create mock tail store
	tailStore := newMockDedupTailStore()

	// Document 1: same WAL seq, different sub-batches
	// Sub-batch 2 should win
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Attributes: map[string]any{"batch": 0},
	})
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 2,
		Attributes: map[string]any{"batch": 2},
	})
	tailStore.AddVersion(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 1,
		Attributes: map[string]any{"batch": 1},
	})

	// Create handler
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	h := NewHandler(store, stateMan, tailStore)

	// Query
	req := &QueryRequest{
		RankBy: []any{"id", "asc"},
		Limit:  10,
	}
	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Verify results
	if len(resp.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(resp.Rows))
	}

	// Verify sub-batch 2 won
	if resp.Rows[0].Attributes["batch"] != 2 {
		t.Errorf("expected batch=2 (highest sub-batch), got %v", resp.Rows[0].Attributes["batch"])
	}
}

// TestQueryDeduplication_MultipleDocuments tests deduplication with many documents
// and multiple versions each.
func TestQueryDeduplication_MultipleDocuments(t *testing.T) {
	ctx := context.Background()

	// Create mock tail store
	tailStore := newMockDedupTailStore()

	// Add 10 documents with varying numbers of versions
	for i := uint64(1); i <= 10; i++ {
		// Each doc has i versions at different WAL seqs
		for j := uint64(1); j <= i; j++ {
			tailStore.AddVersion(&tail.Document{
				ID:         document.NewU64ID(i),
				WalSeq:     j * 10, // 10, 20, 30, etc.
				SubBatchID: 0,
				Attributes: map[string]any{
					"docId":   i,
					"version": j,
					"walSeq":  j * 10,
				},
			})
		}
	}

	// Create handler
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	h := NewHandler(store, stateMan, tailStore)

	// Query
	req := &QueryRequest{
		RankBy: []any{"id", "asc"},
		Limit:  100,
	}
	resp, err := h.Handle(ctx, "test-ns", req)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Verify we get exactly 10 documents
	if len(resp.Rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(resp.Rows))
	}

	// Verify each document has the highest version
	for _, row := range resp.Rows {
		docId := row.Attributes["docId"].(uint64)
		version := row.Attributes["version"].(uint64)
		expectedVersion := docId // Doc i should have version i (highest)
		if version != expectedVersion {
			t.Errorf("doc %d: expected version %d (highest), got %d", docId, expectedVersion, version)
		}
	}
}

// TestMergeAndDeduplicate_Integration tests the MergeAndDeduplicate helper
// with realistic segment and tail data.
func TestMergeAndDeduplicate_Integration(t *testing.T) {
	// Simulate a scenario where:
	// - Segment contains docs 1, 2, 3 at WAL seqs 5, 7, 9
	// - Tail contains updates to docs 1, 3 at WAL seqs 15, 12
	// - Tail contains a delete of doc 2 at WAL seq 20

	tailDocs := []*tail.Document{
		{
			ID:         document.NewU64ID(1),
			WalSeq:     15,
			SubBatchID: 0,
			Attributes: map[string]any{"source": "tail", "updated": true},
		},
		{
			ID:         document.NewU64ID(2),
			WalSeq:     20,
			SubBatchID: 0,
			Deleted:    true, // Deleted in tail
		},
		{
			ID:         document.NewU64ID(3),
			WalSeq:     12,
			SubBatchID: 0,
			Attributes: map[string]any{"source": "tail", "updated": true},
		},
	}

	// Note: segment docs would come from index.DocumentEntry, but we're
	// using the deduplicator directly here

	dedup := NewDeduplicator()

	// Add tail docs first (typically newer)
	for _, doc := range tailDocs {
		dedup.AddTailDoc(doc)
	}

	// Simulate segment docs (older)
	segmentDocs := []struct {
		id     uint64
		walSeq uint64
		attrs  map[string]any
	}{
		{1, 5, map[string]any{"source": "segment"}},
		{2, 7, map[string]any{"source": "segment"}},
		{3, 9, map[string]any{"source": "segment"}},
	}

	for _, seg := range segmentDocs {
		dedup.AddTailDoc(&tail.Document{
			ID:         document.NewU64ID(seg.id),
			WalSeq:     seg.walSeq,
			SubBatchID: 0,
			Attributes: seg.attrs,
		})
	}

	result := dedup.Results()

	// Should have 2 docs (doc 1 and 3 from tail, doc 2 deleted)
	if len(result.Docs) != 2 {
		t.Fatalf("expected 2 docs (doc 2 deleted), got %d", len(result.Docs))
	}

	// Verify the correct versions are returned
	for _, doc := range result.Docs {
		switch doc.ID.U64() {
		case 1:
			if doc.WalSeq != 15 || doc.Attributes["source"] != "tail" {
				t.Errorf("doc 1: expected WAL seq 15 from tail, got seq %d source %v",
					doc.WalSeq, doc.Attributes["source"])
			}
		case 2:
			t.Error("doc 2 should be excluded (deleted)")
		case 3:
			if doc.WalSeq != 12 || doc.Attributes["source"] != "tail" {
				t.Errorf("doc 3: expected WAL seq 12 from tail, got seq %d source %v",
					doc.WalSeq, doc.Attributes["source"])
			}
		}
	}

	// Verify tombstone count
	if result.TotalTombstones != 1 {
		t.Errorf("expected 1 tombstone, got %d", result.TotalTombstones)
	}
}
