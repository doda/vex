package query

import (
	"testing"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/tail"
)

func TestDocVersion_IsNewerThan(t *testing.T) {
	tests := []struct {
		name     string
		v1       DocVersion
		v2       DocVersion
		expected bool
	}{
		{
			name:     "higher WAL seq is newer",
			v1:       DocVersion{WalSeq: 10, SubBatchID: 0},
			v2:       DocVersion{WalSeq: 5, SubBatchID: 0},
			expected: true,
		},
		{
			name:     "lower WAL seq is not newer",
			v1:       DocVersion{WalSeq: 5, SubBatchID: 0},
			v2:       DocVersion{WalSeq: 10, SubBatchID: 0},
			expected: false,
		},
		{
			name:     "same WAL seq, higher sub-batch is newer",
			v1:       DocVersion{WalSeq: 10, SubBatchID: 2},
			v2:       DocVersion{WalSeq: 10, SubBatchID: 1},
			expected: true,
		},
		{
			name:     "same WAL seq, lower sub-batch is not newer",
			v1:       DocVersion{WalSeq: 10, SubBatchID: 1},
			v2:       DocVersion{WalSeq: 10, SubBatchID: 2},
			expected: false,
		},
		{
			name:     "same WAL seq and sub-batch are equal (not newer)",
			v1:       DocVersion{WalSeq: 10, SubBatchID: 1},
			v2:       DocVersion{WalSeq: 10, SubBatchID: 1},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.v1.IsNewerThan(tt.v2)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestDeduplicator_HighestWALSeqWins(t *testing.T) {
	// Test: highest WAL seq version is authoritative
	dedup := NewDeduplicator()

	// Add older version first
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     5,
		SubBatchID: 0,
		Attributes: map[string]any{"version": "old"},
	})

	// Add newer version
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Attributes: map[string]any{"version": "new"},
	})

	result := dedup.Results()
	if len(result.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(result.Docs))
	}

	doc := result.Docs[0]
	if doc.WalSeq != 10 {
		t.Errorf("expected WAL seq 10, got %d", doc.WalSeq)
	}
	if doc.Attributes["version"] != "new" {
		t.Errorf("expected version 'new', got %v", doc.Attributes["version"])
	}
}

func TestDeduplicator_NewerVersionOverridesOlder(t *testing.T) {
	// Test: adding versions in reverse order still results in newest winning
	dedup := NewDeduplicator()

	// Add newer version first
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Attributes: map[string]any{"version": "new"},
	})

	// Try to add older version (should be ignored)
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     5,
		SubBatchID: 0,
		Attributes: map[string]any{"version": "old"},
	})

	result := dedup.Results()
	if len(result.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(result.Docs))
	}

	doc := result.Docs[0]
	if doc.WalSeq != 10 {
		t.Errorf("expected WAL seq 10 (newer version), got %d", doc.WalSeq)
	}
	if doc.Attributes["version"] != "new" {
		t.Errorf("expected version 'new', got %v", doc.Attributes["version"])
	}
}

func TestDeduplicator_TombstoneExcludesDoc(t *testing.T) {
	// Test: tombstones (deletes) exclude docs from results
	dedup := NewDeduplicator()

	// Add original document
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     5,
		SubBatchID: 0,
		Deleted:    false,
		Attributes: map[string]any{"data": "value"},
	})

	// Add tombstone (delete) with higher WAL seq
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Deleted:    true,
	})

	result := dedup.Results()
	if len(result.Docs) != 0 {
		t.Errorf("expected 0 docs (tombstone should exclude), got %d", len(result.Docs))
	}
	if result.TotalTombstones != 1 {
		t.Errorf("expected 1 tombstone, got %d", result.TotalTombstones)
	}
}

func TestDeduplicator_ResurrectionAfterDelete(t *testing.T) {
	// Test: a newer upsert after a delete should be included
	dedup := NewDeduplicator()

	// Add tombstone
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     5,
		SubBatchID: 0,
		Deleted:    true,
	})

	// Add resurrection (upsert after delete)
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Deleted:    false,
		Attributes: map[string]any{"resurrected": true},
	})

	result := dedup.Results()
	if len(result.Docs) != 1 {
		t.Fatalf("expected 1 doc (resurrected), got %d", len(result.Docs))
	}

	doc := result.Docs[0]
	if doc.Deleted {
		t.Error("expected doc to not be deleted after resurrection")
	}
	if doc.Attributes["resurrected"] != true {
		t.Errorf("expected resurrected attribute, got %v", doc.Attributes)
	}
}

func TestDeduplicator_TailTakesPrecedenceOverSegment(t *testing.T) {
	// Test: tail documents with higher WAL seq override segment documents
	dedup := NewDeduplicator()

	// Add segment doc (older)
	dedup.AddSegmentDoc(&index.DocumentEntry{
		ID:         "1",
		WALSeq:     5,
		Deleted:    false,
		Attributes: map[string]any{"source": "segment"},
	})

	// Add tail doc (newer)
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Deleted:    false,
		Attributes: map[string]any{"source": "tail"},
	})

	result := dedup.Results()
	if len(result.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(result.Docs))
	}

	doc := result.Docs[0]
	if doc.Attributes["source"] != "tail" {
		t.Errorf("expected source 'tail' (newer), got %v", doc.Attributes["source"])
	}
}

func TestDeduplicator_SegmentTakesPrecedenceOverOlderTail(t *testing.T) {
	// Test: segment doc with higher WAL seq takes precedence
	dedup := NewDeduplicator()

	// Add tail doc (older - unusual but possible after compaction)
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     5,
		SubBatchID: 0,
		Deleted:    false,
		Attributes: map[string]any{"source": "tail"},
	})

	// Add segment doc (newer)
	dedup.AddSegmentDoc(&index.DocumentEntry{
		ID:         "1",
		WALSeq:     10,
		Deleted:    false,
		Attributes: map[string]any{"source": "segment"},
	})

	result := dedup.Results()
	if len(result.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(result.Docs))
	}

	doc := result.Docs[0]
	if doc.Attributes["source"] != "segment" {
		t.Errorf("expected source 'segment' (newer), got %v", doc.Attributes["source"])
	}
}

func TestDeduplicator_MultipleDocsDeduplication(t *testing.T) {
	// Test: multiple documents with multiple versions each
	dedup := NewDeduplicator()

	// Doc 1: version 5 and 10, 10 should win
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     5,
		Attributes: map[string]any{"id": 1, "version": 5},
	})
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		Attributes: map[string]any{"id": 1, "version": 10},
	})

	// Doc 2: version 7 only
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(2),
		WalSeq:     7,
		Attributes: map[string]any{"id": 2, "version": 7},
	})

	// Doc 3: version 3, then deleted at 8
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(3),
		WalSeq:     3,
		Attributes: map[string]any{"id": 3, "version": 3},
	})
	dedup.AddTailDoc(&tail.Document{
		ID:      document.NewU64ID(3),
		WalSeq:  8,
		Deleted: true,
	})

	result := dedup.Results()
	if len(result.Docs) != 2 {
		t.Fatalf("expected 2 docs (doc 3 deleted), got %d", len(result.Docs))
	}

	// Verify docs are sorted by WAL seq descending
	if result.Docs[0].WalSeq != 10 || result.Docs[1].WalSeq != 7 {
		t.Errorf("expected docs sorted by WAL seq descending: got seqs %d, %d",
			result.Docs[0].WalSeq, result.Docs[1].WalSeq)
	}
}

func TestDeduplicateTailDocs(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), WalSeq: 5, Attributes: map[string]any{"v": 5}},
		{ID: document.NewU64ID(1), WalSeq: 10, Attributes: map[string]any{"v": 10}},
		{ID: document.NewU64ID(2), WalSeq: 7, Attributes: map[string]any{"v": 7}},
		{ID: document.NewU64ID(2), WalSeq: 3, Attributes: map[string]any{"v": 3}},
	}

	result := DeduplicateTailDocs(docs)
	if len(result) != 2 {
		t.Fatalf("expected 2 deduplicated docs, got %d", len(result))
	}

	// Verify newest versions are kept
	for _, doc := range result {
		switch doc.ID.U64() {
		case 1:
			if doc.WalSeq != 10 {
				t.Errorf("doc 1 should have WAL seq 10, got %d", doc.WalSeq)
			}
		case 2:
			if doc.WalSeq != 7 {
				t.Errorf("doc 2 should have WAL seq 7, got %d", doc.WalSeq)
			}
		}
	}
}

func TestDeduplicateTailDocs_WithDeletes(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1), WalSeq: 5, Attributes: map[string]any{"v": 5}},
		{ID: document.NewU64ID(1), WalSeq: 10, Deleted: true}, // Delete at seq 10
		{ID: document.NewU64ID(2), WalSeq: 7, Attributes: map[string]any{"v": 7}},
	}

	result := DeduplicateTailDocs(docs)
	if len(result) != 1 {
		t.Fatalf("expected 1 doc (doc 1 deleted), got %d", len(result))
	}

	if result[0].ID.U64() != 2 {
		t.Errorf("expected doc 2 to survive, got doc %d", result[0].ID.U64())
	}
}

func TestDeduplicateSegmentDocs(t *testing.T) {
	docs := []index.DocumentEntry{
		{ID: "1", WALSeq: 5, Attributes: map[string]any{"v": 5}},
		{ID: "1", WALSeq: 10, Attributes: map[string]any{"v": 10}},
		{ID: "2", WALSeq: 7, Attributes: map[string]any{"v": 7}},
		{ID: "2", WALSeq: 3, Attributes: map[string]any{"v": 3}},
	}

	result := DeduplicateSegmentDocs(docs)
	if len(result) != 2 {
		t.Fatalf("expected 2 deduplicated docs, got %d", len(result))
	}

	// Verify newest versions are kept
	for _, doc := range result {
		switch doc.ID {
		case "1":
			if doc.WALSeq != 10 {
				t.Errorf("doc 1 should have WAL seq 10, got %d", doc.WALSeq)
			}
		case "2":
			if doc.WALSeq != 7 {
				t.Errorf("doc 2 should have WAL seq 7, got %d", doc.WALSeq)
			}
		}
	}
}

func TestDeduplicateSegmentDocs_WithTombstones(t *testing.T) {
	docs := []index.DocumentEntry{
		{ID: "1", WALSeq: 5, Attributes: map[string]any{"v": 5}},
		{ID: "1", WALSeq: 10, Deleted: true}, // Tombstone
		{ID: "2", WALSeq: 7, Attributes: map[string]any{"v": 7}},
	}

	result := DeduplicateSegmentDocs(docs)
	if len(result) != 1 {
		t.Fatalf("expected 1 doc (doc 1 is tombstoned), got %d", len(result))
	}

	if result[0].ID != "2" {
		t.Errorf("expected doc 2 to survive, got doc %s", result[0].ID)
	}
}

func TestMergeAndDeduplicate(t *testing.T) {
	tailDocs := []*tail.Document{
		{ID: document.NewU64ID(1), WalSeq: 15, Attributes: map[string]any{"source": "tail", "v": 15}},
		{ID: document.NewU64ID(3), WalSeq: 12, Attributes: map[string]any{"source": "tail", "v": 12}},
	}

	segmentDocs := []index.DocumentEntry{
		{ID: "1", WALSeq: 10, Attributes: map[string]any{"source": "segment", "v": 10}}, // Older version
		{ID: "2", WALSeq: 8, Attributes: map[string]any{"source": "segment", "v": 8}},
		{ID: "3", WALSeq: 5, Attributes: map[string]any{"source": "segment", "v": 5}}, // Older version
	}

	result := MergeAndDeduplicate(tailDocs, segmentDocs)
	if len(result) != 3 {
		t.Fatalf("expected 3 docs, got %d", len(result))
	}

	// Check that tail versions won for docs 1 and 3
	for _, doc := range result {
		switch doc.ID.String() {
		case "1":
			if doc.WalSeq != 15 {
				t.Errorf("doc 1 should have WAL seq 15 (tail), got %d", doc.WalSeq)
			}
		case "2":
			if doc.WalSeq != 8 {
				t.Errorf("doc 2 should have WAL seq 8 (segment), got %d", doc.WalSeq)
			}
		case "3":
			if doc.WalSeq != 12 {
				t.Errorf("doc 3 should have WAL seq 12 (tail), got %d", doc.WalSeq)
			}
		}
	}
}

func TestMergeAndDeduplicate_TombstoneInTail(t *testing.T) {
	tailDocs := []*tail.Document{
		{ID: document.NewU64ID(1), WalSeq: 15, Deleted: true}, // Delete in tail
	}

	segmentDocs := []index.DocumentEntry{
		{ID: "1", WALSeq: 10, Attributes: map[string]any{"source": "segment"}},
		{ID: "2", WALSeq: 8, Attributes: map[string]any{"source": "segment"}},
	}

	result := MergeAndDeduplicate(tailDocs, segmentDocs)
	if len(result) != 1 {
		t.Fatalf("expected 1 doc (doc 1 deleted in tail), got %d", len(result))
	}

	if result[0].ID.String() != "2" {
		t.Errorf("expected doc 2 to survive, got %s", result[0].ID.String())
	}
}

func TestDeduplicator_SubBatchOrdering(t *testing.T) {
	// Test: within same WAL seq, sub-batch ID determines order
	dedup := NewDeduplicator()

	// Same WAL seq, different sub-batch IDs
	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 0,
		Attributes: map[string]any{"batch": 0},
	})

	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 2,
		Attributes: map[string]any{"batch": 2},
	})

	dedup.AddTailDoc(&tail.Document{
		ID:         document.NewU64ID(1),
		WalSeq:     10,
		SubBatchID: 1,
		Attributes: map[string]any{"batch": 1},
	})

	result := dedup.Results()
	if len(result.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(result.Docs))
	}

	doc := result.Docs[0]
	if doc.SubBatchID != 2 {
		t.Errorf("expected sub-batch 2 (highest), got %d", doc.SubBatchID)
	}
	if doc.Attributes["batch"] != 2 {
		t.Errorf("expected batch=2, got %v", doc.Attributes["batch"])
	}
}

func TestDeduplicator_ResultsSortedNewestFirst(t *testing.T) {
	dedup := NewDeduplicator()

	// Add docs in random order
	dedup.AddTailDoc(&tail.Document{ID: document.NewU64ID(1), WalSeq: 5})
	dedup.AddTailDoc(&tail.Document{ID: document.NewU64ID(2), WalSeq: 15})
	dedup.AddTailDoc(&tail.Document{ID: document.NewU64ID(3), WalSeq: 10})
	dedup.AddTailDoc(&tail.Document{ID: document.NewU64ID(4), WalSeq: 1})

	result := dedup.Results()
	if len(result.Docs) != 4 {
		t.Fatalf("expected 4 docs, got %d", len(result.Docs))
	}

	// Verify sorted by WAL seq descending
	expectedSeqs := []uint64{15, 10, 5, 1}
	for i, doc := range result.Docs {
		if doc.WalSeq != expectedSeqs[i] {
			t.Errorf("doc %d: expected WAL seq %d, got %d", i, expectedSeqs[i], doc.WalSeq)
		}
	}
}

func TestDeduplicator_HasDoc(t *testing.T) {
	dedup := NewDeduplicator()

	dedup.AddTailDoc(&tail.Document{
		ID:     document.NewU64ID(1),
		WalSeq: 10,
	})

	if !dedup.HasDoc("1") {
		t.Error("expected HasDoc to return true for doc 1")
	}

	if dedup.HasDoc("2") {
		t.Error("expected HasDoc to return false for doc 2")
	}
}

func TestDeduplicator_AllDocs_IncludesTombstones(t *testing.T) {
	dedup := NewDeduplicator()

	dedup.AddTailDoc(&tail.Document{
		ID:     document.NewU64ID(1),
		WalSeq: 10,
	})
	dedup.AddTailDoc(&tail.Document{
		ID:      document.NewU64ID(2),
		WalSeq:  5,
		Deleted: true,
	})

	// Results() excludes tombstones
	result := dedup.Results()
	if len(result.Docs) != 1 {
		t.Errorf("Results() should exclude tombstones, got %d docs", len(result.Docs))
	}

	// AllDocs() includes tombstones
	allDocs := dedup.AllDocs()
	if len(allDocs) != 2 {
		t.Errorf("AllDocs() should include tombstones, got %d docs", len(allDocs))
	}
}

func TestBuildDocIDSet(t *testing.T) {
	docs := []*tail.Document{
		{ID: document.NewU64ID(1)},
		{ID: document.NewU64ID(2)},
		{ID: document.NewU64ID(3)},
	}

	set := BuildDocIDSet(docs)
	if len(set) != 3 {
		t.Errorf("expected 3 entries in set, got %d", len(set))
	}

	if !set["1"] || !set["2"] || !set["3"] {
		t.Error("set should contain all doc IDs")
	}

	if set["4"] {
		t.Error("set should not contain doc ID 4")
	}
}
