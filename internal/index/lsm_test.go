package index

import (
	"context"
	"testing"
	"time"
)

func TestLSMTree_NewAndBasicOperations(t *testing.T) {
	tree := NewLSMTree("test-ns", nil, nil)
	if tree.Namespace() != "test-ns" {
		t.Errorf("expected namespace test-ns, got %s", tree.Namespace())
	}

	// Initially empty
	stats := tree.Stats()
	if stats.TotalSegments != 0 {
		t.Errorf("expected 0 segments, got %d", stats.TotalSegments)
	}
	if stats.IndexedWALSeq != 0 {
		t.Errorf("expected indexed WAL seq 0, got %d", stats.IndexedWALSeq)
	}

	if tree.NeedsL0Compaction() {
		t.Error("empty tree should not need L0 compaction")
	}
}

func TestLSMTree_AddL0Segment(t *testing.T) {
	tree := NewLSMTree("test-ns", nil, nil)

	seg := Segment{
		ID:          "seg_1",
		Level:       L0,
		StartWALSeq: 1,
		EndWALSeq:   10,
		CreatedAt:   time.Now(),
		Stats: SegmentStats{
			RowCount:     100,
			LogicalBytes: 1024,
		},
	}

	if err := tree.AddL0Segment(seg); err != nil {
		t.Fatalf("AddL0Segment failed: %v", err)
	}

	// Verify segment was added
	l0Segs := tree.GetL0Segments()
	if len(l0Segs) != 1 {
		t.Fatalf("expected 1 L0 segment, got %d", len(l0Segs))
	}
	if l0Segs[0].ID != "seg_1" {
		t.Errorf("expected segment ID seg_1, got %s", l0Segs[0].ID)
	}

	// Verify stats updated
	stats := tree.Stats()
	if stats.L0SegmentCount != 1 {
		t.Errorf("expected L0 count 1, got %d", stats.L0SegmentCount)
	}
	if stats.IndexedWALSeq != 10 {
		t.Errorf("expected indexed WAL seq 10, got %d", stats.IndexedWALSeq)
	}
}

func TestLSMTree_L0SegmentsBuiltFrequently(t *testing.T) {
	// Test that L0 segments can be built frequently from WAL batches
	tree := NewLSMTree("test-ns", nil, nil)

	// Simulate frequent L0 segment builds from WAL batches
	for i := 1; i <= 10; i++ {
		seg := Segment{
			ID:          GenerateSegmentID(),
			Level:       L0,
			StartWALSeq: uint64((i-1)*10 + 1),
			EndWALSeq:   uint64(i * 10),
			CreatedAt:   time.Now(),
			Stats: SegmentStats{
				RowCount:     int64(i * 10),
				LogicalBytes: int64(i * 1024),
			},
		}
		if err := tree.AddL0Segment(seg); err != nil {
			t.Fatalf("AddL0Segment %d failed: %v", i, err)
		}
	}

	l0Segs := tree.GetL0Segments()
	if len(l0Segs) != 10 {
		t.Errorf("expected 10 L0 segments, got %d", len(l0Segs))
	}

	// Verify segments are sorted by end WAL seq (newest first)
	for i := 0; i < len(l0Segs)-1; i++ {
		if l0Segs[i].EndWALSeq < l0Segs[i+1].EndWALSeq {
			t.Error("L0 segments should be sorted newest first")
		}
	}

	stats := tree.Stats()
	if stats.L0SegmentCount != 10 {
		t.Errorf("expected L0 count 10, got %d", stats.L0SegmentCount)
	}
	if stats.IndexedWALSeq != 100 {
		t.Errorf("expected indexed WAL seq 100, got %d", stats.IndexedWALSeq)
	}
}

func TestLSMTree_L0Compaction(t *testing.T) {
	config := &LSMConfig{
		L0CompactionThreshold: 4,
		L1CompactionThreshold: 4,
		L0TargetSizeBytes:     1024,
		L1TargetSizeBytes:     10 * 1024,
		L2TargetSizeBytes:     100 * 1024,
	}
	tree := NewLSMTree("test-ns", nil, config)

	// Add 3 L0 segments - not enough for compaction
	for i := 1; i <= 3; i++ {
		seg := Segment{
			ID:          GenerateSegmentID(),
			Level:       L0,
			StartWALSeq: uint64((i-1)*10 + 1),
			EndWALSeq:   uint64(i * 10),
			Stats: SegmentStats{
				RowCount:     100,
				LogicalBytes: 1024,
			},
		}
		tree.AddL0Segment(seg)
	}

	if tree.NeedsL0Compaction() {
		t.Error("should not need compaction with 3 segments")
	}

	// Add 4th segment - triggers compaction threshold
	seg := Segment{
		ID:          GenerateSegmentID(),
		Level:       L0,
		StartWALSeq: 31,
		EndWALSeq:   40,
		Stats: SegmentStats{
			RowCount:     100,
			LogicalBytes: 1024,
		},
	}
	tree.AddL0Segment(seg)

	if !tree.NeedsL0Compaction() {
		t.Error("should need compaction with 4 segments")
	}

	// Plan compaction
	plan, err := tree.PlanL0Compaction()
	if err != nil {
		t.Fatalf("PlanL0Compaction failed: %v", err)
	}

	if len(plan.SourceSegments) != 4 {
		t.Errorf("expected 4 source segments, got %d", len(plan.SourceSegments))
	}
	if plan.TargetLevel != L1 {
		t.Errorf("expected target level L1, got %d", plan.TargetLevel)
	}
	if plan.MinWALSeq != 1 || plan.MaxWALSeq != 40 {
		t.Errorf("expected WAL range 1-40, got %d-%d", plan.MinWALSeq, plan.MaxWALSeq)
	}
}

func TestLSMTree_PlanL0Compaction_LimitsSegments(t *testing.T) {
	config := &LSMConfig{
		L0CompactionThreshold: 4,
		L1CompactionThreshold: 4,
		MaxCompactionSegments: 2,
	}
	tree := NewLSMTree("test-ns", nil, config)

	for i := 1; i <= 4; i++ {
		seg := Segment{
			ID:          GenerateSegmentID(),
			Level:       L0,
			StartWALSeq: uint64((i-1)*10 + 1),
			EndWALSeq:   uint64(i * 10),
			Stats: SegmentStats{
				RowCount:     100,
				LogicalBytes: 1024,
			},
		}
		tree.AddL0Segment(seg)
	}

	if !tree.NeedsL0Compaction() {
		t.Fatal("expected L0 compaction to be needed")
	}

	plan, err := tree.PlanL0Compaction()
	if err != nil {
		t.Fatalf("PlanL0Compaction failed: %v", err)
	}

	if len(plan.SourceSegments) != 2 {
		t.Errorf("expected 2 source segments, got %d", len(plan.SourceSegments))
	}
	if plan.MinWALSeq != 1 || plan.MaxWALSeq != 20 {
		t.Errorf("expected WAL range 1-20, got %d-%d", plan.MinWALSeq, plan.MaxWALSeq)
	}
}

func TestLSMTree_ApplyCompaction(t *testing.T) {
	config := DefaultLSMConfig()
	config.L0CompactionThreshold = 2
	tree := NewLSMTree("test-ns", nil, config)

	// Add 2 L0 segments
	seg1 := Segment{ID: "seg_1", Level: L0, StartWALSeq: 1, EndWALSeq: 10, Stats: SegmentStats{RowCount: 100, LogicalBytes: 1024}}
	seg2 := Segment{ID: "seg_2", Level: L0, StartWALSeq: 11, EndWALSeq: 20, Stats: SegmentStats{RowCount: 100, LogicalBytes: 1024}}
	tree.AddL0Segment(seg1)
	tree.AddL0Segment(seg2)

	// Plan compaction
	plan, err := tree.PlanL0Compaction()
	if err != nil {
		t.Fatalf("PlanL0Compaction failed: %v", err)
	}

	// Simulate compaction result
	newSeg := Segment{
		ID:          "seg_merged",
		Level:       L1,
		StartWALSeq: 1,
		EndWALSeq:   20,
		Stats: SegmentStats{
			RowCount:     180, // After deduplication
			LogicalBytes: 1800,
		},
	}

	// Apply compaction
	if err := tree.ApplyCompaction(plan, newSeg); err != nil {
		t.Fatalf("ApplyCompaction failed: %v", err)
	}

	// Verify L0 is empty, L1 has the merged segment
	l0Segs := tree.GetL0Segments()
	if len(l0Segs) != 0 {
		t.Errorf("expected 0 L0 segments after compaction, got %d", len(l0Segs))
	}

	l1Segs := tree.GetL1Segments()
	if len(l1Segs) != 1 {
		t.Fatalf("expected 1 L1 segment after compaction, got %d", len(l1Segs))
	}
	if l1Segs[0].ID != "seg_merged" {
		t.Errorf("expected merged segment ID, got %s", l1Segs[0].ID)
	}

	stats := tree.Stats()
	if stats.L0SegmentCount != 0 {
		t.Errorf("expected L0 count 0, got %d", stats.L0SegmentCount)
	}
	if stats.L1SegmentCount != 1 {
		t.Errorf("expected L1 count 1, got %d", stats.L1SegmentCount)
	}
}

func TestLSMTree_L1ToL2Compaction(t *testing.T) {
	config := &LSMConfig{
		L0CompactionThreshold: 2,
		L1CompactionThreshold: 2,
		L0TargetSizeBytes:     1024,
		L1TargetSizeBytes:     10 * 1024,
		L2TargetSizeBytes:     100 * 1024,
	}
	tree := NewLSMTree("test-ns", nil, config)

	// Manually add L1 segments (simulating multiple L0->L1 compactions)
	for i := 1; i <= 2; i++ {
		// Add segment at L1 level via manifest manipulation
		seg := Segment{
			ID:          GenerateSegmentID(),
			Level:       L1,
			StartWALSeq: uint64((i-1)*100 + 1),
			EndWALSeq:   uint64(i * 100),
			Stats: SegmentStats{
				RowCount:     1000,
				LogicalBytes: 10 * 1024,
			},
		}
		tree.mu.Lock()
		tree.manifest.AddSegment(seg)
		tree.mu.Unlock()
	}

	if !tree.NeedsL1Compaction() {
		t.Error("should need L1 compaction with 2 L1 segments")
	}

	// Plan L1->L2 compaction
	plan, err := tree.PlanL1Compaction()
	if err != nil {
		t.Fatalf("PlanL1Compaction failed: %v", err)
	}

	if len(plan.SourceSegments) != 2 {
		t.Errorf("expected 2 source segments, got %d", len(plan.SourceSegments))
	}
	if plan.TargetLevel != L2 {
		t.Errorf("expected target level L2, got %d", plan.TargetLevel)
	}

	// Apply compaction
	newSeg := Segment{
		ID:          "seg_l2",
		Level:       L2,
		StartWALSeq: 1,
		EndWALSeq:   200,
		Stats: SegmentStats{
			RowCount:     1800,
			LogicalBytes: 18 * 1024,
		},
	}

	if err := tree.ApplyCompaction(plan, newSeg); err != nil {
		t.Fatalf("ApplyCompaction L1->L2 failed: %v", err)
	}

	l2Segs := tree.GetL2Segments()
	if len(l2Segs) != 1 {
		t.Errorf("expected 1 L2 segment, got %d", len(l2Segs))
	}

	stats := tree.Stats()
	if stats.L1SegmentCount != 0 {
		t.Errorf("expected L1 count 0, got %d", stats.L1SegmentCount)
	}
	if stats.L2SegmentCount != 1 {
		t.Errorf("expected L2 count 1, got %d", stats.L2SegmentCount)
	}
}

func TestLSMTree_PlanL1Compaction_LimitsBytes(t *testing.T) {
	config := &LSMConfig{
		L0CompactionThreshold: 2,
		L1CompactionThreshold: 2,
		MaxCompactionBytes:    12 * 1024,
	}
	tree := NewLSMTree("test-ns", nil, config)

	for i := 1; i <= 2; i++ {
		seg := Segment{
			ID:          GenerateSegmentID(),
			Level:       L1,
			StartWALSeq: uint64((i-1)*100 + 1),
			EndWALSeq:   uint64(i * 100),
			Stats: SegmentStats{
				RowCount:     1000,
				LogicalBytes: 10 * 1024,
			},
		}
		tree.mu.Lock()
		tree.manifest.AddSegment(seg)
		tree.mu.Unlock()
	}

	if !tree.NeedsL1Compaction() {
		t.Fatal("expected L1 compaction to be needed")
	}

	plan, err := tree.PlanL1Compaction()
	if err != nil {
		t.Fatalf("PlanL1Compaction failed: %v", err)
	}

	if len(plan.SourceSegments) != 1 {
		t.Errorf("expected 1 source segment, got %d", len(plan.SourceSegments))
	}
	if plan.MinWALSeq != 1 || plan.MaxWALSeq != 100 {
		t.Errorf("expected WAL range 1-100, got %d-%d", plan.MinWALSeq, plan.MaxWALSeq)
	}
}

func TestLSMTree_GetAllSegments_Ordering(t *testing.T) {
	tree := NewLSMTree("test-ns", nil, nil)

	// Add segments at different levels with varying WAL seqs
	segments := []Segment{
		{ID: "l0_1", Level: L0, StartWALSeq: 91, EndWALSeq: 100, Stats: SegmentStats{RowCount: 10}},
		{ID: "l0_2", Level: L0, StartWALSeq: 81, EndWALSeq: 90, Stats: SegmentStats{RowCount: 10}},
		{ID: "l1_1", Level: L1, StartWALSeq: 1, EndWALSeq: 50, Stats: SegmentStats{RowCount: 50}},
		{ID: "l2_1", Level: L2, StartWALSeq: 51, EndWALSeq: 80, Stats: SegmentStats{RowCount: 30}},
	}

	for _, seg := range segments {
		tree.mu.Lock()
		tree.manifest.AddSegment(seg)
		tree.mu.Unlock()
	}

	// GetAllSegments should return segments sorted by end WAL seq (newest first)
	allSegs := tree.GetAllSegments()
	if len(allSegs) != 4 {
		t.Fatalf("expected 4 segments, got %d", len(allSegs))
	}

	// Verify order: newest first
	expectedOrder := []string{"l0_1", "l0_2", "l2_1", "l1_1"}
	for i, seg := range allSegs {
		if seg.ID != expectedOrder[i] {
			t.Errorf("position %d: expected %s, got %s", i, expectedOrder[i], seg.ID)
		}
	}
}

func TestLSMTree_QueryAcrossAllSegmentsAndTail(t *testing.T) {
	// This test verifies the query reading behavior across all segments + tail
	tree := NewLSMTree("test-ns", nil, nil)

	// Add segments representing indexed data
	l2Seg := Segment{ID: "l2_1", Level: L2, StartWALSeq: 1, EndWALSeq: 100, Stats: SegmentStats{RowCount: 100}}
	l1Seg := Segment{ID: "l1_1", Level: L1, StartWALSeq: 101, EndWALSeq: 150, Stats: SegmentStats{RowCount: 50}}
	l0Seg := Segment{ID: "l0_1", Level: L0, StartWALSeq: 151, EndWALSeq: 160, Stats: SegmentStats{RowCount: 10}}

	for _, seg := range []Segment{l2Seg, l1Seg, l0Seg} {
		tree.mu.Lock()
		tree.manifest.AddSegment(seg)
		tree.manifest.UpdateIndexedWALSeq()
		tree.mu.Unlock()
	}

	// Get all segments for query
	allSegs := tree.GetAllSegments()
	if len(allSegs) != 3 {
		t.Fatalf("expected 3 segments, got %d", len(allSegs))
	}

	// Verify indexed WAL seq
	if tree.IndexedWALSeq() != 160 {
		t.Errorf("expected indexed WAL seq 160, got %d", tree.IndexedWALSeq())
	}

	// Simulate tail documents (WAL 161-165, not yet indexed)
	tailDocs := []DocumentEntry{
		{ID: "doc_161", WALSeq: 161, Attributes: map[string]any{"value": 161}},
		{ID: "doc_162", WALSeq: 162, Attributes: map[string]any{"value": 162}},
	}

	// Create query iterator
	reader := NewSegmentReader(nil, "test-ns")
	iter := NewQuerySegmentIterator(reader, allSegs, tailDocs)

	// Query should process tail first (newest), then segments
	doc, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if doc == nil {
		t.Fatal("expected first tail document")
	}
	if doc.ID != "doc_161" {
		t.Errorf("expected doc_161, got %s", doc.ID)
	}
}

func TestL0SegmentBuilder(t *testing.T) {
	builder := NewL0SegmentBuilder("test-ns", nil, 1024)

	// Add documents
	for i := 1; i <= 5; i++ {
		builder.AddDocument(DocumentEntry{
			ID:         string(rune('a' + i - 1)),
			WALSeq:     uint64(i),
			Attributes: map[string]any{"index": i},
		})
	}

	if builder.DocumentCount() != 5 {
		t.Errorf("expected 5 documents, got %d", builder.DocumentCount())
	}

	// Build segment
	seg, data, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if seg == nil {
		t.Fatal("expected segment")
	}
	if data == nil {
		t.Fatal("expected segment data")
	}

	// Verify segment properties
	if seg.Level != L0 {
		t.Errorf("expected level L0, got %d", seg.Level)
	}
	if seg.StartWALSeq != 1 || seg.EndWALSeq != 5 {
		t.Errorf("expected WAL range 1-5, got %d-%d", seg.StartWALSeq, seg.EndWALSeq)
	}
	if seg.Stats.RowCount != 5 {
		t.Errorf("expected 5 rows, got %d", seg.Stats.RowCount)
	}

	// Verify data
	if len(data.Documents) != 5 {
		t.Errorf("expected 5 documents in data, got %d", len(data.Documents))
	}

	// Reset and verify empty
	builder.Reset()
	if builder.DocumentCount() != 0 {
		t.Errorf("expected 0 documents after reset, got %d", builder.DocumentCount())
	}
}

func TestL0SegmentBuilder_WithTombstones(t *testing.T) {
	builder := NewL0SegmentBuilder("test-ns", nil, 1024)

	// Add mix of regular docs and tombstones
	builder.AddDocument(DocumentEntry{ID: "a", WALSeq: 1, Deleted: false})
	builder.AddDocument(DocumentEntry{ID: "b", WALSeq: 2, Deleted: true}) // tombstone
	builder.AddDocument(DocumentEntry{ID: "c", WALSeq: 3, Deleted: false})

	seg, _, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// RowCount should only count non-deleted
	if seg.Stats.RowCount != 2 {
		t.Errorf("expected 2 live rows, got %d", seg.Stats.RowCount)
	}
	if seg.Stats.TombstoneCount != 1 {
		t.Errorf("expected 1 tombstone, got %d", seg.Stats.TombstoneCount)
	}
}

func TestDeduplicateDocuments(t *testing.T) {
	// Documents sorted by WAL seq (newest first)
	docs := []DocumentEntry{
		{ID: "a", WALSeq: 10, Attributes: map[string]any{"v": 10}}, // newest 'a'
		{ID: "b", WALSeq: 9, Deleted: true},                        // 'b' is deleted
		{ID: "a", WALSeq: 5, Attributes: map[string]any{"v": 5}},   // older 'a' (should be ignored)
		{ID: "c", WALSeq: 3, Attributes: map[string]any{"v": 3}},
		{ID: "b", WALSeq: 2, Attributes: map[string]any{"v": 2}}, // older 'b' (but tombstone came later)
	}

	deduped := DeduplicateDocuments(docs)

	// Should have 'a' (v=10) and 'c' (v=3)
	// 'b' should be excluded because its newest version is a tombstone
	if len(deduped) != 2 {
		t.Fatalf("expected 2 documents after dedup, got %d", len(deduped))
	}

	// Verify contents
	idToVal := make(map[string]int)
	for _, doc := range deduped {
		if v, ok := doc.Attributes["v"].(int); ok {
			idToVal[doc.ID] = v
		}
	}

	if idToVal["a"] != 10 {
		t.Errorf("expected 'a' to have v=10, got %d", idToVal["a"])
	}
	if _, exists := idToVal["b"]; exists {
		t.Error("'b' should not be in results (was deleted)")
	}
	if idToVal["c"] != 3 {
		t.Errorf("expected 'c' to have v=3, got %d", idToVal["c"])
	}
}

func TestCompactionPlan_Validate(t *testing.T) {
	tests := []struct {
		name    string
		plan    *CompactionPlan
		wantErr bool
	}{
		{
			name:    "empty source segments",
			plan:    &CompactionPlan{SourceSegments: nil, TargetLevel: L1},
			wantErr: true,
		},
		{
			name:    "target level too low",
			plan:    &CompactionPlan{SourceSegments: []Segment{{ID: "a"}}, TargetLevel: L0},
			wantErr: true,
		},
		{
			name:    "invalid WAL range",
			plan:    &CompactionPlan{SourceSegments: []Segment{{ID: "a"}}, TargetLevel: L1, MinWALSeq: 10, MaxWALSeq: 5},
			wantErr: true,
		},
		{
			name:    "valid plan",
			plan:    &CompactionPlan{SourceSegments: []Segment{{ID: "a"}}, TargetLevel: L1, MinWALSeq: 1, MaxWALSeq: 10},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plan.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLSMTree_CompactingFlag(t *testing.T) {
	tree := NewLSMTree("test-ns", nil, nil)

	if tree.IsCompacting() {
		t.Error("new tree should not be compacting")
	}

	// Set compacting
	if err := tree.SetCompacting(true); err != nil {
		t.Fatalf("SetCompacting(true) failed: %v", err)
	}

	if !tree.IsCompacting() {
		t.Error("should be compacting")
	}

	// Try to set compacting again - should fail
	if err := tree.SetCompacting(true); err != ErrCompactionInProgress {
		t.Errorf("expected ErrCompactionInProgress, got %v", err)
	}

	// Clear compacting
	if err := tree.SetCompacting(false); err != nil {
		t.Fatalf("SetCompacting(false) failed: %v", err)
	}

	if tree.IsCompacting() {
		t.Error("should not be compacting after clear")
	}
}

func TestLSMTree_Close(t *testing.T) {
	tree := NewLSMTree("test-ns", nil, nil)

	if tree.IsClosed() {
		t.Error("new tree should not be closed")
	}

	if err := tree.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !tree.IsClosed() {
		t.Error("tree should be closed")
	}

	// Operations on closed tree should fail
	seg := Segment{ID: "test", Level: L0, StartWALSeq: 1, EndWALSeq: 10}
	if err := tree.AddL0Segment(seg); err != ErrLSMTreeClosed {
		t.Errorf("expected ErrLSMTreeClosed, got %v", err)
	}
}

func TestLSMTree_GetLevelInfo(t *testing.T) {
	tree := NewLSMTree("test-ns", nil, nil)

	// Add segments at different levels
	segments := []Segment{
		{ID: "l0_1", Level: L0, StartWALSeq: 91, EndWALSeq: 100, Stats: SegmentStats{RowCount: 10, LogicalBytes: 100}},
		{ID: "l0_2", Level: L0, StartWALSeq: 81, EndWALSeq: 90, Stats: SegmentStats{RowCount: 10, LogicalBytes: 100}},
		{ID: "l1_1", Level: L1, StartWALSeq: 1, EndWALSeq: 50, Stats: SegmentStats{RowCount: 50, LogicalBytes: 500}},
		{ID: "l2_1", Level: L2, StartWALSeq: 51, EndWALSeq: 80, Stats: SegmentStats{RowCount: 30, LogicalBytes: 300}},
	}

	for _, seg := range segments {
		tree.mu.Lock()
		tree.manifest.AddSegment(seg)
		tree.mu.Unlock()
	}

	levels := tree.GetLevelInfo()
	if len(levels) != 3 {
		t.Fatalf("expected 3 levels, got %d", len(levels))
	}

	// L0
	if levels[0].Level != L0 || levels[0].SegmentCount != 2 {
		t.Errorf("L0: expected level=0, count=2, got level=%d, count=%d", levels[0].Level, levels[0].SegmentCount)
	}
	if levels[0].TotalBytes != 200 {
		t.Errorf("L0: expected 200 bytes, got %d", levels[0].TotalBytes)
	}

	// L1
	if levels[1].Level != L1 || levels[1].SegmentCount != 1 {
		t.Errorf("L1: expected level=1, count=1, got level=%d, count=%d", levels[1].Level, levels[1].SegmentCount)
	}

	// L2
	if levels[2].Level != L2 || levels[2].SegmentCount != 1 {
		t.Errorf("L2: expected level=2, count=1, got level=%d, count=%d", levels[2].Level, levels[2].SegmentCount)
	}
}

func TestCompactor_CompactSegments(t *testing.T) {
	compactor := NewCompactor(nil, nil)

	plan := &CompactionPlan{
		SourceSegments: []Segment{
			{ID: "seg_1", Level: L0, StartWALSeq: 1, EndWALSeq: 10, Stats: SegmentStats{RowCount: 100, LogicalBytes: 1024}},
			{ID: "seg_2", Level: L0, StartWALSeq: 11, EndWALSeq: 20, Stats: SegmentStats{RowCount: 100, LogicalBytes: 1024}},
		},
		TargetLevel: L1,
		MinWALSeq:   1,
		MaxWALSeq:   20,
		TotalBytes:  2048,
		TotalRows:   200,
	}

	merged, err := compactor.CompactSegments(context.Background(), plan)
	if err != nil {
		t.Fatalf("CompactSegments failed: %v", err)
	}

	if merged.Level != L1 {
		t.Errorf("expected level L1, got %d", merged.Level)
	}
	if merged.StartWALSeq != 1 || merged.EndWALSeq != 20 {
		t.Errorf("expected WAL range 1-20, got %d-%d", merged.StartWALSeq, merged.EndWALSeq)
	}
}

func TestLSMConfig_TargetSizeForLevel(t *testing.T) {
	config := DefaultLSMConfig()

	if config.TargetSizeForLevel(L0) != DefaultL0TargetSizeBytes {
		t.Errorf("L0 target size mismatch")
	}
	if config.TargetSizeForLevel(L1) != DefaultL1TargetSizeBytes {
		t.Errorf("L1 target size mismatch")
	}
	if config.TargetSizeForLevel(L2) != DefaultL2TargetSizeBytes {
		t.Errorf("L2 target size mismatch")
	}
}

func TestL0SegmentBuilder_ShouldFlush(t *testing.T) {
	builder := NewL0SegmentBuilder("test-ns", nil, 100) // Small target for testing

	// Initially should not flush
	if builder.ShouldFlush() {
		t.Error("empty builder should not flush")
	}

	// Add documents until we exceed target
	for i := 0; i < 10; i++ {
		builder.AddDocument(DocumentEntry{
			ID:         string(rune('a' + i)),
			WALSeq:     uint64(i + 1),
			Attributes: map[string]any{"data": "some long string value here"},
		})
	}

	if !builder.ShouldFlush() {
		t.Error("builder should flush after exceeding target size")
	}
}

func TestEstimateCompactionOutputSize(t *testing.T) {
	plan := &CompactionPlan{
		TotalBytes: 10000,
	}

	estimate := EstimateCompactionOutputSize(plan)
	expected := int64(8000) // 80% of input

	if estimate != expected {
		t.Errorf("expected estimate %d, got %d", expected, estimate)
	}
}

func TestCompactionPlan_ObsoleteSegmentIDs(t *testing.T) {
	plan := &CompactionPlan{
		SourceSegments: []Segment{
			{ID: "seg_1"},
			{ID: "seg_2"},
			{ID: "seg_3"},
		},
	}

	ids := plan.ObsoleteSegmentIDs()
	if len(ids) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(ids))
	}

	expected := map[string]bool{"seg_1": true, "seg_2": true, "seg_3": true}
	for _, id := range ids {
		if !expected[id] {
			t.Errorf("unexpected ID: %s", id)
		}
	}
}
