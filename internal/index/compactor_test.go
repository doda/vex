package index

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestCompactorConfig_Defaults(t *testing.T) {
	config := DefaultCompactorConfig()

	if config.Recluster {
		t.Error("expected Recluster to be false by default")
	}
	if config.NClusters != 0 {
		t.Errorf("expected NClusters to be 0 (auto), got %d", config.NClusters)
	}
	if config.Metric != vector.MetricCosineDistance {
		t.Errorf("expected MetricCosineDistance, got %s", config.Metric)
	}
	if config.MaxConcurrentCompactions != 1 {
		t.Errorf("expected MaxConcurrentCompactions to be 1, got %d", config.MaxConcurrentCompactions)
	}
}

func TestFullCompactor_MergeDocuments(t *testing.T) {
	compactor := &FullCompactor{config: DefaultCompactorConfig()}

	// Documents with various scenarios:
	// - "doc1" appears twice, newer version should win
	// - "doc2" appears once
	// - "doc3" is deleted (tombstone)
	docs := []MergedDocument{
		{ID: "doc1", WALSeq: 10, Attributes: map[string]any{"v": 10}},
		{ID: "doc2", WALSeq: 8, Attributes: map[string]any{"v": 8}},
		{ID: "doc1", WALSeq: 5, Attributes: map[string]any{"v": 5}}, // older, should be dropped
		{ID: "doc3", WALSeq: 15, Deleted: true},                     // tombstone
		{ID: "doc3", WALSeq: 3, Attributes: map[string]any{"v": 3}}, // old version, but tombstone wins
	}

	merged := compactor.mergeDocuments(docs)

	// Should have doc1 (v=10) and doc2 (v=8)
	// doc3 should be excluded due to tombstone
	if len(merged) != 2 {
		t.Fatalf("expected 2 merged docs, got %d", len(merged))
	}

	found := make(map[string]int)
	for _, doc := range merged {
		if v, ok := doc.Attributes["v"].(int); ok {
			found[doc.ID] = v
		}
	}

	if found["doc1"] != 10 {
		t.Errorf("expected doc1 v=10, got v=%d", found["doc1"])
	}
	if found["doc2"] != 8 {
		t.Errorf("expected doc2 v=8, got v=%d", found["doc2"])
	}
	if _, exists := found["doc3"]; exists {
		t.Error("doc3 should not be in merged output (was deleted)")
	}
}

func TestFullCompactor_MergeDocuments_PreservesNewest(t *testing.T) {
	compactor := &FullCompactor{config: DefaultCompactorConfig()}

	// Same document with different WAL sequences
	docs := []MergedDocument{
		{ID: "doc1", WALSeq: 100, Attributes: map[string]any{"version": "newest"}},
		{ID: "doc1", WALSeq: 50, Attributes: map[string]any{"version": "middle"}},
		{ID: "doc1", WALSeq: 1, Attributes: map[string]any{"version": "oldest"}},
	}

	merged := compactor.mergeDocuments(docs)

	if len(merged) != 1 {
		t.Fatalf("expected 1 merged doc, got %d", len(merged))
	}

	if merged[0].Attributes["version"] != "newest" {
		t.Errorf("expected version=newest, got %v", merged[0].Attributes["version"])
	}
	if merged[0].WALSeq != 100 {
		t.Errorf("expected WAL seq 100, got %d", merged[0].WALSeq)
	}
}

func TestFullCompactor_MergeDocuments_AllDeleted(t *testing.T) {
	compactor := &FullCompactor{config: DefaultCompactorConfig()}

	docs := []MergedDocument{
		{ID: "doc1", WALSeq: 10, Deleted: true},
		{ID: "doc2", WALSeq: 5, Deleted: true},
	}

	merged := compactor.mergeDocuments(docs)

	if len(merged) != 0 {
		t.Errorf("expected 0 docs after merging only tombstones, got %d", len(merged))
	}
}

func TestFullCompactor_Compact_WithMemoryStore(t *testing.T) {
	// Create an in-memory object store for testing
	store := objectstore.NewMemoryStore()
	compactor := NewFullCompactor(store, "test-ns", DefaultCompactorConfig())

	// Create a compaction plan with source segments
	// Since we don't have actual segment data, we'll test with empty segments
	plan := &CompactionPlan{
		SourceSegments: []Segment{
			{ID: "seg_1", Level: L0, StartWALSeq: 1, EndWALSeq: 10, Stats: SegmentStats{RowCount: 100}},
			{ID: "seg_2", Level: L0, StartWALSeq: 11, EndWALSeq: 20, Stats: SegmentStats{RowCount: 100}},
		},
		TargetLevel: L1,
		MinWALSeq:   1,
		MaxWALSeq:   20,
		TotalBytes:  2048,
		TotalRows:   200,
	}

	result, err := compactor.Compact(context.Background(), plan)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	if result.NewSegment == nil {
		t.Fatal("expected NewSegment, got nil")
	}
	if result.NewSegment.Level != L1 {
		t.Errorf("expected level L1, got %d", result.NewSegment.Level)
	}
	if result.NewSegment.StartWALSeq != 1 || result.NewSegment.EndWALSeq != 20 {
		t.Errorf("expected WAL range 1-20, got %d-%d", result.NewSegment.StartWALSeq, result.NewSegment.EndWALSeq)
	}
}

func TestFullCompactor_ConcurrencyLimit(t *testing.T) {
	store := objectstore.NewMemoryStore()
	config := &CompactorConfig{
		MaxConcurrentCompactions: 1,
		Metric:                   vector.MetricCosineDistance,
	}
	compactor := NewFullCompactor(store, "test-ns", config)

	// Manually set active to max
	compactor.mu.Lock()
	compactor.active = 1
	compactor.mu.Unlock()

	plan := &CompactionPlan{
		SourceSegments: []Segment{{ID: "seg_1", Level: L0}},
		TargetLevel:    L1,
		MinWALSeq:      1,
		MaxWALSeq:      10,
	}

	_, err := compactor.Compact(context.Background(), plan)
	if err != ErrCompactionInProgress {
		t.Errorf("expected ErrCompactionInProgress, got %v", err)
	}
}

func TestFullCompactor_InvalidPlan(t *testing.T) {
	store := objectstore.NewMemoryStore()
	compactor := NewFullCompactor(store, "test-ns", nil)

	// Empty source segments
	plan := &CompactionPlan{
		SourceSegments: []Segment{},
		TargetLevel:    L1,
	}

	_, err := compactor.Compact(context.Background(), plan)
	if err == nil {
		t.Error("expected error for empty plan, got nil")
	}
}

func TestBackgroundCompactor_StartStop(t *testing.T) {
	store := objectstore.NewMemoryStore()
	bc := NewBackgroundCompactor(store, nil, nil)

	if bc.IsRunning() {
		t.Error("new BackgroundCompactor should not be running")
	}

	bc.Start()

	if !bc.IsRunning() {
		t.Error("BackgroundCompactor should be running after Start")
	}

	// Try starting again (should be idempotent)
	bc.Start()
	if !bc.IsRunning() {
		t.Error("BackgroundCompactor should still be running")
	}

	bc.Stop()

	if bc.IsRunning() {
		t.Error("BackgroundCompactor should not be running after Stop")
	}

	// Try stopping again (should be idempotent)
	bc.Stop()
	if bc.IsRunning() {
		t.Error("BackgroundCompactor should still be stopped")
	}
}

func TestBackgroundCompactor_RegisterUnregister(t *testing.T) {
	store := objectstore.NewMemoryStore()
	bc := NewBackgroundCompactor(store, nil, nil)

	tree := NewLSMTree("test-ns", store, nil)

	bc.RegisterNamespace("test-ns", tree)

	bc.mu.RLock()
	_, exists := bc.trees["test-ns"]
	bc.mu.RUnlock()

	if !exists {
		t.Error("namespace should be registered")
	}

	bc.UnregisterNamespace("test-ns")

	bc.mu.RLock()
	_, exists = bc.trees["test-ns"]
	bc.mu.RUnlock()

	if exists {
		t.Error("namespace should be unregistered")
	}
}

func TestBackgroundCompactor_TriggerCompaction_NoWork(t *testing.T) {
	store := objectstore.NewMemoryStore()
	bc := NewBackgroundCompactor(store, nil, nil)

	tree := NewLSMTree("test-ns", store, nil)
	bc.RegisterNamespace("test-ns", tree)

	// No segments, so no compaction needed
	err := bc.TriggerCompaction("test-ns")
	if err != ErrNoSegmentsToCompact {
		t.Errorf("expected ErrNoSegmentsToCompact, got %v", err)
	}
}

func TestBackgroundCompactor_TriggerCompaction_UnknownNamespace(t *testing.T) {
	store := objectstore.NewMemoryStore()
	bc := NewBackgroundCompactor(store, nil, nil)

	err := bc.TriggerCompaction("unknown-ns")
	if err == nil {
		t.Error("expected error for unknown namespace")
	}
}

func TestBackgroundCompactor_L0Compaction(t *testing.T) {
	store := objectstore.NewMemoryStore()

	config := &LSMConfig{
		L0CompactionThreshold: 2, // Compact after 2 L0 segments
		L1CompactionThreshold: 4,
		L0TargetSizeBytes:     1024,
		L1TargetSizeBytes:     10 * 1024,
		L2TargetSizeBytes:     100 * 1024,
	}
	bc := NewBackgroundCompactor(store, config, nil)

	tree := NewLSMTree("test-ns", store, config)
	bc.RegisterNamespace("test-ns", tree)

	// Add L0 segments to trigger compaction
	seg1 := Segment{ID: "seg_1", Level: L0, StartWALSeq: 1, EndWALSeq: 10, Stats: SegmentStats{RowCount: 100, LogicalBytes: 1024}}
	seg2 := Segment{ID: "seg_2", Level: L0, StartWALSeq: 11, EndWALSeq: 20, Stats: SegmentStats{RowCount: 100, LogicalBytes: 1024}}
	tree.AddL0Segment(seg1)
	tree.AddL0Segment(seg2)

	if !tree.NeedsL0Compaction() {
		t.Error("should need L0 compaction")
	}

	// Trigger compaction
	err := bc.TriggerCompaction("test-ns")
	if err != nil {
		t.Fatalf("TriggerCompaction failed: %v", err)
	}

	// Verify L0 segments were replaced with L1 segment
	l0Segs := tree.GetL0Segments()
	if len(l0Segs) != 0 {
		t.Errorf("expected 0 L0 segments after compaction, got %d", len(l0Segs))
	}

	l1Segs := tree.GetL1Segments()
	if len(l1Segs) != 1 {
		t.Errorf("expected 1 L1 segment after compaction, got %d", len(l1Segs))
	}

	// Verify L1 segment has correct WAL range
	if len(l1Segs) > 0 {
		if l1Segs[0].StartWALSeq != 1 || l1Segs[0].EndWALSeq != 20 {
			t.Errorf("expected L1 segment WAL range 1-20, got %d-%d", l1Segs[0].StartWALSeq, l1Segs[0].EndWALSeq)
		}
	}
}

func TestBackgroundCompactor_L1ToL2Compaction(t *testing.T) {
	store := objectstore.NewMemoryStore()

	config := &LSMConfig{
		L0CompactionThreshold: 4,
		L1CompactionThreshold: 2, // Compact L1->L2 after 2 L1 segments
		L0TargetSizeBytes:     1024,
		L1TargetSizeBytes:     10 * 1024,
		L2TargetSizeBytes:     100 * 1024,
	}
	bc := NewBackgroundCompactor(store, config, nil)

	tree := NewLSMTree("test-ns", store, config)
	bc.RegisterNamespace("test-ns", tree)

	// Add L1 segments directly
	tree.mu.Lock()
	tree.manifest.AddSegment(Segment{ID: "l1_1", Level: L1, StartWALSeq: 1, EndWALSeq: 50, Stats: SegmentStats{RowCount: 500, LogicalBytes: 5000}})
	tree.manifest.AddSegment(Segment{ID: "l1_2", Level: L1, StartWALSeq: 51, EndWALSeq: 100, Stats: SegmentStats{RowCount: 500, LogicalBytes: 5000}})
	tree.mu.Unlock()

	if !tree.NeedsL1Compaction() {
		t.Error("should need L1 compaction")
	}

	// Trigger compaction
	err := bc.TriggerCompaction("test-ns")
	if err != nil {
		t.Fatalf("TriggerCompaction failed: %v", err)
	}

	// Verify L1 segments were replaced with L2 segment
	l1Segs := tree.GetL1Segments()
	if len(l1Segs) != 0 {
		t.Errorf("expected 0 L1 segments after compaction, got %d", len(l1Segs))
	}

	l2Segs := tree.GetL2Segments()
	if len(l2Segs) != 1 {
		t.Errorf("expected 1 L2 segment after compaction, got %d", len(l2Segs))
	}
}

func TestBackgroundCompactor_AutomaticPolling(t *testing.T) {
	store := objectstore.NewMemoryStore()

	config := &LSMConfig{
		L0CompactionThreshold: 2,
		L1CompactionThreshold: 4,
		L0TargetSizeBytes:     1024,
		L1TargetSizeBytes:     10 * 1024,
		L2TargetSizeBytes:     100 * 1024,
	}
	bc := NewBackgroundCompactor(store, config, nil)
	bc.pollInterval = 50 * time.Millisecond // Fast polling for test

	tree := NewLSMTree("test-ns", store, config)
	bc.RegisterNamespace("test-ns", tree)

	// Add segments to trigger compaction
	seg1 := Segment{ID: "seg_1", Level: L0, StartWALSeq: 1, EndWALSeq: 10, Stats: SegmentStats{RowCount: 100, LogicalBytes: 1024}}
	seg2 := Segment{ID: "seg_2", Level: L0, StartWALSeq: 11, EndWALSeq: 20, Stats: SegmentStats{RowCount: 100, LogicalBytes: 1024}}
	tree.AddL0Segment(seg1)
	tree.AddL0Segment(seg2)

	bc.Start()
	defer bc.Stop()

	// Wait for background compaction
	time.Sleep(200 * time.Millisecond)

	// Verify compaction happened
	l0Segs := tree.GetL0Segments()
	l1Segs := tree.GetL1Segments()

	if len(l0Segs) != 0 {
		t.Errorf("expected 0 L0 segments after background compaction, got %d", len(l0Segs))
	}
	if len(l1Segs) != 1 {
		t.Errorf("expected 1 L1 segment after background compaction, got %d", len(l1Segs))
	}
}

func TestFullCompactor_WithVectors(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create source segments with vector data
	dims := 4
	ns := "test-ns"

	// Build first L0 segment with IVF data
	segID1 := "seg_l0_1"
	builder1 := vector.NewIVFBuilder(dims, vector.MetricCosineDistance, 2)
	builder1.AddVector(1, []float32{1.0, 0.0, 0.0, 0.0})
	builder1.AddVector(2, []float32{0.0, 1.0, 0.0, 0.0})
	builder1.AddVector(3, []float32{0.5, 0.5, 0.0, 0.0})
	ivf1, _ := builder1.Build()

	// Write IVF data to store
	writer1 := NewSegmentWriter(store, ns, segID1)
	writer1.SetChecksumEnabled(false) // Disable checksums for in-memory store test
	var centroidsBuf1 bytes.Buffer
	ivf1.WriteCentroidsFile(&centroidsBuf1)
	centroidsKey1, _ := writer1.WriteIVFCentroids(ctx, centroidsBuf1.Bytes())

	var offsetsBuf1 bytes.Buffer
	ivf1.WriteClusterOffsetsFile(&offsetsBuf1)
	offsetsKey1, _ := writer1.WriteIVFClusterOffsets(ctx, offsetsBuf1.Bytes())

	clusterDataKey1, _ := writer1.WriteIVFClusterData(ctx, ivf1.GetClusterDataBytes())
	writer1.Seal()

	seg1 := Segment{
		ID:          segID1,
		Level:       L0,
		StartWALSeq: 1,
		EndWALSeq:   10,
		IVFKeys: &IVFKeys{
			CentroidsKey:      centroidsKey1,
			ClusterOffsetsKey: offsetsKey1,
			ClusterDataKey:    clusterDataKey1,
			NClusters:         ivf1.NClusters,
			VectorCount:       3,
		},
		Stats: SegmentStats{RowCount: 3, LogicalBytes: 1024},
	}

	// Build second L0 segment with more vectors
	segID2 := "seg_l0_2"
	builder2 := vector.NewIVFBuilder(dims, vector.MetricCosineDistance, 2)
	builder2.AddVector(4, []float32{0.0, 0.0, 1.0, 0.0})
	builder2.AddVector(5, []float32{0.0, 0.0, 0.0, 1.0})
	ivf2, _ := builder2.Build()

	writer2 := NewSegmentWriter(store, ns, segID2)
	writer2.SetChecksumEnabled(false) // Disable checksums for in-memory store test
	var centroidsBuf2 bytes.Buffer
	ivf2.WriteCentroidsFile(&centroidsBuf2)
	centroidsKey2, _ := writer2.WriteIVFCentroids(ctx, centroidsBuf2.Bytes())

	var offsetsBuf2 bytes.Buffer
	ivf2.WriteClusterOffsetsFile(&offsetsBuf2)
	offsetsKey2, _ := writer2.WriteIVFClusterOffsets(ctx, offsetsBuf2.Bytes())

	clusterDataKey2, _ := writer2.WriteIVFClusterData(ctx, ivf2.GetClusterDataBytes())
	writer2.Seal()

	seg2 := Segment{
		ID:          segID2,
		Level:       L0,
		StartWALSeq: 11,
		EndWALSeq:   20,
		IVFKeys: &IVFKeys{
			CentroidsKey:      centroidsKey2,
			ClusterOffsetsKey: offsetsKey2,
			ClusterDataKey:    clusterDataKey2,
			NClusters:         ivf2.NClusters,
			VectorCount:       2,
		},
		Stats: SegmentStats{RowCount: 2, LogicalBytes: 512},
	}

	// Create compaction plan
	plan := &CompactionPlan{
		SourceSegments: []Segment{seg1, seg2},
		TargetLevel:    L1,
		MinWALSeq:      1,
		MaxWALSeq:      20,
		TotalBytes:     1536,
		TotalRows:      5,
	}

	// Run compaction
	compactor := NewFullCompactor(store, ns, &CompactorConfig{
		Recluster:        true, // Force reclustering
		Metric:           vector.MetricCosineDistance,
		DisableChecksums: true, // Required for in-memory store tests
	})

	result, err := compactor.Compact(ctx, plan)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify result
	if result.NewSegment == nil {
		t.Fatal("expected NewSegment, got nil")
	}
	if result.NewSegment.Level != L1 {
		t.Errorf("expected level L1, got %d", result.NewSegment.Level)
	}
	if result.MergedDocs != 5 {
		t.Errorf("expected 5 merged docs, got %d", result.MergedDocs)
	}
	if !result.Reclustered {
		t.Error("expected Reclustered to be true")
	}
	if result.NewSegment.IVFKeys == nil {
		t.Error("expected IVFKeys, got nil")
	}
	if result.NewSegment.IVFKeys != nil && result.NewSegment.IVFKeys.VectorCount != 5 {
		t.Errorf("expected 5 vectors in IVF, got %d", result.NewSegment.IVFKeys.VectorCount)
	}
}

func TestCompactionResult_Duration(t *testing.T) {
	store := objectstore.NewMemoryStore()
	compactor := NewFullCompactor(store, "test-ns", nil)

	plan := &CompactionPlan{
		SourceSegments: []Segment{{ID: "seg_1", Level: L0}},
		TargetLevel:    L1,
		MinWALSeq:      1,
		MaxWALSeq:      10,
	}

	result, err := compactor.Compact(context.Background(), plan)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	if result.Duration <= 0 {
		t.Error("expected Duration > 0")
	}
}

func TestIntSqrt(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 0},
		{1, 1},
		{4, 2},
		{9, 3},
		{10, 3}, // floor(sqrt(10)) = 3
		{100, 10},
		{256, 16},
	}

	for _, tt := range tests {
		result := intSqrt(tt.input)
		if result != tt.expected {
			t.Errorf("intSqrt(%d) = %d, expected %d", tt.input, result, tt.expected)
		}
	}
}

// Test for Issue 1: Recluster flag should control whether k-means is run
func TestFullCompactor_ReclusterFlag(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()
	dims := 4
	ns := "test-ns"

	// Build a source segment with IVF data
	segID := "seg_source"
	builder := vector.NewIVFBuilder(dims, vector.MetricCosineDistance, 2)
	builder.AddVector(1, []float32{1.0, 0.0, 0.0, 0.0})
	builder.AddVector(2, []float32{0.0, 1.0, 0.0, 0.0})
	builder.AddVector(3, []float32{0.5, 0.5, 0.0, 0.0})
	ivf, _ := builder.Build()

	writer := NewSegmentWriter(store, ns, segID)
	writer.SetChecksumEnabled(false)

	var centroidsBuf bytes.Buffer
	ivf.WriteCentroidsFile(&centroidsBuf)
	centroidsKey, _ := writer.WriteIVFCentroids(ctx, centroidsBuf.Bytes())

	var offsetsBuf bytes.Buffer
	ivf.WriteClusterOffsetsFile(&offsetsBuf)
	offsetsKey, _ := writer.WriteIVFClusterOffsets(ctx, offsetsBuf.Bytes())

	clusterDataKey, _ := writer.WriteIVFClusterData(ctx, ivf.GetClusterDataBytes())
	writer.Seal()

	seg := Segment{
		ID:          segID,
		Level:       L0,
		StartWALSeq: 1,
		EndWALSeq:   10,
		IVFKeys: &IVFKeys{
			CentroidsKey:      centroidsKey,
			ClusterOffsetsKey: offsetsKey,
			ClusterDataKey:    clusterDataKey,
			NClusters:         ivf.NClusters,
			VectorCount:       3,
		},
	}

	plan := &CompactionPlan{
		SourceSegments: []Segment{seg},
		TargetLevel:    L1,
		MinWALSeq:      1,
		MaxWALSeq:      10,
	}

	t.Run("Recluster=false uses single-pass assignment", func(t *testing.T) {
		compactor := NewFullCompactor(store, ns, &CompactorConfig{
			Recluster:        false, // Should use single-pass, not full k-means
			Metric:           vector.MetricCosineDistance,
			DisableChecksums: true,
		})

		result, err := compactor.Compact(ctx, plan)
		if err != nil {
			t.Fatalf("Compact failed: %v", err)
		}

		if result.Reclustered {
			t.Error("expected Reclustered to be false when Recluster=false")
		}
		if result.NewSegment.IVFKeys == nil {
			t.Fatal("expected IVFKeys")
		}
		// With single-pass, we still create multiple clusters (based on sqrt(n))
		// Just verify we have valid IVF data
		if result.NewSegment.IVFKeys.NClusters < 1 {
			t.Errorf("expected at least 1 cluster, got %d", result.NewSegment.IVFKeys.NClusters)
		}
		if result.NewSegment.IVFKeys.VectorCount != 3 {
			t.Errorf("expected 3 vectors, got %d", result.NewSegment.IVFKeys.VectorCount)
		}
	})

	t.Run("Recluster=true runs k-means", func(t *testing.T) {
		compactor := NewFullCompactor(store, ns, &CompactorConfig{
			Recluster:        true, // Should run k-means
			Metric:           vector.MetricCosineDistance,
			DisableChecksums: true,
		})

		result, err := compactor.Compact(ctx, plan)
		if err != nil {
			t.Fatalf("Compact failed: %v", err)
		}

		if !result.Reclustered {
			t.Error("expected Reclustered to be true when Recluster=true")
		}
	})
}

// Test for Issue 2: NumericID should be properly handled
func TestMergedDocument_NumericID(t *testing.T) {
	// Verify that NumericID is properly preserved through the compaction process
	docs := []MergedDocument{
		{ID: "123", NumericID: 123, WALSeq: 10, Vector: []float32{1.0, 0.0}},
		{ID: "456", NumericID: 456, WALSeq: 20, Vector: []float32{0.0, 1.0}},
	}

	compactor := &FullCompactor{config: DefaultCompactorConfig()}
	merged := compactor.mergeDocuments(docs)

	if len(merged) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(merged))
	}

	// Check that NumericID is preserved
	for _, doc := range merged {
		if doc.ID == "123" && doc.NumericID != 123 {
			t.Errorf("expected NumericID 123 for doc 123, got %d", doc.NumericID)
		}
		if doc.ID == "456" && doc.NumericID != 456 {
			t.Errorf("expected NumericID 456 for doc 456, got %d", doc.NumericID)
		}
	}
}

// Test for Issue 3: Docs column should always be written to preserve metadata
func TestFullCompactor_DocsColumnAlwaysWritten(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create a segment with vector data but NO attributes
	ns := "test-ns"
	segID := "seg_no_attrs"
	dims := 4

	builder := vector.NewIVFBuilder(dims, vector.MetricCosineDistance, 1)
	builder.AddVector(1, []float32{1.0, 0.0, 0.0, 0.0})
	builder.AddVector(2, []float32{0.0, 1.0, 0.0, 0.0})
	ivf, _ := builder.Build()

	writer := NewSegmentWriter(store, ns, segID)
	writer.SetChecksumEnabled(false)

	var centroidsBuf bytes.Buffer
	ivf.WriteCentroidsFile(&centroidsBuf)
	centroidsKey, _ := writer.WriteIVFCentroids(ctx, centroidsBuf.Bytes())

	var offsetsBuf bytes.Buffer
	ivf.WriteClusterOffsetsFile(&offsetsBuf)
	offsetsKey, _ := writer.WriteIVFClusterOffsets(ctx, offsetsBuf.Bytes())

	clusterDataKey, _ := writer.WriteIVFClusterData(ctx, ivf.GetClusterDataBytes())
	writer.Seal()

	seg := Segment{
		ID:          segID,
		Level:       L0,
		StartWALSeq: 1,
		EndWALSeq:   10,
		IVFKeys: &IVFKeys{
			CentroidsKey:      centroidsKey,
			ClusterOffsetsKey: offsetsKey,
			ClusterDataKey:    clusterDataKey,
			NClusters:         1,
			VectorCount:       2,
		},
	}

	plan := &CompactionPlan{
		SourceSegments: []Segment{seg},
		TargetLevel:    L1,
		MinWALSeq:      1,
		MaxWALSeq:      10,
	}

	compactor := NewFullCompactor(store, ns, &CompactorConfig{
		Recluster:        false,
		Metric:           vector.MetricCosineDistance,
		DisableChecksums: true,
	})

	result, err := compactor.Compact(ctx, plan)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// DocsKey should be set even when documents have no attributes
	// This is needed to preserve WAL sequence and other metadata
	if result.NewSegment.DocsKey == "" {
		t.Error("expected DocsKey to be set even for documents without attributes")
	}

	// Verify we can read back the docs column
	reader, _, err := store.Get(ctx, result.NewSegment.DocsKey, nil)
	if err != nil {
		t.Fatalf("failed to read docs column: %v", err)
	}
	reader.Close()
}

// Test that NumericID is derived from string ID when loading docs from JSON
func TestFullCompactor_NumericIDDerivedFromDocsColumn(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()
	ns := "test-ns"

	// First, create a segment with a docs column (simulating a previous compaction output)
	// Write docs as JSON directly to the store
	docs := []MergedDocument{
		{ID: "100", WALSeq: 10, Vector: []float32{1.0, 0.0, 0.0, 0.0}},
		{ID: "200", WALSeq: 20, Vector: []float32{0.0, 1.0, 0.0, 0.0}},
		{ID: "300", WALSeq: 30, Vector: []float32{0.0, 0.0, 1.0, 0.0}},
	}
	// Note: NumericID is intentionally NOT set (0) to simulate old data or JSON without NumericID

	docsData, _ := json.Marshal(docs)
	docsKey := "vex/namespaces/test-ns/index/segments/seg_with_docs/docs.json"
	store.Put(ctx, docsKey, bytes.NewReader(docsData), int64(len(docsData)), nil)

	seg := Segment{
		ID:          "seg_with_docs",
		Level:       L0,
		StartWALSeq: 1,
		EndWALSeq:   30,
		DocsKey:     docsKey,
	}

	plan := &CompactionPlan{
		SourceSegments: []Segment{seg},
		TargetLevel:    L1,
		MinWALSeq:      1,
		MaxWALSeq:      30,
	}

	compactor := NewFullCompactor(store, ns, &CompactorConfig{
		Recluster:        true,
		Metric:           vector.MetricCosineDistance,
		DisableChecksums: true,
	})

	result, err := compactor.Compact(ctx, plan)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify we got all 3 vectors in the IVF
	if result.NewSegment.IVFKeys == nil {
		t.Fatal("expected IVFKeys")
	}
	if result.NewSegment.IVFKeys.VectorCount != 3 {
		t.Errorf("expected 3 vectors, got %d (NumericID derivation may have failed)", result.NewSegment.IVFKeys.VectorCount)
	}
	if result.MergedDocs != 3 {
		t.Errorf("expected 3 merged docs, got %d", result.MergedDocs)
	}
}
