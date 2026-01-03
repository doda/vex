package index

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewManifest(t *testing.T) {
	ns := "test-namespace"
	m := NewManifest(ns)

	if m.FormatVersion != CurrentManifestVersion {
		t.Errorf("expected format_version %d, got %d", CurrentManifestVersion, m.FormatVersion)
	}
	if m.Namespace != ns {
		t.Errorf("expected namespace %q, got %q", ns, m.Namespace)
	}
	if m.GeneratedAt.IsZero() {
		t.Error("expected generated_at to be set")
	}
	if m.IndexedWALSeq != 0 {
		t.Errorf("expected indexed_wal_seq 0, got %d", m.IndexedWALSeq)
	}
	if len(m.Segments) != 0 {
		t.Errorf("expected empty segments, got %d", len(m.Segments))
	}
}

func TestManifestRequiredFields(t *testing.T) {
	m := NewManifest("my-namespace")
	m.IndexedWALSeq = 100
	m.SchemaVersionHash = "abc123"

	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Check required fields exist
	requiredFields := []string{"format_version", "namespace", "generated_at", "indexed_wal_seq", "segments", "stats"}
	for _, field := range requiredFields {
		if _, ok := parsed[field]; !ok {
			t.Errorf("required field %q missing", field)
		}
	}

	// Verify format_version
	if v, ok := parsed["format_version"].(float64); !ok || int(v) != CurrentManifestVersion {
		t.Errorf("format_version incorrect: %v", parsed["format_version"])
	}

	// Verify namespace
	if v, ok := parsed["namespace"].(string); !ok || v != "my-namespace" {
		t.Errorf("namespace incorrect: %v", parsed["namespace"])
	}

	// Verify generated_at is a valid timestamp
	if _, ok := parsed["generated_at"].(string); !ok {
		t.Error("generated_at should be a string timestamp")
	}
}

func TestIndexedWALSeqTracking(t *testing.T) {
	m := NewManifest("test-ns")

	// Initially zero
	if m.IndexedWALSeq != 0 {
		t.Errorf("expected indexed_wal_seq 0, got %d", m.IndexedWALSeq)
	}

	// Add segments with different WAL ranges
	seg1 := Segment{
		ID:          "seg_001",
		Level:       0,
		StartWALSeq: 1,
		EndWALSeq:   50,
		Stats:       SegmentStats{RowCount: 100},
		CreatedAt:   time.Now().UTC(),
	}
	m.AddSegment(seg1)

	seg2 := Segment{
		ID:          "seg_002",
		Level:       0,
		StartWALSeq: 51,
		EndWALSeq:   100,
		Stats:       SegmentStats{RowCount: 200},
		CreatedAt:   time.Now().UTC(),
	}
	m.AddSegment(seg2)

	// Update indexed WAL seq from segments
	m.UpdateIndexedWALSeq()

	// Should be the max end_wal_seq
	if m.IndexedWALSeq != 100 {
		t.Errorf("expected indexed_wal_seq 100, got %d", m.IndexedWALSeq)
	}

	// Test WAL range coverage
	if !m.WALRangeCovered(50) {
		t.Error("WAL seq 50 should be covered")
	}
	if !m.WALRangeCovered(100) {
		t.Error("WAL seq 100 should be covered")
	}
	if m.WALRangeCovered(101) {
		t.Error("WAL seq 101 should NOT be covered")
	}

	// Add another segment with higher range
	seg3 := Segment{
		ID:          "seg_003",
		Level:       1,
		StartWALSeq: 1,
		EndWALSeq:   150, // Compacted segment covering wider range
		Stats:       SegmentStats{RowCount: 250},
		CreatedAt:   time.Now().UTC(),
	}
	m.AddSegment(seg3)
	m.UpdateIndexedWALSeq()

	if m.IndexedWALSeq != 150 {
		t.Errorf("expected indexed_wal_seq 150, got %d", m.IndexedWALSeq)
	}
}

func TestSegmentsArray(t *testing.T) {
	m := NewManifest("test-ns")

	// Add segment with all fields
	seg := Segment{
		ID:          "seg_01H123",
		Level:       0,
		StartWALSeq: 1,
		EndWALSeq:   100,
		DocsKey:     "vex/namespaces/test-ns/index/segments/seg_01H123/docs.col.zst",
		VectorsKey:  "vex/namespaces/test-ns/index/segments/seg_01H123/vectors.ivf.zst",
		FilterKeys: []string{
			"vex/namespaces/test-ns/index/segments/seg_01H123/filters/status.bitmap",
			"vex/namespaces/test-ns/index/segments/seg_01H123/filters/category.bitmap",
		},
		FTSKeys: []string{
			"vex/namespaces/test-ns/index/segments/seg_01H123/fts.title.bm25",
		},
		Stats: SegmentStats{
			RowCount:       1000,
			LogicalBytes:   50000,
			TombstoneCount: 10,
		},
		CreatedAt: time.Now().UTC(),
	}
	m.AddSegment(seg)

	// Serialize and check
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	segments, ok := parsed["segments"].([]interface{})
	if !ok || len(segments) != 1 {
		t.Fatalf("expected 1 segment, got %v", parsed["segments"])
	}

	segMap := segments[0].(map[string]interface{})

	// Check segment fields
	if segMap["id"] != "seg_01H123" {
		t.Errorf("expected id seg_01H123, got %v", segMap["id"])
	}
	if int(segMap["level"].(float64)) != 0 {
		t.Errorf("expected level 0, got %v", segMap["level"])
	}
	if int(segMap["start_wal_seq"].(float64)) != 1 {
		t.Errorf("expected start_wal_seq 1, got %v", segMap["start_wal_seq"])
	}
	if int(segMap["end_wal_seq"].(float64)) != 100 {
		t.Errorf("expected end_wal_seq 100, got %v", segMap["end_wal_seq"])
	}
	if segMap["docs_key"] != "vex/namespaces/test-ns/index/segments/seg_01H123/docs.col.zst" {
		t.Errorf("docs_key mismatch: %v", segMap["docs_key"])
	}
	if segMap["vectors_key"] != "vex/namespaces/test-ns/index/segments/seg_01H123/vectors.ivf.zst" {
		t.Errorf("vectors_key mismatch: %v", segMap["vectors_key"])
	}

	filterKeys := segMap["filter_keys"].([]interface{})
	if len(filterKeys) != 2 {
		t.Errorf("expected 2 filter_keys, got %d", len(filterKeys))
	}

	segStats := segMap["stats"].(map[string]interface{})
	if int(segStats["row_count"].(float64)) != 1000 {
		t.Errorf("expected row_count 1000, got %v", segStats["row_count"])
	}
}

func TestStats(t *testing.T) {
	m := NewManifest("test-ns")

	// Add segments with various stats
	m.AddSegment(Segment{
		ID:          "seg_001",
		Level:       0,
		StartWALSeq: 1,
		EndWALSeq:   50,
		Stats: SegmentStats{
			RowCount:     500,
			LogicalBytes: 25000,
		},
		CreatedAt: time.Now().UTC(),
	})

	m.AddSegment(Segment{
		ID:          "seg_002",
		Level:       0,
		StartWALSeq: 51,
		EndWALSeq:   100,
		Stats: SegmentStats{
			RowCount:     300,
			LogicalBytes: 15000,
		},
		CreatedAt: time.Now().UTC(),
	})

	m.AddSegment(Segment{
		ID:          "seg_003",
		Level:       1,
		StartWALSeq: 1,
		EndWALSeq:   100,
		Stats: SegmentStats{
			RowCount:     700,
			LogicalBytes: 35000,
		},
		CreatedAt: time.Now().UTC(),
	})

	// Check aggregate stats
	if m.Stats.ApproxRowCount != 1500 {
		t.Errorf("expected approx_row_count 1500, got %d", m.Stats.ApproxRowCount)
	}
	if m.Stats.ApproxLogicalBytes != 75000 {
		t.Errorf("expected approx_logical_bytes 75000, got %d", m.Stats.ApproxLogicalBytes)
	}
	if m.Stats.SegmentCount != 3 {
		t.Errorf("expected segment_count 3, got %d", m.Stats.SegmentCount)
	}
	if m.Stats.L0SegmentCount != 2 {
		t.Errorf("expected l0_segment_count 2, got %d", m.Stats.L0SegmentCount)
	}

	// Verify stats in JSON
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	stats := parsed["stats"].(map[string]interface{})
	if int(stats["approx_row_count"].(float64)) != 1500 {
		t.Errorf("approx_row_count mismatch in JSON: %v", stats["approx_row_count"])
	}
	if int(stats["approx_logical_bytes"].(float64)) != 75000 {
		t.Errorf("approx_logical_bytes mismatch in JSON: %v", stats["approx_logical_bytes"])
	}
}

func TestManifestValidation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Manifest)
		wantErr string
	}{
		{
			name:    "valid manifest",
			modify:  func(m *Manifest) {},
			wantErr: "",
		},
		{
			name: "missing format_version",
			modify: func(m *Manifest) {
				m.FormatVersion = 0
			},
			wantErr: "invalid format_version",
		},
		{
			name: "missing namespace",
			modify: func(m *Manifest) {
				m.Namespace = ""
			},
			wantErr: "namespace is required",
		},
		{
			name: "missing generated_at",
			modify: func(m *Manifest) {
				m.GeneratedAt = time.Time{}
			},
			wantErr: "generated_at is required",
		},
		{
			name: "segment missing id",
			modify: func(m *Manifest) {
				m.Segments = []Segment{{Level: 0, EndWALSeq: 100}}
			},
			wantErr: "id is required",
		},
		{
			name: "duplicate segment id",
			modify: func(m *Manifest) {
				m.Segments = []Segment{
					{ID: "seg_001", EndWALSeq: 50},
					{ID: "seg_001", EndWALSeq: 100},
				}
			},
			wantErr: "duplicate id",
		},
		{
			name: "invalid wal seq range",
			modify: func(m *Manifest) {
				m.Segments = []Segment{{ID: "seg_001", StartWALSeq: 100, EndWALSeq: 50}}
			},
			wantErr: "start_wal_seq (100) > end_wal_seq (50)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewManifest("test-ns")
			tt.modify(m)
			err := m.Validate()

			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.wantErr)
				} else if !containsStr(err.Error(), tt.wantErr) {
					t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
				}
			}
		})
	}
}

func TestManifestClone(t *testing.T) {
	m := NewManifest("test-ns")
	m.IndexedWALSeq = 100
	m.AddSegment(Segment{
		ID:          "seg_001",
		Level:       0,
		StartWALSeq: 1,
		EndWALSeq:   100,
		Stats:       SegmentStats{RowCount: 500},
	})

	clone := m.Clone()
	if clone == nil {
		t.Fatal("clone returned nil")
	}

	// Modify original
	m.IndexedWALSeq = 200
	m.Segments[0].Stats.RowCount = 1000

	// Clone should be unchanged
	if clone.IndexedWALSeq != 100 {
		t.Errorf("clone indexed_wal_seq changed: %d", clone.IndexedWALSeq)
	}
	if clone.Segments[0].Stats.RowCount != 500 {
		t.Errorf("clone segment stats changed: %d", clone.Segments[0].Stats.RowCount)
	}
}

func TestSegmentOperations(t *testing.T) {
	m := NewManifest("test-ns")

	// Add segments
	m.AddSegment(Segment{ID: "seg_001", Level: 0, EndWALSeq: 50})
	m.AddSegment(Segment{ID: "seg_002", Level: 0, EndWALSeq: 100})
	m.AddSegment(Segment{ID: "seg_003", Level: 1, EndWALSeq: 100})

	// GetSegment
	seg := m.GetSegment("seg_002")
	if seg == nil || seg.ID != "seg_002" {
		t.Error("GetSegment failed")
	}

	// GetSegment for non-existent
	if m.GetSegment("seg_999") != nil {
		t.Error("GetSegment should return nil for non-existent")
	}

	// GetSegmentsByLevel
	l0 := m.GetSegmentsByLevel(0)
	if len(l0) != 2 {
		t.Errorf("expected 2 L0 segments, got %d", len(l0))
	}
	l1 := m.GetSegmentsByLevel(1)
	if len(l1) != 1 {
		t.Errorf("expected 1 L1 segment, got %d", len(l1))
	}

	// RemoveSegment
	if !m.RemoveSegment("seg_001") {
		t.Error("RemoveSegment should return true")
	}
	if len(m.Segments) != 2 {
		t.Errorf("expected 2 segments after removal, got %d", len(m.Segments))
	}
	if m.RemoveSegment("seg_001") {
		t.Error("RemoveSegment should return false for already removed")
	}
}

func TestSortSegmentsByWALSeq(t *testing.T) {
	m := NewManifest("test-ns")
	m.AddSegment(Segment{ID: "seg_001", EndWALSeq: 50})
	m.AddSegment(Segment{ID: "seg_002", EndWALSeq: 150})
	m.AddSegment(Segment{ID: "seg_003", EndWALSeq: 100})

	m.SortSegmentsByWALSeq()

	// Should be sorted by EndWALSeq descending (newest first)
	if m.Segments[0].ID != "seg_002" {
		t.Errorf("expected seg_002 first, got %s", m.Segments[0].ID)
	}
	if m.Segments[1].ID != "seg_003" {
		t.Errorf("expected seg_003 second, got %s", m.Segments[1].ID)
	}
	if m.Segments[2].ID != "seg_001" {
		t.Errorf("expected seg_001 third, got %s", m.Segments[2].ID)
	}
}

func TestAllObjectKeys(t *testing.T) {
	m := NewManifest("test-ns")
	m.AddSegment(Segment{
		ID:         "seg_001",
		DocsKey:    "docs1.col.zst",
		VectorsKey: "vectors1.ivf.zst",
		FilterKeys: []string{"filter1.bitmap", "filter2.bitmap"},
		FTSKeys:    []string{"fts.title.bm25"},
	})
	m.AddSegment(Segment{
		ID:         "seg_002",
		DocsKey:    "docs2.col.zst",
		FilterKeys: []string{"filter3.bitmap"},
	})

	keys := m.AllObjectKeys()
	expected := []string{
		"docs1.col.zst",
		"vectors1.ivf.zst",
		"filter1.bitmap",
		"filter2.bitmap",
		"fts.title.bm25",
		"docs2.col.zst",
		"filter3.bitmap",
	}

	if len(keys) != len(expected) {
		t.Errorf("expected %d keys, got %d", len(expected), len(keys))
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	for _, e := range expected {
		if !keySet[e] {
			t.Errorf("missing expected key: %s", e)
		}
	}
}

func TestManifestKey(t *testing.T) {
	key := ManifestKey("my-namespace", 678)
	expected := "vex/namespaces/my-namespace/index/manifests/00000000000000000678.idx.json"
	if key != expected {
		t.Errorf("expected %q, got %q", expected, key)
	}
}

func TestSegmentKey(t *testing.T) {
	key := SegmentKey("my-namespace", "seg_01H123")
	expected := "vex/namespaces/my-namespace/index/segments/seg_01H123"
	if key != expected {
		t.Errorf("expected %q, got %q", expected, key)
	}
}

func TestSchemaHash(t *testing.T) {
	schema1 := map[string]interface{}{
		"title": map[string]interface{}{"type": "string"},
		"count": map[string]interface{}{"type": "int"},
	}
	schema2 := map[string]interface{}{
		"title": map[string]interface{}{"type": "string"},
		"count": map[string]interface{}{"type": "int"},
	}
	schema3 := map[string]interface{}{
		"title": map[string]interface{}{"type": "string"},
		"count": map[string]interface{}{"type": "uint"},
	}

	hash1, err := ComputeSchemaHash(schema1)
	if err != nil {
		t.Fatalf("failed to compute hash: %v", err)
	}
	hash2, err := ComputeSchemaHash(schema2)
	if err != nil {
		t.Fatalf("failed to compute hash: %v", err)
	}
	hash3, err := ComputeSchemaHash(schema3)
	if err != nil {
		t.Fatalf("failed to compute hash: %v", err)
	}

	// Same schema should have same hash
	if hash1 != hash2 {
		t.Errorf("same schema should have same hash: %s != %s", hash1, hash2)
	}

	// Different schema should have different hash
	if hash1 == hash3 {
		t.Errorf("different schema should have different hash: %s == %s", hash1, hash3)
	}

	// Hash should be non-empty
	if len(hash1) == 0 {
		t.Error("hash should not be empty")
	}
}

func TestManifestRoundtrip(t *testing.T) {
	m := NewManifest("test-namespace")
	m.IndexedWALSeq = 12345
	m.SchemaVersionHash = "abc123def"
	m.AddSegment(Segment{
		ID:          "seg_01H_TEST",
		Level:       0,
		StartWALSeq: 1,
		EndWALSeq:   100,
		DocsKey:     "path/to/docs.col.zst",
		VectorsKey:  "path/to/vectors.ivf.zst",
		FilterKeys:  []string{"path/to/filter1.bitmap"},
		FTSKeys:     []string{"path/to/fts.title.bm25"},
		Stats: SegmentStats{
			RowCount:       5000,
			LogicalBytes:   250000,
			TombstoneCount: 50,
		},
		CreatedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	})
	m.AddSegment(Segment{
		ID:          "seg_02H_TEST",
		Level:       1,
		StartWALSeq: 1,
		EndWALSeq:   100,
		Stats: SegmentStats{
			RowCount:     4500,
			LogicalBytes: 200000,
		},
	})

	// Serialize
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	// Deserialize
	var m2 Manifest
	if err := json.Unmarshal(data, &m2); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Validate roundtrip
	if m2.FormatVersion != m.FormatVersion {
		t.Errorf("format_version mismatch")
	}
	if m2.Namespace != m.Namespace {
		t.Errorf("namespace mismatch")
	}
	if m2.IndexedWALSeq != m.IndexedWALSeq {
		t.Errorf("indexed_wal_seq mismatch")
	}
	if m2.SchemaVersionHash != m.SchemaVersionHash {
		t.Errorf("schema_version_hash mismatch")
	}
	if len(m2.Segments) != len(m.Segments) {
		t.Errorf("segments count mismatch")
	}
	if m2.Stats.ApproxRowCount != m.Stats.ApproxRowCount {
		t.Errorf("stats.approx_row_count mismatch")
	}
	if m2.Stats.ApproxLogicalBytes != m.Stats.ApproxLogicalBytes {
		t.Errorf("stats.approx_logical_bytes mismatch")
	}

	// Validate the roundtripped manifest
	if err := m2.Validate(); err != nil {
		t.Errorf("roundtripped manifest invalid: %v", err)
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && (s[:len(substr)] == substr || containsStr(s[1:], substr)))
}
