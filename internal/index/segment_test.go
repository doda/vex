package index

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestSegmentBuilder_Basic(t *testing.T) {
	builder, err := NewSegmentBuilder("seg_001", 1, 100)
	if err != nil {
		t.Fatalf("NewSegmentBuilder failed: %v", err)
	}

	if builder.IsSealed() {
		t.Error("builder should not be sealed initially")
	}

	seg, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if seg.ID != "seg_001" {
		t.Errorf("expected ID seg_001, got %s", seg.ID)
	}
	if seg.StartWALSeq != 1 {
		t.Errorf("expected StartWALSeq 1, got %d", seg.StartWALSeq)
	}
	if seg.EndWALSeq != 100 {
		t.Errorf("expected EndWALSeq 100, got %d", seg.EndWALSeq)
	}
	if seg.Level != 0 {
		t.Errorf("expected Level 0, got %d", seg.Level)
	}
	if !builder.IsSealed() {
		t.Error("builder should be sealed after Build()")
	}
}

func TestSegmentBuilder_InvalidWALRange(t *testing.T) {
	_, err := NewSegmentBuilder("seg_001", 100, 50)
	if err == nil {
		t.Error("expected error for invalid WAL range")
	}
	if !errors.Is(err, ErrInvalidWALRange) {
		t.Errorf("expected ErrInvalidWALRange, got %v", err)
	}
}

func TestSegmentBuilder_MissingID(t *testing.T) {
	_, err := NewSegmentBuilder("", 1, 100)
	if err == nil {
		t.Error("expected error for missing ID")
	}
	if !errors.Is(err, ErrMissingSegmentID) {
		t.Errorf("expected ErrMissingSegmentID, got %v", err)
	}
}

func TestSegmentBuilder_SetAllFields(t *testing.T) {
	builder, err := NewSegmentBuilder("seg_full", 10, 50)
	if err != nil {
		t.Fatalf("NewSegmentBuilder failed: %v", err)
	}

	if err := builder.SetLevel(1); err != nil {
		t.Fatalf("SetLevel failed: %v", err)
	}
	if err := builder.SetDocsKey("ns/segments/seg_full/docs.col.zst"); err != nil {
		t.Fatalf("SetDocsKey failed: %v", err)
	}
	if err := builder.SetVectorsKey("ns/segments/seg_full/vectors.ivf.zst"); err != nil {
		t.Fatalf("SetVectorsKey failed: %v", err)
	}
	if err := builder.SetFilterKeys([]string{"ns/filters/status.bitmap", "ns/filters/type.bitmap"}); err != nil {
		t.Fatalf("SetFilterKeys failed: %v", err)
	}
	if err := builder.SetStats(SegmentStats{RowCount: 1000, LogicalBytes: 50000}); err != nil {
		t.Fatalf("SetStats failed: %v", err)
	}

	seg, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if seg.Level != 1 {
		t.Errorf("expected Level 1, got %d", seg.Level)
	}
	if seg.DocsKey != "ns/segments/seg_full/docs.col.zst" {
		t.Errorf("DocsKey mismatch: %s", seg.DocsKey)
	}
	if seg.VectorsKey != "ns/segments/seg_full/vectors.ivf.zst" {
		t.Errorf("VectorsKey mismatch: %s", seg.VectorsKey)
	}
	if len(seg.FilterKeys) != 2 {
		t.Errorf("expected 2 FilterKeys, got %d", len(seg.FilterKeys))
	}
	if seg.Stats.RowCount != 1000 {
		t.Errorf("expected RowCount 1000, got %d", seg.Stats.RowCount)
	}
}

// TestSegmentsAreImmutableOnceWritten verifies that segments cannot be modified after Build().
func TestSegmentsAreImmutableOnceWritten(t *testing.T) {
	builder, err := NewSegmentBuilder("seg_immutable", 1, 100)
	if err != nil {
		t.Fatalf("NewSegmentBuilder failed: %v", err)
	}

	if err := builder.SetDocsKey("path/to/docs.col.zst"); err != nil {
		t.Fatalf("SetDocsKey failed: %v", err)
	}
	if err := builder.SetVectorsKey("path/to/vectors.ivf.zst"); err != nil {
		t.Fatalf("SetVectorsKey failed: %v", err)
	}
	if err := builder.AddFilterKey("path/to/filter1.bitmap"); err != nil {
		t.Fatalf("AddFilterKey failed: %v", err)
	}

	// Build the segment (seals it)
	seg, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Verify segment is sealed
	if !builder.IsSealed() {
		t.Error("builder should be sealed after Build()")
	}

	// Attempt to modify after sealing - all should fail with ErrSegmentSealed
	tests := []struct {
		name string
		fn   func() error
	}{
		{"SetLevel", func() error { return builder.SetLevel(2) }},
		{"SetDocsKey", func() error { return builder.SetDocsKey("new/docs.col.zst") }},
		{"SetVectorsKey", func() error { return builder.SetVectorsKey("new/vectors.ivf.zst") }},
		{"AddFilterKey", func() error { return builder.AddFilterKey("new/filter.bitmap") }},
		{"SetFilterKeys", func() error { return builder.SetFilterKeys([]string{"new.bitmap"}) }},
		{"AddFTSKey", func() error { return builder.AddFTSKey("new/fts.title.bm25") }},
		{"SetFTSKeys", func() error { return builder.SetFTSKeys([]string{"new.bm25"}) }},
		{"SetStats", func() error { return builder.SetStats(SegmentStats{RowCount: 9999}) }},
		{"SetCreatedAt", func() error { return builder.SetCreatedAt(time.Now()) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if err == nil {
				t.Errorf("%s: expected error after segment sealed", tt.name)
			}
			if !errors.Is(err, ErrSegmentSealed) {
				t.Errorf("%s: expected ErrSegmentSealed, got %v", tt.name, err)
			}
		})
	}

	// Verify the original segment values are unchanged
	if seg.DocsKey != "path/to/docs.col.zst" {
		t.Error("segment DocsKey was modified")
	}
	if seg.VectorsKey != "path/to/vectors.ivf.zst" {
		t.Error("segment VectorsKey was modified")
	}
	if len(seg.FilterKeys) != 1 {
		t.Error("segment FilterKeys was modified")
	}

	// Calling Build() again should return the same segment
	seg2, err := builder.Build()
	if err != nil {
		t.Fatalf("second Build() failed: %v", err)
	}
	if seg2.ID != seg.ID {
		t.Error("second Build() returned different segment")
	}
}

// TestSegmentCoversWALSequenceInterval verifies WAL sequence range tracking.
func TestSegmentCoversWALSequenceInterval(t *testing.T) {
	testCases := []struct {
		name     string
		startSeq uint64
		endSeq   uint64
		testSeqs []uint64
		expected []bool
	}{
		{
			name:     "basic range",
			startSeq: 10,
			endSeq:   50,
			testSeqs: []uint64{5, 9, 10, 30, 50, 51, 100},
			expected: []bool{false, false, true, true, true, false, false},
		},
		{
			name:     "single entry",
			startSeq: 100,
			endSeq:   100,
			testSeqs: []uint64{99, 100, 101},
			expected: []bool{false, true, false},
		},
		{
			name:     "from zero",
			startSeq: 0,
			endSeq:   10,
			testSeqs: []uint64{0, 5, 10, 11},
			expected: []bool{true, true, true, false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder, err := NewSegmentBuilder("seg_test", tc.startSeq, tc.endSeq)
			if err != nil {
				t.Fatalf("NewSegmentBuilder failed: %v", err)
			}

			seg, err := builder.Build()
			if err != nil {
				t.Fatalf("Build failed: %v", err)
			}

			immutable, err := NewImmutableSegment(seg)
			if err != nil {
				t.Fatalf("NewImmutableSegment failed: %v", err)
			}

			// Verify WAL range accessors
			start, end := immutable.WALRange()
			if start != tc.startSeq {
				t.Errorf("WALRange start: expected %d, got %d", tc.startSeq, start)
			}
			if end != tc.endSeq {
				t.Errorf("WALRange end: expected %d, got %d", tc.endSeq, end)
			}

			// Test coverage for each sequence
			for i, seq := range tc.testSeqs {
				covered := immutable.CoversWALSeq(seq)
				if covered != tc.expected[i] {
					t.Errorf("CoversWALSeq(%d): expected %v, got %v", seq, tc.expected[i], covered)
				}
			}
		})
	}
}

// TestSegmentIncludesDocsVectorsFilterKeys verifies segment contains all required keys.
func TestSegmentIncludesDocsVectorsFilterKeys(t *testing.T) {
	builder, err := NewSegmentBuilder("seg_keys", 1, 100)
	if err != nil {
		t.Fatalf("NewSegmentBuilder failed: %v", err)
	}

	// Set all key types
	docsKey := "vex/namespaces/test-ns/index/segments/seg_keys/docs.col.zst"
	vectorsKey := "vex/namespaces/test-ns/index/segments/seg_keys/vectors.ivf.zst"
	filterKeys := []string{
		"vex/namespaces/test-ns/index/segments/seg_keys/filters/status.bitmap",
		"vex/namespaces/test-ns/index/segments/seg_keys/filters/category.bitmap",
		"vex/namespaces/test-ns/index/segments/seg_keys/filters/priority.bitmap",
	}
	ftsKeys := []string{
		"vex/namespaces/test-ns/index/segments/seg_keys/fts.title.bm25",
	}

	if err := builder.SetDocsKey(docsKey); err != nil {
		t.Fatalf("SetDocsKey failed: %v", err)
	}
	if err := builder.SetVectorsKey(vectorsKey); err != nil {
		t.Fatalf("SetVectorsKey failed: %v", err)
	}
	if err := builder.SetFilterKeys(filterKeys); err != nil {
		t.Fatalf("SetFilterKeys failed: %v", err)
	}
	if err := builder.SetFTSKeys(ftsKeys); err != nil {
		t.Fatalf("SetFTSKeys failed: %v", err)
	}

	seg, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Verify docs_key
	if seg.DocsKey != docsKey {
		t.Errorf("docs_key mismatch: expected %s, got %s", docsKey, seg.DocsKey)
	}

	// Verify vectors_key
	if seg.VectorsKey != vectorsKey {
		t.Errorf("vectors_key mismatch: expected %s, got %s", vectorsKey, seg.VectorsKey)
	}

	// Verify filter_keys
	if len(seg.FilterKeys) != len(filterKeys) {
		t.Errorf("filter_keys count mismatch: expected %d, got %d", len(filterKeys), len(seg.FilterKeys))
	}
	for i, key := range filterKeys {
		if seg.FilterKeys[i] != key {
			t.Errorf("filter_keys[%d] mismatch: expected %s, got %s", i, key, seg.FilterKeys[i])
		}
	}

	// Verify fts_keys
	if len(seg.FTSKeys) != len(ftsKeys) {
		t.Errorf("fts_keys count mismatch: expected %d, got %d", len(ftsKeys), len(seg.FTSKeys))
	}
}

func TestImmutableSegment_ReturnsCopies(t *testing.T) {
	builder, err := NewSegmentBuilder("seg_copy", 1, 100)
	if err != nil {
		t.Fatalf("NewSegmentBuilder failed: %v", err)
	}

	if err := builder.SetFilterKeys([]string{"filter1.bitmap", "filter2.bitmap"}); err != nil {
		t.Fatalf("SetFilterKeys failed: %v", err)
	}

	seg, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	immutable, err := NewImmutableSegment(seg)
	if err != nil {
		t.Fatalf("NewImmutableSegment failed: %v", err)
	}

	// Get filter keys and modify the returned slice
	keys := immutable.FilterKeys()
	keys[0] = "modified.bitmap"

	// Verify the original is unchanged
	keysAgain := immutable.FilterKeys()
	if keysAgain[0] == "modified.bitmap" {
		t.Error("modifying returned FilterKeys slice affected the immutable segment")
	}

	// Same for Segment() copy
	segCopy := immutable.Segment()
	segCopy.DocsKey = "modified"
	segCopy.FilterKeys[0] = "modified.bitmap"

	// Verify original unchanged
	segAgain := immutable.Segment()
	if segAgain.DocsKey == "modified" {
		t.Error("modifying Segment() copy affected the immutable segment")
	}
}

func TestImmutableSegment_AllObjectKeys(t *testing.T) {
	builder, err := NewSegmentBuilder("seg_allkeys", 1, 100)
	if err != nil {
		t.Fatalf("NewSegmentBuilder failed: %v", err)
	}

	if err := builder.SetDocsKey("docs.col.zst"); err != nil {
		t.Fatalf("SetDocsKey failed: %v", err)
	}
	if err := builder.SetVectorsKey("vectors.ivf.zst"); err != nil {
		t.Fatalf("SetVectorsKey failed: %v", err)
	}
	if err := builder.SetFilterKeys([]string{"filter1.bitmap", "filter2.bitmap"}); err != nil {
		t.Fatalf("SetFilterKeys failed: %v", err)
	}
	if err := builder.SetFTSKeys([]string{"fts.title.bm25"}); err != nil {
		t.Fatalf("SetFTSKeys failed: %v", err)
	}

	seg, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	immutable, err := NewImmutableSegment(seg)
	if err != nil {
		t.Fatalf("NewImmutableSegment failed: %v", err)
	}

	keys := immutable.AllObjectKeys()

	expected := map[string]bool{
		"docs.col.zst":    true,
		"vectors.ivf.zst": true,
		"filter1.bitmap":  true,
		"filter2.bitmap":  true,
		"fts.title.bm25":  true,
	}

	if len(keys) != len(expected) {
		t.Errorf("expected %d keys, got %d", len(expected), len(keys))
	}

	for _, key := range keys {
		if !expected[key] {
			t.Errorf("unexpected key: %s", key)
		}
	}
}

func TestSegmentBuilder_AddFilterKeyIncrementally(t *testing.T) {
	builder, err := NewSegmentBuilder("seg_add", 1, 100)
	if err != nil {
		t.Fatalf("NewSegmentBuilder failed: %v", err)
	}

	if err := builder.AddFilterKey("filter1.bitmap"); err != nil {
		t.Fatalf("AddFilterKey 1 failed: %v", err)
	}
	if err := builder.AddFilterKey("filter2.bitmap"); err != nil {
		t.Fatalf("AddFilterKey 2 failed: %v", err)
	}
	if err := builder.AddFilterKey("filter3.bitmap"); err != nil {
		t.Fatalf("AddFilterKey 3 failed: %v", err)
	}

	seg, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if len(seg.FilterKeys) != 3 {
		t.Errorf("expected 3 filter keys, got %d", len(seg.FilterKeys))
	}
	if seg.FilterKeys[0] != "filter1.bitmap" {
		t.Errorf("filter key 0 mismatch: %s", seg.FilterKeys[0])
	}
	if seg.FilterKeys[1] != "filter2.bitmap" {
		t.Errorf("filter key 1 mismatch: %s", seg.FilterKeys[1])
	}
	if seg.FilterKeys[2] != "filter3.bitmap" {
		t.Errorf("filter key 2 mismatch: %s", seg.FilterKeys[2])
	}
}

func TestValidateSegment(t *testing.T) {
	tests := []struct {
		name    string
		segment Segment
		wantErr error
	}{
		{
			name: "valid segment",
			segment: Segment{
				ID:          "seg_valid",
				StartWALSeq: 1,
				EndWALSeq:   100,
			},
			wantErr: nil,
		},
		{
			name: "missing ID",
			segment: Segment{
				StartWALSeq: 1,
				EndWALSeq:   100,
			},
			wantErr: ErrMissingSegmentID,
		},
		{
			name: "invalid WAL range",
			segment: Segment{
				ID:          "seg_invalid",
				StartWALSeq: 100,
				EndWALSeq:   50,
			},
			wantErr: ErrInvalidWALRange,
		},
		{
			name: "equal WAL seq is valid",
			segment: Segment{
				ID:          "seg_single",
				StartWALSeq: 50,
				EndWALSeq:   50,
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSegment(tt.segment)
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			} else {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !errors.Is(err, tt.wantErr) {
					t.Errorf("expected error %v, got %v", tt.wantErr, err)
				}
			}
		})
	}
}

func TestSegmentObjectKeyHelpers(t *testing.T) {
	namespace := "my-namespace"
	segmentID := "seg_01H123"

	docsKey := DocsObjectKey(namespace, segmentID)
	expected := "vex/namespaces/my-namespace/index/segments/seg_01H123/docs.col.zst"
	if docsKey != expected {
		t.Errorf("DocsObjectKey: expected %s, got %s", expected, docsKey)
	}

	vectorsKey := VectorsObjectKey(namespace, segmentID)
	expected = "vex/namespaces/my-namespace/index/segments/seg_01H123/vectors.ivf.zst"
	if vectorsKey != expected {
		t.Errorf("VectorsObjectKey: expected %s, got %s", expected, vectorsKey)
	}

	filterKey := FilterObjectKey(namespace, segmentID, "status")
	expected = "vex/namespaces/my-namespace/index/segments/seg_01H123/filters/status.bitmap"
	if filterKey != expected {
		t.Errorf("FilterObjectKey: expected %s, got %s", expected, filterKey)
	}

	ftsKey := FTSObjectKey(namespace, segmentID, "title")
	expected = "vex/namespaces/my-namespace/index/segments/seg_01H123/fts.title.bm25"
	if ftsKey != expected {
		t.Errorf("FTSObjectKey: expected %s, got %s", expected, ftsKey)
	}
}

func TestGenerateSegmentID(t *testing.T) {
	id1 := GenerateSegmentID()
	id2 := GenerateSegmentID()

	if id1 == "" {
		t.Error("GenerateSegmentID returned empty string")
	}
	if id1 == id2 {
		t.Error("GenerateSegmentID should return unique IDs")
	}
	if len(id1) < 5 {
		t.Error("GenerateSegmentID should return reasonable length ID")
	}
}

// mockObjectStore is a simple mock for testing SegmentWriter that implements objectstore.Store
type mockObjectStore struct {
	objects map[string][]byte
}

func newMockObjectStore() *mockObjectStore {
	return &mockObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *mockObjectStore) Get(_ context.Context, key string, _ *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), &objectstore.ObjectInfo{Key: key, Size: int64(len(data)), ETag: "etag-" + key}, nil
}

func (m *mockObjectStore) Head(_ context.Context, key string) (*objectstore.ObjectInfo, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return &objectstore.ObjectInfo{Key: key, Size: int64(len(data)), ETag: "etag-" + key}, nil
}

func (m *mockObjectStore) Put(_ context.Context, key string, body io.Reader, _ int64, _ *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	m.objects[key] = data
	return &objectstore.ObjectInfo{Key: key, Size: int64(len(data)), ETag: "etag-" + key}, nil
}

func (m *mockObjectStore) PutIfAbsent(_ context.Context, key string, body io.Reader, _ int64, _ *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if _, ok := m.objects[key]; ok {
		return nil, objectstore.ErrAlreadyExists
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	m.objects[key] = data
	return &objectstore.ObjectInfo{Key: key, Size: int64(len(data)), ETag: "etag-" + key}, nil
}

func (m *mockObjectStore) PutIfMatch(_ context.Context, key string, body io.Reader, _ int64, _ string, _ *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	m.objects[key] = data
	return &objectstore.ObjectInfo{Key: key, Size: int64(len(data)), ETag: "etag-" + key}, nil
}

func (m *mockObjectStore) List(_ context.Context, _ *objectstore.ListOptions) (*objectstore.ListResult, error) {
	return &objectstore.ListResult{}, nil
}

func (m *mockObjectStore) Delete(_ context.Context, key string) error {
	delete(m.objects, key)
	return nil
}

func TestSegmentWriter_Basic(t *testing.T) {
	store := newMockObjectStore()
	writer := NewSegmentWriter(store, "test-ns", "seg_001")

	ctx := context.Background()

	// Write docs data
	docsKey, err := writer.WriteDocsData(ctx, []byte("docs data"))
	if err != nil {
		t.Fatalf("WriteDocsData failed: %v", err)
	}
	if docsKey == "" {
		t.Error("WriteDocsData returned empty key")
	}

	// Write vectors data
	vectorsKey, err := writer.WriteVectorsData(ctx, []byte("vectors data"))
	if err != nil {
		t.Fatalf("WriteVectorsData failed: %v", err)
	}
	if vectorsKey == "" {
		t.Error("WriteVectorsData returned empty key")
	}

	// Write filter data
	filterKey, err := writer.WriteFilterData(ctx, "status", []byte("filter data"))
	if err != nil {
		t.Fatalf("WriteFilterData failed: %v", err)
	}
	if filterKey == "" {
		t.Error("WriteFilterData returned empty key")
	}

	// Write FTS data
	ftsKey, err := writer.WriteFTSData(ctx, "title", []byte("fts data"))
	if err != nil {
		t.Fatalf("WriteFTSData failed: %v", err)
	}
	if ftsKey == "" {
		t.Error("WriteFTSData returned empty key")
	}

	// Check written keys
	written := writer.WrittenKeys()
	if len(written) != 4 {
		t.Errorf("expected 4 written keys, got %d", len(written))
	}
	if _, ok := written[docsKey]; !ok {
		t.Errorf("docsKey not in written keys: %s", docsKey)
	}
	if _, ok := written[vectorsKey]; !ok {
		t.Errorf("vectorsKey not in written keys: %s", vectorsKey)
	}
}

func TestSegmentWriter_SealPreventsWrites(t *testing.T) {
	store := newMockObjectStore()
	writer := NewSegmentWriter(store, "test-ns", "seg_sealed")

	ctx := context.Background()

	// Write something before sealing
	_, err := writer.WriteDocsData(ctx, []byte("docs data"))
	if err != nil {
		t.Fatalf("WriteDocsData failed: %v", err)
	}

	// Seal the writer
	writer.Seal()

	if !writer.IsSealed() {
		t.Error("writer should be sealed")
	}

	// All writes should fail after sealing
	_, err = writer.WriteDocsData(ctx, []byte("more docs"))
	if !errors.Is(err, ErrSegmentSealed) {
		t.Errorf("expected ErrSegmentSealed, got %v", err)
	}

	_, err = writer.WriteVectorsData(ctx, []byte("vectors"))
	if !errors.Is(err, ErrSegmentSealed) {
		t.Errorf("expected ErrSegmentSealed, got %v", err)
	}

	_, err = writer.WriteFilterData(ctx, "attr", []byte("filter"))
	if !errors.Is(err, ErrSegmentSealed) {
		t.Errorf("expected ErrSegmentSealed, got %v", err)
	}

	_, err = writer.WriteFTSData(ctx, "attr", []byte("fts"))
	if !errors.Is(err, ErrSegmentSealed) {
		t.Errorf("expected ErrSegmentSealed, got %v", err)
	}
}

func TestSegmentBuilder_SetCreatedAt(t *testing.T) {
	builder, err := NewSegmentBuilder("seg_time", 1, 100)
	if err != nil {
		t.Fatalf("NewSegmentBuilder failed: %v", err)
	}

	customTime := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	if err := builder.SetCreatedAt(customTime); err != nil {
		t.Fatalf("SetCreatedAt failed: %v", err)
	}

	seg, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !seg.CreatedAt.Equal(customTime) {
		t.Errorf("CreatedAt mismatch: expected %v, got %v", customTime, seg.CreatedAt)
	}
}

func TestImmutableSegment_Accessors(t *testing.T) {
	builder, err := NewSegmentBuilder("seg_access", 10, 50)
	if err != nil {
		t.Fatalf("NewSegmentBuilder failed: %v", err)
	}

	if err := builder.SetLevel(2); err != nil {
		t.Fatalf("SetLevel failed: %v", err)
	}
	if err := builder.SetDocsKey("docs.col.zst"); err != nil {
		t.Fatalf("SetDocsKey failed: %v", err)
	}
	if err := builder.SetVectorsKey("vectors.ivf.zst"); err != nil {
		t.Fatalf("SetVectorsKey failed: %v", err)
	}
	if err := builder.SetStats(SegmentStats{RowCount: 500, LogicalBytes: 25000, TombstoneCount: 5}); err != nil {
		t.Fatalf("SetStats failed: %v", err)
	}

	customTime := time.Date(2024, 3, 20, 9, 0, 0, 0, time.UTC)
	if err := builder.SetCreatedAt(customTime); err != nil {
		t.Fatalf("SetCreatedAt failed: %v", err)
	}

	seg, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	immutable, err := NewImmutableSegment(seg)
	if err != nil {
		t.Fatalf("NewImmutableSegment failed: %v", err)
	}

	if immutable.ID() != "seg_access" {
		t.Errorf("ID mismatch: %s", immutable.ID())
	}
	if immutable.Level() != 2 {
		t.Errorf("Level mismatch: %d", immutable.Level())
	}
	if immutable.DocsKey() != "docs.col.zst" {
		t.Errorf("DocsKey mismatch: %s", immutable.DocsKey())
	}
	if immutable.VectorsKey() != "vectors.ivf.zst" {
		t.Errorf("VectorsKey mismatch: %s", immutable.VectorsKey())
	}

	stats := immutable.Stats()
	if stats.RowCount != 500 {
		t.Errorf("Stats.RowCount mismatch: %d", stats.RowCount)
	}
	if stats.TombstoneCount != 5 {
		t.Errorf("Stats.TombstoneCount mismatch: %d", stats.TombstoneCount)
	}

	if !immutable.CreatedAt().Equal(customTime) {
		t.Errorf("CreatedAt mismatch: %v", immutable.CreatedAt())
	}
}

func TestImmutableSegment_NilSlices(t *testing.T) {
	seg := Segment{
		ID:          "seg_nil",
		StartWALSeq: 1,
		EndWALSeq:   100,
		// FilterKeys and FTSKeys are nil
	}

	immutable, err := NewImmutableSegment(seg)
	if err != nil {
		t.Fatalf("NewImmutableSegment failed: %v", err)
	}

	if immutable.FilterKeys() != nil {
		t.Error("FilterKeys should return nil for nil slice")
	}
	if immutable.FTSKeys() != nil {
		t.Error("FTSKeys should return nil for nil slice")
	}
}

func TestSegmentMetadata_JSON(t *testing.T) {
	seg := Segment{
		ID:          "seg_meta",
		Level:       0,
		StartWALSeq: 1,
		EndWALSeq:   100,
		DocsKey:     "docs.col.zst",
		VectorsKey:  "vectors.ivf.zst",
	}

	meta := SegmentMetadata{
		Segment: seg,
		ObjectMap: map[string]string{
			"docs.col.zst":    "etag1",
			"vectors.ivf.zst": "etag2",
		},
		SealedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	data, err := meta.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	var decoded SegmentMetadata
	if err := decoded.UnmarshalJSON(data); err != nil {
		t.Fatalf("UnmarshalJSON failed: %v", err)
	}

	if decoded.Segment.ID != "seg_meta" {
		t.Errorf("Segment.ID mismatch: %s", decoded.Segment.ID)
	}
	if len(decoded.ObjectMap) != 2 {
		t.Errorf("ObjectMap length mismatch: %d", len(decoded.ObjectMap))
	}
	if decoded.ObjectMap["docs.col.zst"] != "etag1" {
		t.Errorf("ObjectMap entry mismatch")
	}
}
