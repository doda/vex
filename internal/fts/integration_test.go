package fts

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestFTSIntegration_FullFlow tests the complete FTS indexing flow:
// 1. Build FTS index from WAL entries using word_v3 tokenizer
// 2. Write FTS segments to object storage
// 3. Verify FTS index keys are correctly referenced
func TestFTSIntegration_FullFlow(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()

	// Create FTS config using default word_v3 tokenizer
	ftsConfigs := map[string]*Config{
		"title":       DefaultConfig(), // Uses word_v3 by default
		"description": DefaultConfig(),
	}

	// Verify word_v3 is the default tokenizer
	if ftsConfigs["title"].Tokenizer != "word_v3" {
		t.Fatalf("expected default tokenizer to be word_v3, got %s", ftsConfigs["title"].Tokenizer)
	}

	sb := NewSegmentBuilder("test-ns", ftsConfigs)

	// Simulate WAL entries with documents
	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req-1")

	// Add several documents
	docs := []struct {
		id    uint64
		title string
		desc  string
	}{
		{1, "Introduction to Search Engines", "Learn how search engines work"},
		{2, "Full Text Search with BM25", "Understanding BM25 ranking algorithm"},
		{3, "Tokenization Strategies", "Different approaches to text tokenization"},
		{4, "Query Processing", "How queries are processed in search systems"},
	}

	for _, doc := range docs {
		batch.AddUpsert(
			&wal.DocumentID{Id: &wal.DocumentID_U64{U64: doc.id}},
			map[string]*wal.AttributeValue{
				"title":       wal.StringValue(doc.title),
				"description": wal.StringValue(doc.desc),
			},
			nil, 0,
		)
	}

	entry.SubBatches = append(entry.SubBatches, batch)

	// Process WAL entry
	if err := sb.AddWALEntry(entry); err != nil {
		t.Fatalf("AddWALEntry error: %v", err)
	}

	if !sb.HasData() {
		t.Fatal("expected HasData() to return true")
	}

	// Write to object storage
	segmentKey := "vex/namespaces/test-ns/index/segments/seg_test"
	result, err := sb.WriteToObjectStore(ctx, store, segmentKey)
	if err != nil {
		t.Fatalf("WriteToObjectStore error: %v", err)
	}

	// Verify FTS keys are created for both attributes
	if len(result.FTSKeys) != 2 {
		t.Errorf("expected 2 FTS keys, got %d", len(result.FTSKeys))
	}

	// Verify FTS keys format
	for _, key := range result.FTSKeys {
		if !strings.HasPrefix(key, segmentKey+"/fts/") {
			t.Errorf("unexpected FTS key format: %s", key)
		}
		if !strings.HasSuffix(key, ".idx") {
			t.Errorf("expected .idx suffix for FTS key: %s", key)
		}

		// Verify objects exist in store
		reader, info, err := store.Get(ctx, key, nil)
		if err != nil {
			t.Errorf("failed to get FTS object %s: %v", key, err)
			continue
		}
		if info.Size == 0 {
			t.Errorf("FTS object %s has zero size", key)
		}
		reader.Close()
	}

	// Verify indexes were built correctly
	titleIdx := result.Indexes["title"]
	if titleIdx == nil {
		t.Fatal("expected title index to exist")
	}
	if titleIdx.TotalDocs != 4 {
		t.Errorf("title.TotalDocs = %d, want 4", titleIdx.TotalDocs)
	}

	// Verify tokenization with word_v3
	// "search" should appear in docs 1 and 2
	if !titleIdx.HasTerm("search") {
		t.Error("expected 'search' term in title index")
	}
	searchPosting := titleIdx.GetPostingList("search")
	if searchPosting.GetCardinality() != 2 {
		t.Errorf("'search' should appear in 2 docs, got %d", searchPosting.GetCardinality())
	}

	// Verify description index
	descIdx := result.Indexes["description"]
	if descIdx == nil {
		t.Fatal("expected description index to exist")
	}

	// Verify BM25 relevant data is present
	if titleIdx.AvgDocLength == 0 {
		t.Error("expected non-zero avg doc length")
	}
}

// TestFTSIntegration_ReadSerializedIndex tests that serialized indexes can be read back.
func TestFTSIntegration_ReadSerializedIndex(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()

	ftsConfigs := map[string]*Config{
		"content": DefaultConfig(),
	}

	sb := NewSegmentBuilder("test-ns", ftsConfigs)

	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req-1")

	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{
			"content": wal.StringValue("Hello world this is a test"),
		},
		nil, 0,
	)
	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 2}},
		map[string]*wal.AttributeValue{
			"content": wal.StringValue("Another document for testing search"),
		},
		nil, 0,
	)

	entry.SubBatches = append(entry.SubBatches, batch)

	if err := sb.AddWALEntry(entry); err != nil {
		t.Fatalf("AddWALEntry error: %v", err)
	}

	segmentKey := "vex/namespaces/test-ns/index/segments/seg_read_test"
	result, err := sb.WriteToObjectStore(ctx, store, segmentKey)
	if err != nil {
		t.Fatalf("WriteToObjectStore error: %v", err)
	}

	// Read back the serialized index
	if len(result.FTSKeys) == 0 {
		t.Fatal("expected FTS keys")
	}

	ftsKey := result.FTSKeys[0]
	reader, _, err := store.Get(ctx, ftsKey, nil)
	if err != nil {
		t.Fatalf("failed to get FTS object: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("failed to read FTS object: %v", err)
	}

	// Deserialize and verify
	idx, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize error: %v", err)
	}

	// Verify deserialized index matches
	if idx.TotalDocs != 2 {
		t.Errorf("TotalDocs = %d, want 2", idx.TotalDocs)
	}
	if idx.Attribute != "content" {
		t.Errorf("Attribute = %q, want 'content'", idx.Attribute)
	}
	if !idx.HasTerm("test") {
		t.Error("expected 'test' term to exist")
	}
}

// TestFTSIntegration_SegmentKeysFormat verifies the FTS key format matches segment expectations.
func TestFTSIntegration_SegmentKeysFormat(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()

	ftsConfigs := map[string]*Config{
		"body": DefaultConfig(),
	}

	sb := NewSegmentBuilder("my-namespace", ftsConfigs)

	entry := wal.NewWalEntry("my-namespace", 1)
	batch := wal.NewWriteSubBatch("req-1")
	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{
			"body": wal.StringValue("Some content"),
		},
		nil, 0,
	)
	entry.SubBatches = append(entry.SubBatches, batch)

	if err := sb.AddWALEntry(entry); err != nil {
		t.Fatalf("AddWALEntry error: %v", err)
	}

	segmentID := "seg_1234567890"
	segmentKey := "vex/namespaces/my-namespace/index/segments/" + segmentID
	result, err := sb.WriteToObjectStore(ctx, store, segmentKey)
	if err != nil {
		t.Fatalf("WriteToObjectStore error: %v", err)
	}

	// Verify key format matches the expected pattern from segment.go
	// Expected: vex/namespaces/{ns}/index/segments/{segment_id}/fts/{attr}.idx
	expectedKey := segmentKey + "/fts/body.idx"
	found := false
	for _, key := range result.FTSKeys {
		if key == expectedKey {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected FTS key %q, got %v", expectedKey, result.FTSKeys)
	}
}
