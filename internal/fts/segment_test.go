package fts

import (
	"context"
	"testing"

	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestSegmentBuilder_Basic(t *testing.T) {
	ftsConfigs := map[string]*Config{
		"title":   DefaultConfig(),
		"content": DefaultConfig(),
	}

	sb := NewSegmentBuilder("test-ns", ftsConfigs)

	// Create a WAL entry with upserts
	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req-1")

	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{
			"title":   wal.StringValue("Hello World"),
			"content": wal.StringValue("This is a test document"),
		},
		nil, 0,
	)

	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 2}},
		map[string]*wal.AttributeValue{
			"title":   wal.StringValue("Another Title"),
			"content": wal.StringValue("More content here"),
		},
		nil, 0,
	)

	entry.SubBatches = append(entry.SubBatches, batch)

	if err := sb.AddWALEntry(entry); err != nil {
		t.Fatalf("AddWALEntry error: %v", err)
	}

	if !sb.HasData() {
		t.Error("expected HasData() to return true")
	}

	indexes := sb.Build()

	if len(indexes) != 2 {
		t.Errorf("len(indexes) = %d, want 2", len(indexes))
	}

	titleIdx := indexes["title"]
	if titleIdx == nil {
		t.Fatal("expected 'title' index to exist")
	}
	if titleIdx.TotalDocs != 2 {
		t.Errorf("title.TotalDocs = %d, want 2", titleIdx.TotalDocs)
	}

	// Check that "hello" is indexed
	if !titleIdx.HasTerm("hello") {
		t.Error("expected 'hello' to be indexed in title")
	}

	contentIdx := indexes["content"]
	if contentIdx == nil {
		t.Fatal("expected 'content' index to exist")
	}
	if contentIdx.TotalDocs != 2 {
		t.Errorf("content.TotalDocs = %d, want 2", contentIdx.TotalDocs)
	}
}

func TestSegmentBuilder_AppliesDeletes(t *testing.T) {
	ftsConfigs := map[string]*Config{
		"content": DefaultConfig(),
	}

	sb := NewSegmentBuilder("test-ns", ftsConfigs)

	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req-1")

	// Add an upsert
	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{
			"content": wal.StringValue("Hello World"),
		},
		nil, 0,
	)

	// Add a delete
	batch.AddDelete(&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}})

	entry.SubBatches = append(entry.SubBatches, batch)

	if err := sb.AddWALEntry(entry); err != nil {
		t.Fatalf("AddWALEntry error: %v", err)
	}

	indexes := sb.Build()
	contentIdx := indexes["content"]

	if contentIdx.TotalDocs != 0 {
		t.Errorf("TotalDocs = %d, want 0 after delete", contentIdx.TotalDocs)
	}
	if contentIdx.HasTerm("hello") {
		t.Error("expected 'hello' to be removed after delete")
	}
}

func TestSegmentBuilder_AppliesPatches(t *testing.T) {
	ftsConfigs := map[string]*Config{
		"content": DefaultConfig(),
	}

	sb := NewSegmentBuilder("test-ns", ftsConfigs)

	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req-1")

	// Add an upsert
	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{
			"content": wal.StringValue("Hello World"),
		},
		nil, 0,
	)

	// Add a patch
	batch.AddPatch(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{
			"content": wal.StringValue("Patched Content"),
		},
	)

	entry.SubBatches = append(entry.SubBatches, batch)

	if err := sb.AddWALEntry(entry); err != nil {
		t.Fatalf("AddWALEntry error: %v", err)
	}

	indexes := sb.Build()
	contentIdx := indexes["content"]

	if contentIdx.TotalDocs != 1 {
		t.Errorf("TotalDocs = %d, want 1", contentIdx.TotalDocs)
	}
	if contentIdx.HasTerm("hello") {
		t.Error("expected 'hello' to be removed after patch")
	}
	if !contentIdx.HasTerm("patched") {
		t.Error("expected 'patched' to be indexed after patch")
	}
}

func TestSegmentBuilder_NoFTSConfig(t *testing.T) {
	// Empty FTS config
	ftsConfigs := map[string]*Config{}

	sb := NewSegmentBuilder("test-ns", ftsConfigs)

	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req-1")

	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{
			"content": wal.StringValue("Hello World"),
		},
		nil, 0,
	)

	entry.SubBatches = append(entry.SubBatches, batch)

	if err := sb.AddWALEntry(entry); err != nil {
		t.Fatalf("AddWALEntry error: %v", err)
	}

	if sb.HasData() {
		t.Error("expected HasData() to return false with no FTS config")
	}
}

func TestSegmentBuilder_WriteToObjectStore(t *testing.T) {
	ftsConfigs := map[string]*Config{
		"title":   DefaultConfig(),
		"content": DefaultConfig(),
	}

	sb := NewSegmentBuilder("test-ns", ftsConfigs)

	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req-1")

	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{
			"title":   wal.StringValue("Hello World"),
			"content": wal.StringValue("This is a test document"),
		},
		nil, 0,
	)

	entry.SubBatches = append(entry.SubBatches, batch)

	if err := sb.AddWALEntry(entry); err != nil {
		t.Fatalf("AddWALEntry error: %v", err)
	}

	// Use in-memory store
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	segmentKey := "vex/namespaces/test-ns/index/segments/seg_123"
	result, err := sb.WriteToObjectStore(ctx, store, segmentKey)
	if err != nil {
		t.Fatalf("WriteToObjectStore error: %v", err)
	}

	if len(result.FTSKeys) != 2 {
		t.Errorf("len(FTSKeys) = %d, want 2", len(result.FTSKeys))
	}

	// Verify objects were written
	for _, key := range result.FTSKeys {
		reader, _, err := store.Get(ctx, key, nil)
		if err != nil {
			t.Errorf("failed to read FTS key %q: %v", key, err)
			continue
		}
		reader.Close()
	}
}

func TestSegmentBuilder_DifferentDocIDTypes(t *testing.T) {
	ftsConfigs := map[string]*Config{
		"content": DefaultConfig(),
	}

	sb := NewSegmentBuilder("test-ns", ftsConfigs)

	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req-1")

	// Add documents with different ID types
	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{
			"content": wal.StringValue("Document one"),
		},
		nil, 0,
	)

	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_Str{Str: "doc-2"}},
		map[string]*wal.AttributeValue{
			"content": wal.StringValue("Document two"),
		},
		nil, 0,
	)

	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_Uuid{Uuid: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}},
		map[string]*wal.AttributeValue{
			"content": wal.StringValue("Document three"),
		},
		nil, 0,
	)

	entry.SubBatches = append(entry.SubBatches, batch)

	if err := sb.AddWALEntry(entry); err != nil {
		t.Fatalf("AddWALEntry error: %v", err)
	}

	indexes := sb.Build()
	contentIdx := indexes["content"]

	if contentIdx.TotalDocs != 3 {
		t.Errorf("TotalDocs = %d, want 3", contentIdx.TotalDocs)
	}

	// Verify all "document" tokens are indexed
	if !contentIdx.HasTerm("document") {
		t.Error("expected 'document' to be indexed")
	}

	docPosting := contentIdx.GetPostingList("document")
	if docPosting.GetCardinality() != 3 {
		t.Errorf("'document' posting cardinality = %d, want 3", docPosting.GetCardinality())
	}
}

func TestSegmentBuilder_NonStringAttributesSkipped(t *testing.T) {
	ftsConfigs := map[string]*Config{
		"content": DefaultConfig(),
		"count":   DefaultConfig(), // FTS enabled on non-string field
	}

	sb := NewSegmentBuilder("test-ns", ftsConfigs)

	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req-1")

	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{
			"content": wal.StringValue("Hello World"),
			"count":   wal.IntValue(42), // Non-string value
		},
		nil, 0,
	)

	entry.SubBatches = append(entry.SubBatches, batch)

	if err := sb.AddWALEntry(entry); err != nil {
		t.Fatalf("AddWALEntry error: %v", err)
	}

	indexes := sb.Build()

	// Content should be indexed
	if indexes["content"] == nil {
		t.Error("expected 'content' index to exist")
	}

	// Count should not be indexed (non-string value)
	if indexes["count"] != nil {
		t.Error("did not expect 'count' index (non-string value)")
	}
}
