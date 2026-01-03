package fts

import (
	"testing"
)

func TestIndex_AddDocument(t *testing.T) {
	idx := NewIndex("content", testConfigNoStemming())

	idx.AddDocument(1, "hello world")
	idx.AddDocument(2, "world peace")
	idx.AddDocument(3, "hello peace")

	// Check total docs
	if idx.TotalDocs != 3 {
		t.Errorf("TotalDocs = %d, want 3", idx.TotalDocs)
	}

	// Check that "hello" appears in docs 1 and 3
	helloPosting := idx.GetPostingList("hello")
	if !helloPosting.Contains(1) {
		t.Error("expected 'hello' posting to contain doc 1")
	}
	if helloPosting.Contains(2) {
		t.Error("did not expect 'hello' posting to contain doc 2")
	}
	if !helloPosting.Contains(3) {
		t.Error("expected 'hello' posting to contain doc 3")
	}

	// Check that "world" appears in docs 1 and 2
	worldPosting := idx.GetPostingList("world")
	if worldPosting.GetCardinality() != 2 {
		t.Errorf("'world' posting cardinality = %d, want 2", worldPosting.GetCardinality())
	}

	// Check document frequency
	if df := idx.GetDocumentFrequency("hello"); df != 2 {
		t.Errorf("GetDocumentFrequency('hello') = %d, want 2", df)
	}

	// Check term frequency
	if tf := idx.GetTermFrequency("hello", 1); tf != 1 {
		t.Errorf("GetTermFrequency('hello', 1) = %d, want 1", tf)
	}

	// Check doc length
	if dl := idx.GetDocLength(1); dl != 2 {
		t.Errorf("GetDocLength(1) = %d, want 2", dl)
	}
}

func TestIndex_WithStopwords(t *testing.T) {
	cfg := testConfigNoStemming()
	cfg.RemoveStopwords = true
	idx := NewIndex("content", cfg)

	idx.AddDocument(1, "the quick brown fox")

	// "the" should be removed as a stopword
	if idx.HasTerm("the") {
		t.Error("expected 'the' to be removed as stopword")
	}

	// "quick" should remain
	if !idx.HasTerm("quick") {
		t.Error("expected 'quick' to remain")
	}

	// Doc length should be 3 (quick, brown, fox)
	if dl := idx.GetDocLength(1); dl != 3 {
		t.Errorf("GetDocLength(1) = %d, want 3", dl)
	}
}

func TestIndex_WithoutStopwords(t *testing.T) {
	cfg := testConfigNoStemming()
	cfg.RemoveStopwords = false
	idx := NewIndex("content", cfg)

	idx.AddDocument(1, "the quick brown fox")

	// "the" should remain
	if !idx.HasTerm("the") {
		t.Error("expected 'the' to remain when stopword removal is disabled")
	}

	// Doc length should be 4
	if dl := idx.GetDocLength(1); dl != 4 {
		t.Errorf("GetDocLength(1) = %d, want 4", dl)
	}
}

func TestIndex_SerializeDeserialize(t *testing.T) {
	idx := NewIndex("content", testConfigNoStemming())
	idx.AddDocument(1, "hello world")
	idx.AddDocument(2, "world peace")
	idx.AddDocument(3, "hello peace hello") // "hello" appears twice

	data, err := idx.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	// Deserialize
	idx2, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize() error = %v", err)
	}

	// Verify metadata
	if idx2.Attribute != "content" {
		t.Errorf("Attribute = %q, want %q", idx2.Attribute, "content")
	}
	if idx2.TotalDocs != 3 {
		t.Errorf("TotalDocs = %d, want 3", idx2.TotalDocs)
	}

	// Verify postings
	if !idx2.HasTerm("hello") {
		t.Error("expected 'hello' to exist")
	}
	if !idx2.HasTerm("world") {
		t.Error("expected 'world' to exist")
	}

	// Verify term frequency is preserved
	if tf := idx2.GetTermFrequency("hello", 3); tf != 2 {
		t.Errorf("GetTermFrequency('hello', 3) = %d, want 2", tf)
	}

	// Verify doc lengths
	if dl := idx2.GetDocLength(1); dl != 2 {
		t.Errorf("GetDocLength(1) = %d, want 2", dl)
	}
	if dl := idx2.GetDocLength(3); dl != 3 {
		t.Errorf("GetDocLength(3) = %d, want 3", dl)
	}
}

func TestIndexBuilder(t *testing.T) {
	builder := NewIndexBuilder()

	ftsConfigs := map[string]*Config{
		"title":   testConfigNoStemming(),
		"content": testConfigNoStemming(),
	}

	builder.AddDocument(1, map[string]any{
		"title":   "Hello World",
		"content": "This is a test document",
	}, ftsConfigs)

	builder.AddDocument(2, map[string]any{
		"title":   "Another Title",
		"content": "More content here",
	}, ftsConfigs)

	indexes := builder.Build()

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

	contentIdx := indexes["content"]
	if contentIdx == nil {
		t.Fatal("expected 'content' index to exist")
	}
	if contentIdx.TotalDocs != 2 {
		t.Errorf("content.TotalDocs = %d, want 2", contentIdx.TotalDocs)
	}
}

func TestIndexBuilder_SkipsNonFTSAttributes(t *testing.T) {
	builder := NewIndexBuilder()

	// Only content has FTS config
	ftsConfigs := map[string]*Config{
		"content": testConfigNoStemming(),
	}

	builder.AddDocument(1, map[string]any{
		"title":   "Hello World",
		"content": "This is a test document",
		"author":  "Test Author",
	}, ftsConfigs)

	indexes := builder.Build()

	if len(indexes) != 1 {
		t.Errorf("len(indexes) = %d, want 1", len(indexes))
	}

	if indexes["title"] != nil {
		t.Error("did not expect 'title' index to be created")
	}
	if indexes["author"] != nil {
		t.Error("did not expect 'author' index to be created")
	}
}

func TestIndexBuilder_SkipsNonStringValues(t *testing.T) {
	builder := NewIndexBuilder()

	ftsConfigs := map[string]*Config{
		"content": testConfigNoStemming(),
		"count":   testConfigNoStemming(),
	}

	builder.AddDocument(1, map[string]any{
		"content": "This is text",
		"count":   42, // Not a string
	}, ftsConfigs)

	indexes := builder.Build()

	// Only content should have an index since count is not a string
	if indexes["count"] != nil {
		t.Error("did not expect 'count' index to be created for non-string value")
	}
}

func TestIndex_EmptyDocument(t *testing.T) {
	idx := NewIndex("content", testConfigNoStemming())

	// Add a document with only stopwords
	idx.AddDocument(1, "the a an")

	// With stopword removal, this should result in an empty document
	// which should not be added to the index
	if idx.TotalDocs != 0 {
		t.Errorf("TotalDocs = %d, want 0 for document with only stopwords", idx.TotalDocs)
	}
}

func TestIndex_AvgDocLength(t *testing.T) {
	idx := NewIndex("content", testConfigNoStemming())

	// Add documents with different lengths
	idx.AddDocument(1, "one two")            // 2 tokens
	idx.AddDocument(2, "one two three")      // 3 tokens (stopword removed)
	idx.AddDocument(3, "one two three four") // 4 tokens (stopwords removed)

	// Average should be (2 + 3 + 4) / 3 = 3.0
	expected := 3.0
	if idx.AvgDocLength != expected {
		t.Errorf("AvgDocLength = %f, want %f", idx.AvgDocLength, expected)
	}
}

func TestDeserialize_InvalidMagic(t *testing.T) {
	data := []byte("XXXX0000")
	_, err := Deserialize(data)
	if err == nil {
		t.Error("expected error for invalid magic number")
	}
}

func TestDeserialize_TooShort(t *testing.T) {
	data := []byte("BM25")
	_, err := Deserialize(data)
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestIndex_RepeatedUpsert(t *testing.T) {
	idx := NewIndex("content", testConfigNoStemming())

	// Add a document
	idx.AddDocument(1, "hello world")

	if idx.TotalDocs != 1 {
		t.Errorf("TotalDocs = %d, want 1", idx.TotalDocs)
	}
	if !idx.HasTerm("hello") {
		t.Error("expected 'hello' to exist")
	}
	if !idx.HasTerm("world") {
		t.Error("expected 'world' to exist")
	}

	// Update the same document with different content
	idx.AddDocument(1, "goodbye moon")

	// TotalDocs should still be 1 (not 2)
	if idx.TotalDocs != 1 {
		t.Errorf("after update: TotalDocs = %d, want 1", idx.TotalDocs)
	}

	// Old terms should be removed
	if idx.HasTerm("hello") {
		t.Error("expected 'hello' to be removed after update")
	}
	if idx.HasTerm("world") {
		t.Error("expected 'world' to be removed after update")
	}

	// New terms should exist
	if !idx.HasTerm("goodbye") {
		t.Error("expected 'goodbye' to exist after update")
	}
	if !idx.HasTerm("moon") {
		t.Error("expected 'moon' to exist after update")
	}

	// Document length should be updated
	if dl := idx.GetDocLength(1); dl != 2 {
		t.Errorf("GetDocLength(1) = %d, want 2", dl)
	}
}

func TestIndex_RepeatedUpsertWithOverlappingTerms(t *testing.T) {
	idx := NewIndex("content", testConfigNoStemming())

	// Add documents
	idx.AddDocument(1, "apple banana cherry")
	idx.AddDocument(2, "banana date")

	if idx.TotalDocs != 2 {
		t.Errorf("TotalDocs = %d, want 2", idx.TotalDocs)
	}

	// "banana" should appear in both docs
	bananaPosting := idx.GetPostingList("banana")
	if bananaPosting.GetCardinality() != 2 {
		t.Errorf("'banana' cardinality = %d, want 2", bananaPosting.GetCardinality())
	}

	// Update doc 1 to remove "banana"
	idx.AddDocument(1, "apple elderberry")

	// TotalDocs should still be 2
	if idx.TotalDocs != 2 {
		t.Errorf("after update: TotalDocs = %d, want 2", idx.TotalDocs)
	}

	// "banana" should now only appear in doc 2
	bananaPosting = idx.GetPostingList("banana")
	if bananaPosting.GetCardinality() != 1 {
		t.Errorf("after update: 'banana' cardinality = %d, want 1", bananaPosting.GetCardinality())
	}
	if !bananaPosting.Contains(2) {
		t.Error("expected 'banana' to still be in doc 2")
	}

	// "cherry" should be gone
	if idx.HasTerm("cherry") {
		t.Error("expected 'cherry' to be removed after update")
	}

	// "elderberry" should exist only in doc 1
	elderberryPosting := idx.GetPostingList("elderberry")
	if elderberryPosting.GetCardinality() != 1 {
		t.Errorf("'elderberry' cardinality = %d, want 1", elderberryPosting.GetCardinality())
	}
}

func TestIndex_RepeatedUpsertToEmptyContent(t *testing.T) {
	idx := NewIndex("content", testConfigNoStemming())

	// Add a document
	idx.AddDocument(1, "hello world")
	idx.AddDocument(2, "goodbye moon")

	if idx.TotalDocs != 2 {
		t.Errorf("TotalDocs = %d, want 2", idx.TotalDocs)
	}

	// Update doc 1 to empty content (only stopwords)
	idx.AddDocument(1, "the a an")

	// Document should be removed since it becomes empty after stopword removal
	if idx.TotalDocs != 1 {
		t.Errorf("after empty update: TotalDocs = %d, want 1", idx.TotalDocs)
	}

	// Old terms from doc 1 should be removed
	if idx.HasTerm("hello") {
		t.Error("expected 'hello' to be removed")
	}

	// Doc 2 should still exist
	if !idx.HasTerm("goodbye") {
		t.Error("expected 'goodbye' to still exist in doc 2")
	}
}

func TestIndex_RepeatedUpsertBM25Stats(t *testing.T) {
	idx := NewIndex("content", testConfigNoStemming())

	// Add documents
	idx.AddDocument(1, "one two three")
	idx.AddDocument(2, "four five")

	// Check initial avg doc length: (3 + 2) / 2 = 2.5
	if idx.AvgDocLength != 2.5 {
		t.Errorf("initial AvgDocLength = %f, want 2.5", idx.AvgDocLength)
	}

	// Update doc 1 to have 5 words
	idx.AddDocument(1, "six seven eight nine ten")

	// TotalDocs should still be 2
	if idx.TotalDocs != 2 {
		t.Errorf("after update: TotalDocs = %d, want 2", idx.TotalDocs)
	}

	// Avg doc length should be (5 + 2) / 2 = 3.5
	if idx.AvgDocLength != 3.5 {
		t.Errorf("after update: AvgDocLength = %f, want 3.5", idx.AvgDocLength)
	}

	// Term freq for doc 1 should be updated
	if tf := idx.GetTermFrequency("one", 1); tf != 0 {
		t.Errorf("after update: GetTermFrequency('one', 1) = %d, want 0", tf)
	}
	if tf := idx.GetTermFrequency("six", 1); tf != 1 {
		t.Errorf("after update: GetTermFrequency('six', 1) = %d, want 1", tf)
	}
}
