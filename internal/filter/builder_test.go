package filter

import (
	"testing"

	"github.com/vexsearch/vex/internal/schema"
)

func TestIndexBuilder_Basic(t *testing.T) {
	schemaDef := &schema.Definition{
		Attributes: map[string]schema.Attribute{
			"category": {Type: schema.TypeString},
			"price":    {Type: schema.TypeFloat},
			"active":   {Type: schema.TypeBool},
		},
	}

	builder := NewIndexBuilder("test-ns", schemaDef)

	// Add documents
	builder.AddDocument(0, map[string]any{
		"category": "electronics",
		"price":    99.99,
		"active":   true,
	})
	builder.AddDocument(1, map[string]any{
		"category": "books",
		"price":    14.99,
		"active":   true,
	})
	builder.AddDocument(2, map[string]any{
		"category": "electronics",
		"price":    199.99,
		"active":   false,
	})

	// Serialize all
	data, err := builder.SerializeAll()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	if len(data) != 3 {
		t.Errorf("expected 3 indexes, got %d", len(data))
	}

	// Verify category index
	catData, ok := data["category"]
	if !ok {
		t.Fatal("missing category index")
	}

	catIdx, err := Deserialize(catData)
	if err != nil {
		t.Fatalf("failed to deserialize category: %v", err)
	}

	bm := catIdx.Eq("electronics")
	if bm.GetCardinality() != 2 {
		t.Errorf("expected 2 electronics, got %d", bm.GetCardinality())
	}
}

func TestIndexBuilder_WithArrays(t *testing.T) {
	schemaDef := &schema.Definition{
		Attributes: map[string]schema.Attribute{
			"tags":   {Type: schema.TypeStringArray},
			"scores": {Type: schema.TypeIntArray},
		},
	}

	builder := NewIndexBuilder("test-ns", schemaDef)

	builder.AddDocument(0, map[string]any{
		"tags":   []any{"go", "rust"},
		"scores": []any{10, 20},
	})
	builder.AddDocument(1, map[string]any{
		"tags":   []any{"python", "go"},
		"scores": []any{15, 25},
	})
	builder.AddDocument(2, map[string]any{
		"tags":   []any{"java"},
		"scores": []any{30},
	})

	data, err := builder.SerializeAll()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	tagsIdx, err := Deserialize(data["tags"])
	if err != nil {
		t.Fatalf("failed to deserialize tags: %v", err)
	}

	// Test Contains
	bm := tagsIdx.Contains("go")
	if bm.GetCardinality() != 2 {
		t.Errorf("expected 2 docs with 'go', got %d", bm.GetCardinality())
	}

	// Test ContainsAny
	bm = tagsIdx.ContainsAny([]any{"rust", "java"})
	if bm.GetCardinality() != 2 {
		t.Errorf("expected 2 docs with 'rust' or 'java', got %d", bm.GetCardinality())
	}
}

func TestIndexBuilder_FilterableOnly(t *testing.T) {
	f := false
	schemaDef := &schema.Definition{
		Attributes: map[string]schema.Attribute{
			"name":        {Type: schema.TypeString},
			"description": {Type: schema.TypeString, Filterable: &f}, // Not filterable
		},
	}

	builder := NewIndexBuilder("test-ns", schemaDef)

	builder.AddDocument(0, map[string]any{
		"name":        "test",
		"description": "long text",
	})

	data, err := builder.SerializeAll()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Should only have 'name' index
	if len(data) != 1 {
		t.Errorf("expected 1 index (name only), got %d", len(data))
	}
	if _, ok := data["name"]; !ok {
		t.Error("expected 'name' index")
	}
	if _, ok := data["description"]; ok {
		t.Error("should not have 'description' index")
	}
}

func TestIndexBuilder_NullValues(t *testing.T) {
	schemaDef := &schema.Definition{
		Attributes: map[string]schema.Attribute{
			"optional": {Type: schema.TypeString},
		},
	}

	builder := NewIndexBuilder("test-ns", schemaDef)

	builder.AddDocument(0, map[string]any{"optional": "value"})
	builder.AddDocument(1, map[string]any{"optional": nil})
	builder.AddDocument(2, map[string]any{}) // Missing attribute

	data, err := builder.SerializeAll()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	idx, err := Deserialize(data["optional"])
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	// Eq null should match both nil and missing
	bm := idx.Eq(nil)
	if bm.GetCardinality() != 2 {
		t.Errorf("expected 2 null rows, got %d", bm.GetCardinality())
	}
}

func TestIndexReader(t *testing.T) {
	// Create and serialize an index
	schemaDef := &schema.Definition{
		Attributes: map[string]schema.Attribute{
			"status": {Type: schema.TypeString},
		},
	}

	builder := NewIndexBuilder("test-ns", schemaDef)
	builder.AddDocument(0, map[string]any{"status": "active"})
	builder.AddDocument(1, map[string]any{"status": "pending"})
	builder.AddDocument(2, map[string]any{"status": "active"})

	data, err := builder.SerializeAll()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Create reader and load
	reader := NewIndexReader()
	if err := reader.LoadIndex("status", data["status"]); err != nil {
		t.Fatalf("failed to load index: %v", err)
	}

	if !reader.HasIndex("status") {
		t.Error("expected status index")
	}

	idx := reader.GetIndex("status")
	bm := idx.Eq("active")
	if bm.GetCardinality() != 2 {
		t.Errorf("expected 2 active, got %d", bm.GetCardinality())
	}
}

func TestFilterProcessor(t *testing.T) {
	processor := NewFilterProcessor()

	schemaDef := &schema.Definition{
		Attributes: map[string]schema.Attribute{
			"type": {Type: schema.TypeString},
		},
	}

	processor.ProcessDocument(nil, "ns1", schemaDef, 0, map[string]any{"type": "a"})
	processor.ProcessDocument(nil, "ns1", schemaDef, 1, map[string]any{"type": "b"})
	processor.ProcessDocument(nil, "ns2", schemaDef, 0, map[string]any{"type": "c"})

	data, err := processor.BuildAll()
	if err != nil {
		t.Fatalf("failed to build: %v", err)
	}

	if len(data) != 2 {
		t.Errorf("expected 2 namespaces, got %d", len(data))
	}

	// Verify ns1
	ns1Data, ok := data["ns1"]
	if !ok {
		t.Fatal("missing ns1")
	}

	idx, err := Deserialize(ns1Data["type"])
	if err != nil {
		t.Fatalf("failed to deserialize ns1 type: %v", err)
	}

	bm := idx.Eq("a")
	if bm.GetCardinality() != 1 {
		t.Errorf("expected 1 match for 'a', got %d", bm.GetCardinality())
	}
}
