package schema

import (
	"errors"
	"testing"
)

// Test filterable option (default true, false when regex/fts enabled)
func TestFilterableDefault(t *testing.T) {
	// Default filterable (no options set) should be true
	attr := Attribute{Type: TypeString}
	if !attr.IsFilterable() {
		t.Error("default filterable should be true")
	}
}

func TestFilterableExplicitTrue(t *testing.T) {
	v := true
	attr := Attribute{Type: TypeString, Filterable: &v}
	if !attr.IsFilterable() {
		t.Error("explicit filterable=true should be true")
	}
}

func TestFilterableExplicitFalse(t *testing.T) {
	v := false
	attr := Attribute{Type: TypeString, Filterable: &v}
	if attr.IsFilterable() {
		t.Error("explicit filterable=false should be false")
	}
}

func TestFilterableDefaultFalseWithRegex(t *testing.T) {
	// When regex is enabled, default filterable should be false
	attr := Attribute{Type: TypeString, Regex: true}
	if attr.IsFilterable() {
		t.Error("default filterable should be false when regex enabled")
	}
}

func TestFilterableDefaultFalseWithFTS(t *testing.T) {
	// When full_text_search is enabled, default filterable should be false
	attr := Attribute{Type: TypeString, FullTextSearch: NewFullTextConfig()}
	if attr.IsFilterable() {
		t.Error("default filterable should be false when full_text_search enabled")
	}
}

func TestFilterableExplicitTrueWithRegex(t *testing.T) {
	// Can explicitly set filterable=true even with regex enabled
	v := true
	attr := Attribute{Type: TypeString, Regex: true, Filterable: &v}
	if !attr.IsFilterable() {
		t.Error("explicit filterable=true should override regex default")
	}
}

func TestFilterableExplicitTrueWithFTS(t *testing.T) {
	// Can explicitly set filterable=true even with FTS enabled
	v := true
	attr := Attribute{Type: TypeString, FullTextSearch: NewFullTextConfig(), Filterable: &v}
	if !attr.IsFilterable() {
		t.Error("explicit filterable=true should override FTS default")
	}
}

// Test regex option (default false)
func TestRegexDefault(t *testing.T) {
	attr := Attribute{Type: TypeString}
	if attr.HasRegex() {
		t.Error("default regex should be false")
	}
}

func TestRegexEnabled(t *testing.T) {
	attr := Attribute{Type: TypeString, Regex: true}
	if !attr.HasRegex() {
		t.Error("regex=true should return true from HasRegex")
	}
}

// Test full_text_search boolean option
func TestFullTextSearchBooleanTrue(t *testing.T) {
	cfg, err := ParseFullTextSearch(true)
	if err != nil {
		t.Fatalf("ParseFullTextSearch(true) failed: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	// Check defaults
	if cfg.Tokenizer != "word_v3" {
		t.Errorf("expected tokenizer 'word_v3', got %q", cfg.Tokenizer)
	}
	if cfg.Language != "english" {
		t.Errorf("expected language 'english', got %q", cfg.Language)
	}
	if !cfg.Stemming {
		t.Error("expected stemming to be true by default")
	}
	if !cfg.RemoveStopwords {
		t.Error("expected remove_stopwords to be true by default")
	}
}

func TestFullTextSearchBooleanFalse(t *testing.T) {
	cfg, err := ParseFullTextSearch(false)
	if err != nil {
		t.Fatalf("ParseFullTextSearch(false) failed: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil config for false")
	}
}

// Test full_text_search object with tokenizer/language/etc
func TestFullTextSearchObject(t *testing.T) {
	input := map[string]any{
		"tokenizer":        "whitespace",
		"case_sensitive":   true,
		"language":         "german",
		"stemming":         false,
		"remove_stopwords": false,
		"ascii_folding":    true,
		"k1":               1.5,
		"b":                0.5,
	}
	cfg, err := ParseFullTextSearch(input)
	if err != nil {
		t.Fatalf("ParseFullTextSearch failed: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	if cfg.Tokenizer != "whitespace" {
		t.Errorf("expected tokenizer 'whitespace', got %q", cfg.Tokenizer)
	}
	if !cfg.CaseSensitive {
		t.Error("expected case_sensitive to be true")
	}
	if cfg.Language != "german" {
		t.Errorf("expected language 'german', got %q", cfg.Language)
	}
	if cfg.Stemming {
		t.Error("expected stemming to be false")
	}
	if cfg.RemoveStopwords {
		t.Error("expected remove_stopwords to be false")
	}
	if !cfg.ASCIIFolding {
		t.Error("expected ascii_folding to be true")
	}
	if cfg.K1 != 1.5 {
		t.Errorf("expected k1=1.5, got %f", cfg.K1)
	}
	if cfg.B != 0.5 {
		t.Errorf("expected b=0.5, got %f", cfg.B)
	}
}

func TestFullTextSearchObjectPartial(t *testing.T) {
	// Only specify some options; rest should have defaults
	input := map[string]any{
		"tokenizer": "character",
	}
	cfg, err := ParseFullTextSearch(input)
	if err != nil {
		t.Fatalf("ParseFullTextSearch failed: %v", err)
	}
	if cfg.Tokenizer != "character" {
		t.Errorf("expected tokenizer 'character', got %q", cfg.Tokenizer)
	}
	// Default values
	if cfg.Language != "english" {
		t.Errorf("expected default language 'english', got %q", cfg.Language)
	}
	if cfg.K1 != 1.2 {
		t.Errorf("expected default k1=1.2, got %f", cfg.K1)
	}
}

func TestFullTextSearchInvalidType(t *testing.T) {
	_, err := ParseFullTextSearch("invalid")
	if err == nil {
		t.Error("expected error for invalid type")
	}
}

// Test vector object with type and ann fields
func TestVectorConfigParse(t *testing.T) {
	input := map[string]any{
		"type": "[1536]f32",
		"ann":  true,
	}
	cfg, err := ParseVectorConfig(input)
	if err != nil {
		t.Fatalf("ParseVectorConfig failed: %v", err)
	}
	if cfg.Type != "[1536]f32" {
		t.Errorf("expected type '[1536]f32', got %q", cfg.Type)
	}
	if !cfg.ANN {
		t.Error("expected ann=true")
	}
}

func TestVectorConfigF16(t *testing.T) {
	input := map[string]any{
		"type": "[768]f16",
	}
	cfg, err := ParseVectorConfig(input)
	if err != nil {
		t.Fatalf("ParseVectorConfig failed: %v", err)
	}
	if cfg.Type != "[768]f16" {
		t.Errorf("expected type '[768]f16', got %q", cfg.Type)
	}
	if cfg.ANN {
		t.Error("expected ann=false by default")
	}
}

func TestVectorConfigMissingType(t *testing.T) {
	input := map[string]any{
		"ann": true,
	}
	_, err := ParseVectorConfig(input)
	if err == nil {
		t.Error("expected error for missing type")
	}
}

func TestVectorConfigInvalidType(t *testing.T) {
	_, err := ParseVectorConfig("invalid")
	if err == nil {
		t.Error("expected error for invalid type")
	}
}

func TestVectorConfigNil(t *testing.T) {
	cfg, err := ParseVectorConfig(nil)
	if err != nil {
		t.Fatalf("ParseVectorConfig(nil) failed: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil config")
	}
}

// Test attribute helper methods
func TestAttributeHasFullTextSearch(t *testing.T) {
	attr := Attribute{Type: TypeString}
	if attr.HasFullTextSearch() {
		t.Error("expected HasFullTextSearch=false")
	}

	attr.FullTextSearch = NewFullTextConfig()
	if !attr.HasFullTextSearch() {
		t.Error("expected HasFullTextSearch=true")
	}
}

func TestAttributeIsVector(t *testing.T) {
	attr := Attribute{Type: TypeFloat}
	if attr.IsVector() {
		t.Error("expected IsVector=false")
	}

	attr.Vector = &VectorFieldConfig{Type: "[128]f32"}
	if !attr.IsVector() {
		t.Error("expected IsVector=true")
	}
}

// Verify filterable and full_text_search can be updated online
func TestUpdateFieldOptionsFilterable(t *testing.T) {
	def := NewDefinition()
	def.SetAttribute("content", Attribute{Type: TypeString})

	// Initially, filterable is default (true)
	attr := def.GetAttribute("content")
	if !attr.IsFilterable() {
		t.Error("expected default filterable=true")
	}

	// Update to false
	v := false
	err := def.UpdateFieldOptions("content", &v, nil, nil)
	if err != nil {
		t.Fatalf("UpdateFieldOptions failed: %v", err)
	}

	attr = def.GetAttribute("content")
	if attr.IsFilterable() {
		t.Error("expected filterable=false after update")
	}

	// Update back to true
	v = true
	err = def.UpdateFieldOptions("content", &v, nil, nil)
	if err != nil {
		t.Fatalf("UpdateFieldOptions failed: %v", err)
	}

	attr = def.GetAttribute("content")
	if !attr.IsFilterable() {
		t.Error("expected filterable=true after update")
	}
}

func TestUpdateFieldOptionsFTS(t *testing.T) {
	def := NewDefinition()
	def.SetAttribute("content", Attribute{Type: TypeString})

	// Initially no FTS
	attr := def.GetAttribute("content")
	if attr.HasFullTextSearch() {
		t.Error("expected no FTS initially")
	}

	// Enable FTS
	err := def.UpdateFieldOptions("content", nil, EnableFTS(NewFullTextConfig()), nil)
	if err != nil {
		t.Fatalf("UpdateFieldOptions failed: %v", err)
	}

	attr = def.GetAttribute("content")
	if !attr.HasFullTextSearch() {
		t.Error("expected FTS enabled after update")
	}
	if attr.FullTextSearch.Tokenizer != "word_v3" {
		t.Errorf("expected tokenizer 'word_v3', got %q", attr.FullTextSearch.Tokenizer)
	}
}

func TestUpdateFieldOptionsDisableFTS(t *testing.T) {
	def := NewDefinition()
	def.SetAttribute("content", Attribute{
		Type:           TypeString,
		FullTextSearch: NewFullTextConfig(),
	})

	// Initially has FTS
	attr := def.GetAttribute("content")
	if !attr.HasFullTextSearch() {
		t.Error("expected FTS initially")
	}

	// Disable FTS
	err := def.UpdateFieldOptions("content", nil, DisableFTS(), nil)
	if err != nil {
		t.Fatalf("UpdateFieldOptions failed: %v", err)
	}

	attr = def.GetAttribute("content")
	if attr.HasFullTextSearch() {
		t.Error("expected FTS disabled after update")
	}
}

func TestUpdateFieldOptionsNilDoesNotChangeFTS(t *testing.T) {
	def := NewDefinition()
	def.SetAttribute("content", Attribute{
		Type:           TypeString,
		FullTextSearch: NewFullTextConfig(),
	})

	// Initially has FTS
	attr := def.GetAttribute("content")
	if !attr.HasFullTextSearch() {
		t.Error("expected FTS initially")
	}

	// Pass nil for fts - should not change
	err := def.UpdateFieldOptions("content", nil, nil, nil)
	if err != nil {
		t.Fatalf("UpdateFieldOptions failed: %v", err)
	}

	attr = def.GetAttribute("content")
	if !attr.HasFullTextSearch() {
		t.Error("expected FTS still enabled (nil should not change)")
	}
}

func TestUpdateFieldOptionsNonexistentAttribute(t *testing.T) {
	def := NewDefinition()
	v := true
	err := def.UpdateFieldOptions("nonexistent", &v, nil, nil)
	if err == nil {
		t.Error("expected error for nonexistent attribute")
	}
}

// Test that type changes are not allowed but field options can change
func TestSchemaUpdateAllowsFieldOptionChanges(t *testing.T) {
	existing := NewDefinition()
	existing.SetAttribute("content", Attribute{Type: TypeString})

	// Proposed schema with different field options but same type
	fts := NewFullTextConfig()
	proposed := NewDefinition()
	proposed.SetAttribute("content", Attribute{
		Type:           TypeString,
		FullTextSearch: fts,
		Regex:          true,
	})

	err := ValidateSchemaUpdate(existing, proposed)
	if err != nil {
		t.Errorf("ValidateSchemaUpdate should allow field option changes: %v", err)
	}
}

func TestSchemaUpdateRejectsTypeChange(t *testing.T) {
	existing := NewDefinition()
	existing.SetAttribute("field", Attribute{Type: TypeString})

	proposed := NewDefinition()
	proposed.SetAttribute("field", Attribute{Type: TypeInt})

	err := ValidateSchemaUpdate(existing, proposed)
	if err == nil {
		t.Error("expected error for type change")
	}
	if !errors.Is(err, ErrTypeChangeNotAllowed) {
		t.Errorf("expected ErrTypeChangeNotAllowed, got: %v", err)
	}
}

func TestSchemaUpdateRejectsVectorConfigChange(t *testing.T) {
	existing := NewDefinition()
	existing.SetAttribute("embedding", Attribute{
		Type:   TypeFloat,
		Vector: &VectorFieldConfig{Type: "[1536]f32", ANN: false},
	})

	proposed := NewDefinition()
	proposed.SetAttribute("embedding", Attribute{
		Type:   TypeFloat,
		Vector: &VectorFieldConfig{Type: "[768]f16", ANN: true}, // Different dimensions/dtype
	})

	err := ValidateSchemaUpdate(existing, proposed)
	if err == nil {
		t.Error("expected error for vector config change")
	}
	if !errors.Is(err, ErrVectorConfigChangeNotAllowed) {
		t.Errorf("expected ErrVectorConfigChangeNotAllowed, got: %v", err)
	}
}

func TestSchemaUpdateAllowsVectorANNChange(t *testing.T) {
	existing := NewDefinition()
	existing.SetAttribute("embedding", Attribute{
		Type:   TypeFloat,
		Vector: &VectorFieldConfig{Type: "[1536]f32", ANN: false},
	})

	proposed := NewDefinition()
	proposed.SetAttribute("embedding", Attribute{
		Type:   TypeFloat,
		Vector: &VectorFieldConfig{Type: "[1536]f32", ANN: true}, // Same type, different ANN
	})

	err := ValidateSchemaUpdate(existing, proposed)
	if err != nil {
		t.Errorf("ValidateSchemaUpdate should allow ANN flag change: %v", err)
	}
}

func TestSchemaUpdateRejectsVectorConfigRemoval(t *testing.T) {
	existing := NewDefinition()
	existing.SetAttribute("embedding", Attribute{
		Type:   TypeFloat,
		Vector: &VectorFieldConfig{Type: "[1536]f32", ANN: false},
	})

	proposed := NewDefinition()
	proposed.SetAttribute("embedding", Attribute{
		Type:   TypeFloat,
		Vector: nil, // Removing vector config
	})

	err := ValidateSchemaUpdate(existing, proposed)
	if err == nil {
		t.Error("expected error for removing vector config")
	}
	if !errors.Is(err, ErrVectorConfigChangeNotAllowed) {
		t.Errorf("expected ErrVectorConfigChangeNotAllowed, got: %v", err)
	}
}

func TestSchemaUpdateRejectsVectorConfigAddition(t *testing.T) {
	existing := NewDefinition()
	existing.SetAttribute("data", Attribute{
		Type:   TypeFloat,
		Vector: nil, // No vector config
	})

	proposed := NewDefinition()
	proposed.SetAttribute("data", Attribute{
		Type:   TypeFloat,
		Vector: &VectorFieldConfig{Type: "[1536]f32", ANN: true}, // Adding vector config
	})

	err := ValidateSchemaUpdate(existing, proposed)
	if err == nil {
		t.Error("expected error for adding vector config to existing attribute")
	}
	if !errors.Is(err, ErrVectorConfigChangeNotAllowed) {
		t.Errorf("expected ErrVectorConfigChangeNotAllowed, got: %v", err)
	}
}

// Test attribute Clone
func TestAttributeClone(t *testing.T) {
	f := true
	fts := NewFullTextConfig()
	fts.Language = "spanish"
	vec := &VectorFieldConfig{Type: "[128]f32", ANN: true}

	orig := Attribute{
		Type:           TypeString,
		Filterable:     &f,
		Regex:          true,
		FullTextSearch: fts,
		Vector:         vec,
	}

	clone := orig.Clone()

	// Verify values are copied
	if clone.Type != orig.Type {
		t.Error("Type not cloned")
	}
	if *clone.Filterable != *orig.Filterable {
		t.Error("Filterable not cloned")
	}
	if clone.Regex != orig.Regex {
		t.Error("Regex not cloned")
	}
	if clone.FullTextSearch.Language != orig.FullTextSearch.Language {
		t.Error("FullTextSearch not cloned")
	}
	if clone.Vector.Type != orig.Vector.Type {
		t.Error("Vector not cloned")
	}

	// Verify independence
	*clone.Filterable = false
	if *orig.Filterable == false {
		t.Error("Filterable not independent")
	}
	clone.FullTextSearch.Language = "french"
	if orig.FullTextSearch.Language == "french" {
		t.Error("FullTextSearch not independent")
	}
	clone.Vector.Type = "[256]f16"
	if orig.Vector.Type == "[256]f16" {
		t.Error("Vector not independent")
	}
}

// Test Definition Clone with field options
func TestDefinitionCloneWithFieldOptions(t *testing.T) {
	f := true
	def := NewDefinition()
	def.SetAttribute("content", Attribute{
		Type:           TypeString,
		Filterable:     &f,
		FullTextSearch: NewFullTextConfig(),
	})

	clone := def.Clone()

	// Modify clone
	clone.GetAttribute("content").FullTextSearch.Language = "german"
	clone.SetAttribute("new", Attribute{Type: TypeInt})

	// Original should be unchanged
	orig := def.GetAttribute("content")
	if orig.FullTextSearch.Language != "english" {
		t.Error("Clone modified original FullTextSearch")
	}
	if def.HasAttribute("new") {
		t.Error("Clone added attribute to original")
	}
}

// Test NewFullTextConfig creates proper defaults
func TestNewFullTextConfigDefaults(t *testing.T) {
	cfg := NewFullTextConfig()

	if cfg.Tokenizer != "word_v3" {
		t.Errorf("expected tokenizer 'word_v3', got %q", cfg.Tokenizer)
	}
	if cfg.CaseSensitive {
		t.Error("expected case_sensitive=false")
	}
	if cfg.Language != "english" {
		t.Errorf("expected language 'english', got %q", cfg.Language)
	}
	if !cfg.Stemming {
		t.Error("expected stemming=true")
	}
	if !cfg.RemoveStopwords {
		t.Error("expected remove_stopwords=true")
	}
	if cfg.ASCIIFolding {
		t.Error("expected ascii_folding=false")
	}
	if cfg.K1 != 1.2 {
		t.Errorf("expected k1=1.2, got %f", cfg.K1)
	}
	if cfg.B != 0.75 {
		t.Errorf("expected b=0.75, got %f", cfg.B)
	}
}
