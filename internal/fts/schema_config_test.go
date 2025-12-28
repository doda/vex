package fts

import (
	"testing"
)

// These tests verify the task requirements for bm25-schema-config:
// 1. Test full_text_search: true enables FTS
// 2. Verify full_text_search object options: tokenizer, case_sensitive, language
// 3. Test stemming, remove_stopwords, ascii_folding options
// 4. Verify BM25 params k1, b configuration

// Step 1: Test full_text_search: true enables FTS
func TestFullTextSearchTrueEnablesFTS(t *testing.T) {
	cfg, err := Parse(true)
	if err != nil {
		t.Fatalf("Parse(true) failed: %v", err)
	}

	// Verify FTS is enabled
	if cfg == nil {
		t.Fatal("full_text_search: true should enable FTS")
	}
	if !cfg.IsEnabled() {
		t.Error("IsEnabled() should return true for enabled config")
	}

	// Verify defaults are set
	if cfg.Tokenizer != "word_v3" {
		t.Errorf("expected default tokenizer 'word_v3', got %q", cfg.Tokenizer)
	}
	if cfg.Language != "english" {
		t.Errorf("expected default language 'english', got %q", cfg.Language)
	}
}

func TestFullTextSearchFalseDisablesFTS(t *testing.T) {
	cfg, err := Parse(false)
	if err != nil {
		t.Fatalf("Parse(false) failed: %v", err)
	}

	if cfg != nil {
		t.Error("full_text_search: false should disable FTS (return nil)")
	}
}

// Step 2: Verify full_text_search object options: tokenizer, case_sensitive, language
func TestFullTextSearchObjectTokenizer(t *testing.T) {
	// Test word_v3 tokenizer (default)
	cfg, err := Parse(map[string]any{"tokenizer": "word_v3"})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.Tokenizer != "word_v3" {
		t.Errorf("expected tokenizer 'word_v3', got %q", cfg.Tokenizer)
	}

	// Test whitespace tokenizer
	cfg, err = Parse(map[string]any{"tokenizer": "whitespace"})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.Tokenizer != "whitespace" {
		t.Errorf("expected tokenizer 'whitespace', got %q", cfg.Tokenizer)
	}

	// Test character tokenizer
	cfg, err = Parse(map[string]any{"tokenizer": "character"})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.Tokenizer != "character" {
		t.Errorf("expected tokenizer 'character', got %q", cfg.Tokenizer)
	}

	// Test invalid tokenizer is rejected
	_, err = Parse(map[string]any{"tokenizer": "invalid_tokenizer"})
	if err == nil {
		t.Error("expected error for invalid tokenizer")
	}
}

func TestFullTextSearchObjectCaseSensitive(t *testing.T) {
	// Default is false
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.CaseSensitive != false {
		t.Error("default case_sensitive should be false")
	}

	// Explicit true
	cfg, err = Parse(map[string]any{"case_sensitive": true})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.CaseSensitive != true {
		t.Error("case_sensitive should be true when set")
	}

	// Explicit false
	cfg, err = Parse(map[string]any{"case_sensitive": false})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.CaseSensitive != false {
		t.Error("case_sensitive should be false when set")
	}
}

func TestFullTextSearchObjectLanguage(t *testing.T) {
	// Test various languages
	languages := []string{
		"english", "german", "french", "spanish",
		"italian", "portuguese", "dutch", "russian",
	}

	for _, lang := range languages {
		cfg, err := Parse(map[string]any{"language": lang})
		if err != nil {
			t.Errorf("Parse failed for language %q: %v", lang, err)
			continue
		}
		if cfg.Language != lang {
			t.Errorf("expected language %q, got %q", lang, cfg.Language)
		}
	}

	// Test invalid language is rejected
	_, err := Parse(map[string]any{"language": "invalid_language"})
	if err == nil {
		t.Error("expected error for invalid language")
	}
}

// Step 3: Test stemming, remove_stopwords, ascii_folding options
func TestFullTextSearchStemmingOption(t *testing.T) {
	// Default is true
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.Stemming != true {
		t.Error("default stemming should be true")
	}

	// Explicit false
	cfg, err = Parse(map[string]any{"stemming": false})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.Stemming != false {
		t.Error("stemming should be false when set")
	}

	// Explicit true
	cfg, err = Parse(map[string]any{"stemming": true})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.Stemming != true {
		t.Error("stemming should be true when set")
	}
}

func TestFullTextSearchRemoveStopwordsOption(t *testing.T) {
	// Default is true
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.RemoveStopwords != true {
		t.Error("default remove_stopwords should be true")
	}

	// Explicit false
	cfg, err = Parse(map[string]any{"remove_stopwords": false})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.RemoveStopwords != false {
		t.Error("remove_stopwords should be false when set")
	}

	// Explicit true
	cfg, err = Parse(map[string]any{"remove_stopwords": true})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.RemoveStopwords != true {
		t.Error("remove_stopwords should be true when set")
	}
}

func TestFullTextSearchASCIIFoldingOption(t *testing.T) {
	// Default is false
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.ASCIIFolding != false {
		t.Error("default ascii_folding should be false")
	}

	// Explicit true
	cfg, err = Parse(map[string]any{"ascii_folding": true})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.ASCIIFolding != true {
		t.Error("ascii_folding should be true when set")
	}

	// Explicit false
	cfg, err = Parse(map[string]any{"ascii_folding": false})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.ASCIIFolding != false {
		t.Error("ascii_folding should be false when set")
	}
}

// Step 4: Verify BM25 params k1, b configuration
func TestBM25ParamsK1Configuration(t *testing.T) {
	// Default k1 is 1.2
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.K1 != 1.2 {
		t.Errorf("default k1 should be 1.2, got %f", cfg.K1)
	}

	// Custom k1 values
	testCases := []float64{0.0, 0.5, 1.0, 1.5, 2.0, 2.5}
	for _, k1 := range testCases {
		cfg, err = Parse(map[string]any{"k1": k1})
		if err != nil {
			t.Errorf("Parse failed for k1=%f: %v", k1, err)
			continue
		}
		if cfg.K1 != k1 {
			t.Errorf("expected k1=%f, got %f", k1, cfg.K1)
		}
	}

	// Negative k1 is invalid
	_, err = Parse(map[string]any{"k1": -1.0})
	if err == nil {
		t.Error("expected error for negative k1")
	}

	// k1 > 10 is invalid
	_, err = Parse(map[string]any{"k1": 15.0})
	if err == nil {
		t.Error("expected error for k1 > 10")
	}
}

func TestBM25ParamsBConfiguration(t *testing.T) {
	// Default b is 0.75
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.B != 0.75 {
		t.Errorf("default b should be 0.75, got %f", cfg.B)
	}

	// Custom b values (must be between 0 and 1)
	testCases := []float64{0.0, 0.25, 0.5, 0.75, 1.0}
	for _, b := range testCases {
		cfg, err = Parse(map[string]any{"b": b})
		if err != nil {
			t.Errorf("Parse failed for b=%f: %v", b, err)
			continue
		}
		if cfg.B != b {
			t.Errorf("expected b=%f, got %f", b, cfg.B)
		}
	}

	// Negative b is invalid
	_, err = Parse(map[string]any{"b": -0.1})
	if err == nil {
		t.Error("expected error for negative b")
	}

	// b > 1 is invalid
	_, err = Parse(map[string]any{"b": 1.5})
	if err == nil {
		t.Error("expected error for b > 1")
	}
}

// Integration test: all options combined
func TestFullTextSearchCompleteConfiguration(t *testing.T) {
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

	cfg, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Verify all options
	if cfg.Tokenizer != "whitespace" {
		t.Errorf("expected tokenizer 'whitespace', got %q", cfg.Tokenizer)
	}
	if cfg.CaseSensitive != true {
		t.Error("expected case_sensitive=true")
	}
	if cfg.Language != "german" {
		t.Errorf("expected language 'german', got %q", cfg.Language)
	}
	if cfg.Stemming != false {
		t.Error("expected stemming=false")
	}
	if cfg.RemoveStopwords != false {
		t.Error("expected remove_stopwords=false")
	}
	if cfg.ASCIIFolding != true {
		t.Error("expected ascii_folding=true")
	}
	if cfg.K1 != 1.5 {
		t.Errorf("expected k1=1.5, got %f", cfg.K1)
	}
	if cfg.B != 0.5 {
		t.Errorf("expected b=0.5, got %f", cfg.B)
	}
}
