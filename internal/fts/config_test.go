package fts

import (
	"testing"
)

// Test full_text_search: true enables FTS
func TestParseTrue(t *testing.T) {
	cfg, err := Parse(true)
	if err != nil {
		t.Fatalf("Parse(true) failed: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if !cfg.IsEnabled() {
		t.Error("expected IsEnabled() to return true")
	}
}

// Test full_text_search: false disables FTS
func TestParseFalse(t *testing.T) {
	cfg, err := Parse(false)
	if err != nil {
		t.Fatalf("Parse(false) failed: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil config for false")
	}
}

func TestParseNil(t *testing.T) {
	cfg, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse(nil) failed: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil config for nil")
	}
}

// Test full_text_search object options: tokenizer, case_sensitive, language
func TestParseObjectTokenizer(t *testing.T) {
	tests := []struct {
		tokenizer string
		wantErr   bool
	}{
		{"word_v3", false},
		{"whitespace", false},
		{"character", false},
		{"ngram", false},
		{"invalid_tokenizer", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.tokenizer, func(t *testing.T) {
			input := map[string]any{"tokenizer": tt.tokenizer}
			cfg, err := Parse(input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error for invalid tokenizer")
				}
			} else {
				if err != nil {
					t.Fatalf("Parse failed: %v", err)
				}
				if cfg.Tokenizer != tt.tokenizer {
					t.Errorf("expected tokenizer %q, got %q", tt.tokenizer, cfg.Tokenizer)
				}
			}
		})
	}
}

func TestParseObjectCaseSensitive(t *testing.T) {
	// Default is false
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.CaseSensitive {
		t.Error("expected default case_sensitive=false")
	}

	// Explicit true
	cfg, err = Parse(map[string]any{"case_sensitive": true})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if !cfg.CaseSensitive {
		t.Error("expected case_sensitive=true")
	}

	// Explicit false
	cfg, err = Parse(map[string]any{"case_sensitive": false})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.CaseSensitive {
		t.Error("expected case_sensitive=false")
	}
}

func TestParseObjectLanguage(t *testing.T) {
	tests := []struct {
		language string
		wantErr  bool
	}{
		{"english", false},
		{"german", false},
		{"french", false},
		{"spanish", false},
		{"italian", false},
		{"portuguese", false},
		{"dutch", false},
		{"russian", false},
		{"swedish", false},
		{"norwegian", false},
		{"danish", false},
		{"finnish", false},
		{"arabic", false},
		{"chinese", false},
		{"japanese", false},
		{"korean", false},
		{"invalid_language", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.language, func(t *testing.T) {
			input := map[string]any{"language": tt.language}
			cfg, err := Parse(input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error for invalid language")
				}
			} else {
				if err != nil {
					t.Fatalf("Parse failed: %v", err)
				}
				if cfg.Language != tt.language {
					t.Errorf("expected language %q, got %q", tt.language, cfg.Language)
				}
			}
		})
	}
}

// Test stemming, remove_stopwords, ascii_folding options
func TestParseObjectStemming(t *testing.T) {
	// Default is true
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if !cfg.Stemming {
		t.Error("expected default stemming=true")
	}

	// Explicit false
	cfg, err = Parse(map[string]any{"stemming": false})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.Stemming {
		t.Error("expected stemming=false")
	}

	// Explicit true
	cfg, err = Parse(map[string]any{"stemming": true})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if !cfg.Stemming {
		t.Error("expected stemming=true")
	}
}

func TestParseObjectRemoveStopwords(t *testing.T) {
	// Default is true
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if !cfg.RemoveStopwords {
		t.Error("expected default remove_stopwords=true")
	}

	// Explicit false
	cfg, err = Parse(map[string]any{"remove_stopwords": false})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.RemoveStopwords {
		t.Error("expected remove_stopwords=false")
	}

	// Explicit true
	cfg, err = Parse(map[string]any{"remove_stopwords": true})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if !cfg.RemoveStopwords {
		t.Error("expected remove_stopwords=true")
	}
}

func TestParseObjectASCIIFolding(t *testing.T) {
	// Default is false
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.ASCIIFolding {
		t.Error("expected default ascii_folding=false")
	}

	// Explicit true
	cfg, err = Parse(map[string]any{"ascii_folding": true})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if !cfg.ASCIIFolding {
		t.Error("expected ascii_folding=true")
	}

	// Explicit false
	cfg, err = Parse(map[string]any{"ascii_folding": false})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.ASCIIFolding {
		t.Error("expected ascii_folding=false")
	}
}

// Test BM25 params k1, b configuration
func TestParseObjectBM25K1(t *testing.T) {
	// Default is 1.2
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.K1 != 1.2 {
		t.Errorf("expected default k1=1.2, got %f", cfg.K1)
	}

	// Custom k1
	cfg, err = Parse(map[string]any{"k1": 1.5})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.K1 != 1.5 {
		t.Errorf("expected k1=1.5, got %f", cfg.K1)
	}

	// Zero is valid (no term frequency saturation)
	cfg, err = Parse(map[string]any{"k1": 0.0})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.K1 != 0.0 {
		t.Errorf("expected k1=0.0, got %f", cfg.K1)
	}

	// Negative k1 is invalid
	_, err = Parse(map[string]any{"k1": -1.0})
	if err == nil {
		t.Error("expected error for negative k1")
	}

	// Very large k1 is invalid
	_, err = Parse(map[string]any{"k1": 100.0})
	if err == nil {
		t.Error("expected error for k1 > 10")
	}
}

func TestParseObjectBM25B(t *testing.T) {
	// Default is 0.75
	cfg, err := Parse(map[string]any{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.B != 0.75 {
		t.Errorf("expected default b=0.75, got %f", cfg.B)
	}

	// Custom b
	cfg, err = Parse(map[string]any{"b": 0.5})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.B != 0.5 {
		t.Errorf("expected b=0.5, got %f", cfg.B)
	}

	// Zero is valid (no length normalization)
	cfg, err = Parse(map[string]any{"b": 0.0})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.B != 0.0 {
		t.Errorf("expected b=0.0, got %f", cfg.B)
	}

	// One is valid (full length normalization)
	cfg, err = Parse(map[string]any{"b": 1.0})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if cfg.B != 1.0 {
		t.Errorf("expected b=1.0, got %f", cfg.B)
	}

	// Negative b is invalid
	_, err = Parse(map[string]any{"b": -0.1})
	if err == nil {
		t.Error("expected error for negative b")
	}

	// b > 1 is invalid
	_, err = Parse(map[string]any{"b": 1.1})
	if err == nil {
		t.Error("expected error for b > 1")
	}
}

// Test combined configuration
func TestParseObjectCombined(t *testing.T) {
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

	if cfg.Tokenizer != "whitespace" {
		t.Errorf("expected tokenizer 'whitespace', got %q", cfg.Tokenizer)
	}
	if !cfg.CaseSensitive {
		t.Error("expected case_sensitive=true")
	}
	if cfg.Language != "german" {
		t.Errorf("expected language 'german', got %q", cfg.Language)
	}
	if cfg.Stemming {
		t.Error("expected stemming=false")
	}
	if cfg.RemoveStopwords {
		t.Error("expected remove_stopwords=false")
	}
	if !cfg.ASCIIFolding {
		t.Error("expected ascii_folding=true")
	}
	if cfg.K1 != 1.5 {
		t.Errorf("expected k1=1.5, got %f", cfg.K1)
	}
	if cfg.B != 0.5 {
		t.Errorf("expected b=0.5, got %f", cfg.B)
	}
}

// Test partial configuration uses defaults for unspecified options
func TestParseObjectPartial(t *testing.T) {
	input := map[string]any{
		"tokenizer": "character",
	}
	cfg, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Specified value
	if cfg.Tokenizer != "character" {
		t.Errorf("expected tokenizer 'character', got %q", cfg.Tokenizer)
	}

	// Default values for unspecified
	if cfg.CaseSensitive {
		t.Error("expected default case_sensitive=false")
	}
	if cfg.Language != "english" {
		t.Errorf("expected default language 'english', got %q", cfg.Language)
	}
	if !cfg.Stemming {
		t.Error("expected default stemming=true")
	}
	if !cfg.RemoveStopwords {
		t.Error("expected default remove_stopwords=true")
	}
	if cfg.ASCIIFolding {
		t.Error("expected default ascii_folding=false")
	}
	if cfg.K1 != 1.2 {
		t.Errorf("expected default k1=1.2, got %f", cfg.K1)
	}
	if cfg.B != 0.75 {
		t.Errorf("expected default b=0.75, got %f", cfg.B)
	}
}

// Test invalid input types
func TestParseInvalidType(t *testing.T) {
	_, err := Parse("invalid")
	if err == nil {
		t.Error("expected error for string type")
	}

	_, err = Parse(123)
	if err == nil {
		t.Error("expected error for int type")
	}

	_, err = Parse([]string{"a", "b"})
	if err == nil {
		t.Error("expected error for slice type")
	}
}

// Test DefaultConfig creates proper defaults
func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

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

// Test Validate
func TestValidate(t *testing.T) {
	// Valid config
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid config should not error: %v", err)
	}

	// Invalid tokenizer
	cfg = DefaultConfig()
	cfg.Tokenizer = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid tokenizer")
	}

	// Empty tokenizer
	cfg = DefaultConfig()
	cfg.Tokenizer = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty tokenizer")
	}

	// Invalid language
	cfg = DefaultConfig()
	cfg.Language = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid language")
	}

	// Empty language
	cfg = DefaultConfig()
	cfg.Language = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty language")
	}

	// Negative k1
	cfg = DefaultConfig()
	cfg.K1 = -1.0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative k1")
	}

	// k1 too large
	cfg = DefaultConfig()
	cfg.K1 = 15.0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for k1 > 10")
	}

	// Negative b
	cfg = DefaultConfig()
	cfg.B = -0.1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative b")
	}

	// b > 1
	cfg = DefaultConfig()
	cfg.B = 1.5
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for b > 1")
	}
}

// Test Clone
func TestClone(t *testing.T) {
	orig := &Config{
		Tokenizer:       "whitespace",
		CaseSensitive:   true,
		Language:        "german",
		Stemming:        false,
		RemoveStopwords: false,
		ASCIIFolding:    true,
		K1:              1.5,
		B:               0.5,
	}

	clone := orig.Clone()

	// Verify values are copied
	if clone.Tokenizer != orig.Tokenizer {
		t.Error("Tokenizer not cloned")
	}
	if clone.CaseSensitive != orig.CaseSensitive {
		t.Error("CaseSensitive not cloned")
	}
	if clone.Language != orig.Language {
		t.Error("Language not cloned")
	}
	if clone.Stemming != orig.Stemming {
		t.Error("Stemming not cloned")
	}
	if clone.RemoveStopwords != orig.RemoveStopwords {
		t.Error("RemoveStopwords not cloned")
	}
	if clone.ASCIIFolding != orig.ASCIIFolding {
		t.Error("ASCIIFolding not cloned")
	}
	if clone.K1 != orig.K1 {
		t.Error("K1 not cloned")
	}
	if clone.B != orig.B {
		t.Error("B not cloned")
	}

	// Verify independence
	clone.Tokenizer = "character"
	if orig.Tokenizer == "character" {
		t.Error("Tokenizer not independent")
	}
}

func TestCloneNil(t *testing.T) {
	var cfg *Config
	clone := cfg.Clone()
	if clone != nil {
		t.Error("nil clone should return nil")
	}
}

// Test Equal
func TestEqual(t *testing.T) {
	cfg1 := DefaultConfig()
	cfg2 := DefaultConfig()

	if !cfg1.Equal(cfg2) {
		t.Error("identical configs should be equal")
	}

	cfg2.Tokenizer = "whitespace"
	if cfg1.Equal(cfg2) {
		t.Error("different tokenizer should not be equal")
	}

	cfg2 = DefaultConfig()
	cfg2.K1 = 1.5
	if cfg1.Equal(cfg2) {
		t.Error("different k1 should not be equal")
	}
}

func TestEqualNil(t *testing.T) {
	var cfg1, cfg2 *Config

	if !cfg1.Equal(cfg2) {
		t.Error("both nil should be equal")
	}

	cfg1 = DefaultConfig()
	if cfg1.Equal(cfg2) {
		t.Error("nil and non-nil should not be equal")
	}
	if cfg2.Equal(cfg1) {
		t.Error("nil and non-nil should not be equal")
	}
}

// Test ToMap
func TestToMap(t *testing.T) {
	cfg := &Config{
		Tokenizer:       "whitespace",
		CaseSensitive:   true,
		Language:        "german",
		Stemming:        false,
		RemoveStopwords: false,
		ASCIIFolding:    true,
		K1:              1.5,
		B:               0.5,
	}

	m := cfg.ToMap()

	if m["tokenizer"] != "whitespace" {
		t.Error("tokenizer not mapped")
	}
	if m["case_sensitive"] != true {
		t.Error("case_sensitive not mapped")
	}
	if m["language"] != "german" {
		t.Error("language not mapped")
	}
	if m["stemming"] != false {
		t.Error("stemming not mapped")
	}
	if m["remove_stopwords"] != false {
		t.Error("remove_stopwords not mapped")
	}
	if m["ascii_folding"] != true {
		t.Error("ascii_folding not mapped")
	}
	if m["k1"] != 1.5 {
		t.Error("k1 not mapped")
	}
	if m["b"] != 0.5 {
		t.Error("b not mapped")
	}
}

func TestToMapNil(t *testing.T) {
	var cfg *Config
	m := cfg.ToMap()
	if m != nil {
		t.Error("nil config should return nil map")
	}
}

// Test supported tokenizers
func TestSupportedTokenizers(t *testing.T) {
	for tok := range SupportedTokenizers {
		cfg := DefaultConfig()
		cfg.Tokenizer = tok
		if err := cfg.Validate(); err != nil {
			t.Errorf("tokenizer %q should be valid: %v", tok, err)
		}
	}
}

// Test supported languages
func TestSupportedLanguages(t *testing.T) {
	for lang := range SupportedLanguages {
		cfg := DefaultConfig()
		cfg.Language = lang
		if err := cfg.Validate(); err != nil {
			t.Errorf("language %q should be valid: %v", lang, err)
		}
	}
}

// Test IsEnabled
func TestIsEnabled(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.IsEnabled() {
		t.Error("non-nil config should be enabled")
	}

	var nilCfg *Config
	if nilCfg.IsEnabled() {
		t.Error("nil config should not be enabled")
	}
}
