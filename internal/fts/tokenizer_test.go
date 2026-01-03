package fts

import (
	"reflect"
	"testing"
)

func TestWordV3Tokenizer(t *testing.T) {
	baseCfg := DefaultConfig()
	baseCfg.RemoveStopwords = false
	baseCfg.Stemming = false

	tests := []struct {
		name     string
		config   *Config
		input    string
		expected []string
	}{
		{
			name:     "simple words",
			config:   baseCfg,
			input:    "hello world",
			expected: []string{"hello", "world"},
		},
		{
			name:     "with punctuation",
			config:   baseCfg,
			input:    "Hello, World! How are you?",
			expected: []string{"hello", "world", "how", "are", "you"},
		},
		{
			name:     "with numbers",
			config:   baseCfg,
			input:    "test123 user42",
			expected: []string{"test123", "user42"},
		},
		{
			name:     "case sensitive",
			config:   &Config{Tokenizer: "word_v3", CaseSensitive: true, Stemming: false, RemoveStopwords: false},
			input:    "Hello World",
			expected: []string{"Hello", "World"},
		},
		{
			name:     "empty string",
			config:   baseCfg,
			input:    "",
			expected: nil,
		},
		{
			name:     "only punctuation",
			config:   baseCfg,
			input:    "!@#$%",
			expected: nil,
		},
		{
			name:     "unicode",
			config:   baseCfg,
			input:    "café résumé",
			expected: []string{"café", "résumé"},
		},
		{
			name:     "ascii folding",
			config:   &Config{Tokenizer: "word_v3", ASCIIFolding: true, Stemming: false, RemoveStopwords: false},
			input:    "café résumé",
			expected: []string{"cafe", "resume"},
		},
		{
			name:     "underscores in words",
			config:   baseCfg,
			input:    "user_id field_name",
			expected: []string{"user_id", "field_name"},
		},
		{
			name:     "mixed content",
			config:   baseCfg,
			input:    "The quick brown fox jumps over 42 lazy dogs!",
			expected: []string{"the", "quick", "brown", "fox", "jumps", "over", "42", "lazy", "dogs"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.config)
			result := tokenizer.Tokenize(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Tokenize(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestWhitespaceTokenizer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple",
			input:    "hello world",
			expected: []string{"hello", "world"},
		},
		{
			name:     "keeps punctuation",
			input:    "hello, world!",
			expected: []string{"hello,", "world!"},
		},
		{
			name:     "multiple spaces",
			input:    "hello    world",
			expected: []string{"hello", "world"},
		},
	}

	cfg := &Config{Tokenizer: "whitespace", RemoveStopwords: false, Stemming: false}
	tokenizer := NewTokenizer(cfg)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tokenizer.Tokenize(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Tokenize(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestCharacterTokenizer(t *testing.T) {
	cfg := &Config{Tokenizer: "character", RemoveStopwords: false, Stemming: false}
	tokenizer := NewTokenizer(cfg)

	input := "abc"
	expected := []string{"a", "b", "c"}

	result := tokenizer.Tokenize(input)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Tokenize(%q) = %v, want %v", input, result, expected)
	}
}

func TestNgramTokenizer(t *testing.T) {
	cfg := &Config{Tokenizer: "ngram", RemoveStopwords: false, Stemming: false}
	tokenizer := NewTokenizer(cfg)

	input := "hello"
	expected := []string{"hel", "ell", "llo"}

	result := tokenizer.Tokenize(input)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Tokenize(%q) = %v, want %v", input, result, expected)
	}
}

func TestStopwords(t *testing.T) {
	english := stopwordsForLanguage("english")
	stopwords := []string{"the", "a", "an", "is", "are", "and", "or", "but"}
	for _, sw := range stopwords {
		if !IsStopword(sw, english) {
			t.Errorf("expected %q to be a stopword", sw)
		}
	}

	nonStopwords := []string{"hello", "world", "search", "index"}
	for _, w := range nonStopwords {
		if IsStopword(w, english) {
			t.Errorf("did not expect %q to be a stopword", w)
		}
	}
}

func TestRemoveStopwords(t *testing.T) {
	tokens := []string{"the", "quick", "brown", "fox", "is", "a", "dog"}
	expected := []string{"quick", "brown", "fox", "dog"}

	result := RemoveStopwords(tokens, stopwordsForLanguage("english"))
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("RemoveStopwords() = %v, want %v", result, expected)
	}
}

func TestTokenizerSpanishStopwords(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Language = "spanish"
	cfg.RemoveStopwords = true
	cfg.Stemming = false

	tokenizer := NewTokenizer(cfg)
	input := "el rapido zorro y la luna"
	expected := []string{"rapido", "zorro", "luna"}
	result := tokenizer.Tokenize(input)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Tokenize(%q) = %v, want %v", input, result, expected)
	}
}

func TestTokenizerStemmingToggle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RemoveStopwords = false
	cfg.Stemming = true

	tokenizer := NewTokenizer(cfg)
	input := "running runs"
	expected := []string{"run", "run"}
	result := tokenizer.Tokenize(input)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Tokenize(%q) = %v, want %v", input, result, expected)
	}

	cfg.Stemming = false
	tokenizer = NewTokenizer(cfg)
	expected = []string{"running", "runs"}
	result = tokenizer.Tokenize(input)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Tokenize(%q) without stemming = %v, want %v", input, result, expected)
	}
}

func TestWhitespaceTokenizerASCIIFolding(t *testing.T) {
	cfg := &Config{Tokenizer: "whitespace", ASCIIFolding: true, RemoveStopwords: false, Stemming: false}
	tokenizer := NewTokenizer(cfg)

	input := "café résumé"
	expected := []string{"cafe", "resume"}
	result := tokenizer.Tokenize(input)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Tokenize(%q) = %v, want %v", input, result, expected)
	}
}

func TestASCIIFolding(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"café", "cafe"},
		{"résumé", "resume"},
		{"naïve", "naive"},
		{"Über", "Uber"},
		{"fiancée", "fiancee"},
		{"coöperate", "cooperate"},
		{"straße", "strasse"},
		{"señor", "senor"},
	}

	for _, tt := range tests {
		result := foldASCII(tt.input)
		if result != tt.expected {
			t.Errorf("foldASCII(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
