// Package fts provides full-text search configuration and utilities.
package fts

import (
	"fmt"
)

// Config represents full-text search configuration for an attribute.
type Config struct {
	// Core options
	Tokenizer       string  // default "word_v3"
	CaseSensitive   bool    // default false
	Language        string  // default "english"
	Stemming        bool    // default true
	RemoveStopwords bool    // default true
	ASCIIFolding    bool    // default false

	// BM25 parameters
	K1 float64 // default 1.2, controls term frequency saturation
	B  float64 // default 0.75, controls document length normalization
}

// Supported tokenizers
var SupportedTokenizers = map[string]bool{
	"word_v3":    true, // Default: word boundaries with smart handling
	"whitespace": true, // Split on whitespace only
	"character":  true, // Character-level tokens
	"ngram":      true, // N-gram tokenization
}

// Supported languages for stemming and stopwords
var SupportedLanguages = map[string]bool{
	"english":    true,
	"german":     true,
	"french":     true,
	"spanish":    true,
	"italian":    true,
	"portuguese": true,
	"dutch":      true,
	"russian":    true,
	"swedish":    true,
	"norwegian":  true,
	"danish":     true,
	"finnish":    true,
	"arabic":     true,
	"chinese":    true,
	"japanese":   true,
	"korean":     true,
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		Tokenizer:       "word_v3",
		CaseSensitive:   false,
		Language:        "english",
		Stemming:        true,
		RemoveStopwords: true,
		ASCIIFolding:    false,
		K1:              1.2,
		B:               0.75,
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.Tokenizer == "" {
		return fmt.Errorf("tokenizer cannot be empty")
	}
	if !SupportedTokenizers[c.Tokenizer] {
		return fmt.Errorf("unsupported tokenizer: %q", c.Tokenizer)
	}

	if c.Language == "" {
		return fmt.Errorf("language cannot be empty")
	}
	if !SupportedLanguages[c.Language] {
		return fmt.Errorf("unsupported language: %q", c.Language)
	}

	// BM25 parameter validation
	if c.K1 < 0 {
		return fmt.Errorf("k1 must be non-negative, got %f", c.K1)
	}
	if c.K1 > 10 {
		return fmt.Errorf("k1 should be <= 10, got %f", c.K1)
	}

	if c.B < 0 || c.B > 1 {
		return fmt.Errorf("b must be between 0 and 1, got %f", c.B)
	}

	return nil
}

// Clone creates a deep copy of the config.
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	return &Config{
		Tokenizer:       c.Tokenizer,
		CaseSensitive:   c.CaseSensitive,
		Language:        c.Language,
		Stemming:        c.Stemming,
		RemoveStopwords: c.RemoveStopwords,
		ASCIIFolding:    c.ASCIIFolding,
		K1:              c.K1,
		B:               c.B,
	}
}

// Equal checks if two configs are equal.
func (c *Config) Equal(other *Config) bool {
	if c == nil && other == nil {
		return true
	}
	if c == nil || other == nil {
		return false
	}
	return c.Tokenizer == other.Tokenizer &&
		c.CaseSensitive == other.CaseSensitive &&
		c.Language == other.Language &&
		c.Stemming == other.Stemming &&
		c.RemoveStopwords == other.RemoveStopwords &&
		c.ASCIIFolding == other.ASCIIFolding &&
		c.K1 == other.K1 &&
		c.B == other.B
}

// Parse parses a full_text_search value which can be:
// - bool: true means use default config, false means disabled
// - map: custom configuration options
func Parse(v any) (*Config, error) {
	if v == nil {
		return nil, nil
	}

	switch val := v.(type) {
	case bool:
		if val {
			return DefaultConfig(), nil
		}
		return nil, nil

	case map[string]any:
		cfg := DefaultConfig()

		if tokenizer, ok := val["tokenizer"].(string); ok {
			cfg.Tokenizer = tokenizer
		}
		if caseSensitive, ok := val["case_sensitive"].(bool); ok {
			cfg.CaseSensitive = caseSensitive
		}
		if language, ok := val["language"].(string); ok {
			cfg.Language = language
		}
		if stemming, ok := val["stemming"].(bool); ok {
			cfg.Stemming = stemming
		}
		if removeStopwords, ok := val["remove_stopwords"].(bool); ok {
			cfg.RemoveStopwords = removeStopwords
		}
		if asciiFolding, ok := val["ascii_folding"].(bool); ok {
			cfg.ASCIIFolding = asciiFolding
		}
		if k1, ok := val["k1"].(float64); ok {
			cfg.K1 = k1
		}
		if b, ok := val["b"].(float64); ok {
			cfg.B = b
		}

		if err := cfg.Validate(); err != nil {
			return nil, err
		}

		return cfg, nil

	default:
		return nil, fmt.Errorf("full_text_search must be a boolean or object, got %T", v)
	}
}

// IsEnabled returns true if the config represents an enabled FTS configuration.
func (c *Config) IsEnabled() bool {
	return c != nil
}

// ToMap converts the config to a map for JSON serialization.
func (c *Config) ToMap() map[string]any {
	if c == nil {
		return nil
	}
	return map[string]any{
		"tokenizer":        c.Tokenizer,
		"case_sensitive":   c.CaseSensitive,
		"language":         c.Language,
		"stemming":         c.Stemming,
		"remove_stopwords": c.RemoveStopwords,
		"ascii_folding":    c.ASCIIFolding,
		"k1":               c.K1,
		"b":                c.B,
	}
}
