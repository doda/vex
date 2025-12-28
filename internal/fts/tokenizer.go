// Package fts provides full-text search indexing and tokenization.
package fts

import (
	"strings"
	"unicode"
)

// Tokenizer defines the interface for text tokenization.
type Tokenizer interface {
	// Tokenize splits text into tokens.
	Tokenize(text string) []string
}

// NewTokenizer creates a tokenizer based on the config.
func NewTokenizer(cfg *Config) Tokenizer {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	switch cfg.Tokenizer {
	case "whitespace":
		return &whitespaceTokenizer{caseSensitive: cfg.CaseSensitive}
	case "character":
		return &characterTokenizer{caseSensitive: cfg.CaseSensitive}
	case "ngram":
		return &ngramTokenizer{n: 3, caseSensitive: cfg.CaseSensitive}
	default:
		// word_v3 is the default
		return &wordV3Tokenizer{
			caseSensitive: cfg.CaseSensitive,
			asciiFolding:  cfg.ASCIIFolding,
		}
	}
}

// wordV3Tokenizer implements the word_v3 tokenizer which splits on word boundaries
// with smart handling of punctuation, numbers, and Unicode.
type wordV3Tokenizer struct {
	caseSensitive bool
	asciiFolding  bool
}

// Tokenize splits text on word boundaries.
// word_v3 is the default tokenizer that:
// - Splits on whitespace and punctuation
// - Keeps numbers intact
// - Handles Unicode properly
// - Optionally lowercases (default) and ASCII-folds
func (t *wordV3Tokenizer) Tokenize(text string) []string {
	if text == "" {
		return nil
	}

	if t.asciiFolding {
		text = foldASCII(text)
	}

	if !t.caseSensitive {
		text = strings.ToLower(text)
	}

	var tokens []string
	var current strings.Builder

	for _, r := range text {
		if isWordChar(r) {
			current.WriteRune(r)
		} else {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		}
	}

	// Don't forget the last token
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens
}

// isWordChar returns true if the rune should be part of a word token.
func isWordChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

// whitespaceTokenizer splits on whitespace only.
type whitespaceTokenizer struct {
	caseSensitive bool
}

func (t *whitespaceTokenizer) Tokenize(text string) []string {
	if text == "" {
		return nil
	}

	if !t.caseSensitive {
		text = strings.ToLower(text)
	}

	fields := strings.Fields(text)
	if len(fields) == 0 {
		return nil
	}
	return fields
}

// characterTokenizer produces individual character tokens.
type characterTokenizer struct {
	caseSensitive bool
}

func (t *characterTokenizer) Tokenize(text string) []string {
	if text == "" {
		return nil
	}

	if !t.caseSensitive {
		text = strings.ToLower(text)
	}

	var tokens []string
	for _, r := range text {
		if !unicode.IsSpace(r) {
			tokens = append(tokens, string(r))
		}
	}
	return tokens
}

// ngramTokenizer produces n-gram tokens.
type ngramTokenizer struct {
	n             int
	caseSensitive bool
}

func (t *ngramTokenizer) Tokenize(text string) []string {
	if text == "" {
		return nil
	}

	if !t.caseSensitive {
		text = strings.ToLower(text)
	}

	// Remove spaces for ngram
	text = strings.ReplaceAll(text, " ", "")
	if len(text) == 0 {
		return nil
	}

	runes := []rune(text)
	if len(runes) < t.n {
		return []string{text}
	}

	var tokens []string
	for i := 0; i <= len(runes)-t.n; i++ {
		tokens = append(tokens, string(runes[i:i+t.n]))
	}
	return tokens
}

// foldASCII converts common Unicode characters to ASCII equivalents.
func foldASCII(s string) string {
	var result strings.Builder
	result.Grow(len(s))

	for _, r := range s {
		if r < 128 {
			result.WriteRune(r)
			continue
		}
		// Common foldings
		switch r {
		case 'à', 'á', 'â', 'ã', 'ä', 'å', 'æ':
			result.WriteRune('a')
		case 'ç':
			result.WriteRune('c')
		case 'è', 'é', 'ê', 'ë':
			result.WriteRune('e')
		case 'ì', 'í', 'î', 'ï':
			result.WriteRune('i')
		case 'ñ':
			result.WriteRune('n')
		case 'ò', 'ó', 'ô', 'õ', 'ö', 'ø':
			result.WriteRune('o')
		case 'ù', 'ú', 'û', 'ü':
			result.WriteRune('u')
		case 'ý', 'ÿ':
			result.WriteRune('y')
		case 'ß':
			result.WriteString("ss")
		case 'À', 'Á', 'Â', 'Ã', 'Ä', 'Å', 'Æ':
			result.WriteRune('A')
		case 'Ç':
			result.WriteRune('C')
		case 'È', 'É', 'Ê', 'Ë':
			result.WriteRune('E')
		case 'Ì', 'Í', 'Î', 'Ï':
			result.WriteRune('I')
		case 'Ñ':
			result.WriteRune('N')
		case 'Ò', 'Ó', 'Ô', 'Õ', 'Ö', 'Ø':
			result.WriteRune('O')
		case 'Ù', 'Ú', 'Û', 'Ü':
			result.WriteRune('U')
		case 'Ý':
			result.WriteRune('Y')
		default:
			// Keep other characters as-is
			result.WriteRune(r)
		}
	}
	return result.String()
}

// English stopwords (common words that often don't add meaning to searches)
var englishStopwords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true,
	"at": true, "be": true, "by": true, "for": true, "from": true,
	"has": true, "he": true, "in": true, "is": true, "it": true,
	"its": true, "of": true, "on": true, "that": true, "the": true,
	"to": true, "was": true, "were": true, "will": true, "with": true,
	"i": true, "me": true, "my": true, "we": true, "our": true,
	"you": true, "your": true, "they": true, "their": true, "this": true,
	"but": true, "or": true, "not": true, "no": true, "so": true,
	"if": true, "do": true, "does": true, "did": true, "have": true,
	"had": true, "been": true, "being": true, "which": true, "who": true,
	"what": true, "when": true, "where": true, "how": true, "why": true,
	"all": true, "each": true, "some": true, "any": true, "most": true,
	"other": true, "such": true, "only": true, "own": true, "same": true,
	"than": true, "too": true, "very": true, "can": true, "just": true,
	"should": true, "now": true, "also": true, "more": true,
}

// IsStopword returns true if the token is a common English stopword.
func IsStopword(token string) bool {
	return englishStopwords[strings.ToLower(token)]
}

// RemoveStopwords filters stopwords from a token list.
func RemoveStopwords(tokens []string) []string {
	result := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if !IsStopword(t) {
			result = append(result, t)
		}
	}
	return result
}
