// Package fts provides full-text search indexing and tokenization.
package fts

import (
	"strings"
	"unicode"

	"github.com/kljensen/snowball"
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

	var base Tokenizer
	switch cfg.Tokenizer {
	case "whitespace":
		base = &whitespaceTokenizer{caseSensitive: cfg.CaseSensitive, asciiFolding: cfg.ASCIIFolding}
	case "character":
		base = &characterTokenizer{caseSensitive: cfg.CaseSensitive, asciiFolding: cfg.ASCIIFolding}
	case "ngram":
		base = &ngramTokenizer{n: 3, caseSensitive: cfg.CaseSensitive, asciiFolding: cfg.ASCIIFolding}
	default:
		// word_v3 is the default
		base = &wordV3Tokenizer{
			caseSensitive: cfg.CaseSensitive,
			asciiFolding:  cfg.ASCIIFolding,
		}
	}

	return &configTokenizer{
		base:         base,
		cfg:          cfg,
		stopwords:    stopwordsForLanguage(cfg.Language),
		stemLanguage: stemLanguage(cfg.Language),
	}
}

type configTokenizer struct {
	base         Tokenizer
	cfg          *Config
	stopwords    map[string]struct{}
	stemLanguage string
}

func (t *configTokenizer) Tokenize(text string) []string {
	if t.base == nil {
		return nil
	}

	tokens := t.base.Tokenize(text)
	if len(tokens) == 0 {
		return tokens
	}

	if t.cfg.RemoveStopwords && len(t.stopwords) > 0 {
		tokens = RemoveStopwords(tokens, t.stopwords)
	}

	if t.cfg.Stemming && t.stemLanguage != "" {
		tokens = StemTokens(tokens, t.stemLanguage)
	}

	return tokens
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
	asciiFolding  bool
}

func (t *whitespaceTokenizer) Tokenize(text string) []string {
	if text == "" {
		return nil
	}

	if t.asciiFolding {
		text = foldASCII(text)
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
	asciiFolding  bool
}

func (t *characterTokenizer) Tokenize(text string) []string {
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
	asciiFolding  bool
}

func (t *ngramTokenizer) Tokenize(text string) []string {
	if text == "" {
		return nil
	}

	if t.asciiFolding {
		text = foldASCII(text)
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

func stopwordSet(words ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(words))
	for _, word := range words {
		set[word] = struct{}{}
	}
	return set
}

// English stopwords (common words that often don't add meaning to searches)
var englishStopwords = stopwordSet(
	"a", "an", "and", "are", "as",
	"at", "be", "by", "for", "from",
	"has", "he", "in", "is", "it",
	"its", "of", "on", "that", "the",
	"to", "was", "were", "will", "with",
	"i", "me", "my", "we", "our",
	"you", "your", "they", "their", "this",
	"but", "or", "not", "no", "so",
	"if", "do", "does", "did", "have",
	"had", "been", "being", "which", "who",
	"what", "when", "where", "how", "why",
	"all", "each", "some", "any", "most",
	"other", "such", "only", "own", "same",
	"than", "too", "very", "can", "just",
	"should", "now", "also", "more",
)

var spanishStopwords = stopwordSet(
	"el", "la", "los", "las", "un",
	"una", "unos", "unas", "y", "o",
	"de", "del", "al", "en", "por",
	"para", "con", "sin", "que", "es",
	"son", "como", "pero", "su", "sus",
)

var frenchStopwords = stopwordSet(
	"le", "la", "les", "un", "une",
	"des", "et", "ou", "de", "du",
	"au", "aux", "en", "pour", "par",
	"avec", "sans", "que", "est", "sont",
)

var germanStopwords = stopwordSet(
	"der", "die", "das", "ein", "eine",
	"und", "oder", "zu", "im", "in",
	"mit", "von", "ist", "sind",
	"am", "auf", "nicht", "als", "auch",
)

var italianStopwords = stopwordSet(
	"il", "lo", "la", "i", "gli",
	"le", "un", "una", "e", "o",
	"di", "del", "della", "in", "per",
	"con", "su", "che", "sono",
)

var portugueseStopwords = stopwordSet(
	"o", "a", "os", "as", "um",
	"uma", "uns", "umas", "e", "ou",
	"de", "do", "da", "em", "para",
	"por", "com", "sem", "que",
	"sao", "nao",
)

var dutchStopwords = stopwordSet(
	"de", "het", "een", "en", "of",
	"van", "voor", "met", "in", "op",
	"aan", "bij", "niet", "wel", "als",
)

var russianStopwords = stopwordSet(
	"i", "v", "vo", "ne", "na",
	"ya", "ty", "on", "ona", "ono",
	"my", "vy", "oni", "a", "no",
	"ili", "da", "s", "k", "po",
)

var swedishStopwords = stopwordSet(
	"och", "det", "att", "i", "en",
	"jag", "hon", "som", "han", "pa",
	"den", "med", "var", "sig", "for",
)

var norwegianStopwords = stopwordSet(
	"og", "det", "at", "i", "en",
	"jeg", "hun", "som", "han", "pa",
	"den", "med", "var", "seg", "for",
)

var danishStopwords = stopwordSet(
	"og", "det", "at", "i", "en",
	"jeg", "hun", "som", "han", "pa",
	"den", "med", "var", "sig", "for",
)

var finnishStopwords = stopwordSet(
	"ja", "on", "se", "ett", "ei",
	"oli", "olen", "sin", "me", "te",
	"he", "mina", "tassa", "tuo",
)

var arabicStopwords = stopwordSet(
	"wa", "fi", "min", "ila", "an",
	"ma", "lan", "al", "bi", "li",
)

var chineseStopwords = stopwordSet(
	"de", "le", "shi", "zai", "wo",
	"ni", "ta", "men", "ye", "bu",
)

var japaneseStopwords = stopwordSet(
	"no", "ni", "wa", "ga", "wo",
	"de", "to", "mo", "ka", "shi",
)

var koreanStopwords = stopwordSet(
	"geu", "i", "ga", "eul", "reul",
	"wa", "gwa", "ui", "eseo", "ha",
)

var stopwordsByLanguage = map[string]map[string]struct{}{
	"english":    englishStopwords,
	"spanish":    spanishStopwords,
	"french":     frenchStopwords,
	"german":     germanStopwords,
	"italian":    italianStopwords,
	"portuguese": portugueseStopwords,
	"dutch":      dutchStopwords,
	"russian":    russianStopwords,
	"swedish":    swedishStopwords,
	"norwegian":  norwegianStopwords,
	"danish":     danishStopwords,
	"finnish":    finnishStopwords,
	"arabic":     arabicStopwords,
	"chinese":    chineseStopwords,
	"japanese":   japaneseStopwords,
	"korean":     koreanStopwords,
}

var stemLanguages = map[string]bool{
	"english":   true,
	"spanish":   true,
	"french":    true,
	"russian":   true,
	"swedish":   true,
	"norwegian": true,
}

func stopwordsForLanguage(language string) map[string]struct{} {
	if language == "" {
		return nil
	}
	return stopwordsByLanguage[strings.ToLower(language)]
}

func stemLanguage(language string) string {
	if stemLanguages[strings.ToLower(language)] {
		return strings.ToLower(language)
	}
	return ""
}

// IsStopword returns true if the token is a common stopword for the language.
func IsStopword(token string, stopwords map[string]struct{}) bool {
	if len(stopwords) == 0 {
		return false
	}
	_, ok := stopwords[strings.ToLower(token)]
	return ok
}

// RemoveStopwords filters stopwords from a token list.
func RemoveStopwords(tokens []string, stopwords map[string]struct{}) []string {
	if len(stopwords) == 0 {
		return tokens
	}
	result := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if !IsStopword(t, stopwords) {
			result = append(result, t)
		}
	}
	return result
}

// StemTokens applies stemming to tokens for a supported language.
func StemTokens(tokens []string, language string) []string {
	if language == "" {
		return tokens
	}
	result := make([]string, 0, len(tokens))
	for _, t := range tokens {
		stemmed, err := snowball.Stem(t, language, true)
		if err != nil || stemmed == "" {
			result = append(result, t)
			continue
		}
		result = append(result, stemmed)
	}
	return result
}
