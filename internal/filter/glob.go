package filter

import (
	"strings"
	"unicode/utf8"
)

// GlobMatcher represents a compiled glob pattern.
type GlobMatcher struct {
	pattern       string
	prefix        string  // For prefix optimization
	isPrefixOnly  bool    // True if pattern is "prefix*" with no other wildcards
	caseInsensitive bool
}

// NewGlobMatcher creates a new glob matcher from a pattern.
// If caseInsensitive is true, matching will be case-insensitive.
func NewGlobMatcher(pattern string, caseInsensitive bool) *GlobMatcher {
	m := &GlobMatcher{
		pattern:         pattern,
		caseInsensitive: caseInsensitive,
	}

	if caseInsensitive {
		m.pattern = strings.ToLower(pattern)
	}

	// Check if this is a prefix-only pattern (for range query optimization)
	m.analyzePrefix()

	return m
}

// analyzePrefix checks if the pattern is a simple prefix glob (e.g., "hello*").
// If so, it extracts the prefix for potential range query optimization.
func (m *GlobMatcher) analyzePrefix() {
	pattern := m.pattern

	// Find first wildcard
	wildcardIdx := -1
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		if c == '*' || c == '?' || c == '[' {
			wildcardIdx = i
			break
		}
	}

	// No wildcards - exact match (also a prefix case with isPrefixOnly=false)
	if wildcardIdx == -1 {
		m.prefix = pattern
		m.isPrefixOnly = false
		return
	}

	// Extract prefix (literal part before first wildcard)
	m.prefix = pattern[:wildcardIdx]

	// Check if it's a simple "prefix*" pattern:
	// - First wildcard is at the end and is a single '*'
	// - No other wildcards after
	if wildcardIdx == len(pattern)-1 && pattern[wildcardIdx] == '*' {
		m.isPrefixOnly = true
	}
}

// IsPrefixGlob returns true if the pattern is a prefix-only glob (e.g., "hello*").
// These patterns can be optimized to range queries on sorted data.
func (m *GlobMatcher) IsPrefixGlob() bool {
	return m.isPrefixOnly
}

// Prefix returns the literal prefix of the glob pattern.
// For "hello*", this returns "hello".
// For "he?lo*", this returns "he".
func (m *GlobMatcher) Prefix() string {
	return m.prefix
}

// PrefixRange returns the range bounds for a prefix glob.
// For "hello*", this returns ("hello", "hellp") where "hellp" is the exclusive upper bound.
// Returns empty strings if this is not a prefix-only glob.
func (m *GlobMatcher) PrefixRange() (lower, upper string) {
	if !m.isPrefixOnly || m.prefix == "" {
		return "", ""
	}

	lower = m.prefix

	// Calculate upper bound by incrementing the last character
	// Handle edge case where last byte is 0xFF
	upperBytes := []byte(lower)
	for i := len(upperBytes) - 1; i >= 0; i-- {
		if upperBytes[i] < 0xFF {
			upperBytes[i]++
			upper = string(upperBytes[:i+1])
			return lower, upper
		}
		// If 0xFF, continue to previous byte
	}

	// All 0xFF - no upper bound
	return lower, ""
}

// Match returns true if the string matches the glob pattern.
// Supports:
//   - * matches zero or more characters
//   - ? matches exactly one character
//   - [abc] matches one character from the set
//   - [a-z] matches one character in the range
//   - [!abc] or [^abc] matches one character not in the set
func (m *GlobMatcher) Match(s string) bool {
	if m.caseInsensitive {
		s = strings.ToLower(s)
	}
	return globMatch(m.pattern, s)
}

// globMatch performs glob matching using a recursive algorithm with backtracking.
// This handles *, ?, and character class patterns.
func globMatch(pattern, s string) bool {
	// Quick paths
	if pattern == "" {
		return s == ""
	}
	if pattern == "*" {
		return true
	}

	// Use iterative approach with backtracking for * handling
	px, sx := 0, 0 // pattern and string indices
	starPx, starSx := -1, -1 // position of last star and string position to backtrack to

	for sx < len(s) {
		if px < len(pattern) {
			c := pattern[px]
			switch c {
			case '*':
				// Record star position and current string position
				starPx = px
				starSx = sx
				px++
				continue

			case '?':
				// Match any single character
				_, size := utf8.DecodeRuneInString(s[sx:])
				sx += size
				px++
				continue

			case '[':
				// Character class
				matched, newPx := matchCharClass(pattern[px:], s[sx:])
				if newPx > 0 {
					// Valid character class
					if matched {
						_, size := utf8.DecodeRuneInString(s[sx:])
						sx += size
						px = px + newPx
						continue
					}
					// Valid class but no match - fall through to backtrack
				} else {
					// Malformed class - treat '[' as literal
					if s[sx] == '[' {
						sx++
						px++
						continue
					}
					// No match - fall through to backtrack
				}

			default:
				// Match literal character
				if s[sx] == c {
					sx++
					px++
					continue
				}
				// Fall through to backtrack
			}
		}

		// No match at current position - try to backtrack to last star
		if starPx >= 0 {
			px = starPx + 1
			starSx++
			// Consume one character of input at the star position
			_, size := utf8.DecodeRuneInString(s[starSx-1:])
			_ = size // we already advanced starSx
			sx = starSx
			continue
		}

		return false
	}

	// Remaining pattern must be all stars
	for px < len(pattern) {
		if pattern[px] != '*' {
			return false
		}
		px++
	}

	return true
}

// matchCharClass matches a character class like [abc] or [a-z] or [!abc].
// Returns (matched, bytesConsumed) where bytesConsumed is the length of the class pattern.
// If the character class is malformed (unclosed), returns (false, 0) to indicate it should
// be treated as a literal '[' character.
func matchCharClass(pattern, s string) (bool, int) {
	if len(s) == 0 {
		return false, 0
	}

	c, _ := utf8.DecodeRuneInString(s)

	// Skip opening bracket
	if len(pattern) < 2 || pattern[0] != '[' {
		return false, 0
	}

	// First, check if this is a valid character class (has closing bracket)
	hasClosingBracket := false
	for i := 1; i < len(pattern); i++ {
		if pattern[i] == ']' && i > 1 {
			hasClosingBracket = true
			break
		}
		// Handle ] as first character in class (it's a literal)
		if pattern[i] == ']' && i == 1 {
			continue
		}
	}
	if !hasClosingBracket {
		// Malformed class - treat '[' as literal
		return false, 0
	}

	i := 1
	negated := false

	// Check for negation
	if i < len(pattern) && (pattern[i] == '!' || pattern[i] == '^') {
		negated = true
		i++
	}

	matched := false
	first := true

	for i < len(pattern) {
		ch := pattern[i]

		// End of class
		if ch == ']' && !first {
			i++ // consume ]
			if negated {
				return !matched, i
			}
			return matched, i
		}
		first = false

		// Check for range (a-z)
		if i+2 < len(pattern) && pattern[i+1] == '-' && pattern[i+2] != ']' {
			low := rune(ch)
			high := rune(pattern[i+2])
			if low <= c && c <= high {
				matched = true
			}
			i += 3
			continue
		}

		// Literal character
		if c == rune(ch) {
			matched = true
		}
		i++
	}

	// Unterminated class - treat as literal characters, no match
	return false, 0
}
