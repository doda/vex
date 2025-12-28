package filter

import (
	"testing"
)

func TestGlobMatcher_BasicPatterns(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		input    string
		expected bool
	}{
		// Exact matches
		{"exact match", "hello", "hello", true},
		{"exact mismatch", "hello", "world", false},
		{"empty pattern empty string", "", "", true},
		{"empty pattern non-empty string", "", "hello", false},

		// Star (*) patterns
		{"star matches all", "*", "anything", true},
		{"star matches empty", "*", "", true},
		{"prefix star", "hello*", "hello", true},
		{"prefix star with suffix", "hello*", "hello world", true},
		{"prefix star no match", "hello*", "hi", false},
		{"suffix star", "*world", "hello world", true},
		{"suffix star no match", "*world", "hello", false},
		{"middle star", "hello*world", "hello world", true},
		{"middle star minimal", "hello*world", "helloworld", true},
		{"middle star long", "hello*world", "hello there beautiful world", true},
		{"middle star no match", "hello*world", "hello there", false},
		{"multiple stars", "*hello*world*", "say hello to the world today", true},
		{"multiple stars no match", "*hello*world*", "say hi there", false},
		{"consecutive stars", "**hello**", "hello", true},
		{"star only", "*", "", true},
		{"double star", "**", "anything", true},

		// Question mark (?) patterns
		{"single question mark", "?", "a", true},
		{"single question mark empty", "?", "", false},
		{"single question mark multi", "?", "ab", false},
		{"question mark prefix", "?ello", "hello", true},
		{"question mark prefix no match", "?ello", "hi", false},
		{"question mark suffix", "hell?", "hello", true},
		{"question mark suffix no match", "hell?", "hell", false},
		{"question mark middle", "he??o", "hello", true},
		{"question mark middle no match", "he??o", "heo", false},
		{"multiple question marks", "???", "abc", true},
		{"multiple question marks no match", "???", "ab", false},

		// Mixed star and question mark
		{"star and question", "h*?", "hello", true},
		{"star and question minimal", "h*?", "hx", true},
		{"star and question no match", "h*?", "h", false},
		{"question and star", "?*", "a", true},
		{"question and star multi", "?*", "abc", true},
		{"question and star empty", "?*", "", false},
		{"complex mixed", "a?c*e?g", "abcdefg", true},
		{"complex mixed 2", "a?c*e?g", "abcefg", true}, // a + b + c + "" + e + f + g

		// Character classes
		{"char class simple", "[abc]", "a", true},
		{"char class simple b", "[abc]", "b", true},
		{"char class simple c", "[abc]", "c", true},
		{"char class no match", "[abc]", "d", false},
		{"char class empty string", "[abc]", "", false},
		{"char class range", "[a-z]", "m", true},
		{"char class range start", "[a-z]", "a", true},
		{"char class range end", "[a-z]", "z", true},
		{"char class range no match", "[a-z]", "A", false},
		{"char class range no match 2", "[a-z]", "1", false},
		{"char class negated", "[!abc]", "d", true},
		{"char class negated no match", "[!abc]", "a", false},
		{"char class caret negated", "[^abc]", "d", true},
		{"char class caret negated no match", "[^abc]", "b", false},
		{"char class multiple ranges", "[a-zA-Z]", "M", true},
		{"char class multiple ranges lower", "[a-zA-Z]", "m", true},
		{"char class digit range", "[0-9]", "5", true},
		{"char class digit range no match", "[0-9]", "a", false},
		{"char class with literals", "[aeiou]", "e", true},
		{"char class with literals no match", "[aeiou]", "b", false},

		// Character class in patterns
		{"class with prefix", "hello[123]", "hello1", true},
		{"class with prefix 2", "hello[123]", "hello2", true},
		{"class with prefix no match", "hello[123]", "hello4", false},
		{"class with suffix", "[abc]world", "aworld", true},
		{"class with suffix no match", "[abc]world", "dworld", false},
		{"class with star", "[abc]*", "atest", true},
		{"class with star no match", "[abc]*", "dtest", false},

		// Edge cases
		{"literal bracket", "test[", "test[", true},
		{"unclosed bracket", "test[abc", "test[abc", true},
		{"question in middle", "te?t", "test", true},
		{"star at boundaries", "*a*b*c*", "abc", true},
		{"star at boundaries with chars", "*a*b*c*", "xaybzcw", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matcher := NewGlobMatcher(tc.pattern, false)
			got := matcher.Match(tc.input)
			if got != tc.expected {
				t.Errorf("Match(%q, %q) = %v, want %v", tc.pattern, tc.input, got, tc.expected)
			}
		})
	}
}

func TestGlobMatcher_CaseInsensitive(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		input    string
		expected bool
	}{
		{"exact match same case", "hello", "hello", true},
		{"exact match upper input", "hello", "HELLO", true},
		{"exact match mixed input", "hello", "HeLLo", true},
		{"exact match upper pattern", "HELLO", "hello", true},
		{"prefix star case insensitive", "HELLO*", "hello world", true},
		{"suffix star case insensitive", "*WORLD", "hello world", true},
		{"question mark case insensitive", "?ELLO", "hello", true},
		{"char class case insensitive", "[ABC]*", "atest", true},
		{"mixed pattern case", "HeLLo*", "hello world", true},
		{"mixed input case", "hello*", "HELLO WORLD", true},
		{"no match even case insensitive", "hello", "world", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matcher := NewGlobMatcher(tc.pattern, true)
			got := matcher.Match(tc.input)
			if got != tc.expected {
				t.Errorf("Match(%q, %q, caseInsensitive=true) = %v, want %v", tc.pattern, tc.input, got, tc.expected)
			}
		})
	}
}

func TestGlobMatcher_PrefixOptimization(t *testing.T) {
	tests := []struct {
		name         string
		pattern      string
		isPrefixOnly bool
		prefix       string
	}{
		{"simple prefix glob", "hello*", true, "hello"},
		{"just star", "*", true, ""},
		{"exact match", "hello", false, "hello"},
		{"question mark pattern", "hello?", false, "hello"},
		{"middle star", "hello*world", false, "hello"},
		{"char class", "hello[abc]*", false, "hello"},
		{"question then star", "?ello*", false, ""},
		{"long prefix", "verylongprefix*", true, "verylongprefix"},
		{"empty prefix star", "*suffix", false, ""},
		{"multiple stars", "a*b*c", false, "a"},
		{"star at start", "*hello", false, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matcher := NewGlobMatcher(tc.pattern, false)
			if got := matcher.IsPrefixGlob(); got != tc.isPrefixOnly {
				t.Errorf("IsPrefixGlob() = %v, want %v", got, tc.isPrefixOnly)
			}
			if got := matcher.Prefix(); got != tc.prefix {
				t.Errorf("Prefix() = %q, want %q", got, tc.prefix)
			}
		})
	}
}

func TestGlobMatcher_PrefixRange(t *testing.T) {
	tests := []struct {
		name        string
		pattern     string
		wantLower   string
		wantUpper   string
	}{
		{"simple prefix", "hello*", "hello", "hellp"},
		{"single char prefix", "a*", "a", "b"},
		{"multi char prefix", "abc*", "abc", "abd"},
		{"trailing z", "abz*", "abz", "ab{"},
		{"empty prefix star", "*", "", ""},
		{"not prefix only", "hello*world", "", ""},
		{"exact match", "hello", "", ""},
		{"all 0xFF would need special handling", "\xFF*", "\xFF", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matcher := NewGlobMatcher(tc.pattern, false)
			lower, upper := matcher.PrefixRange()
			if lower != tc.wantLower {
				t.Errorf("PrefixRange() lower = %q, want %q", lower, tc.wantLower)
			}
			if upper != tc.wantUpper {
				t.Errorf("PrefixRange() upper = %q, want %q", upper, tc.wantUpper)
			}
		})
	}
}

func TestGlobMatcher_Unicode(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		input    string
		expected bool
	}{
		{"unicode literal", "héllo", "héllo", true},
		{"unicode star", "hé*", "héllo world", true},
		{"unicode question mark", "h?llo", "héllo", true},
		{"emoji literal", "hello 🎉", "hello 🎉", true},
		{"emoji star", "hello *", "hello 🎉", true},
		{"emoji question", "hello ?", "hello 🎉", true},
		{"chinese characters", "你好*", "你好世界", true},
		{"mixed unicode", "h?llo 世界", "héllo 世界", true},
		{"japanese hiragana", "こんにちは*", "こんにちは世界", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matcher := NewGlobMatcher(tc.pattern, false)
			got := matcher.Match(tc.input)
			if got != tc.expected {
				t.Errorf("Match(%q, %q) = %v, want %v", tc.pattern, tc.input, got, tc.expected)
			}
		})
	}
}

// TestGlob_FilterIntegration tests the glob operators through the Filter interface.
func TestGlob_FilterIntegration(t *testing.T) {
	tests := []struct {
		name     string
		filter   any
		doc      Document
		expected bool
	}{
		// Glob operator
		{
			name:     "Glob matches prefix pattern",
			filter:   []any{"filename", "Glob", "*.txt"},
			doc:      Document{"filename": "document.txt"},
			expected: true,
		},
		{
			name:     "Glob matches suffix pattern",
			filter:   []any{"filename", "Glob", "report*"},
			doc:      Document{"filename": "report_2024.pdf"},
			expected: true,
		},
		{
			name:     "Glob matches middle pattern",
			filter:   []any{"path", "Glob", "/home/*/documents"},
			doc:      Document{"path": "/home/user/documents"},
			expected: true,
		},
		{
			name:     "Glob no match",
			filter:   []any{"filename", "Glob", "*.txt"},
			doc:      Document{"filename": "document.pdf"},
			expected: false,
		},
		{
			name:     "Glob missing attribute",
			filter:   []any{"filename", "Glob", "*.txt"},
			doc:      Document{"other": "document.txt"},
			expected: false,
		},
		{
			name:     "Glob nil value",
			filter:   []any{"filename", "Glob", "*.txt"},
			doc:      Document{"filename": nil},
			expected: false,
		},
		{
			name:     "Glob non-string attribute",
			filter:   []any{"count", "Glob", "1*"},
			doc:      Document{"count": float64(123)},
			expected: false,
		},
		{
			name:     "Glob question mark",
			filter:   []any{"code", "Glob", "A?C"},
			doc:      Document{"code": "ABC"},
			expected: true,
		},
		{
			name:     "Glob character class",
			filter:   []any{"grade", "Glob", "[A-C]"},
			doc:      Document{"grade": "B"},
			expected: true,
		},

		// NotGlob operator
		{
			name:     "NotGlob excludes matching pattern",
			filter:   []any{"filename", "NotGlob", "*.tmp"},
			doc:      Document{"filename": "document.txt"},
			expected: true,
		},
		{
			name:     "NotGlob includes non-matching",
			filter:   []any{"filename", "NotGlob", "*.tmp"},
			doc:      Document{"filename": "temp.tmp"},
			expected: false,
		},
		{
			name:     "NotGlob missing attribute",
			filter:   []any{"filename", "NotGlob", "*.tmp"},
			doc:      Document{"other": "temp.tmp"},
			expected: true,
		},

		// IGlob operator (case-insensitive)
		{
			name:     "IGlob matches case insensitive",
			filter:   []any{"filename", "IGlob", "*.TXT"},
			doc:      Document{"filename": "document.txt"},
			expected: true,
		},
		{
			name:     "IGlob matches upper case input",
			filter:   []any{"filename", "IGlob", "*.txt"},
			doc:      Document{"filename": "DOCUMENT.TXT"},
			expected: true,
		},
		{
			name:     "IGlob no match",
			filter:   []any{"filename", "IGlob", "*.txt"},
			doc:      Document{"filename": "document.pdf"},
			expected: false,
		},
		{
			name:     "IGlob prefix pattern",
			filter:   []any{"name", "IGlob", "JOHN*"},
			doc:      Document{"name": "john doe"},
			expected: true,
		},

		// NotIGlob operator (case-insensitive negation)
		{
			name:     "NotIGlob excludes case insensitive",
			filter:   []any{"filename", "NotIGlob", "*.TMP"},
			doc:      Document{"filename": "temp.TMP"},
			expected: false,
		},
		{
			name:     "NotIGlob includes non-matching",
			filter:   []any{"filename", "NotIGlob", "*.TMP"},
			doc:      Document{"filename": "document.txt"},
			expected: true,
		},
		{
			name:     "NotIGlob excludes lower case match",
			filter:   []any{"filename", "NotIGlob", "*.tmp"},
			doc:      Document{"filename": "TEMP.TMP"},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, err := Parse(tc.filter)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			got := f.Eval(tc.doc)
			if got != tc.expected {
				t.Errorf("Eval() = %v, want %v", got, tc.expected)
			}
		})
	}
}

// TestGlob_TaskVerification tests exactly the verification steps for the filter-glob task.
func TestGlob_TaskVerification(t *testing.T) {
	t.Run("Glob_with_Unix_style_patterns", func(t *testing.T) {
		// Test Glob with Unix-style glob patterns

		// Test * wildcard (matches any characters)
		f1, err := Parse([]any{"path", "Glob", "/home/*/documents"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f1.Eval(Document{"path": "/home/user/documents"}) {
			t.Error("Glob should match * pattern in middle")
		}
		if !f1.Eval(Document{"path": "/home/admin/documents"}) {
			t.Error("Glob should match * pattern with different content")
		}
		if f1.Eval(Document{"path": "/home/documents"}) {
			t.Error("Glob * should require at least one character")
		}

		// Test ? wildcard (matches single character)
		f2, err := Parse([]any{"code", "Glob", "item_?"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f2.Eval(Document{"code": "item_1"}) {
			t.Error("Glob should match ? for single character")
		}
		if !f2.Eval(Document{"code": "item_a"}) {
			t.Error("Glob should match ? for any single character")
		}
		if f2.Eval(Document{"code": "item_10"}) {
			t.Error("Glob ? should match only one character")
		}
		if f2.Eval(Document{"code": "item_"}) {
			t.Error("Glob ? should require exactly one character")
		}

		// Test character class [...]
		f3, err := Parse([]any{"grade", "Glob", "Grade [A-C]"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f3.Eval(Document{"grade": "Grade A"}) {
			t.Error("Glob should match character class range [A-C] for A")
		}
		if !f3.Eval(Document{"grade": "Grade B"}) {
			t.Error("Glob should match character class range [A-C] for B")
		}
		if !f3.Eval(Document{"grade": "Grade C"}) {
			t.Error("Glob should match character class range [A-C] for C")
		}
		if f3.Eval(Document{"grade": "Grade D"}) {
			t.Error("Glob should not match character outside range [A-C]")
		}

		// Test negated character class [!...]
		f4, err := Parse([]any{"status", "Glob", "status_[!0-9]"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f4.Eval(Document{"status": "status_a"}) {
			t.Error("Glob should match negated class [!0-9] for letter")
		}
		if f4.Eval(Document{"status": "status_5"}) {
			t.Error("Glob should not match negated class [!0-9] for digit")
		}

		// Test prefix pattern (common use case)
		f5, err := Parse([]any{"filename", "Glob", "report_2024*"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f5.Eval(Document{"filename": "report_2024"}) {
			t.Error("Glob prefix pattern should match exact prefix")
		}
		if !f5.Eval(Document{"filename": "report_2024_01"}) {
			t.Error("Glob prefix pattern should match with suffix")
		}
		if !f5.Eval(Document{"filename": "report_2024_final.pdf"}) {
			t.Error("Glob prefix pattern should match with longer suffix")
		}
		if f5.Eval(Document{"filename": "report_2023_final.pdf"}) {
			t.Error("Glob prefix pattern should not match different prefix")
		}

		// Test suffix pattern (common use case)
		f6, err := Parse([]any{"filename", "Glob", "*.pdf"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f6.Eval(Document{"filename": "document.pdf"}) {
			t.Error("Glob suffix pattern *.pdf should match")
		}
		if !f6.Eval(Document{"filename": ".pdf"}) {
			t.Error("Glob suffix pattern *.pdf should match just extension")
		}
		if f6.Eval(Document{"filename": "document.txt"}) {
			t.Error("Glob suffix pattern *.pdf should not match different extension")
		}
	})

	t.Run("NotGlob_excludes_matching_patterns", func(t *testing.T) {
		// Test NotGlob excludes matching patterns

		f, err := Parse([]any{"filename", "NotGlob", "*.tmp"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when pattern does NOT match
		if !f.Eval(Document{"filename": "document.txt"}) {
			t.Error("NotGlob should match when glob pattern does not match")
		}
		if !f.Eval(Document{"filename": "report.pdf"}) {
			t.Error("NotGlob should match different extension")
		}

		// Should NOT match when pattern matches
		if f.Eval(Document{"filename": "cache.tmp"}) {
			t.Error("NotGlob should not match when glob pattern matches")
		}
		if f.Eval(Document{"filename": ".tmp"}) {
			t.Error("NotGlob should not match when glob pattern matches (edge case)")
		}

		// Test NotGlob with more complex patterns
		f2, err := Parse([]any{"path", "NotGlob", "/var/log/*"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f2.Eval(Document{"path": "/home/user/file.txt"}) {
			t.Error("NotGlob should match paths outside /var/log/")
		}
		if f2.Eval(Document{"path": "/var/log/syslog"}) {
			t.Error("NotGlob should not match paths in /var/log/")
		}
	})

	t.Run("IGlob_case_insensitive_matching", func(t *testing.T) {
		// Test IGlob for case-insensitive matching

		f, err := Parse([]any{"filename", "IGlob", "*.PDF"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match regardless of case
		if !f.Eval(Document{"filename": "document.pdf"}) {
			t.Error("IGlob should match lowercase extension with uppercase pattern")
		}
		if !f.Eval(Document{"filename": "document.PDF"}) {
			t.Error("IGlob should match uppercase extension with uppercase pattern")
		}
		if !f.Eval(Document{"filename": "DOCUMENT.pdf"}) {
			t.Error("IGlob should match mixed case filename")
		}
		if !f.Eval(Document{"filename": "Document.Pdf"}) {
			t.Error("IGlob should match any case combination")
		}

		// Should not match different extension
		if f.Eval(Document{"filename": "document.txt"}) {
			t.Error("IGlob should not match different extension")
		}

		// Test with prefix pattern
		f2, err := Parse([]any{"name", "IGlob", "JOHN*"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f2.Eval(Document{"name": "john doe"}) {
			t.Error("IGlob should match lowercase 'john' with uppercase 'JOHN' pattern")
		}
		if !f2.Eval(Document{"name": "John Smith"}) {
			t.Error("IGlob should match title case 'John'")
		}
		if !f2.Eval(Document{"name": "JOHN SMITH"}) {
			t.Error("IGlob should match uppercase 'JOHN'")
		}
		if f2.Eval(Document{"name": "jane doe"}) {
			t.Error("IGlob should not match 'jane'")
		}

		// Test with character class (should be case-insensitive)
		f3, err := Parse([]any{"code", "IGlob", "[A-Z]123"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f3.Eval(Document{"code": "a123"}) {
			t.Error("IGlob should match lowercase 'a' with [A-Z] pattern")
		}
		if !f3.Eval(Document{"code": "Z123"}) {
			t.Error("IGlob should match uppercase 'Z' with [A-Z] pattern")
		}
	})

	t.Run("NotIGlob_case_insensitive_exclusion", func(t *testing.T) {
		// Test NotIGlob for case-insensitive exclusion

		f, err := Parse([]any{"filename", "NotIGlob", "*.TMP"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when pattern does NOT match (any case)
		if !f.Eval(Document{"filename": "document.txt"}) {
			t.Error("NotIGlob should match when pattern does not match")
		}
		if !f.Eval(Document{"filename": "DOCUMENT.PDF"}) {
			t.Error("NotIGlob should match different extension in any case")
		}

		// Should NOT match when pattern matches (any case)
		if f.Eval(Document{"filename": "cache.tmp"}) {
			t.Error("NotIGlob should not match lowercase .tmp")
		}
		if f.Eval(Document{"filename": "cache.TMP"}) {
			t.Error("NotIGlob should not match uppercase .TMP")
		}
		if f.Eval(Document{"filename": "CACHE.Tmp"}) {
			t.Error("NotIGlob should not match mixed case .Tmp")
		}

		// Test with prefix pattern
		f2, err := Parse([]any{"name", "NotIGlob", "test_*"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f2.Eval(Document{"name": "production_file"}) {
			t.Error("NotIGlob should match non-test files")
		}
		if f2.Eval(Document{"name": "TEST_data"}) {
			t.Error("NotIGlob should not match uppercase TEST_ with lowercase test_*")
		}
		if f2.Eval(Document{"name": "Test_File"}) {
			t.Error("NotIGlob should not match mixed case Test_")
		}
	})

	t.Run("prefix_globs_compile_to_range_queries", func(t *testing.T) {
		// Verify prefix globs compile to range queries

		// Test simple prefix glob detection
		m1 := NewGlobMatcher("hello*", false)
		if !m1.IsPrefixGlob() {
			t.Error("'hello*' should be detected as a prefix glob")
		}
		if m1.Prefix() != "hello" {
			t.Errorf("Prefix() = %q, want %q", m1.Prefix(), "hello")
		}
		lower, upper := m1.PrefixRange()
		if lower != "hello" {
			t.Errorf("PrefixRange() lower = %q, want %q", lower, "hello")
		}
		if upper != "hellp" {
			t.Errorf("PrefixRange() upper = %q, want %q", upper, "hellp")
		}

		// Test that complex patterns are NOT prefix globs
		m2 := NewGlobMatcher("hello*world", false)
		if m2.IsPrefixGlob() {
			t.Error("'hello*world' should NOT be a prefix glob")
		}

		m3 := NewGlobMatcher("*hello", false)
		if m3.IsPrefixGlob() {
			t.Error("'*hello' should NOT be a prefix glob")
		}

		m4 := NewGlobMatcher("hello?*", false)
		if m4.IsPrefixGlob() {
			t.Error("'hello?*' should NOT be a prefix glob (has ? before *)")
		}

		m5 := NewGlobMatcher("[abc]*", false)
		if m5.IsPrefixGlob() {
			t.Error("'[abc]*' should NOT be a prefix glob (starts with class)")
		}

		// Test range calculation for various prefixes
		testCases := []struct {
			pattern     string
			wantLower   string
			wantUpper   string
		}{
			{"a*", "a", "b"},
			{"ab*", "ab", "ac"},
			{"abc*", "abc", "abd"},
			{"abcd*", "abcd", "abce"},
			{"user_*", "user_", "user`"}, // underscore + 1 = backtick
			{"test123*", "test123", "test124"},
		}

		for _, tc := range testCases {
			m := NewGlobMatcher(tc.pattern, false)
			if !m.IsPrefixGlob() {
				t.Errorf("'%s' should be a prefix glob", tc.pattern)
				continue
			}
			lower, upper := m.PrefixRange()
			if lower != tc.wantLower {
				t.Errorf("PrefixRange(%q) lower = %q, want %q", tc.pattern, lower, tc.wantLower)
			}
			if upper != tc.wantUpper {
				t.Errorf("PrefixRange(%q) upper = %q, want %q", tc.pattern, upper, tc.wantUpper)
			}
		}

		// Verify the range logic: prefix matches if lower <= s < upper
		m := NewGlobMatcher("abc*", false)
		lower, upper = m.PrefixRange()

		// These should match the range
		rangeMatches := []string{"abc", "abcd", "abcdef", "abczzz"}
		for _, s := range rangeMatches {
			if !(s >= lower && s < upper) {
				t.Errorf("Range check failed for %q: should be in [%q, %q)", s, lower, upper)
			}
			if !m.Match(s) {
				t.Errorf("Match(%q) should be true for pattern 'abc*'", s)
			}
		}

		// These should NOT match the range
		rangeNoMatches := []string{"ab", "abd", "abb", "xyz"}
		for _, s := range rangeNoMatches {
			if s >= lower && s < upper {
				t.Errorf("Range check should fail for %q: should NOT be in [%q, %q)", s, lower, upper)
			}
			if m.Match(s) {
				t.Errorf("Match(%q) should be false for pattern 'abc*'", s)
			}
		}
	})
}

// TestGlob_EdgeCases tests edge cases and error handling.
func TestGlob_EdgeCases(t *testing.T) {
	t.Run("special_characters", func(t *testing.T) {
		// Test patterns with special characters

		// Literal period
		f, err := Parse([]any{"filename", "Glob", "file.txt"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f.Eval(Document{"filename": "file.txt"}) {
			t.Error("Glob should match literal period")
		}
		if f.Eval(Document{"filename": "fileXtxt"}) {
			t.Error("Glob should not match X in place of period")
		}

		// Test star after period
		f2, err := Parse([]any{"ext", "Glob", ".*"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f2.Eval(Document{"ext": ".txt"}) {
			t.Error("Glob .* should match .txt")
		}
		if !f2.Eval(Document{"ext": ".gitignore"}) {
			t.Error("Glob .* should match .gitignore")
		}
	})

	t.Run("empty_values", func(t *testing.T) {
		// Empty pattern matches empty string
		f, err := Parse([]any{"value", "Glob", ""})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f.Eval(Document{"value": ""}) {
			t.Error("Empty Glob pattern should match empty string")
		}
		if f.Eval(Document{"value": "something"}) {
			t.Error("Empty Glob pattern should not match non-empty string")
		}

		// Star pattern matches empty string
		f2, err := Parse([]any{"value", "Glob", "*"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f2.Eval(Document{"value": ""}) {
			t.Error("Star Glob pattern should match empty string")
		}
	})

	t.Run("non_string_pattern", func(t *testing.T) {
		// Non-string pattern should not match
		f, err := Parse([]any{"value", "Glob", float64(123)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if f.Eval(Document{"value": "123"}) {
			t.Error("Glob with numeric pattern should not match")
		}
	})

	t.Run("boolean_operators_with_glob", func(t *testing.T) {
		// And with Glob
		f, err := Parse([]any{"And", []any{
			[]any{"filename", "Glob", "*.txt"},
			[]any{"size", "Gt", float64(0)},
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f.Eval(Document{"filename": "doc.txt", "size": float64(100)}) {
			t.Error("And with Glob should match both conditions")
		}
		if f.Eval(Document{"filename": "doc.pdf", "size": float64(100)}) {
			t.Error("And with Glob should not match when Glob fails")
		}

		// Or with Glob
		f2, err := Parse([]any{"Or", []any{
			[]any{"filename", "Glob", "*.txt"},
			[]any{"filename", "Glob", "*.pdf"},
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f2.Eval(Document{"filename": "doc.txt"}) {
			t.Error("Or with Glob should match first pattern")
		}
		if !f2.Eval(Document{"filename": "doc.pdf"}) {
			t.Error("Or with Glob should match second pattern")
		}
		if f2.Eval(Document{"filename": "doc.doc"}) {
			t.Error("Or with Glob should not match neither pattern")
		}

		// Not with Glob
		f3, err := Parse([]any{"Not", []any{"filename", "Glob", "*.tmp"}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f3.Eval(Document{"filename": "doc.txt"}) {
			t.Error("Not Glob should match when pattern doesn't match")
		}
		if f3.Eval(Document{"filename": "cache.tmp"}) {
			t.Error("Not Glob should not match when pattern matches")
		}
	})
}
