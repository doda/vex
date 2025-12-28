package filter

import (
	"errors"
	"testing"
)

// mockSchema is a simple SchemaChecker implementation for testing.
type mockSchema struct {
	regexAttrs map[string]bool
}

func newMockSchema(regexAttrs ...string) *mockSchema {
	m := &mockSchema{regexAttrs: make(map[string]bool)}
	for _, attr := range regexAttrs {
		m.regexAttrs[attr] = true
	}
	return m
}

func (m *mockSchema) HasRegex(attrName string) bool {
	return m.regexAttrs[attrName]
}

func TestParseAndEval_Regex(t *testing.T) {
	tests := []struct {
		name     string
		filter   any
		doc      Document
		expected bool
	}{
		{
			name:     "Regex matches simple pattern",
			filter:   []any{"email", "Regex", ".*@example\\.com"},
			doc:      Document{"email": "user@example.com"},
			expected: true,
		},
		{
			name:     "Regex doesn't match",
			filter:   []any{"email", "Regex", ".*@example\\.com"},
			doc:      Document{"email": "user@other.com"},
			expected: false,
		},
		{
			name:     "Regex with word boundary",
			filter:   []any{"text", "Regex", "\\bfoo\\b"},
			doc:      Document{"text": "the foo bar"},
			expected: true,
		},
		{
			name:     "Regex word boundary doesn't match partial",
			filter:   []any{"text", "Regex", "\\bfoo\\b"},
			doc:      Document{"text": "the foobar baz"},
			expected: false,
		},
		{
			name:     "Regex matches beginning of string",
			filter:   []any{"name", "Regex", "^John"},
			doc:      Document{"name": "John Smith"},
			expected: true,
		},
		{
			name:     "Regex matches end of string",
			filter:   []any{"name", "Regex", "Smith$"},
			doc:      Document{"name": "John Smith"},
			expected: true,
		},
		{
			name:     "Regex with alternation",
			filter:   []any{"status", "Regex", "^(active|pending)$"},
			doc:      Document{"status": "active"},
			expected: true,
		},
		{
			name:     "Regex alternation doesn't match",
			filter:   []any{"status", "Regex", "^(active|pending)$"},
			doc:      Document{"status": "deleted"},
			expected: false,
		},
		{
			name:     "Regex case sensitive by default",
			filter:   []any{"name", "Regex", "^john"},
			doc:      Document{"name": "John"},
			expected: false,
		},
		{
			name:     "Regex with case insensitive flag",
			filter:   []any{"name", "Regex", "(?i)^john"},
			doc:      Document{"name": "John"},
			expected: true,
		},
		{
			name:     "Regex missing attribute returns false",
			filter:   []any{"email", "Regex", ".*"},
			doc:      Document{"other": "value"},
			expected: false,
		},
		{
			name:     "Regex nil attribute returns false",
			filter:   []any{"email", "Regex", ".*"},
			doc:      Document{"email": nil},
			expected: false,
		},
		{
			name:     "Regex non-string attribute returns false",
			filter:   []any{"count", "Regex", "\\d+"},
			doc:      Document{"count": float64(42)},
			expected: false,
		},
		{
			name:     "Regex with digit pattern",
			filter:   []any{"phone", "Regex", "^\\d{3}-\\d{4}$"},
			doc:      Document{"phone": "555-1234"},
			expected: true,
		},
		{
			name:     "Regex empty string matches empty pattern",
			filter:   []any{"text", "Regex", "^$"},
			doc:      Document{"text": ""},
			expected: true,
		},
		{
			name:     "Regex matches anywhere in string",
			filter:   []any{"text", "Regex", "needle"},
			doc:      Document{"text": "haystack needle haystack"},
			expected: true,
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

func TestParseAndEval_NotRegex(t *testing.T) {
	tests := []struct {
		name     string
		filter   any
		doc      Document
		expected bool
	}{
		{
			name:     "NotRegex doesn't match when pattern matches",
			filter:   []any{"email", "NotRegex", ".*@example\\.com"},
			doc:      Document{"email": "user@example.com"},
			expected: false,
		},
		{
			name:     "NotRegex matches when pattern doesn't match",
			filter:   []any{"email", "NotRegex", ".*@example\\.com"},
			doc:      Document{"email": "user@other.com"},
			expected: true,
		},
		{
			name:     "NotRegex matches for missing attribute",
			filter:   []any{"email", "NotRegex", ".*"},
			doc:      Document{"other": "value"},
			expected: true,
		},
		{
			name:     "NotRegex matches for nil attribute",
			filter:   []any{"email", "NotRegex", ".*"},
			doc:      Document{"email": nil},
			expected: true,
		},
		{
			name:     "NotRegex matches for non-string attribute",
			filter:   []any{"count", "NotRegex", "\\d+"},
			doc:      Document{"count": float64(42)},
			expected: true,
		},
		{
			name:     "NotRegex excludes pattern",
			filter:   []any{"status", "NotRegex", "^(deleted|spam)$"},
			doc:      Document{"status": "active"},
			expected: true,
		},
		{
			name:     "NotRegex doesn't exclude valid status",
			filter:   []any{"status", "NotRegex", "^(deleted|spam)$"},
			doc:      Document{"status": "deleted"},
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

func TestRegex_InvalidPattern(t *testing.T) {
	// Invalid regex pattern should return false (not panic)
	f, err := Parse([]any{"text", "Regex", "[invalid"})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	// Invalid regex returns false
	if f.Eval(Document{"text": "anything"}) {
		t.Error("Invalid regex pattern should return false")
	}
}

func TestRegex_ValidateWithSchema(t *testing.T) {
	t.Run("Regex with regex:true in schema passes", func(t *testing.T) {
		f, err := Parse([]any{"email", "Regex", ".*@example\\.com"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		schema := newMockSchema("email")
		if err := f.ValidateWithSchema(schema); err != nil {
			t.Errorf("ValidateWithSchema should pass for attribute with regex:true, got: %v", err)
		}
	})

	t.Run("NotRegex with regex:true in schema passes", func(t *testing.T) {
		f, err := Parse([]any{"email", "NotRegex", ".*@spam\\.com"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		schema := newMockSchema("email")
		if err := f.ValidateWithSchema(schema); err != nil {
			t.Errorf("ValidateWithSchema should pass for attribute with regex:true, got: %v", err)
		}
	})

	t.Run("Regex without regex:true returns error", func(t *testing.T) {
		f, err := Parse([]any{"email", "Regex", ".*@example\\.com"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		schema := newMockSchema() // empty - no regex attributes
		err = f.ValidateWithSchema(schema)
		if err == nil {
			t.Error("ValidateWithSchema should return error for attribute without regex:true")
		}
		if !errors.Is(err, ErrRegexNotEnabled) {
			t.Errorf("Expected ErrRegexNotEnabled, got: %v", err)
		}
	})

	t.Run("Regex with nil schema returns error", func(t *testing.T) {
		f, err := Parse([]any{"email", "Regex", ".*"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		err = f.ValidateWithSchema(nil)
		if err == nil {
			t.Error("ValidateWithSchema should return error when schema is nil")
		}
		if !errors.Is(err, ErrRegexNotEnabled) {
			t.Errorf("Expected ErrRegexNotEnabled, got: %v", err)
		}
	})

	t.Run("Regex on wrong attribute returns error", func(t *testing.T) {
		f, err := Parse([]any{"name", "Regex", ".*"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		schema := newMockSchema("email") // only email has regex:true
		err = f.ValidateWithSchema(schema)
		if err == nil {
			t.Error("ValidateWithSchema should return error for wrong attribute")
		}
		if !errors.Is(err, ErrRegexNotEnabled) {
			t.Errorf("Expected ErrRegexNotEnabled, got: %v", err)
		}
	})

	t.Run("Non-regex operators pass without regex:true", func(t *testing.T) {
		f, err := Parse([]any{"email", "Eq", "test@example.com"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		schema := newMockSchema() // empty - no regex attributes
		if err := f.ValidateWithSchema(schema); err != nil {
			t.Errorf("Non-regex operators should pass validation: %v", err)
		}
	})

	t.Run("Regex nested in And validated", func(t *testing.T) {
		f, err := Parse([]any{"And", []any{
			[]any{"status", "Eq", "active"},
			[]any{"email", "Regex", ".*@example\\.com"},
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// With email having regex:true - should pass
		schemaOk := newMockSchema("email")
		if err := f.ValidateWithSchema(schemaOk); err != nil {
			t.Errorf("Nested Regex with schema should pass: %v", err)
		}

		// Without email having regex:true - should fail
		schemaNo := newMockSchema()
		err = f.ValidateWithSchema(schemaNo)
		if err == nil {
			t.Error("Nested Regex without schema should fail")
		}
	})

	t.Run("Regex nested in Or validated", func(t *testing.T) {
		f, err := Parse([]any{"Or", []any{
			[]any{"status", "Eq", "deleted"},
			[]any{"name", "Regex", "^spam"},
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// With name having regex:true - should pass
		schemaOk := newMockSchema("name")
		if err := f.ValidateWithSchema(schemaOk); err != nil {
			t.Errorf("Nested Regex in Or with schema should pass: %v", err)
		}

		// Without name having regex:true - should fail
		schemaNo := newMockSchema()
		err = f.ValidateWithSchema(schemaNo)
		if err == nil {
			t.Error("Nested Regex in Or without schema should fail")
		}
	})

	t.Run("Regex nested in Not validated", func(t *testing.T) {
		f, err := Parse([]any{"Not", []any{"email", "Regex", ".*@spam\\.com"}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// With email having regex:true - should pass
		schemaOk := newMockSchema("email")
		if err := f.ValidateWithSchema(schemaOk); err != nil {
			t.Errorf("Nested Regex in Not with schema should pass: %v", err)
		}

		// Without email having regex:true - should fail
		schemaNo := newMockSchema()
		err = f.ValidateWithSchema(schemaNo)
		if err == nil {
			t.Error("Nested Regex in Not without schema should fail")
		}
	})

	t.Run("Nil filter passes validation", func(t *testing.T) {
		var f *Filter = nil
		schema := newMockSchema()
		if err := f.ValidateWithSchema(schema); err != nil {
			t.Errorf("Nil filter should pass validation: %v", err)
		}
	})
}

// TestRegex_TaskVerification tests exactly the verification steps for the filter-regex task.
func TestRegex_TaskVerification(t *testing.T) {
	t.Run("Regex_matches_string_patterns", func(t *testing.T) {
		// Test Regex matches string patterns

		// Simple pattern matching
		f1, err := Parse([]any{"email", "Regex", "user@example\\.com"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f1.Eval(Document{"email": "user@example.com"}) {
			t.Error("Regex should match exact pattern")
		}
		if f1.Eval(Document{"email": "other@example.com"}) {
			t.Error("Regex should not match different string")
		}

		// Pattern with wildcards
		f2, err := Parse([]any{"email", "Regex", ".*@example\\.com$"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f2.Eval(Document{"email": "anyone@example.com"}) {
			t.Error("Regex with .* should match any prefix")
		}
		if !f2.Eval(Document{"email": "test@example.com"}) {
			t.Error("Regex with .* should match another prefix")
		}
		if f2.Eval(Document{"email": "user@example.com.evil"}) {
			t.Error("Regex with $ anchor should not match suffix")
		}

		// Character classes
		f3, err := Parse([]any{"code", "Regex", "^[A-Z]{2}\\d{4}$"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f3.Eval(Document{"code": "AB1234"}) {
			t.Error("Regex should match valid code pattern")
		}
		if f3.Eval(Document{"code": "ab1234"}) {
			t.Error("Regex should be case sensitive")
		}
		if f3.Eval(Document{"code": "ABC12345"}) {
			t.Error("Regex should not match wrong length")
		}

		// Empty and special cases
		f4, err := Parse([]any{"text", "Regex", "^$"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f4.Eval(Document{"text": ""}) {
			t.Error("Regex ^$ should match empty string")
		}
		if f4.Eval(Document{"text": "not empty"}) {
			t.Error("Regex ^$ should not match non-empty string")
		}
	})

	t.Run("regex_true_must_be_set_in_schema", func(t *testing.T) {
		// Verify regex: true must be set in schema

		f, err := Parse([]any{"searchable", "Regex", "pattern"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Schema with regex:true for the attribute - should pass
		schemaWithRegex := newMockSchema("searchable")
		if err := f.ValidateWithSchema(schemaWithRegex); err != nil {
			t.Errorf("Regex should be valid when schema has regex:true: %v", err)
		}

		// Schema without regex:true - should fail validation
		schemaWithoutRegex := newMockSchema("other_attr")
		err = f.ValidateWithSchema(schemaWithoutRegex)
		if err == nil {
			t.Error("Regex should require regex:true in schema")
		}

		// Multiple regex-enabled attributes
		schemaMultiple := newMockSchema("searchable", "description", "title")
		if err := f.ValidateWithSchema(schemaMultiple); err != nil {
			t.Errorf("Should work when attribute is in list of regex-enabled: %v", err)
		}
	})

	t.Run("regex_without_schema_option_returns_error", func(t *testing.T) {
		// Test regex without schema option returns error

		f, err := Parse([]any{"text", "Regex", ".*"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// No schema at all - should return error
		err = f.ValidateWithSchema(nil)
		if err == nil {
			t.Error("Regex with nil schema should return error")
		}
		if !errors.Is(err, ErrRegexNotEnabled) {
			t.Errorf("Error should be ErrRegexNotEnabled, got: %v", err)
		}

		// Empty schema (no regex-enabled attributes) - should return error
		emptySchema := newMockSchema()
		err = f.ValidateWithSchema(emptySchema)
		if err == nil {
			t.Error("Regex with empty schema should return error")
		}
		if !errors.Is(err, ErrRegexNotEnabled) {
			t.Errorf("Error should be ErrRegexNotEnabled, got: %v", err)
		}

		// Schema with different attribute - should return error
		wrongAttrSchema := newMockSchema("email")
		err = f.ValidateWithSchema(wrongAttrSchema)
		if err == nil {
			t.Error("Regex with schema for different attribute should return error")
		}
		if !errors.Is(err, ErrRegexNotEnabled) {
			t.Errorf("Error should be ErrRegexNotEnabled, got: %v", err)
		}

		// NotRegex should also require schema
		f2, err := Parse([]any{"text", "NotRegex", "spam"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		err = f2.ValidateWithSchema(emptySchema)
		if err == nil {
			t.Error("NotRegex without schema option should also return error")
		}
	})
}

func TestIsRegexOp(t *testing.T) {
	tests := []struct {
		op       Operator
		expected bool
	}{
		{OpRegex, true},
		{OpNotRegex, true},
		{OpEq, false},
		{OpNotEq, false},
		{OpGlob, false},
		{OpAnd, false},
	}

	for _, tc := range tests {
		t.Run(string(tc.op), func(t *testing.T) {
			if got := tc.op.IsRegexOp(); got != tc.expected {
				t.Errorf("IsRegexOp() = %v, want %v", got, tc.expected)
			}
		})
	}
}
