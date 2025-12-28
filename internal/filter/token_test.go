package filter

import (
	"testing"
)

func TestTokenize(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"hello world", []string{"hello", "world"}},
		{"Hello World", []string{"hello", "world"}},
		{"HELLO WORLD", []string{"hello", "world"}},
		{"hello, world!", []string{"hello", "world"}},
		{"hello-world", []string{"hello", "world"}},
		{"hello_world", []string{"hello", "world"}},
		{"hello123 world456", []string{"hello123", "world456"}},
		{"  hello   world  ", []string{"hello", "world"}},
		{"", []string{}},
		{"   ", []string{}},
		{".,;!?", []string{}},
		{"one", []string{"one"}},
		{"CamelCase", []string{"camelcase"}},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := tokenize(tc.input)
			if len(got) != len(tc.expected) {
				t.Fatalf("tokenize(%q) = %v, want %v", tc.input, got, tc.expected)
			}
			for i, token := range got {
				if token != tc.expected[i] {
					t.Errorf("tokenize(%q)[%d] = %q, want %q", tc.input, i, token, tc.expected[i])
				}
			}
		})
	}
}

func TestContainsTokenSequence_TaskVerification(t *testing.T) {
	t.Run("adjacent_ordered_tokens", func(t *testing.T) {
		// Test ContainsTokenSequence for adjacent ordered tokens

		// Basic phrase match
		f, err := Parse([]any{"content", "ContainsTokenSequence", "hello world"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match exact phrase
		if !f.Eval(Document{"content": "hello world"}) {
			t.Error("Should match exact phrase 'hello world'")
		}

		// Should match phrase at beginning
		if !f.Eval(Document{"content": "hello world foo bar"}) {
			t.Error("Should match phrase at beginning")
		}

		// Should match phrase at end
		if !f.Eval(Document{"content": "foo bar hello world"}) {
			t.Error("Should match phrase at end")
		}

		// Should match phrase in middle
		if !f.Eval(Document{"content": "foo hello world bar"}) {
			t.Error("Should match phrase in middle")
		}

		// Should be case-insensitive
		if !f.Eval(Document{"content": "Hello World"}) {
			t.Error("Should match case-insensitively")
		}
		if !f.Eval(Document{"content": "HELLO WORLD"}) {
			t.Error("Should match all uppercase")
		}

		// Should ignore punctuation
		if !f.Eval(Document{"content": "hello, world!"}) {
			t.Error("Should match with punctuation")
		}

		// Should NOT match non-adjacent tokens
		if f.Eval(Document{"content": "hello foo world"}) {
			t.Error("Should NOT match when tokens are not adjacent")
		}

		// Should NOT match reversed order
		if f.Eval(Document{"content": "world hello"}) {
			t.Error("Should NOT match reversed order")
		}

		// Should NOT match partial tokens
		if f.Eval(Document{"content": "helloworld"}) {
			t.Error("Should NOT match when tokens are concatenated")
		}

		// Should NOT match when only first token present
		if f.Eval(Document{"content": "hello foo bar"}) {
			t.Error("Should NOT match when only first token present")
		}

		// Should NOT match when only second token present
		if f.Eval(Document{"content": "foo bar world"}) {
			t.Error("Should NOT match when only second token present")
		}
	})

	t.Run("single_token_sequence", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsTokenSequence", "hello"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if !f.Eval(Document{"content": "hello"}) {
			t.Error("Should match single token")
		}
		if !f.Eval(Document{"content": "say hello there"}) {
			t.Error("Should match single token in middle")
		}
		if f.Eval(Document{"content": "world"}) {
			t.Error("Should NOT match when token not present")
		}
	})

	t.Run("empty_query", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsTokenSequence", ""})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if !f.Eval(Document{"content": "anything"}) {
			t.Error("Empty query should match any content")
		}
	})

	t.Run("missing_attribute", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsTokenSequence", "hello"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if f.Eval(Document{"other": "hello"}) {
			t.Error("Should NOT match when attribute is missing")
		}
	})

	t.Run("non_string_attribute", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsTokenSequence", "hello"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if f.Eval(Document{"content": float64(123)}) {
			t.Error("Should NOT match non-string attribute")
		}
		if f.Eval(Document{"content": []any{"hello", "world"}}) {
			t.Error("Should NOT match array attribute")
		}
	})

	t.Run("longer_phrases", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsTokenSequence", "the quick brown fox"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if !f.Eval(Document{"content": "the quick brown fox jumps"}) {
			t.Error("Should match longer phrase at beginning")
		}
		if !f.Eval(Document{"content": "see the quick brown fox run"}) {
			t.Error("Should match longer phrase in middle")
		}
		if f.Eval(Document{"content": "the quick fox brown"}) {
			t.Error("Should NOT match when order is wrong")
		}
	})
}

func TestContainsAllTokens_TaskVerification(t *testing.T) {
	t.Run("tokens_regardless_of_adjacency", func(t *testing.T) {
		// Test ContainsAllTokens regardless of adjacency

		f, err := Parse([]any{"content", "ContainsAllTokens", "hello world"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match adjacent tokens
		if !f.Eval(Document{"content": "hello world"}) {
			t.Error("Should match adjacent tokens")
		}

		// Should match non-adjacent tokens
		if !f.Eval(Document{"content": "hello foo bar world"}) {
			t.Error("Should match non-adjacent tokens")
		}

		// Should match tokens in any order
		if !f.Eval(Document{"content": "world hello"}) {
			t.Error("Should match tokens in reverse order")
		}

		// Should match with extra tokens
		if !f.Eval(Document{"content": "say world and hello to everyone"}) {
			t.Error("Should match with extra tokens interspersed")
		}

		// Should be case-insensitive
		if !f.Eval(Document{"content": "Hello World"}) {
			t.Error("Should match case-insensitively")
		}
		if !f.Eval(Document{"content": "HELLO WORLD"}) {
			t.Error("Should match all uppercase")
		}

		// Should ignore punctuation
		if !f.Eval(Document{"content": "hello, world!"}) {
			t.Error("Should match with punctuation")
		}

		// Should NOT match when only first token present
		if f.Eval(Document{"content": "hello foo bar"}) {
			t.Error("Should NOT match when second token is missing")
		}

		// Should NOT match when only second token present
		if f.Eval(Document{"content": "foo bar world"}) {
			t.Error("Should NOT match when first token is missing")
		}

		// Should NOT match when neither token present
		if f.Eval(Document{"content": "foo bar baz"}) {
			t.Error("Should NOT match when no tokens present")
		}
	})

	t.Run("single_token", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsAllTokens", "hello"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if !f.Eval(Document{"content": "hello"}) {
			t.Error("Should match single token")
		}
		if !f.Eval(Document{"content": "say hello there"}) {
			t.Error("Should match single token in middle")
		}
		if f.Eval(Document{"content": "world"}) {
			t.Error("Should NOT match when token not present")
		}
	})

	t.Run("empty_query", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsAllTokens", ""})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if !f.Eval(Document{"content": "anything"}) {
			t.Error("Empty query should match any content")
		}
	})

	t.Run("missing_attribute", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsAllTokens", "hello"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if f.Eval(Document{"other": "hello"}) {
			t.Error("Should NOT match when attribute is missing")
		}
	})

	t.Run("non_string_attribute", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsAllTokens", "hello"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if f.Eval(Document{"content": float64(123)}) {
			t.Error("Should NOT match non-string attribute")
		}
	})

	t.Run("many_tokens", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsAllTokens", "the quick brown fox"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// All tokens present but scattered
		if !f.Eval(Document{"content": "fox is quick and the color is brown"}) {
			t.Error("Should match when all tokens are present but scattered")
		}

		// One token missing
		if f.Eval(Document{"content": "the quick fox"}) {
			t.Error("Should NOT match when 'brown' is missing")
		}
	})
}

func TestLastAsPrefix_TaskVerification(t *testing.T) {
	t.Run("ContainsTokenSequence_with_prefix", func(t *testing.T) {
		// Test last_as_prefix: true option for prefix matching with ContainsTokenSequence

		f, err := Parse([]any{"content", "ContainsTokenSequence", map[string]any{
			"query":          "hello wor",
			"last_as_prefix": true,
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when last token is a prefix
		if !f.Eval(Document{"content": "hello world"}) {
			t.Error("Should match 'hello world' with prefix 'wor'")
		}
		if !f.Eval(Document{"content": "hello worldwide"}) {
			t.Error("Should match 'hello worldwide' with prefix 'wor'")
		}
		if !f.Eval(Document{"content": "hello wording"}) {
			t.Error("Should match 'hello wording' with prefix 'wor'")
		}

		// Should still require exact match for non-last tokens
		if f.Eval(Document{"content": "hel world"}) {
			t.Error("Should NOT match when first token is partial")
		}

		// Should still require adjacent tokens
		if f.Eval(Document{"content": "hello foo world"}) {
			t.Error("Should NOT match when tokens are not adjacent")
		}

		// Should match when prefix matches (wor is prefix of word)
		if !f.Eval(Document{"content": "hello word"}) {
			t.Error("Should match 'word' because 'wor' is a prefix of 'word'")
		}

		// Should NOT match when prefix doesn't match
		if f.Eval(Document{"content": "hello war"}) {
			t.Error("Should NOT match 'war' because 'wor' is not a prefix of 'war'")
		}
	})

	t.Run("ContainsAllTokens_with_prefix", func(t *testing.T) {
		// Test last_as_prefix: true option for prefix matching with ContainsAllTokens

		f, err := Parse([]any{"content", "ContainsAllTokens", map[string]any{
			"query":          "hello wor",
			"last_as_prefix": true,
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when last token is a prefix
		if !f.Eval(Document{"content": "hello world"}) {
			t.Error("Should match 'hello world' with prefix 'wor'")
		}
		if !f.Eval(Document{"content": "worldwide hello"}) {
			t.Error("Should match 'worldwide hello' with prefix 'wor' (any order)")
		}
		if !f.Eval(Document{"content": "say hello to the worldwide community"}) {
			t.Error("Should match when tokens are scattered")
		}

		// Should still require exact match for non-last tokens
		if f.Eval(Document{"content": "hel worldwide"}) {
			t.Error("Should NOT match when first token is partial")
		}

		// Should NOT match when prefix doesn't match any token
		if f.Eval(Document{"content": "hello war"}) {
			t.Error("Should NOT match 'war' because 'wor' is not a prefix of 'war'")
		}
	})

	t.Run("single_token_prefix", func(t *testing.T) {
		// When there's only one query token and last_as_prefix is true,
		// that token should be treated as a prefix

		f, err := Parse([]any{"content", "ContainsTokenSequence", map[string]any{
			"query":          "hel",
			"last_as_prefix": true,
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if !f.Eval(Document{"content": "hello"}) {
			t.Error("Should match 'hello' with prefix 'hel'")
		}
		if !f.Eval(Document{"content": "say helicopter"}) {
			t.Error("Should match 'helicopter' with prefix 'hel'")
		}
		if f.Eval(Document{"content": "world"}) {
			t.Error("Should NOT match 'world' - no prefix match")
		}
	})

	t.Run("last_as_prefix_false", func(t *testing.T) {
		// When last_as_prefix is false (or missing), require exact match

		f, err := Parse([]any{"content", "ContainsTokenSequence", map[string]any{
			"query":          "hello wor",
			"last_as_prefix": false,
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should require exact match for last token
		if f.Eval(Document{"content": "hello world"}) {
			t.Error("Should NOT match 'world' when expecting exact 'wor'")
		}
		if !f.Eval(Document{"content": "hello wor"}) {
			t.Error("Should match exact 'hello wor'")
		}
	})

	t.Run("object_format_without_last_as_prefix", func(t *testing.T) {
		// Object format without last_as_prefix should default to false

		f, err := Parse([]any{"content", "ContainsTokenSequence", map[string]any{
			"query": "hello wor",
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should require exact match (last_as_prefix defaults to false)
		if f.Eval(Document{"content": "hello world"}) {
			t.Error("Should NOT match 'world' when expecting exact 'wor'")
		}
		if !f.Eval(Document{"content": "hello wor"}) {
			t.Error("Should match exact 'hello wor'")
		}
	})

	t.Run("typeahead_scenario", func(t *testing.T) {
		// Simulate typeahead search as user types

		// User types "qui"
		f1, _ := Parse([]any{"content", "ContainsAllTokens", map[string]any{
			"query":          "qui",
			"last_as_prefix": true,
		}})
		if !f1.Eval(Document{"content": "the quick brown fox"}) {
			t.Error("Should match 'quick' with prefix 'qui'")
		}

		// User types "quick br"
		f2, _ := Parse([]any{"content", "ContainsAllTokens", map[string]any{
			"query":          "quick br",
			"last_as_prefix": true,
		}})
		if !f2.Eval(Document{"content": "the quick brown fox"}) {
			t.Error("Should match 'quick' and 'brown' with prefix 'br'")
		}

		// User types "quick brown f"
		f3, _ := Parse([]any{"content", "ContainsAllTokens", map[string]any{
			"query":          "quick brown f",
			"last_as_prefix": true,
		}})
		if !f3.Eval(Document{"content": "the quick brown fox"}) {
			t.Error("Should match 'quick', 'brown', and 'fox' with prefix 'f'")
		}
	})
}

func TestTokenOperators_EdgeCases(t *testing.T) {
	t.Run("numeric_tokens", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsAllTokens", "test123"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if !f.Eval(Document{"content": "test123 is a valid token"}) {
			t.Error("Should match alphanumeric tokens")
		}
	})

	t.Run("document_with_only_punctuation", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsAllTokens", "hello"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if f.Eval(Document{"content": "...!!!???"}) {
			t.Error("Should NOT match document with only punctuation")
		}
	})

	t.Run("query_with_only_punctuation", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsAllTokens", "..."})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Query tokenizes to empty, should match anything
		if !f.Eval(Document{"content": "hello world"}) {
			t.Error("Empty token query should match any content")
		}
	})

	t.Run("nil_value", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsAllTokens", "hello"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if f.Eval(Document{"content": nil}) {
			t.Error("Should NOT match nil value")
		}
	})

	t.Run("repeated_tokens_in_query", func(t *testing.T) {
		// Query has repeated tokens
		f, err := Parse([]any{"content", "ContainsAllTokens", "hello hello"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if !f.Eval(Document{"content": "hello"}) {
			t.Error("Should match when document has the token once")
		}
	})

	t.Run("sequence_at_exact_length", func(t *testing.T) {
		f, err := Parse([]any{"content", "ContainsTokenSequence", "a b c"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if !f.Eval(Document{"content": "a b c"}) {
			t.Error("Should match exactly 3 tokens in sequence")
		}
	})
}
