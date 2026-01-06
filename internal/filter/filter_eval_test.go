package filter

import (
	"testing"
)

// TestAllFilterOperators verifies that all filter operators are implemented and work correctly.
// This is a comprehensive test covering all operators defined in filter.go.
func TestAllFilterOperators(t *testing.T) {
	// Sample documents for testing
	doc := Document{
		"name":      "alice",
		"age":       float64(30),
		"active":    true,
		"tags":      []any{"admin", "user", "premium"},
		"scores":    []any{float64(85), float64(90), float64(95)},
		"optional":  nil,
		"text":      "The quick brown fox jumps",
		"path":      "/home/user/docs/file.txt",
		"email":     "alice@example.com",
		"nilField":  nil,
		"emptyTags": []any{},
	}

	docMissing := Document{
		"other": "value",
	}

	t.Run("Boolean_Operators", func(t *testing.T) {
		// And
		t.Run("And", func(t *testing.T) {
			f, err := Parse([]any{"And", []any{
				[]any{"name", "Eq", "alice"},
				[]any{"age", "Eq", float64(30)},
			}})
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			if !f.Eval(doc) {
				t.Error("And should match when all children match")
			}

			// And with one failing
			f2, _ := Parse([]any{"And", []any{
				[]any{"name", "Eq", "alice"},
				[]any{"age", "Eq", float64(99)},
			}})
			if f2.Eval(doc) {
				t.Error("And should not match when any child fails")
			}

			// Empty And
			f3, _ := Parse([]any{"And", []any{}})
			if !f3.Eval(doc) {
				t.Error("Empty And should match (vacuously true)")
			}
		})

		// Or
		t.Run("Or", func(t *testing.T) {
			f, err := Parse([]any{"Or", []any{
				[]any{"name", "Eq", "bob"},
				[]any{"age", "Eq", float64(30)},
			}})
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			if !f.Eval(doc) {
				t.Error("Or should match when any child matches")
			}

			// Or with all failing
			f2, _ := Parse([]any{"Or", []any{
				[]any{"name", "Eq", "bob"},
				[]any{"age", "Eq", float64(99)},
			}})
			if f2.Eval(doc) {
				t.Error("Or should not match when all children fail")
			}

			// Empty Or
			f3, _ := Parse([]any{"Or", []any{}})
			if !f3.Eval(doc) {
				t.Error("Empty Or should match (vacuously true)")
			}
		})

		// Not
		t.Run("Not", func(t *testing.T) {
			f, err := Parse([]any{"Not", []any{"name", "Eq", "bob"}})
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			if !f.Eval(doc) {
				t.Error("Not should invert false to true")
			}

			f2, _ := Parse([]any{"Not", []any{"name", "Eq", "alice"}})
			if f2.Eval(doc) {
				t.Error("Not should invert true to false")
			}

			// Not with nil child (directly constructed filter)
			f3 := &Filter{Op: OpNot}
			if !f3.Eval(doc) {
				t.Error("Not with nil child should match")
			}
		})
	})

	t.Run("Comparison_Operators", func(t *testing.T) {
		// Eq
		t.Run("Eq", func(t *testing.T) {
			f, _ := Parse([]any{"name", "Eq", "alice"})
			if !f.Eval(doc) {
				t.Error("Eq should match exact value")
			}

			f2, _ := Parse([]any{"name", "Eq", "bob"})
			if f2.Eval(doc) {
				t.Error("Eq should not match different value")
			}
		})

		// NotEq
		t.Run("NotEq", func(t *testing.T) {
			f, _ := Parse([]any{"name", "NotEq", "bob"})
			if !f.Eval(doc) {
				t.Error("NotEq should match when values differ")
			}

			f2, _ := Parse([]any{"name", "NotEq", "alice"})
			if f2.Eval(doc) {
				t.Error("NotEq should not match when values are same")
			}
		})

		// In
		t.Run("In", func(t *testing.T) {
			f, _ := Parse([]any{"name", "In", []any{"alice", "bob", "charlie"}})
			if !f.Eval(doc) {
				t.Error("In should match when value is in set")
			}

			f2, _ := Parse([]any{"name", "In", []any{"bob", "charlie"}})
			if f2.Eval(doc) {
				t.Error("In should not match when value is not in set")
			}
		})

		// NotIn
		t.Run("NotIn", func(t *testing.T) {
			f, _ := Parse([]any{"name", "NotIn", []any{"bob", "charlie"}})
			if !f.Eval(doc) {
				t.Error("NotIn should match when value is not in set")
			}

			f2, _ := Parse([]any{"name", "NotIn", []any{"alice", "bob"}})
			if f2.Eval(doc) {
				t.Error("NotIn should not match when value is in set")
			}
		})

		// Lt
		t.Run("Lt", func(t *testing.T) {
			f, _ := Parse([]any{"age", "Lt", float64(40)})
			if !f.Eval(doc) {
				t.Error("Lt should match when value is less")
			}

			f2, _ := Parse([]any{"age", "Lt", float64(30)})
			if f2.Eval(doc) {
				t.Error("Lt should not match when value is equal")
			}

			f3, _ := Parse([]any{"age", "Lt", float64(20)})
			if f3.Eval(doc) {
				t.Error("Lt should not match when value is greater")
			}
		})

		// Lte
		t.Run("Lte", func(t *testing.T) {
			f, _ := Parse([]any{"age", "Lte", float64(40)})
			if !f.Eval(doc) {
				t.Error("Lte should match when value is less")
			}

			f2, _ := Parse([]any{"age", "Lte", float64(30)})
			if !f2.Eval(doc) {
				t.Error("Lte should match when value is equal")
			}

			f3, _ := Parse([]any{"age", "Lte", float64(20)})
			if f3.Eval(doc) {
				t.Error("Lte should not match when value is greater")
			}
		})

		// Gt
		t.Run("Gt", func(t *testing.T) {
			f, _ := Parse([]any{"age", "Gt", float64(20)})
			if !f.Eval(doc) {
				t.Error("Gt should match when value is greater")
			}

			f2, _ := Parse([]any{"age", "Gt", float64(30)})
			if f2.Eval(doc) {
				t.Error("Gt should not match when value is equal")
			}

			f3, _ := Parse([]any{"age", "Gt", float64(40)})
			if f3.Eval(doc) {
				t.Error("Gt should not match when value is less")
			}
		})

		// Gte
		t.Run("Gte", func(t *testing.T) {
			f, _ := Parse([]any{"age", "Gte", float64(20)})
			if !f.Eval(doc) {
				t.Error("Gte should match when value is greater")
			}

			f2, _ := Parse([]any{"age", "Gte", float64(30)})
			if !f2.Eval(doc) {
				t.Error("Gte should match when value is equal")
			}

			f3, _ := Parse([]any{"age", "Gte", float64(40)})
			if f3.Eval(doc) {
				t.Error("Gte should not match when value is less")
			}
		})
	})

	t.Run("Array_Operators", func(t *testing.T) {
		// Contains
		t.Run("Contains", func(t *testing.T) {
			f, _ := Parse([]any{"tags", "Contains", "admin"})
			if !f.Eval(doc) {
				t.Error("Contains should match when array contains value")
			}

			f2, _ := Parse([]any{"tags", "Contains", "guest"})
			if f2.Eval(doc) {
				t.Error("Contains should not match when array does not contain value")
			}

			f3, _ := Parse([]any{"emptyTags", "Contains", "admin"})
			if f3.Eval(doc) {
				t.Error("Contains should not match on empty array")
			}
		})

		// NotContains
		t.Run("NotContains", func(t *testing.T) {
			f, _ := Parse([]any{"tags", "NotContains", "guest"})
			if !f.Eval(doc) {
				t.Error("NotContains should match when array does not contain value")
			}

			f2, _ := Parse([]any{"tags", "NotContains", "admin"})
			if f2.Eval(doc) {
				t.Error("NotContains should not match when array contains value")
			}
		})

		// ContainsAny
		t.Run("ContainsAny", func(t *testing.T) {
			f, _ := Parse([]any{"tags", "ContainsAny", []any{"admin", "guest"}})
			if !f.Eval(doc) {
				t.Error("ContainsAny should match when array contains any value")
			}

			f2, _ := Parse([]any{"tags", "ContainsAny", []any{"guest", "visitor"}})
			if f2.Eval(doc) {
				t.Error("ContainsAny should not match when array contains no values from set")
			}
		})

		// NotContainsAny
		t.Run("NotContainsAny", func(t *testing.T) {
			f, _ := Parse([]any{"tags", "NotContainsAny", []any{"guest", "visitor"}})
			if !f.Eval(doc) {
				t.Error("NotContainsAny should match when array contains none of the values")
			}

			f2, _ := Parse([]any{"tags", "NotContainsAny", []any{"admin", "guest"}})
			if f2.Eval(doc) {
				t.Error("NotContainsAny should not match when array contains any value from set")
			}
		})

		// AnyLt
		t.Run("AnyLt", func(t *testing.T) {
			f, _ := Parse([]any{"scores", "AnyLt", float64(90)})
			if !f.Eval(doc) {
				t.Error("AnyLt should match when any element is less than value")
			}

			f2, _ := Parse([]any{"scores", "AnyLt", float64(85)})
			if f2.Eval(doc) {
				t.Error("AnyLt should not match when no element is less than value")
			}
		})

		// AnyLte
		t.Run("AnyLte", func(t *testing.T) {
			f, _ := Parse([]any{"scores", "AnyLte", float64(85)})
			if !f.Eval(doc) {
				t.Error("AnyLte should match when any element is less than or equal to value")
			}

			f2, _ := Parse([]any{"scores", "AnyLte", float64(84)})
			if f2.Eval(doc) {
				t.Error("AnyLte should not match when no element is less than or equal to value")
			}
		})

		// AnyGt
		t.Run("AnyGt", func(t *testing.T) {
			f, _ := Parse([]any{"scores", "AnyGt", float64(90)})
			if !f.Eval(doc) {
				t.Error("AnyGt should match when any element is greater than value")
			}

			f2, _ := Parse([]any{"scores", "AnyGt", float64(95)})
			if f2.Eval(doc) {
				t.Error("AnyGt should not match when no element is greater than value")
			}
		})

		// AnyGte
		t.Run("AnyGte", func(t *testing.T) {
			f, _ := Parse([]any{"scores", "AnyGte", float64(95)})
			if !f.Eval(doc) {
				t.Error("AnyGte should match when any element is greater than or equal to value")
			}

			f2, _ := Parse([]any{"scores", "AnyGte", float64(96)})
			if f2.Eval(doc) {
				t.Error("AnyGte should not match when no element is greater than or equal to value")
			}
		})
	})

	t.Run("Glob_Operators", func(t *testing.T) {
		// Glob
		t.Run("Glob", func(t *testing.T) {
			f, _ := Parse([]any{"path", "Glob", "/home/*"})
			if !f.Eval(doc) {
				t.Error("Glob should match when pattern matches")
			}

			f2, _ := Parse([]any{"path", "Glob", "/var/*"})
			if f2.Eval(doc) {
				t.Error("Glob should not match when pattern does not match")
			}
		})

		// NotGlob
		t.Run("NotGlob", func(t *testing.T) {
			f, _ := Parse([]any{"path", "NotGlob", "/var/*"})
			if !f.Eval(doc) {
				t.Error("NotGlob should match when pattern does not match")
			}

			f2, _ := Parse([]any{"path", "NotGlob", "/home/*"})
			if f2.Eval(doc) {
				t.Error("NotGlob should not match when pattern matches")
			}
		})

		// IGlob (case-insensitive)
		t.Run("IGlob", func(t *testing.T) {
			f, _ := Parse([]any{"path", "IGlob", "/HOME/*"})
			if !f.Eval(doc) {
				t.Error("IGlob should match case-insensitively")
			}
		})

		// NotIGlob
		t.Run("NotIGlob", func(t *testing.T) {
			f, _ := Parse([]any{"path", "NotIGlob", "/VAR/*"})
			if !f.Eval(doc) {
				t.Error("NotIGlob should match when pattern does not match (case-insensitive)")
			}
		})
	})

	t.Run("Regex_Operators", func(t *testing.T) {
		// Regex
		t.Run("Regex", func(t *testing.T) {
			f, _ := Parse([]any{"email", "Regex", "^[a-z]+@"})
			if !f.Eval(doc) {
				t.Error("Regex should match when pattern matches")
			}

			f2, _ := Parse([]any{"email", "Regex", "^[0-9]+@"})
			if f2.Eval(doc) {
				t.Error("Regex should not match when pattern does not match")
			}
		})

		// NotRegex
		t.Run("NotRegex", func(t *testing.T) {
			f, _ := Parse([]any{"email", "NotRegex", "^[0-9]+@"})
			if !f.Eval(doc) {
				t.Error("NotRegex should match when pattern does not match")
			}

			f2, _ := Parse([]any{"email", "NotRegex", "^[a-z]+@"})
			if f2.Eval(doc) {
				t.Error("NotRegex should not match when pattern matches")
			}
		})
	})

	t.Run("Token_Operators", func(t *testing.T) {
		// ContainsTokenSequence
		t.Run("ContainsTokenSequence", func(t *testing.T) {
			f, _ := Parse([]any{"text", "ContainsTokenSequence", "quick brown"})
			if !f.Eval(doc) {
				t.Error("ContainsTokenSequence should match adjacent tokens in order")
			}

			f2, _ := Parse([]any{"text", "ContainsTokenSequence", "brown quick"})
			if f2.Eval(doc) {
				t.Error("ContainsTokenSequence should not match tokens in wrong order")
			}

			f3, _ := Parse([]any{"text", "ContainsTokenSequence", "the fox"})
			if f3.Eval(doc) {
				t.Error("ContainsTokenSequence should not match non-adjacent tokens")
			}
		})

		// ContainsAllTokens
		t.Run("ContainsAllTokens", func(t *testing.T) {
			f, _ := Parse([]any{"text", "ContainsAllTokens", "fox quick"})
			if !f.Eval(doc) {
				t.Error("ContainsAllTokens should match tokens in any order")
			}

			f2, _ := Parse([]any{"text", "ContainsAllTokens", "fox cat"})
			if f2.Eval(doc) {
				t.Error("ContainsAllTokens should not match when token is missing")
			}
		})

		// ContainsTokenSequence with last_as_prefix
		t.Run("ContainsTokenSequence_LastAsPrefix", func(t *testing.T) {
			f, _ := Parse([]any{"text", "ContainsTokenSequence", map[string]any{
				"query":          "quick bro",
				"last_as_prefix": true,
			}})
			if !f.Eval(doc) {
				t.Error("ContainsTokenSequence with last_as_prefix should match prefix of last token")
			}
		})

		// ContainsAllTokens with last_as_prefix
		t.Run("ContainsAllTokens_LastAsPrefix", func(t *testing.T) {
			f, _ := Parse([]any{"text", "ContainsAllTokens", map[string]any{
				"query":          "fox qui",
				"last_as_prefix": true,
			}})
			if !f.Eval(doc) {
				t.Error("ContainsAllTokens with last_as_prefix should match prefix of last token")
			}
		})
	})

	t.Run("Missing_Attribute_Behavior", func(t *testing.T) {
		// Most operators should return false for missing attributes
		t.Run("Eq_missing", func(t *testing.T) {
			f, _ := Parse([]any{"missing", "Eq", "value"})
			if f.Eval(docMissing) {
				t.Error("Eq should not match when attribute is missing (and not comparing to null)")
			}
		})

		t.Run("Lt_missing", func(t *testing.T) {
			f, _ := Parse([]any{"missing", "Lt", float64(10)})
			if f.Eval(docMissing) {
				t.Error("Lt should not match when attribute is missing")
			}
		})

		t.Run("Gt_missing", func(t *testing.T) {
			f, _ := Parse([]any{"missing", "Gt", float64(10)})
			if f.Eval(docMissing) {
				t.Error("Gt should not match when attribute is missing")
			}
		})

		t.Run("Contains_missing", func(t *testing.T) {
			f, _ := Parse([]any{"missing", "Contains", "value"})
			if f.Eval(docMissing) {
				t.Error("Contains should not match when attribute is missing")
			}
		})

		t.Run("In_missing", func(t *testing.T) {
			f, _ := Parse([]any{"missing", "In", []any{"a", "b", "c"}})
			if f.Eval(docMissing) {
				t.Error("In should not match when attribute is missing")
			}
		})

		t.Run("Glob_missing", func(t *testing.T) {
			f, _ := Parse([]any{"missing", "Glob", "*"})
			if f.Eval(docMissing) {
				t.Error("Glob should not match when attribute is missing")
			}
		})

		t.Run("Regex_missing", func(t *testing.T) {
			f, _ := Parse([]any{"missing", "Regex", ".*"})
			if f.Eval(docMissing) {
				t.Error("Regex should not match when attribute is missing")
			}
		})

		// NotEq should match for missing attribute (missing != value)
		t.Run("NotEq_missing", func(t *testing.T) {
			f, _ := Parse([]any{"missing", "NotEq", "value"})
			if !f.Eval(docMissing) {
				t.Error("NotEq should match when attribute is missing (missing != value)")
			}
		})

		// NotIn should match for missing attribute
		t.Run("NotIn_missing", func(t *testing.T) {
			f, _ := Parse([]any{"missing", "NotIn", []any{"a", "b", "c"}})
			if !f.Eval(docMissing) {
				t.Error("NotIn should match when attribute is missing")
			}
		})

		// NotContains should match for missing attribute
		t.Run("NotContains_missing", func(t *testing.T) {
			f, _ := Parse([]any{"missing", "NotContains", "value"})
			if !f.Eval(docMissing) {
				t.Error("NotContains should match when attribute is missing")
			}
		})
	})
}

// TestNullSemantics verifies the null/missing attribute semantics per the spec.
// Eq null matches missing attribute, NotEq null matches attribute present.
func TestNullSemantics(t *testing.T) {
	docWithField := Document{"field": "value", "nilField": nil}
	docWithoutField := Document{"other": "value"}

	t.Run("Eq_null_matches_missing", func(t *testing.T) {
		f, err := Parse([]any{"field", "Eq", nil})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should NOT match when field has a value
		if f.Eval(docWithField) {
			t.Error("Eq null should not match when field has a value")
		}

		// Should match when field is missing
		if !f.Eval(docWithoutField) {
			t.Error("Eq null should match when field is missing")
		}
	})

	t.Run("Eq_null_matches_nil_value", func(t *testing.T) {
		f, err := Parse([]any{"nilField", "Eq", nil})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when field is explicitly nil
		if !f.Eval(docWithField) {
			t.Error("Eq null should match when field is explicit nil")
		}
	})

	t.Run("NotEq_null_matches_present", func(t *testing.T) {
		f, err := Parse([]any{"field", "NotEq", nil})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when field has a value
		if !f.Eval(docWithField) {
			t.Error("NotEq null should match when field is present with value")
		}

		// Should NOT match when field is missing
		if f.Eval(docWithoutField) {
			t.Error("NotEq null should not match when field is missing")
		}
	})

	t.Run("NotEq_null_not_match_nil_value", func(t *testing.T) {
		f, err := Parse([]any{"nilField", "NotEq", nil})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should NOT match when field is explicitly nil
		if f.Eval(docWithField) {
			t.Error("NotEq null should not match when field is explicit nil")
		}
	})

	t.Run("Eq_null_various_attributes", func(t *testing.T) {
		doc := Document{
			"present_string": "hello",
			"present_int":    float64(42),
			"present_bool":   true,
			"present_array":  []any{"a", "b"},
			"nil_value":      nil,
		}
		docEmpty := Document{}

		// Test that Eq null matches missing attributes AND explicit nil
		tests := []struct {
			attr     string
			expected bool
		}{
			{"present_string", false},
			{"present_int", false},
			{"present_bool", false},
			{"present_array", false},
			{"nil_value", true}, // explicit nil matches Eq null
			{"nonexistent", true},
		}

		for _, tc := range tests {
			f, _ := Parse([]any{tc.attr, "Eq", nil})
			got := f.Eval(doc)
			if got != tc.expected {
				t.Errorf("Eq null on %q: got %v, want %v", tc.attr, got, tc.expected)
			}
		}

		// Empty doc: all attributes are missing
		f, _ := Parse([]any{"anything", "Eq", nil})
		if !f.Eval(docEmpty) {
			t.Error("Eq null should match any missing field in empty doc")
		}
	})

	t.Run("null_in_complex_filters", func(t *testing.T) {
		doc := Document{"name": "alice"}
		docActive := Document{"name": "bob", "deleted_at": nil}
		docDeleted := Document{"name": "charlie", "deleted_at": "2024-01-01"}

		// Filter: name = "alice" AND deleted_at is null (missing or explicit nil)
		f, _ := Parse([]any{"And", []any{
			[]any{"name", "Eq", "alice"},
			[]any{"deleted_at", "Eq", nil},
		}})

		if !f.Eval(doc) {
			t.Error("Should match alice with missing deleted_at")
		}
		// docActive has name="bob", so AND fails due to name check (not deleted_at)
		if f.Eval(docActive) {
			t.Error("Should not match bob (name doesn't match)")
		}
		if f.Eval(docDeleted) {
			t.Error("Should not match charlie with set deleted_at")
		}

		// Filter: deleted_at is NOT null (present with non-nil value)
		f2, _ := Parse([]any{"deleted_at", "NotEq", nil})
		if !f2.Eval(docDeleted) {
			t.Error("NotEq null should match when deleted_at is set")
		}
		if f2.Eval(docActive) {
			t.Error("NotEq null should not match when deleted_at is explicit null")
		}
		if f2.Eval(doc) {
			t.Error("NotEq null should not match when deleted_at is missing")
		}
	})
}

// TestNestedBooleanOperators tests deeply nested boolean operator combinations.
func TestNestedBooleanOperators(t *testing.T) {
	doc := Document{
		"a": float64(1),
		"b": float64(2),
		"c": float64(3),
		"d": float64(4),
		"e": float64(5),
	}

	t.Run("deeply_nested_and_or", func(t *testing.T) {
		// (a=1 AND (b=2 OR (c=3 AND d=4)))
		f, err := Parse([]any{"And", []any{
			[]any{"a", "Eq", float64(1)},
			[]any{"Or", []any{
				[]any{"b", "Eq", float64(2)},
				[]any{"And", []any{
					[]any{"c", "Eq", float64(3)},
					[]any{"d", "Eq", float64(4)},
				}},
			}},
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f.Eval(doc) {
			t.Error("Deeply nested And/Or should match")
		}

		// Test with b=99 (should still match via c=3 AND d=4)
		doc2 := Document{"a": float64(1), "b": float64(99), "c": float64(3), "d": float64(4)}
		if !f.Eval(doc2) {
			t.Error("Should match via second branch of Or")
		}

		// Test with a=99 (should fail at top And)
		doc3 := Document{"a": float64(99), "b": float64(2), "c": float64(3), "d": float64(4)}
		if f.Eval(doc3) {
			t.Error("Should not match when top-level And fails")
		}
	})

	t.Run("multiple_nots", func(t *testing.T) {
		// NOT(NOT(NOT(a=1))) = NOT(a=1) = a!=1
		f, err := Parse([]any{"Not", []any{"Not", []any{"Not", []any{"a", "Eq", float64(1)}}}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if f.Eval(doc) {
			t.Error("Triple Not of a=1 should not match when a=1")
		}

		doc2 := Document{"a": float64(99)}
		if !f.Eval(doc2) {
			t.Error("Triple Not of a=1 should match when a!=1")
		}
	})

	t.Run("not_with_nested_and_or", func(t *testing.T) {
		// NOT(a=1 OR (b=2 AND c=3))
		f, err := Parse([]any{"Not", []any{"Or", []any{
			[]any{"a", "Eq", float64(1)},
			[]any{"And", []any{
				[]any{"b", "Eq", float64(2)},
				[]any{"c", "Eq", float64(3)},
			}},
		}}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// doc has a=1, so Or is true, Not is false
		if f.Eval(doc) {
			t.Error("NOT(a=1 OR ...) should not match when a=1")
		}

		// doc without a=1 but with b=2, c=3
		doc2 := Document{"a": float64(99), "b": float64(2), "c": float64(3)}
		if f.Eval(doc2) {
			t.Error("NOT(... OR (b=2 AND c=3)) should not match when b=2 AND c=3")
		}

		// doc without matching any condition
		doc3 := Document{"a": float64(99), "b": float64(99), "c": float64(99)}
		if !f.Eval(doc3) {
			t.Error("NOT should match when inner Or is false")
		}
	})

	t.Run("complex_four_level_nesting", func(t *testing.T) {
		// ((a=1 AND b=2) OR (c=3 AND d=4)) AND (e=5 OR NOT(a=1))
		f, err := Parse([]any{"And", []any{
			[]any{"Or", []any{
				[]any{"And", []any{
					[]any{"a", "Eq", float64(1)},
					[]any{"b", "Eq", float64(2)},
				}},
				[]any{"And", []any{
					[]any{"c", "Eq", float64(3)},
					[]any{"d", "Eq", float64(4)},
				}},
			}},
			[]any{"Or", []any{
				[]any{"e", "Eq", float64(5)},
				[]any{"Not", []any{"a", "Eq", float64(1)}},
			}},
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// doc satisfies first Or (a=1 AND b=2) and second Or (e=5)
		if !f.Eval(doc) {
			t.Error("Complex nested filter should match doc")
		}

		// doc with e=99 should still match via NOT(a=1) if a!=1, but here a=1
		doc2 := Document{"a": float64(1), "b": float64(2), "c": float64(3), "d": float64(4), "e": float64(99)}
		// First Or matches (a=1 AND b=2), but second Or: e=99 fails, NOT(a=1) fails
		if f.Eval(doc2) {
			t.Error("Should not match when e!=5 and a=1")
		}

		// doc with a=99, b=99, c=3, d=4, e=99: first Or matches via (c=3 AND d=4), second Or via NOT(a=1)
		doc3 := Document{"a": float64(99), "b": float64(99), "c": float64(3), "d": float64(4), "e": float64(99)}
		if !f.Eval(doc3) {
			t.Error("Should match via (c=3 AND d=4) and NOT(a=1)")
		}
	})

	t.Run("mixed_operators_in_nesting", func(t *testing.T) {
		// (name In ["alice", "bob"]) AND (NOT(status Eq "deleted")) AND ((age Gt 18) OR (verified Eq true))
		doc := Document{
			"name":     "alice",
			"status":   "active",
			"age":      float64(25),
			"verified": false,
		}

		f, err := Parse([]any{"And", []any{
			[]any{"name", "In", []any{"alice", "bob"}},
			[]any{"Not", []any{"status", "Eq", "deleted"}},
			[]any{"Or", []any{
				[]any{"age", "Gt", float64(18)},
				[]any{"verified", "Eq", true},
			}},
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		if !f.Eval(doc) {
			t.Error("Mixed operators should match")
		}

		// Test with deleted status
		docDeleted := Document{"name": "alice", "status": "deleted", "age": float64(25), "verified": false}
		if f.Eval(docDeleted) {
			t.Error("Should not match deleted status")
		}

		// Test with age < 18 but verified = true
		docYoungVerified := Document{"name": "bob", "status": "active", "age": float64(15), "verified": true}
		if !f.Eval(docYoungVerified) {
			t.Error("Should match via verified=true branch")
		}
	})

	t.Run("empty_nested_operators", func(t *testing.T) {
		// And with empty Or inside
		f, err := Parse([]any{"And", []any{
			[]any{"a", "Eq", float64(1)},
			[]any{"Or", []any{}}, // Empty Or is true
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f.Eval(doc) {
			t.Error("And with empty Or should match")
		}

		// Or with empty And inside
		f2, err := Parse([]any{"Or", []any{
			[]any{"a", "Eq", float64(99)},
			[]any{"And", []any{}}, // Empty And is true
		}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f2.Eval(doc) {
			t.Error("Or with empty And should match")
		}

		// Not(empty And) = Not(true) = false
		f3, err := Parse([]any{"Not", []any{"And", []any{}}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if f3.Eval(doc) {
			t.Error("Not(empty And) should not match")
		}
	})
}

// TestNilFilterEvaluation tests that a nil filter matches everything.
func TestNilFilterEvaluation(t *testing.T) {
	doc := Document{"any": "value"}

	var f *Filter = nil
	if !f.Eval(doc) {
		t.Error("nil filter should match everything")
	}

	if !f.Eval(Document{}) {
		t.Error("nil filter should match empty document")
	}
}

// TestParseErrors verifies that malformed filter expressions return appropriate errors.
func TestParseErrors(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		errMsg string
	}{
		{"non-array input", "not an array", "filter must be an array"},
		{"empty array", []any{}, "must have at least 2 elements"},
		{"single element", []any{"a"}, "must have at least 2 elements"},
		{"non-string first element", []any{123, "Eq", "val"}, "first element must be a string"},
		{"And with non-array children", []any{"And", "not array"}, "must be an array of filters"},
		{"Or with non-array children", []any{"Or", 123}, "must be an array of filters"},
		{"Not with wrong arity", []any{"Not", []any{"a", "Eq", 1}, "extra"}, "must have exactly 2 elements"},
		{"comparison with non-string op", []any{"attr", 123, "value"}, "must be a string"},
		{"unknown operator", []any{"attr", "InvalidOp", "value"}, "unknown operator"},
		{"comparison wrong arity", []any{"attr", "Eq"}, "must have exactly 3 elements"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.input)
			if err == nil {
				t.Errorf("Expected error containing %q, got nil", tc.errMsg)
			}
		})
	}
}

// TestTypeCoercion tests that different numeric types are compared correctly.
func TestTypeCoercion(t *testing.T) {
	t.Run("int_float_comparison", func(t *testing.T) {
		doc := Document{"value": int(42)}
		f, _ := Parse([]any{"value", "Eq", float64(42)})
		if !f.Eval(doc) {
			t.Error("int and float64 with same value should be equal")
		}
	})

	t.Run("int64_float_comparison", func(t *testing.T) {
		doc := Document{"value": int64(100)}
		f, _ := Parse([]any{"value", "Lt", float64(101)})
		if !f.Eval(doc) {
			t.Error("int64 should be comparable to float64")
		}
	})

	t.Run("array_with_typed_slice", func(t *testing.T) {
		doc := Document{"nums": []int{1, 2, 3}}
		f, _ := Parse([]any{"nums", "Contains", float64(2)})
		if !f.Eval(doc) {
			t.Error("Contains should work with typed int slice and float64 filter value")
		}
	})

	t.Run("array_with_float64_slice", func(t *testing.T) {
		doc := Document{"nums": []float64{1.0, 2.0, 3.0}}
		f, _ := Parse([]any{"nums", "AnyGt", float64(2.5)})
		if !f.Eval(doc) {
			t.Error("AnyGt should work with typed float64 slice")
		}
	})
}

// TestStringComparison tests lexicographic string comparisons.
func TestStringComparison(t *testing.T) {
	t.Run("lexicographic_order", func(t *testing.T) {
		doc := Document{"name": "banana"}

		fLt, _ := Parse([]any{"name", "Lt", "cherry"})
		if !fLt.Eval(doc) {
			t.Error("banana should be Lt cherry")
		}

		fGt, _ := Parse([]any{"name", "Gt", "apple"})
		if !fGt.Eval(doc) {
			t.Error("banana should be Gt apple")
		}

		fEq, _ := Parse([]any{"name", "Eq", "banana"})
		if !fEq.Eval(doc) {
			t.Error("banana should Eq banana")
		}
	})

	t.Run("case_sensitive", func(t *testing.T) {
		doc := Document{"name": "Banana"}

		// Uppercase letters come before lowercase in ASCII
		f, _ := Parse([]any{"name", "Lt", "apple"})
		if !f.Eval(doc) {
			t.Error("Banana (uppercase) should be Lt apple (lowercase) in ASCII")
		}
	})

	t.Run("empty_string", func(t *testing.T) {
		doc := Document{"name": ""}

		f, _ := Parse([]any{"name", "Lt", "a"})
		if !f.Eval(doc) {
			t.Error("empty string should be Lt any non-empty string")
		}

		f2, _ := Parse([]any{"name", "Eq", ""})
		if !f2.Eval(doc) {
			t.Error("empty string should Eq empty string")
		}
	})
}
