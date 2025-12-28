package filter

import (
	"testing"
	"time"
)

func TestParseAndEval_And(t *testing.T) {
	tests := []struct {
		name     string
		filter   any
		doc      Document
		expected bool
	}{
		{
			name:     "empty And is true",
			filter:   []any{"And", []any{}},
			doc:      Document{"a": 1},
			expected: true,
		},
		{
			name: "And with single matching child",
			filter: []any{"And", []any{
				[]any{"a", "Eq", float64(1)},
			}},
			doc:      Document{"a": float64(1)},
			expected: true,
		},
		{
			name: "And with all matching children",
			filter: []any{"And", []any{
				[]any{"a", "Eq", float64(1)},
				[]any{"b", "Eq", "hello"},
			}},
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: true,
		},
		{
			name: "And with one non-matching child",
			filter: []any{"And", []any{
				[]any{"a", "Eq", float64(1)},
				[]any{"b", "Eq", "world"},
			}},
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: false,
		},
		{
			name: "And with all non-matching children",
			filter: []any{"And", []any{
				[]any{"a", "Eq", float64(2)},
				[]any{"b", "Eq", "world"},
			}},
			doc:      Document{"a": float64(1), "b": "hello"},
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

func TestParseAndEval_Or(t *testing.T) {
	tests := []struct {
		name     string
		filter   any
		doc      Document
		expected bool
	}{
		{
			name:     "empty Or is true",
			filter:   []any{"Or", []any{}},
			doc:      Document{"a": 1},
			expected: true,
		},
		{
			name: "Or with single matching child",
			filter: []any{"Or", []any{
				[]any{"a", "Eq", float64(1)},
			}},
			doc:      Document{"a": float64(1)},
			expected: true,
		},
		{
			name: "Or with first matching",
			filter: []any{"Or", []any{
				[]any{"a", "Eq", float64(1)},
				[]any{"b", "Eq", "world"},
			}},
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: true,
		},
		{
			name: "Or with second matching",
			filter: []any{"Or", []any{
				[]any{"a", "Eq", float64(2)},
				[]any{"b", "Eq", "hello"},
			}},
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: true,
		},
		{
			name: "Or with both matching",
			filter: []any{"Or", []any{
				[]any{"a", "Eq", float64(1)},
				[]any{"b", "Eq", "hello"},
			}},
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: true,
		},
		{
			name: "Or with none matching",
			filter: []any{"Or", []any{
				[]any{"a", "Eq", float64(2)},
				[]any{"b", "Eq", "world"},
			}},
			doc:      Document{"a": float64(1), "b": "hello"},
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

func TestParseAndEval_Not(t *testing.T) {
	tests := []struct {
		name     string
		filter   any
		doc      Document
		expected bool
	}{
		{
			name:     "Not inverts true to false",
			filter:   []any{"Not", []any{"a", "Eq", float64(1)}},
			doc:      Document{"a": float64(1)},
			expected: false,
		},
		{
			name:     "Not inverts false to true",
			filter:   []any{"Not", []any{"a", "Eq", float64(2)}},
			doc:      Document{"a": float64(1)},
			expected: true,
		},
		{
			name:     "Not with nil child",
			filter:   []any{"Not", nil},
			doc:      Document{"a": float64(1)},
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

func TestParseAndEval_NestedBoolean(t *testing.T) {
	tests := []struct {
		name     string
		filter   any
		doc      Document
		expected bool
	}{
		{
			name: "And inside Or - matches first And",
			filter: []any{"Or", []any{
				[]any{"And", []any{
					[]any{"a", "Eq", float64(1)},
					[]any{"b", "Eq", "hello"},
				}},
				[]any{"And", []any{
					[]any{"c", "Eq", float64(3)},
				}},
			}},
			doc:      Document{"a": float64(1), "b": "hello", "c": float64(2)},
			expected: true,
		},
		{
			name: "And inside Or - matches second And",
			filter: []any{"Or", []any{
				[]any{"And", []any{
					[]any{"a", "Eq", float64(999)},
					[]any{"b", "Eq", "hello"},
				}},
				[]any{"And", []any{
					[]any{"c", "Eq", float64(2)},
				}},
			}},
			doc:      Document{"a": float64(1), "b": "hello", "c": float64(2)},
			expected: true,
		},
		{
			name: "Or inside And - both must match",
			filter: []any{"And", []any{
				[]any{"Or", []any{
					[]any{"a", "Eq", float64(1)},
					[]any{"a", "Eq", float64(2)},
				}},
				[]any{"Or", []any{
					[]any{"b", "Eq", "hello"},
					[]any{"b", "Eq", "world"},
				}},
			}},
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: true,
		},
		{
			name: "Or inside And - second Or fails",
			filter: []any{"And", []any{
				[]any{"Or", []any{
					[]any{"a", "Eq", float64(1)},
					[]any{"a", "Eq", float64(2)},
				}},
				[]any{"Or", []any{
					[]any{"b", "Eq", "foo"},
					[]any{"b", "Eq", "bar"},
				}},
			}},
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: false,
		},
		{
			name: "Not with And",
			filter: []any{"Not", []any{"And", []any{
				[]any{"a", "Eq", float64(1)},
				[]any{"b", "Eq", "hello"},
			}}},
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: false,
		},
		{
			name: "Not with Or",
			filter: []any{"Not", []any{"Or", []any{
				[]any{"a", "Eq", float64(999)},
				[]any{"b", "Eq", "world"},
			}}},
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: true,
		},
		{
			name: "Double Not",
			filter: []any{"Not", []any{"Not", []any{"a", "Eq", float64(1)}}},
			doc:      Document{"a": float64(1)},
			expected: true,
		},
		{
			name: "Triple nesting - And(Or(Not(...)))",
			filter: []any{"And", []any{
				[]any{"Or", []any{
					[]any{"Not", []any{"a", "Eq", float64(999)}},
					[]any{"b", "Eq", "wrong"},
				}},
				[]any{"c", "Eq", float64(3)},
			}},
			doc:      Document{"a": float64(1), "b": "hello", "c": float64(3)},
			expected: true,
		},
		{
			name: "Complex nested - (a=1 AND b=2) OR (NOT c=3 AND d=4)",
			filter: []any{"Or", []any{
				[]any{"And", []any{
					[]any{"a", "Eq", float64(1)},
					[]any{"b", "Eq", float64(2)},
				}},
				[]any{"And", []any{
					[]any{"Not", []any{"c", "Eq", float64(3)}},
					[]any{"d", "Eq", float64(4)},
				}},
			}},
			doc:      Document{"a": float64(1), "b": float64(999), "c": float64(999), "d": float64(4)},
			expected: true, // second And matches: NOT(c=3) is true, d=4 is true
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

func TestParseAndEval_Comparison(t *testing.T) {
	tests := []struct {
		name     string
		filter   any
		doc      Document
		expected bool
	}{
		// Eq
		{
			name:     "Eq matches",
			filter:   []any{"a", "Eq", float64(1)},
			doc:      Document{"a": float64(1)},
			expected: true,
		},
		{
			name:     "Eq doesn't match",
			filter:   []any{"a", "Eq", float64(2)},
			doc:      Document{"a": float64(1)},
			expected: false,
		},
		{
			name:     "Eq null matches missing",
			filter:   []any{"a", "Eq", nil},
			doc:      Document{"b": float64(1)},
			expected: true,
		},
		{
			name:     "Eq null matches nil value",
			filter:   []any{"a", "Eq", nil},
			doc:      Document{"a": nil},
			expected: true,
		},
		{
			name:     "Eq null doesn't match present",
			filter:   []any{"a", "Eq", nil},
			doc:      Document{"a": float64(1)},
			expected: false,
		},

		// NotEq
		{
			name:     "NotEq matches when different",
			filter:   []any{"a", "NotEq", float64(2)},
			doc:      Document{"a": float64(1)},
			expected: true,
		},
		{
			name:     "NotEq doesn't match when same",
			filter:   []any{"a", "NotEq", float64(1)},
			doc:      Document{"a": float64(1)},
			expected: false,
		},
		{
			name:     "NotEq null matches present",
			filter:   []any{"a", "NotEq", nil},
			doc:      Document{"a": float64(1)},
			expected: true,
		},
		{
			name:     "NotEq null doesn't match missing",
			filter:   []any{"a", "NotEq", nil},
			doc:      Document{"b": float64(1)},
			expected: false,
		},

		// Lt/Lte/Gt/Gte
		{
			name:     "Lt matches",
			filter:   []any{"a", "Lt", float64(10)},
			doc:      Document{"a": float64(5)},
			expected: true,
		},
		{
			name:     "Lt doesn't match equal",
			filter:   []any{"a", "Lt", float64(5)},
			doc:      Document{"a": float64(5)},
			expected: false,
		},
		{
			name:     "Lte matches less",
			filter:   []any{"a", "Lte", float64(10)},
			doc:      Document{"a": float64(5)},
			expected: true,
		},
		{
			name:     "Lte matches equal",
			filter:   []any{"a", "Lte", float64(5)},
			doc:      Document{"a": float64(5)},
			expected: true,
		},
		{
			name:     "Gt matches",
			filter:   []any{"a", "Gt", float64(1)},
			doc:      Document{"a": float64(5)},
			expected: true,
		},
		{
			name:     "Gt doesn't match equal",
			filter:   []any{"a", "Gt", float64(5)},
			doc:      Document{"a": float64(5)},
			expected: false,
		},
		{
			name:     "Gte matches greater",
			filter:   []any{"a", "Gte", float64(1)},
			doc:      Document{"a": float64(5)},
			expected: true,
		},
		{
			name:     "Gte matches equal",
			filter:   []any{"a", "Gte", float64(5)},
			doc:      Document{"a": float64(5)},
			expected: true,
		},

		// String comparison (lexicographic)
		{
			name:     "Lt string lexicographic",
			filter:   []any{"s", "Lt", "banana"},
			doc:      Document{"s": "apple"},
			expected: true,
		},
		{
			name:     "Gt string lexicographic",
			filter:   []any{"s", "Gt", "apple"},
			doc:      Document{"s": "banana"},
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

func TestParseAndEval_In(t *testing.T) {
	tests := []struct {
		name     string
		filter   any
		doc      Document
		expected bool
	}{
		{
			name:     "In matches first",
			filter:   []any{"a", "In", []any{float64(1), float64(2), float64(3)}},
			doc:      Document{"a": float64(1)},
			expected: true,
		},
		{
			name:     "In matches middle",
			filter:   []any{"a", "In", []any{float64(1), float64(2), float64(3)}},
			doc:      Document{"a": float64(2)},
			expected: true,
		},
		{
			name:     "In matches last",
			filter:   []any{"a", "In", []any{float64(1), float64(2), float64(3)}},
			doc:      Document{"a": float64(3)},
			expected: true,
		},
		{
			name:     "In doesn't match",
			filter:   []any{"a", "In", []any{float64(1), float64(2), float64(3)}},
			doc:      Document{"a": float64(4)},
			expected: false,
		},
		{
			name:     "In with strings",
			filter:   []any{"s", "In", []any{"foo", "bar", "baz"}},
			doc:      Document{"s": "bar"},
			expected: true,
		},
		{
			name:     "NotIn matches when not in set",
			filter:   []any{"a", "NotIn", []any{float64(1), float64(2), float64(3)}},
			doc:      Document{"a": float64(4)},
			expected: true,
		},
		{
			name:     "NotIn doesn't match when in set",
			filter:   []any{"a", "NotIn", []any{float64(1), float64(2), float64(3)}},
			doc:      Document{"a": float64(2)},
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

func TestParse_Errors(t *testing.T) {
	tests := []struct {
		name   string
		filter any
	}{
		{
			name:   "non-array",
			filter: "not an array",
		},
		{
			name:   "empty array",
			filter: []any{},
		},
		{
			name:   "single element",
			filter: []any{"foo"},
		},
		{
			name:   "first element not string",
			filter: []any{123, "Eq", 1},
		},
		{
			name:   "And with non-array children",
			filter: []any{"And", "not an array"},
		},
		{
			name:   "Or with non-array children",
			filter: []any{"Or", "not an array"},
		},
		{
			name:   "Not with extra elements",
			filter: []any{"Not", []any{"a", "Eq", 1}, "extra"},
		},
		{
			name:   "comparison with non-string op",
			filter: []any{"a", 123, 1},
		},
		{
			name:   "unknown operator",
			filter: []any{"a", "UnknownOp", 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.filter)
			if err == nil {
				t.Error("Parse should have failed but didn't")
			}
		})
	}
}

func TestParseJSON(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		doc      Document
		expected bool
	}{
		{
			name:     "And from JSON",
			json:     `["And", [["a", "Eq", 1], ["b", "Eq", "hello"]]]`,
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: true,
		},
		{
			name:     "Or from JSON",
			json:     `["Or", [["a", "Eq", 1], ["b", "Eq", "world"]]]`,
			doc:      Document{"a": float64(1), "b": "hello"},
			expected: true,
		},
		{
			name:     "Not from JSON",
			json:     `["Not", ["a", "Eq", 999]]`,
			doc:      Document{"a": float64(1)},
			expected: true,
		},
		{
			name:     "Nested from JSON",
			json:     `["And", [["Or", [["a", "Eq", 1], ["a", "Eq", 2]]], ["Not", ["b", "Eq", "bad"]]]]`,
			doc:      Document{"a": float64(1), "b": "good"},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, err := ParseJSON([]byte(tc.json))
			if err != nil {
				t.Fatalf("ParseJSON failed: %v", err)
			}
			got := f.Eval(tc.doc)
			if got != tc.expected {
				t.Errorf("Eval() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestNilFilter(t *testing.T) {
	var f *Filter
	if !f.Eval(Document{"a": 1}) {
		t.Error("nil filter should match everything")
	}
}

// TestEqNotEq_TaskVerification tests exactly the verification steps for the filter-eq-noteq task.
func TestEqNotEq_TaskVerification(t *testing.T) {
	t.Run("Eq_matches_exact_value", func(t *testing.T) {
		// Test ["attr", "Eq", value] matches exact value
		f, err := Parse([]any{"status", "Eq", "active"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f.Eval(Document{"status": "active"}) {
			t.Error("Eq should match exact value")
		}
		if f.Eval(Document{"status": "inactive"}) {
			t.Error("Eq should not match different value")
		}
	})

	t.Run("Eq_null_matches_missing_attribute", func(t *testing.T) {
		// Test ["attr", "Eq", null] matches missing attribute
		f, err := Parse([]any{"optional_field", "Eq", nil})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f.Eval(Document{"other": "value"}) {
			t.Error("Eq null should match when attribute is missing")
		}
		if !f.Eval(Document{"optional_field": nil}) {
			t.Error("Eq null should match when attribute is nil")
		}
		if f.Eval(Document{"optional_field": "exists"}) {
			t.Error("Eq null should not match when attribute has a value")
		}
	})

	t.Run("NotEq_excludes_exact_value", func(t *testing.T) {
		// Test ["attr", "NotEq", value] excludes exact value
		f, err := Parse([]any{"status", "NotEq", "deleted"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f.Eval(Document{"status": "active"}) {
			t.Error("NotEq should match when value is different")
		}
		if f.Eval(Document{"status": "deleted"}) {
			t.Error("NotEq should not match when value is the same")
		}
		// Missing attribute should also pass NotEq (not equal to "deleted")
		if !f.Eval(Document{"other": "value"}) {
			t.Error("NotEq should match when attribute is missing")
		}
	})

	t.Run("NotEq_null_matches_attribute_present", func(t *testing.T) {
		// Test ["attr", "NotEq", null] matches attribute present
		f, err := Parse([]any{"required_field", "NotEq", nil})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !f.Eval(Document{"required_field": "any_value"}) {
			t.Error("NotEq null should match when attribute is present with a value")
		}
		if !f.Eval(Document{"required_field": float64(123)}) {
			t.Error("NotEq null should match when attribute is present with a numeric value")
		}
		if f.Eval(Document{"other": "value"}) {
			t.Error("NotEq null should not match when attribute is missing")
		}
		if f.Eval(Document{"required_field": nil}) {
			t.Error("NotEq null should not match when attribute is nil")
		}
	})
}

// TestInNotIn_TaskVerification tests exactly the verification steps for the filter-in-notin task.
func TestInNotIn_TaskVerification(t *testing.T) {
	t.Run("In_matches_any_value_in_set", func(t *testing.T) {
		// Test In operator matches any value in set
		f, err := Parse([]any{"category", "In", []any{"electronics", "books", "clothing"}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match first value
		if !f.Eval(Document{"category": "electronics"}) {
			t.Error("In should match first value in set")
		}

		// Should match middle value
		if !f.Eval(Document{"category": "books"}) {
			t.Error("In should match middle value in set")
		}

		// Should match last value
		if !f.Eval(Document{"category": "clothing"}) {
			t.Error("In should match last value in set")
		}

		// Should not match value outside set
		if f.Eval(Document{"category": "toys"}) {
			t.Error("In should not match value outside set")
		}

		// Test with numeric values
		fNumeric, err := Parse([]any{"priority", "In", []any{float64(1), float64(2), float64(3)}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fNumeric.Eval(Document{"priority": float64(2)}) {
			t.Error("In should match numeric value in set")
		}
		if fNumeric.Eval(Document{"priority": float64(5)}) {
			t.Error("In should not match numeric value outside set")
		}

		// Missing attribute should not match
		if f.Eval(Document{"other_field": "value"}) {
			t.Error("In should not match when attribute is missing")
		}
	})

	t.Run("NotIn_excludes_all_values_in_set", func(t *testing.T) {
		// Test NotIn operator excludes all values in set
		f, err := Parse([]any{"status", "NotIn", []any{"deleted", "archived", "spam"}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match value outside set
		if !f.Eval(Document{"status": "active"}) {
			t.Error("NotIn should match value outside set")
		}
		if !f.Eval(Document{"status": "pending"}) {
			t.Error("NotIn should match another value outside set")
		}

		// Should not match first value in set
		if f.Eval(Document{"status": "deleted"}) {
			t.Error("NotIn should not match first value in set")
		}

		// Should not match middle value in set
		if f.Eval(Document{"status": "archived"}) {
			t.Error("NotIn should not match middle value in set")
		}

		// Should not match last value in set
		if f.Eval(Document{"status": "spam"}) {
			t.Error("NotIn should not match last value in set")
		}

		// Test with numeric values
		fNumeric, err := Parse([]any{"level", "NotIn", []any{float64(0), float64(-1)}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fNumeric.Eval(Document{"level": float64(5)}) {
			t.Error("NotIn should match numeric value outside set")
		}
		if fNumeric.Eval(Document{"level": float64(0)}) {
			t.Error("NotIn should not match numeric value in set")
		}

		// Missing attribute: NotIn should match (not in set = true)
		if !f.Eval(Document{"other_field": "value"}) {
			t.Error("NotIn should match when attribute is missing (missing is not in set)")
		}
	})
}

func TestArrayOperators(t *testing.T) {
	tests := []struct {
		name     string
		filter   any
		doc      Document
		expected bool
	}{
		{
			name:     "Contains matches",
			filter:   []any{"tags", "Contains", "foo"},
			doc:      Document{"tags": []any{"foo", "bar", "baz"}},
			expected: true,
		},
		{
			name:     "Contains doesn't match",
			filter:   []any{"tags", "Contains", "qux"},
			doc:      Document{"tags": []any{"foo", "bar", "baz"}},
			expected: false,
		},
		{
			name:     "NotContains matches",
			filter:   []any{"tags", "NotContains", "qux"},
			doc:      Document{"tags": []any{"foo", "bar", "baz"}},
			expected: true,
		},
		{
			name:     "NotContains doesn't match",
			filter:   []any{"tags", "NotContains", "foo"},
			doc:      Document{"tags": []any{"foo", "bar", "baz"}},
			expected: false,
		},
		{
			name:     "ContainsAny matches first",
			filter:   []any{"tags", "ContainsAny", []any{"foo", "qux"}},
			doc:      Document{"tags": []any{"foo", "bar", "baz"}},
			expected: true,
		},
		{
			name:     "ContainsAny matches second",
			filter:   []any{"tags", "ContainsAny", []any{"qux", "bar"}},
			doc:      Document{"tags": []any{"foo", "bar", "baz"}},
			expected: true,
		},
		{
			name:     "ContainsAny doesn't match",
			filter:   []any{"tags", "ContainsAny", []any{"qux", "quux"}},
			doc:      Document{"tags": []any{"foo", "bar", "baz"}},
			expected: false,
		},
		{
			name:     "NotContainsAny matches",
			filter:   []any{"tags", "NotContainsAny", []any{"qux", "quux"}},
			doc:      Document{"tags": []any{"foo", "bar", "baz"}},
			expected: true,
		},
		{
			name:     "NotContainsAny doesn't match",
			filter:   []any{"tags", "NotContainsAny", []any{"foo", "qux"}},
			doc:      Document{"tags": []any{"foo", "bar", "baz"}},
			expected: false,
		},
		{
			name:     "AnyLt matches",
			filter:   []any{"nums", "AnyLt", float64(3)},
			doc:      Document{"nums": []any{float64(1), float64(5), float64(10)}},
			expected: true,
		},
		{
			name:     "AnyLt doesn't match",
			filter:   []any{"nums", "AnyLt", float64(1)},
			doc:      Document{"nums": []any{float64(1), float64(5), float64(10)}},
			expected: false,
		},
		{
			name:     "AnyLte matches equal",
			filter:   []any{"nums", "AnyLte", float64(1)},
			doc:      Document{"nums": []any{float64(1), float64(5), float64(10)}},
			expected: true,
		},
		{
			name:     "AnyGt matches",
			filter:   []any{"nums", "AnyGt", float64(5)},
			doc:      Document{"nums": []any{float64(1), float64(5), float64(10)}},
			expected: true,
		},
		{
			name:     "AnyGte matches equal",
			filter:   []any{"nums", "AnyGte", float64(10)},
			doc:      Document{"nums": []any{float64(1), float64(5), float64(10)}},
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

// TestComparison_TaskVerification tests exactly the verification steps for the filter-comparisons task.
func TestComparison_TaskVerification(t *testing.T) {
	t.Run("Lt_numeric_comparison", func(t *testing.T) {
		// Test Lt operator for numeric comparison
		f, err := Parse([]any{"value", "Lt", float64(10)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when value is less
		if !f.Eval(Document{"value": float64(5)}) {
			t.Error("Lt should match when doc value (5) < filter value (10)")
		}
		if !f.Eval(Document{"value": float64(9.9)}) {
			t.Error("Lt should match when doc value (9.9) < filter value (10)")
		}
		if !f.Eval(Document{"value": float64(0)}) {
			t.Error("Lt should match when doc value (0) < filter value (10)")
		}
		if !f.Eval(Document{"value": float64(-5)}) {
			t.Error("Lt should match when doc value (-5) < filter value (10)")
		}

		// Should not match when equal
		if f.Eval(Document{"value": float64(10)}) {
			t.Error("Lt should not match when doc value equals filter value")
		}

		// Should not match when greater
		if f.Eval(Document{"value": float64(11)}) {
			t.Error("Lt should not match when doc value (11) > filter value (10)")
		}

		// Missing attribute should not match
		if f.Eval(Document{"other": float64(5)}) {
			t.Error("Lt should not match when attribute is missing")
		}

		// Test with integer types
		if !f.Eval(Document{"value": int(5)}) {
			t.Error("Lt should match with int type")
		}
		if !f.Eval(Document{"value": int64(5)}) {
			t.Error("Lt should match with int64 type")
		}
	})

	t.Run("Lte_numeric_comparison", func(t *testing.T) {
		// Test Lte operator for numeric comparison
		f, err := Parse([]any{"value", "Lte", float64(10)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when value is less
		if !f.Eval(Document{"value": float64(5)}) {
			t.Error("Lte should match when doc value (5) < filter value (10)")
		}

		// Should match when equal
		if !f.Eval(Document{"value": float64(10)}) {
			t.Error("Lte should match when doc value equals filter value")
		}

		// Should not match when greater
		if f.Eval(Document{"value": float64(11)}) {
			t.Error("Lte should not match when doc value (11) > filter value (10)")
		}
		if f.Eval(Document{"value": float64(10.001)}) {
			t.Error("Lte should not match when doc value (10.001) > filter value (10)")
		}

		// Missing attribute should not match
		if f.Eval(Document{"other": float64(5)}) {
			t.Error("Lte should not match when attribute is missing")
		}
	})

	t.Run("Gt_numeric_comparison", func(t *testing.T) {
		// Test Gt operator for numeric comparison
		f, err := Parse([]any{"value", "Gt", float64(10)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when value is greater
		if !f.Eval(Document{"value": float64(15)}) {
			t.Error("Gt should match when doc value (15) > filter value (10)")
		}
		if !f.Eval(Document{"value": float64(10.001)}) {
			t.Error("Gt should match when doc value (10.001) > filter value (10)")
		}
		if !f.Eval(Document{"value": float64(100)}) {
			t.Error("Gt should match when doc value (100) > filter value (10)")
		}

		// Should not match when equal
		if f.Eval(Document{"value": float64(10)}) {
			t.Error("Gt should not match when doc value equals filter value")
		}

		// Should not match when less
		if f.Eval(Document{"value": float64(5)}) {
			t.Error("Gt should not match when doc value (5) < filter value (10)")
		}
		if f.Eval(Document{"value": float64(-10)}) {
			t.Error("Gt should not match when doc value (-10) < filter value (10)")
		}

		// Missing attribute should not match
		if f.Eval(Document{"other": float64(15)}) {
			t.Error("Gt should not match when attribute is missing")
		}
	})

	t.Run("Gte_numeric_comparison", func(t *testing.T) {
		// Test Gte operator for numeric comparison
		f, err := Parse([]any{"value", "Gte", float64(10)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when value is greater
		if !f.Eval(Document{"value": float64(15)}) {
			t.Error("Gte should match when doc value (15) > filter value (10)")
		}

		// Should match when equal
		if !f.Eval(Document{"value": float64(10)}) {
			t.Error("Gte should match when doc value equals filter value")
		}

		// Should not match when less
		if f.Eval(Document{"value": float64(5)}) {
			t.Error("Gte should not match when doc value (5) < filter value (10)")
		}
		if f.Eval(Document{"value": float64(9.999)}) {
			t.Error("Gte should not match when doc value (9.999) < filter value (10)")
		}

		// Missing attribute should not match
		if f.Eval(Document{"other": float64(15)}) {
			t.Error("Gte should not match when attribute is missing")
		}
	})

	t.Run("string_lexicographic_comparison", func(t *testing.T) {
		// Verify strings use lexicographic comparison

		// Test Lt for strings
		ltFilter, err := Parse([]any{"name", "Lt", "banana"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !ltFilter.Eval(Document{"name": "apple"}) {
			t.Error("Lt should match 'apple' < 'banana' (lexicographic)")
		}
		if !ltFilter.Eval(Document{"name": "aardvark"}) {
			t.Error("Lt should match 'aardvark' < 'banana' (lexicographic)")
		}
		if ltFilter.Eval(Document{"name": "cherry"}) {
			t.Error("Lt should not match 'cherry' > 'banana' (lexicographic)")
		}
		if ltFilter.Eval(Document{"name": "banana"}) {
			t.Error("Lt should not match 'banana' = 'banana'")
		}

		// Test Lte for strings
		lteFilter, err := Parse([]any{"name", "Lte", "banana"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !lteFilter.Eval(Document{"name": "banana"}) {
			t.Error("Lte should match 'banana' = 'banana'")
		}
		if !lteFilter.Eval(Document{"name": "apple"}) {
			t.Error("Lte should match 'apple' < 'banana'")
		}

		// Test Gt for strings
		gtFilter, err := Parse([]any{"name", "Gt", "banana"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !gtFilter.Eval(Document{"name": "cherry"}) {
			t.Error("Gt should match 'cherry' > 'banana' (lexicographic)")
		}
		if !gtFilter.Eval(Document{"name": "zoo"}) {
			t.Error("Gt should match 'zoo' > 'banana' (lexicographic)")
		}
		if gtFilter.Eval(Document{"name": "apple"}) {
			t.Error("Gt should not match 'apple' < 'banana' (lexicographic)")
		}
		if gtFilter.Eval(Document{"name": "banana"}) {
			t.Error("Gt should not match 'banana' = 'banana'")
		}

		// Test Gte for strings
		gteFilter, err := Parse([]any{"name", "Gte", "banana"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !gteFilter.Eval(Document{"name": "banana"}) {
			t.Error("Gte should match 'banana' = 'banana'")
		}
		if !gteFilter.Eval(Document{"name": "cherry"}) {
			t.Error("Gte should match 'cherry' > 'banana'")
		}

		// Test case sensitivity (uppercase comes before lowercase in ASCII)
		if !ltFilter.Eval(Document{"name": "Apple"}) {
			t.Error("Lt should match 'Apple' < 'banana' (uppercase before lowercase)")
		}
		if !ltFilter.Eval(Document{"name": "BANANA"}) {
			t.Error("Lt should match 'BANANA' < 'banana' (uppercase before lowercase)")
		}
	})

	t.Run("datetime_comparison_as_numeric_milliseconds", func(t *testing.T) {
		// Verify datetimes compare as numeric milliseconds
		now := time.Now()
		past := now.Add(-1 * time.Hour)
		future := now.Add(1 * time.Hour)

		// Create filter comparing against 'now' as time.Time
		ltFilter, err := Parse([]any{"timestamp", "Lt", now})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !ltFilter.Eval(Document{"timestamp": past}) {
			t.Error("Lt should match past < now for time.Time values")
		}
		if ltFilter.Eval(Document{"timestamp": future}) {
			t.Error("Lt should not match future > now for time.Time values")
		}

		// Test Gt with datetimes
		gtFilter, err := Parse([]any{"timestamp", "Gt", now})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !gtFilter.Eval(Document{"timestamp": future}) {
			t.Error("Gt should match future > now for time.Time values")
		}
		if gtFilter.Eval(Document{"timestamp": past}) {
			t.Error("Gt should not match past < now for time.Time values")
		}

		// Test comparing time.Time against numeric milliseconds
		nowMs := now.UnixMilli()

		// Document has time.Time, filter has milliseconds (as float64)
		ltMsFilter, err := Parse([]any{"timestamp", "Lt", float64(nowMs)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !ltMsFilter.Eval(Document{"timestamp": past}) {
			t.Error("Lt should match when comparing time.Time < milliseconds")
		}
		if ltMsFilter.Eval(Document{"timestamp": future}) {
			t.Error("Lt should not match when comparing time.Time > milliseconds")
		}

		// Document has time.Time, filter has milliseconds (as int64)
		ltInt64Filter, err := Parse([]any{"timestamp", "Lt", int64(nowMs)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !ltInt64Filter.Eval(Document{"timestamp": past}) {
			t.Error("Lt should match when comparing time.Time < int64 milliseconds")
		}

		// Test Lte/Gte with exact equal datetime
		lteFilter, err := Parse([]any{"timestamp", "Lte", now})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !lteFilter.Eval(Document{"timestamp": now}) {
			t.Error("Lte should match when timestamps are equal")
		}
		if !lteFilter.Eval(Document{"timestamp": past}) {
			t.Error("Lte should match when timestamp is before")
		}

		gteFilter, err := Parse([]any{"timestamp", "Gte", now})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !gteFilter.Eval(Document{"timestamp": now}) {
			t.Error("Gte should match when timestamps are equal")
		}
		if !gteFilter.Eval(Document{"timestamp": future}) {
			t.Error("Gte should match when timestamp is after")
		}

		// Test with RFC3339 string format
		pastStr := past.Format(time.RFC3339)
		ltStrFilter, err := Parse([]any{"timestamp", "Lt", pastStr})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		// Document time is "past" parsed from string, should be equal to itself
		parsedPast, _ := time.Parse(time.RFC3339, pastStr)
		if ltStrFilter.Eval(Document{"timestamp": parsedPast}) {
			t.Error("Lt should not match when timestamps are equal")
		}
	})
}
