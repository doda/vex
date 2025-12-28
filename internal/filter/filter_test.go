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

// TestArrayOps_TaskVerification tests exactly the verification steps for the filter-array-ops task.
func TestArrayOps_TaskVerification(t *testing.T) {
	t.Run("Contains_operator", func(t *testing.T) {
		// Test Contains operator - checks if array attribute contains a value

		// Contains with string array
		f, err := Parse([]any{"tags", "Contains", "foo"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when array contains the value
		if !f.Eval(Document{"tags": []any{"foo", "bar", "baz"}}) {
			t.Error("Contains should match when array contains the value")
		}

		// Should match when value is at different positions
		if !f.Eval(Document{"tags": []any{"foo"}}) {
			t.Error("Contains should match with single element array")
		}
		if !f.Eval(Document{"tags": []any{"bar", "foo", "baz"}}) {
			t.Error("Contains should match when value is in middle")
		}
		if !f.Eval(Document{"tags": []any{"bar", "baz", "foo"}}) {
			t.Error("Contains should match when value is at end")
		}

		// Should not match when value is absent
		if f.Eval(Document{"tags": []any{"bar", "baz"}}) {
			t.Error("Contains should not match when value is absent")
		}
		if f.Eval(Document{"tags": []any{}}) {
			t.Error("Contains should not match on empty array")
		}

		// Should not match for missing attribute
		if f.Eval(Document{"other": []any{"foo"}}) {
			t.Error("Contains should not match when attribute is missing")
		}

		// Should not match for non-array attribute
		if f.Eval(Document{"tags": "foo"}) {
			t.Error("Contains should not match when attribute is not an array")
		}

		// Test Contains with numeric values
		fNum, err := Parse([]any{"nums", "Contains", float64(42)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fNum.Eval(Document{"nums": []any{float64(1), float64(42), float64(100)}}) {
			t.Error("Contains should match numeric value in array")
		}
		if fNum.Eval(Document{"nums": []any{float64(1), float64(2), float64(3)}}) {
			t.Error("Contains should not match when numeric value is absent")
		}

		// Test with typed slices ([]float64, []string, []int, etc.)
		if !fNum.Eval(Document{"nums": []float64{1, 42, 100}}) {
			t.Error("Contains should work with []float64 typed arrays")
		}
		if !fNum.Eval(Document{"nums": []int{1, 42, 100}}) {
			t.Error("Contains should work with []int typed arrays")
		}

		// Test Contains with string typed slice
		if !f.Eval(Document{"tags": []string{"foo", "bar", "baz"}}) {
			t.Error("Contains should work with []string typed arrays")
		}
	})

	t.Run("NotContains_operator", func(t *testing.T) {
		// Test NotContains operator - negation of Contains

		f, err := Parse([]any{"tags", "NotContains", "foo"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when value is absent from array
		if !f.Eval(Document{"tags": []any{"bar", "baz", "qux"}}) {
			t.Error("NotContains should match when value is absent")
		}
		if !f.Eval(Document{"tags": []any{}}) {
			t.Error("NotContains should match for empty array")
		}

		// Should not match when value is present
		if f.Eval(Document{"tags": []any{"foo", "bar"}}) {
			t.Error("NotContains should not match when value is present")
		}
		if f.Eval(Document{"tags": []any{"foo"}}) {
			t.Error("NotContains should not match with single matching element")
		}

		// Missing attribute behavior - NotContains should match
		// (since Contains returns false for missing, NotContains returns !false = true)
		if !f.Eval(Document{"other": []any{"foo"}}) {
			t.Error("NotContains should match when attribute is missing (not contained)")
		}

		// Non-array attribute - should match (Contains returns false, NotContains returns true)
		if !f.Eval(Document{"tags": "foo"}) {
			t.Error("NotContains should match when attribute is not an array")
		}

		// Test with numeric values
		fNum, err := Parse([]any{"nums", "NotContains", float64(99)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fNum.Eval(Document{"nums": []any{float64(1), float64(2), float64(3)}}) {
			t.Error("NotContains should match when numeric value is absent")
		}
		if fNum.Eval(Document{"nums": []any{float64(99), float64(100)}}) {
			t.Error("NotContains should not match when numeric value is present")
		}
	})

	t.Run("ContainsAny_operator", func(t *testing.T) {
		// Test ContainsAny operator - checks if array contains any value from a set

		f, err := Parse([]any{"tags", "ContainsAny", []any{"foo", "qux", "xyz"}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when array contains first value from set
		if !f.Eval(Document{"tags": []any{"foo", "bar", "baz"}}) {
			t.Error("ContainsAny should match when array contains first set value")
		}

		// Should match when array contains middle value from set
		if !f.Eval(Document{"tags": []any{"bar", "qux", "baz"}}) {
			t.Error("ContainsAny should match when array contains middle set value")
		}

		// Should match when array contains last value from set
		if !f.Eval(Document{"tags": []any{"bar", "baz", "xyz"}}) {
			t.Error("ContainsAny should match when array contains last set value")
		}

		// Should match when array contains multiple values from set
		if !f.Eval(Document{"tags": []any{"foo", "qux", "xyz"}}) {
			t.Error("ContainsAny should match when array contains multiple set values")
		}

		// Should not match when array contains no values from set
		if f.Eval(Document{"tags": []any{"bar", "baz", "other"}}) {
			t.Error("ContainsAny should not match when no set values are present")
		}
		if f.Eval(Document{"tags": []any{}}) {
			t.Error("ContainsAny should not match for empty array")
		}

		// Missing attribute should not match
		if f.Eval(Document{"other": []any{"foo"}}) {
			t.Error("ContainsAny should not match when attribute is missing")
		}

		// Test with numeric values
		fNum, err := Parse([]any{"nums", "ContainsAny", []any{float64(5), float64(10), float64(15)}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fNum.Eval(Document{"nums": []any{float64(1), float64(5), float64(9)}}) {
			t.Error("ContainsAny should match numeric value in array")
		}
		if fNum.Eval(Document{"nums": []any{float64(1), float64(2), float64(3)}}) {
			t.Error("ContainsAny should not match when no numeric values match")
		}

		// Test with typed slices
		if !fNum.Eval(Document{"nums": []float64{1, 10, 20}}) {
			t.Error("ContainsAny should work with []float64 typed arrays")
		}
	})

	t.Run("NotContainsAny_operator", func(t *testing.T) {
		// Test NotContainsAny operator - negation of ContainsAny

		f, err := Parse([]any{"tags", "NotContainsAny", []any{"deleted", "spam", "archived"}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when array contains none of the set values
		if !f.Eval(Document{"tags": []any{"active", "published", "featured"}}) {
			t.Error("NotContainsAny should match when no set values are present")
		}
		if !f.Eval(Document{"tags": []any{}}) {
			t.Error("NotContainsAny should match for empty array")
		}

		// Should not match when array contains any value from set
		if f.Eval(Document{"tags": []any{"active", "deleted", "other"}}) {
			t.Error("NotContainsAny should not match when first set value is present")
		}
		if f.Eval(Document{"tags": []any{"spam"}}) {
			t.Error("NotContainsAny should not match when middle set value is present")
		}
		if f.Eval(Document{"tags": []any{"active", "archived"}}) {
			t.Error("NotContainsAny should not match when last set value is present")
		}

		// Should not match when multiple set values are present
		if f.Eval(Document{"tags": []any{"deleted", "spam", "archived"}}) {
			t.Error("NotContainsAny should not match when all set values are present")
		}

		// Missing attribute - should match (ContainsAny returns false, so NotContainsAny returns true)
		if !f.Eval(Document{"other": []any{"deleted"}}) {
			t.Error("NotContainsAny should match when attribute is missing")
		}

		// Test with numeric values
		fNum, err := Parse([]any{"scores", "NotContainsAny", []any{float64(0), float64(-1)}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fNum.Eval(Document{"scores": []any{float64(100), float64(95), float64(80)}}) {
			t.Error("NotContainsAny should match when no numeric values match")
		}
		if fNum.Eval(Document{"scores": []any{float64(100), float64(0), float64(80)}}) {
			t.Error("NotContainsAny should not match when numeric value is present")
		}
	})

	t.Run("AnyLt_operator", func(t *testing.T) {
		// Test AnyLt operator - checks if any array element is less than value

		f, err := Parse([]any{"scores", "AnyLt", float64(50)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when any element is less than value
		if !f.Eval(Document{"scores": []any{float64(10), float64(60), float64(70)}}) {
			t.Error("AnyLt should match when first element is less")
		}
		if !f.Eval(Document{"scores": []any{float64(60), float64(30), float64(70)}}) {
			t.Error("AnyLt should match when middle element is less")
		}
		if !f.Eval(Document{"scores": []any{float64(60), float64(70), float64(40)}}) {
			t.Error("AnyLt should match when last element is less")
		}

		// Should match when multiple elements are less
		if !f.Eval(Document{"scores": []any{float64(10), float64(20), float64(30)}}) {
			t.Error("AnyLt should match when all elements are less")
		}

		// Should not match when no element is less
		if f.Eval(Document{"scores": []any{float64(50), float64(60), float64(70)}}) {
			t.Error("AnyLt should not match when no element is less (equal doesn't count)")
		}
		if f.Eval(Document{"scores": []any{float64(100), float64(200), float64(300)}}) {
			t.Error("AnyLt should not match when all elements are greater")
		}
		if f.Eval(Document{"scores": []any{}}) {
			t.Error("AnyLt should not match for empty array")
		}

		// Missing attribute should not match
		if f.Eval(Document{"other": []any{float64(10)}}) {
			t.Error("AnyLt should not match when attribute is missing")
		}

		// Test with typed arrays
		if !f.Eval(Document{"scores": []float64{10, 60, 70}}) {
			t.Error("AnyLt should work with []float64 typed arrays")
		}
		if !f.Eval(Document{"scores": []int{10, 60, 70}}) {
			t.Error("AnyLt should work with []int typed arrays")
		}

		// Test with string comparison (lexicographic)
		fStr, err := Parse([]any{"names", "AnyLt", "banana"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fStr.Eval(Document{"names": []any{"cherry", "apple", "date"}}) {
			t.Error("AnyLt should match lexicographically for strings")
		}
		if fStr.Eval(Document{"names": []any{"cherry", "banana", "date"}}) {
			t.Error("AnyLt should not match when no string is less (equal doesn't count)")
		}
	})

	t.Run("AnyLte_operator", func(t *testing.T) {
		// Test AnyLte operator - checks if any array element is less than or equal to value

		f, err := Parse([]any{"scores", "AnyLte", float64(50)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when any element is less than value
		if !f.Eval(Document{"scores": []any{float64(10), float64(60), float64(70)}}) {
			t.Error("AnyLte should match when element is less")
		}

		// Should match when any element is equal to value
		if !f.Eval(Document{"scores": []any{float64(60), float64(50), float64(70)}}) {
			t.Error("AnyLte should match when element is equal")
		}

		// Should match with multiple qualifying elements
		if !f.Eval(Document{"scores": []any{float64(40), float64(50), float64(30)}}) {
			t.Error("AnyLte should match when multiple elements qualify")
		}

		// Should not match when all elements are greater
		if f.Eval(Document{"scores": []any{float64(51), float64(60), float64(70)}}) {
			t.Error("AnyLte should not match when all elements are greater")
		}
		if f.Eval(Document{"scores": []any{}}) {
			t.Error("AnyLte should not match for empty array")
		}

		// Missing attribute should not match
		if f.Eval(Document{"other": []any{float64(10)}}) {
			t.Error("AnyLte should not match when attribute is missing")
		}

		// Test with typed arrays
		if !f.Eval(Document{"scores": []float64{50, 60, 70}}) {
			t.Error("AnyLte should work with []float64 typed arrays (equal case)")
		}

		// Test with string comparison
		fStr, err := Parse([]any{"names", "AnyLte", "banana"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fStr.Eval(Document{"names": []any{"cherry", "banana", "date"}}) {
			t.Error("AnyLte should match when string is equal")
		}
		if !fStr.Eval(Document{"names": []any{"cherry", "apple", "date"}}) {
			t.Error("AnyLte should match when string is less")
		}
	})

	t.Run("AnyGt_operator", func(t *testing.T) {
		// Test AnyGt operator - checks if any array element is greater than value

		f, err := Parse([]any{"scores", "AnyGt", float64(50)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when any element is greater than value
		if !f.Eval(Document{"scores": []any{float64(100), float64(30), float64(40)}}) {
			t.Error("AnyGt should match when first element is greater")
		}
		if !f.Eval(Document{"scores": []any{float64(30), float64(100), float64(40)}}) {
			t.Error("AnyGt should match when middle element is greater")
		}
		if !f.Eval(Document{"scores": []any{float64(30), float64(40), float64(100)}}) {
			t.Error("AnyGt should match when last element is greater")
		}

		// Should match when multiple elements are greater
		if !f.Eval(Document{"scores": []any{float64(60), float64(70), float64(80)}}) {
			t.Error("AnyGt should match when all elements are greater")
		}

		// Should not match when no element is greater
		if f.Eval(Document{"scores": []any{float64(50), float64(40), float64(30)}}) {
			t.Error("AnyGt should not match when no element is greater (equal doesn't count)")
		}
		if f.Eval(Document{"scores": []any{float64(10), float64(20), float64(30)}}) {
			t.Error("AnyGt should not match when all elements are less")
		}
		if f.Eval(Document{"scores": []any{}}) {
			t.Error("AnyGt should not match for empty array")
		}

		// Missing attribute should not match
		if f.Eval(Document{"other": []any{float64(100)}}) {
			t.Error("AnyGt should not match when attribute is missing")
		}

		// Test with typed arrays
		if !f.Eval(Document{"scores": []float64{100, 30, 40}}) {
			t.Error("AnyGt should work with []float64 typed arrays")
		}
		if !f.Eval(Document{"scores": []int64{100, 30, 40}}) {
			t.Error("AnyGt should work with []int64 typed arrays")
		}

		// Test with string comparison (lexicographic)
		fStr, err := Parse([]any{"names", "AnyGt", "banana"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fStr.Eval(Document{"names": []any{"apple", "cherry", "aardvark"}}) {
			t.Error("AnyGt should match lexicographically for strings")
		}
		if fStr.Eval(Document{"names": []any{"apple", "banana", "aardvark"}}) {
			t.Error("AnyGt should not match when no string is greater (equal doesn't count)")
		}
	})

	t.Run("AnyGte_operator", func(t *testing.T) {
		// Test AnyGte operator - checks if any array element is greater than or equal to value

		f, err := Parse([]any{"scores", "AnyGte", float64(50)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}

		// Should match when any element is greater than value
		if !f.Eval(Document{"scores": []any{float64(100), float64(30), float64(40)}}) {
			t.Error("AnyGte should match when element is greater")
		}

		// Should match when any element is equal to value
		if !f.Eval(Document{"scores": []any{float64(30), float64(50), float64(40)}}) {
			t.Error("AnyGte should match when element is equal")
		}

		// Should match with multiple qualifying elements
		if !f.Eval(Document{"scores": []any{float64(50), float64(60), float64(70)}}) {
			t.Error("AnyGte should match when multiple elements qualify")
		}

		// Should not match when all elements are less
		if f.Eval(Document{"scores": []any{float64(49), float64(40), float64(30)}}) {
			t.Error("AnyGte should not match when all elements are less")
		}
		if f.Eval(Document{"scores": []any{}}) {
			t.Error("AnyGte should not match for empty array")
		}

		// Missing attribute should not match
		if f.Eval(Document{"other": []any{float64(100)}}) {
			t.Error("AnyGte should not match when attribute is missing")
		}

		// Test with typed arrays
		if !f.Eval(Document{"scores": []float64{30, 50, 40}}) {
			t.Error("AnyGte should work with []float64 typed arrays (equal case)")
		}

		// Test with string comparison
		fStr, err := Parse([]any{"names", "AnyGte", "banana"})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fStr.Eval(Document{"names": []any{"apple", "banana", "aardvark"}}) {
			t.Error("AnyGte should match when string is equal")
		}
		if !fStr.Eval(Document{"names": []any{"apple", "cherry", "aardvark"}}) {
			t.Error("AnyGte should match when string is greater")
		}
		if fStr.Eval(Document{"names": []any{"apple", "aardvark", "avocado"}}) {
			t.Error("AnyGte should not match when all strings are less")
		}
	})

	t.Run("mixed_type_arrays", func(t *testing.T) {
		// Test operators with arrays containing mixed types

		// Contains with mixed numeric types
		fContains, err := Parse([]any{"values", "Contains", float64(42)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		// JSON numbers are always float64, but Go-native arrays can have int
		if !fContains.Eval(Document{"values": []any{int(42), "hello", true}}) {
			t.Error("Contains should match int value with float64 filter due to numeric coercion")
		}

		// ContainsAny with mixed types
		fContainsAny, err := Parse([]any{"values", "ContainsAny", []any{float64(100), "hello"}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fContainsAny.Eval(Document{"values": []any{int(42), "hello", true}}) {
			t.Error("ContainsAny should match string value in mixed array")
		}

		// AnyLt with mixed comparable types (only numerics will compare)
		fAnyLt, err := Parse([]any{"values", "AnyLt", float64(50)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		// String and bool elements are skipped, only numeric is compared
		if !fAnyLt.Eval(Document{"values": []any{int(10), "hello", true}}) {
			t.Error("AnyLt should match numeric element, ignoring incompatible types")
		}
	})

	t.Run("nil_and_null_handling", func(t *testing.T) {
		// Test operators with nil/null values in arrays

		// Contains with nil value
		fContainsNil, err := Parse([]any{"values", "Contains", nil})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fContainsNil.Eval(Document{"values": []any{nil, "hello", float64(42)}}) {
			t.Error("Contains should match nil value in array")
		}
		if fContainsNil.Eval(Document{"values": []any{"hello", float64(42)}}) {
			t.Error("Contains should not match when nil is not in array")
		}

		// ContainsAny with nil in set
		fContainsAnyNil, err := Parse([]any{"values", "ContainsAny", []any{nil, "special"}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fContainsAnyNil.Eval(Document{"values": []any{nil, "hello"}}) {
			t.Error("ContainsAny should match nil value")
		}
		if !fContainsAnyNil.Eval(Document{"values": []any{"special", "hello"}}) {
			t.Error("ContainsAny should match 'special' value")
		}

		// AnyLt with nil elements should skip them
		fAnyLt, err := Parse([]any{"values", "AnyLt", float64(50)})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		// nil elements should be skipped in comparison
		if !fAnyLt.Eval(Document{"values": []any{nil, float64(10), nil}}) {
			t.Error("AnyLt should match, skipping nil elements")
		}
	})

	t.Run("boolean_arrays", func(t *testing.T) {
		// Test operators with boolean arrays

		fContains, err := Parse([]any{"flags", "Contains", true})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fContains.Eval(Document{"flags": []any{false, true, false}}) {
			t.Error("Contains should match boolean value in array")
		}
		if !fContains.Eval(Document{"flags": []bool{false, true, false}}) {
			t.Error("Contains should match boolean value in typed []bool array")
		}
		if fContains.Eval(Document{"flags": []any{false, false}}) {
			t.Error("Contains should not match when boolean value is absent")
		}

		// ContainsAny with booleans
		fContainsAny, err := Parse([]any{"flags", "ContainsAny", []any{true}})
		if err != nil {
			t.Fatalf("Parse failed: %v", err)
		}
		if !fContainsAny.Eval(Document{"flags": []any{false, true}}) {
			t.Error("ContainsAny should match boolean value")
		}
	})
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
