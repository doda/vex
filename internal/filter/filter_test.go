package filter

import (
	"testing"
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
