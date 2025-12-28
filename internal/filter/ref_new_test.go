package filter

import (
	"reflect"
	"testing"
)

func TestResolveRefNew_SimpleAttribute(t *testing.T) {
	newDoc := map[string]any{
		"version": 10,
		"name":    "test",
	}

	// Test resolving $ref_new.version
	result := ResolveRefNew("$ref_new.version", newDoc)
	if result != 10 {
		t.Errorf("expected 10, got %v", result)
	}

	// Test resolving $ref_new.name
	result = ResolveRefNew("$ref_new.name", newDoc)
	if result != "test" {
		t.Errorf("expected 'test', got %v", result)
	}
}

func TestResolveRefNew_MissingAttribute(t *testing.T) {
	newDoc := map[string]any{
		"version": 10,
	}

	// Missing attribute should resolve to nil
	result := ResolveRefNew("$ref_new.missing", newDoc)
	if result != nil {
		t.Errorf("expected nil for missing attribute, got %v", result)
	}
}

func TestResolveRefNew_NonRefString(t *testing.T) {
	newDoc := map[string]any{
		"version": 10,
	}

	// Regular string should pass through unchanged
	result := ResolveRefNew("regular_string", newDoc)
	if result != "regular_string" {
		t.Errorf("expected 'regular_string', got %v", result)
	}
}

func TestResolveRefNew_ComparisonFilter(t *testing.T) {
	newDoc := map[string]any{
		"version": 10,
	}

	// Test: ["version", "Lt", "$ref_new.version"]
	// Should resolve to: ["version", "Lt", 10]
	expr := []any{"version", "Lt", "$ref_new.version"}
	result := ResolveRefNew(expr, newDoc)

	expected := []any{"version", "Lt", 10}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestResolveRefNew_BooleanAnd(t *testing.T) {
	newDoc := map[string]any{
		"version": 10,
		"status":  "active",
	}

	// Test: ["And", [["version", "Lt", "$ref_new.version"], ["status", "Eq", "active"]]]
	expr := []any{"And", []any{
		[]any{"version", "Lt", "$ref_new.version"},
		[]any{"status", "Eq", "$ref_new.status"},
	}}

	result := ResolveRefNew(expr, newDoc)

	expected := []any{"And", []any{
		[]any{"version", "Lt", 10},
		[]any{"status", "Eq", "active"},
	}}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestResolveRefNew_BooleanNot(t *testing.T) {
	newDoc := map[string]any{
		"version": 5,
	}

	// Test: ["Not", ["version", "Eq", "$ref_new.version"]]
	expr := []any{"Not", []any{"version", "Eq", "$ref_new.version"}}

	result := ResolveRefNew(expr, newDoc)

	expected := []any{"Not", []any{"version", "Eq", 5}}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestResolveRefNew_Primitives(t *testing.T) {
	newDoc := map[string]any{}

	// Numbers should pass through unchanged
	if result := ResolveRefNew(42, newDoc); result != 42 {
		t.Errorf("expected 42, got %v", result)
	}

	// Booleans should pass through unchanged
	if result := ResolveRefNew(true, newDoc); result != true {
		t.Errorf("expected true, got %v", result)
	}

	// Nil should pass through as nil
	if result := ResolveRefNew(nil, newDoc); result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestResolveRefNew_MapWithRefs(t *testing.T) {
	newDoc := map[string]any{
		"version": 10,
	}

	// Map with $ref_new values
	expr := map[string]any{
		"key1": "$ref_new.version",
		"key2": "normal_value",
	}

	result := ResolveRefNew(expr, newDoc)

	expected := map[string]any{
		"key1": 10,
		"key2": "normal_value",
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestHasRefNew_True(t *testing.T) {
	tests := []struct {
		name string
		expr any
	}{
		{"simple string", "$ref_new.version"},
		{"in array", []any{"version", "Lt", "$ref_new.version"}},
		{"nested", []any{"And", []any{[]any{"version", "Lt", "$ref_new.version"}}}},
		{"in map", map[string]any{"key": "$ref_new.attr"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !HasRefNew(tt.expr) {
				t.Errorf("expected HasRefNew to return true for %v", tt.expr)
			}
		})
	}
}

func TestHasRefNew_False(t *testing.T) {
	tests := []struct {
		name string
		expr any
	}{
		{"nil", nil},
		{"number", 42},
		{"regular string", "version"},
		{"regular array", []any{"version", "Lt", 10}},
		{"regular map", map[string]any{"key": "value"}},
		{"$ref without _new", "$ref.version"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if HasRefNew(tt.expr) {
				t.Errorf("expected HasRefNew to return false for %v", tt.expr)
			}
		})
	}
}
