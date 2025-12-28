// Package filter provides filter AST parsing and evaluation for Vex.
//
// Filters are expressed as JSON arrays in turbopuffer style:
//   - Boolean operators: ["And", [f1, f2, ...]], ["Or", [f1, f2, ...]], ["Not", f]
//   - Comparisons: ["attr", "Eq", value], ["attr", "Lt", value], etc.
//   - Array ops: ["attr", "Contains", value], ["attr", "ContainsAny", [...]], etc.
package filter

import (
	"encoding/json"
	"fmt"
)

// Operator represents a filter operator.
type Operator string

// Boolean operators
const (
	OpAnd Operator = "And"
	OpOr  Operator = "Or"
	OpNot Operator = "Not"
)

// Comparison operators
const (
	OpEq    Operator = "Eq"
	OpNotEq Operator = "NotEq"
	OpIn    Operator = "In"
	OpNotIn Operator = "NotIn"
	OpLt    Operator = "Lt"
	OpLte   Operator = "Lte"
	OpGt    Operator = "Gt"
	OpGte   Operator = "Gte"
)

// Array operators
const (
	OpContains       Operator = "Contains"
	OpNotContains    Operator = "NotContains"
	OpContainsAny    Operator = "ContainsAny"
	OpNotContainsAny Operator = "NotContainsAny"
	OpAnyLt          Operator = "AnyLt"
	OpAnyLte         Operator = "AnyLte"
	OpAnyGt          Operator = "AnyGt"
	OpAnyGte         Operator = "AnyGte"
)

// Glob operators
const (
	OpGlob     Operator = "Glob"
	OpNotGlob  Operator = "NotGlob"
	OpIGlob    Operator = "IGlob"
	OpNotIGlob Operator = "NotIGlob"
)

// Other operators
const (
	OpRegex                 Operator = "Regex"
	OpContainsTokenSequence Operator = "ContainsTokenSequence"
	OpContainsAllTokens     Operator = "ContainsAllTokens"
)

// IsBooleanOp returns true if the operator is a boolean operator (And, Or, Not).
func (o Operator) IsBooleanOp() bool {
	switch o {
	case OpAnd, OpOr, OpNot:
		return true
	}
	return false
}

// IsComparisonOp returns true if the operator is a comparison operator.
func (o Operator) IsComparisonOp() bool {
	switch o {
	case OpEq, OpNotEq, OpIn, OpNotIn, OpLt, OpLte, OpGt, OpGte:
		return true
	}
	return false
}

// IsArrayOp returns true if the operator is an array operator.
func (o Operator) IsArrayOp() bool {
	switch o {
	case OpContains, OpNotContains, OpContainsAny, OpNotContainsAny,
		OpAnyLt, OpAnyLte, OpAnyGt, OpAnyGte:
		return true
	}
	return false
}

// Filter represents a parsed filter expression.
type Filter struct {
	// For boolean operators (And, Or, Not)
	Op       Operator
	Children []*Filter // For And/Or
	Child    *Filter   // For Not

	// For comparison/array operators
	Attr  string
	Value any // The comparison value
}

// Parse parses a filter expression from a JSON-compatible value.
// The input can be:
//   - ["And", [f1, f2, ...]] - logical AND
//   - ["Or", [f1, f2, ...]] - logical OR
//   - ["Not", f] - logical NOT
//   - ["attr", "Op", value] - comparison
func Parse(v any) (*Filter, error) {
	if v == nil {
		return nil, nil
	}

	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("filter must be an array, got %T", v)
	}

	if len(arr) < 2 {
		return nil, fmt.Errorf("filter array must have at least 2 elements, got %d", len(arr))
	}

	// First element is either an operator or attribute name
	first, ok := arr[0].(string)
	if !ok {
		return nil, fmt.Errorf("first element must be a string, got %T", arr[0])
	}

	op := Operator(first)

	// Handle boolean operators
	switch op {
	case OpAnd, OpOr:
		return parseBooleanFilter(op, arr)
	case OpNot:
		return parseNotFilter(arr)
	default:
		// It's an attribute comparison
		return parseComparisonFilter(first, arr)
	}
}

func parseBooleanFilter(op Operator, arr []any) (*Filter, error) {
	if len(arr) != 2 {
		return nil, fmt.Errorf("%s filter must have exactly 2 elements [op, filters], got %d", op, len(arr))
	}

	childArr, ok := arr[1].([]any)
	if !ok {
		return nil, fmt.Errorf("%s filter second element must be an array of filters, got %T", op, arr[1])
	}

	children := make([]*Filter, 0, len(childArr))
	for i, child := range childArr {
		f, err := Parse(child)
		if err != nil {
			return nil, fmt.Errorf("%s filter child %d: %w", op, i, err)
		}
		if f != nil {
			children = append(children, f)
		}
	}

	return &Filter{
		Op:       op,
		Children: children,
	}, nil
}

func parseNotFilter(arr []any) (*Filter, error) {
	if len(arr) != 2 {
		return nil, fmt.Errorf("Not filter must have exactly 2 elements [\"Not\", filter], got %d", len(arr))
	}

	child, err := Parse(arr[1])
	if err != nil {
		return nil, fmt.Errorf("Not filter child: %w", err)
	}

	return &Filter{
		Op:    OpNot,
		Child: child,
	}, nil
}

func parseComparisonFilter(attr string, arr []any) (*Filter, error) {
	if len(arr) != 3 {
		return nil, fmt.Errorf("comparison filter must have exactly 3 elements [attr, op, value], got %d", len(arr))
	}

	opStr, ok := arr[1].(string)
	if !ok {
		return nil, fmt.Errorf("comparison operator must be a string, got %T", arr[1])
	}

	op := Operator(opStr)

	// Validate operator
	if !op.IsComparisonOp() && !op.IsArrayOp() && op != OpGlob && op != OpNotGlob && op != OpIGlob && op != OpNotIGlob && op != OpRegex && op != OpContainsTokenSequence && op != OpContainsAllTokens {
		return nil, fmt.Errorf("unknown operator: %s", op)
	}

	value := arr[2]

	return &Filter{
		Op:    op,
		Attr:  attr,
		Value: value,
	}, nil
}

// ParseJSON parses a filter from a JSON byte slice.
func ParseJSON(data []byte) (*Filter, error) {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return Parse(v)
}

// Document represents a document with attributes for filter evaluation.
type Document map[string]any

// Eval evaluates the filter against a document.
// Returns true if the document matches the filter.
func (f *Filter) Eval(doc Document) bool {
	if f == nil {
		return true // nil filter matches everything
	}

	switch f.Op {
	case OpAnd:
		return f.evalAnd(doc)
	case OpOr:
		return f.evalOr(doc)
	case OpNot:
		return f.evalNot(doc)
	case OpEq:
		return f.evalEq(doc)
	case OpNotEq:
		return f.evalNotEq(doc)
	case OpIn:
		return f.evalIn(doc)
	case OpNotIn:
		return f.evalNotIn(doc)
	case OpLt:
		return f.evalComparison(doc, -1, false)
	case OpLte:
		return f.evalComparison(doc, -1, true)
	case OpGt:
		return f.evalComparison(doc, 1, false)
	case OpGte:
		return f.evalComparison(doc, 1, true)
	case OpContains:
		return f.evalContains(doc)
	case OpNotContains:
		return !f.evalContains(doc)
	case OpContainsAny:
		return f.evalContainsAny(doc)
	case OpNotContainsAny:
		return !f.evalContainsAny(doc)
	case OpAnyLt:
		return f.evalAnyComparison(doc, -1, false)
	case OpAnyLte:
		return f.evalAnyComparison(doc, -1, true)
	case OpAnyGt:
		return f.evalAnyComparison(doc, 1, false)
	case OpAnyGte:
		return f.evalAnyComparison(doc, 1, true)
	case OpGlob:
		return f.evalGlob(doc, false)
	case OpNotGlob:
		return !f.evalGlob(doc, false)
	case OpIGlob:
		return f.evalGlob(doc, true)
	case OpNotIGlob:
		return !f.evalGlob(doc, true)
	case OpRegex:
		return f.evalRegex(doc)
	case OpContainsTokenSequence:
		return f.evalContainsTokenSequence(doc)
	case OpContainsAllTokens:
		return f.evalContainsAllTokens(doc)
	default:
		return false
	}
}

func (f *Filter) evalAnd(doc Document) bool {
	for _, child := range f.Children {
		if !child.Eval(doc) {
			return false
		}
	}
	return true
}

func (f *Filter) evalOr(doc Document) bool {
	if len(f.Children) == 0 {
		return true // Empty OR is true (vacuously)
	}
	for _, child := range f.Children {
		if child.Eval(doc) {
			return true
		}
	}
	return false
}

func (f *Filter) evalNot(doc Document) bool {
	if f.Child == nil {
		return true
	}
	return !f.Child.Eval(doc)
}

func (f *Filter) evalEq(doc Document) bool {
	docVal, exists := doc[f.Attr]

	// Eq null matches missing attribute
	if f.Value == nil {
		return !exists || docVal == nil
	}

	// If attribute doesn't exist, can't match
	if !exists {
		return false
	}

	return valuesEqual(docVal, f.Value)
}

func (f *Filter) evalNotEq(doc Document) bool {
	docVal, exists := doc[f.Attr]

	// NotEq null matches "attribute present"
	if f.Value == nil {
		return exists && docVal != nil
	}

	// If attribute doesn't exist, it's not equal
	if !exists {
		return true
	}

	return !valuesEqual(docVal, f.Value)
}

func (f *Filter) evalIn(doc Document) bool {
	docVal, exists := doc[f.Attr]
	if !exists {
		return false
	}

	values, ok := f.Value.([]any)
	if !ok {
		return false
	}

	for _, v := range values {
		if valuesEqual(docVal, v) {
			return true
		}
	}
	return false
}

func (f *Filter) evalNotIn(doc Document) bool {
	return !f.evalIn(doc)
}

// evalComparison evaluates Lt, Lte, Gt, Gte.
// direction: -1 for Lt/Lte, 1 for Gt/Gte
// orEqual: true for Lte/Gte
func (f *Filter) evalComparison(doc Document, direction int, orEqual bool) bool {
	docVal, exists := doc[f.Attr]
	if !exists || docVal == nil {
		return false
	}

	cmp, ok := compareValues(docVal, f.Value)
	if !ok {
		return false
	}
	if cmp == 0 {
		return orEqual
	}
	return cmp == direction
}

func (f *Filter) evalContains(doc Document) bool {
	docVal, exists := doc[f.Attr]
	if !exists {
		return false
	}

	arr := toSlice(docVal)
	if arr == nil {
		return false
	}

	for _, elem := range arr {
		if valuesEqual(elem, f.Value) {
			return true
		}
	}
	return false
}

func (f *Filter) evalContainsAny(doc Document) bool {
	docVal, exists := doc[f.Attr]
	if !exists {
		return false
	}

	arr := toSlice(docVal)
	if arr == nil {
		return false
	}

	values, ok := f.Value.([]any)
	if !ok {
		return false
	}

	for _, elem := range arr {
		for _, v := range values {
			if valuesEqual(elem, v) {
				return true
			}
		}
	}
	return false
}

// evalAnyComparison evaluates AnyLt, AnyLte, AnyGt, AnyGte on array fields.
func (f *Filter) evalAnyComparison(doc Document, direction int, orEqual bool) bool {
	docVal, exists := doc[f.Attr]
	if !exists {
		return false
	}

	arr := toSlice(docVal)
	if arr == nil {
		return false
	}

	for _, elem := range arr {
		cmp, ok := compareValues(elem, f.Value)
		if !ok {
			continue
		}
		if cmp == 0 && orEqual {
			return true
		}
		if cmp == direction {
			return true
		}
	}
	return false
}

func (f *Filter) evalGlob(doc Document, caseInsensitive bool) bool {
	docVal, exists := doc[f.Attr]
	if !exists || docVal == nil {
		return false
	}

	// Get the string value to match against
	docStr, ok := docVal.(string)
	if !ok {
		return false
	}

	// Get the pattern from the filter value
	pattern, ok := f.Value.(string)
	if !ok {
		return false
	}

	// Create a glob matcher and match
	matcher := NewGlobMatcher(pattern, caseInsensitive)
	return matcher.Match(docStr)
}

func (f *Filter) evalRegex(_ Document) bool {
	// Regex matching will be implemented in a separate task
	return false
}

func (f *Filter) evalContainsTokenSequence(_ Document) bool {
	// Token sequence matching will be implemented in a separate task
	return false
}

func (f *Filter) evalContainsAllTokens(_ Document) bool {
	// Token matching will be implemented in a separate task
	return false
}
