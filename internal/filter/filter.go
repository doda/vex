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
	"regexp"
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

// Regex operators
const (
	OpRegex    Operator = "Regex"
	OpNotRegex Operator = "NotRegex"
)

// Token operators
const (
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
	if !op.IsComparisonOp() && !op.IsArrayOp() && op != OpGlob && op != OpNotGlob && op != OpIGlob && op != OpNotIGlob && op != OpRegex && op != OpNotRegex && op != OpContainsTokenSequence && op != OpContainsAllTokens {
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
	case OpNotRegex:
		return !f.evalRegex(doc)
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

func (f *Filter) evalRegex(doc Document) bool {
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

	// Compile and match the regex
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}

	return re.MatchString(docStr)
}

func (f *Filter) evalContainsTokenSequence(doc Document) bool {
	docVal, exists := doc[f.Attr]
	if !exists || docVal == nil {
		return false
	}

	docStr, ok := docVal.(string)
	if !ok {
		return false
	}

	queryTokens, lastAsPrefix := parseTokenFilterValue(f.Value)
	if len(queryTokens) == 0 {
		return true // Empty query matches everything
	}

	docTokens := tokenize(docStr)
	return containsTokenSequence(docTokens, queryTokens, lastAsPrefix)
}

func (f *Filter) evalContainsAllTokens(doc Document) bool {
	docVal, exists := doc[f.Attr]
	if !exists || docVal == nil {
		return false
	}

	docStr, ok := docVal.(string)
	if !ok {
		return false
	}

	queryTokens, lastAsPrefix := parseTokenFilterValue(f.Value)
	if len(queryTokens) == 0 {
		return true // Empty query matches everything
	}

	docTokens := tokenize(docStr)
	return containsAllTokens(docTokens, queryTokens, lastAsPrefix)
}

// parseTokenFilterValue extracts query tokens and last_as_prefix option from filter value.
// Supports two formats:
// 1. Simple string: "hello world" -> tokens=["hello", "world"], lastAsPrefix=false
// 2. Object: {"query": "hello wo", "last_as_prefix": true} -> tokens=["hello", "wo"], lastAsPrefix=true
func parseTokenFilterValue(value any) (tokens []string, lastAsPrefix bool) {
	switch v := value.(type) {
	case string:
		return tokenize(v), false
	case map[string]any:
		if q, ok := v["query"].(string); ok {
			tokens = tokenize(q)
		}
		if lap, ok := v["last_as_prefix"].(bool); ok {
			lastAsPrefix = lap
		}
		return tokens, lastAsPrefix
	default:
		return nil, false
	}
}

// tokenize splits a string into lowercase tokens by whitespace and punctuation.
func tokenize(s string) []string {
	var tokens []string
	var current []rune
	for _, r := range s {
		if isTokenChar(r) {
			current = append(current, toLower(r))
		} else if len(current) > 0 {
			tokens = append(tokens, string(current))
			current = current[:0]
		}
	}
	if len(current) > 0 {
		tokens = append(tokens, string(current))
	}
	return tokens
}

// isTokenChar returns true if the rune is part of a token (alphanumeric).
func isTokenChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// toLower returns the lowercase version of an ASCII letter.
func toLower(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r + ('a' - 'A')
	}
	return r
}

// containsTokenSequence checks if docTokens contains queryTokens in adjacent order.
// If lastAsPrefix is true, the last query token can be a prefix of a doc token.
func containsTokenSequence(docTokens, queryTokens []string, lastAsPrefix bool) bool {
	if len(queryTokens) == 0 {
		return true
	}
	if len(docTokens) < len(queryTokens) {
		return false
	}

	// Slide a window of len(queryTokens) across docTokens
	for i := 0; i <= len(docTokens)-len(queryTokens); i++ {
		match := true
		for j := 0; j < len(queryTokens); j++ {
			docToken := docTokens[i+j]
			queryToken := queryTokens[j]
			if j == len(queryTokens)-1 && lastAsPrefix {
				// Last token: prefix match
				if !hasPrefix(docToken, queryToken) {
					match = false
					break
				}
			} else {
				// Exact match
				if docToken != queryToken {
					match = false
					break
				}
			}
		}
		if match {
			return true
		}
	}
	return false
}

// containsAllTokens checks if all queryTokens appear somewhere in docTokens (any order).
// If lastAsPrefix is true, the last query token can be a prefix of a doc token.
func containsAllTokens(docTokens, queryTokens []string, lastAsPrefix bool) bool {
	if len(queryTokens) == 0 {
		return true
	}

	// Build a set of doc tokens for O(1) lookup
	docSet := make(map[string]bool, len(docTokens))
	for _, t := range docTokens {
		docSet[t] = true
	}

	for i, queryToken := range queryTokens {
		if i == len(queryTokens)-1 && lastAsPrefix {
			// Last token: need to check if any doc token has this prefix
			found := false
			for _, docToken := range docTokens {
				if hasPrefix(docToken, queryToken) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		} else {
			// Exact match required
			if !docSet[queryToken] {
				return false
			}
		}
	}
	return true
}

// hasPrefix returns true if s starts with prefix.
func hasPrefix(s, prefix string) bool {
	if len(prefix) > len(s) {
		return false
	}
	return s[:len(prefix)] == prefix
}

// IsRegexOp returns true if the operator requires regex: true in schema.
func (o Operator) IsRegexOp() bool {
	return o == OpRegex || o == OpNotRegex
}

// ErrRegexNotEnabled is returned when a Regex filter is used on an attribute
// that does not have regex: true in the schema.
var ErrRegexNotEnabled = fmt.Errorf("Regex operator requires regex: true in schema for the attribute")

// SchemaChecker provides schema information for filter validation.
type SchemaChecker interface {
	// HasRegex returns true if the attribute has regex: true in its schema.
	HasRegex(attrName string) bool
}

// ValidateWithSchema validates that filter operators are compatible with the schema.
// Returns an error if Regex/NotRegex operators are used on attributes without regex: true.
func (f *Filter) ValidateWithSchema(schema SchemaChecker) error {
	if f == nil {
		return nil
	}

	switch f.Op {
	case OpAnd:
		for _, child := range f.Children {
			if err := child.ValidateWithSchema(schema); err != nil {
				return err
			}
		}
	case OpOr:
		for _, child := range f.Children {
			if err := child.ValidateWithSchema(schema); err != nil {
				return err
			}
		}
	case OpNot:
		if f.Child != nil {
			return f.Child.ValidateWithSchema(schema)
		}
	case OpRegex, OpNotRegex:
		if schema == nil || !schema.HasRegex(f.Attr) {
			return fmt.Errorf("%w: attribute %q", ErrRegexNotEnabled, f.Attr)
		}
	}
	return nil
}

// UsesRegexOperators returns true if this filter or any nested filter uses Regex/NotRegex operators.
func (f *Filter) UsesRegexOperators() bool {
	if f == nil {
		return false
	}

	switch f.Op {
	case OpRegex, OpNotRegex:
		return true
	case OpAnd:
		for _, child := range f.Children {
			if child.UsesRegexOperators() {
				return true
			}
		}
	case OpOr:
		for _, child := range f.Children {
			if child.UsesRegexOperators() {
				return true
			}
		}
	case OpNot:
		if f.Child != nil {
			return f.Child.UsesRegexOperators()
		}
	}
	return false
}

// UsedAttributes returns a list of all attribute names used in the filter.
// This is used to check for pending index rebuilds on filtered attributes.
func (f *Filter) UsedAttributes() []string {
	if f == nil {
		return nil
	}

	seen := make(map[string]bool)
	f.collectAttributes(seen)

	attrs := make([]string, 0, len(seen))
	for attr := range seen {
		attrs = append(attrs, attr)
	}
	return attrs
}

// collectAttributes recursively collects all attribute names used in the filter.
func (f *Filter) collectAttributes(seen map[string]bool) {
	if f == nil {
		return
	}

	switch f.Op {
	case OpAnd, OpOr:
		for _, child := range f.Children {
			child.collectAttributes(seen)
		}
	case OpNot:
		if f.Child != nil {
			f.Child.collectAttributes(seen)
		}
	default:
		// It's an attribute comparison - add the attribute name
		if f.Attr != "" {
			seen[f.Attr] = true
		}
	}
}
