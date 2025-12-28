package filter

import (
	"strings"
)

const refNewPrefix = "$ref_new."

// ResolveRefNew resolves $ref_new references in a filter expression.
// It takes the raw filter expression and a map of new document attributes,
// and returns a new filter expression with all $ref_new.attr references
// replaced with the actual values from newDoc.
//
// For example:
//   - ["$ref_new.version", "Gt", ["version"]] with newDoc{"version": 2}
//     becomes [2, "Gt", ["version"]]
//   - ["And", [["$ref_new.status", "Eq", "active"], ["version", "Lt", 10]]]
//     with newDoc{"status": "active"} becomes ["And", [["active", "Eq", "active"], ...]]
//
// Note: $ref_new references can appear as attribute names in comparison filters
// or as values. When they appear as attribute names (first element), the value
// is substituted directly. The filter is then evaluated against the current document.
func ResolveRefNew(expr any, newDoc map[string]any) any {
	if expr == nil {
		return nil
	}

	switch v := expr.(type) {
	case string:
		// Check if this is a $ref_new reference
		if strings.HasPrefix(v, refNewPrefix) {
			attrName := strings.TrimPrefix(v, refNewPrefix)
			if val, ok := newDoc[attrName]; ok {
				return val
			}
			return nil // Attribute not found in new doc
		}
		return v

	case []any:
		if len(v) == 0 {
			return v
		}

		// Handle boolean operators: ["And", [filters]] or ["Or", [filters]] or ["Not", filter]
		first, ok := v[0].(string)
		if ok && len(v) >= 2 {
			switch Operator(first) {
			case OpAnd, OpOr:
				// Recurse into the children array
				if children, ok := v[1].([]any); ok {
					resolvedChildren := make([]any, len(children))
					for i, child := range children {
						resolvedChildren[i] = ResolveRefNew(child, newDoc)
					}
					return []any{first, resolvedChildren}
				}
			case OpNot:
				// Recurse into the single child
				return []any{first, ResolveRefNew(v[1], newDoc)}
			}
		}

		// Handle comparison filters: ["attr", "Op", value] or ["$ref_new.attr", "Op", value]
		if len(v) == 3 {
			resolvedFirst := ResolveRefNew(v[0], newDoc)
			resolvedValue := ResolveRefNew(v[2], newDoc)
			return []any{resolvedFirst, v[1], resolvedValue}
		}

		// Generic array: resolve each element
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = ResolveRefNew(elem, newDoc)
		}
		return result

	case map[string]any:
		// Resolve $ref_new references in map values
		result := make(map[string]any, len(v))
		for k, val := range v {
			result[k] = ResolveRefNew(val, newDoc)
		}
		return result

	default:
		// Primitives (numbers, bools, etc.) pass through unchanged
		return v
	}
}

// HasRefNew checks if an expression contains any $ref_new references.
func HasRefNew(expr any) bool {
	if expr == nil {
		return false
	}

	switch v := expr.(type) {
	case string:
		return strings.HasPrefix(v, refNewPrefix)

	case []any:
		for _, elem := range v {
			if HasRefNew(elem) {
				return true
			}
		}
		return false

	case map[string]any:
		for _, val := range v {
			if HasRefNew(val) {
				return true
			}
		}
		return false

	default:
		return false
	}
}
