package filter

// NamespaceSchemaAdapter adapts a namespace schema to the SchemaChecker interface.
// This allows filter validation to work with namespace.Schema without importing it.
type NamespaceSchemaAdapter struct {
	// regexAttrs maps attribute names to whether they have regex: true
	regexAttrs map[string]bool
}

// NewNamespaceSchemaAdapter creates a SchemaChecker from a map of attribute regex settings.
// Use this to bridge namespace.Schema to filter.SchemaChecker.
func NewNamespaceSchemaAdapter(regexAttrs map[string]bool) *NamespaceSchemaAdapter {
	if regexAttrs == nil {
		regexAttrs = make(map[string]bool)
	}
	return &NamespaceSchemaAdapter{regexAttrs: regexAttrs}
}

// HasRegex returns true if the attribute has regex: true in its schema.
func (a *NamespaceSchemaAdapter) HasRegex(attrName string) bool {
	return a.regexAttrs[attrName]
}
