package schema

import (
	"fmt"
)

// Definition represents a complete schema definition for a namespace.
type Definition struct {
	Attributes map[string]Attribute
}

// Attribute represents the schema for a single attribute.
type Attribute struct {
	Type AttrType

	// Field options
	Filterable     *bool              // nil = use default (true unless regex or FTS enabled)
	Regex          bool               // Default false
	FullTextSearch *FullTextConfig    // nil = disabled
	Vector         *VectorFieldConfig // nil = not a vector field
}

// FullTextConfig represents full-text search configuration for an attribute.
// Can be enabled with just a boolean (use defaults) or with detailed options.
type FullTextConfig struct {
	// Core options
	Tokenizer       string  // default "word_v3"
	CaseSensitive   bool    // default false
	Language        string  // default "english"
	Stemming        bool    // default true
	RemoveStopwords bool    // default true
	ASCIIFolding    bool    // default false

	// BM25 parameters
	BM25K1 float64 // default 1.2
	BM25B  float64 // default 0.75
}

// NewFullTextConfig creates a FullTextConfig with default values.
func NewFullTextConfig() *FullTextConfig {
	return &FullTextConfig{
		Tokenizer:       "word_v3",
		CaseSensitive:   false,
		Language:        "english",
		Stemming:        true,
		RemoveStopwords: true,
		ASCIIFolding:    false,
		BM25K1:          1.2,
		BM25B:           0.75,
	}
}

// VectorFieldConfig represents vector configuration for a vector attribute.
type VectorFieldConfig struct {
	Type string // e.g., "[1536]f32" or "[1536]f16"
	ANN  bool   // default false (exhaustive search)
}

// IsFilterable returns the effective filterable value for the attribute.
// Default is true, but becomes false when regex or full_text_search is enabled.
func (a Attribute) IsFilterable() bool {
	if a.Filterable != nil {
		return *a.Filterable
	}
	// Default is true, but false if regex or FTS enabled
	if a.Regex || a.FullTextSearch != nil {
		return false
	}
	return true
}

// HasFullTextSearch returns true if full-text search is enabled.
func (a Attribute) HasFullTextSearch() bool {
	return a.FullTextSearch != nil
}

// HasRegex returns true if regex matching is enabled.
func (a Attribute) HasRegex() bool {
	return a.Regex
}

// IsVector returns true if this is a vector attribute.
func (a Attribute) IsVector() bool {
	return a.Vector != nil
}

// Clone creates a deep copy of the attribute.
func (a Attribute) Clone() Attribute {
	clone := Attribute{
		Type:  a.Type,
		Regex: a.Regex,
	}
	if a.Filterable != nil {
		v := *a.Filterable
		clone.Filterable = &v
	}
	if a.FullTextSearch != nil {
		ftsCopy := *a.FullTextSearch
		clone.FullTextSearch = &ftsCopy
	}
	if a.Vector != nil {
		vecCopy := *a.Vector
		clone.Vector = &vecCopy
	}
	return clone
}

// NewDefinition creates an empty schema definition.
func NewDefinition() *Definition {
	return &Definition{
		Attributes: make(map[string]Attribute),
	}
}

// SetAttribute sets or updates an attribute in the schema.
// Returns an error if attempting to change an existing attribute's type.
func (d *Definition) SetAttribute(name string, attr Attribute) error {
	if err := ValidateAttributeName(name); err != nil {
		return err
	}
	if !attr.Type.IsValid() {
		return fmt.Errorf("%w: %s", ErrInvalidType, attr.Type)
	}

	if existing, ok := d.Attributes[name]; ok {
		if existing.Type != attr.Type {
			return fmt.Errorf("%w: attribute %q has type %s, cannot change to %s",
				ErrTypeChangeNotAllowed, name, existing.Type, attr.Type)
		}
	}

	d.Attributes[name] = attr
	return nil
}

// GetAttribute returns the attribute schema for the given name.
// Returns nil if the attribute doesn't exist.
func (d *Definition) GetAttribute(name string) *Attribute {
	if attr, ok := d.Attributes[name]; ok {
		return &attr
	}
	return nil
}

// HasAttribute returns true if the attribute exists in the schema.
func (d *Definition) HasAttribute(name string) bool {
	_, ok := d.Attributes[name]
	return ok
}

// InferAndAddAttribute infers the type from a value and adds the attribute.
// Returns an error if the value's type conflicts with an existing attribute.
func (d *Definition) InferAndAddAttribute(name string, value any) error {
	if value == nil {
		// null values don't define type
		return nil
	}

	attrType, err := InferTypeFromJSON(value)
	if err != nil {
		return fmt.Errorf("attribute %q: %w", name, err)
	}

	return d.SetAttribute(name, Attribute{Type: attrType})
}

// Merge merges another schema definition into this one.
// Returns an error if there are type conflicts.
func (d *Definition) Merge(other *Definition) error {
	if other == nil {
		return nil
	}
	for name, attr := range other.Attributes {
		if err := d.SetAttribute(name, attr); err != nil {
			return err
		}
	}
	return nil
}

// Validate validates that all attributes in the schema have valid types.
func (d *Definition) Validate() error {
	for name, attr := range d.Attributes {
		if err := ValidateAttributeName(name); err != nil {
			return fmt.Errorf("attribute %q: %w", name, err)
		}
		if !attr.Type.IsValid() {
			return fmt.Errorf("attribute %q: %w: %s", name, ErrInvalidType, attr.Type)
		}
	}
	return nil
}

// Clone creates a deep copy of the schema definition.
func (d *Definition) Clone() *Definition {
	clone := NewDefinition()
	for name, attr := range d.Attributes {
		clone.Attributes[name] = attr.Clone()
	}
	return clone
}

// ValidateSchemaUpdate validates that a schema update is compatible.
// Returns an error if any attribute types would change.
// Note: filterable and full_text_search CAN be updated online.
func ValidateSchemaUpdate(existing, proposed *Definition) error {
	if existing == nil {
		return nil
	}
	if proposed == nil {
		return nil
	}

	for name, newAttr := range proposed.Attributes {
		if oldAttr, ok := existing.Attributes[name]; ok {
			if oldAttr.Type != newAttr.Type {
				return fmt.Errorf("%w: attribute %q has type %s, cannot change to %s",
					ErrTypeChangeNotAllowed, name, oldAttr.Type, newAttr.Type)
			}
			// Note: filterable and full_text_search can be changed online
			// regex changes also allowed (may require index rebuild)
			// vector config changes are NOT allowed once set
			if oldAttr.Vector != nil && newAttr.Vector == nil {
				return fmt.Errorf("%w: cannot remove vector config from attribute %q",
					ErrVectorConfigChangeNotAllowed, name)
			}
			if oldAttr.Vector == nil && newAttr.Vector != nil {
				return fmt.Errorf("%w: cannot add vector config to existing attribute %q",
					ErrVectorConfigChangeNotAllowed, name)
			}
			if oldAttr.Vector != nil && newAttr.Vector != nil {
				if oldAttr.Vector.Type != newAttr.Vector.Type {
					return fmt.Errorf("%w: cannot change vector type from %s to %s",
						ErrVectorConfigChangeNotAllowed, oldAttr.Vector.Type, newAttr.Vector.Type)
				}
			}
		}
	}
	return nil
}

// FullTextUpdate represents an update to the full_text_search field option.
// Use nil to indicate no change, DisableFTS() to disable, or EnableFTS(config) to enable.
type FullTextUpdate struct {
	disable bool
	config  *FullTextConfig
}

// DisableFTS returns a FullTextUpdate that disables full-text search.
func DisableFTS() *FullTextUpdate {
	return &FullTextUpdate{disable: true}
}

// EnableFTS returns a FullTextUpdate that enables full-text search with the given config.
func EnableFTS(cfg *FullTextConfig) *FullTextUpdate {
	if cfg == nil {
		cfg = NewFullTextConfig()
	}
	return &FullTextUpdate{config: cfg}
}

// UpdateFieldOptions updates the field options (filterable, full_text_search) on an existing attribute.
// This is allowed online and may trigger an index rebuild in the background.
// Pass nil for fts to leave unchanged, DisableFTS() to disable, or EnableFTS(config) to enable.
func (d *Definition) UpdateFieldOptions(name string, filterable *bool, fts *FullTextUpdate, regex *bool) error {
	attr, ok := d.Attributes[name]
	if !ok {
		return fmt.Errorf("attribute %q not found", name)
	}

	if filterable != nil {
		attr.Filterable = filterable
	}
	if fts != nil {
		if fts.disable {
			attr.FullTextSearch = nil
		} else {
			attr.FullTextSearch = fts.config
		}
	}
	if regex != nil {
		attr.Regex = *regex
	}

	d.Attributes[name] = attr
	return nil
}

// ParseFullTextSearch parses a full_text_search value which can be:
// - bool: true means use default config, false means disabled
// - map: custom configuration options
func ParseFullTextSearch(v any) (*FullTextConfig, error) {
	if v == nil {
		return nil, nil
	}

	switch val := v.(type) {
	case bool:
		if val {
			return NewFullTextConfig(), nil
		}
		return nil, nil

	case map[string]any:
		cfg := NewFullTextConfig()
		if tokenizer, ok := val["tokenizer"].(string); ok {
			cfg.Tokenizer = tokenizer
		}
		if caseSensitive, ok := val["case_sensitive"].(bool); ok {
			cfg.CaseSensitive = caseSensitive
		}
		if language, ok := val["language"].(string); ok {
			cfg.Language = language
		}
		if stemming, ok := val["stemming"].(bool); ok {
			cfg.Stemming = stemming
		}
		if removeStopwords, ok := val["remove_stopwords"].(bool); ok {
			cfg.RemoveStopwords = removeStopwords
		}
		if asciiFolding, ok := val["ascii_folding"].(bool); ok {
			cfg.ASCIIFolding = asciiFolding
		}
		if k1, ok := val["k1"].(float64); ok {
			cfg.BM25K1 = k1
		}
		if b, ok := val["b"].(float64); ok {
			cfg.BM25B = b
		}
		return cfg, nil

	default:
		return nil, fmt.Errorf("full_text_search must be a boolean or object, got %T", v)
	}
}

// ParseVectorConfig parses a vector field configuration from JSON.
func ParseVectorConfig(v any) (*VectorFieldConfig, error) {
	if v == nil {
		return nil, nil
	}

	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("vector config must be an object, got %T", v)
	}

	cfg := &VectorFieldConfig{}

	if typeVal, ok := m["type"].(string); ok {
		cfg.Type = typeVal
	} else {
		return nil, fmt.Errorf("vector config must have 'type' field")
	}

	if ann, ok := m["ann"].(bool); ok {
		cfg.ANN = ann
	}

	return cfg, nil
}
