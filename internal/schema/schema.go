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

	// Optional field options (to be implemented in schema-field-options task)
	Filterable     *bool
	Regex          bool
	FullTextSearch any
	Vector         *VectorFieldConfig
}

// VectorFieldConfig represents vector configuration for a vector attribute.
type VectorFieldConfig struct {
	Type string // e.g., "[1536]f32"
	ANN  bool
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
		clone.Attributes[name] = attr
	}
	return clone
}

// ValidateSchemaUpdate validates that a schema update is compatible.
// Returns an error if any attribute types would change.
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
		}
	}
	return nil
}
