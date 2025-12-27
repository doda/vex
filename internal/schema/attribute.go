// Package schema provides schema-related types and validation for Vex.
package schema

// MaxAttributeNameLength is the maximum length for attribute names.
const MaxAttributeNameLength = 128

// ValidateAttributeName validates an attribute name according to the spec:
// - Up to 128 characters
// - Must not start with '$'
// - Empty names are rejected
// The special attribute "vector" is valid.
func ValidateAttributeName(name string) error {
	if name == "" {
		return ErrEmptyAttributeName
	}
	if len(name) > MaxAttributeNameLength {
		return ErrAttributeNameTooLong
	}
	if name[0] == '$' {
		return ErrAttributeDollarPrefix
	}
	return nil
}
