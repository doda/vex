package schema

import "errors"

var (
	// Attribute name validation errors
	ErrEmptyAttributeName    = errors.New("attribute name cannot be empty")
	ErrAttributeNameTooLong  = errors.New("attribute name exceeds 128 characters")
	ErrAttributeDollarPrefix = errors.New("attribute name cannot start with '$'")

	// Type system errors
	ErrCannotInferNil        = errors.New("cannot infer type from nil value")
	ErrCannotInferEmptyArray = errors.New("cannot infer type from empty array")
	ErrUnsupportedType       = errors.New("unsupported type")
	ErrInvalidType           = errors.New("invalid type")
	ErrTypeMismatch          = errors.New("type mismatch")
	ErrTypeChangeNotAllowed  = errors.New("changing attribute type is not allowed")

	// Field option errors
	ErrVectorConfigChangeNotAllowed = errors.New("changing vector configuration is not allowed")
)
