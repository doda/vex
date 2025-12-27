package schema

import "errors"

var (
	ErrEmptyAttributeName     = errors.New("attribute name cannot be empty")
	ErrAttributeNameTooLong   = errors.New("attribute name exceeds 128 characters")
	ErrAttributeDollarPrefix  = errors.New("attribute name cannot start with '$'")
)
