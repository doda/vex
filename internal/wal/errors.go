package wal

import "errors"

var (
	ErrChecksumMismatch     = errors.New("checksum mismatch")
	ErrInvalidDocumentID    = errors.New("invalid document ID")
	ErrInvalidUTF8          = errors.New("invalid UTF-8 string")
	ErrInvalidDatetime      = errors.New("invalid datetime format")
	ErrInvalidAttributeType = errors.New("invalid attribute type")
	ErrMixedArrayTypes      = errors.New("mixed types in array")
	ErrInvalidVectorType    = errors.New("invalid vector type")
)
