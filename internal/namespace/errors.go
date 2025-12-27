package namespace

import "errors"

var (
	ErrEmptyName         = errors.New("namespace name cannot be empty")
	ErrNameTooLong       = errors.New("namespace name exceeds 128 characters")
	ErrInvalidCharacters = errors.New("namespace name contains invalid characters; only A-Z, a-z, 0-9, dash, underscore, and dot are allowed")
)
