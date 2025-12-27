package namespace

import "regexp"

const MaxNameLength = 128

var validNamePattern = regexp.MustCompile(`^[A-Za-z0-9\-_.]+$`)

func ValidateName(name string) error {
	if name == "" {
		return ErrEmptyName
	}
	if len(name) > MaxNameLength {
		return ErrNameTooLong
	}
	if !validNamePattern.MatchString(name) {
		return ErrInvalidCharacters
	}
	return nil
}
