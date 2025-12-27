package namespace

import (
	"strings"
	"testing"
)

func TestValidateName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"valid simple", "test", nil},
		{"valid with numbers", "test123", nil},
		{"valid with dash", "test-namespace", nil},
		{"valid with underscore", "test_namespace", nil},
		{"valid with dot", "test.namespace", nil},
		{"valid mixed", "Test-123_foo.bar", nil},
		{"valid single char", "a", nil},
		{"valid 128 chars", strings.Repeat("a", 128), nil},

		{"empty", "", ErrEmptyName},

		{"too long 129 chars", strings.Repeat("a", 129), ErrNameTooLong},
		{"too long 200 chars", strings.Repeat("a", 200), ErrNameTooLong},

		{"invalid space", "test namespace", ErrInvalidCharacters},
		{"invalid slash", "test/namespace", ErrInvalidCharacters},
		{"invalid backslash", "test\\namespace", ErrInvalidCharacters},
		{"invalid colon", "test:namespace", ErrInvalidCharacters},
		{"invalid star", "test*namespace", ErrInvalidCharacters},
		{"invalid question", "test?namespace", ErrInvalidCharacters},
		{"invalid quote", "test\"namespace", ErrInvalidCharacters},
		{"invalid lt", "test<namespace", ErrInvalidCharacters},
		{"invalid gt", "test>namespace", ErrInvalidCharacters},
		{"invalid pipe", "test|namespace", ErrInvalidCharacters},
		{"invalid at", "test@namespace", ErrInvalidCharacters},
		{"invalid hash", "test#namespace", ErrInvalidCharacters},
		{"invalid dollar", "test$namespace", ErrInvalidCharacters},
		{"invalid percent", "test%namespace", ErrInvalidCharacters},
		{"invalid unicode", "тест", ErrInvalidCharacters},
		{"invalid emoji", "test🎉", ErrInvalidCharacters},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateName(tt.input)
			if err != tt.wantErr {
				t.Errorf("ValidateName(%q) = %v, want %v", tt.input, err, tt.wantErr)
			}
		})
	}
}
