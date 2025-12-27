package schema

import (
	"strings"
	"testing"
)

func TestValidateAttributeNameAcceptsUpTo128Chars(t *testing.T) {
	// Test attribute names up to exactly 128 chars are accepted
	for length := 1; length <= 128; length++ {
		name := strings.Repeat("x", length)
		if err := ValidateAttributeName(name); err != nil {
			t.Errorf("ValidateAttributeName(string of %d chars) returned error: %v", length, err)
		}
	}
}

func TestValidateAttributeNameRejectsOver128Chars(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{"129 chars", 129},
		{"200 chars", 200},
		{"500 chars", 500},
		{"1000 chars", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := strings.Repeat("a", tt.length)
			err := ValidateAttributeName(name)
			if err == nil {
				t.Fatalf("ValidateAttributeName(string of %d chars) should return error, got nil", tt.length)
			}
			if err != ErrAttributeNameTooLong {
				t.Errorf("expected ErrAttributeNameTooLong, got %v", err)
			}
		})
	}
}

func TestValidateAttributeNameRejectsDollarPrefix(t *testing.T) {
	tests := []string{
		"$",
		"$attr",
		"$dist",
		"$ref_new",
		"$something_else",
		"$123",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateAttributeName(name)
			if err == nil {
				t.Fatalf("ValidateAttributeName(%q) should return error, got nil", name)
			}
			if err != ErrAttributeDollarPrefix {
				t.Errorf("expected ErrAttributeDollarPrefix, got %v", err)
			}
		})
	}
}

func TestValidateAttributeNameAcceptsVector(t *testing.T) {
	err := ValidateAttributeName("vector")
	if err != nil {
		t.Errorf("ValidateAttributeName(\"vector\") returned error: %v", err)
	}
}

func TestValidateAttributeNameRejectsEmpty(t *testing.T) {
	err := ValidateAttributeName("")
	if err == nil {
		t.Fatal("ValidateAttributeName(\"\") should return error, got nil")
	}
	if err != ErrEmptyAttributeName {
		t.Errorf("expected ErrEmptyAttributeName, got %v", err)
	}
}

func TestValidateAttributeNameAcceptsVariousNames(t *testing.T) {
	// Valid attribute names
	validNames := []string{
		"a",
		"name",
		"vector",
		"my_attribute",
		"attribute123",
		"CamelCase",
		"snake_case",
		"with-dashes",
		"with.dots",
		"MixedCase_123",
		"unicode_日本語",
		"émojis_💯",
		strings.Repeat("x", 128), // exactly 128 chars
	}

	for _, name := range validNames {
		t.Run(name[:min(len(name), 20)], func(t *testing.T) {
			if err := ValidateAttributeName(name); err != nil {
				t.Errorf("ValidateAttributeName(%q) returned error: %v", name, err)
			}
		})
	}
}

func TestValidateAttributeNameDollarInMiddleIsOK(t *testing.T) {
	// Dollar sign in middle or end of name is OK, only prefix is rejected
	validNames := []string{
		"a$b",
		"price$",
		"foo$bar$baz",
		"mid$dle",
	}

	for _, name := range validNames {
		t.Run(name, func(t *testing.T) {
			if err := ValidateAttributeName(name); err != nil {
				t.Errorf("ValidateAttributeName(%q) returned error: %v", name, err)
			}
		})
	}
}
