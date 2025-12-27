package wal

import (
	"encoding/base64"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCanonicalizeID(t *testing.T) {
	c := NewCanonicalizer()

	tests := []struct {
		name    string
		input   any
		wantErr bool
	}{
		{"uint64", uint64(42), false},
		{"int64", int64(42), false},
		{"float64_whole", float64(42), false},
		{"string_numeric", "123", false},
		{"string_uuid", "123e4567-e89b-12d3-a456-426614174000", false},
		{"string_regular", "my-doc-id", false},
		{"json_number", "999", false},
		{"negative_int", int64(-1), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := c.CanonicalizeID(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("CanonicalizeID() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCanonicalizeString(t *testing.T) {
	c := NewCanonicalizer()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid_ascii", "hello world", false},
		{"valid_unicode", "こんにちは", false},
		{"valid_emoji", "🎉", false},
		{"empty", "", false},
		{"invalid_utf8", string([]byte{0xff, 0xfe}), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := c.CanonicalizeString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("CanonicalizeString() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInvalidUTF8Returns400(t *testing.T) {
	c := NewCanonicalizer()

	invalidUTF8 := string([]byte{0x80, 0x81, 0x82})

	_, err := c.CanonicalizeString(invalidUTF8)
	if err == nil {
		t.Fatal("expected error for invalid UTF-8")
	}
	if err != ErrInvalidUTF8 {
		t.Errorf("expected ErrInvalidUTF8, got %v", err)
	}

	_, err = c.CanonicalizeValue(invalidUTF8)
	if err == nil {
		t.Fatal("expected error for invalid UTF-8 value")
	}
	if err != ErrInvalidUTF8 {
		t.Errorf("expected ErrInvalidUTF8, got %v", err)
	}

	attrs := map[string]any{
		"invalid": invalidUTF8,
	}
	_, err = c.CanonicalizeAttributes(attrs)
	if err == nil {
		t.Fatal("expected error for invalid UTF-8 in attributes")
	}
}

func TestCanonicalizeAttributes(t *testing.T) {
	c := NewCanonicalizer()

	attrs := map[string]any{
		"zebra":   "z",
		"apple":   "a",
		"banana":  "b",
		"number":  42,
		"float":   3.14,
		"boolean": true,
	}

	result, err := c.CanonicalizeAttributes(attrs)
	if err != nil {
		t.Fatalf("CanonicalizeAttributes failed: %v", err)
	}

	if len(result) != 6 {
		t.Errorf("expected 6 attributes, got %d", len(result))
	}

	if result["apple"].GetStringVal() != "a" {
		t.Error("apple value mismatch")
	}
	if result["number"].GetIntVal() != 42 {
		t.Error("number value mismatch")
	}
	if result["float"].GetFloatVal() != 3.14 {
		t.Error("float value mismatch")
	}
	if result["boolean"].GetBoolVal() != true {
		t.Error("boolean value mismatch")
	}
}

func TestCanonicalizeValue(t *testing.T) {
	c := NewCanonicalizer()
	u := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	now := time.Now()

	tests := []struct {
		name    string
		input   any
		check   func(*AttributeValue) bool
		wantErr bool
	}{
		{
			name:  "string",
			input: "hello",
			check: func(v *AttributeValue) bool { return v.GetStringVal() == "hello" },
		},
		{
			name:  "int",
			input: int(42),
			check: func(v *AttributeValue) bool { return v.GetIntVal() == 42 },
		},
		{
			name:  "int64",
			input: int64(-100),
			check: func(v *AttributeValue) bool { return v.GetIntVal() == -100 },
		},
		{
			name:  "uint64",
			input: uint64(100),
			check: func(v *AttributeValue) bool { return v.GetUintVal() == 100 },
		},
		{
			name:  "float64",
			input: float64(3.14),
			check: func(v *AttributeValue) bool { return v.GetFloatVal() == 3.14 },
		},
		{
			name:  "bool",
			input: true,
			check: func(v *AttributeValue) bool { return v.GetBoolVal() == true },
		},
		{
			name:  "nil",
			input: nil,
			check: func(v *AttributeValue) bool { return v.GetNullVal() == true },
		},
		{
			name:  "uuid",
			input: u,
			check: func(v *AttributeValue) bool { return len(v.GetUuidVal()) == 16 },
		},
		{
			name:  "time",
			input: now,
			check: func(v *AttributeValue) bool { return v.GetDatetimeVal() == now.UnixMilli() },
		},
		{
			name:  "string_array",
			input: []string{"a", "b", "c"},
			check: func(v *AttributeValue) bool {
				arr := v.GetStringArray()
				return arr != nil && len(arr.Values) == 3
			},
		},
		{
			name:  "int_array",
			input: []int64{1, 2, 3},
			check: func(v *AttributeValue) bool {
				arr := v.GetIntArray()
				return arr != nil && len(arr.Values) == 3
			},
		},
		{
			name:  "float_array",
			input: []float64{1.1, 2.2, 3.3},
			check: func(v *AttributeValue) bool {
				arr := v.GetFloatArray()
				return arr != nil && len(arr.Values) == 3
			},
		},
		{
			name:  "bool_array",
			input: []bool{true, false, true},
			check: func(v *AttributeValue) bool {
				arr := v.GetBoolArray()
				return arr != nil && len(arr.Values) == 3
			},
		},
		{
			name:  "any_string_slice",
			input: []any{"x", "y", "z"},
			check: func(v *AttributeValue) bool {
				arr := v.GetStringArray()
				return arr != nil && len(arr.Values) == 3
			},
		},
		{
			name:  "any_number_slice",
			input: []any{float64(1), float64(2), float64(3)},
			check: func(v *AttributeValue) bool {
				arr := v.GetFloatArray()
				return arr != nil && len(arr.Values) == 3
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := c.CanonicalizeValue(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !tt.check(result) {
				t.Errorf("check failed for %s", tt.name)
			}
		})
	}
}

func TestCanonicalizeDatetime(t *testing.T) {
	c := NewCanonicalizer()

	tests := []struct {
		name    string
		input   any
		want    int64
		wantErr bool
	}{
		{
			name:  "int64_ms",
			input: int64(1234567890000),
			want:  1234567890000,
		},
		{
			name:  "float64_ms",
			input: float64(1234567890000),
			want:  1234567890000,
		},
		{
			name:  "rfc3339",
			input: "2024-01-15T10:30:00Z",
			want:  1705314600000,
		},
		{
			name:  "rfc3339_nano",
			input: "2024-01-15T10:30:00.123456789Z",
			want:  1705314600123,
		},
		{
			name:  "rfc3339_offset",
			input: "2024-01-15T10:30:00+02:00",
			want:  1705307400000,
		},
		{
			name:  "date_only",
			input: "2024-01-15",
			want:  1705276800000,
		},
		{
			name:  "space_separator",
			input: "2024-01-15 10:30:00",
			want:  1705314600000,
		},
		{
			name:  "time_type",
			input: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			want:  1705314600000,
		},
		{
			name:    "invalid_format",
			input:   "not a date",
			wantErr: true,
		},
		{
			name:    "partial_date",
			input:   "2024-01",
			wantErr: true,
		},
		{
			name:    "invalid_type",
			input:   struct{}{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.CanonicalizeDatetime(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestInvalidDatetimeReturns400(t *testing.T) {
	c := NewCanonicalizer()

	invalidDates := []string{
		"not a date",
		"2024/01/15",
		"15-01-2024",
		"January 15, 2024",
		"",
	}

	for _, s := range invalidDates {
		_, err := c.CanonicalizeDatetime(s)
		if err == nil {
			t.Errorf("expected error for invalid datetime %q", s)
		}
		if err != ErrInvalidDatetime {
			t.Errorf("expected ErrInvalidDatetime for %q, got %v", s, err)
		}
	}
}

func TestCanonicalizeVector(t *testing.T) {
	c := NewCanonicalizer()

	t.Run("float32_array", func(t *testing.T) {
		input := []float32{1.0, 2.0, 3.0, 4.0}
		data, dims, err := c.CanonicalizeVector(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dims != 4 {
			t.Errorf("dims = %d, want 4", dims)
		}
		if len(data) != 16 {
			t.Errorf("data length = %d, want 16", len(data))
		}

		decoded := DecodeFloat32Vector(data)
		for i, f := range input {
			if decoded[i] != f {
				t.Errorf("decoded[%d] = %f, want %f", i, decoded[i], f)
			}
		}
	})

	t.Run("float64_array", func(t *testing.T) {
		input := []float64{1.5, 2.5, 3.5}
		data, dims, err := c.CanonicalizeVector(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dims != 3 {
			t.Errorf("dims = %d, want 3", dims)
		}

		decoded := DecodeFloat32Vector(data)
		for i, f := range input {
			if decoded[i] != float32(f) {
				t.Errorf("decoded[%d] = %f, want %f", i, decoded[i], float32(f))
			}
		}
	})

	t.Run("any_slice", func(t *testing.T) {
		input := []any{float64(1.0), float64(2.0)}
		data, dims, err := c.CanonicalizeVector(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dims != 2 {
			t.Errorf("dims = %d, want 2", dims)
		}
		if len(data) != 8 {
			t.Errorf("data length = %d, want 8", len(data))
		}
	})

	t.Run("base64_standard", func(t *testing.T) {
		vector := []float32{1.0, 2.0}
		vectorBytes := make([]byte, 8)
		for i, f := range vector {
			bits := math.Float32bits(f)
			vectorBytes[i*4] = byte(bits)
			vectorBytes[i*4+1] = byte(bits >> 8)
			vectorBytes[i*4+2] = byte(bits >> 16)
			vectorBytes[i*4+3] = byte(bits >> 24)
		}
		encoded := base64.StdEncoding.EncodeToString(vectorBytes)

		data, dims, err := c.CanonicalizeVector(encoded)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dims != 2 {
			t.Errorf("dims = %d, want 2", dims)
		}

		decoded := DecodeFloat32Vector(data)
		for i, f := range vector {
			if decoded[i] != f {
				t.Errorf("decoded[%d] = %f, want %f", i, decoded[i], f)
			}
		}
	})

	t.Run("invalid_type", func(t *testing.T) {
		_, _, err := c.CanonicalizeVector("not base64!!!")
		if err == nil {
			t.Error("expected error for invalid input")
		}
	})

	t.Run("mixed_any_slice", func(t *testing.T) {
		input := []any{"string", 123}
		_, _, err := c.CanonicalizeVector(input)
		if err == nil {
			t.Error("expected error for mixed types")
		}
	})
}

func TestDecodeFloat32Vector(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		input := []float32{1.0, -2.5, 3.14, 0.0}
		encoded := make([]byte, len(input)*4)
		for i, f := range input {
			bits := math.Float32bits(f)
			encoded[i*4] = byte(bits)
			encoded[i*4+1] = byte(bits >> 8)
			encoded[i*4+2] = byte(bits >> 16)
			encoded[i*4+3] = byte(bits >> 24)
		}

		decoded := DecodeFloat32Vector(encoded)
		if len(decoded) != len(input) {
			t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(input))
		}

		for i, f := range input {
			if decoded[i] != f {
				t.Errorf("element %d mismatch: got %f, want %f", i, decoded[i], f)
			}
		}
	})

	t.Run("invalid_length", func(t *testing.T) {
		data := []byte{1, 2, 3}
		decoded := DecodeFloat32Vector(data)
		if decoded != nil {
			t.Error("expected nil for invalid length")
		}
	})

	t.Run("empty", func(t *testing.T) {
		decoded := DecodeFloat32Vector([]byte{})
		if len(decoded) != 0 {
			t.Error("expected empty slice for empty input")
		}
	})
}

func TestParseDatetime(t *testing.T) {
	tests := []struct {
		input   string
		wantErr bool
	}{
		{"2024-01-15T10:30:00Z", false},
		{"2024-01-15T10:30:00.123Z", false},
		{"2024-01-15T10:30:00+05:30", false},
		{"2024-01-15T10:30:00-08:00", false},
		{"2024-01-15 10:30:00", false},
		{"2024-01-15 10:30:00.123456789", false},
		{"2024-01-15", false},
		{"invalid", true},
		{"2024", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := ParseDatetime(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDatetime(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestMixedArrayTypes(t *testing.T) {
	c := NewCanonicalizer()

	_, err := c.CanonicalizeValue([]any{"string", 123})
	if err == nil {
		t.Error("expected error for mixed string/int array")
	}
	if err != ErrMixedArrayTypes {
		t.Errorf("expected ErrMixedArrayTypes, got %v", err)
	}

	_, err = c.CanonicalizeValue([]any{true, "false"})
	if err == nil {
		t.Error("expected error for mixed bool/string array")
	}
}

func TestEmptyArrayHandling(t *testing.T) {
	c := NewCanonicalizer()

	result, err := c.CanonicalizeValue([]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	arr := result.GetStringArray()
	if arr == nil {
		t.Error("expected string array for empty slice")
	}
}

func TestValidateString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid_ascii", "hello", false},
		{"valid_unicode", "日本語", false},
		{"valid_emoji", "😀", false},
		{"empty", "", false},
		{"invalid_sequence", string([]byte{0xc0, 0x80}), true},
		{"truncated_sequence", string([]byte{0xe0, 0x80}), true},
		{"invalid_continuation", string([]byte{0xc0, 0x41}), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateString() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
