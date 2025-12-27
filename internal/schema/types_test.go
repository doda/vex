package schema

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAttrTypeIsValid(t *testing.T) {
	validTypes := []AttrType{
		TypeString, TypeInt, TypeUint, TypeFloat, TypeUUID, TypeDatetime, TypeBool,
		TypeStringArray, TypeIntArray, TypeUintArray, TypeFloatArray, TypeUUIDArray, TypeDatetimeArray, TypeBoolArray,
	}
	for _, typ := range validTypes {
		if !typ.IsValid() {
			t.Errorf("expected %s to be valid", typ)
		}
	}

	invalidTypes := []AttrType{"invalid", "foo", "[]bar", ""}
	for _, typ := range invalidTypes {
		if typ.IsValid() {
			t.Errorf("expected %s to be invalid", typ)
		}
	}
}

func TestAttrTypeIsArray(t *testing.T) {
	arrayTypes := []AttrType{
		TypeStringArray, TypeIntArray, TypeUintArray, TypeFloatArray, TypeUUIDArray, TypeDatetimeArray, TypeBoolArray,
	}
	for _, typ := range arrayTypes {
		if !typ.IsArray() {
			t.Errorf("expected %s to be an array type", typ)
		}
	}

	scalarTypes := []AttrType{
		TypeString, TypeInt, TypeUint, TypeFloat, TypeUUID, TypeDatetime, TypeBool,
	}
	for _, typ := range scalarTypes {
		if typ.IsArray() {
			t.Errorf("expected %s to be a scalar type", typ)
		}
	}
}

func TestAttrTypeElementType(t *testing.T) {
	tests := []struct {
		input    AttrType
		expected AttrType
	}{
		{TypeString, TypeString},
		{TypeInt, TypeInt},
		{TypeUint, TypeUint},
		{TypeFloat, TypeFloat},
		{TypeUUID, TypeUUID},
		{TypeDatetime, TypeDatetime},
		{TypeBool, TypeBool},
		{TypeStringArray, TypeString},
		{TypeIntArray, TypeInt},
		{TypeUintArray, TypeUint},
		{TypeFloatArray, TypeFloat},
		{TypeUUIDArray, TypeUUID},
		{TypeDatetimeArray, TypeDatetime},
		{TypeBoolArray, TypeBool},
	}
	for _, tt := range tests {
		if got := tt.input.ElementType(); got != tt.expected {
			t.Errorf("ElementType(%s) = %s, want %s", tt.input, got, tt.expected)
		}
	}
}

func TestAttrTypeArrayType(t *testing.T) {
	tests := []struct {
		input    AttrType
		expected AttrType
	}{
		{TypeString, TypeStringArray},
		{TypeInt, TypeIntArray},
		{TypeUint, TypeUintArray},
		{TypeFloat, TypeFloatArray},
		{TypeUUID, TypeUUIDArray},
		{TypeDatetime, TypeDatetimeArray},
		{TypeBool, TypeBoolArray},
		// Array types should return themselves
		{TypeStringArray, TypeStringArray},
	}
	for _, tt := range tests {
		if got := tt.input.ArrayType(); got != tt.expected {
			t.Errorf("ArrayType(%s) = %s, want %s", tt.input, got, tt.expected)
		}
	}
}

func TestInferType(t *testing.T) {
	testUUID := uuid.New()
	testTime := time.Now()

	tests := []struct {
		name     string
		value    any
		expected AttrType
		wantErr  bool
	}{
		// Scalar types
		{"string", "hello", TypeString, false},
		{"int", int(42), TypeInt, false},
		{"int64", int64(42), TypeInt, false},
		{"int32", int32(42), TypeInt, false},
		{"int16", int16(42), TypeInt, false},
		{"int8", int8(42), TypeInt, false},
		{"uint", uint(42), TypeUint, false},
		{"uint64", uint64(42), TypeUint, false},
		{"uint32", uint32(42), TypeUint, false},
		{"uint16", uint16(42), TypeUint, false},
		{"uint8", uint8(42), TypeUint, false},
		{"float64", float64(3.14), TypeFloat, false},
		{"float32", float32(3.14), TypeFloat, false},
		{"bool_true", true, TypeBool, false},
		{"bool_false", false, TypeBool, false},
		{"time", testTime, TypeDatetime, false},
		{"uuid", testUUID, TypeUUID, false},

		// Array types (typed slices)
		{"[]string", []string{"a", "b"}, TypeStringArray, false},
		{"[]int", []int{1, 2, 3}, TypeIntArray, false},
		{"[]int64", []int64{1, 2, 3}, TypeIntArray, false},
		{"[]uint", []uint{1, 2, 3}, TypeUintArray, false},
		{"[]uint64", []uint64{1, 2, 3}, TypeUintArray, false},
		{"[]float64", []float64{1.1, 2.2}, TypeFloatArray, false},
		{"[]float32", []float32{1.1, 2.2}, TypeFloatArray, false},
		{"[]bool", []bool{true, false}, TypeBoolArray, false},
		{"[]time", []time.Time{testTime}, TypeDatetimeArray, false},
		{"[]uuid", []uuid.UUID{testUUID}, TypeUUIDArray, false},

		// []any with elements
		{"[]any_string", []any{"a", "b"}, TypeStringArray, false},
		{"[]any_int", []any{int(1), int(2)}, TypeIntArray, false},
		{"[]any_float", []any{float64(1.1)}, TypeFloatArray, false},
		{"[]any_bool", []any{true}, TypeBoolArray, false},

		// Error cases
		{"nil", nil, "", true},
		{"empty_array", []any{}, "", true},
		{"unsupported", map[string]string{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InferType(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("InferType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("InferType() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestInferTypeFromJSON(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected AttrType
		wantErr  bool
	}{
		// String types
		{"plain_string", "hello", TypeString, false},
		{"empty_string", "", TypeString, false},

		// UUID detection from string
		{"uuid_string", "550e8400-e29b-41d4-a716-446655440000", TypeUUID, false},
		{"uuid_no_dashes", "550e8400e29b41d4a716446655440000", TypeUUID, false},

		// Datetime detection from string
		{"datetime_rfc3339", "2024-03-15T10:30:45Z", TypeDatetime, false},
		{"datetime_rfc3339_nano", "2024-03-15T10:30:45.123456789Z", TypeDatetime, false},
		{"datetime_with_tz", "2024-03-15T10:30:45+05:00", TypeDatetime, false},

		// JSON numbers (float64)
		{"positive_int_as_float", float64(42), TypeUint, false},
		{"negative_int_as_float", float64(-42), TypeInt, false},
		{"zero", float64(0), TypeUint, false},
		{"actual_float", float64(3.14), TypeFloat, false},
		{"large_int", float64(1e15), TypeUint, false},

		// Boolean
		{"bool_true", true, TypeBool, false},
		{"bool_false", false, TypeBool, false},

		// Arrays
		{"array_strings", []any{"a", "b"}, TypeStringArray, false},
		{"array_ints", []any{float64(1), float64(2)}, TypeUintArray, false},
		{"array_floats", []any{float64(1.5), float64(2.5)}, TypeFloatArray, false},
		{"array_uuids", []any{"550e8400-e29b-41d4-a716-446655440000"}, TypeUUIDArray, false},

		// Error cases
		{"nil", nil, "", true},
		{"empty_array", []any{}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InferTypeFromJSON(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("InferTypeFromJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("InferTypeFromJSON() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestValidateValue(t *testing.T) {
	testUUID := uuid.New()
	testTime := time.Now()

	tests := []struct {
		name    string
		typ     AttrType
		value   any
		wantErr bool
	}{
		// null is valid for any type
		{"null_string", TypeString, nil, false},
		{"null_int", TypeInt, nil, false},
		{"null_array", TypeStringArray, nil, false},

		// String
		{"string_valid", TypeString, "hello", false},
		{"string_empty", TypeString, "", false},
		{"string_invalid", TypeString, 42, true},

		// Int
		{"int_valid_int", TypeInt, int(42), false},
		{"int_valid_int64", TypeInt, int64(-42), false},
		{"int_valid_float_int", TypeInt, float64(42), false},
		{"int_invalid_float", TypeInt, float64(3.14), true},
		{"int_invalid_string", TypeInt, "42", true},

		// Uint
		{"uint_valid_uint", TypeUint, uint(42), false},
		{"uint_valid_uint64", TypeUint, uint64(42), false},
		{"uint_valid_int", TypeUint, int(42), false},
		{"uint_valid_float_uint", TypeUint, float64(42), false},
		{"uint_invalid_negative_int", TypeUint, int(-1), true},
		{"uint_invalid_negative", TypeUint, float64(-1), true},
		{"uint_invalid_string", TypeUint, "42", true},

		// Float
		{"float_valid_float64", TypeFloat, float64(3.14), false},
		{"float_valid_float32", TypeFloat, float32(3.14), false},
		{"float_valid_int", TypeFloat, int(42), false},
		{"float_valid_uint", TypeFloat, uint(42), false},
		{"float_invalid_string", TypeFloat, "3.14", true},

		// Bool
		{"bool_valid_true", TypeBool, true, false},
		{"bool_valid_false", TypeBool, false, false},
		{"bool_invalid_int", TypeBool, 1, true},
		{"bool_invalid_string", TypeBool, "true", true},

		// Datetime
		{"datetime_valid_time", TypeDatetime, testTime, false},
		{"datetime_valid_rfc3339", TypeDatetime, "2024-03-15T10:30:45Z", false},
		{"datetime_valid_epoch", TypeDatetime, int64(1710500000000), false},
		{"datetime_valid_float_epoch", TypeDatetime, float64(1710500000000), false},
		{"datetime_invalid_string", TypeDatetime, "not a date", true},

		// UUID
		{"uuid_valid_uuid", TypeUUID, testUUID, false},
		{"uuid_valid_string", TypeUUID, "550e8400-e29b-41d4-a716-446655440000", false},
		{"uuid_valid_bytes", TypeUUID, make([]byte, 16), false},
		{"uuid_invalid_string", TypeUUID, "not-a-uuid", true},
		{"uuid_invalid_bytes", TypeUUID, make([]byte, 8), true},

		// String array
		{"array_string_valid", TypeStringArray, []any{"a", "b"}, false},
		{"array_string_typed", TypeStringArray, []string{"a", "b"}, false},
		{"array_string_invalid", TypeStringArray, []any{1, 2}, true},

		// Int array
		{"array_int_valid", TypeIntArray, []any{int(1), int(2)}, false},
		{"array_int_typed", TypeIntArray, []int{1, 2}, false},
		{"array_int_typed64", TypeIntArray, []int64{1, 2}, false},
		{"array_int_from_float", TypeIntArray, []any{float64(1), float64(2)}, false},
		{"array_int_invalid", TypeIntArray, []any{"a", "b"}, true},

		// Float array
		{"array_float_valid", TypeFloatArray, []any{float64(1.1), float64(2.2)}, false},
		{"array_float_typed", TypeFloatArray, []float64{1.1, 2.2}, false},
		{"array_float_invalid", TypeFloatArray, []any{"a"}, true},

		// Bool array
		{"array_bool_valid", TypeBoolArray, []any{true, false}, false},
		{"array_bool_typed", TypeBoolArray, []bool{true, false}, false},
		{"array_bool_invalid", TypeBoolArray, []any{1, 0}, true},

		// UUID array
		{"array_uuid_valid", TypeUUIDArray, []any{"550e8400-e29b-41d4-a716-446655440000"}, false},
		{"array_uuid_typed", TypeUUIDArray, []uuid.UUID{testUUID}, false},

		// Datetime array
		{"array_datetime_valid", TypeDatetimeArray, []any{"2024-03-15T10:30:45Z"}, false},
		{"array_datetime_typed", TypeDatetimeArray, []time.Time{testTime}, false},

		// Invalid type
		{"invalid_type", AttrType("foo"), "bar", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateValue(tt.typ, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateValue(%s, %v) error = %v, wantErr %v", tt.typ, tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestTypesCompatible(t *testing.T) {
	tests := []struct {
		old      AttrType
		new      AttrType
		expected bool
	}{
		{TypeString, TypeString, true},
		{TypeInt, TypeInt, true},
		{TypeStringArray, TypeStringArray, true},
		{TypeString, TypeInt, false},
		{TypeInt, TypeUint, false},
		{TypeString, TypeStringArray, false},
		{TypeFloat, TypeInt, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.old)+"_to_"+string(tt.new), func(t *testing.T) {
			if got := TypesCompatible(tt.old, tt.new); got != tt.expected {
				t.Errorf("TypesCompatible(%s, %s) = %v, want %v", tt.old, tt.new, got, tt.expected)
			}
		})
	}
}

func TestAttrTypeString(t *testing.T) {
	tests := []struct {
		typ      AttrType
		expected string
	}{
		{TypeString, "string"},
		{TypeInt, "int"},
		{TypeUint, "uint"},
		{TypeFloat, "float"},
		{TypeUUID, "uuid"},
		{TypeDatetime, "datetime"},
		{TypeBool, "bool"},
		{TypeStringArray, "[]string"},
		{TypeIntArray, "[]int"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.expected {
			t.Errorf("AttrType.String() = %s, want %s", got, tt.expected)
		}
	}
}

// Test all supported types per the spec
func TestAllSupportedTypesFromSpec(t *testing.T) {
	// From spec: "Supported types: string, int(i64), uint(u64), float(f64), uuid(16B), datetime(ms epoch), bool and array variants"

	// Test string type
	t.Run("string", func(t *testing.T) {
		typ, err := InferType("hello")
		if err != nil || typ != TypeString {
			t.Errorf("string type not supported correctly")
		}
	})

	// Test int (i64) type
	t.Run("int_i64", func(t *testing.T) {
		typ, err := InferType(int64(-42))
		if err != nil || typ != TypeInt {
			t.Errorf("int (i64) type not supported correctly")
		}
	})

	// Test uint (u64) type
	t.Run("uint_u64", func(t *testing.T) {
		typ, err := InferType(uint64(42))
		if err != nil || typ != TypeUint {
			t.Errorf("uint (u64) type not supported correctly")
		}
	})

	// Test float (f64) type
	t.Run("float_f64", func(t *testing.T) {
		typ, err := InferType(float64(3.14))
		if err != nil || typ != TypeFloat {
			t.Errorf("float (f64) type not supported correctly")
		}
	})

	// Test uuid (16B) type
	t.Run("uuid_16b", func(t *testing.T) {
		typ, err := InferType(uuid.New())
		if err != nil || typ != TypeUUID {
			t.Errorf("uuid (16B) type not supported correctly")
		}
	})

	// Test datetime (ms epoch) type
	t.Run("datetime_ms_epoch", func(t *testing.T) {
		typ, err := InferType(time.Now())
		if err != nil || typ != TypeDatetime {
			t.Errorf("datetime type not supported correctly")
		}
	})

	// Test bool type
	t.Run("bool", func(t *testing.T) {
		typ, err := InferType(true)
		if err != nil || typ != TypeBool {
			t.Errorf("bool type not supported correctly")
		}
	})

	// Test array variants
	t.Run("array_string", func(t *testing.T) {
		typ, err := InferType([]string{"a", "b"})
		if err != nil || typ != TypeStringArray {
			t.Errorf("[]string type not supported correctly")
		}
	})

	t.Run("array_int", func(t *testing.T) {
		typ, err := InferType([]int64{1, 2})
		if err != nil || typ != TypeIntArray {
			t.Errorf("[]int type not supported correctly")
		}
	})

	t.Run("array_uint", func(t *testing.T) {
		typ, err := InferType([]uint64{1, 2})
		if err != nil || typ != TypeUintArray {
			t.Errorf("[]uint type not supported correctly")
		}
	})

	t.Run("array_float", func(t *testing.T) {
		typ, err := InferType([]float64{1.1, 2.2})
		if err != nil || typ != TypeFloatArray {
			t.Errorf("[]float type not supported correctly")
		}
	})

	t.Run("array_uuid", func(t *testing.T) {
		typ, err := InferType([]uuid.UUID{uuid.New()})
		if err != nil || typ != TypeUUIDArray {
			t.Errorf("[]uuid type not supported correctly")
		}
	})

	t.Run("array_datetime", func(t *testing.T) {
		typ, err := InferType([]time.Time{time.Now()})
		if err != nil || typ != TypeDatetimeArray {
			t.Errorf("[]datetime type not supported correctly")
		}
	})

	t.Run("array_bool", func(t *testing.T) {
		typ, err := InferType([]bool{true, false})
		if err != nil || typ != TypeBoolArray {
			t.Errorf("[]bool type not supported correctly")
		}
	})
}

// Test that InferTypeFromJSON correctly detects types from JSON-like values
func TestInferTypeFromJSONDetection(t *testing.T) {
	// Test detection of positive integers from JSON number
	t.Run("positive_int_from_json", func(t *testing.T) {
		typ, err := InferTypeFromJSON(float64(100))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if typ != TypeUint {
			t.Errorf("expected TypeUint, got %s", typ)
		}
	})

	// Test detection of negative integers from JSON number
	t.Run("negative_int_from_json", func(t *testing.T) {
		typ, err := InferTypeFromJSON(float64(-100))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if typ != TypeInt {
			t.Errorf("expected TypeInt, got %s", typ)
		}
	})

	// Test detection of floats from JSON number
	t.Run("float_from_json", func(t *testing.T) {
		typ, err := InferTypeFromJSON(float64(3.14159))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if typ != TypeFloat {
			t.Errorf("expected TypeFloat, got %s", typ)
		}
	})
}

func TestInferTypeNilError(t *testing.T) {
	_, err := InferType(nil)
	if !errors.Is(err, ErrCannotInferNil) {
		t.Errorf("expected ErrCannotInferNil, got %v", err)
	}
}

func TestInferTypeEmptyArrayError(t *testing.T) {
	_, err := InferType([]any{})
	if !errors.Is(err, ErrCannotInferEmptyArray) {
		t.Errorf("expected ErrCannotInferEmptyArray, got %v", err)
	}
}
