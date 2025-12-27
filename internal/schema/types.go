// Package schema provides schema-related types and validation for Vex.
package schema

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// AttrType represents the type of an attribute in the schema.
// Supported types: string, int (i64), uint (u64), float (f64), uuid (16B), datetime (ms epoch), bool.
// Array variants are prefixed with "[]".
type AttrType string

const (
	// Scalar types
	TypeString   AttrType = "string"
	TypeInt      AttrType = "int"
	TypeUint     AttrType = "uint"
	TypeFloat    AttrType = "float"
	TypeUUID     AttrType = "uuid"
	TypeDatetime AttrType = "datetime"
	TypeBool     AttrType = "bool"

	// Array types
	TypeStringArray   AttrType = "[]string"
	TypeIntArray      AttrType = "[]int"
	TypeUintArray     AttrType = "[]uint"
	TypeFloatArray    AttrType = "[]float"
	TypeUUIDArray     AttrType = "[]uuid"
	TypeDatetimeArray AttrType = "[]datetime"
	TypeBoolArray     AttrType = "[]bool"
)

// IsValid returns true if the type is a recognized attribute type.
func (t AttrType) IsValid() bool {
	switch t {
	case TypeString, TypeInt, TypeUint, TypeFloat, TypeUUID, TypeDatetime, TypeBool,
		TypeStringArray, TypeIntArray, TypeUintArray, TypeFloatArray, TypeUUIDArray, TypeDatetimeArray, TypeBoolArray:
		return true
	default:
		return false
	}
}

// IsArray returns true if the type is an array type.
func (t AttrType) IsArray() bool {
	switch t {
	case TypeStringArray, TypeIntArray, TypeUintArray, TypeFloatArray, TypeUUIDArray, TypeDatetimeArray, TypeBoolArray:
		return true
	default:
		return false
	}
}

// ElementType returns the element type if this is an array type, or the type itself if scalar.
func (t AttrType) ElementType() AttrType {
	switch t {
	case TypeStringArray:
		return TypeString
	case TypeIntArray:
		return TypeInt
	case TypeUintArray:
		return TypeUint
	case TypeFloatArray:
		return TypeFloat
	case TypeUUIDArray:
		return TypeUUID
	case TypeDatetimeArray:
		return TypeDatetime
	case TypeBoolArray:
		return TypeBool
	default:
		return t
	}
}

// ArrayType returns the array variant of a scalar type. Returns the type unchanged if already an array.
func (t AttrType) ArrayType() AttrType {
	switch t {
	case TypeString:
		return TypeStringArray
	case TypeInt:
		return TypeIntArray
	case TypeUint:
		return TypeUintArray
	case TypeFloat:
		return TypeFloatArray
	case TypeUUID:
		return TypeUUIDArray
	case TypeDatetime:
		return TypeDatetimeArray
	case TypeBool:
		return TypeBoolArray
	default:
		return t
	}
}

// String returns the string representation of the type.
func (t AttrType) String() string {
	return string(t)
}

// InferType infers the AttrType from a Go value.
// Supported input types:
//   - string -> TypeString
//   - int, int64, int32, int16, int8 -> TypeInt
//   - uint, uint64, uint32, uint16, uint8 -> TypeUint
//   - float64, float32 -> TypeFloat
//   - bool -> TypeBool
//   - time.Time -> TypeDatetime
//   - uuid.UUID -> TypeUUID
//   - []T -> array variant of T's type
//
// Returns an error for unsupported types.
func InferType(v any) (AttrType, error) {
	if v == nil {
		return "", ErrCannotInferNil
	}

	switch val := v.(type) {
	case string:
		return TypeString, nil
	case int, int64, int32, int16, int8:
		return TypeInt, nil
	case uint, uint64, uint32, uint16, uint8:
		return TypeUint, nil
	case float64, float32:
		return TypeFloat, nil
	case bool:
		return TypeBool, nil
	case time.Time:
		return TypeDatetime, nil
	case uuid.UUID:
		return TypeUUID, nil

	// Handle []interface{} which is common from JSON
	case []any:
		if len(val) == 0 {
			return "", ErrCannotInferEmptyArray
		}
		elemType, err := InferType(val[0])
		if err != nil {
			return "", err
		}
		return elemType.ArrayType(), nil

	// Handle typed slices
	case []string:
		return TypeStringArray, nil
	case []int:
		return TypeIntArray, nil
	case []int64:
		return TypeIntArray, nil
	case []int32:
		return TypeIntArray, nil
	case []uint:
		return TypeUintArray, nil
	case []uint64:
		return TypeUintArray, nil
	case []uint32:
		return TypeUintArray, nil
	case []float64:
		return TypeFloatArray, nil
	case []float32:
		return TypeFloatArray, nil
	case []bool:
		return TypeBoolArray, nil
	case []time.Time:
		return TypeDatetimeArray, nil
	case []uuid.UUID:
		return TypeUUIDArray, nil

	default:
		return "", fmt.Errorf("%w: %T", ErrUnsupportedType, v)
	}
}

// InferTypeFromJSON infers the AttrType from a value as it would appear after JSON unmarshaling.
// JSON numbers may be float64 by default, so this function handles that case.
// It also handles special string formats like UUIDs and datetimes.
func InferTypeFromJSON(v any) (AttrType, error) {
	if v == nil {
		return "", ErrCannotInferNil
	}

	switch val := v.(type) {
	case string:
		// Check if it's a UUID
		if _, err := uuid.Parse(val); err == nil && len(val) >= 32 {
			return TypeUUID, nil
		}
		// Check if it's a datetime (ISO8601)
		if _, err := time.Parse(time.RFC3339, val); err == nil {
			return TypeDatetime, nil
		}
		if _, err := time.Parse(time.RFC3339Nano, val); err == nil {
			return TypeDatetime, nil
		}
		return TypeString, nil

	case float64:
		// JSON numbers are float64 by default
		// Check if it's actually an integer
		if val == float64(int64(val)) {
			if val >= 0 {
				return TypeUint, nil
			}
			return TypeInt, nil
		}
		return TypeFloat, nil

	case bool:
		return TypeBool, nil

	case []any:
		if len(val) == 0 {
			return "", ErrCannotInferEmptyArray
		}
		elemType, err := InferTypeFromJSON(val[0])
		if err != nil {
			return "", err
		}
		return elemType.ArrayType(), nil

	default:
		return InferType(v)
	}
}

// ValidateValue checks if a value is compatible with the given type.
// Returns nil if compatible, or an error describing the incompatibility.
func ValidateValue(t AttrType, v any) error {
	if v == nil {
		// null is valid for any type
		return nil
	}

	if !t.IsValid() {
		return fmt.Errorf("%w: %s", ErrInvalidType, t)
	}

	if t.IsArray() {
		return validateArrayValue(t, v)
	}
	return validateScalarValue(t.ElementType(), v)
}

func validateScalarValue(t AttrType, v any) error {
	switch t {
	case TypeString:
		if _, ok := v.(string); ok {
			return nil
		}
	case TypeInt:
		switch v.(type) {
		case int, int64, int32, int16, int8:
			return nil
		case float64:
			// Allow integers represented as float64 (from JSON)
			f := v.(float64)
			if f == float64(int64(f)) {
				return nil
			}
		}
	case TypeUint:
		switch val := v.(type) {
		case uint, uint64, uint32, uint16, uint8:
			return nil
		case int:
			// Allow non-negative ints as uint
			if val >= 0 {
				return nil
			}
		case int64:
			if val >= 0 {
				return nil
			}
		case int32:
			if val >= 0 {
				return nil
			}
		case int16:
			if val >= 0 {
				return nil
			}
		case int8:
			if val >= 0 {
				return nil
			}
		case float64:
			// Allow non-negative integers represented as float64 (from JSON)
			if val >= 0 && val == float64(uint64(val)) {
				return nil
			}
		}
	case TypeFloat:
		switch v.(type) {
		case float64, float32, int, int64, int32, uint, uint64, uint32:
			return nil
		}
	case TypeBool:
		if _, ok := v.(bool); ok {
			return nil
		}
	case TypeDatetime:
		switch val := v.(type) {
		case time.Time:
			return nil
		case string:
			if _, err := time.Parse(time.RFC3339, val); err == nil {
				return nil
			}
			if _, err := time.Parse(time.RFC3339Nano, val); err == nil {
				return nil
			}
		case int64, uint64, int, uint, float64:
			// Allow millisecond epoch
			return nil
		}
	case TypeUUID:
		switch val := v.(type) {
		case uuid.UUID:
			return nil
		case string:
			if _, err := uuid.Parse(val); err == nil {
				return nil
			}
		case []byte:
			if len(val) == 16 {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: expected %s, got %T", ErrTypeMismatch, t, v)
}

func validateArrayValue(t AttrType, v any) error {
	elemType := t.ElementType()

	switch arr := v.(type) {
	case []any:
		for i, elem := range arr {
			if err := validateScalarValue(elemType, elem); err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
		}
		return nil
	case []string:
		if elemType == TypeString {
			return nil
		}
	case []int, []int64, []int32:
		if elemType == TypeInt {
			return nil
		}
	case []uint, []uint64, []uint32:
		if elemType == TypeUint {
			return nil
		}
	case []float64, []float32:
		if elemType == TypeFloat {
			return nil
		}
	case []bool:
		if elemType == TypeBool {
			return nil
		}
	case []time.Time:
		if elemType == TypeDatetime {
			return nil
		}
	case []uuid.UUID:
		if elemType == TypeUUID {
			return nil
		}
	}
	return fmt.Errorf("%w: expected %s, got %T", ErrTypeMismatch, t, v)
}

// TypesCompatible checks if two types are compatible (same base type).
// Returns true if oldType and newType are the same.
func TypesCompatible(oldType, newType AttrType) bool {
	return oldType == newType
}
