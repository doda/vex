package filter

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

// valuesEqual compares two values for equality.
// Handles type coercion for numeric types and special types.
func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Try direct comparison first
	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	case uuid.UUID:
		switch bv := b.(type) {
		case uuid.UUID:
			return av == bv
		case string:
			parsed, err := uuid.Parse(bv)
			return err == nil && av == parsed
		}
	case time.Time:
		switch bv := b.(type) {
		case time.Time:
			return av.Equal(bv)
		case string:
			parsed, err := time.Parse(time.RFC3339Nano, bv)
			if err != nil {
				parsed, err = time.Parse(time.RFC3339, bv)
			}
			return err == nil && av.Equal(parsed)
		}
	}

	// Numeric comparison with type coercion
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		return aNum == bNum
	}

	// String comparison for UUID stored as string
	if aStr, ok := a.(string); ok {
		if bv, ok := b.(uuid.UUID); ok {
			return aStr == bv.String()
		}
	}

	return false
}

// compareValues compares two values.
// Returns -1 if a < b, 0 if a == b, 1 if a > b, and ok=false if not comparable.
// For strings, uses lexicographic comparison.
// For datetimes, compares as numeric milliseconds.
func compareValues(a, b any) (int, bool) {
	if a == nil || b == nil {
		return 0, false
	}

	// String comparison (lexicographic)
	if aStr, ok := a.(string); ok {
		if bStr, ok := b.(string); ok {
			return strings.Compare(aStr, bStr), true
		}
	}

	// Time comparison
	if aTime, ok := a.(time.Time); ok {
		var bTime time.Time
		switch bv := b.(type) {
		case time.Time:
			bTime = bv
		case string:
			var err error
			bTime, err = time.Parse(time.RFC3339Nano, bv)
			if err != nil {
				bTime, err = time.Parse(time.RFC3339, bv)
				if err != nil {
					return 0, false
				}
			}
		case float64:
			bTime = time.UnixMilli(int64(bv))
		case int64:
			bTime = time.UnixMilli(bv)
		case int:
			bTime = time.UnixMilli(int64(bv))
		default:
			return 0, false
		}
		if aTime.Before(bTime) {
			return -1, true
		}
		if aTime.After(bTime) {
			return 1, true
		}
		return 0, true
	}

	// Numeric comparison
	aNum, aOk := toFloat64(a)
	bNum, bOk := toFloat64(b)
	if aOk && bOk {
		if aNum < bNum {
			return -1, true
		}
		if aNum > bNum {
			return 1, true
		}
		return 0, true
	}

	// Bool comparison (false < true)
	if aBool, ok := a.(bool); ok {
		if bBool, ok := b.(bool); ok {
			if aBool == bBool {
				return 0, true
			}
			if !aBool && bBool {
				return -1, true
			}
			return 1, true
		}
	}

	return 0, false
}

// toFloat64 converts a value to float64 if possible.
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	default:
		return 0, false
	}
}

// toSlice converts a value to a slice if possible.
func toSlice(v any) []any {
	if v == nil {
		return nil
	}

	switch arr := v.(type) {
	case []any:
		return arr
	case []string:
		result := make([]any, len(arr))
		for i, s := range arr {
			result[i] = s
		}
		return result
	case []int:
		result := make([]any, len(arr))
		for i, n := range arr {
			result[i] = n
		}
		return result
	case []int64:
		result := make([]any, len(arr))
		for i, n := range arr {
			result[i] = n
		}
		return result
	case []float64:
		result := make([]any, len(arr))
		for i, n := range arr {
			result[i] = n
		}
		return result
	case []bool:
		result := make([]any, len(arr))
		for i, b := range arr {
			result[i] = b
		}
		return result
	case []uuid.UUID:
		result := make([]any, len(arr))
		for i, u := range arr {
			result[i] = u
		}
		return result
	default:
		return nil
	}
}
