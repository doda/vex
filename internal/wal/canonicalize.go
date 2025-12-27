package wal

import (
	"encoding/base64"
	"encoding/binary"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vexsearch/vex/internal/document"
)

type Canonicalizer struct{}

func NewCanonicalizer() *Canonicalizer {
	return &Canonicalizer{}
}

func (c *Canonicalizer) CanonicalizeID(v any) (*DocumentID, error) {
	id, err := document.ParseID(v)
	if err != nil {
		return nil, err
	}
	return DocumentIDFromID(id), nil
}

func (c *Canonicalizer) CanonicalizeString(s string) (string, error) {
	if !utf8.ValidString(s) {
		return "", ErrInvalidUTF8
	}
	return s, nil
}

func (c *Canonicalizer) CanonicalizeAttributes(attrs map[string]any) (map[string]*AttributeValue, error) {
	result := make(map[string]*AttributeValue, len(attrs))

	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if err := ValidateString(key); err != nil {
			return nil, err
		}

		val, err := c.CanonicalizeValue(attrs[key])
		if err != nil {
			return nil, err
		}
		result[key] = val
	}

	return result, nil
}

func (c *Canonicalizer) CanonicalizeValue(v any) (*AttributeValue, error) {
	if v == nil {
		return NullValue(), nil
	}

	switch val := v.(type) {
	case string:
		if err := ValidateString(val); err != nil {
			return nil, err
		}
		return StringValue(val), nil

	case int:
		return IntValue(int64(val)), nil
	case int8:
		return IntValue(int64(val)), nil
	case int16:
		return IntValue(int64(val)), nil
	case int32:
		return IntValue(int64(val)), nil
	case int64:
		return IntValue(val), nil

	case uint:
		return UintValue(uint64(val)), nil
	case uint8:
		return UintValue(uint64(val)), nil
	case uint16:
		return UintValue(uint64(val)), nil
	case uint32:
		return UintValue(uint64(val)), nil
	case uint64:
		return UintValue(val), nil

	case float32:
		return FloatValue(float64(val)), nil
	case float64:
		return FloatValue(val), nil

	case bool:
		return BoolValue(val), nil

	case uuid.UUID:
		return UuidValue(val), nil

	case time.Time:
		return DatetimeValue(val.UnixMilli()), nil

	case []string:
		for _, s := range val {
			if err := ValidateString(s); err != nil {
				return nil, err
			}
		}
		return StringArrayValue(val), nil

	case []int64:
		return IntArrayValue(val), nil

	case []uint64:
		return UintArrayValue(val), nil

	case []float64:
		return FloatArrayValue(val), nil

	case []bool:
		return BoolArrayValue(val), nil

	case []any:
		return c.canonicalizeSlice(val)

	default:
		return nil, ErrInvalidAttributeType
	}
}

func (c *Canonicalizer) canonicalizeSlice(slice []any) (*AttributeValue, error) {
	if len(slice) == 0 {
		return StringArrayValue(nil), nil
	}

	switch slice[0].(type) {
	case string:
		strs := make([]string, len(slice))
		for i, v := range slice {
			s, ok := v.(string)
			if !ok {
				return nil, ErrMixedArrayTypes
			}
			if err := ValidateString(s); err != nil {
				return nil, err
			}
			strs[i] = s
		}
		return StringArrayValue(strs), nil

	case int, int8, int16, int32, int64:
		ints := make([]int64, len(slice))
		for i, v := range slice {
			switch n := v.(type) {
			case int:
				ints[i] = int64(n)
			case int8:
				ints[i] = int64(n)
			case int16:
				ints[i] = int64(n)
			case int32:
				ints[i] = int64(n)
			case int64:
				ints[i] = n
			case float64:
				if n != float64(int64(n)) {
					return nil, ErrMixedArrayTypes
				}
				ints[i] = int64(n)
			default:
				return nil, ErrMixedArrayTypes
			}
		}
		return IntArrayValue(ints), nil

	case float64:
		floats := make([]float64, len(slice))
		for i, v := range slice {
			f, ok := v.(float64)
			if !ok {
				return nil, ErrMixedArrayTypes
			}
			floats[i] = f
		}
		return FloatArrayValue(floats), nil

	case bool:
		bools := make([]bool, len(slice))
		for i, v := range slice {
			b, ok := v.(bool)
			if !ok {
				return nil, ErrMixedArrayTypes
			}
			bools[i] = b
		}
		return BoolArrayValue(bools), nil

	default:
		return nil, ErrMixedArrayTypes
	}
}

func (c *Canonicalizer) CanonicalizeDatetime(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case float64:
		return int64(val), nil
	case time.Time:
		return val.UnixMilli(), nil
	case string:
		return ParseDatetime(val)
	default:
		return 0, ErrInvalidDatetime
	}
}

func ParseDatetime(s string) (int64, error) {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UnixMilli(), nil
		}
	}

	return 0, ErrInvalidDatetime
}

func (c *Canonicalizer) CanonicalizeVector(v any) ([]byte, uint32, error) {
	switch val := v.(type) {
	case []float32:
		return encodeFloat32Vector(val), uint32(len(val)), nil
	case []float64:
		f32 := make([]float32, len(val))
		for i, f := range val {
			f32[i] = float32(f)
		}
		return encodeFloat32Vector(f32), uint32(len(val)), nil
	case []any:
		f32 := make([]float32, len(val))
		for i, x := range val {
			switch n := x.(type) {
			case float64:
				f32[i] = float32(n)
			case float32:
				f32[i] = n
			default:
				return nil, 0, ErrInvalidVectorType
			}
		}
		return encodeFloat32Vector(f32), uint32(len(val)), nil
	case string:
		return decodeBase64Vector(val)
	default:
		return nil, 0, ErrInvalidVectorType
	}
}

func encodeFloat32Vector(v []float32) []byte {
	data := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(f))
	}
	return data
}

func decodeBase64Vector(s string) ([]byte, uint32, error) {
	s = strings.TrimSpace(s)

	var data []byte
	var err error

	if strings.ContainsAny(s, "+/") {
		data, err = base64.StdEncoding.DecodeString(s)
	} else {
		data, err = base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
		if err != nil {
			data, err = base64.StdEncoding.DecodeString(s)
		}
	}

	if err != nil {
		return nil, 0, ErrInvalidVectorType
	}

	if len(data)%4 != 0 {
		return nil, 0, ErrInvalidVectorType
	}

	dims := uint32(len(data) / 4)
	return data, dims, nil
}

func DecodeFloat32Vector(data []byte) []float32 {
	if len(data)%4 != 0 {
		return nil
	}
	result := make([]float32, len(data)/4)
	for i := range result {
		result[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[i*4:]))
	}
	return result
}
