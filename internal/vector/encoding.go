package vector

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
)

// Encoding represents the encoding format for vectors.
type Encoding string

const (
	// EncodingFloat represents vectors as arrays of float numbers (JSON).
	EncodingFloat Encoding = "float"
	// EncodingBase64 represents vectors as base64-encoded binary data.
	EncodingBase64 Encoding = "base64"
)

// IsValid returns true if the encoding is a recognized format.
func (e Encoding) IsValid() bool {
	switch e {
	case EncodingFloat, EncodingBase64:
		return true
	default:
		return false
	}
}

// DefaultEncoding is the default encoding when not specified.
const DefaultEncoding = EncodingFloat

// DecodeVector decodes a vector from its encoded representation.
// The input can be:
//   - []float64, []float32, []any (float array)
//   - string (base64-encoded little-endian float32)
//
// Returns the vector as []float32.
func DecodeVector(input any, expectedDims int) ([]float32, error) {
	switch v := input.(type) {
	case []float64:
		return decodeFloat64Array(v, expectedDims)
	case []float32:
		return decodeFloat32Array(v, expectedDims)
	case []any:
		return decodeAnyArray(v, expectedDims)
	case string:
		return decodeBase64(v, expectedDims)
	default:
		return nil, fmt.Errorf("unsupported vector type: %T", input)
	}
}

// DecodeVectorWithEncoding decodes a vector using the specified encoding hint.
func DecodeVectorWithEncoding(input any, expectedDims int, encoding Encoding) ([]float32, error) {
	switch encoding {
	case EncodingBase64:
		s, ok := input.(string)
		if !ok {
			return nil, fmt.Errorf("base64 encoding requires string input, got %T", input)
		}
		return decodeBase64(s, expectedDims)
	case EncodingFloat:
		return DecodeVector(input, expectedDims)
	default:
		return DecodeVector(input, expectedDims)
	}
}

func decodeFloat64Array(v []float64, expectedDims int) ([]float32, error) {
	if expectedDims > 0 && len(v) != expectedDims {
		return nil, fmt.Errorf("expected %d dimensions, got %d", expectedDims, len(v))
	}
	result := make([]float32, len(v))
	for i, f := range v {
		result[i] = float32(f)
	}
	return result, nil
}

func decodeFloat32Array(v []float32, expectedDims int) ([]float32, error) {
	if expectedDims > 0 && len(v) != expectedDims {
		return nil, fmt.Errorf("expected %d dimensions, got %d", expectedDims, len(v))
	}
	result := make([]float32, len(v))
	copy(result, v)
	return result, nil
}

func decodeAnyArray(v []any, expectedDims int) ([]float32, error) {
	if expectedDims > 0 && len(v) != expectedDims {
		return nil, fmt.Errorf("expected %d dimensions, got %d", expectedDims, len(v))
	}
	result := make([]float32, len(v))
	for i, elem := range v {
		switch f := elem.(type) {
		case float64:
			result[i] = float32(f)
		case float32:
			result[i] = f
		case int:
			result[i] = float32(f)
		case int64:
			result[i] = float32(f)
		default:
			return nil, fmt.Errorf("element %d: expected number, got %T", i, elem)
		}
	}
	return result, nil
}

func decodeBase64(s string, expectedDims int) ([]float32, error) {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 encoding: %w", err)
	}

	if len(data)%4 != 0 {
		return nil, fmt.Errorf("base64 data length must be multiple of 4 (float32), got %d bytes", len(data))
	}

	numFloats := len(data) / 4
	if expectedDims > 0 && numFloats != expectedDims {
		return nil, fmt.Errorf("expected %d dimensions, got %d", expectedDims, numFloats)
	}

	result := make([]float32, numFloats)
	for i := 0; i < numFloats; i++ {
		bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		result[i] = math.Float32frombits(bits)
	}
	return result, nil
}

// EncodeVectorBase64 encodes a vector as base64 little-endian float32.
func EncodeVectorBase64(v []float32) string {
	data := make([]byte, len(v)*4)
	for i, f := range v {
		bits := math.Float32bits(f)
		binary.LittleEndian.PutUint32(data[i*4:(i+1)*4], bits)
	}
	return base64.StdEncoding.EncodeToString(data)
}

// EncodeVectorFloat encodes a vector as a float64 array for JSON serialization.
func EncodeVectorFloat(v []float32) []float64 {
	result := make([]float64, len(v))
	for i, f := range v {
		result[i] = float64(f)
	}
	return result
}
