package vector

import (
	"encoding/base64"
	"encoding/binary"
	"math"
	"testing"
)

// TestBase64Float32LittleEndianRoundtrip verifies the base64 encoding uses
// little-endian byte order for float32 values.
func TestBase64Float32LittleEndianRoundtrip(t *testing.T) {
	t.Run("explicit little-endian byte order", func(t *testing.T) {
		// 1.0 as float32: bits = 0x3F800000
		// Little-endian: [0x00, 0x00, 0x80, 0x3F]
		vec := []float32{1.0}
		encoded := EncodeVectorBase64(vec)

		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("base64 decode failed: %v", err)
		}

		if len(raw) != 4 {
			t.Fatalf("expected 4 bytes, got %d", len(raw))
		}

		// Verify little-endian byte order
		expectedBytes := []byte{0x00, 0x00, 0x80, 0x3F}
		for i, b := range expectedBytes {
			if raw[i] != b {
				t.Errorf("byte %d: got 0x%02X, want 0x%02X", i, raw[i], b)
			}
		}
	})

	t.Run("roundtrip with known values", func(t *testing.T) {
		testCases := []struct {
			name string
			vec  []float32
		}{
			{"zero", []float32{0.0}},
			{"one", []float32{1.0}},
			{"negative one", []float32{-1.0}},
			{"pi", []float32{3.14159265}},
			{"e", []float32{2.71828182}},
			{"small", []float32{1e-38}},
			{"large", []float32{1e38}},
			{"max float32", []float32{math.MaxFloat32}},
			{"smallest positive", []float32{math.SmallestNonzeroFloat32}},
			{"negative max", []float32{-math.MaxFloat32}},
			{"multi dim", []float32{1.0, 2.0, 3.0, 4.0, 5.0}},
			{"mixed signs", []float32{-1.5, 0.0, 1.5, -2.5, 2.5}},
			{"high dimensions", make128DimVector()},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				encoded := EncodeVectorBase64(tc.vec)
				decoded, err := DecodeVector(encoded, len(tc.vec))
				if err != nil {
					t.Fatalf("decode failed: %v", err)
				}

				if len(decoded) != len(tc.vec) {
					t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(tc.vec))
				}

				for i, v := range decoded {
					if v != tc.vec[i] {
						t.Errorf("element %d: got %v, want %v", i, v, tc.vec[i])
					}
				}
			})
		}
	})

	t.Run("special float values", func(t *testing.T) {
		// Positive infinity
		posInf := float32(math.Inf(1))
		encoded := EncodeVectorBase64([]float32{posInf})
		decoded, err := DecodeVector(encoded, 1)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if !math.IsInf(float64(decoded[0]), 1) {
			t.Errorf("expected +Inf, got %v", decoded[0])
		}

		// Negative infinity
		negInf := float32(math.Inf(-1))
		encoded = EncodeVectorBase64([]float32{negInf})
		decoded, err = DecodeVector(encoded, 1)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if !math.IsInf(float64(decoded[0]), -1) {
			t.Errorf("expected -Inf, got %v", decoded[0])
		}

		// NaN
		nan := float32(math.NaN())
		encoded = EncodeVectorBase64([]float32{nan})
		decoded, err = DecodeVector(encoded, 1)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if !math.IsNaN(float64(decoded[0])) {
			t.Errorf("expected NaN, got %v", decoded[0])
		}

		// Negative zero
		negZero := float32(math.Copysign(0, -1))
		encoded = EncodeVectorBase64([]float32{negZero})
		decoded, err = DecodeVector(encoded, 1)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if math.Signbit(float64(decoded[0])) != true || decoded[0] != 0 {
			t.Errorf("expected -0, got %v", decoded[0])
		}
	})

	t.Run("byte order verification for multi-byte values", func(t *testing.T) {
		// 0x12345678 as float32 bits
		bits := uint32(0x12345678)
		vec := []float32{math.Float32frombits(bits)}
		encoded := EncodeVectorBase64(vec)

		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("base64 decode failed: %v", err)
		}

		// Little-endian: least significant byte first
		expectedBytes := []byte{0x78, 0x56, 0x34, 0x12}
		for i, b := range expectedBytes {
			if raw[i] != b {
				t.Errorf("byte %d: got 0x%02X, want 0x%02X", i, raw[i], b)
			}
		}
	})

	t.Run("decode manually constructed base64", func(t *testing.T) {
		// Manually construct base64 for [1.0, 2.0]
		// 1.0 = 0x3F800000, 2.0 = 0x40000000
		data := make([]byte, 8)
		binary.LittleEndian.PutUint32(data[0:4], 0x3F800000)
		binary.LittleEndian.PutUint32(data[4:8], 0x40000000)
		b64 := base64.StdEncoding.EncodeToString(data)

		decoded, err := DecodeVector(b64, 2)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}

		if decoded[0] != 1.0 || decoded[1] != 2.0 {
			t.Errorf("got %v, want [1.0, 2.0]", decoded)
		}
	})

	t.Run("empty vector", func(t *testing.T) {
		vec := []float32{}
		encoded := EncodeVectorBase64(vec)
		decoded, err := DecodeVector(encoded, 0)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		if len(decoded) != 0 {
			t.Errorf("expected empty vector, got %v", decoded)
		}
	})
}

// TestFloatArrayEncodingDecoding verifies float array encoding and decoding.
func TestFloatArrayEncodingDecoding(t *testing.T) {
	t.Run("float64 to float32 conversion", func(t *testing.T) {
		input := []float64{1.0, 2.5, -3.14159, 0.0, 100.0}
		decoded, err := DecodeVector(input, len(input))
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}

		for i, v := range decoded {
			if v != float32(input[i]) {
				t.Errorf("element %d: got %v, want %v", i, v, float32(input[i]))
			}
		}
	})

	t.Run("float32 passthrough", func(t *testing.T) {
		input := []float32{1.0, 2.5, -3.14159, 0.0, 100.0}
		decoded, err := DecodeVector(input, len(input))
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}

		for i, v := range decoded {
			if v != input[i] {
				t.Errorf("element %d: got %v, want %v", i, v, input[i])
			}
		}
	})

	t.Run("any array with mixed number types", func(t *testing.T) {
		input := []any{float64(1.0), float32(2.0), int(3), int64(4)}
		decoded, err := DecodeVector(input, len(input))
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}

		expected := []float32{1.0, 2.0, 3.0, 4.0}
		for i, v := range decoded {
			if v != expected[i] {
				t.Errorf("element %d: got %v, want %v", i, v, expected[i])
			}
		}
	})

	t.Run("EncodeVectorFloat roundtrip", func(t *testing.T) {
		input := []float32{1.0, 2.5, -3.14159, 0.0}
		encoded := EncodeVectorFloat(input)

		// Verify length
		if len(encoded) != len(input) {
			t.Fatalf("length mismatch: got %d, want %d", len(encoded), len(input))
		}

		// Decode back
		decoded, err := DecodeVector(encoded, len(input))
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}

		for i, v := range decoded {
			if v != input[i] {
				t.Errorf("element %d: got %v, want %v", i, v, input[i])
			}
		}
	})

	t.Run("dimension validation", func(t *testing.T) {
		input := []float64{1.0, 2.0, 3.0}

		// Correct dimensions
		_, err := DecodeVector(input, 3)
		if err != nil {
			t.Errorf("unexpected error with correct dims: %v", err)
		}

		// Wrong dimensions
		_, err = DecodeVector(input, 5)
		if err == nil {
			t.Error("expected error with wrong dims")
		}

		// Zero dimensions (no check)
		_, err = DecodeVector(input, 0)
		if err != nil {
			t.Errorf("unexpected error with zero dims: %v", err)
		}
	})

	t.Run("precision preservation", func(t *testing.T) {
		// Values that are exactly representable in float32
		exactValues := []float32{
			0.5,
			0.25,
			0.125,
			1.0 / 1024.0,
			1.0 + 1.0/8388608.0, // 1 + smallest fraction
		}

		for _, v := range exactValues {
			// Roundtrip through float64
			encoded := EncodeVectorFloat([]float32{v})
			decoded, err := DecodeVector(encoded, 1)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}
			if decoded[0] != v {
				t.Errorf("precision lost: got %v, want %v", decoded[0], v)
			}
		}
	})

	t.Run("error cases", func(t *testing.T) {
		// Invalid base64
		_, err := DecodeVector("!!!invalid!!!", 1)
		if err == nil {
			t.Error("expected error for invalid base64")
		}

		// Wrong byte count
		data := []byte{1, 2, 3} // Not multiple of 4
		b64 := base64.StdEncoding.EncodeToString(data)
		_, err = DecodeVector(b64, 1)
		if err == nil {
			t.Error("expected error for wrong byte count")
		}

		// Non-numeric in any array
		_, err = DecodeVector([]any{"not a number"}, 1)
		if err == nil {
			t.Error("expected error for non-numeric element")
		}

		// Unsupported type
		_, err = DecodeVector(map[string]int{}, 0)
		if err == nil {
			t.Error("expected error for unsupported type")
		}
	})
}

// TestEncodingBidirectional verifies that base64 and float encodings
// produce equivalent results when decoded.
func TestEncodingBidirectional(t *testing.T) {
	testVectors := [][]float32{
		{0.0},
		{1.0, 2.0, 3.0},
		{-1.5, 0.0, 1.5},
		{math.MaxFloat32, -math.MaxFloat32},
		make128DimVector(),
	}

	for i, vec := range testVectors {
		t.Run("", func(t *testing.T) {
			// Encode as base64
			b64 := EncodeVectorBase64(vec)
			// Encode as float
			floatEnc := EncodeVectorFloat(vec)

			// Decode both
			fromB64, err := DecodeVector(b64, len(vec))
			if err != nil {
				t.Fatalf("case %d: base64 decode failed: %v", i, err)
			}
			fromFloat, err := DecodeVector(floatEnc, len(vec))
			if err != nil {
				t.Fatalf("case %d: float decode failed: %v", i, err)
			}

			// Compare
			for j := range vec {
				if fromB64[j] != fromFloat[j] {
					t.Errorf("case %d, element %d: base64=%v, float=%v",
						i, j, fromB64[j], fromFloat[j])
				}
				if fromB64[j] != vec[j] {
					t.Errorf("case %d, element %d: got %v, want %v",
						i, j, fromB64[j], vec[j])
				}
			}
		})
	}
}

// TestDecodeVectorWithEncodingHint verifies encoding hint behavior.
func TestDecodeVectorWithEncodingHint(t *testing.T) {
	testVec := []float32{1.0, 2.0, 3.0}
	b64 := EncodeVectorBase64(testVec)

	t.Run("base64 hint enforces string input", func(t *testing.T) {
		// Valid: string with base64 hint
		decoded, err := DecodeVectorWithEncoding(b64, 3, EncodingBase64)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range decoded {
			if v != testVec[i] {
				t.Errorf("element %d: got %v, want %v", i, v, testVec[i])
			}
		}

		// Invalid: array with base64 hint
		_, err = DecodeVectorWithEncoding([]float64{1.0, 2.0}, 2, EncodingBase64)
		if err == nil {
			t.Error("expected error for array input with base64 hint")
		}
	})

	t.Run("float hint accepts arrays", func(t *testing.T) {
		// float64 array
		decoded, err := DecodeVectorWithEncoding([]float64{1.0, 2.0, 3.0}, 3, EncodingFloat)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range decoded {
			if v != testVec[i] {
				t.Errorf("element %d: got %v, want %v", i, v, testVec[i])
			}
		}

		// float32 array
		decoded, err = DecodeVectorWithEncoding([]float32{1.0, 2.0, 3.0}, 3, EncodingFloat)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range decoded {
			if v != testVec[i] {
				t.Errorf("element %d: got %v, want %v", i, v, testVec[i])
			}
		}
	})

	t.Run("unknown encoding falls back to auto-detect", func(t *testing.T) {
		// String input auto-detected as base64
		decoded, err := DecodeVectorWithEncoding(b64, 3, Encoding("unknown"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range decoded {
			if v != testVec[i] {
				t.Errorf("element %d: got %v, want %v", i, v, testVec[i])
			}
		}

		// Array input auto-detected as float
		decoded, err = DecodeVectorWithEncoding([]float64{1.0, 2.0, 3.0}, 3, Encoding("unknown"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range decoded {
			if v != testVec[i] {
				t.Errorf("element %d: got %v, want %v", i, v, testVec[i])
			}
		}
	})
}

// make128DimVector creates a 128-dimensional test vector.
func make128DimVector() []float32 {
	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = float32(i) * 0.1
	}
	return vec
}
