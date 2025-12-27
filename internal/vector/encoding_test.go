package vector

import (
	"encoding/base64"
	"encoding/binary"
	"math"
	"testing"
)

func TestDecodeVector_FloatArray(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		dims    int
		expect  []float32
		wantErr bool
	}{
		{
			name:   "float64 array",
			input:  []float64{1.0, 2.0, 3.0},
			dims:   3,
			expect: []float32{1.0, 2.0, 3.0},
		},
		{
			name:   "float32 array",
			input:  []float32{1.5, 2.5, 3.5},
			dims:   3,
			expect: []float32{1.5, 2.5, 3.5},
		},
		{
			name:   "any array with float64",
			input:  []any{1.0, 2.0, 3.0},
			dims:   3,
			expect: []float32{1.0, 2.0, 3.0},
		},
		{
			name:   "any array with int",
			input:  []any{1, 2, 3},
			dims:   3,
			expect: []float32{1.0, 2.0, 3.0},
		},
		{
			name:   "dimension check disabled",
			input:  []float64{1.0, 2.0},
			dims:   0, // 0 means no check
			expect: []float32{1.0, 2.0},
		},
		{
			name:    "dimension mismatch",
			input:   []float64{1.0, 2.0},
			dims:    3,
			wantErr: true,
		},
		{
			name:    "invalid type in any array",
			input:   []any{1.0, "hello", 3.0},
			dims:    3,
			wantErr: true,
		},
		{
			name:    "unsupported input type",
			input:   "not a vector",
			dims:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeVector(tt.input, tt.dims)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeVector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if len(got) != len(tt.expect) {
					t.Errorf("DecodeVector() length = %d, want %d", len(got), len(tt.expect))
					return
				}
				for i, v := range got {
					if v != tt.expect[i] {
						t.Errorf("DecodeVector()[%d] = %v, want %v", i, v, tt.expect[i])
					}
				}
			}
		})
	}
}

func TestDecodeVector_Base64(t *testing.T) {
	// Create a test vector and encode it as base64
	testVec := []float32{1.0, 2.5, -3.14159, 0.0}
	data := make([]byte, len(testVec)*4)
	for i, f := range testVec {
		bits := math.Float32bits(f)
		binary.LittleEndian.PutUint32(data[i*4:(i+1)*4], bits)
	}
	b64 := base64.StdEncoding.EncodeToString(data)

	tests := []struct {
		name    string
		input   string
		dims    int
		expect  []float32
		wantErr bool
	}{
		{
			name:   "valid base64 vector",
			input:  b64,
			dims:   4,
			expect: testVec,
		},
		{
			name:   "valid base64 no dim check",
			input:  b64,
			dims:   0,
			expect: testVec,
		},
		{
			name:    "dimension mismatch",
			input:   b64,
			dims:    3,
			wantErr: true,
		},
		{
			name:    "invalid base64",
			input:   "not valid base64!!!",
			dims:    0,
			wantErr: true,
		},
		{
			name:    "base64 with wrong length",
			input:   base64.StdEncoding.EncodeToString([]byte{1, 2, 3}), // 3 bytes, not multiple of 4
			dims:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeVector(tt.input, tt.dims)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeVector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if len(got) != len(tt.expect) {
					t.Errorf("DecodeVector() length = %d, want %d", len(got), len(tt.expect))
					return
				}
				for i, v := range got {
					if v != tt.expect[i] {
						t.Errorf("DecodeVector()[%d] = %v, want %v", i, v, tt.expect[i])
					}
				}
			}
		})
	}
}

func TestDecodeVectorWithEncoding(t *testing.T) {
	// Create test vector
	testVec := []float32{1.0, 2.0, 3.0}
	b64 := EncodeVectorBase64(testVec)

	t.Run("base64 encoding with string input", func(t *testing.T) {
		got, err := DecodeVectorWithEncoding(b64, 3, EncodingBase64)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range got {
			if v != testVec[i] {
				t.Errorf("element %d = %v, want %v", i, v, testVec[i])
			}
		}
	})

	t.Run("base64 encoding requires string", func(t *testing.T) {
		_, err := DecodeVectorWithEncoding([]float64{1.0, 2.0}, 2, EncodingBase64)
		if err == nil {
			t.Error("expected error for non-string input with base64 encoding")
		}
	})

	t.Run("float encoding with array input", func(t *testing.T) {
		got, err := DecodeVectorWithEncoding([]float64{1.0, 2.0, 3.0}, 3, EncodingFloat)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range got {
			if v != testVec[i] {
				t.Errorf("element %d = %v, want %v", i, v, testVec[i])
			}
		}
	})
}

func TestEncodeVectorBase64_Roundtrip(t *testing.T) {
	testCases := [][]float32{
		{1.0, 2.0, 3.0},
		{-1.5, 0.0, 1.5, 3.14159},
		{0.0},
		{math.MaxFloat32, -math.MaxFloat32},
		{1e-10, 1e10},
	}

	for i, input := range testCases {
		t.Run("", func(t *testing.T) {
			encoded := EncodeVectorBase64(input)
			decoded, err := DecodeVector(encoded, len(input))
			if err != nil {
				t.Fatalf("case %d: DecodeVector failed: %v", i, err)
			}
			if len(decoded) != len(input) {
				t.Fatalf("case %d: length mismatch: got %d, want %d", i, len(decoded), len(input))
			}
			for j, v := range decoded {
				if v != input[j] {
					t.Errorf("case %d, element %d: got %v, want %v", i, j, v, input[j])
				}
			}
		})
	}
}

func TestEncodeVectorFloat(t *testing.T) {
	input := []float32{1.0, 2.5, -3.14}
	result := EncodeVectorFloat(input)

	if len(result) != len(input) {
		t.Errorf("length mismatch: got %d, want %d", len(result), len(input))
	}
	for i, v := range result {
		if float32(v) != input[i] {
			t.Errorf("element %d: got %v, want %v", i, v, input[i])
		}
	}
}

func TestEncoding_IsValid(t *testing.T) {
	tests := []struct {
		encoding Encoding
		expect   bool
	}{
		{EncodingFloat, true},
		{EncodingBase64, true},
		{Encoding(""), false},
		{Encoding("binary"), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.encoding), func(t *testing.T) {
			if got := tt.encoding.IsValid(); got != tt.expect {
				t.Errorf("Encoding(%q).IsValid() = %v, want %v", tt.encoding, got, tt.expect)
			}
		})
	}
}
