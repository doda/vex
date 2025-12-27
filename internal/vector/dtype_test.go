package vector

import (
	"testing"
)

func TestDType_IsValid(t *testing.T) {
	tests := []struct {
		name   string
		dtype  DType
		expect bool
	}{
		{"f16 is valid", DTypeF16, true},
		{"f32 is valid", DTypeF32, true},
		{"empty is invalid", DType(""), false},
		{"f64 is invalid", DType("f64"), false},
		{"float is invalid", DType("float"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.dtype.IsValid(); got != tt.expect {
				t.Errorf("DType(%q).IsValid() = %v, want %v", tt.dtype, got, tt.expect)
			}
		})
	}
}

func TestDType_BytesPerElement(t *testing.T) {
	tests := []struct {
		dtype  DType
		expect int
	}{
		{DTypeF16, 2},
		{DTypeF32, 4},
		{DType("f64"), 0},
		{DType(""), 0},
	}

	for _, tt := range tests {
		t.Run(string(tt.dtype), func(t *testing.T) {
			if got := tt.dtype.BytesPerElement(); got != tt.expect {
				t.Errorf("DType(%q).BytesPerElement() = %v, want %v", tt.dtype, got, tt.expect)
			}
		})
	}
}

func TestParseVectorType(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantDims  int
		wantDType DType
		wantErr   bool
	}{
		{"valid f32 1536 dims", "[1536]f32", 1536, DTypeF32, false},
		{"valid f16 768 dims", "[768]f16", 768, DTypeF16, false},
		{"valid f32 3 dims", "[3]f32", 3, DTypeF32, false},
		{"invalid format no brackets", "1536f32", 0, "", true},
		{"invalid format missing dtype", "[1536]", 0, "", true},
		{"invalid dtype f64", "[1536]f64", 0, "", true},
		{"zero dimensions", "[0]f32", 0, "", true},
		{"negative dimensions", "[-1]f32", 0, "", true},
		{"empty string", "", 0, "", true},
		{"just brackets", "[]f32", 0, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dims, dtype, err := ParseVectorType(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseVectorType(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if err == nil {
				if dims != tt.wantDims {
					t.Errorf("ParseVectorType(%q) dims = %v, want %v", tt.input, dims, tt.wantDims)
				}
				if dtype != tt.wantDType {
					t.Errorf("ParseVectorType(%q) dtype = %v, want %v", tt.input, dtype, tt.wantDType)
				}
			}
		})
	}
}

func TestFormatVectorType(t *testing.T) {
	tests := []struct {
		dims   int
		dtype  DType
		expect string
	}{
		{1536, DTypeF32, "[1536]f32"},
		{768, DTypeF16, "[768]f16"},
		{3, DTypeF32, "[3]f32"},
	}

	for _, tt := range tests {
		t.Run(tt.expect, func(t *testing.T) {
			got := FormatVectorType(tt.dims, tt.dtype)
			if got != tt.expect {
				t.Errorf("FormatVectorType(%d, %q) = %q, want %q", tt.dims, tt.dtype, got, tt.expect)
			}
		})
	}
}

func TestParseFormatRoundtrip(t *testing.T) {
	testCases := []string{
		"[1536]f32",
		"[768]f16",
		"[3]f32",
		"[4096]f16",
	}

	for _, input := range testCases {
		t.Run(input, func(t *testing.T) {
			dims, dtype, err := ParseVectorType(input)
			if err != nil {
				t.Fatalf("ParseVectorType(%q) failed: %v", input, err)
			}

			output := FormatVectorType(dims, dtype)
			if output != input {
				t.Errorf("roundtrip failed: input=%q, output=%q", input, output)
			}
		})
	}
}
