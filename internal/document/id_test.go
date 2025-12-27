package document

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestParseU64IDs(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		wantU64 uint64
	}{
		{"uint64 zero", uint64(0), 0},
		{"uint64 max", uint64(18446744073709551615), 18446744073709551615},
		{"uint64 regular", uint64(12345), 12345},
		{"int64 zero", int64(0), 0},
		{"int64 positive", int64(12345), 12345},
		{"int zero", int(0), 0},
		{"int positive", int(12345), 12345},
		{"uint regular", uint(12345), 12345},
		{"float64 whole", float64(12345), 12345},
		{"float64 zero", float64(0), 0},
		{"string numeric", "12345", 12345},
		{"string zero", "0", 0},
		{"string large", "18446744073709551615", 18446744073709551615},
		{"json.Number", json.Number("12345"), 12345},
		{"json.Number max u64", json.Number("18446744073709551615"), 18446744073709551615},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := ParseID(tt.input)
			if err != nil {
				t.Fatalf("ParseID(%v) returned error: %v", tt.input, err)
			}
			if id.Type() != IDTypeU64 {
				t.Errorf("ParseID(%v).Type() = %v, want %v", tt.input, id.Type(), IDTypeU64)
			}
			if id.U64() != tt.wantU64 {
				t.Errorf("ParseID(%v).U64() = %v, want %v", tt.input, id.U64(), tt.wantU64)
			}
		})
	}
}

func TestParseUUIDIDs(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		wantUUID string
	}{
		{"uuid.UUID type", uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), "550e8400-e29b-41d4-a716-446655440000"},
		{"string UUID lowercase", "550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440000"},
		{"string UUID uppercase", "550E8400-E29B-41D4-A716-446655440000", "550e8400-e29b-41d4-a716-446655440000"},
		{"string UUID mixed", "550E8400-e29b-41D4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440000"},
		{"nil UUID", uuid.Nil, "00000000-0000-0000-0000-000000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := ParseID(tt.input)
			if err != nil {
				t.Fatalf("ParseID(%v) returned error: %v", tt.input, err)
			}
			if id.Type() != IDTypeUUID {
				t.Errorf("ParseID(%v).Type() = %v, want %v", tt.input, id.Type(), IDTypeUUID)
			}
			if id.UUID().String() != tt.wantUUID {
				t.Errorf("ParseID(%v).UUID() = %v, want %v", tt.input, id.UUID(), tt.wantUUID)
			}
		})
	}
}

func TestParseStringIDs(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantStr string
	}{
		{"simple string", "hello", "hello"},
		{"string with spaces", "hello world", "hello world"},
		{"string with special chars", "user@example.com", "user@example.com"},
		{"string 64 bytes", strings.Repeat("a", 64), strings.Repeat("a", 64)},
		{"alphanumeric", "abc123", "abc123"},
		{"with underscores", "user_123", "user_123"},
		{"with dashes", "user-123", "user-123"},
		{"uuid-like but invalid", "not-a-real-uuid-format", "not-a-real-uuid-format"},
		{"path-like", "foo/bar/baz", "foo/bar/baz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := ParseID(tt.input)
			if err != nil {
				t.Fatalf("ParseID(%v) returned error: %v", tt.input, err)
			}
			if id.Type() != IDTypeString {
				t.Errorf("ParseID(%v).Type() = %v, want %v", tt.input, id.Type(), IDTypeString)
			}
			if id.String() != tt.wantStr {
				t.Errorf("ParseID(%v).String() = %v, want %v", tt.input, id.String(), tt.wantStr)
			}
		})
	}
}

func TestStringIDUpTo64Bytes(t *testing.T) {
	// Test that string IDs up to exactly 64 bytes are accepted
	for length := 1; length <= 64; length++ {
		s := strings.Repeat("x", length)
		id, err := ParseID(s)
		if err != nil {
			t.Errorf("ParseID(string of %d bytes) returned error: %v", length, err)
			continue
		}
		if id.Type() != IDTypeString {
			t.Errorf("ParseID(string of %d bytes).Type() = %v, want %v", length, id.Type(), IDTypeString)
		}
		if id.String() != s {
			t.Errorf("ParseID(string of %d bytes).String() = %q, want %q", length, id.String(), s)
		}
	}
}

func TestStringIDOver64BytesRejected(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{"65 bytes", 65},
		{"100 bytes", 100},
		{"256 bytes", 256},
		{"1000 bytes", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := strings.Repeat("x", tt.length)
			_, err := ParseID(s)
			if err == nil {
				t.Fatalf("ParseID(string of %d bytes) should return error, got nil", tt.length)
			}
			valErr, ok := err.(*ValidationError)
			if !ok {
				t.Fatalf("expected *ValidationError, got %T", err)
			}
			if valErr.Field != "id" {
				t.Errorf("ValidationError.Field = %q, want %q", valErr.Field, "id")
			}
			if !strings.Contains(valErr.Message, "exceeds maximum") {
				t.Errorf("ValidationError.Message = %q, want to contain 'exceeds maximum'", valErr.Message)
			}
		})
	}
}

func TestNewStringIDOver64BytesRejected(t *testing.T) {
	s := strings.Repeat("a", 65)
	_, err := NewStringID(s)
	if err == nil {
		t.Fatal("NewStringID with 65 bytes should return error")
	}
	valErr, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if valErr.Field != "id" {
		t.Errorf("ValidationError.Field = %q, want %q", valErr.Field, "id")
	}
}

func TestIDNormalization(t *testing.T) {
	// Test that IDs are normalized to typed representation
	tests := []struct {
		name     string
		input    any
		wantType IDType
	}{
		// Numeric inputs normalize to u64
		{"uint64 to u64", uint64(42), IDTypeU64},
		{"int64 to u64", int64(42), IDTypeU64},
		{"float64 whole to u64", float64(42), IDTypeU64},
		{"json.Number to u64", json.Number("42"), IDTypeU64},
		{"string numeric to u64", "42", IDTypeU64},

		// UUID inputs normalize to UUID
		{"uuid.UUID to uuid", uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), IDTypeUUID},
		{"string uuid to uuid", "550e8400-e29b-41d4-a716-446655440000", IDTypeUUID},

		// Non-numeric, non-UUID strings normalize to string
		{"string to string", "hello", IDTypeString},
		{"email to string", "user@example.com", IDTypeString},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := ParseID(tt.input)
			if err != nil {
				t.Fatalf("ParseID(%v) returned error: %v", tt.input, err)
			}
			if id.Type() != tt.wantType {
				t.Errorf("ParseID(%v).Type() = %v, want %v", tt.input, id.Type(), tt.wantType)
			}
		})
	}
}

func TestInvalidIDs(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"negative int64", int64(-1)},
		{"negative int", int(-1)},
		{"negative float64", float64(-1)},
		{"fractional float64", float64(1.5)},
		{"empty string", ""},
		{"nil", nil},
		{"bool", true},
		{"slice", []string{"a"}},
		{"map", map[string]string{"a": "b"}},
		{"negative json.Number", json.Number("-1")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseID(tt.input)
			if err == nil {
				t.Errorf("ParseID(%v) should return error, got nil", tt.input)
			}
		})
	}
}

func TestIDJSONRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		id   ID
	}{
		{"u64", NewU64ID(12345)},
		{"uuid", NewUUIDID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"))},
		{"string", func() ID { id, _ := NewStringID("hello"); return id }()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.id)
			if err != nil {
				t.Fatalf("json.Marshal(%v) returned error: %v", tt.id, err)
			}

			var decoded ID
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("json.Unmarshal(%s) returned error: %v", data, err)
			}

			if !tt.id.Equal(decoded) {
				t.Errorf("roundtrip failed: original=%v, decoded=%v", tt.id, decoded)
			}
		})
	}
}

func TestIDEquality(t *testing.T) {
	tests := []struct {
		name  string
		a, b  ID
		equal bool
	}{
		{"u64 equal", NewU64ID(42), NewU64ID(42), true},
		{"u64 not equal", NewU64ID(42), NewU64ID(43), false},
		{"uuid equal", NewUUIDID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")), NewUUIDID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")), true},
		{"uuid not equal", NewUUIDID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")), NewUUIDID(uuid.MustParse("660e8400-e29b-41d4-a716-446655440000")), false},
		{"string equal", func() ID { id, _ := NewStringID("hello"); return id }(), func() ID { id, _ := NewStringID("hello"); return id }(), true},
		{"string not equal", func() ID { id, _ := NewStringID("hello"); return id }(), func() ID { id, _ := NewStringID("world"); return id }(), false},
		{"different types u64 vs uuid", NewU64ID(42), NewUUIDID(uuid.Nil), false},
		{"different types u64 vs string", NewU64ID(42), func() ID { id, _ := NewStringID("42"); return id }(), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Equal(tt.b); got != tt.equal {
				t.Errorf("%v.Equal(%v) = %v, want %v", tt.a, tt.b, got, tt.equal)
			}
		})
	}
}

func TestIDCompare(t *testing.T) {
	tests := []struct {
		name    string
		a, b    ID
		wantCmp int
	}{
		{"u64 equal", NewU64ID(42), NewU64ID(42), 0},
		{"u64 less", NewU64ID(42), NewU64ID(100), -1},
		{"u64 greater", NewU64ID(100), NewU64ID(42), 1},
		{"uuid equal", NewUUIDID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")), NewUUIDID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")), 0},
		{"uuid less", NewUUIDID(uuid.MustParse("000e8400-e29b-41d4-a716-446655440000")), NewUUIDID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")), -1},
		{"string equal", func() ID { id, _ := NewStringID("aaa"); return id }(), func() ID { id, _ := NewStringID("aaa"); return id }(), 0},
		{"string less", func() ID { id, _ := NewStringID("aaa"); return id }(), func() ID { id, _ := NewStringID("bbb"); return id }(), -1},
		{"string greater", func() ID { id, _ := NewStringID("bbb"); return id }(), func() ID { id, _ := NewStringID("aaa"); return id }(), 1},
		{"u64 < uuid (different types)", NewU64ID(42), NewUUIDID(uuid.Nil), -1},
		{"uuid > u64 (different types)", NewUUIDID(uuid.Nil), NewU64ID(42), 1},
		{"uuid < string (different types)", NewUUIDID(uuid.Nil), func() ID { id, _ := NewStringID("a"); return id }(), -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.wantCmp {
				t.Errorf("%v.Compare(%v) = %v, want %v", tt.a, tt.b, got, tt.wantCmp)
			}
		})
	}
}

func TestIDTypeString(t *testing.T) {
	tests := []struct {
		typ  IDType
		want string
	}{
		{IDTypeU64, "u64"},
		{IDTypeUUID, "uuid"},
		{IDTypeString, "string"},
		{IDType(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("IDType(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

func TestIDString(t *testing.T) {
	tests := []struct {
		name string
		id   ID
		want string
	}{
		{"u64", NewU64ID(12345), "12345"},
		{"uuid", NewUUIDID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")), "550e8400-e29b-41d4-a716-446655440000"},
		{"string", func() ID { id, _ := NewStringID("hello"); return id }(), "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.id.String(); got != tt.want {
				t.Errorf("%+v.String() = %q, want %q", tt.id, got, tt.want)
			}
		})
	}
}

func TestValidationErrorFormat(t *testing.T) {
	err := &ValidationError{Field: "id", Message: "test message"}
	want := "id: test message"
	if got := err.Error(); got != want {
		t.Errorf("ValidationError.Error() = %q, want %q", got, want)
	}
}

func BenchmarkParseIDU64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseID(uint64(12345))
	}
}

func BenchmarkParseIDUUID(b *testing.B) {
	u := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	for i := 0; i < b.N; i++ {
		_, _ = ParseID(u)
	}
}

func BenchmarkParseIDStringUUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseID("550e8400-e29b-41d4-a716-446655440000")
	}
}

func BenchmarkParseIDStringNumeric(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseID("12345")
	}
}

func BenchmarkParseIDStringPlain(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseID("hello-world")
	}
}
