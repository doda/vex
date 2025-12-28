package schema

import (
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestSchemaInferenceForAllSupportedTypes tests schema inference for all 7 scalar types
// and their array variants, as required by the task.
func TestSchemaInferenceForAllSupportedTypes(t *testing.T) {
	t.Run("scalar_types", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected AttrType
		}{
			// String type
			{"string_simple", "hello", TypeString},
			{"string_empty", "", TypeString},
			{"string_symbols", "hello-world", TypeString},
			{"string_with_punct", "hello!", TypeString},
			{"string_long", "a very long string that tests the string type handling", TypeString},

			// Int type (i64)
			{"int_positive", int64(42), TypeInt},
			{"int_negative", int64(-42), TypeInt},
			{"int_zero", int64(0), TypeInt},
			{"int_max", int64(9223372036854775807), TypeInt},
			{"int_min", int64(-9223372036854775808), TypeInt},
			{"int32", int32(42), TypeInt},
			{"int16", int16(42), TypeInt},
			{"int8", int8(42), TypeInt},

			// Uint type (u64)
			{"uint_positive", uint64(42), TypeUint},
			{"uint_zero", uint64(0), TypeUint},
			{"uint_max", uint64(18446744073709551615), TypeUint},
			{"uint32", uint32(42), TypeUint},
			{"uint16", uint16(42), TypeUint},
			{"uint8", uint8(42), TypeUint},

			// Float type (f64)
			{"float_positive", float64(3.14), TypeFloat},
			{"float_negative", float64(-3.14), TypeFloat},
			{"float_zero", float64(0.0), TypeFloat},
			{"float_small", float64(0.0000001), TypeFloat},
			{"float_large", float64(1e308), TypeFloat},
			{"float32", float32(3.14), TypeFloat},

			// Bool type
			{"bool_true", true, TypeBool},
			{"bool_false", false, TypeBool},

			// Datetime type (ms epoch)
			{"datetime_time", time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC), TypeDatetime},
			{"datetime_now", time.Now(), TypeDatetime},

			// UUID type (16B)
			{"uuid_random", uuid.New(), TypeUUID},
			{"uuid_nil", uuid.Nil, TypeUUID},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := InferType(tt.value)
				if err != nil {
					t.Fatalf("InferType(%v) returned error: %v", tt.value, err)
				}
				if got != tt.expected {
					t.Errorf("InferType(%v) = %s, want %s", tt.value, got, tt.expected)
				}
			})
		}
	})

	t.Run("array_types", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected AttrType
		}{
			// String array
			{"string_array", []string{"a", "b", "c"}, TypeStringArray},
			{"string_array_single", []string{"only"}, TypeStringArray},

			// Int array
			{"int_array", []int64{1, 2, 3}, TypeIntArray},
			{"int_array_negative", []int64{-1, -2, -3}, TypeIntArray},
			{"int32_array", []int32{1, 2, 3}, TypeIntArray},

			// Uint array
			{"uint_array", []uint64{1, 2, 3}, TypeUintArray},
			{"uint32_array", []uint32{1, 2, 3}, TypeUintArray},

			// Float array
			{"float_array", []float64{1.1, 2.2, 3.3}, TypeFloatArray},
			{"float32_array", []float32{1.1, 2.2, 3.3}, TypeFloatArray},

			// Bool array
			{"bool_array", []bool{true, false, true}, TypeBoolArray},

			// Datetime array
			{"datetime_array", []time.Time{time.Now(), time.Now().Add(time.Hour)}, TypeDatetimeArray},

			// UUID array
			{"uuid_array", []uuid.UUID{uuid.New(), uuid.New()}, TypeUUIDArray},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := InferType(tt.value)
				if err != nil {
					t.Fatalf("InferType(%v) returned error: %v", tt.value, err)
				}
				if got != tt.expected {
					t.Errorf("InferType(%v) = %s, want %s", tt.value, got, tt.expected)
				}
			})
		}
	})

	t.Run("json_types", func(t *testing.T) {
		// Test types as they appear after JSON unmarshaling
		tests := []struct {
			name     string
			value    any
			expected AttrType
		}{
			// JSON numbers come as float64
			{"json_positive_int", float64(42), TypeUint},
			{"json_negative_int", float64(-42), TypeInt},
			{"json_float", float64(3.14), TypeFloat},
			{"json_zero", float64(0), TypeUint},

			// JSON strings
			{"json_string", "hello", TypeString},

			// UUID detection from JSON string
			{"json_uuid", "550e8400-e29b-41d4-a716-446655440000", TypeUUID},

			// Datetime detection from JSON string
			{"json_datetime", "2024-03-15T10:30:45Z", TypeDatetime},

			// JSON boolean
			{"json_bool", true, TypeBool},

			// JSON arrays
			{"json_string_array", []any{"a", "b"}, TypeStringArray},
			{"json_int_array", []any{float64(1), float64(2)}, TypeUintArray},
			{"json_float_array", []any{float64(1.5), float64(2.5)}, TypeFloatArray},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := InferTypeFromJSON(tt.value)
				if err != nil {
					t.Fatalf("InferTypeFromJSON(%v) returned error: %v", tt.value, err)
				}
				if got != tt.expected {
					t.Errorf("InferTypeFromJSON(%v) = %s, want %s", tt.value, got, tt.expected)
				}
			})
		}
	})
}

// TestTypeCheckingRejectsInvalidTypes tests that type checking properly rejects
// invalid types and type mismatches.
func TestTypeCheckingRejectsInvalidTypes(t *testing.T) {
	t.Run("invalid_type_strings", func(t *testing.T) {
		invalidTypes := []AttrType{
			"",
			"invalid",
			"String", // case matters
			"INTEGER",
			"[]",
			"[]invalid",
			"map",
			"object",
			"null",
			"any",
		}

		for _, typ := range invalidTypes {
			t.Run(string(typ), func(t *testing.T) {
				if typ.IsValid() {
					t.Errorf("type %q should be invalid", typ)
				}
			})
		}
	})

	t.Run("type_mismatch_detection", func(t *testing.T) {
		tests := []struct {
			name  string
			typ   AttrType
			value any
		}{
			// String mismatches
			{"string_vs_int", TypeString, 42},
			{"string_vs_bool", TypeString, true},
			{"string_vs_float", TypeString, 3.14},

			// Int mismatches
			{"int_vs_string", TypeInt, "42"},
			{"int_vs_bool", TypeInt, true},
			{"int_vs_float_value", TypeInt, float64(3.14)}, // non-integer float

			// Uint mismatches
			{"uint_vs_string", TypeUint, "42"},
			{"uint_vs_negative", TypeUint, float64(-1)},
			{"uint_vs_negative_int", TypeUint, int(-1)},

			// Float mismatches
			{"float_vs_string", TypeFloat, "3.14"},
			{"float_vs_bool", TypeFloat, true},

			// Bool mismatches
			{"bool_vs_int", TypeBool, 1},
			{"bool_vs_string", TypeBool, "true"},
			{"bool_vs_float", TypeBool, 1.0},

			// Datetime mismatches
			{"datetime_vs_invalid_string", TypeDatetime, "not a date"},
			{"datetime_vs_bool", TypeDatetime, true},

			// UUID mismatches
			{"uuid_vs_invalid_string", TypeUUID, "not-a-uuid"},
			{"uuid_vs_short_bytes", TypeUUID, make([]byte, 8)},
			{"uuid_vs_int", TypeUUID, 123},

			// Array type mismatches
			{"string_array_vs_int_elements", TypeStringArray, []any{1, 2, 3}},
			{"int_array_vs_string_elements", TypeIntArray, []any{"a", "b"}},
			{"bool_array_vs_int_elements", TypeBoolArray, []any{1, 0}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := ValidateValue(tt.typ, tt.value)
				if err == nil {
					t.Errorf("ValidateValue(%s, %v) should return error", tt.typ, tt.value)
				}
			})
		}
	})

	t.Run("schema_rejects_invalid_type", func(t *testing.T) {
		def := NewDefinition()
		err := def.SetAttribute("field", Attribute{Type: AttrType("invalid")})
		if err == nil {
			t.Error("expected error when setting attribute with invalid type")
		}
	})

	t.Run("schema_rejects_type_change", func(t *testing.T) {
		def := NewDefinition()

		// Set initial type
		err := def.SetAttribute("field", Attribute{Type: TypeString})
		if err != nil {
			t.Fatalf("failed to set initial attribute: %v", err)
		}

		// Try to change type
		err = def.SetAttribute("field", Attribute{Type: TypeInt})
		if err == nil {
			t.Error("expected error when changing attribute type")
		}
	})

	t.Run("infer_rejects_unsupported_types", func(t *testing.T) {
		unsupported := []any{
			map[string]string{"key": "value"},
			struct{ Name string }{"test"},
			func() {},
			make(chan int),
			complex(1, 2),
		}

		for _, v := range unsupported {
			_, err := InferType(v)
			if err == nil {
				t.Errorf("InferType(%T) should return error for unsupported type", v)
			}
		}
	})

	t.Run("infer_rejects_nil", func(t *testing.T) {
		_, err := InferType(nil)
		if err == nil {
			t.Error("InferType(nil) should return error")
		}
	})

	t.Run("infer_rejects_empty_array", func(t *testing.T) {
		_, err := InferType([]any{})
		if err == nil {
			t.Error("InferType([]) should return error for empty array")
		}
	})
}

// TestDeterministicSchemaInference tests that schema inference is deterministic,
// i.e., the same input always produces the same schema.
func TestDeterministicSchemaInference(t *testing.T) {
	t.Run("type_inference_consistency", func(t *testing.T) {
		// Run type inference multiple times on the same value
		values := []any{
			"hello",
			int64(42),
			uint64(100),
			float64(3.14),
			true,
			time.Now(),
			uuid.New(),
			[]string{"a", "b"},
			[]int64{1, 2, 3},
		}

		for _, v := range values {
			first, err := InferType(v)
			if err != nil {
				t.Fatalf("InferType failed: %v", err)
			}

			// Run 100 times to ensure consistency
			for i := 0; i < 100; i++ {
				got, err := InferType(v)
				if err != nil {
					t.Fatalf("InferType failed on iteration %d: %v", i, err)
				}
				if got != first {
					t.Errorf("InferType produced different result on iteration %d: got %s, want %s", i, got, first)
				}
			}
		}
	})

	t.Run("json_inference_consistency", func(t *testing.T) {
		// Test that JSON inference is consistent
		jsonValues := []any{
			"hello",
			float64(42),
			float64(-42),
			float64(3.14),
			true,
			"550e8400-e29b-41d4-a716-446655440000", // UUID
			"2024-03-15T10:30:45Z",                 // datetime
			[]any{"a", "b"},
			[]any{float64(1), float64(2)},
		}

		for _, v := range jsonValues {
			first, err := InferTypeFromJSON(v)
			if err != nil {
				t.Fatalf("InferTypeFromJSON failed: %v", err)
			}

			for i := 0; i < 100; i++ {
				got, err := InferTypeFromJSON(v)
				if err != nil {
					t.Fatalf("InferTypeFromJSON failed on iteration %d: %v", i, err)
				}
				if got != first {
					t.Errorf("InferTypeFromJSON produced different result on iteration %d: got %s, want %s", i, got, first)
				}
			}
		}
	})

	t.Run("schema_build_order_independence", func(t *testing.T) {
		// Build schema in different orders, should produce same result
		docs := []map[string]any{
			{"name": "Alice", "age": float64(30), "active": true},
			{"name": "Bob", "age": float64(25), "email": "bob@example.com"},
			{"name": "Carol", "tags": []any{"admin", "user"}},
		}

		// Build schema forward
		def1 := NewDefinition()
		for _, doc := range docs {
			for k, v := range doc {
				def1.InferAndAddAttribute(k, v)
			}
		}

		// Build schema backward
		def2 := NewDefinition()
		for i := len(docs) - 1; i >= 0; i-- {
			for k, v := range docs[i] {
				def2.InferAndAddAttribute(k, v)
			}
		}

		// Compare schemas
		if len(def1.Attributes) != len(def2.Attributes) {
			t.Errorf("schemas have different attribute counts: %d vs %d", len(def1.Attributes), len(def2.Attributes))
		}

		for name, attr1 := range def1.Attributes {
			attr2, ok := def2.Attributes[name]
			if !ok {
				t.Errorf("attribute %q missing from second schema", name)
				continue
			}
			if attr1.Type != attr2.Type {
				t.Errorf("attribute %q has different types: %s vs %s", name, attr1.Type, attr2.Type)
			}
		}
	})

	t.Run("first_value_determines_type", func(t *testing.T) {
		// The first non-null value should determine the type
		def := NewDefinition()

		// First document sets the type
		def.InferAndAddAttribute("field", "string_value")

		attr := def.GetAttribute("field")
		if attr.Type != TypeString {
			t.Errorf("expected TypeString, got %s", attr.Type)
		}

		// Adding same type should succeed
		err := def.InferAndAddAttribute("field", "another_string")
		if err != nil {
			t.Errorf("adding same type should succeed: %v", err)
		}

		// Adding different type should fail
		err = def.InferAndAddAttribute("field", float64(42))
		if err == nil {
			t.Error("adding different type should fail")
		}
	})

	t.Run("null_values_dont_affect_inference", func(t *testing.T) {
		def := NewDefinition()

		// Null value should not define type
		err := def.InferAndAddAttribute("field", nil)
		if err != nil {
			t.Fatalf("InferAndAddAttribute with nil failed: %v", err)
		}
		if def.HasAttribute("field") {
			t.Error("null value should not create attribute")
		}

		// First real value defines type
		err = def.InferAndAddAttribute("field", "hello")
		if err != nil {
			t.Fatalf("InferAndAddAttribute failed: %v", err)
		}

		attr := def.GetAttribute("field")
		if attr.Type != TypeString {
			t.Errorf("expected TypeString, got %s", attr.Type)
		}
	})

	t.Run("array_element_type_from_first_element", func(t *testing.T) {
		// Array type is determined by first element
		tests := []struct {
			name     string
			array    []any
			expected AttrType
		}{
			{"string_array", []any{"a", "b", "c"}, TypeStringArray},
			{"int_array", []any{float64(1), float64(2)}, TypeUintArray}, // positive ints
			{"float_array", []any{float64(1.5), float64(2.5)}, TypeFloatArray},
			{"bool_array", []any{true, false}, TypeBoolArray},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := InferTypeFromJSON(tt.array)
				if err != nil {
					t.Fatalf("InferTypeFromJSON failed: %v", err)
				}
				if got != tt.expected {
					t.Errorf("InferTypeFromJSON = %s, want %s", got, tt.expected)
				}
			})
		}
	})
}

// TestSchemaInferenceFromJSONDocument tests inferring schema from JSON documents
// as they would appear in API requests.
func TestSchemaInferenceFromJSONDocument(t *testing.T) {
	t.Run("simple_document", func(t *testing.T) {
		jsonDoc := `{
			"name": "Alice",
			"age": 30,
			"score": 95.5,
			"active": true,
			"tags": ["admin", "user"]
		}`

		var doc map[string]any
		if err := json.Unmarshal([]byte(jsonDoc), &doc); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}

		def := NewDefinition()
		for k, v := range doc {
			if err := def.InferAndAddAttribute(k, v); err != nil {
				t.Errorf("InferAndAddAttribute(%q, %v) failed: %v", k, v, err)
			}
		}

		expected := map[string]AttrType{
			"name":   TypeString,
			"age":    TypeUint,
			"score":  TypeFloat,
			"active": TypeBool,
			"tags":   TypeStringArray,
		}

		for name, expectedType := range expected {
			attr := def.GetAttribute(name)
			if attr == nil {
				t.Errorf("attribute %q not found", name)
				continue
			}
			if attr.Type != expectedType {
				t.Errorf("attribute %q: got %s, want %s", name, attr.Type, expectedType)
			}
		}
	})

	t.Run("document_with_special_types", func(t *testing.T) {
		jsonDoc := `{
			"id": "550e8400-e29b-41d4-a716-446655440000",
			"created_at": "2024-03-15T10:30:45Z",
			"count": -5
		}`

		var doc map[string]any
		if err := json.Unmarshal([]byte(jsonDoc), &doc); err != nil {
			t.Fatalf("failed to parse JSON: %v", err)
		}

		def := NewDefinition()
		for k, v := range doc {
			if err := def.InferAndAddAttribute(k, v); err != nil {
				t.Errorf("InferAndAddAttribute(%q, %v) failed: %v", k, v, err)
			}
		}

		expected := map[string]AttrType{
			"id":         TypeUUID,
			"created_at": TypeDatetime,
			"count":      TypeInt, // negative number
		}

		for name, expectedType := range expected {
			attr := def.GetAttribute(name)
			if attr == nil {
				t.Errorf("attribute %q not found", name)
				continue
			}
			if attr.Type != expectedType {
				t.Errorf("attribute %q: got %s, want %s", name, attr.Type, expectedType)
			}
		}
	})

	t.Run("multiple_documents_schema_merge", func(t *testing.T) {
		docs := []string{
			`{"name": "Alice", "age": 30}`,
			`{"name": "Bob", "email": "bob@example.com"}`,
			`{"name": "Carol", "age": 25, "department": "Engineering"}`,
		}

		def := NewDefinition()
		for _, jsonDoc := range docs {
			var doc map[string]any
			if err := json.Unmarshal([]byte(jsonDoc), &doc); err != nil {
				t.Fatalf("failed to parse JSON: %v", err)
			}

			for k, v := range doc {
				def.InferAndAddAttribute(k, v)
			}
		}

		// All attributes should be present
		expected := []string{"name", "age", "email", "department"}
		for _, name := range expected {
			if !def.HasAttribute(name) {
				t.Errorf("expected attribute %q to be present", name)
			}
		}
	})
}

// TestSchemaAttributeOrdering tests that attribute iteration is predictable.
func TestSchemaAttributeOrdering(t *testing.T) {
	def := NewDefinition()
	attrs := []string{"zebra", "alpha", "mike", "charlie"}

	for _, name := range attrs {
		def.SetAttribute(name, Attribute{Type: TypeString})
	}

	// Get attributes multiple times and verify consistent ordering
	var orders [][]string
	for i := 0; i < 10; i++ {
		var names []string
		for name := range def.Attributes {
			names = append(names, name)
		}
		sort.Strings(names) // Sort for comparison
		orders = append(orders, names)
	}

	// All orderings should be the same when sorted
	for i := 1; i < len(orders); i++ {
		if len(orders[0]) != len(orders[i]) {
			t.Errorf("iteration %d has different length", i)
			continue
		}
		for j := range orders[0] {
			if orders[0][j] != orders[i][j] {
				t.Errorf("iteration %d differs at position %d: got %s, want %s", i, j, orders[i][j], orders[0][j])
			}
		}
	}
}

// TestSchemaValidation tests the Validate method.
func TestSchemaValidation(t *testing.T) {
	t.Run("valid_schema", func(t *testing.T) {
		def := NewDefinition()
		def.SetAttribute("name", Attribute{Type: TypeString})
		def.SetAttribute("age", Attribute{Type: TypeInt})
		def.SetAttribute("tags", Attribute{Type: TypeStringArray})

		if err := def.Validate(); err != nil {
			t.Errorf("valid schema returned error: %v", err)
		}
	})

	t.Run("empty_schema_is_valid", func(t *testing.T) {
		def := NewDefinition()
		if err := def.Validate(); err != nil {
			t.Errorf("empty schema returned error: %v", err)
		}
	})
}

// TestTypeCompatibilityMatrix tests all type compatibility combinations.
func TestTypeCompatibilityMatrix(t *testing.T) {
	allTypes := []AttrType{
		TypeString, TypeInt, TypeUint, TypeFloat, TypeUUID, TypeDatetime, TypeBool,
		TypeStringArray, TypeIntArray, TypeUintArray, TypeFloatArray, TypeUUIDArray, TypeDatetimeArray, TypeBoolArray,
	}

	for _, t1 := range allTypes {
		for _, t2 := range allTypes {
			name := string(t1) + "_to_" + string(t2)
			t.Run(name, func(t *testing.T) {
				expected := t1 == t2
				got := TypesCompatible(t1, t2)
				if got != expected {
					t.Errorf("TypesCompatible(%s, %s) = %v, want %v", t1, t2, got, expected)
				}
			})
		}
	}
}

// TestEdgeCaseTypeInference tests edge cases in type inference.
func TestEdgeCaseTypeInference(t *testing.T) {
	t.Run("uuid_formats", func(t *testing.T) {
		uuidFormats := []string{
			"550e8400-e29b-41d4-a716-446655440000", // standard
			"550e8400e29b41d4a716446655440000",     // no dashes
			"550E8400-E29B-41D4-A716-446655440000", // uppercase
		}

		for _, u := range uuidFormats {
			t.Run(u[:8], func(t *testing.T) {
				typ, err := InferTypeFromJSON(u)
				if err != nil {
					t.Fatalf("InferTypeFromJSON failed: %v", err)
				}
				if typ != TypeUUID {
					t.Errorf("expected TypeUUID, got %s", typ)
				}
			})
		}
	})

	t.Run("datetime_formats", func(t *testing.T) {
		datetimeFormats := []string{
			"2024-03-15T10:30:45Z",
			"2024-03-15T10:30:45.123Z",
			"2024-03-15T10:30:45.123456789Z",
			"2024-03-15T10:30:45+05:00",
			"2024-03-15T10:30:45-08:00",
		}

		for _, dt := range datetimeFormats {
			t.Run(dt[:19], func(t *testing.T) {
				typ, err := InferTypeFromJSON(dt)
				if err != nil {
					t.Fatalf("InferTypeFromJSON failed: %v", err)
				}
				if typ != TypeDatetime {
					t.Errorf("expected TypeDatetime, got %s", typ)
				}
			})
		}
	})

	t.Run("integer_boundary_values", func(t *testing.T) {
		// Test integer vs float detection at boundaries
		tests := []struct {
			name     string
			value    float64
			expected AttrType
		}{
			{"zero", 0, TypeUint},
			{"one", 1, TypeUint},
			{"negative_one", -1, TypeInt},
			{"max_safe_integer", 9007199254740991, TypeUint},     // 2^53 - 1
			{"min_safe_integer", -9007199254740991, TypeInt},     // -(2^53 - 1)
			{"with_fraction", 1.5, TypeFloat},
			{"tiny_fraction", 1.0000001, TypeFloat},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				typ, err := InferTypeFromJSON(tt.value)
				if err != nil {
					t.Fatalf("InferTypeFromJSON failed: %v", err)
				}
				if typ != tt.expected {
					t.Errorf("InferTypeFromJSON(%v) = %s, want %s", tt.value, typ, tt.expected)
				}
			})
		}
	})

	t.Run("string_not_uuid", func(t *testing.T) {
		// These should be strings, not UUIDs
		notUUIDs := []string{
			"hello",
			"12345678",                  // too short
			"12345678-1234-1234-1234",   // incomplete
			"gggggggg-gggg-gggg-gggg-gggggggggggg", // invalid hex
		}

		for _, s := range notUUIDs {
			t.Run(s[:min(len(s), 10)], func(t *testing.T) {
				typ, err := InferTypeFromJSON(s)
				if err != nil {
					t.Fatalf("InferTypeFromJSON failed: %v", err)
				}
				if typ != TypeString {
					t.Errorf("expected TypeString, got %s", typ)
				}
			})
		}
	})

	t.Run("string_not_datetime", func(t *testing.T) {
		// These should be strings, not datetimes
		notDatetimes := []string{
			"hello",
			"2024-03-15",     // date only, no time
			"10:30:45",       // time only
			"March 15, 2024", // informal format
			"yesterday",
		}

		for _, s := range notDatetimes {
			t.Run(s[:min(len(s), 10)], func(t *testing.T) {
				typ, err := InferTypeFromJSON(s)
				if err != nil {
					t.Fatalf("InferTypeFromJSON failed: %v", err)
				}
				if typ != TypeString {
					t.Errorf("expected TypeString, got %s", typ)
				}
			})
		}
	})
}
