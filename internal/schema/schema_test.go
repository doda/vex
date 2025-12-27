package schema

import (
	"errors"
	"testing"
)

func TestNewDefinition(t *testing.T) {
	def := NewDefinition()
	if def == nil {
		t.Fatal("NewDefinition returned nil")
	}
	if def.Attributes == nil {
		t.Fatal("Attributes map is nil")
	}
	if len(def.Attributes) != 0 {
		t.Errorf("expected empty attributes, got %d", len(def.Attributes))
	}
}

func TestDefinitionSetAttribute(t *testing.T) {
	def := NewDefinition()

	// Set a new attribute
	err := def.SetAttribute("name", Attribute{Type: TypeString})
	if err != nil {
		t.Fatalf("SetAttribute failed: %v", err)
	}

	// Verify it was set
	attr := def.GetAttribute("name")
	if attr == nil {
		t.Fatal("GetAttribute returned nil")
	}
	if attr.Type != TypeString {
		t.Errorf("expected TypeString, got %s", attr.Type)
	}
}

func TestDefinitionSetAttributeTypeChange(t *testing.T) {
	def := NewDefinition()

	// Set initial attribute
	err := def.SetAttribute("count", Attribute{Type: TypeInt})
	if err != nil {
		t.Fatalf("SetAttribute failed: %v", err)
	}

	// Try to change type - should fail
	err = def.SetAttribute("count", Attribute{Type: TypeString})
	if err == nil {
		t.Fatal("expected error when changing attribute type, got nil")
	}
	if !errors.Is(err, ErrTypeChangeNotAllowed) {
		t.Errorf("expected ErrTypeChangeNotAllowed, got %v", err)
	}
}

func TestDefinitionSetAttributeSameType(t *testing.T) {
	def := NewDefinition()

	// Set initial attribute
	err := def.SetAttribute("count", Attribute{Type: TypeInt})
	if err != nil {
		t.Fatalf("SetAttribute failed: %v", err)
	}

	// Set same type again - should succeed
	err = def.SetAttribute("count", Attribute{Type: TypeInt})
	if err != nil {
		t.Errorf("SetAttribute with same type failed: %v", err)
	}
}

func TestDefinitionSetAttributeInvalidName(t *testing.T) {
	def := NewDefinition()

	// Test empty name
	err := def.SetAttribute("", Attribute{Type: TypeString})
	if err == nil {
		t.Error("expected error for empty name")
	}

	// Test $ prefix
	err = def.SetAttribute("$reserved", Attribute{Type: TypeString})
	if err == nil {
		t.Error("expected error for $ prefix")
	}
}

func TestDefinitionSetAttributeInvalidType(t *testing.T) {
	def := NewDefinition()

	err := def.SetAttribute("field", Attribute{Type: AttrType("invalid")})
	if err == nil {
		t.Error("expected error for invalid type")
	}
	if !errors.Is(err, ErrInvalidType) {
		t.Errorf("expected ErrInvalidType, got %v", err)
	}
}

func TestDefinitionHasAttribute(t *testing.T) {
	def := NewDefinition()

	if def.HasAttribute("name") {
		t.Error("HasAttribute returned true for non-existent attribute")
	}

	def.SetAttribute("name", Attribute{Type: TypeString})

	if !def.HasAttribute("name") {
		t.Error("HasAttribute returned false for existing attribute")
	}
}

func TestDefinitionInferAndAddAttribute(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected AttrType
	}{
		{"string", "hello", TypeString},
		{"int", float64(-42), TypeInt},
		{"uint", float64(42), TypeUint},
		{"float", float64(3.14), TypeFloat},
		{"bool", true, TypeBool},
		{"uuid", "550e8400-e29b-41d4-a716-446655440000", TypeUUID},
		{"datetime", "2024-03-15T10:30:45Z", TypeDatetime},
		{"array", []any{"a", "b"}, TypeStringArray},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := NewDefinition()
			err := def.InferAndAddAttribute("field", tt.value)
			if err != nil {
				t.Fatalf("InferAndAddAttribute failed: %v", err)
			}
			attr := def.GetAttribute("field")
			if attr == nil {
				t.Fatal("GetAttribute returned nil")
			}
			if attr.Type != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, attr.Type)
			}
		})
	}
}

func TestDefinitionInferAndAddAttributeNull(t *testing.T) {
	def := NewDefinition()

	// null values should not add to schema
	err := def.InferAndAddAttribute("field", nil)
	if err != nil {
		t.Fatalf("InferAndAddAttribute with nil failed: %v", err)
	}
	if def.HasAttribute("field") {
		t.Error("null value should not have created attribute")
	}
}

func TestDefinitionInferAndAddAttributeTypeConflict(t *testing.T) {
	def := NewDefinition()

	// Add string attribute
	err := def.InferAndAddAttribute("field", "hello")
	if err != nil {
		t.Fatalf("InferAndAddAttribute failed: %v", err)
	}

	// Try to add int value - should fail
	err = def.InferAndAddAttribute("field", float64(42))
	if err == nil {
		t.Fatal("expected error for type conflict")
	}
	if !errors.Is(err, ErrTypeChangeNotAllowed) {
		t.Errorf("expected ErrTypeChangeNotAllowed, got %v", err)
	}
}

func TestDefinitionMerge(t *testing.T) {
	def1 := NewDefinition()
	def1.SetAttribute("name", Attribute{Type: TypeString})

	def2 := NewDefinition()
	def2.SetAttribute("age", Attribute{Type: TypeInt})

	err := def1.Merge(def2)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	if !def1.HasAttribute("name") || !def1.HasAttribute("age") {
		t.Error("Merge didn't include all attributes")
	}
}

func TestDefinitionMergeConflict(t *testing.T) {
	def1 := NewDefinition()
	def1.SetAttribute("field", Attribute{Type: TypeString})

	def2 := NewDefinition()
	def2.SetAttribute("field", Attribute{Type: TypeInt})

	err := def1.Merge(def2)
	if err == nil {
		t.Fatal("expected error for type conflict in merge")
	}
	if !errors.Is(err, ErrTypeChangeNotAllowed) {
		t.Errorf("expected ErrTypeChangeNotAllowed, got %v", err)
	}
}

func TestDefinitionMergeNil(t *testing.T) {
	def := NewDefinition()
	def.SetAttribute("name", Attribute{Type: TypeString})

	err := def.Merge(nil)
	if err != nil {
		t.Errorf("Merge with nil failed: %v", err)
	}

	// Schema should be unchanged
	if !def.HasAttribute("name") {
		t.Error("Merge with nil should not change schema")
	}
}

func TestDefinitionValidate(t *testing.T) {
	def := NewDefinition()
	def.SetAttribute("name", Attribute{Type: TypeString})
	def.SetAttribute("age", Attribute{Type: TypeInt})

	err := def.Validate()
	if err != nil {
		t.Errorf("Validate failed for valid schema: %v", err)
	}
}

func TestDefinitionClone(t *testing.T) {
	def := NewDefinition()
	def.SetAttribute("name", Attribute{Type: TypeString})
	def.SetAttribute("age", Attribute{Type: TypeInt})

	clone := def.Clone()
	if clone == nil {
		t.Fatal("Clone returned nil")
	}

	// Verify clone has same attributes
	if !clone.HasAttribute("name") || !clone.HasAttribute("age") {
		t.Error("Clone missing attributes")
	}

	// Verify clone is independent
	clone.SetAttribute("extra", Attribute{Type: TypeBool})
	if def.HasAttribute("extra") {
		t.Error("Clone is not independent from original")
	}
}

func TestValidateSchemaUpdate(t *testing.T) {
	tests := []struct {
		name     string
		existing *Definition
		proposed *Definition
		wantErr  bool
	}{
		{
			name:     "nil_existing",
			existing: nil,
			proposed: NewDefinition(),
			wantErr:  false,
		},
		{
			name:     "nil_proposed",
			existing: NewDefinition(),
			proposed: nil,
			wantErr:  false,
		},
		{
			name: "new_attribute",
			existing: func() *Definition {
				d := NewDefinition()
				d.SetAttribute("name", Attribute{Type: TypeString})
				return d
			}(),
			proposed: func() *Definition {
				d := NewDefinition()
				d.SetAttribute("name", Attribute{Type: TypeString})
				d.SetAttribute("age", Attribute{Type: TypeInt})
				return d
			}(),
			wantErr: false,
		},
		{
			name: "same_type",
			existing: func() *Definition {
				d := NewDefinition()
				d.SetAttribute("name", Attribute{Type: TypeString})
				return d
			}(),
			proposed: func() *Definition {
				d := NewDefinition()
				d.SetAttribute("name", Attribute{Type: TypeString})
				return d
			}(),
			wantErr: false,
		},
		{
			name: "type_change_string_to_int",
			existing: func() *Definition {
				d := NewDefinition()
				d.SetAttribute("field", Attribute{Type: TypeString})
				return d
			}(),
			proposed: func() *Definition {
				d := NewDefinition()
				d.SetAttribute("field", Attribute{Type: TypeInt})
				return d
			}(),
			wantErr: true,
		},
		{
			name: "type_change_int_to_uint",
			existing: func() *Definition {
				d := NewDefinition()
				d.SetAttribute("count", Attribute{Type: TypeInt})
				return d
			}(),
			proposed: func() *Definition {
				d := NewDefinition()
				d.SetAttribute("count", Attribute{Type: TypeUint})
				return d
			}(),
			wantErr: true,
		},
		{
			name: "type_change_scalar_to_array",
			existing: func() *Definition {
				d := NewDefinition()
				d.SetAttribute("tags", Attribute{Type: TypeString})
				return d
			}(),
			proposed: func() *Definition {
				d := NewDefinition()
				d.SetAttribute("tags", Attribute{Type: TypeStringArray})
				return d
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSchemaUpdate(tt.existing, tt.proposed)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSchemaUpdate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil && !errors.Is(err, ErrTypeChangeNotAllowed) {
				t.Errorf("expected ErrTypeChangeNotAllowed, got %v", err)
			}
		})
	}
}

// Test that changing attribute type returns 400 (error)
// This verifies the requirement: "Verify changing attribute type returns 400"
func TestChangingAttributeTypeReturnsError(t *testing.T) {
	testCases := []struct {
		name    string
		oldType AttrType
		newType AttrType
	}{
		{"string_to_int", TypeString, TypeInt},
		{"int_to_string", TypeInt, TypeString},
		{"int_to_uint", TypeInt, TypeUint},
		{"uint_to_int", TypeUint, TypeInt},
		{"float_to_int", TypeFloat, TypeInt},
		{"bool_to_string", TypeBool, TypeString},
		{"uuid_to_string", TypeUUID, TypeString},
		{"datetime_to_string", TypeDatetime, TypeString},
		{"string_to_array_string", TypeString, TypeStringArray},
		{"array_int_to_array_string", TypeIntArray, TypeStringArray},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			def := NewDefinition()

			// Set initial type
			err := def.SetAttribute("field", Attribute{Type: tc.oldType})
			if err != nil {
				t.Fatalf("setting initial type failed: %v", err)
			}

			// Try to change type
			err = def.SetAttribute("field", Attribute{Type: tc.newType})
			if err == nil {
				t.Fatalf("changing type from %s to %s should have failed", tc.oldType, tc.newType)
			}
			if !errors.Is(err, ErrTypeChangeNotAllowed) {
				t.Errorf("expected ErrTypeChangeNotAllowed, got: %v", err)
			}
		})
	}
}

// Test all 7 scalar types are supported
func TestAllScalarTypesSupported(t *testing.T) {
	def := NewDefinition()

	// Test all scalar types can be added
	types := []struct {
		name     string
		attrType AttrType
	}{
		{"string_field", TypeString},
		{"int_field", TypeInt},
		{"uint_field", TypeUint},
		{"float_field", TypeFloat},
		{"uuid_field", TypeUUID},
		{"datetime_field", TypeDatetime},
		{"bool_field", TypeBool},
	}

	for _, tt := range types {
		err := def.SetAttribute(tt.name, Attribute{Type: tt.attrType})
		if err != nil {
			t.Errorf("failed to add %s type: %v", tt.attrType, err)
		}
	}

	// Verify all were added
	for _, tt := range types {
		attr := def.GetAttribute(tt.name)
		if attr == nil {
			t.Errorf("attribute %s not found", tt.name)
			continue
		}
		if attr.Type != tt.attrType {
			t.Errorf("attribute %s has wrong type: expected %s, got %s", tt.name, tt.attrType, attr.Type)
		}
	}
}

// Test all 7 array types are supported
func TestAllArrayTypesSupported(t *testing.T) {
	def := NewDefinition()

	// Test all array types can be added
	types := []struct {
		name     string
		attrType AttrType
	}{
		{"string_array", TypeStringArray},
		{"int_array", TypeIntArray},
		{"uint_array", TypeUintArray},
		{"float_array", TypeFloatArray},
		{"uuid_array", TypeUUIDArray},
		{"datetime_array", TypeDatetimeArray},
		{"bool_array", TypeBoolArray},
	}

	for _, tt := range types {
		err := def.SetAttribute(tt.name, Attribute{Type: tt.attrType})
		if err != nil {
			t.Errorf("failed to add %s type: %v", tt.attrType, err)
		}
	}

	// Verify all were added
	for _, tt := range types {
		attr := def.GetAttribute(tt.name)
		if attr == nil {
			t.Errorf("attribute %s not found", tt.name)
			continue
		}
		if attr.Type != tt.attrType {
			t.Errorf("attribute %s has wrong type: expected %s, got %s", tt.name, tt.attrType, attr.Type)
		}
		if !attr.Type.IsArray() {
			t.Errorf("attribute %s should be an array type", tt.name)
		}
	}
}
