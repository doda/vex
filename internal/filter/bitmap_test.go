package filter

import (
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/schema"
)

func TestFilterIndex_ScalarString(t *testing.T) {
	idx := NewFilterIndex("category", schema.TypeString)

	// Add some values
	idx.AddValue(0, "electronics")
	idx.AddValue(1, "books")
	idx.AddValue(2, "electronics")
	idx.AddValue(3, "clothing")
	idx.AddValue(4, nil) // null value
	idx.AddMissing(5)

	idx.Finalize()

	// Test Eq
	t.Run("Eq", func(t *testing.T) {
		bm := idx.Eq("electronics")
		if bm.GetCardinality() != 2 {
			t.Errorf("expected 2 rows, got %d", bm.GetCardinality())
		}
		if !bm.Contains(0) || !bm.Contains(2) {
			t.Error("expected rows 0 and 2")
		}
	})

	// Test Eq null
	t.Run("EqNull", func(t *testing.T) {
		bm := idx.Eq(nil)
		if bm.GetCardinality() != 1 {
			t.Errorf("expected 1 row, got %d", bm.GetCardinality())
		}
		if !bm.Contains(5) {
			t.Error("expected row 5")
		}
	})

	t.Run("NullBitmap", func(t *testing.T) {
		bm := idx.NullBitmap()
		if bm.GetCardinality() != 1 {
			t.Errorf("expected 1 explicit null row, got %d", bm.GetCardinality())
		}
		if !bm.Contains(4) {
			t.Error("expected row 4")
		}
	})

	// Test In
	t.Run("In", func(t *testing.T) {
		bm := idx.In([]any{"electronics", "books"})
		if bm.GetCardinality() != 3 {
			t.Errorf("expected 3 rows, got %d", bm.GetCardinality())
		}
	})
}

func TestFilterIndex_ScalarNumeric(t *testing.T) {
	idx := NewFilterIndex("price", schema.TypeFloat)

	// Add values
	idx.AddValue(0, 10.5)
	idx.AddValue(1, 25.0)
	idx.AddValue(2, 15.75)
	idx.AddValue(3, 30.0)
	idx.AddValue(4, 5.0)
	idx.AddValue(5, nil)

	idx.Finalize()

	// Test Lt
	t.Run("Lt", func(t *testing.T) {
		bm := idx.Lt(15.75)
		if bm.GetCardinality() != 2 {
			t.Errorf("expected 2 rows (10.5, 5.0), got %d", bm.GetCardinality())
		}
		if !bm.Contains(0) || !bm.Contains(4) {
			t.Error("expected rows 0 and 4")
		}
	})

	// Test Lte
	t.Run("Lte", func(t *testing.T) {
		bm := idx.Lte(15.75)
		if bm.GetCardinality() != 3 {
			t.Errorf("expected 3 rows (10.5, 15.75, 5.0), got %d", bm.GetCardinality())
		}
	})

	// Test Gt
	t.Run("Gt", func(t *testing.T) {
		bm := idx.Gt(15.75)
		if bm.GetCardinality() != 2 {
			t.Errorf("expected 2 rows (25.0, 30.0), got %d", bm.GetCardinality())
		}
	})

	// Test Gte
	t.Run("Gte", func(t *testing.T) {
		bm := idx.Gte(15.75)
		if bm.GetCardinality() != 3 {
			t.Errorf("expected 3 rows (15.75, 25.0, 30.0), got %d", bm.GetCardinality())
		}
	})

	// Test Range
	t.Run("Range", func(t *testing.T) {
		bm := idx.Range(10.5, 25.0, true, true)
		if bm.GetCardinality() != 3 {
			t.Errorf("expected 3 rows (10.5, 15.75, 25.0), got %d", bm.GetCardinality())
		}
	})
}

func TestFilterIndex_ScalarInt(t *testing.T) {
	idx := NewFilterIndex("count", schema.TypeInt)

	// Add values including negative
	idx.AddValue(0, int64(-10))
	idx.AddValue(1, int64(0))
	idx.AddValue(2, int64(5))
	idx.AddValue(3, int64(10))
	idx.AddValue(4, int64(-5))

	idx.Finalize()

	// Test Lt with negative values
	t.Run("LtWithNegative", func(t *testing.T) {
		bm := idx.Lt(int64(0))
		if bm.GetCardinality() != 2 {
			t.Errorf("expected 2 rows (-10, -5), got %d", bm.GetCardinality())
		}
		if !bm.Contains(0) || !bm.Contains(4) {
			t.Error("expected rows 0 and 4")
		}
	})

	// Test Gte with negative
	t.Run("GteWithNegative", func(t *testing.T) {
		bm := idx.Gte(int64(-5))
		if bm.GetCardinality() != 4 {
			t.Errorf("expected 4 rows (-5, 0, 5, 10), got %d", bm.GetCardinality())
		}
	})
}

func TestFilterIndex_Datetime(t *testing.T) {
	idx := NewFilterIndex("timestamp", schema.TypeDatetime)

	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	t3 := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)

	idx.AddValue(0, t1)
	idx.AddValue(1, t2)
	idx.AddValue(2, t3)

	idx.Finalize()

	// Test Lt datetime
	t.Run("LtDatetime", func(t *testing.T) {
		bm := idx.Lt(t2)
		if bm.GetCardinality() != 1 {
			t.Errorf("expected 1 row, got %d", bm.GetCardinality())
		}
		if !bm.Contains(0) {
			t.Error("expected row 0")
		}
	})

	// Test range
	t.Run("RangeDatetime", func(t *testing.T) {
		bm := idx.Range(t1, t3, true, true)
		if bm.GetCardinality() != 3 {
			t.Errorf("expected 3 rows, got %d", bm.GetCardinality())
		}
	})
}

func TestFilterIndex_ArrayString(t *testing.T) {
	idx := NewFilterIndex("tags", schema.TypeStringArray)

	// Add arrays
	idx.AddValue(0, []any{"go", "rust", "python"})
	idx.AddValue(1, []any{"java", "go"})
	idx.AddValue(2, []any{"python"})
	idx.AddValue(3, []any{}) // empty array = null
	idx.AddValue(4, nil)     // null

	idx.Finalize()

	// Test Contains
	t.Run("Contains", func(t *testing.T) {
		bm := idx.Contains("go")
		if bm.GetCardinality() != 2 {
			t.Errorf("expected 2 rows, got %d", bm.GetCardinality())
		}
		if !bm.Contains(0) || !bm.Contains(1) {
			t.Error("expected rows 0 and 1")
		}
	})

	// Test ContainsAny
	t.Run("ContainsAny", func(t *testing.T) {
		bm := idx.ContainsAny([]any{"rust", "java"})
		if bm.GetCardinality() != 2 {
			t.Errorf("expected 2 rows, got %d", bm.GetCardinality())
		}
		if !bm.Contains(0) || !bm.Contains(1) {
			t.Error("expected rows 0 and 1")
		}
	})

	// Test null bitmap for empty arrays
	t.Run("NullBitmap", func(t *testing.T) {
		bm := idx.NullBitmap()
		if bm.GetCardinality() != 2 {
			t.Errorf("expected 2 rows (empty array + null), got %d", bm.GetCardinality())
		}
	})
}

func TestFilterIndex_ArrayNumeric(t *testing.T) {
	idx := NewFilterIndex("scores", schema.TypeIntArray)

	idx.AddValue(0, []any{10, 20, 30})
	idx.AddValue(1, []any{5, 15})
	idx.AddValue(2, []any{25})

	idx.Finalize()

	// Test Contains for numeric array
	t.Run("ContainsNumeric", func(t *testing.T) {
		bm := idx.Contains(20)
		if bm.GetCardinality() != 1 {
			t.Errorf("expected 1 row, got %d", bm.GetCardinality())
		}
		if !bm.Contains(0) {
			t.Error("expected row 0")
		}
	})
}

func TestFilterIndex_Serialize(t *testing.T) {
	idx := NewFilterIndex("category", schema.TypeString)

	idx.AddValue(0, "a")
	idx.AddValue(1, "b")
	idx.AddValue(2, "a")
	idx.AddValue(3, nil)
	idx.AddMissing(4)

	idx.Finalize()

	// Serialize
	data, err := idx.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Deserialize
	idx2, err := Deserialize(data)
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	// Verify
	if idx2.header.Attribute != "category" {
		t.Errorf("expected attribute 'category', got %q", idx2.header.Attribute)
	}
	if idx2.header.Type != schema.TypeString {
		t.Errorf("expected type TypeString, got %v", idx2.header.Type)
	}
	if idx2.header.ValueCount != 2 {
		t.Errorf("expected 2 values, got %d", idx2.header.ValueCount)
	}

	// Verify queries work on deserialized index
	bm := idx2.Eq("a")
	if bm.GetCardinality() != 2 {
		t.Errorf("expected 2 rows, got %d", bm.GetCardinality())
	}

	nullBm := idx2.NullBitmap()
	if nullBm.GetCardinality() != 1 {
		t.Errorf("expected 1 null row, got %d", nullBm.GetCardinality())
	}

	missingBm := idx2.MissingBitmap()
	if missingBm.GetCardinality() != 1 {
		t.Errorf("expected 1 missing row, got %d", missingBm.GetCardinality())
	}
}

func TestFilterIndex_SerializeNumeric(t *testing.T) {
	idx := NewFilterIndex("price", schema.TypeFloat)

	idx.AddValue(0, 10.5)
	idx.AddValue(1, -5.0)
	idx.AddValue(2, 100.0)

	idx.Finalize()

	data, err := idx.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	idx2, err := Deserialize(data)
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	// Test range query on deserialized index
	bm := idx2.Gt(-5.0)
	if bm.GetCardinality() != 2 {
		t.Errorf("expected 2 rows, got %d", bm.GetCardinality())
	}
}

func TestFilterIndex_Bool(t *testing.T) {
	idx := NewFilterIndex("active", schema.TypeBool)

	idx.AddValue(0, true)
	idx.AddValue(1, false)
	idx.AddValue(2, true)
	idx.AddValue(3, nil)

	idx.Finalize()

	t.Run("EqTrue", func(t *testing.T) {
		bm := idx.Eq(true)
		if bm.GetCardinality() != 2 {
			t.Errorf("expected 2 rows, got %d", bm.GetCardinality())
		}
	})

	t.Run("EqFalse", func(t *testing.T) {
		bm := idx.Eq(false)
		if bm.GetCardinality() != 1 {
			t.Errorf("expected 1 row, got %d", bm.GetCardinality())
		}
	})
}

func TestSerializeValue_Ordering(t *testing.T) {
	// Test that serialized values maintain correct sort order

	t.Run("IntOrdering", func(t *testing.T) {
		s1 := serializeValue(int64(-10))
		s2 := serializeValue(int64(-5))
		s3 := serializeValue(int64(0))
		s4 := serializeValue(int64(5))
		s5 := serializeValue(int64(10))

		if !(s1 < s2 && s2 < s3 && s3 < s4 && s4 < s5) {
			t.Errorf("int ordering failed: %s, %s, %s, %s, %s", s1, s2, s3, s4, s5)
		}
	})

	t.Run("FloatOrdering", func(t *testing.T) {
		s1 := serializeValue(-100.5)
		s2 := serializeValue(-0.5)
		s3 := serializeValue(0.0)
		s4 := serializeValue(0.5)
		s5 := serializeValue(100.5)

		if !(s1 < s2 && s2 < s3 && s3 < s4 && s4 < s5) {
			t.Errorf("float ordering failed: %s, %s, %s, %s, %s", s1, s2, s3, s4, s5)
		}
	})

	t.Run("StringOrdering", func(t *testing.T) {
		s1 := serializeValue("a")
		s2 := serializeValue("b")
		s3 := serializeValue("z")

		if !(s1 < s2 && s2 < s3) {
			t.Errorf("string ordering failed")
		}
	})
}

func TestFilterIndex_MinMax(t *testing.T) {
	idx := NewFilterIndex("value", schema.TypeFloat)

	idx.AddValue(0, 10.0)
	idx.AddValue(1, -5.0)
	idx.AddValue(2, 100.0)
	idx.AddValue(3, 50.0)

	idx.Finalize()

	header := idx.Header()
	if header.MinValue.(float64) != -5.0 {
		t.Errorf("expected min -5.0, got %v", header.MinValue)
	}
	if header.MaxValue.(float64) != 100.0 {
		t.Errorf("expected max 100.0, got %v", header.MaxValue)
	}
}
