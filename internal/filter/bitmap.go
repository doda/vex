// Package filter provides filter bitmap indexes using roaring bitmaps.
package filter

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/google/uuid"
	"github.com/vexsearch/vex/internal/schema"
)

var (
	// ErrInvalidBitmapFormat is returned when bitmap data is corrupted.
	ErrInvalidBitmapFormat = errors.New("invalid bitmap index format")
)

// FilterIndexHeader contains metadata about the filter index.
type FilterIndexHeader struct {
	FormatVersion int             `json:"format_version"`
	Attribute     string          `json:"attribute"`
	Type          schema.AttrType `json:"type"`
	ValueCount    int             `json:"value_count"`
	DocCount      uint32          `json:"doc_count"`
	IsArray       bool            `json:"is_array"`
	MinValue      any             `json:"min_value,omitempty"`
	MaxValue      any             `json:"max_value,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

// FilterIndex represents a roaring bitmap index for a single filterable attribute.
// It maps values to document row IDs.
type FilterIndex struct {
	header        FilterIndexHeader
	valueBitmaps  map[string]*roaring.Bitmap // value (serialized) -> bitmap of row IDs
	nullBitmap    *roaring.Bitmap            // rows with explicit null values
	missingBitmap *roaring.Bitmap            // rows where attribute is missing
	sortedKeys    []string                   // sorted keys for range queries
}

// NewFilterIndex creates a new filter index for the given attribute.
func NewFilterIndex(attrName string, attrType schema.AttrType) *FilterIndex {
	return &FilterIndex{
		header: FilterIndexHeader{
			FormatVersion: 2,
			Attribute:     attrName,
			Type:          attrType,
			IsArray:       attrType.IsArray(),
			CreatedAt:     time.Now().UTC(),
		},
		valueBitmaps:  make(map[string]*roaring.Bitmap),
		nullBitmap:    roaring.NewBitmap(),
		missingBitmap: roaring.NewBitmap(),
	}
}

// AddValue adds a document with the given row ID and value to the index.
// For array values, each element is indexed separately.
func (idx *FilterIndex) AddValue(rowID uint32, value any) {
	if value == nil {
		idx.nullBitmap.Add(rowID)
		return
	}

	// Handle arrays: index each element
	if idx.header.IsArray {
		arr := toAnySlice(value)
		if len(arr) == 0 {
			idx.nullBitmap.Add(rowID)
			return
		}
		for _, elem := range arr {
			idx.addSingleValue(rowID, elem)
		}
		return
	}

	idx.addSingleValue(rowID, value)
}

// AddMissing marks a document where the attribute is missing.
func (idx *FilterIndex) AddMissing(rowID uint32) {
	idx.missingBitmap.Add(rowID)
}

// addSingleValue adds a single value to the index.
func (idx *FilterIndex) addSingleValue(rowID uint32, value any) {
	key, ok := idx.serializeValue(value)
	if !ok {
		idx.nullBitmap.Add(rowID)
		return
	}

	bm, ok := idx.valueBitmaps[key]
	if !ok {
		bm = roaring.NewBitmap()
		idx.valueBitmaps[key] = bm
	}
	bm.Add(rowID)

	// Track min/max for numeric types
	idx.updateMinMax(value)
}

// updateMinMax updates the min/max values for range query optimization.
func (idx *FilterIndex) updateMinMax(value any) {
	elemType := idx.header.Type.ElementType()

	switch elemType {
	case schema.TypeInt, schema.TypeUint, schema.TypeFloat, schema.TypeDatetime:
		numVal, ok := valueToFloat64(value)
		if !ok {
			return
		}
		if idx.header.MinValue == nil {
			idx.header.MinValue = numVal
			idx.header.MaxValue = numVal
		} else {
			if numVal < idx.header.MinValue.(float64) {
				idx.header.MinValue = numVal
			}
			if numVal > idx.header.MaxValue.(float64) {
				idx.header.MaxValue = numVal
			}
		}
	case schema.TypeString:
		strVal, ok := value.(string)
		if !ok {
			return
		}
		if idx.header.MinValue == nil {
			idx.header.MinValue = strVal
			idx.header.MaxValue = strVal
		} else {
			if strVal < idx.header.MinValue.(string) {
				idx.header.MinValue = strVal
			}
			if strVal > idx.header.MaxValue.(string) {
				idx.header.MaxValue = strVal
			}
		}
	}
}

// Finalize prepares the index for serialization.
func (idx *FilterIndex) Finalize() {
	idx.header.ValueCount = len(idx.valueBitmaps)
	idx.header.DocCount = idx.GetDocCount()

	// Build sorted keys for range queries
	idx.sortedKeys = make([]string, 0, len(idx.valueBitmaps))
	for k := range idx.valueBitmaps {
		idx.sortedKeys = append(idx.sortedKeys, k)
	}
	sort.Strings(idx.sortedKeys)

	// Optimize all bitmaps for size
	for _, bm := range idx.valueBitmaps {
		bm.RunOptimize()
	}
	idx.nullBitmap.RunOptimize()
	idx.missingBitmap.RunOptimize()
}

// GetDocCount returns the total number of unique documents in the index.
func (idx *FilterIndex) GetDocCount() uint32 {
	all := roaring.NewBitmap()
	for _, bm := range idx.valueBitmaps {
		all.Or(bm)
	}
	all.Or(idx.nullBitmap)
	all.Or(idx.missingBitmap)
	return uint32(all.GetCardinality())
}

// Eq returns the bitmap of rows where the attribute equals the given value.
func (idx *FilterIndex) Eq(value any) *roaring.Bitmap {
	if value == nil {
		return idx.missingBitmap.Clone()
	}

	key, ok := idx.serializeValue(value)
	if !ok {
		return roaring.NewBitmap()
	}
	if bm, ok := idx.valueBitmaps[key]; ok {
		return bm.Clone()
	}
	return roaring.NewBitmap()
}

// NotEq returns the bitmap of rows where the attribute does not equal the given value.
func (idx *FilterIndex) NotEq(value any, allRows *roaring.Bitmap) *roaring.Bitmap {
	if value == nil {
		result := allRows.Clone()
		result.AndNot(idx.missingBitmap)
		return result
	}
	eq := idx.Eq(value)
	result := allRows.Clone()
	result.AndNot(eq)
	return result
}

// In returns the bitmap of rows where the attribute is in the given set.
func (idx *FilterIndex) In(values []any) *roaring.Bitmap {
	result := roaring.NewBitmap()
	for _, v := range values {
		eq := idx.Eq(v)
		result.Or(eq)
	}
	return result
}

// Lt returns the bitmap of rows where the attribute is less than the given value.
func (idx *FilterIndex) Lt(value any) *roaring.Bitmap {
	return idx.rangeQuery(value, false, false, true)
}

// Lte returns the bitmap of rows where the attribute is <= the given value.
func (idx *FilterIndex) Lte(value any) *roaring.Bitmap {
	return idx.rangeQuery(value, false, true, true)
}

// Gt returns the bitmap of rows where the attribute is greater than the given value.
func (idx *FilterIndex) Gt(value any) *roaring.Bitmap {
	return idx.rangeQuery(value, true, false, false)
}

// Gte returns the bitmap of rows where the attribute is >= the given value.
func (idx *FilterIndex) Gte(value any) *roaring.Bitmap {
	return idx.rangeQuery(value, true, true, false)
}

// Range returns the bitmap of rows where the attribute is in [minVal, maxVal].
func (idx *FilterIndex) Range(minVal, maxVal any, includeMin, includeMax bool) *roaring.Bitmap {
	result := roaring.NewBitmap()

	targetMinKey, ok := idx.serializeValue(minVal)
	if !ok {
		return result
	}
	targetMaxKey, ok := idx.serializeValue(maxVal)
	if !ok {
		return result
	}

	for _, key := range idx.sortedKeys {
		include := false
		if includeMin && key >= targetMinKey || !includeMin && key > targetMinKey {
			if includeMax && key <= targetMaxKey || !includeMax && key < targetMaxKey {
				include = true
			}
		}
		if include {
			result.Or(idx.valueBitmaps[key])
		}
	}
	return result
}

// rangeQuery performs a range query based on comparison parameters.
func (idx *FilterIndex) rangeQuery(value any, above, includeEqual, below bool) *roaring.Bitmap {
	result := roaring.NewBitmap()
	targetKey, ok := idx.serializeValue(value)
	if !ok {
		return result
	}

	for _, key := range idx.sortedKeys {
		include := false
		if above {
			if includeEqual && key >= targetKey || !includeEqual && key > targetKey {
				include = true
			}
		} else if below {
			if includeEqual && key <= targetKey || !includeEqual && key < targetKey {
				include = true
			}
		}
		if include {
			result.Or(idx.valueBitmaps[key])
		}
	}
	return result
}

// Contains returns the bitmap of rows where the array attribute contains the value.
// Only applicable for array types.
func (idx *FilterIndex) Contains(value any) *roaring.Bitmap {
	if !idx.header.IsArray {
		return roaring.NewBitmap()
	}
	return idx.Eq(value)
}

// ContainsAny returns rows where the array contains any of the values.
func (idx *FilterIndex) ContainsAny(values []any) *roaring.Bitmap {
	if !idx.header.IsArray {
		return roaring.NewBitmap()
	}
	return idx.In(values)
}

// NullBitmap returns the bitmap of rows with explicit null values.
func (idx *FilterIndex) NullBitmap() *roaring.Bitmap {
	return idx.nullBitmap.Clone()
}

// MissingBitmap returns the bitmap of rows where the attribute is missing.
func (idx *FilterIndex) MissingBitmap() *roaring.Bitmap {
	return idx.missingBitmap.Clone()
}

// AllValuesBitmap returns a bitmap of all rows with non-null values.
func (idx *FilterIndex) AllValuesBitmap() *roaring.Bitmap {
	result := roaring.NewBitmap()
	for _, bm := range idx.valueBitmaps {
		result.Or(bm)
	}
	return result
}

// Serialize writes the filter index to a byte slice.
func (idx *FilterIndex) Serialize() ([]byte, error) {
	idx.Finalize()

	var buf bytes.Buffer

	// Write header
	headerBytes, err := json.Marshal(idx.header)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal header: %w", err)
	}

	// Write header length and header
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(headerBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(headerBytes); err != nil {
		return nil, err
	}

	// Write null bitmap
	nullBytes, err := idx.nullBitmap.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize null bitmap: %w", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(nullBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(nullBytes); err != nil {
		return nil, err
	}

	// Write missing bitmap
	missingBytes, err := idx.missingBitmap.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize missing bitmap: %w", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(missingBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(missingBytes); err != nil {
		return nil, err
	}

	// Write sorted keys count
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(idx.sortedKeys))); err != nil {
		return nil, err
	}

	// Write each key and its bitmap
	for _, key := range idx.sortedKeys {
		// Write key length and key
		keyBytes := []byte(key)
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return nil, err
		}

		// Write bitmap
		bm := idx.valueBitmaps[key]
		bmBytes, err := bm.ToBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize bitmap for key %s: %w", key, err)
		}
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(bmBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(bmBytes); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// Deserialize reads a filter index from a byte slice.
func Deserialize(data []byte) (*FilterIndex, error) {
	if len(data) < 4 {
		return nil, ErrInvalidBitmapFormat
	}

	r := bytes.NewReader(data)

	// Read header length and header
	var headerLen uint32
	if err := binary.Read(r, binary.LittleEndian, &headerLen); err != nil {
		return nil, fmt.Errorf("failed to read header length: %w", err)
	}
	if headerLen > uint32(len(data)) {
		return nil, ErrInvalidBitmapFormat
	}

	headerBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(r, headerBytes); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	var header FilterIndexHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("failed to unmarshal header: %w", err)
	}

	idx := &FilterIndex{
		header:        header,
		valueBitmaps:  make(map[string]*roaring.Bitmap),
		nullBitmap:    roaring.NewBitmap(),
		missingBitmap: roaring.NewBitmap(),
	}

	// Read null bitmap
	var nullLen uint32
	if err := binary.Read(r, binary.LittleEndian, &nullLen); err != nil {
		return nil, fmt.Errorf("failed to read null bitmap length: %w", err)
	}

	nullBytes := make([]byte, nullLen)
	if _, err := io.ReadFull(r, nullBytes); err != nil {
		return nil, fmt.Errorf("failed to read null bitmap: %w", err)
	}

	if len(nullBytes) > 0 {
		if err := idx.nullBitmap.UnmarshalBinary(nullBytes); err != nil {
			return nil, fmt.Errorf("failed to deserialize null bitmap: %w", err)
		}
	}

	if header.FormatVersion >= 2 {
		var missingLen uint32
		if err := binary.Read(r, binary.LittleEndian, &missingLen); err != nil {
			return nil, fmt.Errorf("failed to read missing bitmap length: %w", err)
		}

		missingBytes := make([]byte, missingLen)
		if _, err := io.ReadFull(r, missingBytes); err != nil {
			return nil, fmt.Errorf("failed to read missing bitmap: %w", err)
		}
		if len(missingBytes) > 0 {
			if err := idx.missingBitmap.UnmarshalBinary(missingBytes); err != nil {
				return nil, fmt.Errorf("failed to deserialize missing bitmap: %w", err)
			}
		}
	} else {
		idx.missingBitmap = idx.nullBitmap.Clone()
	}

	// Read key count
	var keyCount uint32
	if err := binary.Read(r, binary.LittleEndian, &keyCount); err != nil {
		return nil, fmt.Errorf("failed to read key count: %w", err)
	}

	idx.sortedKeys = make([]string, 0, keyCount)

	// Read each key and bitmap
	for i := uint32(0); i < keyCount; i++ {
		// Read key
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read key length: %w", err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}
		key := string(keyBytes)

		// Read bitmap
		var bmLen uint32
		if err := binary.Read(r, binary.LittleEndian, &bmLen); err != nil {
			return nil, fmt.Errorf("failed to read bitmap length: %w", err)
		}
		bmBytes := make([]byte, bmLen)
		if _, err := io.ReadFull(r, bmBytes); err != nil {
			return nil, fmt.Errorf("failed to read bitmap: %w", err)
		}

		bm := roaring.NewBitmap()
		if len(bmBytes) > 0 {
			if err := bm.UnmarshalBinary(bmBytes); err != nil {
				return nil, fmt.Errorf("failed to deserialize bitmap: %w", err)
			}
		}

		idx.sortedKeys = append(idx.sortedKeys, key)
		idx.valueBitmaps[key] = bm
	}

	return idx, nil
}

// Header returns the filter index header.
func (idx *FilterIndex) Header() FilterIndexHeader {
	return idx.header
}

func (idx *FilterIndex) serializeValue(value any) (string, bool) {
	if value == nil {
		return "", false
	}

	switch idx.header.Type.ElementType() {
	case schema.TypeString:
		if v, ok := value.(string); ok {
			return "s" + v, true
		}
		return "", false
	case schema.TypeInt:
		v, ok := normalizeInt64(value)
		if !ok {
			return "", false
		}
		return serializeInt64(v), true
	case schema.TypeUint:
		v, ok := normalizeUint64(value)
		if !ok {
			return "", false
		}
		return serializeUint64(v), true
	case schema.TypeFloat:
		v, ok := normalizeFloat64(value)
		if !ok {
			return "", false
		}
		return serializeFloat64(v), true
	case schema.TypeDatetime:
		v, ok := normalizeDatetimeMillis(value)
		if !ok {
			return "", false
		}
		return serializeInt64(v), true
	case schema.TypeBool:
		if v, ok := value.(bool); ok {
			if v {
				return "b1", true
			}
			return "b0", true
		}
		return "", false
	case schema.TypeUUID:
		u, ok := normalizeUUID(value)
		if !ok {
			return "", false
		}
		return "g" + u.String(), true
	default:
		return "", false
	}
}

// serializeValue converts a value to a sortable string key.
// This ensures consistent ordering for range queries.
func serializeValue(value any) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		// Prefix with type byte for sortability
		return "s" + v
	case int:
		return serializeInt64(int64(v))
	case int8:
		return serializeInt64(int64(v))
	case int16:
		return serializeInt64(int64(v))
	case int32:
		return serializeInt64(int64(v))
	case int64:
		return serializeInt64(v)
	case uint:
		return serializeUint64(uint64(v))
	case uint8:
		return serializeUint64(uint64(v))
	case uint16:
		return serializeUint64(uint64(v))
	case uint32:
		return serializeUint64(uint64(v))
	case uint64:
		return serializeUint64(v)
	case float32:
		return serializeFloat64(float64(v))
	case float64:
		return serializeFloat64(v)
	case bool:
		if v {
			return "b1"
		}
		return "b0"
	case time.Time:
		// Store as milliseconds since epoch
		return serializeInt64(v.UnixMilli())
	case uuid.UUID:
		return "g" + v.String()
	default:
		// Fallback: use JSON encoding
		data, _ := json.Marshal(v)
		return "j" + string(data)
	}
}

// serializeInt64 converts an int64 to a sortable string.
// Uses offset binary encoding to maintain sort order.
func serializeInt64(v int64) string {
	// Add offset to make all values positive for correct string sorting
	u := uint64(v) + (1 << 63)
	return "i" + fmt.Sprintf("%016x", u)
}

// serializeUint64 converts a uint64 to a sortable string.
func serializeUint64(v uint64) string {
	return "u" + fmt.Sprintf("%016x", v)
}

// serializeFloat64 converts a float64 to a sortable string.
// Uses IEEE 754 bit representation with sign adjustment.
func serializeFloat64(v float64) string {
	bits := math.Float64bits(v)
	if v >= 0 {
		bits ^= (1 << 63) // Flip sign bit for positive
	} else {
		bits = ^bits // Invert all bits for negative
	}
	return "f" + fmt.Sprintf("%016x", bits)
}

// valueToFloat64 converts a value to float64 for min/max tracking.
func valueToFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case time.Time:
		return float64(v.UnixMilli()), true
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339, v)
		}
		if err != nil {
			return 0, false
		}
		return float64(parsed.UnixMilli()), true
	default:
		return 0, false
	}
}

// toAnySlice converts various slice types to []any.
func toAnySlice(v any) []any {
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
	case []int16:
		result := make([]any, len(arr))
		for i, n := range arr {
			result[i] = n
		}
		return result
	case []int32:
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
	case []uint:
		result := make([]any, len(arr))
		for i, n := range arr {
			result[i] = n
		}
		return result
	case []uint16:
		result := make([]any, len(arr))
		for i, n := range arr {
			result[i] = n
		}
		return result
	case []uint32:
		result := make([]any, len(arr))
		for i, n := range arr {
			result[i] = n
		}
		return result
	case []uint64:
		result := make([]any, len(arr))
		for i, n := range arr {
			result[i] = n
		}
		return result
	case []float32:
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
	case []time.Time:
		result := make([]any, len(arr))
		for i, ts := range arr {
			result[i] = ts
		}
		return result
	case []uuid.UUID:
		result := make([]any, len(arr))
		for i, id := range arr {
			result[i] = id
		}
		return result
	default:
		return nil
	}
}

func normalizeInt64(value any) (int64, bool) {
	const maxInt64 = int64(^uint64(0) >> 1)
	const minInt64 = -maxInt64 - 1

	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		if uint64(v) > uint64(maxInt64) {
			return 0, false
		}
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > uint64(maxInt64) {
			return 0, false
		}
		return int64(v), true
	case float64:
		if v != math.Trunc(v) {
			return 0, false
		}
		if v < float64(minInt64) || v > float64(maxInt64) {
			return 0, false
		}
		return int64(v), true
	case float32:
		f := float64(v)
		if f != math.Trunc(f) {
			return 0, false
		}
		return int64(f), true
	default:
		return 0, false
	}
}

func normalizeUint64(value any) (uint64, bool) {
	switch v := value.(type) {
	case uint:
		return uint64(v), true
	case uint8:
		return uint64(v), true
	case uint16:
		return uint64(v), true
	case uint32:
		return uint64(v), true
	case uint64:
		return v, true
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int8:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int16:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int32:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case float64:
		if v < 0 || v != math.Trunc(v) {
			return 0, false
		}
		return uint64(v), true
	case float32:
		f := float64(v)
		if f < 0 || f != math.Trunc(f) {
			return 0, false
		}
		return uint64(f), true
	default:
		return 0, false
	}
}

func normalizeFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

func normalizeDatetimeMillis(value any) (int64, bool) {
	const maxInt64 = int64(^uint64(0) >> 1)

	switch v := value.(type) {
	case time.Time:
		return v.UnixMilli(), true
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339, v)
		}
		if err != nil {
			return 0, false
		}
		return parsed.UnixMilli(), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	case uint64:
		if v > uint64(maxInt64) {
			return 0, false
		}
		return int64(v), true
	case uint:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	default:
		return 0, false
	}
}

func normalizeUUID(value any) (uuid.UUID, bool) {
	switch v := value.(type) {
	case uuid.UUID:
		return v, true
	case []byte:
		if len(v) != 16 {
			return uuid.UUID{}, false
		}
		id, err := uuid.FromBytes(v)
		if err != nil {
			return uuid.UUID{}, false
		}
		return id, true
	case string:
		id, err := uuid.Parse(v)
		if err != nil {
			return uuid.UUID{}, false
		}
		return id, true
	default:
		return uuid.UUID{}, false
	}
}
