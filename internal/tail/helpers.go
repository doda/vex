package tail

import (
	"encoding/binary"
	"io"
	"math"
	"sort"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/wal"
)

// readAll reads all bytes from a reader.
func readAll(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}

// materializeEntry converts a WAL entry into materialized documents.
func materializeEntry(entry *wal.WalEntry) []*Document {
	var docs []*Document

	for batchIdx, batch := range entry.SubBatches {
		for _, mutation := range batch.Mutations {
			doc := &Document{
				WalSeq:     entry.Seq,
				SubBatchID: batchIdx,
			}

			// Convert document ID
			if mutation.Id != nil {
				id, err := wal.DocumentIDToID(mutation.Id)
				if err == nil {
					doc.ID = id
				}
			}

			switch mutation.Type {
			case wal.MutationType_MUTATION_TYPE_DELETE:
				doc.Deleted = true

			case wal.MutationType_MUTATION_TYPE_UPSERT, wal.MutationType_MUTATION_TYPE_PATCH:
				doc.Attributes = convertAttributes(mutation.Attributes)
				if len(mutation.Vector) > 0 {
					doc.Vector = decodeVector(mutation.Vector, mutation.VectorDims)
				}
			}

			docs = append(docs, doc)
		}
	}

	return docs
}

// convertAttributes converts WAL attribute values to native Go types.
func convertAttributes(attrs map[string]*wal.AttributeValue) map[string]any {
	if attrs == nil {
		return nil
	}

	result := make(map[string]any, len(attrs))
	for k, v := range attrs {
		result[k] = convertAttributeValue(v)
	}
	return result
}

// convertAttributeValue converts a single WAL attribute value.
func convertAttributeValue(v *wal.AttributeValue) any {
	if v == nil {
		return nil
	}

	switch val := v.Value.(type) {
	case *wal.AttributeValue_StringVal:
		return val.StringVal
	case *wal.AttributeValue_IntVal:
		return val.IntVal
	case *wal.AttributeValue_UintVal:
		return val.UintVal
	case *wal.AttributeValue_FloatVal:
		return val.FloatVal
	case *wal.AttributeValue_UuidVal:
		return val.UuidVal
	case *wal.AttributeValue_DatetimeVal:
		return val.DatetimeVal
	case *wal.AttributeValue_BoolVal:
		return val.BoolVal
	case *wal.AttributeValue_NullVal:
		return nil
	case *wal.AttributeValue_StringArray:
		if val.StringArray != nil {
			return val.StringArray.Values
		}
	case *wal.AttributeValue_IntArray:
		if val.IntArray != nil {
			return val.IntArray.Values
		}
	case *wal.AttributeValue_UintArray:
		if val.UintArray != nil {
			return val.UintArray.Values
		}
	case *wal.AttributeValue_FloatArray:
		if val.FloatArray != nil {
			return val.FloatArray.Values
		}
	case *wal.AttributeValue_UuidArray:
		if val.UuidArray != nil {
			return val.UuidArray.Values
		}
	case *wal.AttributeValue_DatetimeArray:
		if val.DatetimeArray != nil {
			return val.DatetimeArray.Values
		}
	case *wal.AttributeValue_BoolArray:
		if val.BoolArray != nil {
			return val.BoolArray.Values
		}
	}

	return nil
}

// decodeVector decodes a raw byte slice into float32 vector.
func decodeVector(data []byte, dims uint32) []float32 {
	if len(data) == 0 {
		return nil
	}

	// Determine format based on size
	if uint32(len(data)) == dims*4 {
		// float32 format
		vec := make([]float32, dims)
		for i := uint32(0); i < dims; i++ {
			bits := binary.LittleEndian.Uint32(data[i*4:])
			vec[i] = math.Float32frombits(bits)
		}
		return vec
	} else if uint32(len(data)) == dims*2 {
		// float16 format - convert to float32
		vec := make([]float32, dims)
		for i := uint32(0); i < dims; i++ {
			bits := binary.LittleEndian.Uint16(data[i*2:])
			vec[i] = float16ToFloat32(bits)
		}
		return vec
	}

	return nil
}

// float16ToFloat32 converts a float16 (half-precision) to float32.
func float16ToFloat32(h uint16) float32 {
	sign := uint32((h >> 15) & 1)
	exp := uint32((h >> 10) & 0x1F)
	mant := uint32(h & 0x3FF)

	var f uint32

	if exp == 0 {
		if mant == 0 {
			// Zero
			f = sign << 31
		} else {
			// Subnormal
			for mant&0x400 == 0 {
				mant <<= 1
				exp--
			}
			exp++
			mant &= 0x3FF
			f = (sign << 31) | ((exp + 127 - 15) << 23) | (mant << 13)
		}
	} else if exp == 31 {
		// Inf or NaN
		f = (sign << 31) | 0x7F800000 | (mant << 13)
	} else {
		// Normal
		f = (sign << 31) | ((exp + 127 - 15) << 23) | (mant << 13)
	}

	return math.Float32frombits(f)
}

// computeDistance computes the distance between two vectors.
func computeDistance(a, b []float32, metric DistanceMetric) float64 {
	if len(a) != len(b) {
		return math.MaxFloat64
	}

	switch metric {
	case MetricCosineDistance:
		return cosineDistance(a, b)
	case MetricEuclideanSquared:
		return euclideanSquared(a, b)
	case MetricDotProduct:
		return -dotProduct(a, b)
	default:
		return cosineDistance(a, b)
	}
}

// cosineDistance computes 1 - cosine_similarity(a, b).
func cosineDistance(a, b []float32) float64 {
	var dot, normA, normB float64
	for i := 0; i < len(a); i++ {
		ai := float64(a[i])
		bi := float64(b[i])
		dot += ai * bi
		normA += ai * ai
		normB += bi * bi
	}

	if normA == 0 || normB == 0 {
		return 1.0
	}

	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return 1.0 - similarity
}

// euclideanSquared computes sum((a[i] - b[i])^2).
func euclideanSquared(a, b []float32) float64 {
	var sum float64
	for i := 0; i < len(a); i++ {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return sum
}

// dotProduct computes sum(a[i] * b[i]).
func dotProduct(a, b []float32) float64 {
	var sum float64
	for i := 0; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

// sortByDistance sorts results by distance ascending.
func sortByDistance(results []VectorScanResult) {
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})
}

// estimateAttributeSize estimates the memory size of an attribute value.
func estimateAttributeSize(v *wal.AttributeValue) int64 {
	if v == nil {
		return 0
	}

	switch val := v.Value.(type) {
	case *wal.AttributeValue_StringVal:
		return int64(len(val.StringVal))
	case *wal.AttributeValue_IntVal, *wal.AttributeValue_UintVal,
		*wal.AttributeValue_FloatVal, *wal.AttributeValue_DatetimeVal:
		return 8
	case *wal.AttributeValue_UuidVal:
		return 16
	case *wal.AttributeValue_BoolVal:
		return 1
	case *wal.AttributeValue_StringArray:
		if val.StringArray != nil {
			size := int64(0)
			for _, s := range val.StringArray.Values {
				size += int64(len(s))
			}
			return size
		}
	case *wal.AttributeValue_IntArray:
		if val.IntArray != nil {
			return int64(len(val.IntArray.Values) * 8)
		}
	case *wal.AttributeValue_UintArray:
		if val.UintArray != nil {
			return int64(len(val.UintArray.Values) * 8)
		}
	case *wal.AttributeValue_FloatArray:
		if val.FloatArray != nil {
			return int64(len(val.FloatArray.Values) * 8)
		}
	case *wal.AttributeValue_UuidArray:
		if val.UuidArray != nil {
			return int64(len(val.UuidArray.Values) * 16)
		}
	case *wal.AttributeValue_DatetimeArray:
		if val.DatetimeArray != nil {
			return int64(len(val.DatetimeArray.Values) * 8)
		}
	case *wal.AttributeValue_BoolArray:
		if val.BoolArray != nil {
			return int64(len(val.BoolArray.Values))
		}
	}

	return 0
}

// documentIDFromProto converts a protobuf DocumentID to document.ID.
func documentIDFromProto(protoID *wal.DocumentID) (document.ID, error) {
	return wal.DocumentIDToID(protoID)
}
