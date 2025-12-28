// Package fts provides full-text search indexing and tokenization.
package fts

import (
	"bytes"
	"context"
	"fmt"

	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// SegmentBuilder builds FTS indexes from WAL entries.
type SegmentBuilder struct {
	namespace string
	builder   *IndexBuilder

	// FTS configs per attribute
	ftsConfigs map[string]*Config

	// Mapping from document ID to internal row ID
	docIDMap map[string]uint32
	nextRowID uint32
}

// NewSegmentBuilder creates a new FTS segment builder.
func NewSegmentBuilder(namespace string, ftsConfigs map[string]*Config) *SegmentBuilder {
	return &SegmentBuilder{
		namespace:  namespace,
		builder:    NewIndexBuilder(),
		ftsConfigs: ftsConfigs,
		docIDMap:   make(map[string]uint32),
		nextRowID:  0,
	}
}

// AddWALEntry adds a WAL entry to the segment builder.
func (sb *SegmentBuilder) AddWALEntry(entry *wal.WalEntry) error {
	for _, batch := range entry.SubBatches {
		for _, mutation := range batch.Mutations {
			if err := sb.processMutation(mutation); err != nil {
				return fmt.Errorf("processing mutation: %w", err)
			}
		}
	}
	return nil
}

// processMutation processes a single mutation.
func (sb *SegmentBuilder) processMutation(mutation *wal.Mutation) error {
	if mutation.Type != wal.MutationType_MUTATION_TYPE_UPSERT {
		// Only process upserts for FTS indexing
		// Deletes and patches are handled via tombstones
		return nil
	}

	if len(sb.ftsConfigs) == 0 {
		// No FTS-enabled fields
		return nil
	}

	// Get or assign row ID for this document
	docKey := documentIDKey(mutation.Id)
	rowID, ok := sb.docIDMap[docKey]
	if !ok {
		rowID = sb.nextRowID
		sb.docIDMap[docKey] = rowID
		sb.nextRowID++
	}

	// Extract attributes as map[string]any
	attrs := make(map[string]any)
	for name, value := range mutation.Attributes {
		attrs[name] = attributeValueToAny(value)
	}

	// Add to index builder
	sb.builder.AddDocument(rowID, attrs, sb.ftsConfigs)
	return nil
}

// documentIDKey converts a document ID to a string key.
func documentIDKey(id *wal.DocumentID) string {
	if id == nil {
		return ""
	}
	switch v := id.GetId().(type) {
	case *wal.DocumentID_U64:
		return fmt.Sprintf("u64:%d", v.U64)
	case *wal.DocumentID_Uuid:
		return fmt.Sprintf("uuid:%x", v.Uuid)
	case *wal.DocumentID_Str:
		return fmt.Sprintf("str:%s", v.Str)
	default:
		return ""
	}
}

// attributeValueToAny converts a WAL AttributeValue to any type.
func attributeValueToAny(v *wal.AttributeValue) any {
	if v == nil {
		return nil
	}
	switch val := v.GetValue().(type) {
	case *wal.AttributeValue_StringVal:
		return val.StringVal
	case *wal.AttributeValue_IntVal:
		return val.IntVal
	case *wal.AttributeValue_UintVal:
		return val.UintVal
	case *wal.AttributeValue_FloatVal:
		return val.FloatVal
	case *wal.AttributeValue_DatetimeVal:
		return val.DatetimeVal
	case *wal.AttributeValue_BoolVal:
		return val.BoolVal
	case *wal.AttributeValue_NullVal:
		return nil
	case *wal.AttributeValue_UuidVal:
		return val.UuidVal
	case *wal.AttributeValue_StringArray:
		if val.StringArray != nil {
			return val.StringArray.Values
		}
		return nil
	case *wal.AttributeValue_IntArray:
		if val.IntArray != nil {
			return val.IntArray.Values
		}
		return nil
	case *wal.AttributeValue_UintArray:
		if val.UintArray != nil {
			return val.UintArray.Values
		}
		return nil
	case *wal.AttributeValue_FloatArray:
		if val.FloatArray != nil {
			return val.FloatArray.Values
		}
		return nil
	case *wal.AttributeValue_DatetimeArray:
		if val.DatetimeArray != nil {
			return val.DatetimeArray.Values
		}
		return nil
	case *wal.AttributeValue_BoolArray:
		if val.BoolArray != nil {
			return val.BoolArray.Values
		}
		return nil
	default:
		return nil
	}
}

// Build returns all built FTS indexes.
func (sb *SegmentBuilder) Build() map[string]*Index {
	return sb.builder.Build()
}

// HasData returns true if any FTS indexes have been built.
func (sb *SegmentBuilder) HasData() bool {
	for _, idx := range sb.builder.indexes {
		if idx.TotalDocs > 0 {
			return true
		}
	}
	return false
}

// SegmentResult contains the results of building FTS segment files.
type SegmentResult struct {
	// FTSKeys is a list of object storage keys for the FTS indexes.
	FTSKeys []string

	// Indexes contains the built indexes (for testing/inspection).
	Indexes map[string]*Index
}

// WriteToObjectStore writes the FTS indexes to object storage.
// segmentKey is the base key prefix for the segment (e.g., "vex/namespaces/ns/index/segments/seg_123")
func (sb *SegmentBuilder) WriteToObjectStore(ctx context.Context, store objectstore.Store, segmentKey string) (*SegmentResult, error) {
	indexes := sb.Build()
	if len(indexes) == 0 {
		return &SegmentResult{}, nil
	}

	result := &SegmentResult{
		Indexes: indexes,
	}

	for attrName, idx := range indexes {
		if idx.TotalDocs == 0 {
			continue
		}

		data, err := idx.Serialize()
		if err != nil {
			return nil, fmt.Errorf("serializing FTS index for %q: %w", attrName, err)
		}

		key := fmt.Sprintf("%s/fts/%s.idx", segmentKey, attrName)

		_, err = store.PutIfAbsent(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
		if err != nil {
			// Check if the object already exists (idempotent write)
			if objectstore.IsConflictError(err) {
				result.FTSKeys = append(result.FTSKeys, key)
				continue
			}
			return nil, fmt.Errorf("writing FTS index for %q: %w", attrName, err)
		}

		result.FTSKeys = append(result.FTSKeys, key)
	}

	return result, nil
}

