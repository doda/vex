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
	// Tracks which FTS fields are currently indexed for a document
	docFields map[string]map[string]bool
	// Tracks tombstoned documents
	deletedDocs map[string]bool
	nextRowID   uint32
}

// NewSegmentBuilder creates a new FTS segment builder.
func NewSegmentBuilder(namespace string, ftsConfigs map[string]*Config) *SegmentBuilder {
	return &SegmentBuilder{
		namespace:   namespace,
		builder:     NewIndexBuilder(),
		ftsConfigs:  ftsConfigs,
		docIDMap:    make(map[string]uint32),
		docFields:   make(map[string]map[string]bool),
		deletedDocs: make(map[string]bool),
		nextRowID:   0,
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
	if len(sb.ftsConfigs) == 0 {
		// No FTS-enabled fields
		return nil
	}

	if mutation.Id == nil {
		return nil
	}

	// Get or assign row ID for this document
	docKey := documentIDKey(mutation.Id)
	if docKey == "" {
		return nil
	}
	rowID, ok := sb.docIDMap[docKey]
	if !ok {
		rowID = sb.nextRowID
		sb.docIDMap[docKey] = rowID
		sb.nextRowID++
	}

	switch mutation.Type {
	case wal.MutationType_MUTATION_TYPE_DELETE:
		sb.applyDelete(docKey, rowID)
	case wal.MutationType_MUTATION_TYPE_PATCH:
		sb.applyPatch(docKey, rowID, mutation.Attributes)
	case wal.MutationType_MUTATION_TYPE_UPSERT:
		sb.applyUpsert(docKey, rowID, mutation.Attributes)
	}
	return nil
}

func (sb *SegmentBuilder) applyDelete(docKey string, rowID uint32) {
	fields := sb.docFields[docKey]
	for field := range fields {
		if idx := sb.builder.indexes[field]; idx != nil {
			idx.removeDocument(rowID)
		}
	}
	delete(sb.docFields, docKey)
	sb.deletedDocs[docKey] = true
}

func (sb *SegmentBuilder) applyPatch(docKey string, rowID uint32, attrs map[string]*wal.AttributeValue) {
	if sb.deletedDocs[docKey] {
		return
	}

	for name, value := range attrs {
		cfg := sb.ftsConfigs[name]
		if cfg == nil {
			continue
		}
		val := attributeValueToAny(value)
		text, ok := val.(string)
		if ok {
			idx := sb.ensureIndex(name, cfg)
			idx.AddDocument(rowID, text)
			fields := sb.ensureDocFields(docKey)
			fields[name] = true
			continue
		}

		sb.removeDocField(docKey, rowID, name)
	}
}

func (sb *SegmentBuilder) applyUpsert(docKey string, rowID uint32, attrs map[string]*wal.AttributeValue) {
	delete(sb.deletedDocs, docKey)

	for name, cfg := range sb.ftsConfigs {
		if cfg == nil {
			continue
		}
		val, ok := attrs[name]
		if !ok {
			sb.removeDocField(docKey, rowID, name)
			continue
		}
		conv := attributeValueToAny(val)
		text, ok := conv.(string)
		if !ok {
			sb.removeDocField(docKey, rowID, name)
			continue
		}
		idx := sb.ensureIndex(name, cfg)
		idx.AddDocument(rowID, text)
		fields := sb.ensureDocFields(docKey)
		fields[name] = true
	}
}

func (sb *SegmentBuilder) ensureIndex(name string, cfg *Config) *Index {
	idx := sb.builder.indexes[name]
	if idx == nil {
		idx = NewIndex(name, cfg)
		sb.builder.indexes[name] = idx
	}
	return idx
}

func (sb *SegmentBuilder) ensureDocFields(docKey string) map[string]bool {
	fields := sb.docFields[docKey]
	if fields == nil {
		fields = make(map[string]bool)
		sb.docFields[docKey] = fields
	}
	return fields
}

func (sb *SegmentBuilder) removeDocField(docKey string, rowID uint32, name string) {
	fields := sb.docFields[docKey]
	if fields == nil || !fields[name] {
		return
	}
	if idx := sb.builder.indexes[name]; idx != nil {
		idx.removeDocument(rowID)
	}
	delete(fields, name)
	if len(fields) == 0 {
		delete(sb.docFields, docKey)
	}
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

		key := fmt.Sprintf("%s/fts.%s.bm25", segmentKey, attrName)

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
