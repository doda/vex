// Package filter provides filter bitmap index building for the indexer.
package filter

import (
	"context"
	"fmt"

	"github.com/vexsearch/vex/internal/schema"
)

// IndexBuilder builds filter indexes for multiple filterable attributes.
type IndexBuilder struct {
	namespace string
	schema    *schema.Definition
	indexes   map[string]*FilterIndex // attribute name -> index
	rowCount  uint32
}

// NewIndexBuilder creates a new filter index builder.
func NewIndexBuilder(namespace string, schemaDef *schema.Definition) *IndexBuilder {
	return &IndexBuilder{
		namespace: namespace,
		schema:    schemaDef,
		indexes:   make(map[string]*FilterIndex),
	}
}

// AddDocument adds a document's attributes to the filter indexes.
// The rowID is the document's position in the segment.
func (b *IndexBuilder) AddDocument(rowID uint32, attrs map[string]any) {
	if b.schema == nil {
		return
	}

	for attrName, attr := range b.schema.Attributes {
		if !attr.IsFilterable() {
			continue
		}

		// Create index on first encounter
		if _, ok := b.indexes[attrName]; !ok {
			b.indexes[attrName] = NewFilterIndex(attrName, attr.Type)
		}

		value, exists := attrs[attrName]
		if !exists {
			value = nil
		}

		b.indexes[attrName].AddValue(rowID, value)
	}

	if rowID >= b.rowCount {
		b.rowCount = rowID + 1
	}
}

// Finalize prepares all indexes for serialization.
func (b *IndexBuilder) Finalize() {
	for _, idx := range b.indexes {
		idx.Finalize()
	}
}

// AttributeNames returns the names of attributes with indexes.
func (b *IndexBuilder) AttributeNames() []string {
	names := make([]string, 0, len(b.indexes))
	for name := range b.indexes {
		names = append(names, name)
	}
	return names
}

// GetIndex returns the filter index for the given attribute.
func (b *IndexBuilder) GetIndex(attrName string) *FilterIndex {
	return b.indexes[attrName]
}

// SerializeAll serializes all filter indexes.
// Returns a map of attribute name -> serialized bytes.
func (b *IndexBuilder) SerializeAll() (map[string][]byte, error) {
	b.Finalize()

	result := make(map[string][]byte, len(b.indexes))
	for name, idx := range b.indexes {
		data, err := idx.Serialize()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize filter index for %q: %w", name, err)
		}
		result[name] = data
	}
	return result, nil
}

// IndexReader provides read access to filter indexes for a segment.
type IndexReader struct {
	indexes map[string]*FilterIndex
}

// NewIndexReader creates a reader from serialized filter index data.
func NewIndexReader() *IndexReader {
	return &IndexReader{
		indexes: make(map[string]*FilterIndex),
	}
}

// LoadIndex loads a filter index from serialized data.
func (r *IndexReader) LoadIndex(attrName string, data []byte) error {
	idx, err := Deserialize(data)
	if err != nil {
		return fmt.Errorf("failed to deserialize filter index for %q: %w", attrName, err)
	}
	r.indexes[attrName] = idx
	return nil
}

// GetIndex returns the filter index for the given attribute.
func (r *IndexReader) GetIndex(attrName string) *FilterIndex {
	return r.indexes[attrName]
}

// HasIndex returns true if an index exists for the attribute.
func (r *IndexReader) HasIndex(attrName string) bool {
	_, ok := r.indexes[attrName]
	return ok
}

// FilterProcessor handles filter indexes for the indexer.
type FilterProcessor struct {
	builders map[string]*IndexBuilder // namespace -> builder
}

// NewFilterProcessor creates a new filter processor.
func NewFilterProcessor() *FilterProcessor {
	return &FilterProcessor{
		builders: make(map[string]*IndexBuilder),
	}
}

// GetOrCreateBuilder returns the builder for a namespace, creating it if needed.
func (p *FilterProcessor) GetOrCreateBuilder(namespace string, schemaDef *schema.Definition) *IndexBuilder {
	if b, ok := p.builders[namespace]; ok {
		return b
	}

	b := NewIndexBuilder(namespace, schemaDef)
	p.builders[namespace] = b
	return b
}

// ProcessDocument adds a document to the filter indexes.
func (p *FilterProcessor) ProcessDocument(ctx context.Context, namespace string, schemaDef *schema.Definition, rowID uint32, attrs map[string]any) error {
	b := p.GetOrCreateBuilder(namespace, schemaDef)
	b.AddDocument(rowID, attrs)
	return nil
}

// BuildAll serializes all filter indexes for all namespaces.
// Returns a map of namespace -> attribute name -> serialized bytes.
func (p *FilterProcessor) BuildAll() (map[string]map[string][]byte, error) {
	result := make(map[string]map[string][]byte, len(p.builders))
	for ns, builder := range p.builders {
		data, err := builder.SerializeAll()
		if err != nil {
			return nil, fmt.Errorf("namespace %q: %w", ns, err)
		}
		result[ns] = data
	}
	return result, nil
}

// Reset clears all builders.
func (p *FilterProcessor) Reset() {
	p.builders = make(map[string]*IndexBuilder)
}
