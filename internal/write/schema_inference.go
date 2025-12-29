package write

import (
	"encoding/json"
	"fmt"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/schema"
	"github.com/vexsearch/vex/internal/wal"
)

type schemaCollector struct {
	existing map[string]schema.AttrType
	inferred map[string]schema.AttrType
}

func newSchemaCollector(existingSchema *namespace.Schema, explicitDelta *namespace.Schema) *schemaCollector {
	existing := make(map[string]schema.AttrType)
	if existingSchema != nil {
		for name, attr := range existingSchema.Attributes {
			existing[name] = schema.AttrType(attr.Type)
		}
	}
	if explicitDelta != nil {
		for name, attr := range explicitDelta.Attributes {
			existing[name] = schema.AttrType(attr.Type)
		}
	}
	return &schemaCollector{
		existing: existing,
		inferred: make(map[string]schema.AttrType),
	}
}

func (c *schemaCollector) ObserveAttributes(attrs map[string]any) error {
	for name, value := range attrs {
		if err := c.Observe(name, value); err != nil {
			return err
		}
	}
	return nil
}

func (c *schemaCollector) Observe(name string, value any) error {
	if name == "id" || name == "vector" {
		return nil
	}
	if value == nil {
		return nil
	}
	if _, ok := c.existing[name]; ok {
		return nil
	}
	inferredType, err := schema.InferTypeFromJSON(value)
	if err != nil {
		return fmt.Errorf("%w: attribute %q: %v", ErrInvalidSchema, name, err)
	}
	if existingType, ok := c.inferred[name]; ok && existingType != inferredType {
		return fmt.Errorf("%w: attribute %q has type %s, cannot change to %s",
			ErrSchemaTypeChange, name, existingType, inferredType)
	}
	c.inferred[name] = inferredType
	c.existing[name] = inferredType
	return nil
}

func (c *schemaCollector) Delta() *namespace.Schema {
	if len(c.inferred) == 0 {
		return nil
	}
	delta := &namespace.Schema{Attributes: make(map[string]namespace.AttributeSchema, len(c.inferred))}
	for name, attrType := range c.inferred {
		delta.Attributes[name] = namespace.AttributeSchema{
			Type: attrType.String(),
		}
	}
	return delta
}

func mergeSchemaDelta(dst, src *namespace.Schema) *namespace.Schema {
	if src == nil {
		return dst
	}
	if dst == nil {
		return src
	}
	if dst.Attributes == nil {
		dst.Attributes = make(map[string]namespace.AttributeSchema)
	}
	for name, attr := range src.Attributes {
		dst.Attributes[name] = attr
	}
	return dst
}

func schemaDeltaToWAL(delta *namespace.Schema) ([]*wal.SchemaDelta, error) {
	if delta == nil || len(delta.Attributes) == 0 {
		return nil, nil
	}
	deltas := make([]*wal.SchemaDelta, 0, len(delta.Attributes))
	for name, attr := range delta.Attributes {
		attrType := schema.AttrType(attr.Type)
		walType, ok := schemaTypeToWALType[attrType]
		if !ok {
			return nil, fmt.Errorf("%w: attribute %q has invalid type %q", ErrInvalidSchema, name, attr.Type)
		}
		ftsConfig, err := parseFullTextConfig(attr.FullTextSearch)
		if err != nil {
			return nil, fmt.Errorf("%w: attribute %q full_text_search: %v", ErrInvalidSchema, name, err)
		}
		deltas = append(deltas, &wal.SchemaDelta{
			Name:           name,
			Type:           walType,
			Filterable:     isFilterable(attr),
			Regex:          attr.Regex,
			FullTextSearch: ftsConfig,
		})
	}
	return deltas, nil
}

func isFilterable(attr namespace.AttributeSchema) bool {
	if attr.Filterable != nil {
		return *attr.Filterable
	}
	if attr.Regex || len(attr.FullTextSearch) > 0 {
		return false
	}
	return true
}

func parseFullTextConfig(raw json.RawMessage) (*wal.FullTextConfig, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	cfg, err := schema.ParseFullTextSearch(value)
	if err != nil || cfg == nil {
		return nil, err
	}
	return &wal.FullTextConfig{
		Enabled:   true,
		Tokenizer: cfg.Tokenizer,
		Language:  cfg.Language,
		Stemming:  cfg.Stemming,
	}, nil
}

var schemaTypeToWALType = map[schema.AttrType]wal.AttributeType{
	schema.TypeString:        wal.AttributeType_ATTRIBUTE_TYPE_STRING,
	schema.TypeInt:           wal.AttributeType_ATTRIBUTE_TYPE_INT,
	schema.TypeUint:          wal.AttributeType_ATTRIBUTE_TYPE_UINT,
	schema.TypeFloat:         wal.AttributeType_ATTRIBUTE_TYPE_FLOAT,
	schema.TypeUUID:          wal.AttributeType_ATTRIBUTE_TYPE_UUID,
	schema.TypeDatetime:      wal.AttributeType_ATTRIBUTE_TYPE_DATETIME,
	schema.TypeBool:          wal.AttributeType_ATTRIBUTE_TYPE_BOOL,
	schema.TypeStringArray:   wal.AttributeType_ATTRIBUTE_TYPE_STRING_ARRAY,
	schema.TypeIntArray:      wal.AttributeType_ATTRIBUTE_TYPE_INT_ARRAY,
	schema.TypeUintArray:     wal.AttributeType_ATTRIBUTE_TYPE_UINT_ARRAY,
	schema.TypeFloatArray:    wal.AttributeType_ATTRIBUTE_TYPE_FLOAT_ARRAY,
	schema.TypeUUIDArray:     wal.AttributeType_ATTRIBUTE_TYPE_UUID_ARRAY,
	schema.TypeDatetimeArray: wal.AttributeType_ATTRIBUTE_TYPE_DATETIME_ARRAY,
	schema.TypeBoolArray:     wal.AttributeType_ATTRIBUTE_TYPE_BOOL_ARRAY,
}
