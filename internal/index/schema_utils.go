package index

import (
	"encoding/json"
	"sort"

	"github.com/vexsearch/vex/internal/fts"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/schema"
)

// SchemaDefinitionFromState builds a schema definition from namespace state.
// Returns nil if no schema is configured.
func SchemaDefinitionFromState(state *namespace.State) *schema.Definition {
	if state == nil || state.Schema == nil || len(state.Schema.Attributes) == 0 {
		return nil
	}
	def := schema.NewDefinition()
	for name, attr := range state.Schema.Attributes {
		attrType := schema.AttrType(attr.Type)
		if !attrType.IsValid() {
			continue
		}
		converted := schema.Attribute{
			Type:       attrType,
			Filterable: attr.Filterable,
			Regex:      attr.Regex,
		}
		if len(attr.FullTextSearch) > 0 {
			converted.FullTextSearch = schema.NewFullTextConfig()
		}
		if err := def.SetAttribute(name, converted); err != nil {
			continue
		}
	}
	if len(def.Attributes) == 0 {
		return nil
	}
	return def
}

// FTSConfigsFromState builds FTS configs for all attributes with full_text_search enabled.
func FTSConfigsFromState(state *namespace.State) (map[string]*fts.Config, error) {
	if state == nil || state.Schema == nil || len(state.Schema.Attributes) == 0 {
		return nil, nil
	}

	configs := make(map[string]*fts.Config)
	names := make([]string, 0, len(state.Schema.Attributes))
	for name := range state.Schema.Attributes {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		attr := state.Schema.Attributes[name]
		if len(attr.FullTextSearch) == 0 {
			continue
		}
		if schema.AttrType(attr.Type) != schema.TypeString {
			continue
		}
		var raw any
		if err := json.Unmarshal(attr.FullTextSearch, &raw); err != nil {
			return nil, err
		}
		cfg, err := fts.Parse(raw)
		if err != nil {
			return nil, err
		}
		configs[name] = cfg
	}
	if len(configs) == 0 {
		return nil, nil
	}
	return configs, nil
}
