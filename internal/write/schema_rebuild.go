package write

import (
	"bytes"
	"encoding/json"

	"github.com/vexsearch/vex/internal/namespace"
)

// RebuildKind represents the type of index rebuild needed.
const (
	RebuildKindFilter = "filter"
	RebuildKindFTS    = "fts"
)

// PendingRebuildChange describes a single pending rebuild that should be added.
type PendingRebuildChange struct {
	Kind      string
	Attribute string
}

// DetectSchemaRebuildChanges compares the schema update against the existing schema
// and returns a list of pending rebuilds that should be added.
//
// Rebuilds are needed when:
// - filterable is enabled on an attribute that wasn't filterable before
// - full_text_search is enabled or its configuration changes
func DetectSchemaRebuildChanges(update *SchemaUpdate, existingSchema *namespace.Schema) []PendingRebuildChange {
	if update == nil || len(update.Attributes) == 0 {
		return nil
	}

	var changes []PendingRebuildChange

	for name, attrUpdate := range update.Attributes {
		var existingAttr *namespace.AttributeSchema
		if existingSchema != nil && existingSchema.Attributes != nil {
			if ea, exists := existingSchema.Attributes[name]; exists {
				existingAttr = &ea
			}
		}

		// Check for filterable changes
		if needsFilterRebuild(attrUpdate, existingAttr) {
			changes = append(changes, PendingRebuildChange{
				Kind:      RebuildKindFilter,
				Attribute: name,
			})
		}

		// Check for FTS changes
		if needsFTSRebuild(attrUpdate, existingAttr) {
			changes = append(changes, PendingRebuildChange{
				Kind:      RebuildKindFTS,
				Attribute: name,
			})
		}
	}

	return changes
}

// needsFilterRebuild returns true if enabling filterable requires an index rebuild.
// A rebuild is needed when:
// - Attribute is new and filterable is explicitly true
// - Attribute exists and was not filterable, but update sets filterable=true
func needsFilterRebuild(update AttributeSchemaUpdate, existing *namespace.AttributeSchema) bool {
	if update.Filterable == nil || !*update.Filterable {
		return false
	}

	// If this is a new attribute with filterable=true, we need to build the filter index
	if existing == nil {
		// New attribute with filterable explicitly set to true
		// For new attributes, the data isn't indexed yet anyway, so no "rebuild" is needed.
		// The indexer will just include it in normal indexing.
		// Only trigger rebuild if we're enabling filterable on an EXISTING attribute.
		return false
	}

	// Existing attribute: check if it was previously not filterable
	wasFilterable := true // default is true
	if existing.Filterable != nil {
		wasFilterable = *existing.Filterable
	} else if existing.Regex || len(existing.FullTextSearch) > 0 {
		// Default is false when regex or FTS is enabled
		wasFilterable = false
	}

	// Only trigger rebuild if transitioning from not-filterable to filterable
	return !wasFilterable && *update.Filterable
}

// needsFTSRebuild returns true if enabling/changing FTS requires an index rebuild.
// A rebuild is needed when:
// - FTS is enabled on an attribute that didn't have it
// - FTS configuration changes (different tokenizer, language, etc.)
func needsFTSRebuild(update AttributeSchemaUpdate, existing *namespace.AttributeSchema) bool {
	if update.FullTextSearch == nil {
		return false
	}

	// Check if update is enabling FTS
	updateEnabled, updateConfig := parseFTSUpdate(update.FullTextSearch)
	if !updateEnabled {
		// Disabling FTS doesn't require a rebuild (just stop using the index)
		return false
	}

	// If this is a new attribute, no rebuild is needed - indexer will include it
	if existing == nil {
		return false
	}

	// Check existing FTS state
	existingEnabled := len(existing.FullTextSearch) > 0
	if !existingEnabled {
		// Enabling FTS on existing attribute - needs rebuild
		return true
	}

	// Both have FTS enabled - check if configuration changed
	// Parse existing FTS config and compare
	return ftsConfigChanged(existing.FullTextSearch, updateConfig)
}

// parseFTSUpdate parses the FTS update value and returns whether it's enabled
// and the raw config bytes if it's an object.
func parseFTSUpdate(v any) (enabled bool, config []byte) {
	if v == nil {
		return false, nil
	}

	switch val := v.(type) {
	case bool:
		return val, nil
	case map[string]any:
		data, err := json.Marshal(val)
		if err != nil {
			return false, nil
		}
		return true, data
	default:
		return false, nil
	}
}

// ftsConfigChanged compares two FTS configurations and returns true if they differ.
func ftsConfigChanged(existingRaw json.RawMessage, updateConfig []byte) bool {
	// If update has no config (just true), and existing has config, they differ
	if len(updateConfig) == 0 {
		// Update is just "true", compare with existing
		// If existing is also just "true" or equivalent defaults, no change
		if bytes.Equal(existingRaw, []byte("true")) {
			return false
		}
		// Existing has config object, update is just true - might be different
		// For simplicity, treat different representations as changed
		return true
	}

	// Both have config objects - compare them
	if bytes.Equal(existingRaw, updateConfig) {
		return false
	}

	// Different bytes - could be semantically equivalent, but for safety
	// treat any difference as requiring rebuild
	return true
}
