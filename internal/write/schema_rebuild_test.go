package write

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestDetectSchemaRebuildChanges_FilterableChange(t *testing.T) {
	tests := []struct {
		name           string
		update         *SchemaUpdate
		existingSchema *namespace.Schema
		wantRebuilds   []PendingRebuildChange
	}{
		{
			name:           "nil update",
			update:         nil,
			existingSchema: nil,
			wantRebuilds:   nil,
		},
		{
			name:   "new attribute with filterable - no rebuild needed",
			update: &SchemaUpdate{Attributes: map[string]AttributeSchemaUpdate{"title": {Type: "string", Filterable: boolPtr(true)}}},
			existingSchema: nil,
			wantRebuilds:   nil, // New attributes don't need rebuild
		},
		{
			name:   "enable filterable on existing attribute",
			update: &SchemaUpdate{Attributes: map[string]AttributeSchemaUpdate{"title": {Type: "string", Filterable: boolPtr(true)}}},
			existingSchema: &namespace.Schema{Attributes: map[string]namespace.AttributeSchema{
				"title": {Type: "string", Filterable: boolPtr(false)},
			}},
			wantRebuilds: []PendingRebuildChange{{Kind: RebuildKindFilter, Attribute: "title"}},
		},
		{
			name:   "disable filterable - no rebuild needed",
			update: &SchemaUpdate{Attributes: map[string]AttributeSchemaUpdate{"title": {Type: "string", Filterable: boolPtr(false)}}},
			existingSchema: &namespace.Schema{Attributes: map[string]namespace.AttributeSchema{
				"title": {Type: "string", Filterable: boolPtr(true)},
			}},
			wantRebuilds: nil, // Disabling doesn't require rebuild
		},
		{
			name:   "filterable already true - no rebuild needed",
			update: &SchemaUpdate{Attributes: map[string]AttributeSchemaUpdate{"title": {Type: "string", Filterable: boolPtr(true)}}},
			existingSchema: &namespace.Schema{Attributes: map[string]namespace.AttributeSchema{
				"title": {Type: "string", Filterable: boolPtr(true)},
			}},
			wantRebuilds: nil,
		},
		{
			name:   "enable filterable on FTS attribute (was default false)",
			update: &SchemaUpdate{Attributes: map[string]AttributeSchemaUpdate{"content": {Type: "string", Filterable: boolPtr(true)}}},
			existingSchema: &namespace.Schema{Attributes: map[string]namespace.AttributeSchema{
				"content": {Type: "string", FullTextSearch: json.RawMessage(`true`)},
			}},
			wantRebuilds: []PendingRebuildChange{{Kind: RebuildKindFilter, Attribute: "content"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectSchemaRebuildChanges(tt.update, tt.existingSchema)
			if len(got) != len(tt.wantRebuilds) {
				t.Errorf("got %d rebuilds, want %d", len(got), len(tt.wantRebuilds))
				return
			}
			for i, want := range tt.wantRebuilds {
				if got[i].Kind != want.Kind || got[i].Attribute != want.Attribute {
					t.Errorf("rebuild[%d] = %+v, want %+v", i, got[i], want)
				}
			}
		})
	}
}

func TestDetectSchemaRebuildChanges_FTSChange(t *testing.T) {
	tests := []struct {
		name           string
		update         *SchemaUpdate
		existingSchema *namespace.Schema
		wantRebuilds   []PendingRebuildChange
	}{
		{
			name:   "enable FTS on existing attribute",
			update: &SchemaUpdate{Attributes: map[string]AttributeSchemaUpdate{"content": {Type: "string", FullTextSearch: true}}},
			existingSchema: &namespace.Schema{Attributes: map[string]namespace.AttributeSchema{
				"content": {Type: "string"},
			}},
			wantRebuilds: []PendingRebuildChange{{Kind: RebuildKindFTS, Attribute: "content"}},
		},
		{
			name:   "disable FTS - no rebuild needed",
			update: &SchemaUpdate{Attributes: map[string]AttributeSchemaUpdate{"content": {Type: "string", FullTextSearch: false}}},
			existingSchema: &namespace.Schema{Attributes: map[string]namespace.AttributeSchema{
				"content": {Type: "string", FullTextSearch: json.RawMessage(`true`)},
			}},
			wantRebuilds: nil,
		},
		{
			name:   "change FTS config (tokenizer)",
			update: &SchemaUpdate{Attributes: map[string]AttributeSchemaUpdate{"content": {Type: "string", FullTextSearch: map[string]any{"tokenizer": "whitespace"}}}},
			existingSchema: &namespace.Schema{Attributes: map[string]namespace.AttributeSchema{
				"content": {Type: "string", FullTextSearch: json.RawMessage(`true`)},
			}},
			wantRebuilds: []PendingRebuildChange{{Kind: RebuildKindFTS, Attribute: "content"}},
		},
		{
			name:   "new attribute with FTS - no rebuild needed",
			update: &SchemaUpdate{Attributes: map[string]AttributeSchemaUpdate{"content": {Type: "string", FullTextSearch: true}}},
			existingSchema: nil,
			wantRebuilds:   nil, // New attributes don't need rebuild
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectSchemaRebuildChanges(tt.update, tt.existingSchema)
			if len(got) != len(tt.wantRebuilds) {
				t.Errorf("got %d rebuilds, want %d", len(got), len(tt.wantRebuilds))
				return
			}
			for i, want := range tt.wantRebuilds {
				if got[i].Kind != want.Kind || got[i].Attribute != want.Attribute {
					t.Errorf("rebuild[%d] = %+v, want %+v", i, got[i], want)
				}
			}
		})
	}
}

func TestHandler_SchemaUpdate_AddsPendingRebuilds(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// First, create the namespace with an attribute
	req1 := &WriteRequest{
		RequestID: "req-1",
		UpsertRows: []map[string]any{
			{"id": 1, "title": "test"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"title": {Type: "string", Filterable: boolPtr(false)},
			},
		},
	}
	_, err = handler.Handle(ctx, "test-ns", req1)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Now enable filterable on the attribute
	req2 := &WriteRequest{
		RequestID: "req-2",
		UpsertRows: []map[string]any{
			{"id": 2, "title": "another"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"title": {Type: "string", Filterable: boolPtr(true)},
			},
		},
	}
	_, err = handler.Handle(ctx, "test-ns", req2)
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	// Check that a pending rebuild was added
	loaded, err := stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if len(loaded.State.Index.PendingRebuilds) != 1 {
		t.Errorf("expected 1 pending rebuild, got %d", len(loaded.State.Index.PendingRebuilds))
		return
	}

	pr := loaded.State.Index.PendingRebuilds[0]
	if pr.Kind != RebuildKindFilter {
		t.Errorf("expected filter rebuild, got %s", pr.Kind)
	}
	if pr.Attribute != "title" {
		t.Errorf("expected attribute 'title', got %s", pr.Attribute)
	}
	if pr.Ready {
		t.Error("expected Ready=false")
	}
}

func TestHandler_SchemaUpdate_EnableFTS_AddsPendingRebuild(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Create namespace with attribute without FTS
	req1 := &WriteRequest{
		RequestID: "req-1",
		UpsertRows: []map[string]any{
			{"id": 1, "content": "test document"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"content": {Type: "string"},
			},
		},
	}
	_, err = handler.Handle(ctx, "test-ns", req1)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Enable FTS on the attribute
	req2 := &WriteRequest{
		RequestID: "req-2",
		UpsertRows: []map[string]any{
			{"id": 2, "content": "another document"},
		},
		Schema: &SchemaUpdate{
			Attributes: map[string]AttributeSchemaUpdate{
				"content": {Type: "string", FullTextSearch: true},
			},
		},
	}
	_, err = handler.Handle(ctx, "test-ns", req2)
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	// Check that a pending FTS rebuild was added
	loaded, err := stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if len(loaded.State.Index.PendingRebuilds) != 1 {
		t.Errorf("expected 1 pending rebuild, got %d", len(loaded.State.Index.PendingRebuilds))
		return
	}

	pr := loaded.State.Index.PendingRebuilds[0]
	if pr.Kind != RebuildKindFTS {
		t.Errorf("expected fts rebuild, got %s", pr.Kind)
	}
	if pr.Attribute != "content" {
		t.Errorf("expected attribute 'content', got %s", pr.Attribute)
	}
	if pr.Ready {
		t.Error("expected Ready=false")
	}
}

func boolPtr(b bool) *bool {
	return &b
}
