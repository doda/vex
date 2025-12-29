package write

import (
	"context"
	"io"
	"testing"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/schema"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestHandler_SchemaInferencePersistsToWAL(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ns := "test-schema-infer-wal"
	req := &WriteRequest{
		RequestID: "schema-infer-wal",
		UpsertRows: []map[string]any{
			{
				"id":     1,
				"name":   "alice",
				"count":  float64(10),
				"price":  float64(19.5),
				"active": true,
				"tags":   []any{"a", "b"},
			},
		},
	}

	if _, err := handler.Handle(ctx, ns, req); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load namespace state: %v", err)
	}
	if loaded.State.Schema == nil {
		t.Fatal("expected schema to be inferred")
	}

	expectedTypes := map[string]string{
		"name":   "string",
		"count":  "uint",
		"price":  "float",
		"active": "bool",
		"tags":   "[]string",
	}
	for name, expectedType := range expectedTypes {
		attr, ok := loaded.State.Schema.Attributes[name]
		if !ok {
			t.Fatalf("expected schema to include %q", name)
		}
		if attr.Type != expectedType {
			t.Fatalf("expected %q type %q, got %q", name, expectedType, attr.Type)
		}
	}

	walKey := "vex/namespaces/" + ns + "/" + wal.KeyForSeq(1)
	rc, _, err := store.Get(ctx, walKey, nil)
	if err != nil {
		t.Fatalf("failed to load WAL entry: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("failed to read WAL entry: %v", err)
	}

	decoder, err := wal.NewDecoder()
	if err != nil {
		t.Fatalf("failed to create WAL decoder: %v", err)
	}
	defer decoder.Close()

	entry, err := decoder.Decode(data)
	if err != nil {
		t.Fatalf("failed to decode WAL entry: %v", err)
	}
	if len(entry.SubBatches) != 1 {
		t.Fatalf("expected 1 sub-batch, got %d", len(entry.SubBatches))
	}

	replayed := &namespace.Schema{Attributes: make(map[string]namespace.AttributeSchema)}
	for _, delta := range entry.SubBatches[0].SchemaDeltas {
		attrType, err := walTypeToSchema(delta.Type)
		if err != nil {
			t.Fatalf("failed to map WAL type: %v", err)
		}
		replayed.Attributes[delta.Name] = namespace.AttributeSchema{
			Type: attrType.String(),
		}
	}

	for name, attr := range loaded.State.Schema.Attributes {
		replayedAttr, ok := replayed.Attributes[name]
		if !ok {
			t.Fatalf("replay missing attribute %q", name)
		}
		if replayedAttr.Type != attr.Type {
			t.Fatalf("replay type mismatch for %q: got %q, want %q", name, replayedAttr.Type, attr.Type)
		}
	}
}

func walTypeToSchema(t wal.AttributeType) (schema.AttrType, error) {
	for schemaType, walType := range schemaTypeToWALType {
		if walType == t {
			return schemaType, nil
		}
	}
	return "", ErrInvalidSchema
}
