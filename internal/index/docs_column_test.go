package index

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"

	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestDocsColumnZstdRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()

	docs := []DocColumn{
		{
			ID:         "u64:1",
			NumericID:  1,
			WALSeq:     10,
			Deleted:    false,
			Attributes: map[string]any{"name": "alpha"},
			Vector:     []float32{0.1, 0.2},
		},
		{
			ID:         "u64:2",
			NumericID:  2,
			WALSeq:     11,
			Deleted:    true,
			Attributes: map[string]any{"name": "beta"},
		},
	}

	docsData, err := EncodeDocsColumnZstd(docs)
	if err != nil {
		t.Fatalf("failed to encode docs: %v", err)
	}

	writer := NewSegmentWriter(store, "test-ns", "seg_round")
	docsKey, err := writer.WriteDocsData(ctx, docsData)
	if err != nil {
		t.Fatalf("failed to write docs: %v", err)
	}
	writer.Seal()

	reader, _, err := store.Get(ctx, docsKey, nil)
	if err != nil {
		t.Fatalf("failed to read docs object: %v", err)
	}
	raw, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("failed to read docs bytes: %v", err)
	}
	if !IsZstdCompressed(raw) {
		t.Fatalf("expected docs column to be zstd-compressed")
	}

	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     "test-ns",
		IndexedWALSeq: 11,
		Segments: []Segment{
			{
				ID:          "seg_round",
				Level:       L0,
				StartWALSeq: 10,
				EndWALSeq:   11,
				DocsKey:     docsKey,
			},
		},
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	manifestKey := "vex/namespaces/test-ns/index/manifests/00000000000000000011.idx.json"
	if _, err := store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil); err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	idxReader := NewReader(store, nil, nil)
	loaded, err := idxReader.LoadSegmentDocs(ctx, manifestKey)
	if err != nil {
		t.Fatalf("failed to load docs: %v", err)
	}
	if len(loaded) != len(docs) {
		t.Fatalf("expected %d docs, got %d", len(docs), len(loaded))
	}
	for i := range docs {
		if !reflect.DeepEqual(loaded[i], docs[i]) {
			t.Fatalf("doc %d mismatch: %#v vs %#v", i, loaded[i], docs[i])
		}
	}
}
