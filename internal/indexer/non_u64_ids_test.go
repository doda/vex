package indexer

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/internal/wal"
)

type vectorIDDoc struct {
	id     *wal.DocumentID
	vector []float32
}

func createVectorWALEntryWithIDs(namespace string, seq uint64, docs []vectorIDDoc) ([]byte, error) {
	entry := wal.NewWalEntry(namespace, seq)
	batch := wal.NewWriteSubBatch("test-request")

	for _, doc := range docs {
		var vectorBytes []byte
		var dims uint32
		if doc.vector != nil {
			dims = uint32(len(doc.vector))
			vectorBytes = encodeVectorForTest(doc.vector)
		}
		batch.AddUpsert(doc.id, nil, vectorBytes, dims)
	}

	entry.SubBatches = append(entry.SubBatches, batch)

	encoder, err := wal.NewEncoder()
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	result, err := encoder.Encode(entry)
	if err != nil {
		return nil, err
	}

	return result.Data, nil
}

func TestIndexerSupportsUUIDAndStringIDs(t *testing.T) {
	store := newMockStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	uuidVal := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	stringID := "doc-string-1"
	expectedUUIDKey := fmt.Sprintf("uuid:%x", uuidVal[:])
	expectedStringKey := "str:" + stringID

	docs := []vectorIDDoc{
		{id: &wal.DocumentID{Id: &wal.DocumentID_Uuid{Uuid: uuidVal[:]}}, vector: []float32{1.0, 0.0, 0.0, 0.0}},
		{id: &wal.DocumentID{Id: &wal.DocumentID_Str{Str: stringID}}, vector: []float32{0.0, 1.0, 0.0, 0.0}},
	}
	data, err := createVectorWALEntryWithIDs("test-ns", 1, docs)
	if err != nil {
		t.Fatalf("Failed to encode WAL: %v", err)
	}

	walKey := fmt.Sprintf("vex/namespaces/test-ns/%s", wal.KeyForSeq(1))
	store.mu.Lock()
	store.objects[walKey] = mockObject{data: data, etag: "etag1"}
	store.mu.Unlock()

	loaded, err := stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("Failed to load namespace: %v", err)
	}
	loaded, err = stateMan.AdvanceWAL(ctx, "test-ns", loaded.ETag, walKey, int64(len(data)), nil)
	if err != nil {
		t.Fatalf("Failed to advance WAL: %v", err)
	}

	idxer := New(store, stateMan, DefaultConfig(), nil)
	processor := NewL0SegmentProcessor(store, stateMan, nil, idxer)

	result, err := processor.ProcessWAL(ctx, "test-ns", 0, 1, loaded.State, loaded.ETag)
	if err != nil {
		t.Fatalf("ProcessWAL failed: %v", err)
	}
	if result == nil || result.ManifestKey == "" {
		t.Fatal("expected manifest key after indexing")
	}

	reader := index.NewReader(store, nil, nil)
	indexedDocs, err := reader.LoadIVFSegmentDocs(ctx, result.ManifestKey)
	if err != nil {
		t.Fatalf("LoadIVFSegmentDocs failed: %v", err)
	}
	if len(indexedDocs) != 2 {
		t.Fatalf("expected 2 indexed docs, got %d", len(indexedDocs))
	}
	seen := make(map[string]bool)
	for _, doc := range indexedDocs {
		seen[doc.ID] = true
	}
	if !seen[expectedUUIDKey] {
		t.Errorf("expected UUID doc key %s in docs column", expectedUUIDKey)
	}
	if !seen[expectedStringKey] {
		t.Errorf("expected string doc key %s in docs column", expectedStringKey)
	}

	handler := query.NewHandler(store, stateMan, nil)

	reqUUID := &query.QueryRequest{
		RankBy: []any{"vector", "ANN", []any{1.0, 0.0, 0.0, 0.0}},
		Limit:  1,
	}
	respUUID, err := handler.Handle(ctx, "test-ns", reqUUID)
	if err != nil {
		t.Fatalf("UUID query failed: %v", err)
	}
	if len(respUUID.Rows) != 1 {
		t.Fatalf("expected 1 UUID result, got %d", len(respUUID.Rows))
	}
	if respUUID.Rows[0].ID != uuidVal.String() {
		t.Errorf("expected UUID result %s, got %v", uuidVal.String(), respUUID.Rows[0].ID)
	}

	reqString := &query.QueryRequest{
		RankBy: []any{"vector", "ANN", []any{0.0, 1.0, 0.0, 0.0}},
		Limit:  1,
	}
	respString, err := handler.Handle(ctx, "test-ns", reqString)
	if err != nil {
		t.Fatalf("string query failed: %v", err)
	}
	if len(respString.Rows) != 1 {
		t.Fatalf("expected 1 string result, got %d", len(respString.Rows))
	}
	if respString.Rows[0].ID != stringID {
		t.Errorf("expected string result %s, got %v", stringID, respString.Rows[0].ID)
	}
}
