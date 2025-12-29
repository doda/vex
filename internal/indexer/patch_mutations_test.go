package indexer

import (
	"context"
	"testing"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/internal/wal"
)

func TestIndexerAppliesPatchMutations(t *testing.T) {
	store := newMockStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	entry1 := wal.NewWalEntry("test-ns", 1)
	batch1 := wal.NewWriteSubBatch("req-1")
	batch1.AddUpsert(&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}}, map[string]*wal.AttributeValue{
		"category": wal.StringValue("A"),
		"count":    wal.IntValue(1),
		"status":   wal.StringValue("new"),
	}, nil, 0)
	batch1.AddUpsert(&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 2}}, map[string]*wal.AttributeValue{
		"category": wal.StringValue("B"),
		"count":    wal.IntValue(2),
		"status":   wal.StringValue("new"),
	}, nil, 0)
	entry1.SubBatches = append(entry1.SubBatches, batch1)

	data1 := encodeWalEntry(t, entry1)
	store.objects["vex/namespaces/test-ns/"+wal.KeyForSeq(1)] = mockObject{data: data1, etag: "etag1"}

	loaded, err := stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to load namespace: %v", err)
	}
	loaded, err = stateMan.AdvanceWAL(ctx, "test-ns", loaded.ETag, "vex/namespaces/test-ns/"+wal.KeyForSeq(1), int64(len(data1)), nil)
	if err != nil {
		t.Fatalf("failed to advance WAL: %v", err)
	}

	idxer := New(store, stateMan, DefaultConfig(), nil)
	processor := NewL0SegmentProcessor(store, stateMan, nil, idxer)

	result1, err := processor.ProcessWAL(ctx, "test-ns", 0, 1, loaded.State, loaded.ETag)
	if err != nil {
		t.Fatalf("ProcessWAL failed: %v", err)
	}
	if result1 == nil || !result1.ManifestWritten {
		t.Fatalf("expected initial manifest to be written")
	}

	entry2 := wal.NewWalEntry("test-ns", 2)
	batch2 := wal.NewWriteSubBatch("req-2")
	batch2.AddPatch(&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}}, map[string]*wal.AttributeValue{
		"status": wal.StringValue("patched"),
	})
	entry2.SubBatches = append(entry2.SubBatches, batch2)

	entry3 := wal.NewWalEntry("test-ns", 3)
	batch3 := wal.NewWriteSubBatch("req-3")
	batch3.AddFilterOp(&wal.FilterOperation{
		Type: wal.FilterOperationType_FILTER_OPERATION_TYPE_PATCH,
		CandidateIds: []*wal.DocumentID{
			{Id: &wal.DocumentID_U64{U64: 2}},
		},
		PatchJson: `{"status":"filtered"}`,
	})
	batch3.AddPatch(&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 2}}, map[string]*wal.AttributeValue{
		"status": wal.StringValue("filtered"),
	})
	entry3.SubBatches = append(entry3.SubBatches, batch3)

	data2 := encodeWalEntry(t, entry2)
	store.objects["vex/namespaces/test-ns/"+wal.KeyForSeq(2)] = mockObject{data: data2, etag: "etag2"}
	data3 := encodeWalEntry(t, entry3)
	store.objects["vex/namespaces/test-ns/"+wal.KeyForSeq(3)] = mockObject{data: data3, etag: "etag3"}

	loaded, err = stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to reload namespace: %v", err)
	}
	loaded, err = stateMan.AdvanceWAL(ctx, "test-ns", loaded.ETag, "vex/namespaces/test-ns/"+wal.KeyForSeq(2), int64(len(data2)), nil)
	if err != nil {
		t.Fatalf("failed to advance WAL to seq 2: %v", err)
	}
	loaded, err = stateMan.AdvanceWAL(ctx, "test-ns", loaded.ETag, "vex/namespaces/test-ns/"+wal.KeyForSeq(3), int64(len(data3)), nil)
	if err != nil {
		t.Fatalf("failed to advance WAL to seq 3: %v", err)
	}

	loaded, err = stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("failed to reload namespace after WAL updates: %v", err)
	}
	result2, err := processor.ProcessWAL(ctx, "test-ns", 1, 3, loaded.State, loaded.ETag)
	if err != nil {
		t.Fatalf("ProcessWAL for patches failed: %v", err)
	}
	if result2 == nil || !result2.ManifestWritten {
		t.Fatalf("expected patch manifest to be written")
	}

	reader := index.NewReader(store, nil, nil)
	indexedDocs, err := reader.LoadSegmentDocs(ctx, result2.ManifestKey)
	if err != nil {
		t.Fatalf("LoadSegmentDocs failed: %v", err)
	}
	if len(indexedDocs) == 0 {
		t.Fatalf("expected indexed docs, got none")
	}

	handler := query.NewHandler(store, stateMan, &emptyTailStore{})
	resp, err := handler.Handle(ctx, "test-ns", &query.QueryRequest{
		RankBy: []any{"count", "asc"},
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("query Handle failed: %v", err)
	}
	if len(resp.Rows) != 2 {
		t.Fatalf("expected 2 query rows, got %d", len(resp.Rows))
	}

	rowsByID := make(map[uint64]map[string]any)
	for _, row := range resp.Rows {
		if id, ok := row.ID.(uint64); ok {
			rowsByID[id] = row.Attributes
		}
	}

	if attrs := rowsByID[1]; attrs == nil || attrs["status"] != "patched" || attrs["category"] != "A" || !attrEqualsInt(attrs["count"], 1) {
		t.Errorf("unexpected attributes for doc 1: %v", attrs)
	}
	if attrs := rowsByID[2]; attrs == nil || attrs["status"] != "filtered" || attrs["category"] != "B" || !attrEqualsInt(attrs["count"], 2) {
		t.Errorf("unexpected attributes for doc 2: %v", attrs)
	}
}

func attrEqualsInt(value any, expected int64) bool {
	switch v := value.(type) {
	case int64:
		return v == expected
	case float64:
		return v == float64(expected)
	default:
		return false
	}
}

func encodeWalEntry(t *testing.T, entry *wal.WalEntry) []byte {
	t.Helper()
	encoder, err := wal.NewEncoder()
	if err != nil {
		t.Fatalf("failed to create WAL encoder: %v", err)
	}
	defer encoder.Close()

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("failed to encode WAL entry: %v", err)
	}
	return result.Data
}
