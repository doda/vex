package wal

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vexsearch/vex/internal/document"
)

func TestEncoderDecoder(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry := NewWalEntry("test-namespace", 1)
	batch := NewWriteSubBatch("request-123")
	batch.AddUpsert(
		&DocumentID{Id: &DocumentID_U64{U64: 42}},
		map[string]*AttributeValue{
			"name":   StringValue("test"),
			"count":  IntValue(100),
			"active": BoolValue(true),
		},
		nil, 0,
	)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("failed to encode entry: %v", err)
	}

	if len(result.Data) == 0 {
		t.Fatal("encoded data is empty")
	}

	decoded, err := decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("failed to decode entry: %v", err)
	}

	if decoded.Namespace != entry.Namespace {
		t.Errorf("namespace mismatch: got %q, want %q", decoded.Namespace, entry.Namespace)
	}
	if decoded.Seq != entry.Seq {
		t.Errorf("seq mismatch: got %d, want %d", decoded.Seq, entry.Seq)
	}
	if len(decoded.SubBatches) != 1 {
		t.Fatalf("sub_batches count mismatch: got %d, want 1", len(decoded.SubBatches))
	}
	if decoded.SubBatches[0].RequestId != "request-123" {
		t.Errorf("request_id mismatch: got %q, want %q", decoded.SubBatches[0].RequestId, "request-123")
	}
}

func TestZstdCompression(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	entry := NewWalEntry("test-namespace", 1)
	for i := 0; i < 100; i++ {
		batch := NewWriteSubBatch("request-" + uintToString(uint64(i)))
		batch.AddUpsert(
			&DocumentID{Id: &DocumentID_U64{U64: uint64(i)}},
			map[string]*AttributeValue{
				"text": StringValue("this is a test string that should compress well when repeated many times"),
			},
			nil, 0,
		)
		entry.SubBatches = append(entry.SubBatches, batch)
	}

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("failed to encode entry: %v", err)
	}

	if !bytes.HasSuffix([]byte(FileExtension), []byte(".zst")) {
		t.Error("FileExtension should end with .zst")
	}

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	_, err = decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("failed to decode compressed entry: %v", err)
	}
}

func TestSubBatchContainsRequiredFields(t *testing.T) {
	batch := NewWriteSubBatch("test-request-id")

	if batch.RequestId != "test-request-id" {
		t.Errorf("request_id not set: got %q", batch.RequestId)
	}
	if batch.ReceivedAtMs == 0 {
		t.Error("received_at_ms not set")
	}
	if batch.Mutations == nil {
		t.Error("mutations not initialized")
	}
	if batch.Stats == nil {
		t.Error("stats not initialized")
	}
}

func TestSHA256Checksum(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	entry := NewWalEntry("checksum-test", 1)
	batch := NewWriteSubBatch("request-1")
	batch.AddUpsert(&DocumentID{Id: &DocumentID_U64{U64: 1}}, nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	if result.Checksum == [32]byte{} {
		t.Error("checksum is empty")
	}

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	decoded, err := decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if len(decoded.Checksum) != 32 {
		t.Errorf("stored checksum has wrong length: got %d, want 32", len(decoded.Checksum))
	}
}

func TestDeterministicEncoding(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	entry := NewWalEntry("deterministic-test", 1)
	entry.CommittedUnixMs = 1234567890000

	batch := NewWriteSubBatch("request-1")
	batch.ReceivedAtMs = 1234567890000

	batch.Mutations = []*Mutation{
		{
			Type: MutationType_MUTATION_TYPE_UPSERT,
			Id:   &DocumentID{Id: &DocumentID_U64{U64: 3}},
			Attributes: map[string]*AttributeValue{
				"zebra":   StringValue("z"),
				"apple":   StringValue("a"),
				"mango":   StringValue("m"),
			},
		},
		{
			Type: MutationType_MUTATION_TYPE_UPSERT,
			Id:   &DocumentID{Id: &DocumentID_U64{U64: 1}},
		},
		{
			Type: MutationType_MUTATION_TYPE_UPSERT,
			Id:   &DocumentID{Id: &DocumentID_U64{U64: 2}},
		},
	}

	batch.SchemaDeltas = []*SchemaDelta{
		{Name: "zebra", Type: AttributeType_ATTRIBUTE_TYPE_STRING},
		{Name: "apple", Type: AttributeType_ATTRIBUTE_TYPE_STRING},
		{Name: "mango", Type: AttributeType_ATTRIBUTE_TYPE_STRING},
	}

	entry.SubBatches = append(entry.SubBatches, batch)

	result1, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("first encode failed: %v", err)
	}

	result2, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("second encode failed: %v", err)
	}

	if !bytes.Equal(result1.Data, result2.Data) {
		t.Error("encoding is not deterministic: got different results for same input")
	}

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	decoded, err := decoder.Decode(result1.Data)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	mutations := decoded.SubBatches[0].Mutations
	if mutations[0].Id.GetU64() != 1 {
		t.Error("mutations not sorted by docID: expected ID 1 first")
	}
	if mutations[1].Id.GetU64() != 2 {
		t.Error("mutations not sorted by docID: expected ID 2 second")
	}
	if mutations[2].Id.GetU64() != 3 {
		t.Error("mutations not sorted by docID: expected ID 3 third")
	}

	deltas := decoded.SubBatches[0].SchemaDeltas
	if deltas[0].Name != "apple" {
		t.Error("schema deltas not sorted: expected 'apple' first")
	}
	if deltas[1].Name != "mango" {
		t.Error("schema deltas not sorted: expected 'mango' second")
	}
	if deltas[2].Name != "zebra" {
		t.Error("schema deltas not sorted: expected 'zebra' third")
	}
}

func TestDocumentIDTypes(t *testing.T) {
	u := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")

	tests := []struct {
		name string
		id   *DocumentID
	}{
		{
			name: "u64",
			id:   &DocumentID{Id: &DocumentID_U64{U64: 12345}},
		},
		{
			name: "uuid",
			id:   &DocumentID{Id: &DocumentID_Uuid{Uuid: u[:]}},
		},
		{
			name: "string",
			id:   &DocumentID{Id: &DocumentID_Str{Str: "my-doc-id"}},
		},
	}

	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := NewWalEntry("test", 1)
			batch := NewWriteSubBatch("req-1")
			batch.AddUpsert(tt.id, nil, nil, 0)
			entry.SubBatches = append(entry.SubBatches, batch)

			result, err := encoder.Encode(entry)
			if err != nil {
				t.Fatalf("encode failed: %v", err)
			}

			decoded, err := decoder.Decode(result.Data)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			gotID := decoded.SubBatches[0].Mutations[0].Id
			if compareDocumentIDs(gotID, tt.id) != 0 {
				t.Errorf("ID mismatch")
			}
		})
	}
}

func TestDocumentIDConversion(t *testing.T) {
	t.Run("u64", func(t *testing.T) {
		original := document.NewU64ID(42)
		protoID := DocumentIDFromID(original)
		recovered, err := DocumentIDToID(protoID)
		if err != nil {
			t.Fatalf("conversion failed: %v", err)
		}
		if !original.Equal(recovered) {
			t.Errorf("u64 ID mismatch: got %v, want %v", recovered, original)
		}
	})

	t.Run("uuid", func(t *testing.T) {
		u := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
		original := document.NewUUIDID(u)
		protoID := DocumentIDFromID(original)
		recovered, err := DocumentIDToID(protoID)
		if err != nil {
			t.Fatalf("conversion failed: %v", err)
		}
		if !original.Equal(recovered) {
			t.Errorf("uuid ID mismatch: got %v, want %v", recovered, original)
		}
	})

	t.Run("string", func(t *testing.T) {
		original, _ := document.NewStringID("my-doc")
		protoID := DocumentIDFromID(original)
		recovered, err := DocumentIDToID(protoID)
		if err != nil {
			t.Fatalf("conversion failed: %v", err)
		}
		if !original.Equal(recovered) {
			t.Errorf("string ID mismatch: got %v, want %v", recovered, original)
		}
	})
}

func TestKeyForSeq(t *testing.T) {
	tests := []struct {
		seq  uint64
		want string
	}{
		{0, "wal/0.wal.zst"},
		{1, "wal/1.wal.zst"},
		{123, "wal/123.wal.zst"},
		{math.MaxUint64, "wal/18446744073709551615.wal.zst"},
	}

	for _, tt := range tests {
		got := KeyForSeq(tt.seq)
		if got != tt.want {
			t.Errorf("KeyForSeq(%d) = %q, want %q", tt.seq, got, tt.want)
		}
	}
}

func TestAttributeValueTypes(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	u := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	nowMs := time.Now().UnixMilli()

	attrs := map[string]*AttributeValue{
		"str":      StringValue("hello"),
		"int":      IntValue(-42),
		"uint":     UintValue(42),
		"float":    FloatValue(3.14),
		"uuid":     UuidValue(u),
		"datetime": DatetimeValue(nowMs),
		"bool":     BoolValue(true),
		"null":     NullValue(),
		"str_arr":  StringArrayValue([]string{"a", "b", "c"}),
		"int_arr":  IntArrayValue([]int64{1, 2, 3}),
		"uint_arr": UintArrayValue([]uint64{1, 2, 3}),
		"flt_arr":  FloatArrayValue([]float64{1.1, 2.2, 3.3}),
		"dt_arr":   DatetimeArrayValue([]int64{nowMs, nowMs + 1000}),
		"bool_arr": BoolArrayValue([]bool{true, false, true}),
	}

	entry := NewWalEntry("types-test", 1)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(&DocumentID{Id: &DocumentID_U64{U64: 1}}, attrs, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	gotAttrs := decoded.SubBatches[0].Mutations[0].Attributes

	if gotAttrs["str"].GetStringVal() != "hello" {
		t.Error("string value mismatch")
	}
	if gotAttrs["int"].GetIntVal() != -42 {
		t.Error("int value mismatch")
	}
	if gotAttrs["uint"].GetUintVal() != 42 {
		t.Error("uint value mismatch")
	}
	if gotAttrs["float"].GetFloatVal() != 3.14 {
		t.Error("float value mismatch")
	}
	if !bytes.Equal(gotAttrs["uuid"].GetUuidVal(), u[:]) {
		t.Error("uuid value mismatch")
	}
	if gotAttrs["datetime"].GetDatetimeVal() != nowMs {
		t.Error("datetime value mismatch")
	}
	if gotAttrs["bool"].GetBoolVal() != true {
		t.Error("bool value mismatch")
	}
	if gotAttrs["null"].GetNullVal() != true {
		t.Error("null value mismatch")
	}
}

func TestChecksumValidation(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	entry := NewWalEntry("checksum-test", 1)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(&DocumentID{Id: &DocumentID_U64{U64: 1}}, nil, nil, 0)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	_, err = decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("valid data should decode without error: %v", err)
	}
}

func TestVectorEncoding(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	vector := []float32{1.0, 2.0, 3.0, 4.0}
	vectorBytes := make([]byte, len(vector)*4)
	for i, f := range vector {
		bits := math.Float32bits(f)
		vectorBytes[i*4] = byte(bits)
		vectorBytes[i*4+1] = byte(bits >> 8)
		vectorBytes[i*4+2] = byte(bits >> 16)
		vectorBytes[i*4+3] = byte(bits >> 24)
	}

	entry := NewWalEntry("vector-test", 1)
	batch := NewWriteSubBatch("req-1")
	batch.AddUpsert(
		&DocumentID{Id: &DocumentID_U64{U64: 1}},
		nil,
		vectorBytes,
		uint32(len(vector)),
	)
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	mutation := decoded.SubBatches[0].Mutations[0]
	if mutation.VectorDims != 4 {
		t.Errorf("vector dims mismatch: got %d, want 4", mutation.VectorDims)
	}
	if !bytes.Equal(mutation.Vector, vectorBytes) {
		t.Error("vector data mismatch")
	}

	recoveredVector := DecodeFloat32Vector(mutation.Vector)
	for i, f := range vector {
		if recoveredVector[i] != f {
			t.Errorf("vector element %d mismatch: got %f, want %f", i, recoveredVector[i], f)
		}
	}
}

func TestMutationTypes(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry := NewWalEntry("mutation-types-test", 1)
	batch := NewWriteSubBatch("req-1")

	batch.AddUpsert(&DocumentID{Id: &DocumentID_U64{U64: 1}}, map[string]*AttributeValue{"a": StringValue("new")}, nil, 0)
	batch.AddPatch(&DocumentID{Id: &DocumentID_U64{U64: 2}}, map[string]*AttributeValue{"b": IntValue(42)})
	batch.AddDelete(&DocumentID{Id: &DocumentID_U64{U64: 3}})

	entry.SubBatches = append(entry.SubBatches, batch)

	if batch.Stats.RowsUpserted != 1 {
		t.Errorf("rows_upserted mismatch: got %d, want 1", batch.Stats.RowsUpserted)
	}
	if batch.Stats.RowsPatched != 1 {
		t.Errorf("rows_patched mismatch: got %d, want 1", batch.Stats.RowsPatched)
	}
	if batch.Stats.RowsDeleted != 1 {
		t.Errorf("rows_deleted mismatch: got %d, want 1", batch.Stats.RowsDeleted)
	}
	if batch.Stats.RowsAffected != 3 {
		t.Errorf("rows_affected mismatch: got %d, want 3", batch.Stats.RowsAffected)
	}

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	mutations := decoded.SubBatches[0].Mutations
	if mutations[0].Type != MutationType_MUTATION_TYPE_UPSERT {
		t.Error("first mutation should be upsert")
	}
	if mutations[1].Type != MutationType_MUTATION_TYPE_PATCH {
		t.Error("second mutation should be patch")
	}
	if mutations[2].Type != MutationType_MUTATION_TYPE_DELETE {
		t.Error("third mutation should be delete")
	}
}

func TestFilterOperation(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry := NewWalEntry("filter-op-test", 1)
	batch := NewWriteSubBatch("req-1")
	batch.AddFilterOp(&FilterOperation{
		Type:              FilterOperationType_FILTER_OPERATION_TYPE_DELETE,
		Phase1SnapshotSeq: 5,
		CandidateIds: []*DocumentID{
			{Id: &DocumentID_U64{U64: 1}},
			{Id: &DocumentID_U64{U64: 2}},
		},
		FilterJson:   `{"status": {"$eq": "inactive"}}`,
		AllowPartial: true,
	})
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	filterOps := decoded.SubBatches[0].GetAllFilterOps()
	if len(filterOps) != 1 {
		t.Fatalf("expected 1 filter op, got %d", len(filterOps))
	}
	filterOp := filterOps[0]
	if filterOp.Type != FilterOperationType_FILTER_OPERATION_TYPE_DELETE {
		t.Error("filter operation type mismatch")
	}
	if filterOp.Phase1SnapshotSeq != 5 {
		t.Errorf("phase1_snapshot_seq mismatch: got %d, want 5", filterOp.Phase1SnapshotSeq)
	}
	if len(filterOp.CandidateIds) != 2 {
		t.Errorf("candidate_ids count mismatch: got %d, want 2", len(filterOp.CandidateIds))
	}
	if filterOp.FilterJson != `{"status": {"$eq": "inactive"}}` {
		t.Error("filter_json mismatch")
	}
	if !filterOp.AllowPartial {
		t.Error("allow_partial should be true")
	}
}

// TestMultiFilterOperations verifies that WAL sub-batches can record both
// delete_by_filter and patch_by_filter operations in the same request.
func TestMultiFilterOperations(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry := NewWalEntry("multi-filter-test", 1)
	batch := NewWriteSubBatch("req-1")

	// Add delete_by_filter operation
	batch.AddFilterOp(&FilterOperation{
		Type:              FilterOperationType_FILTER_OPERATION_TYPE_DELETE,
		Phase1SnapshotSeq: 5,
		CandidateIds: []*DocumentID{
			{Id: &DocumentID_U64{U64: 1}},
			{Id: &DocumentID_U64{U64: 2}},
		},
		FilterJson:   `["status", "Eq", "deleted"]`,
		AllowPartial: true,
	})

	// Add patch_by_filter operation
	batch.AddFilterOp(&FilterOperation{
		Type:              FilterOperationType_FILTER_OPERATION_TYPE_PATCH,
		Phase1SnapshotSeq: 5,
		CandidateIds: []*DocumentID{
			{Id: &DocumentID_U64{U64: 3}},
			{Id: &DocumentID_U64{U64: 4}},
		},
		FilterJson:   `["status", "Eq", "pending"]`,
		PatchJson:    `{"status": "processed"}`,
		AllowPartial: false,
	})

	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify both filter operations are preserved
	filterOps := decoded.SubBatches[0].GetAllFilterOps()
	if len(filterOps) != 2 {
		t.Fatalf("expected 2 filter ops, got %d", len(filterOps))
	}

	// Verify ordering: delete before patch (sortEntry sorts by type)
	if filterOps[0].Type != FilterOperationType_FILTER_OPERATION_TYPE_DELETE {
		t.Errorf("first filter op should be DELETE, got %v", filterOps[0].Type)
	}
	if filterOps[1].Type != FilterOperationType_FILTER_OPERATION_TYPE_PATCH {
		t.Errorf("second filter op should be PATCH, got %v", filterOps[1].Type)
	}

	// Verify delete operation details
	deleteOp := filterOps[0]
	if deleteOp.Phase1SnapshotSeq != 5 {
		t.Errorf("delete op phase1_snapshot_seq mismatch: got %d, want 5", deleteOp.Phase1SnapshotSeq)
	}
	if len(deleteOp.CandidateIds) != 2 {
		t.Errorf("delete op candidate_ids count mismatch: got %d, want 2", len(deleteOp.CandidateIds))
	}
	if deleteOp.FilterJson != `["status", "Eq", "deleted"]` {
		t.Error("delete op filter_json mismatch")
	}
	if !deleteOp.AllowPartial {
		t.Error("delete op allow_partial should be true")
	}

	// Verify patch operation details
	patchOp := filterOps[1]
	if patchOp.Phase1SnapshotSeq != 5 {
		t.Errorf("patch op phase1_snapshot_seq mismatch: got %d, want 5", patchOp.Phase1SnapshotSeq)
	}
	if len(patchOp.CandidateIds) != 2 {
		t.Errorf("patch op candidate_ids count mismatch: got %d, want 2", len(patchOp.CandidateIds))
	}
	if patchOp.FilterJson != `["status", "Eq", "pending"]` {
		t.Error("patch op filter_json mismatch")
	}
	if patchOp.PatchJson != `{"status": "processed"}` {
		t.Error("patch op patch_json mismatch")
	}
	if patchOp.AllowPartial {
		t.Error("patch op allow_partial should be false")
	}
}

// TestMultiFilterOperationsOrdering verifies that filter operations are sorted
// by type (delete before patch) for deterministic encoding.
func TestMultiFilterOperationsOrdering(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry := NewWalEntry("order-test", 1)
	batch := NewWriteSubBatch("req-1")

	// Add patch first, then delete (opposite of expected order)
	batch.AddFilterOp(&FilterOperation{
		Type:       FilterOperationType_FILTER_OPERATION_TYPE_PATCH,
		FilterJson: `["a", "Eq", "1"]`,
	})
	batch.AddFilterOp(&FilterOperation{
		Type:       FilterOperationType_FILTER_OPERATION_TYPE_DELETE,
		FilterJson: `["b", "Eq", "2"]`,
	})

	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// After encoding, filter ops should be sorted: delete before patch
	filterOps := decoded.SubBatches[0].GetAllFilterOps()
	if len(filterOps) != 2 {
		t.Fatalf("expected 2 filter ops, got %d", len(filterOps))
	}

	if filterOps[0].Type != FilterOperationType_FILTER_OPERATION_TYPE_DELETE {
		t.Error("first filter op should be DELETE after sorting")
	}
	if filterOps[1].Type != FilterOperationType_FILTER_OPERATION_TYPE_PATCH {
		t.Error("second filter op should be PATCH after sorting")
	}
}

// TestBackwardCompatibility_FilterOp verifies that old WAL entries with the
// deprecated FilterOp field can still be read using GetAllFilterOps.
func TestBackwardCompatibility_FilterOp(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry := NewWalEntry("compat-test", 1)
	batch := NewWriteSubBatch("req-1")

	// Use the deprecated FilterOp field (simulating old WAL entry)
	batch.FilterOp = &FilterOperation{
		Type:       FilterOperationType_FILTER_OPERATION_TYPE_DELETE,
		FilterJson: `["status", "Eq", "old"]`,
	}
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// GetAllFilterOps should return the deprecated FilterOp
	filterOps := decoded.SubBatches[0].GetAllFilterOps()
	if len(filterOps) != 1 {
		t.Fatalf("expected 1 filter op from backward compat, got %d", len(filterOps))
	}
	if filterOps[0].Type != FilterOperationType_FILTER_OPERATION_TYPE_DELETE {
		t.Error("filter op type mismatch")
	}
	if filterOps[0].FilterJson != `["status", "Eq", "old"]` {
		t.Error("filter op filter_json mismatch")
	}
}

func TestSchemaDeltas(t *testing.T) {
	encoder, err := NewEncoder()
	if err != nil {
		t.Fatalf("failed to create encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := NewDecoder()
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}
	defer decoder.Close()

	entry := NewWalEntry("schema-test", 1)
	batch := NewWriteSubBatch("req-1")
	batch.SchemaDeltas = []*SchemaDelta{
		{
			Name:       "name",
			Type:       AttributeType_ATTRIBUTE_TYPE_STRING,
			Filterable: true,
		},
		{
			Name: "content",
			Type: AttributeType_ATTRIBUTE_TYPE_STRING,
			FullTextSearch: &FullTextConfig{
				Enabled:   true,
				Tokenizer: "unicode",
				Language:  "en",
				Stemming:  true,
			},
		},
		{
			Name: "embedding",
			Type: AttributeType_ATTRIBUTE_TYPE_FLOAT_ARRAY,
			Vector: &VectorConfig{
				Dtype:          "f32",
				Dimensions:     768,
				DistanceMetric: "cosine_distance",
			},
		},
	}
	entry.SubBatches = append(entry.SubBatches, batch)

	result, err := encoder.Encode(entry)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decoder.Decode(result.Data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	deltas := decoded.SubBatches[0].SchemaDeltas
	if len(deltas) != 3 {
		t.Fatalf("schema deltas count mismatch: got %d, want 3", len(deltas))
	}

	contentDelta := deltas[0]
	if contentDelta.Name != "content" {
		t.Errorf("expected 'content' first (sorted), got %q", contentDelta.Name)
	}
	if contentDelta.FullTextSearch == nil {
		t.Error("full_text_search should be set for content")
	} else if !contentDelta.FullTextSearch.Enabled {
		t.Error("full_text_search.enabled should be true")
	}

	embeddingDelta := deltas[1]
	if embeddingDelta.Name != "embedding" {
		t.Errorf("expected 'embedding' second, got %q", embeddingDelta.Name)
	}
	if embeddingDelta.Vector == nil {
		t.Error("vector config should be set for embedding")
	} else {
		if embeddingDelta.Vector.Dimensions != 768 {
			t.Errorf("vector dimensions mismatch: got %d, want 768", embeddingDelta.Vector.Dimensions)
		}
		if embeddingDelta.Vector.DistanceMetric != "cosine_distance" {
			t.Errorf("distance_metric mismatch: got %q", embeddingDelta.Vector.DistanceMetric)
		}
	}
}
