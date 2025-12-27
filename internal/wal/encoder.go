// Package wal implements the Write-Ahead Log entry format with protobuf encoding and zstd compression.
package wal

import (
	"bytes"
	"crypto/sha256"
	"io"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/vexsearch/vex/internal/document"
	"google.golang.org/protobuf/proto"
)

const (
	FormatVersion = 1

	FileExtension = ".wal.zst"
)

type Encoder struct {
	compressor *zstd.Encoder
	buf        bytes.Buffer
}

func NewEncoder() (*Encoder, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	return &Encoder{
		compressor: enc,
	}, nil
}

func (e *Encoder) Close() error {
	return e.compressor.Close()
}

type EncodeResult struct {
	Data     []byte
	Checksum [32]byte
}

func (e *Encoder) Encode(entry *WalEntry) (*EncodeResult, error) {
	sortEntry(entry)

	entryForChecksum := proto.Clone(entry).(*WalEntry)
	entryForChecksum.Checksum = nil

	opts := proto.MarshalOptions{Deterministic: true}
	protoBytes, err := opts.Marshal(entryForChecksum)
	if err != nil {
		return nil, err
	}

	checksum := sha256.Sum256(protoBytes)
	entry.Checksum = checksum[:]

	protoBytes, err = opts.Marshal(entry)
	if err != nil {
		return nil, err
	}

	e.buf.Reset()
	e.compressor.Reset(&e.buf)
	if _, err := e.compressor.Write(protoBytes); err != nil {
		return nil, err
	}
	if err := e.compressor.Close(); err != nil {
		return nil, err
	}

	data := append([]byte(nil), e.buf.Bytes()...)
	return &EncodeResult{
		Data:     data,
		Checksum: checksum,
	}, nil
}

type Decoder struct {
	decompressor *zstd.Decoder
}

func NewDecoder() (*Decoder, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return &Decoder{
		decompressor: dec,
	}, nil
}

func (d *Decoder) Close() {
	d.decompressor.Close()
}

func (d *Decoder) Decode(compressed []byte) (*WalEntry, error) {
	if err := d.decompressor.Reset(bytes.NewReader(compressed)); err != nil {
		return nil, err
	}

	decompressed, err := io.ReadAll(d.decompressor)
	if err != nil {
		return nil, err
	}

	var entry WalEntry
	if err := proto.Unmarshal(decompressed, &entry); err != nil {
		return nil, err
	}

	storedChecksum := entry.Checksum
	entryForVerify := proto.Clone(&entry).(*WalEntry)
	entryForVerify.Checksum = nil

	opts := proto.MarshalOptions{Deterministic: true}
	protoBytes, err := opts.Marshal(entryForVerify)
	if err != nil {
		return nil, err
	}
	computedChecksum := sha256.Sum256(protoBytes)
	if !bytes.Equal(storedChecksum, computedChecksum[:]) {
		return nil, ErrChecksumMismatch
	}

	return &entry, nil
}

func sortEntry(entry *WalEntry) {
	for _, batch := range entry.SubBatches {
		sort.Slice(batch.Mutations, func(i, j int) bool {
			return compareDocumentIDs(batch.Mutations[i].Id, batch.Mutations[j].Id) < 0
		})
		sort.Slice(batch.SchemaDeltas, func(i, j int) bool {
			return batch.SchemaDeltas[i].Name < batch.SchemaDeltas[j].Name
		})
	}
}

func compareDocumentIDs(a, b *DocumentID) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	aType := getIDType(a)
	bType := getIDType(b)
	if aType != bType {
		return aType - bType
	}

	switch aType {
	case 1:
		if a.GetU64() < b.GetU64() {
			return -1
		}
		if a.GetU64() > b.GetU64() {
			return 1
		}
		return 0
	case 2:
		return bytes.Compare(a.GetUuid(), b.GetUuid())
	case 3:
		if a.GetStr() < b.GetStr() {
			return -1
		}
		if a.GetStr() > b.GetStr() {
			return 1
		}
		return 0
	}
	return 0
}

func getIDType(id *DocumentID) int {
	switch id.GetId().(type) {
	case *DocumentID_U64:
		return 1
	case *DocumentID_Uuid:
		return 2
	case *DocumentID_Str:
		return 3
	default:
		return 0
	}
}

func DocumentIDFromID(id document.ID) *DocumentID {
	switch id.Type() {
	case document.IDTypeU64:
		return &DocumentID{Id: &DocumentID_U64{U64: id.U64()}}
	case document.IDTypeUUID:
		u := id.UUID()
		return &DocumentID{Id: &DocumentID_Uuid{Uuid: u[:]}}
	case document.IDTypeString:
		return &DocumentID{Id: &DocumentID_Str{Str: id.String()}}
	default:
		return nil
	}
}

func DocumentIDToID(protoID *DocumentID) (document.ID, error) {
	if protoID == nil {
		return document.ID{}, ErrInvalidDocumentID
	}

	switch v := protoID.GetId().(type) {
	case *DocumentID_U64:
		return document.NewU64ID(v.U64), nil
	case *DocumentID_Uuid:
		if len(v.Uuid) != 16 {
			return document.ID{}, ErrInvalidDocumentID
		}
		var u uuid.UUID
		copy(u[:], v.Uuid)
		return document.NewUUIDID(u), nil
	case *DocumentID_Str:
		return document.NewStringID(v.Str)
	default:
		return document.ID{}, ErrInvalidDocumentID
	}
}

func NewWalEntry(namespace string, seq uint64) *WalEntry {
	return &WalEntry{
		FormatVersion:   FormatVersion,
		Namespace:       namespace,
		Seq:             seq,
		CommittedUnixMs: time.Now().UnixMilli(),
		SubBatches:      make([]*WriteSubBatch, 0),
	}
}

func NewWriteSubBatch(requestID string) *WriteSubBatch {
	return &WriteSubBatch{
		RequestId:    requestID,
		ReceivedAtMs: time.Now().UnixMilli(),
		Mutations:    make([]*Mutation, 0),
		Stats:        &SubBatchStats{},
		SchemaDeltas: make([]*SchemaDelta, 0),
	}
}

func (b *WriteSubBatch) AddUpsert(id *DocumentID, attrs map[string]*AttributeValue, vector []byte, dims uint32) {
	b.Mutations = append(b.Mutations, &Mutation{
		Type:       MutationType_MUTATION_TYPE_UPSERT,
		Id:         id,
		Attributes: attrs,
		Vector:     vector,
		VectorDims: dims,
	})
	b.Stats.RowsUpserted++
	b.Stats.RowsAffected++
}

func (b *WriteSubBatch) AddPatch(id *DocumentID, attrs map[string]*AttributeValue) {
	b.Mutations = append(b.Mutations, &Mutation{
		Type:       MutationType_MUTATION_TYPE_PATCH,
		Id:         id,
		Attributes: attrs,
	})
	b.Stats.RowsPatched++
	b.Stats.RowsAffected++
}

func (b *WriteSubBatch) AddDelete(id *DocumentID) {
	b.Mutations = append(b.Mutations, &Mutation{
		Type: MutationType_MUTATION_TYPE_DELETE,
		Id:   id,
	})
	b.Stats.RowsDeleted++
	b.Stats.RowsAffected++
}

func StringValue(s string) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_StringVal{StringVal: s}}
}

func IntValue(v int64) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_IntVal{IntVal: v}}
}

func UintValue(v uint64) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_UintVal{UintVal: v}}
}

func FloatValue(v float64) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_FloatVal{FloatVal: v}}
}

func UuidValue(u uuid.UUID) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_UuidVal{UuidVal: u[:]}}
}

func DatetimeValue(ms int64) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_DatetimeVal{DatetimeVal: ms}}
}

func BoolValue(b bool) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_BoolVal{BoolVal: b}}
}

func NullValue() *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_NullVal{NullVal: true}}
}

func StringArrayValue(values []string) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_StringArray{StringArray: &StringArray{Values: values}}}
}

func IntArrayValue(values []int64) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_IntArray{IntArray: &IntArray{Values: values}}}
}

func UintArrayValue(values []uint64) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_UintArray{UintArray: &UintArray{Values: values}}}
}

func FloatArrayValue(values []float64) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_FloatArray{FloatArray: &FloatArray{Values: values}}}
}

func DatetimeArrayValue(values []int64) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_DatetimeArray{DatetimeArray: &DatetimeArray{Values: values}}}
}

func BoolArrayValue(values []bool) *AttributeValue {
	return &AttributeValue{Value: &AttributeValue_BoolArray{BoolArray: &BoolArray{Values: values}}}
}

func ValidateString(s string) error {
	if !utf8.ValidString(s) {
		return ErrInvalidUTF8
	}
	return nil
}

func KeyForSeq(seq uint64) string {
	return keyForSeq(seq)
}

func keyForSeq(seq uint64) string {
	return "wal/" + uintToString(seq) + FileExtension
}

func uintToString(n uint64) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 20)
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte(n%10) + '0'
		n /= 10
	}
	return string(buf[i:])
}
