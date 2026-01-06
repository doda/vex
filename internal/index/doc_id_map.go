package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var (
	docIDMapMagic   = [4]byte{'D', 'I', 'D', 'M'}
	docIDMapVersion = uint32(1)
)

// EncodeDocIDMap serializes numeric IDs in row order for fast row lookups.
// Format:
// - 4 bytes magic "DIDM"
// - 4 bytes version
// - 8 bytes count
// - count * 8 bytes numeric IDs (row order)
func EncodeDocIDMap(ids []uint64) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(16 + len(ids)*8)
	buf.Write(docIDMapMagic[:])
	if err := binary.Write(&buf, binary.LittleEndian, docIDMapVersion); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint64(len(ids))); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := binary.Write(&buf, binary.LittleEndian, id); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// DecodeDocIDMap deserializes numeric IDs from a DIDM buffer.
func DecodeDocIDMap(data []byte) ([]uint64, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("doc id map too short")
	}
	if !bytes.Equal(data[:4], docIDMapMagic[:]) {
		return nil, fmt.Errorf("invalid doc id map magic")
	}
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != docIDMapVersion {
		return nil, fmt.Errorf("unsupported doc id map version %d", version)
	}
	count := binary.LittleEndian.Uint64(data[8:16])
	expected := 16 + int(count)*8
	if len(data) < expected {
		return nil, fmt.Errorf("doc id map truncated")
	}
	ids := make([]uint64, count)
	offset := 16
	for i := 0; i < int(count); i++ {
		ids[i] = binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
	}
	return ids, nil
}

// EncodeDocIDMapFromDocs builds a DIDM payload from docs in row order.
func EncodeDocIDMapFromDocs(docs []DocColumn) ([]byte, error) {
	ids := make([]uint64, len(docs))
	for i, doc := range docs {
		ids[i] = doc.NumericID
	}
	return EncodeDocIDMap(ids)
}
