package index

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/klauspost/compress/zstd"
)

const docsColumnVersion = 1

var (
	docsColumnMagic = [4]byte{'V', 'E', 'X', 'D'}
	zstdMagic       = [4]byte{0x28, 0xB5, 0x2F, 0xFD}
)

var (
	ErrDocsColumnFormat  = errors.New("invalid docs column format")
	ErrDocsColumnVersion = errors.New("unsupported docs column version")
)

// DocColumn represents a document in the columnar docs format.
type DocColumn struct {
	ID         string
	NumericID  uint64
	WALSeq     uint64
	Deleted    bool
	Attributes map[string]any
	Vector     []float32
}

// EncodeDocsColumn encodes documents into the uncompressed columnar format.
func EncodeDocsColumn(docs []DocColumn) ([]byte, error) {
	docCount := len(docs)
	vectorDims := 0
	for _, doc := range docs {
		if len(doc.Vector) == 0 {
			continue
		}
		if vectorDims == 0 {
			vectorDims = len(doc.Vector)
			continue
		}
		if len(doc.Vector) != vectorDims {
			return nil, fmt.Errorf("vector dims mismatch: expected %d, got %d", vectorDims, len(doc.Vector))
		}
	}

	var buf bytes.Buffer
	buf.Grow(32 + docCount*32)

	buf.Write(docsColumnMagic[:])
	buf.WriteByte(byte(docsColumnVersion))
	buf.WriteByte(0)
	buf.Write([]byte{0, 0})
	writeUint64(&buf, uint64(docCount))
	writeUint32(&buf, uint32(vectorDims))

	for _, doc := range docs {
		writeUint64(&buf, doc.NumericID)
	}
	for _, doc := range docs {
		writeUint64(&buf, doc.WALSeq)
	}
	for _, doc := range docs {
		if doc.Deleted {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}
	for _, doc := range docs {
		if err := writeBytesWithLen(&buf, []byte(doc.ID)); err != nil {
			return nil, err
		}
	}
	for _, doc := range docs {
		if len(doc.Attributes) == 0 {
			writeUint32(&buf, 0)
			continue
		}
		attrBytes, err := json.Marshal(doc.Attributes)
		if err != nil {
			return nil, err
		}
		if err := writeBytesWithLen(&buf, attrBytes); err != nil {
			return nil, err
		}
	}
	if vectorDims > 0 {
		for _, doc := range docs {
			if len(doc.Vector) > 0 {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		}
		for _, doc := range docs {
			if len(doc.Vector) == 0 {
				continue
			}
			if len(doc.Vector) != vectorDims {
				return nil, fmt.Errorf("vector dims mismatch: expected %d, got %d", vectorDims, len(doc.Vector))
			}
			for _, value := range doc.Vector {
				writeFloat32(&buf, value)
			}
		}
	}

	return buf.Bytes(), nil
}

// CompressDocsColumn zstd-compresses the uncompressed columnar bytes.
func CompressDocsColumn(data []byte) ([]byte, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	defer enc.Close()

	var buf bytes.Buffer
	enc.Reset(&buf)
	if _, err := enc.Write(data); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// EncodeDocsColumnZstd encodes documents into the columnar format and compresses them with zstd.
func EncodeDocsColumnZstd(docs []DocColumn) ([]byte, error) {
	raw, err := EncodeDocsColumn(docs)
	if err != nil {
		return nil, err
	}
	return CompressDocsColumn(raw)
}

// DecodeDocsColumn decodes documents from the uncompressed columnar format.
func DecodeDocsColumn(data []byte) ([]DocColumn, error) {
	reader := bytes.NewReader(data)
	return decodeDocsColumn(reader)
}

// DecodeDocsColumnForIDs decodes only the requested numeric IDs from an uncompressed columnar reader.
func DecodeDocsColumnForIDs(r io.Reader, idSet map[uint64]struct{}) ([]DocColumn, error) {
	if len(idSet) == 0 {
		return nil, nil
	}

	docCount, vectorDims, err := readDocsColumnHeader(r)
	if err != nil {
		return nil, err
	}
	if docCount == 0 {
		return nil, nil
	}

	numericIDs := make([]uint64, docCount)
	if err := binary.Read(r, binary.LittleEndian, numericIDs); err != nil {
		return nil, fmt.Errorf("failed to read numeric IDs: %w", err)
	}

	walSeqs := make([]uint64, docCount)
	if err := binary.Read(r, binary.LittleEndian, walSeqs); err != nil {
		return nil, fmt.Errorf("failed to read WAL sequences: %w", err)
	}

	deletedFlags := make([]byte, docCount)
	if _, err := io.ReadFull(r, deletedFlags); err != nil {
		return nil, fmt.Errorf("failed to read deleted flags: %w", err)
	}

	selectedPositions := make(map[int]int)
	result := make([]DocColumn, 0, len(idSet))
	for i, id := range numericIDs {
		if _, ok := idSet[id]; !ok {
			continue
		}
		selectedPositions[i] = len(result)
		result = append(result, DocColumn{
			NumericID: id,
			WALSeq:    walSeqs[i],
			Deleted:   deletedFlags[i] == 1,
		})
	}
	if len(result) == 0 {
		return nil, nil
	}

	for i := 0; i < docCount; i++ {
		length, err := readUint32(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read ID length: %w", err)
		}
		pos, keep := selectedPositions[i]
		if !keep {
			if err := discardBytes(r, int64(length)); err != nil {
				return nil, fmt.Errorf("failed to skip ID bytes: %w", err)
			}
			continue
		}
		if length == 0 {
			continue
		}
		idBytes := make([]byte, length)
		if _, err := io.ReadFull(r, idBytes); err != nil {
			return nil, fmt.Errorf("failed to read ID bytes: %w", err)
		}
		result[pos].ID = string(idBytes)
	}

	for i := 0; i < docCount; i++ {
		length, err := readUint32(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read attributes length: %w", err)
		}
		pos, keep := selectedPositions[i]
		if !keep {
			if err := discardBytes(r, int64(length)); err != nil {
				return nil, fmt.Errorf("failed to skip attributes bytes: %w", err)
			}
			continue
		}
		if length == 0 {
			continue
		}
		attrBytes := make([]byte, length)
		if _, err := io.ReadFull(r, attrBytes); err != nil {
			return nil, fmt.Errorf("failed to read attributes bytes: %w", err)
		}
		var attrs map[string]any
		if err := json.Unmarshal(attrBytes, &attrs); err != nil {
			attrs = make(map[string]any)
		}
		result[pos].Attributes = attrs
	}

	if vectorDims > 0 {
		hasVector := make([]byte, docCount)
		if _, err := io.ReadFull(r, hasVector); err != nil {
			return nil, fmt.Errorf("failed to read vector flags: %w", err)
		}
		for i := 0; i < docCount; i++ {
			if hasVector[i] == 0 {
				continue
			}
			vectorBytes := int64(vectorDims) * 4
			pos, keep := selectedPositions[i]
			if !keep {
				if err := discardBytes(r, vectorBytes); err != nil {
					return nil, fmt.Errorf("failed to skip vector bytes: %w", err)
				}
				continue
			}
			raw := make([]byte, vectorBytes)
			if _, err := io.ReadFull(r, raw); err != nil {
				return nil, fmt.Errorf("failed to read vector bytes: %w", err)
			}
			vec := make([]float32, vectorDims)
			for j := 0; j < vectorDims; j++ {
				start := j * 4
				vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(raw[start : start+4]))
			}
			result[pos].Vector = vec
		}
	}

	return result, nil
}

// DecodeDocsColumnForRowIDs decodes only the requested row indices from an uncompressed columnar reader.
// This returns a map from row index to DocColumn for efficient lookup.
func DecodeDocsColumnForRowIDs(r io.Reader, rowIDSet map[uint32]struct{}) (map[uint32]DocColumn, error) {
	if len(rowIDSet) == 0 {
		return nil, nil
	}

	docCount, vectorDims, err := readDocsColumnHeader(r)
	if err != nil {
		return nil, err
	}
	if docCount == 0 {
		return nil, nil
	}

	numericIDs := make([]uint64, docCount)
	if err := binary.Read(r, binary.LittleEndian, numericIDs); err != nil {
		return nil, fmt.Errorf("failed to read numeric IDs: %w", err)
	}

	walSeqs := make([]uint64, docCount)
	if err := binary.Read(r, binary.LittleEndian, walSeqs); err != nil {
		return nil, fmt.Errorf("failed to read WAL sequences: %w", err)
	}

	deletedFlags := make([]byte, docCount)
	if _, err := io.ReadFull(r, deletedFlags); err != nil {
		return nil, fmt.Errorf("failed to read deleted flags: %w", err)
	}

	result := make(map[uint32]DocColumn, len(rowIDSet))
	for i := 0; i < docCount; i++ {
		if _, ok := rowIDSet[uint32(i)]; !ok {
			continue
		}
		result[uint32(i)] = DocColumn{
			NumericID: numericIDs[i],
			WALSeq:    walSeqs[i],
			Deleted:   deletedFlags[i] == 1,
		}
	}
	if len(result) == 0 {
		return nil, nil
	}

	for i := 0; i < docCount; i++ {
		length, err := readUint32(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read ID length: %w", err)
		}
		if _, keep := rowIDSet[uint32(i)]; !keep {
			if err := discardBytes(r, int64(length)); err != nil {
				return nil, fmt.Errorf("failed to skip ID bytes: %w", err)
			}
			continue
		}
		if length == 0 {
			continue
		}
		idBytes := make([]byte, length)
		if _, err := io.ReadFull(r, idBytes); err != nil {
			return nil, fmt.Errorf("failed to read ID bytes: %w", err)
		}
		doc := result[uint32(i)]
		doc.ID = string(idBytes)
		result[uint32(i)] = doc
	}

	for i := 0; i < docCount; i++ {
		length, err := readUint32(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read attributes length: %w", err)
		}
		if _, keep := rowIDSet[uint32(i)]; !keep {
			if err := discardBytes(r, int64(length)); err != nil {
				return nil, fmt.Errorf("failed to skip attributes bytes: %w", err)
			}
			continue
		}
		if length == 0 {
			continue
		}
		attrBytes := make([]byte, length)
		if _, err := io.ReadFull(r, attrBytes); err != nil {
			return nil, fmt.Errorf("failed to read attributes bytes: %w", err)
		}
		var attrs map[string]any
		if err := json.Unmarshal(attrBytes, &attrs); err != nil {
			attrs = make(map[string]any)
		}
		doc := result[uint32(i)]
		doc.Attributes = attrs
		result[uint32(i)] = doc
	}

	if vectorDims == 0 {
		return result, nil
	}

	bytesPerVector := int(vectorDims * 4)
	for i := 0; i < docCount; i++ {
		if _, keep := rowIDSet[uint32(i)]; !keep {
			if err := discardBytes(r, int64(bytesPerVector)); err != nil {
				return nil, fmt.Errorf("failed to skip vector bytes: %w", err)
			}
			continue
		}
		raw := make([]byte, bytesPerVector)
		if _, err := io.ReadFull(r, raw); err != nil {
			return nil, fmt.Errorf("failed to read vector bytes: %w", err)
		}
		vec := make([]float32, vectorDims)
		for j := 0; j < int(vectorDims); j++ {
			start := j * 4
			vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(raw[start : start+4]))
		}
		doc := result[uint32(i)]
		doc.Vector = vec
		result[uint32(i)] = doc
	}

	return result, nil
}

// IsZstdCompressed reports whether the data starts with the zstd magic header.
func IsZstdCompressed(data []byte) bool {
	if len(data) < len(zstdMagic) {
		return false
	}
	return bytes.Equal(data[:len(zstdMagic)], zstdMagic[:])
}

// DecompressZstd decompresses zstd-compressed data.
func DecompressZstd(data []byte) ([]byte, error) {
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxWindow(32*1024*1024),
		zstd.WithDecoderConcurrency(1),
	)
	if err != nil {
		return nil, err
	}
	defer dec.Close()

	return dec.DecodeAll(data, nil)
}

func decodeDocsColumn(r io.Reader) ([]DocColumn, error) {
	docCount, vectorDims, err := readDocsColumnHeader(r)
	if err != nil {
		return nil, err
	}
	if docCount == 0 {
		return nil, nil
	}

	numericIDs := make([]uint64, docCount)
	if err := binary.Read(r, binary.LittleEndian, numericIDs); err != nil {
		return nil, fmt.Errorf("failed to read numeric IDs: %w", err)
	}

	walSeqs := make([]uint64, docCount)
	if err := binary.Read(r, binary.LittleEndian, walSeqs); err != nil {
		return nil, fmt.Errorf("failed to read WAL sequences: %w", err)
	}

	deletedFlags := make([]byte, docCount)
	if _, err := io.ReadFull(r, deletedFlags); err != nil {
		return nil, fmt.Errorf("failed to read deleted flags: %w", err)
	}

	docs := make([]DocColumn, docCount)
	for i := 0; i < docCount; i++ {
		docs[i].NumericID = numericIDs[i]
		docs[i].WALSeq = walSeqs[i]
		docs[i].Deleted = deletedFlags[i] == 1
	}

	for i := 0; i < docCount; i++ {
		length, err := readUint32(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read ID length: %w", err)
		}
		if length == 0 {
			continue
		}
		idBytes := make([]byte, length)
		if _, err := io.ReadFull(r, idBytes); err != nil {
			return nil, fmt.Errorf("failed to read ID bytes: %w", err)
		}
		docs[i].ID = string(idBytes)
	}

	for i := 0; i < docCount; i++ {
		length, err := readUint32(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read attributes length: %w", err)
		}
		if length == 0 {
			continue
		}
		attrBytes := make([]byte, length)
		if _, err := io.ReadFull(r, attrBytes); err != nil {
			return nil, fmt.Errorf("failed to read attributes bytes: %w", err)
		}
		var attrs map[string]any
		if err := json.Unmarshal(attrBytes, &attrs); err != nil {
			return nil, fmt.Errorf("failed to decode attributes: %w", err)
		}
		docs[i].Attributes = attrs
	}

	if vectorDims > 0 {
		hasVector := make([]byte, docCount)
		if _, err := io.ReadFull(r, hasVector); err != nil {
			return nil, fmt.Errorf("failed to read vector flags: %w", err)
		}
		for i := 0; i < docCount; i++ {
			if hasVector[i] == 0 {
				continue
			}
			raw := make([]byte, int64(vectorDims)*4)
			if _, err := io.ReadFull(r, raw); err != nil {
				return nil, fmt.Errorf("failed to read vector bytes: %w", err)
			}
			vec := make([]float32, vectorDims)
			for j := 0; j < vectorDims; j++ {
				start := j * 4
				vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(raw[start : start+4]))
			}
			docs[i].Vector = vec
		}
	}

	return docs, nil
}

func readDocsColumnHeader(r io.Reader) (int, int, error) {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return 0, 0, fmt.Errorf("failed to read docs magic: %w", err)
	}
	if magic != docsColumnMagic {
		return 0, 0, ErrDocsColumnFormat
	}
	version, err := readByte(r)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read docs version: %w", err)
	}
	if version != docsColumnVersion {
		return 0, 0, ErrDocsColumnVersion
	}
	if _, err := readByte(r); err != nil {
		return 0, 0, fmt.Errorf("failed to read docs flags: %w", err)
	}
	if _, err := readUint16(r); err != nil {
		return 0, 0, fmt.Errorf("failed to read docs padding: %w", err)
	}
	docCount, err := readUint64(r)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read docs count: %w", err)
	}
	vectorDims, err := readUint32(r)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read vector dims: %w", err)
	}
	if docCount > uint64(maxInt()) {
		return 0, 0, fmt.Errorf("docs column too large: %d", docCount)
	}
	return int(docCount), int(vectorDims), nil
}

func writeUint64(buf *bytes.Buffer, value uint64) {
	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], value)
	buf.Write(scratch[:])
}

func writeUint32(buf *bytes.Buffer, value uint32) {
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[:], value)
	buf.Write(scratch[:])
}

func writeFloat32(buf *bytes.Buffer, value float32) {
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[:], math.Float32bits(value))
	buf.Write(scratch[:])
}

func writeBytesWithLen(buf *bytes.Buffer, data []byte) error {
	if len(data) > math.MaxUint32 {
		return fmt.Errorf("column data too large: %d", len(data))
	}
	writeUint32(buf, uint32(len(data)))
	if len(data) > 0 {
		buf.Write(data)
	}
	return nil
}

func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

func readUint16(r io.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b[:]), nil
}

func readUint32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func readUint64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b[:]), nil
}

func discardBytes(r io.Reader, n int64) error {
	if n <= 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, r, n)
	return err
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
