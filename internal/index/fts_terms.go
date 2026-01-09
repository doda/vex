package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	ftsTermsMagic   = "FTTR"
	ftsTermsVersion = 1
)

// EncodeFTSTerms serializes a sorted list of terms for a segment.
func EncodeFTSTerms(terms []string) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(ftsTermsMagic)
	buf.WriteByte(ftsTermsVersion)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(terms))); err != nil {
		return nil, fmt.Errorf("failed to write fts terms count: %w", err)
	}
	for _, term := range terms {
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(term))); err != nil {
			return nil, fmt.Errorf("failed to write fts term length: %w", err)
		}
		if _, err := buf.WriteString(term); err != nil {
			return nil, fmt.Errorf("failed to write fts term: %w", err)
		}
	}
	return buf.Bytes(), nil
}

// DecodeFTSTerms parses a serialized term list.
func DecodeFTSTerms(data []byte) ([]string, error) {
	if len(data) < len(ftsTermsMagic)+1 {
		return nil, fmt.Errorf("fts terms too short")
	}
	if string(data[:len(ftsTermsMagic)]) != ftsTermsMagic {
		return nil, fmt.Errorf("invalid fts terms magic")
	}
	version := data[len(ftsTermsMagic)]
	if version != ftsTermsVersion {
		return nil, fmt.Errorf("unsupported fts terms version %d", version)
	}
	reader := bytes.NewReader(data[len(ftsTermsMagic)+1:])

	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf("failed to read fts terms count: %w", err)
	}

	terms := make([]string, 0, count)
	for i := uint32(0); i < count; i++ {
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			return nil, fmt.Errorf("failed to read fts term length: %w", err)
		}
		if length == 0 {
			terms = append(terms, "")
			continue
		}
		raw := make([]byte, length)
		if _, err := io.ReadFull(reader, raw); err != nil {
			return nil, fmt.Errorf("failed to read fts term data: %w", err)
		}
		terms = append(terms, string(raw))
	}
	return terms, nil
}
