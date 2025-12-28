// Package fts provides full-text search indexing and tokenization.
package fts

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/RoaringBitmap/roaring"
)

// Index represents a BM25 full-text search index for a single attribute.
type Index struct {
	// Attribute is the name of the indexed attribute.
	Attribute string `json:"attribute"`

	// Config is the FTS configuration used to build this index.
	Config *Config `json:"config"`

	// TermPostings maps each term to a posting list (document IDs containing the term).
	// The document IDs are internal row IDs within the segment.
	TermPostings map[string]*roaring.Bitmap `json:"-"`

	// TermFreqs maps term -> docID -> term frequency (how many times term appears in doc).
	TermFreqs map[string]map[uint32]uint32 `json:"-"`

	// DocLengths maps docID to the total number of tokens in that document's field.
	DocLengths map[uint32]uint32 `json:"-"`

	// TotalDocs is the total number of documents in this index.
	TotalDocs uint32 `json:"total_docs"`

	// AvgDocLength is the average document length (for BM25 normalization).
	AvgDocLength float64 `json:"avg_doc_length"`

	// TotalTerms is the total number of distinct terms.
	TotalTerms uint32 `json:"total_terms"`
}

// NewIndex creates a new empty BM25 index.
func NewIndex(attribute string, cfg *Config) *Index {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return &Index{
		Attribute:    attribute,
		Config:       cfg,
		TermPostings: make(map[string]*roaring.Bitmap),
		TermFreqs:    make(map[string]map[uint32]uint32),
		DocLengths:   make(map[uint32]uint32),
	}
}

// AddDocument adds a document to the index.
// docID is the internal row ID within the segment.
// text is the text content to index.
// If a document with the same docID already exists, it is replaced (old terms removed first).
func (idx *Index) AddDocument(docID uint32, text string) {
	tokenizer := NewTokenizer(idx.Config)
	tokens := tokenizer.Tokenize(text)

	if idx.Config.RemoveStopwords {
		tokens = RemoveStopwords(tokens)
	}

	// Check if this document already exists (update case)
	_, isUpdate := idx.DocLengths[docID]

	if len(tokens) == 0 {
		// If updating and new text is empty, remove the document entirely
		if isUpdate {
			idx.removeDocument(docID)
		}
		return
	}

	// If updating, remove old terms first to avoid stale postings
	if isUpdate {
		idx.removeDocumentTerms(docID)
	}

	// Count token frequencies in this document
	termFreq := make(map[string]uint32)
	for _, token := range tokens {
		termFreq[token]++
	}

	// Update postings and term frequencies
	for term, freq := range termFreq {
		if _, ok := idx.TermPostings[term]; !ok {
			idx.TermPostings[term] = roaring.New()
			idx.TermFreqs[term] = make(map[uint32]uint32)
		}
		idx.TermPostings[term].Add(docID)
		idx.TermFreqs[term][docID] = freq
	}

	// Store document length
	idx.DocLengths[docID] = uint32(len(tokens))

	// Only increment TotalDocs for new documents
	if !isUpdate {
		idx.TotalDocs++
	}

	// Recalculate average document length
	idx.recalculateAvgDocLength()

	idx.TotalTerms = uint32(len(idx.TermPostings))
}

// removeDocumentTerms removes all term postings and frequencies for a document
// but keeps the document in DocLengths (for update scenarios).
func (idx *Index) removeDocumentTerms(docID uint32) {
	for term, posting := range idx.TermPostings {
		if posting.Contains(docID) {
			posting.Remove(docID)
			delete(idx.TermFreqs[term], docID)

			// If no more documents have this term, clean up
			if posting.IsEmpty() {
				delete(idx.TermPostings, term)
				delete(idx.TermFreqs, term)
			}
		}
	}
}

// removeDocument completely removes a document from the index.
func (idx *Index) removeDocument(docID uint32) {
	if _, exists := idx.DocLengths[docID]; !exists {
		return
	}

	idx.removeDocumentTerms(docID)
	delete(idx.DocLengths, docID)
	idx.TotalDocs--
	idx.recalculateAvgDocLength()
	idx.TotalTerms = uint32(len(idx.TermPostings))
}

// recalculateAvgDocLength updates the average document length.
func (idx *Index) recalculateAvgDocLength() {
	var totalLen uint64
	for _, l := range idx.DocLengths {
		totalLen += uint64(l)
	}
	if idx.TotalDocs > 0 {
		idx.AvgDocLength = float64(totalLen) / float64(idx.TotalDocs)
	} else {
		idx.AvgDocLength = 0
	}
}

// IndexBuilder builds FTS indexes for multiple attributes.
type IndexBuilder struct {
	indexes map[string]*Index
}

// NewIndexBuilder creates a new index builder.
func NewIndexBuilder() *IndexBuilder {
	return &IndexBuilder{
		indexes: make(map[string]*Index),
	}
}

// AddDocument adds a document with the given attributes to the builder.
// Only attributes with FTS config will be indexed.
func (b *IndexBuilder) AddDocument(docID uint32, attrs map[string]any, ftsConfigs map[string]*Config) {
	for attrName, cfg := range ftsConfigs {
		if cfg == nil {
			continue
		}

		val, ok := attrs[attrName]
		if !ok || val == nil {
			continue
		}

		text, ok := val.(string)
		if !ok {
			continue
		}

		idx, ok := b.indexes[attrName]
		if !ok {
			idx = NewIndex(attrName, cfg)
			b.indexes[attrName] = idx
		}

		idx.AddDocument(docID, text)
	}
}

// Build returns all built indexes.
func (b *IndexBuilder) Build() map[string]*Index {
	return b.indexes
}

// Serialize serializes the index to bytes for storage.
// Format:
// - 4 bytes: magic number "BM25"
// - 4 bytes: format version (1)
// - 4 bytes: JSON metadata length
// - N bytes: JSON metadata
// - 4 bytes: number of terms
// - For each term:
//   - 4 bytes: term length
//   - N bytes: term string
//   - 4 bytes: posting list size
//   - N bytes: roaring bitmap
//   - 4 bytes: number of term freq entries
//   - For each entry: 4 bytes docID, 4 bytes freq
//
// - 4 bytes: number of doc lengths
// - For each: 4 bytes docID, 4 bytes length
func (idx *Index) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Magic number
	buf.WriteString("BM25")

	// Version
	if err := binary.Write(&buf, binary.LittleEndian, uint32(1)); err != nil {
		return nil, err
	}

	// Metadata JSON
	meta := struct {
		Attribute    string  `json:"attribute"`
		TotalDocs    uint32  `json:"total_docs"`
		AvgDocLength float64 `json:"avg_doc_length"`
		TotalTerms   uint32  `json:"total_terms"`
		Config       *Config `json:"config"`
	}{
		Attribute:    idx.Attribute,
		TotalDocs:    idx.TotalDocs,
		AvgDocLength: idx.AvgDocLength,
		TotalTerms:   idx.TotalTerms,
		Config:       idx.Config,
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(metaBytes))); err != nil {
		return nil, err
	}
	buf.Write(metaBytes)

	// Sort terms for deterministic output
	terms := make([]string, 0, len(idx.TermPostings))
	for term := range idx.TermPostings {
		terms = append(terms, term)
	}
	sort.Strings(terms)

	// Number of terms
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(terms))); err != nil {
		return nil, err
	}

	// Write each term
	for _, term := range terms {
		posting := idx.TermPostings[term]
		freqs := idx.TermFreqs[term]

		// Term string
		termBytes := []byte(term)
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(termBytes))); err != nil {
			return nil, err
		}
		buf.Write(termBytes)

		// Posting list (roaring bitmap)
		bitmapBytes, err := posting.ToBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize posting list: %w", err)
		}
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(bitmapBytes))); err != nil {
			return nil, err
		}
		buf.Write(bitmapBytes)

		// Term frequencies
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(freqs))); err != nil {
			return nil, err
		}
		// Sort doc IDs for deterministic output
		docIDs := make([]uint32, 0, len(freqs))
		for docID := range freqs {
			docIDs = append(docIDs, docID)
		}
		sort.Slice(docIDs, func(i, j int) bool { return docIDs[i] < docIDs[j] })

		for _, docID := range docIDs {
			if err := binary.Write(&buf, binary.LittleEndian, docID); err != nil {
				return nil, err
			}
			if err := binary.Write(&buf, binary.LittleEndian, freqs[docID]); err != nil {
				return nil, err
			}
		}
	}

	// Document lengths
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(idx.DocLengths))); err != nil {
		return nil, err
	}
	// Sort doc IDs for deterministic output
	docIDs := make([]uint32, 0, len(idx.DocLengths))
	for docID := range idx.DocLengths {
		docIDs = append(docIDs, docID)
	}
	sort.Slice(docIDs, func(i, j int) bool { return docIDs[i] < docIDs[j] })

	for _, docID := range docIDs {
		if err := binary.Write(&buf, binary.LittleEndian, docID); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, idx.DocLengths[docID]); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// Deserialize reads an index from serialized bytes.
func Deserialize(data []byte) (*Index, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("data too short")
	}

	r := bytes.NewReader(data)

	// Magic number
	magic := make([]byte, 4)
	if _, err := r.Read(magic); err != nil {
		return nil, fmt.Errorf("failed to read magic: %w", err)
	}
	if string(magic) != "BM25" {
		return nil, fmt.Errorf("invalid magic number: %s", magic)
	}

	// Version
	var version uint32
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	if version != 1 {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	// Metadata JSON
	var metaLen uint32
	if err := binary.Read(r, binary.LittleEndian, &metaLen); err != nil {
		return nil, fmt.Errorf("failed to read metadata length: %w", err)
	}

	metaBytes := make([]byte, metaLen)
	if _, err := r.Read(metaBytes); err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta struct {
		Attribute    string  `json:"attribute"`
		TotalDocs    uint32  `json:"total_docs"`
		AvgDocLength float64 `json:"avg_doc_length"`
		TotalTerms   uint32  `json:"total_terms"`
		Config       *Config `json:"config"`
	}
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	idx := &Index{
		Attribute:    meta.Attribute,
		Config:       meta.Config,
		TotalDocs:    meta.TotalDocs,
		AvgDocLength: meta.AvgDocLength,
		TotalTerms:   meta.TotalTerms,
		TermPostings: make(map[string]*roaring.Bitmap),
		TermFreqs:    make(map[string]map[uint32]uint32),
		DocLengths:   make(map[uint32]uint32),
	}

	// Number of terms
	var numTerms uint32
	if err := binary.Read(r, binary.LittleEndian, &numTerms); err != nil {
		return nil, fmt.Errorf("failed to read term count: %w", err)
	}

	// Read each term
	for i := uint32(0); i < numTerms; i++ {
		// Term string
		var termLen uint32
		if err := binary.Read(r, binary.LittleEndian, &termLen); err != nil {
			return nil, fmt.Errorf("failed to read term length: %w", err)
		}
		termBytes := make([]byte, termLen)
		if _, err := r.Read(termBytes); err != nil {
			return nil, fmt.Errorf("failed to read term: %w", err)
		}
		term := string(termBytes)

		// Posting list
		var postingLen uint32
		if err := binary.Read(r, binary.LittleEndian, &postingLen); err != nil {
			return nil, fmt.Errorf("failed to read posting length: %w", err)
		}
		postingBytes := make([]byte, postingLen)
		if _, err := r.Read(postingBytes); err != nil {
			return nil, fmt.Errorf("failed to read posting: %w", err)
		}
		bitmap := roaring.New()
		if err := bitmap.UnmarshalBinary(postingBytes); err != nil {
			return nil, fmt.Errorf("failed to unmarshal posting: %w", err)
		}
		idx.TermPostings[term] = bitmap

		// Term frequencies
		var numFreqs uint32
		if err := binary.Read(r, binary.LittleEndian, &numFreqs); err != nil {
			return nil, fmt.Errorf("failed to read freq count: %w", err)
		}
		freqs := make(map[uint32]uint32, numFreqs)
		for j := uint32(0); j < numFreqs; j++ {
			var docID, freq uint32
			if err := binary.Read(r, binary.LittleEndian, &docID); err != nil {
				return nil, fmt.Errorf("failed to read docID: %w", err)
			}
			if err := binary.Read(r, binary.LittleEndian, &freq); err != nil {
				return nil, fmt.Errorf("failed to read freq: %w", err)
			}
			freqs[docID] = freq
		}
		idx.TermFreqs[term] = freqs
	}

	// Document lengths
	var numDocs uint32
	if err := binary.Read(r, binary.LittleEndian, &numDocs); err != nil {
		return nil, fmt.Errorf("failed to read doc count: %w", err)
	}
	for i := uint32(0); i < numDocs; i++ {
		var docID, length uint32
		if err := binary.Read(r, binary.LittleEndian, &docID); err != nil {
			return nil, fmt.Errorf("failed to read docID: %w", err)
		}
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return nil, fmt.Errorf("failed to read length: %w", err)
		}
		idx.DocLengths[docID] = length
	}

	return idx, nil
}

// HasTerm returns true if the index contains the given term.
func (idx *Index) HasTerm(term string) bool {
	_, ok := idx.TermPostings[term]
	return ok
}

// GetPostingList returns the document IDs that contain the given term.
func (idx *Index) GetPostingList(term string) *roaring.Bitmap {
	if posting, ok := idx.TermPostings[term]; ok {
		return posting.Clone()
	}
	return roaring.New()
}

// GetTermFrequency returns the number of times a term appears in a document.
func (idx *Index) GetTermFrequency(term string, docID uint32) uint32 {
	if freqs, ok := idx.TermFreqs[term]; ok {
		return freqs[docID]
	}
	return 0
}

// GetDocLength returns the length (token count) of a document.
func (idx *Index) GetDocLength(docID uint32) uint32 {
	return idx.DocLengths[docID]
}

// GetDocumentFrequency returns the number of documents containing the term.
func (idx *Index) GetDocumentFrequency(term string) uint32 {
	if posting, ok := idx.TermPostings[term]; ok {
		return uint32(posting.GetCardinality())
	}
	return 0
}
