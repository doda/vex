// Package query provides deduplication utilities for query execution.
package query

import (
	"sort"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/tail"
)

// DeduplicatedDoc represents a document with version information for deduplication.
// When multiple versions of the same document exist across segments and tail,
// the version with the highest WAL sequence number is authoritative.
type DeduplicatedDoc struct {
	ID         document.ID
	WalSeq     uint64
	SubBatchID int
	Deleted    bool
	Attributes map[string]any
	Vector     []float32
}

// DocVersion contains version information for deduplication.
type DocVersion struct {
	WalSeq     uint64
	SubBatchID int
	Deleted    bool
}

// IsNewerThan returns true if this version is newer than the other version.
// Comparison is based on WAL sequence (higher is newer), with sub-batch ID
// as a tiebreaker for documents within the same WAL sequence.
func (v DocVersion) IsNewerThan(other DocVersion) bool {
	if v.WalSeq != other.WalSeq {
		return v.WalSeq > other.WalSeq
	}
	return v.SubBatchID > other.SubBatchID
}

// DeduplicationResult holds the result of deduplication across sources.
type DeduplicationResult struct {
	// Docs contains the deduplicated documents, newest version only.
	Docs []*DeduplicatedDoc

	// TotalVersionsSeen is the total number of document versions processed.
	TotalVersionsSeen int

	// TotalDeduped is the number of duplicate versions eliminated.
	TotalDeduped int

	// TotalTombstones is the number of documents excluded due to tombstones.
	TotalTombstones int
}

// Deduplicator handles document deduplication across segments and tail.
// It ensures that:
// 1. The highest WAL sequence version is authoritative
// 2. Tombstones (deletes) at the highest sequence exclude the document
// 3. Newest sources are processed first (tail, then segments by end WAL seq)
type Deduplicator struct {
	// seenVersions maps document ID string to the newest version seen.
	seenVersions map[string]*DeduplicatedDoc
	// versionsSeen counts all document versions processed (for metrics).
	versionsSeen int
}

// NewDeduplicator creates a new deduplicator.
func NewDeduplicator() *Deduplicator {
	return &Deduplicator{
		seenVersions: make(map[string]*DeduplicatedDoc),
	}
}

// AddTailDoc adds a document from the tail store.
// Tail documents always have version information from WAL seq and sub-batch ID.
func (d *Deduplicator) AddTailDoc(doc *tail.Document) {
	if doc == nil {
		return
	}

	d.versionsSeen++

	idStr := doc.ID.String()
	existing, exists := d.seenVersions[idStr]

	newVersion := DocVersion{
		WalSeq:     doc.WalSeq,
		SubBatchID: doc.SubBatchID,
		Deleted:    doc.Deleted,
	}

	if !exists || newVersion.IsNewerThan(DocVersion{WalSeq: existing.WalSeq, SubBatchID: existing.SubBatchID}) {
		d.seenVersions[idStr] = &DeduplicatedDoc{
			ID:         doc.ID,
			WalSeq:     doc.WalSeq,
			SubBatchID: doc.SubBatchID,
			Deleted:    doc.Deleted,
			Attributes: doc.Attributes,
			Vector:     doc.Vector,
		}
	}
}

// AddSegmentDoc adds a document from an index segment.
// Returns an error if the document ID cannot be parsed.
func (d *Deduplicator) AddSegmentDoc(entry *index.DocumentEntry) error {
	if entry == nil {
		return nil
	}

	// Parse the ID first to validate it
	id, err := document.ParseID(entry.ID)
	if err != nil {
		return err
	}

	d.versionsSeen++

	idStr := entry.ID
	existing, exists := d.seenVersions[idStr]

	newVersion := DocVersion{
		WalSeq:  entry.WALSeq,
		Deleted: entry.Deleted,
	}

	if !exists || newVersion.IsNewerThan(DocVersion{WalSeq: existing.WalSeq, SubBatchID: existing.SubBatchID}) {
		d.seenVersions[idStr] = &DeduplicatedDoc{
			ID:         id,
			WalSeq:     entry.WALSeq,
			SubBatchID: 0, // Segment docs don't have sub-batch ID
			Deleted:    entry.Deleted,
			Attributes: entry.Attributes,
			Vector:     entry.Vector,
		}
	}
	return nil
}

// AddVectorResult adds a vector search result from segment IVF index.
// These results don't have full document data, only docID and distance.
func (d *Deduplicator) AddVectorResult(docID uint64, walSeq uint64) {
	d.versionsSeen++

	idStr := document.NewU64ID(docID).String()
	_, exists := d.seenVersions[idStr]

	// For vector results, we only add if not already seen (tail is authoritative)
	if !exists {
		d.seenVersions[idStr] = &DeduplicatedDoc{
			ID:     document.NewU64ID(docID),
			WalSeq: walSeq,
		}
	}
}

// HasDoc returns true if the document ID has already been seen.
func (d *Deduplicator) HasDoc(id string) bool {
	_, exists := d.seenVersions[id]
	return exists
}

// HasDocID returns true if the document ID has already been seen.
func (d *Deduplicator) HasDocID(id document.ID) bool {
	return d.HasDoc(id.String())
}

// GetDoc returns the document for the given ID, or nil if not found.
func (d *Deduplicator) GetDoc(id string) *DeduplicatedDoc {
	return d.seenVersions[id]
}

// Results returns the deduplicated results.
// Only non-deleted documents are returned.
// Results are sorted by WAL sequence (newest first).
func (d *Deduplicator) Results() *DeduplicationResult {
	result := &DeduplicationResult{
		Docs:              make([]*DeduplicatedDoc, 0, len(d.seenVersions)),
		TotalVersionsSeen: d.versionsSeen,
	}

	for _, doc := range d.seenVersions {
		if doc.Deleted {
			result.TotalTombstones++
			continue
		}
		result.Docs = append(result.Docs, doc)
	}

	// Sort by WAL seq descending (newest first)
	sort.Slice(result.Docs, func(i, j int) bool {
		if result.Docs[i].WalSeq != result.Docs[j].WalSeq {
			return result.Docs[i].WalSeq > result.Docs[j].WalSeq
		}
		return result.Docs[i].SubBatchID > result.Docs[j].SubBatchID
	})

	result.TotalDeduped = result.TotalVersionsSeen - len(result.Docs) - result.TotalTombstones

	return result
}

// AllDocs returns all documents including deleted ones.
// Used for specialized queries that need to see tombstones.
func (d *Deduplicator) AllDocs() []*DeduplicatedDoc {
	docs := make([]*DeduplicatedDoc, 0, len(d.seenVersions))
	for _, doc := range d.seenVersions {
		docs = append(docs, doc)
	}

	// Sort by WAL seq descending
	sort.Slice(docs, func(i, j int) bool {
		if docs[i].WalSeq != docs[j].WalSeq {
			return docs[i].WalSeq > docs[j].WalSeq
		}
		return docs[i].SubBatchID > docs[j].SubBatchID
	})

	return docs
}

// DeduplicateTailDocs deduplicates a slice of tail documents.
// This is useful for processing tail documents in newest-first order.
// Documents are sorted by WAL seq descending before deduplication.
func DeduplicateTailDocs(docs []*tail.Document) []*tail.Document {
	if len(docs) == 0 {
		return docs
	}

	// Sort by WAL seq descending (newest first)
	sorted := make([]*tail.Document, len(docs))
	copy(sorted, docs)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].WalSeq != sorted[j].WalSeq {
			return sorted[i].WalSeq > sorted[j].WalSeq
		}
		return sorted[i].SubBatchID > sorted[j].SubBatchID
	})

	seen := make(map[string]bool)
	result := make([]*tail.Document, 0, len(sorted))

	for _, doc := range sorted {
		idStr := doc.ID.String()
		if seen[idStr] {
			continue
		}
		seen[idStr] = true

		// Skip deleted documents
		if doc.Deleted {
			continue
		}

		result = append(result, doc)
	}

	return result
}

// DeduplicateSegmentDocs deduplicates segment document entries.
// Documents are sorted by WAL seq descending before deduplication.
// This applies the last-write-wins rule: highest WAL seq is authoritative.
func DeduplicateSegmentDocs(docs []index.DocumentEntry) []index.DocumentEntry {
	if len(docs) == 0 {
		return docs
	}

	// Sort by WAL seq descending (newest first)
	sorted := make([]index.DocumentEntry, len(docs))
	copy(sorted, docs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].WALSeq > sorted[j].WALSeq
	})

	seen := make(map[string]bool)
	result := make([]index.DocumentEntry, 0, len(sorted))

	for _, doc := range sorted {
		if seen[doc.ID] {
			continue
		}
		seen[doc.ID] = true

		// Skip tombstones
		if doc.Deleted {
			continue
		}

		result = append(result, doc)
	}

	return result
}

// MergeAndDeduplicate merges documents from segments and tail with deduplication.
// Tail documents take precedence over segment documents for the same ID
// when they have a higher WAL sequence number.
//
// Process order (for efficiency, newest first):
// 1. Tail documents (always newest)
// 2. Index segments sorted by end WAL seq descending
//
// For each document ID, only the version with highest WAL seq is kept.
// Tombstones (deleted=true) at the highest seq exclude the document.
// Segment documents with invalid IDs are skipped.
func MergeAndDeduplicate(tailDocs []*tail.Document, segmentDocs []index.DocumentEntry) []*DeduplicatedDoc {
	dedup := NewDeduplicator()

	// Add tail documents first (they are typically newer)
	for _, doc := range tailDocs {
		dedup.AddTailDoc(doc)
	}

	// Add segment documents (skip any with invalid IDs)
	for i := range segmentDocs {
		_ = dedup.AddSegmentDoc(&segmentDocs[i])
	}

	return dedup.Results().Docs
}

// IsDocIDInSet checks if a document ID is in the given set (used for deduplication).
func IsDocIDInSet(id document.ID, idSet map[string]bool) bool {
	return idSet[id.String()]
}

// BuildDocIDSet builds a set of document ID strings from tail documents.
func BuildDocIDSet(docs []*tail.Document) map[string]bool {
	set := make(map[string]bool, len(docs))
	for _, doc := range docs {
		set[doc.ID.String()] = true
	}
	return set
}

// BuildDocIDSetFromDedup builds a set of document ID strings from deduplicated docs.
func BuildDocIDSetFromDedup(docs []*DeduplicatedDoc) map[string]bool {
	set := make(map[string]bool, len(docs))
	for _, doc := range docs {
		set[doc.ID.String()] = true
	}
	return set
}
