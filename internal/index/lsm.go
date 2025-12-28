// Package index implements LSM-like incremental indexing model.
package index

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/vexsearch/vex/pkg/objectstore"
)

const (
	// L0 is the first level where segments are created from WAL batches.
	L0 = 0
	// L1 is the first compacted level.
	L1 = 1
	// L2 is the second compacted level for larger segments.
	L2 = 2

	// DefaultL0CompactionThreshold is the number of L0 segments that triggers compaction.
	DefaultL0CompactionThreshold = 4

	// DefaultL1CompactionThreshold is the number of L1 segments that triggers L1->L2 compaction.
	DefaultL1CompactionThreshold = 4

	// DefaultL0TargetSizeBytes is the approximate target size for L0 segments (1MB).
	DefaultL0TargetSizeBytes = 1 * 1024 * 1024

	// DefaultL1TargetSizeBytes is the approximate target size for L1 segments (10MB).
	DefaultL1TargetSizeBytes = 10 * 1024 * 1024

	// DefaultL2TargetSizeBytes is the approximate target size for L2 segments (100MB).
	DefaultL2TargetSizeBytes = 100 * 1024 * 1024
)

var (
	ErrLSMTreeClosed         = errors.New("LSM tree is closed")
	ErrNoSegmentsToCompact   = errors.New("no segments available for compaction")
	ErrCompactionInProgress  = errors.New("compaction is already in progress")
	ErrSegmentNotFound       = errors.New("segment not found")
	ErrInvalidCompactionPlan = errors.New("invalid compaction plan")
)

// LSMConfig holds configuration for the LSM tree.
type LSMConfig struct {
	// L0CompactionThreshold is the number of L0 segments that triggers L0->L1 compaction.
	L0CompactionThreshold int

	// L1CompactionThreshold is the number of L1 segments that triggers L1->L2 compaction.
	L1CompactionThreshold int

	// L0TargetSizeBytes is the approximate target size for L0 segments.
	L0TargetSizeBytes int64

	// L1TargetSizeBytes is the approximate target size for L1 segments.
	L1TargetSizeBytes int64

	// L2TargetSizeBytes is the approximate target size for L2 segments.
	L2TargetSizeBytes int64
}

// DefaultLSMConfig returns the default LSM configuration.
func DefaultLSMConfig() *LSMConfig {
	return &LSMConfig{
		L0CompactionThreshold: DefaultL0CompactionThreshold,
		L1CompactionThreshold: DefaultL1CompactionThreshold,
		L0TargetSizeBytes:     DefaultL0TargetSizeBytes,
		L1TargetSizeBytes:     DefaultL1TargetSizeBytes,
		L2TargetSizeBytes:     DefaultL2TargetSizeBytes,
	}
}

// LSMTree manages the LSM-like segment structure for a namespace.
type LSMTree struct {
	mu        sync.RWMutex
	namespace string
	store     objectstore.Store
	config    *LSMConfig
	manifest  *Manifest

	// compacting tracks whether compaction is in progress
	compacting bool
	closed     bool
}

// NewLSMTree creates a new LSM tree for a namespace.
func NewLSMTree(namespace string, store objectstore.Store, config *LSMConfig) *LSMTree {
	if config == nil {
		config = DefaultLSMConfig()
	}
	return &LSMTree{
		namespace: namespace,
		store:     store,
		config:    config,
		manifest:  NewManifest(namespace),
	}
}

// LoadLSMTree loads an existing LSM tree from a manifest.
func LoadLSMTree(namespace string, store objectstore.Store, manifest *Manifest, config *LSMConfig) *LSMTree {
	if config == nil {
		config = DefaultLSMConfig()
	}
	tree := &LSMTree{
		namespace: namespace,
		store:     store,
		config:    config,
		manifest:  manifest,
	}
	return tree
}

// Namespace returns the namespace this tree belongs to.
func (t *LSMTree) Namespace() string {
	return t.namespace
}

// Manifest returns a copy of the current manifest.
func (t *LSMTree) Manifest() *Manifest {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.manifest.Clone()
}

// AddL0Segment adds a new L0 segment to the tree.
// This is called after building an L0 segment from WAL batches.
func (t *LSMTree) AddL0Segment(seg Segment) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrLSMTreeClosed
	}

	// Force level to 0 for L0 segments
	seg.Level = L0

	t.manifest.AddSegment(seg)
	t.manifest.UpdateIndexedWALSeq()
	t.manifest.GeneratedAt = time.Now().UTC()

	return nil
}

// GetL0Segments returns all L0 segments, sorted by end WAL seq (newest first).
func (t *LSMTree) GetL0Segments() []Segment {
	t.mu.RLock()
	defer t.mu.RUnlock()

	segments := t.manifest.GetSegmentsByLevel(L0)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].EndWALSeq > segments[j].EndWALSeq
	})
	return segments
}

// GetL1Segments returns all L1 segments, sorted by end WAL seq (newest first).
func (t *LSMTree) GetL1Segments() []Segment {
	t.mu.RLock()
	defer t.mu.RUnlock()

	segments := t.manifest.GetSegmentsByLevel(L1)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].EndWALSeq > segments[j].EndWALSeq
	})
	return segments
}

// GetL2Segments returns all L2 segments, sorted by end WAL seq (newest first).
func (t *LSMTree) GetL2Segments() []Segment {
	t.mu.RLock()
	defer t.mu.RUnlock()

	segments := t.manifest.GetSegmentsByLevel(L2)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].EndWALSeq > segments[j].EndWALSeq
	})
	return segments
}

// GetAllSegments returns all segments across all levels, sorted by end WAL seq (newest first).
// This is the order used for query execution to ensure newest data is processed first.
func (t *LSMTree) GetAllSegments() []Segment {
	t.mu.RLock()
	defer t.mu.RUnlock()

	segments := make([]Segment, len(t.manifest.Segments))
	copy(segments, t.manifest.Segments)

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].EndWALSeq > segments[j].EndWALSeq
	})

	return segments
}

// GetSegmentsByLevelRange returns all segments in levels [minLevel, maxLevel].
func (t *LSMTree) GetSegmentsByLevelRange(minLevel, maxLevel int) []Segment {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var segments []Segment
	for _, seg := range t.manifest.Segments {
		if seg.Level >= minLevel && seg.Level <= maxLevel {
			segments = append(segments, seg)
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].EndWALSeq > segments[j].EndWALSeq
	})

	return segments
}

// NeedsL0Compaction returns true if L0 has enough segments to trigger compaction.
func (t *LSMTree) NeedsL0Compaction() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.manifest.Stats.L0SegmentCount >= t.config.L0CompactionThreshold
}

// NeedsL1Compaction returns true if L1 has enough segments to trigger L1->L2 compaction.
func (t *LSMTree) NeedsL1Compaction() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	l1Count := 0
	for _, seg := range t.manifest.Segments {
		if seg.Level == L1 {
			l1Count++
		}
	}
	return l1Count >= t.config.L1CompactionThreshold
}

// CompactionPlan describes a planned compaction operation.
type CompactionPlan struct {
	// SourceSegments are the segments to be merged.
	SourceSegments []Segment

	// TargetLevel is the level for the output segment.
	TargetLevel int

	// MinWALSeq is the minimum WAL seq across source segments.
	MinWALSeq uint64

	// MaxWALSeq is the maximum WAL seq across source segments.
	MaxWALSeq uint64

	// TotalBytes is the total bytes across source segments.
	TotalBytes int64

	// TotalRows is the total rows across source segments.
	TotalRows int64
}

// PlanL0Compaction creates a plan to compact L0 segments into L1.
func (t *LSMTree) PlanL0Compaction() (*CompactionPlan, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return nil, ErrLSMTreeClosed
	}

	l0Segments := t.manifest.GetSegmentsByLevel(L0)
	if len(l0Segments) < t.config.L0CompactionThreshold {
		return nil, ErrNoSegmentsToCompact
	}

	// Sort by start WAL seq to get contiguous segments
	sort.Slice(l0Segments, func(i, j int) bool {
		return l0Segments[i].StartWALSeq < l0Segments[j].StartWALSeq
	})

	// Select segments to compact (all L0 segments)
	var minSeq, maxSeq uint64
	var totalBytes, totalRows int64
	for i, seg := range l0Segments {
		if i == 0 {
			minSeq = seg.StartWALSeq
		}
		if seg.EndWALSeq > maxSeq {
			maxSeq = seg.EndWALSeq
		}
		totalBytes += seg.Stats.LogicalBytes
		totalRows += seg.Stats.RowCount
	}

	return &CompactionPlan{
		SourceSegments: l0Segments,
		TargetLevel:    L1,
		MinWALSeq:      minSeq,
		MaxWALSeq:      maxSeq,
		TotalBytes:     totalBytes,
		TotalRows:      totalRows,
	}, nil
}

// PlanL1Compaction creates a plan to compact L1 segments into L2.
func (t *LSMTree) PlanL1Compaction() (*CompactionPlan, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return nil, ErrLSMTreeClosed
	}

	l1Segments := t.manifest.GetSegmentsByLevel(L1)
	if len(l1Segments) < t.config.L1CompactionThreshold {
		return nil, ErrNoSegmentsToCompact
	}

	// Sort by start WAL seq
	sort.Slice(l1Segments, func(i, j int) bool {
		return l1Segments[i].StartWALSeq < l1Segments[j].StartWALSeq
	})

	var minSeq, maxSeq uint64
	var totalBytes, totalRows int64
	for i, seg := range l1Segments {
		if i == 0 {
			minSeq = seg.StartWALSeq
		}
		if seg.EndWALSeq > maxSeq {
			maxSeq = seg.EndWALSeq
		}
		totalBytes += seg.Stats.LogicalBytes
		totalRows += seg.Stats.RowCount
	}

	return &CompactionPlan{
		SourceSegments: l1Segments,
		TargetLevel:    L2,
		MinWALSeq:      minSeq,
		MaxWALSeq:      maxSeq,
		TotalBytes:     totalBytes,
		TotalRows:      totalRows,
	}, nil
}

// ApplyCompaction applies a compaction result, replacing source segments with the new segment.
func (t *LSMTree) ApplyCompaction(plan *CompactionPlan, newSegment Segment) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrLSMTreeClosed
	}

	// Remove source segments
	for _, src := range plan.SourceSegments {
		t.manifest.RemoveSegment(src.ID)
	}

	// Add new segment at target level
	newSegment.Level = plan.TargetLevel
	t.manifest.AddSegment(newSegment)
	t.manifest.UpdateIndexedWALSeq()
	t.manifest.GeneratedAt = time.Now().UTC()

	return nil
}

// SetCompacting marks compaction as in progress or complete.
func (t *LSMTree) SetCompacting(inProgress bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrLSMTreeClosed
	}

	if inProgress && t.compacting {
		return ErrCompactionInProgress
	}

	t.compacting = inProgress
	return nil
}

// IsCompacting returns true if compaction is in progress.
func (t *LSMTree) IsCompacting() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.compacting
}

// Stats returns the current LSM tree statistics.
func (t *LSMTree) Stats() LSMStats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var l0, l1, l2 int
	var l0Bytes, l1Bytes, l2Bytes int64

	for _, seg := range t.manifest.Segments {
		switch seg.Level {
		case L0:
			l0++
			l0Bytes += seg.Stats.LogicalBytes
		case L1:
			l1++
			l1Bytes += seg.Stats.LogicalBytes
		case L2:
			l2++
			l2Bytes += seg.Stats.LogicalBytes
		}
	}

	return LSMStats{
		L0SegmentCount: l0,
		L1SegmentCount: l1,
		L2SegmentCount: l2,
		L0Bytes:        l0Bytes,
		L1Bytes:        l1Bytes,
		L2Bytes:        l2Bytes,
		TotalSegments:  t.manifest.Stats.SegmentCount,
		TotalRows:      t.manifest.Stats.ApproxRowCount,
		TotalBytes:     t.manifest.Stats.ApproxLogicalBytes,
		IndexedWALSeq:  t.manifest.IndexedWALSeq,
	}
}

// LSMStats contains statistics about the LSM tree.
type LSMStats struct {
	L0SegmentCount int
	L1SegmentCount int
	L2SegmentCount int
	L0Bytes        int64
	L1Bytes        int64
	L2Bytes        int64
	TotalSegments  int
	TotalRows      int64
	TotalBytes     int64
	IndexedWALSeq  uint64
}

// Close closes the LSM tree.
func (t *LSMTree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

// IsClosed returns true if the tree is closed.
func (t *LSMTree) IsClosed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.closed
}

// IndexedWALSeq returns the highest WAL sequence that has been indexed.
func (t *LSMTree) IndexedWALSeq() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.manifest.IndexedWALSeq
}

// L0SegmentBuilder builds L0 segments from WAL data.
type L0SegmentBuilder struct {
	namespace   string
	store       objectstore.Store
	targetBytes int64

	mu       sync.Mutex
	docs     []DocumentEntry
	docBytes int64
}

// DocumentEntry represents a document to be indexed.
type DocumentEntry struct {
	ID         string
	WALSeq     uint64
	Deleted    bool
	Attributes map[string]any
	Vector     []float32
}

// NewL0SegmentBuilder creates a builder for L0 segments.
func NewL0SegmentBuilder(namespace string, store objectstore.Store, targetBytes int64) *L0SegmentBuilder {
	if targetBytes <= 0 {
		targetBytes = DefaultL0TargetSizeBytes
	}
	return &L0SegmentBuilder{
		namespace:   namespace,
		store:       store,
		targetBytes: targetBytes,
	}
}

// AddDocument adds a document to the current L0 segment.
func (b *L0SegmentBuilder) AddDocument(doc DocumentEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.docs = append(b.docs, doc)
	// Approximate size: ID + attributes
	b.docBytes += int64(len(doc.ID)) + int64(len(doc.Attributes)*50)
	if doc.Vector != nil {
		b.docBytes += int64(len(doc.Vector) * 4)
	}
}

// ShouldFlush returns true if the builder has enough data to create a segment.
func (b *L0SegmentBuilder) ShouldFlush() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.docBytes >= b.targetBytes
}

// DocumentCount returns the number of documents in the builder.
func (b *L0SegmentBuilder) DocumentCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.docs)
}

// Build creates an L0 segment from the accumulated documents.
// Returns the segment and its data, ready for upload.
func (b *L0SegmentBuilder) Build(ctx context.Context) (*Segment, *L0SegmentData, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.docs) == 0 {
		return nil, nil, nil
	}

	// Determine WAL range
	var minSeq, maxSeq uint64
	for i, doc := range b.docs {
		if i == 0 {
			minSeq = doc.WALSeq
			maxSeq = doc.WALSeq
		}
		if doc.WALSeq < minSeq {
			minSeq = doc.WALSeq
		}
		if doc.WALSeq > maxSeq {
			maxSeq = doc.WALSeq
		}
	}

	// Count non-deleted docs
	liveCount := int64(0)
	tombstoneCount := int64(0)
	for _, doc := range b.docs {
		if doc.Deleted {
			tombstoneCount++
		} else {
			liveCount++
		}
	}

	segID := GenerateSegmentID()
	seg := &Segment{
		ID:          segID,
		Level:       L0,
		StartWALSeq: minSeq,
		EndWALSeq:   maxSeq,
		CreatedAt:   time.Now().UTC(),
		Stats: SegmentStats{
			RowCount:       liveCount,
			LogicalBytes:   b.docBytes,
			TombstoneCount: tombstoneCount,
		},
	}

	data := &L0SegmentData{
		SegmentID: segID,
		Namespace: b.namespace,
		Documents: make([]DocumentEntry, len(b.docs)),
	}
	copy(data.Documents, b.docs)

	return seg, data, nil
}

// Reset clears the builder for reuse.
func (b *L0SegmentBuilder) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.docs = nil
	b.docBytes = 0
}

// L0SegmentData contains the data for an L0 segment ready for upload.
type L0SegmentData struct {
	SegmentID string
	Namespace string
	Documents []DocumentEntry
}

// Compactor handles segment compaction operations.
type Compactor struct {
	store  objectstore.Store
	config *LSMConfig
}

// NewCompactor creates a new segment compactor.
func NewCompactor(store objectstore.Store, config *LSMConfig) *Compactor {
	if config == nil {
		config = DefaultLSMConfig()
	}
	return &Compactor{
		store:  store,
		config: config,
	}
}

// CompactSegments merges multiple segments into a single segment.
// The output segment will be at the specified target level.
// This is a placeholder that returns a merged segment structure.
// Full implementation would read segment data and merge documents.
func (c *Compactor) CompactSegments(ctx context.Context, plan *CompactionPlan) (*Segment, error) {
	if len(plan.SourceSegments) == 0 {
		return nil, ErrInvalidCompactionPlan
	}

	// Create the merged segment
	segID := GenerateSegmentID()
	merged := &Segment{
		ID:          segID,
		Level:       plan.TargetLevel,
		StartWALSeq: plan.MinWALSeq,
		EndWALSeq:   plan.MaxWALSeq,
		CreatedAt:   time.Now().UTC(),
		Stats: SegmentStats{
			RowCount:     plan.TotalRows,
			LogicalBytes: plan.TotalBytes,
		},
	}

	// In a full implementation, we would:
	// 1. Read all documents from source segments
	// 2. Merge by ID, keeping newest version (highest WAL seq)
	// 3. Apply tombstones (remove deleted docs)
	// 4. Write merged data to new segment files
	// 5. Upload segment files to object storage

	return merged, nil
}

// MergeResult contains the result of a merge operation.
type MergeResult struct {
	Segment      *Segment
	DocsKey      string
	VectorsKey   string
	FilterKeys   []string
	FTSKeys      []string
	MergedDocs   int64
	RemovedDocs  int64
	TotalBytes   int64
}

// SegmentReader reads documents from segments for query execution.
type SegmentReader struct {
	store     objectstore.Store
	namespace string
}

// NewSegmentReader creates a reader for reading segment data.
func NewSegmentReader(store objectstore.Store, namespace string) *SegmentReader {
	return &SegmentReader{
		store:     store,
		namespace: namespace,
	}
}

// ReadableSegment represents a segment that can be read for queries.
type ReadableSegment struct {
	Segment   Segment
	Documents []DocumentEntry
}

// ScanAllSegments scans all segments in the tree and returns document entries.
// Documents are returned in WAL sequence order (newest first) for deduplication.
// The caller is responsible for deduplication using the document ID.
func (r *SegmentReader) ScanAllSegments(ctx context.Context, segments []Segment) ([]DocumentEntry, error) {
	if len(segments) == 0 {
		return nil, nil
	}

	// Sort segments by end WAL seq (newest first) for deduplication
	sortedSegs := make([]Segment, len(segments))
	copy(sortedSegs, segments)
	sort.Slice(sortedSegs, func(i, j int) bool {
		return sortedSegs[i].EndWALSeq > sortedSegs[j].EndWALSeq
	})

	var allDocs []DocumentEntry

	// In a full implementation, we would:
	// 1. Read docs.col.zst from each segment
	// 2. Decompress and decode documents
	// 3. Return in WAL seq order

	return allDocs, nil
}

// DeduplicateDocuments deduplicates documents by ID, keeping the one with highest WAL seq.
// The input should already be sorted by WAL seq (newest first).
func DeduplicateDocuments(docs []DocumentEntry) []DocumentEntry {
	seen := make(map[string]bool)
	var result []DocumentEntry

	for _, doc := range docs {
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

// QuerySegmentIterator iterates over segments and tail for query execution.
type QuerySegmentIterator struct {
	segments    []Segment
	tailDocs    []DocumentEntry
	tailIndex   int
	segmentIdx  int
	seenIDs     map[string]bool
	namespace   string
	reader      *SegmentReader
}

// NewQuerySegmentIterator creates an iterator for query execution.
// Segments should be sorted by end WAL seq (newest first).
// Tail docs should also be sorted by WAL seq (newest first).
func NewQuerySegmentIterator(reader *SegmentReader, segments []Segment, tailDocs []DocumentEntry) *QuerySegmentIterator {
	return &QuerySegmentIterator{
		segments:  segments,
		tailDocs:  tailDocs,
		seenIDs:   make(map[string]bool),
		reader:    reader,
	}
}

// Next returns the next unique document, or nil if exhausted.
// Documents are returned in newest-first order, with deduplication.
func (it *QuerySegmentIterator) Next(ctx context.Context) (*DocumentEntry, error) {
	// First, iterate through tail documents (newest)
	for it.tailIndex < len(it.tailDocs) {
		doc := it.tailDocs[it.tailIndex]
		it.tailIndex++
		if it.seenIDs[doc.ID] {
			continue
		}
		it.seenIDs[doc.ID] = true
		if doc.Deleted {
			continue
		}
		return &doc, nil
	}
	// Clear tail docs to avoid re-iteration
	it.tailDocs = nil

	// Then iterate through segments
	// In a full implementation, this would read from segment files
	return nil, nil
}

// LevelInfo contains information about a single LSM level.
type LevelInfo struct {
	Level        int
	SegmentCount int
	TotalBytes   int64
	TotalRows    int64
	MinWALSeq    uint64
	MaxWALSeq    uint64
}

// GetLevelInfo returns information about each level in the tree.
func (t *LSMTree) GetLevelInfo() []LevelInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	levelMap := make(map[int]*LevelInfo)

	for _, seg := range t.manifest.Segments {
		info, exists := levelMap[seg.Level]
		if !exists {
			info = &LevelInfo{Level: seg.Level}
			levelMap[seg.Level] = info
		}

		info.SegmentCount++
		info.TotalBytes += seg.Stats.LogicalBytes
		info.TotalRows += seg.Stats.RowCount

		if info.MinWALSeq == 0 || seg.StartWALSeq < info.MinWALSeq {
			info.MinWALSeq = seg.StartWALSeq
		}
		if seg.EndWALSeq > info.MaxWALSeq {
			info.MaxWALSeq = seg.EndWALSeq
		}
	}

	// Convert map to sorted slice
	var result []LevelInfo
	for level := L0; level <= L2; level++ {
		if info, exists := levelMap[level]; exists {
			result = append(result, *info)
		}
	}

	return result
}

// TargetSizeForLevel returns the target size in bytes for segments at the given level.
func (c *LSMConfig) TargetSizeForLevel(level int) int64 {
	switch level {
	case L0:
		return c.L0TargetSizeBytes
	case L1:
		return c.L1TargetSizeBytes
	case L2:
		return c.L2TargetSizeBytes
	default:
		return c.L2TargetSizeBytes
	}
}

// CompactionTriggerForLevel returns the compaction threshold for the given level.
func (c *LSMConfig) CompactionTriggerForLevel(level int) int {
	switch level {
	case L0:
		return c.L0CompactionThreshold
	case L1:
		return c.L1CompactionThreshold
	default:
		return 0 // No compaction trigger for L2
	}
}

// EstimateCompactionOutputSize estimates the output size of a compaction.
// This accounts for deduplication and tombstone removal.
func EstimateCompactionOutputSize(plan *CompactionPlan) int64 {
	// Simple estimation: assume 80% of input size after deduplication
	return int64(float64(plan.TotalBytes) * 0.8)
}

// ObsoleteSegmentIDs returns the IDs of segments that would become obsolete after compaction.
func (plan *CompactionPlan) ObsoleteSegmentIDs() []string {
	ids := make([]string, len(plan.SourceSegments))
	for i, seg := range plan.SourceSegments {
		ids[i] = seg.ID
	}
	return ids
}

// Validate validates the compaction plan.
func (plan *CompactionPlan) Validate() error {
	if len(plan.SourceSegments) == 0 {
		return fmt.Errorf("%w: no source segments", ErrInvalidCompactionPlan)
	}
	if plan.TargetLevel < L1 {
		return fmt.Errorf("%w: target level must be >= L1", ErrInvalidCompactionPlan)
	}
	if plan.MinWALSeq > plan.MaxWALSeq {
		return fmt.Errorf("%w: min WAL seq > max WAL seq", ErrInvalidCompactionPlan)
	}
	return nil
}
