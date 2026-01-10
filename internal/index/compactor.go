// Package index implements segment compaction for the LSM-like index.
package index

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/metrics"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var ErrCompactionStale = errors.New("compaction is stale")

// CompactorConfig holds configuration for segment compaction.
type CompactorConfig struct {
	// Recluster controls whether to rebuild IVF centroids during compaction.
	// If false, cluster assignments are preserved from source segments.
	Recluster bool

	// NClusters is the target number of clusters for reclustering.
	// If 0, auto-calculates based on vector count (sqrt(n), max 256).
	NClusters int

	// Metric is the distance metric for IVF index.
	Metric vector.DistanceMetric

	// MaxConcurrentCompactions limits parallel compaction operations.
	MaxConcurrentCompactions int

	// DisableChecksums disables checksum verification when writing segments.
	// This is primarily used for testing with in-memory stores.
	DisableChecksums bool

	// RetentionTime is the minimum age for obsolete segments before cleanup.
	// A zero value falls back to the default retention window.
	RetentionTime time.Duration
}

// DefaultCompactorConfig returns sensible defaults.
func DefaultCompactorConfig() *CompactorConfig {
	return &CompactorConfig{
		Recluster:                false, // Preserve cluster assignments by default
		NClusters:                0,     // Auto-calculate
		Metric:                   vector.MetricCosineDistance,
		MaxConcurrentCompactions: 1, // Single compaction at a time
		RetentionTime:            24 * time.Hour,
	}
}

func normalizeCompactorConfig(config *CompactorConfig) *CompactorConfig {
	if config == nil {
		return DefaultCompactorConfig()
	}
	if config.MaxConcurrentCompactions <= 0 {
		config.MaxConcurrentCompactions = 1
	}
	if config.Metric == "" {
		config.Metric = vector.MetricCosineDistance
	}
	if config.RetentionTime == 0 {
		config.RetentionTime = DefaultCompactorConfig().RetentionTime
	}
	return config
}

// FullCompactor performs actual segment merging with document deduplication.
type FullCompactor struct {
	store  objectstore.Store
	config *CompactorConfig

	mu        sync.Mutex
	active    int // Currently running compactions
	namespace string
}

// NewFullCompactor creates a compactor that performs real segment merging.
func NewFullCompactor(store objectstore.Store, namespace string, config *CompactorConfig) *FullCompactor {
	config = normalizeCompactorConfig(config)
	return &FullCompactor{
		store:     store,
		namespace: namespace,
		config:    config,
	}
}

// CompactionResult contains the output of a compaction operation.
type CompactionResult struct {
	// NewSegment is the merged output segment.
	NewSegment *Segment

	// MergedDocs is the number of documents after deduplication.
	MergedDocs int64

	// RemovedDocs is the number of documents removed (tombstones + dupes).
	RemovedDocs int64

	// Reclustered indicates if vectors were reclustered.
	Reclustered bool

	// ObjectsWritten lists all object keys uploaded.
	ObjectsWritten []string

	// BytesWritten is total bytes uploaded.
	BytesWritten int64

	// Duration is how long compaction took.
	Duration time.Duration
}

// Compact merges source segments into a single target segment.
// Returns the new segment with all data uploaded to object storage.
func (c *FullCompactor) Compact(ctx context.Context, plan *CompactionPlan) (*CompactionResult, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.active >= c.config.MaxConcurrentCompactions {
		c.mu.Unlock()
		return nil, ErrCompactionInProgress
	}
	c.active++
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()

	start := time.Now()

	// Step 1: Read all documents from source segments
	allDocs, err := c.readSegmentDocuments(ctx, plan.SourceSegments)
	if err != nil {
		return nil, fmt.Errorf("failed to read segment documents: %w", err)
	}

	// Step 2: Merge documents with deduplication (newest version wins)
	mergedDocs := c.mergeDocuments(allDocs)

	// Step 3: Build new segment with merged data
	result, err := c.buildMergedSegment(ctx, plan, mergedDocs)
	if err != nil {
		return nil, fmt.Errorf("failed to build merged segment: %w", err)
	}

	result.RemovedDocs = int64(len(allDocs)) - result.MergedDocs
	result.Duration = time.Since(start)

	return result, nil
}

// MergedDocument represents a document after merging/deduplication.
type MergedDocument = DocColumn

type docWinner struct {
	walSeq     uint64
	segIndex   int
	rowID      uint32
	deleted    bool
	fromColumn bool
}

type legacyDoc struct {
	doc      MergedDocument
	segIndex int
}

// readSegmentDocuments reads documents from all source segments.
// This uses a two-pass approach for columnar docs to avoid loading full rows into memory.
func (c *FullCompactor) readSegmentDocuments(ctx context.Context, segments []Segment) ([]MergedDocument, error) {
	winners, legacyDocs, err := c.collectCompactionWinners(ctx, segments)
	if err != nil {
		return nil, err
	}

	rowIDSets := make([]map[uint32]struct{}, len(segments))
	for _, winner := range winners {
		if winner.deleted || !winner.fromColumn {
			continue
		}
		if winner.segIndex < 0 || winner.segIndex >= len(segments) {
			continue
		}
		if rowIDSets[winner.segIndex] == nil {
			rowIDSets[winner.segIndex] = make(map[uint32]struct{})
		}
		rowIDSets[winner.segIndex][winner.rowID] = struct{}{}
	}

	var allDocs []MergedDocument
	for segIndex, rowIDs := range rowIDSets {
		if len(rowIDs) == 0 {
			continue
		}
		seg := segments[segIndex]
		docs, err := c.readDocsForRowIDs(ctx, seg, rowIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to read segment %s: %w", seg.ID, err)
		}
		allDocs = append(allDocs, docs...)
	}

	for _, doc := range legacyDocs {
		winner, ok := winners[doc.doc.ID]
		if !ok || winner.deleted || winner.fromColumn {
			continue
		}
		if winner.segIndex != doc.segIndex || winner.walSeq != doc.doc.WALSeq {
			continue
		}
		allDocs = append(allDocs, doc.doc)
	}

	return allDocs, nil
}

func (c *FullCompactor) collectCompactionWinners(ctx context.Context, segments []Segment) (map[string]docWinner, []legacyDoc, error) {
	winners := make(map[string]docWinner)
	var legacyDocs []legacyDoc

	for segIndex, seg := range segments {
		reader, ok, err := c.openDocsColumnReader(ctx, seg)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open segment %s docs: %w", seg.ID, err)
		}
		if ok {
			err := c.scanDocsColumnWinners(reader, segIndex, winners)
			reader.Close()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan segment %s docs: %w", seg.ID, err)
			}
			continue
		}

		docs, err := c.readSegmentDocs(ctx, seg)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read segment %s: %w", seg.ID, err)
		}
		for _, doc := range docs {
			c.applyWinner(winners, doc.ID, docWinner{
				walSeq:     doc.WALSeq,
				segIndex:   segIndex,
				deleted:    doc.Deleted,
				fromColumn: false,
			})
			legacyDocs = append(legacyDocs, legacyDoc{doc: doc, segIndex: segIndex})
		}
	}

	return winners, legacyDocs, nil
}

func (c *FullCompactor) applyWinner(winners map[string]docWinner, id string, candidate docWinner) {
	existing, ok := winners[id]
	if !ok || candidate.walSeq > existing.walSeq {
		winners[id] = candidate
	}
}

func (c *FullCompactor) scanDocsColumnWinners(r io.Reader, segIndex int, winners map[string]docWinner) error {
	docCount, _, err := readDocsColumnHeader(r)
	if err != nil {
		return err
	}
	if docCount == 0 {
		return nil
	}

	numericIDs := make([]uint64, docCount)
	if err := binary.Read(r, binary.LittleEndian, numericIDs); err != nil {
		return fmt.Errorf("failed to read numeric IDs: %w", err)
	}

	walSeqs := make([]uint64, docCount)
	if err := binary.Read(r, binary.LittleEndian, walSeqs); err != nil {
		return fmt.Errorf("failed to read WAL sequences: %w", err)
	}

	deletedFlags := make([]byte, docCount)
	if _, err := io.ReadFull(r, deletedFlags); err != nil {
		return fmt.Errorf("failed to read deleted flags: %w", err)
	}

	for i := 0; i < docCount; i++ {
		length, err := readUint32(r)
		if err != nil {
			return fmt.Errorf("failed to read ID length: %w", err)
		}
		var id string
		if length > 0 {
			idBytes := make([]byte, length)
			if _, err := io.ReadFull(r, idBytes); err != nil {
				return fmt.Errorf("failed to read ID bytes: %w", err)
			}
			id = string(idBytes)
		}
		c.applyWinner(winners, id, docWinner{
			walSeq:     walSeqs[i],
			segIndex:   segIndex,
			rowID:      uint32(i),
			deleted:    deletedFlags[i] == 1,
			fromColumn: true,
		})
	}

	return nil
}

func (c *FullCompactor) readDocsForRowIDs(ctx context.Context, seg Segment, rowIDs map[uint32]struct{}) ([]MergedDocument, error) {
	if len(rowIDs) == 0 {
		return nil, nil
	}

	reader, ok, err := c.openDocsColumnReader(ctx, seg)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("segment %s missing columnar docs", seg.ID)
	}
	defer reader.Close()

	docMap, err := DecodeDocsColumnForRowIDs(reader, rowIDs)
	if err != nil {
		return nil, err
	}

	docs := make([]MergedDocument, 0, len(docMap))
	for _, doc := range docMap {
		docs = append(docs, doc)
	}
	return docs, nil
}

type multiReadCloser struct {
	reader   io.Reader
	closeFns []func() error
}

func (m *multiReadCloser) Read(p []byte) (int, error) {
	return m.reader.Read(p)
}

func (m *multiReadCloser) Close() error {
	var firstErr error
	for _, fn := range m.closeFns {
		if err := fn(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *FullCompactor) openDocsColumnReader(ctx context.Context, seg Segment) (io.ReadCloser, bool, error) {
	if seg.DocsKey == "" {
		return nil, false, nil
	}

	reader, _, err := c.store.Get(ctx, seg.DocsKey, nil)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	buf := bufio.NewReader(reader)
	header, err := buf.Peek(len(zstdMagic))
	if err != nil {
		reader.Close()
		return nil, false, fmt.Errorf("failed to peek docs header: %w", err)
	}

	if bytes.Equal(header, zstdMagic[:]) {
		dec, err := zstd.NewReader(buf,
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(32*1024*1024),
			zstd.WithDecoderConcurrency(1),
		)
		if err != nil {
			reader.Close()
			return nil, false, fmt.Errorf("failed to init docs decompressor: %w", err)
		}
		return &multiReadCloser{reader: dec, closeFns: []func() error{
			func() error { dec.Close(); return nil },
			reader.Close,
		}}, true, nil
	}

	if bytes.Equal(header, docsColumnMagic[:]) {
		return &multiReadCloser{reader: buf, closeFns: []func() error{reader.Close}}, true, nil
	}

	reader.Close()
	return nil, false, nil
}

// readSegmentDocs reads documents from a single segment.
func (c *FullCompactor) readSegmentDocs(ctx context.Context, seg Segment) ([]MergedDocument, error) {
	if seg.DocsKey == "" {
		// Segment might only have vector data (IVF), read from IVF
		return c.readDocsFromIVF(ctx, seg)
	}

	// Read docs column file
	reader, _, err := c.store.Get(ctx, seg.DocsKey, nil)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			// No docs file, try IVF
			return c.readDocsFromIVF(ctx, seg)
		}
		return nil, fmt.Errorf("failed to get docs file: %w", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read docs data: %w", err)
	}

	decoded := data
	if IsZstdCompressed(data) {
		decoded, err = DecompressZstd(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress docs data: %w", err)
		}
	}

	docs, err := DecodeDocsColumn(decoded)
	if err != nil {
		if errors.Is(err, ErrDocsColumnFormat) || errors.Is(err, ErrDocsColumnVersion) {
			var fallbackDocs []MergedDocument
			if err := json.Unmarshal(decoded, &fallbackDocs); err != nil {
				// Fallback: try to read from IVF if docs format fails
				return c.readDocsFromIVF(ctx, seg)
			}
			docs = fallbackDocs
		} else {
			return nil, fmt.Errorf("failed to decode docs column: %w", err)
		}
	}

	// Ensure NumericID is derived from ID for docs loaded from JSON
	for i := range docs {
		if docs[i].NumericID == 0 && docs[i].ID != "" {
			if parsedID, err := document.ParseIDKey(docs[i].ID); err == nil {
				if parsedID.Type() == document.IDTypeU64 {
					docs[i].NumericID = parsedID.U64()
					continue
				}
			}
			if parsedID, err := document.ParseID(docs[i].ID); err == nil {
				if parsedID.Type() == document.IDTypeU64 {
					docs[i].NumericID = parsedID.U64()
					continue
				}
			}
			if numID, err := strconv.ParseUint(docs[i].ID, 10, 64); err == nil && numID != 0 {
				docs[i].NumericID = numID
			}
		}
	}

	return docs, nil
}

// readDocsFromIVF reads documents from IVF index (vector-only segments).
func (c *FullCompactor) readDocsFromIVF(ctx context.Context, seg Segment) ([]MergedDocument, error) {
	if seg.IVFKeys == nil || !seg.IVFKeys.HasIVF() {
		// No IVF data either, return empty
		return nil, nil
	}

	// Load IVF centroids to get document count
	centroidsReader, _, err := c.store.Get(ctx, seg.IVFKeys.CentroidsKey, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read centroids: %w", err)
	}
	defer centroidsReader.Close()

	centroidsData, err := io.ReadAll(centroidsReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read centroids data: %w", err)
	}

	// Parse IVF header to determine vector dimensions
	ivfIndex, err := vector.LoadCentroidsFromBytes(centroidsData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse centroids: %w", err)
	}

	// Load cluster data to get vectors
	clusterReader, _, err := c.store.Get(ctx, seg.IVFKeys.ClusterDataKey, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster data: %w", err)
	}
	defer clusterReader.Close()

	clusterData, err := io.ReadAll(clusterReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster data: %w", err)
	}

	// Load cluster offsets
	offsetsReader, _, err := c.store.Get(ctx, seg.IVFKeys.ClusterOffsetsKey, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read cluster offsets: %w", err)
	}
	defer offsetsReader.Close()

	offsetsData, err := io.ReadAll(offsetsReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read offsets data: %w", err)
	}

	// Extract documents from IVF data
	docs, err := ivfIndex.ExtractDocuments(offsetsData, clusterData)
	if err != nil {
		return nil, fmt.Errorf("failed to extract documents from IVF: %w", err)
	}

	// Convert to MergedDocument format
	merged := make([]MergedDocument, 0, len(docs))
	for _, doc := range docs {
		merged = append(merged, MergedDocument{
			ID:        fmt.Sprintf("u64:%d", doc.ID),
			NumericID: doc.ID,
			WALSeq:    seg.EndWALSeq, // Approximate with segment's end WAL seq
			Vector:    doc.Vector,
		})
	}

	return merged, nil
}

// mergeDocuments deduplicates documents, keeping newest version (highest WAL seq).
func (c *FullCompactor) mergeDocuments(docs []MergedDocument) []MergedDocument {
	if len(docs) == 0 {
		return nil
	}

	// Sort by WAL seq descending (newest first)
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].WALSeq > docs[j].WALSeq
	})

	// Deduplicate: keep first occurrence (newest) of each ID
	seen := make(map[string]bool)
	var result []MergedDocument

	for _, doc := range docs {
		if seen[doc.ID] {
			continue
		}
		seen[doc.ID] = true

		// Skip tombstones (deleted documents)
		if doc.Deleted {
			continue
		}

		result = append(result, doc)
	}

	return result
}

// buildMergedSegment builds the output segment from merged documents.
func (c *FullCompactor) buildMergedSegment(ctx context.Context, plan *CompactionPlan, docs []MergedDocument) (*CompactionResult, error) {
	segID := GenerateSegmentID()
	writer := NewSegmentWriter(c.store, c.namespace, segID)
	if c.config.DisableChecksums {
		writer.SetChecksumEnabled(false)
	}

	result := &CompactionResult{
		MergedDocs: int64(len(docs)),
	}

	// Collect vectors for IVF index
	var vectors []vectorDoc
	var dims int
	for _, doc := range docs {
		if len(doc.Vector) > 0 {
			if dims == 0 {
				dims = len(doc.Vector)
			}
			vectors = append(vectors, vectorDoc{
				id:        doc.ID,
				numericID: doc.NumericID,
				vec:       doc.Vector,
			})
		}
	}

	// Build IVF index if we have vectors
	var ivfKeys *IVFKeys
	if len(vectors) > 0 {
		var err error
		ivfKeys, result.Reclustered, err = c.buildIVFIndex(ctx, writer, vectors, dims)
		if err != nil {
			return nil, fmt.Errorf("failed to build IVF index: %w", err)
		}
		result.ObjectsWritten = append(result.ObjectsWritten,
			ivfKeys.CentroidsKey,
			ivfKeys.ClusterOffsetsKey,
			ivfKeys.ClusterDataKey)
	}

	// Always write docs column to preserve WAL sequence and tombstone metadata
	var docsKey string
	if len(docs) > 0 {
		rawDocsData, err := EncodeDocsColumn(docs)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize docs: %w", err)
		}
		docsData, err := CompressDocsColumn(rawDocsData)
		if err != nil {
			return nil, fmt.Errorf("failed to compress docs: %w", err)
		}
		docsKey, err = writer.WriteDocsData(ctx, docsData)
		if err != nil {
			return nil, fmt.Errorf("failed to write docs: %w", err)
		}
		result.ObjectsWritten = append(result.ObjectsWritten, docsKey)

		idMapData, err := EncodeDocIDMapFromDocs(docs)
		if err != nil {
			return nil, fmt.Errorf("failed to build doc id map: %w", err)
		}
		idMapKey, err := writer.WriteDocIDMapData(ctx, idMapData)
		if err != nil {
			return nil, fmt.Errorf("failed to write doc id map: %w", err)
		}
		result.ObjectsWritten = append(result.ObjectsWritten, idMapKey)
	}

	writer.Seal()

	// Calculate stats
	logicalBytes := int64(0)
	for _, key := range result.ObjectsWritten {
		if info, err := c.store.Head(ctx, key); err == nil {
			logicalBytes += info.Size
		}
	}
	result.BytesWritten = logicalBytes

	// Create segment metadata
	result.NewSegment = &Segment{
		ID:          segID,
		Level:       plan.TargetLevel,
		StartWALSeq: plan.MinWALSeq,
		EndWALSeq:   plan.MaxWALSeq,
		CreatedAt:   time.Now().UTC(),
		DocsKey:     docsKey,
		IVFKeys:     ivfKeys,
		Stats: SegmentStats{
			RowCount:     int64(len(docs)),
			LogicalBytes: logicalBytes,
		},
	}

	return result, nil
}

type vectorDoc struct {
	id        string
	numericID uint64
	vec       []float32
}

// buildIVFIndex builds an IVF index from vectors.
// When Recluster is true, k-means is run to optimize cluster assignments.
// When Recluster is false, vectors are assigned to clusters using a single-pass
// assignment (no k-means iteration), which is faster but may produce less optimal clusters.
func (c *FullCompactor) buildIVFIndex(ctx context.Context, writer *SegmentWriter, vectors []vectorDoc, dims int) (*IVFKeys, bool, error) {
	if len(vectors) == 0 {
		return nil, false, nil
	}

	// Determine number of clusters
	nClusters := c.config.NClusters
	if nClusters <= 0 {
		// Auto-calculate: sqrt(n), capped at 256
		nClusters = intSqrt(len(vectors))
		if nClusters < 1 {
			nClusters = 1
		}
		if nClusters > 256 {
			nClusters = 256
		}
	}
	if nClusters > len(vectors) {
		nClusters = len(vectors)
	}

	// Build IVF index using the builder
	builder := vector.NewIVFBuilder(dims, c.config.Metric, nClusters)
	for _, v := range vectors {
		if err := builder.AddVector(v.numericID, v.vec); err != nil {
			return nil, false, fmt.Errorf("failed to add vector %s (id=%d): %w", v.id, v.numericID, err)
		}
	}

	var ivfIndex *vector.IVFIndex
	var clusterDataSize int64
	var writeClusterData func(io.Writer) error
	var err error

	if c.config.Recluster {
		// Full k-means clustering for optimal cluster assignments
		ivfIndex, clusterDataSize, writeClusterData, err = builder.BuildStreaming()
		if err != nil {
			return nil, false, fmt.Errorf("failed to build IVF with k-means: %w", err)
		}
	} else {
		// Single-pass assignment without k-means iteration
		// This is faster but produces less optimal clusters
		ivfIndex, clusterDataSize, writeClusterData, err = builder.BuildWithSinglePassPlan()
		if err != nil {
			return nil, false, fmt.Errorf("failed to build IVF with single-pass: %w", err)
		}
	}

	// Write centroids
	var centroidsBuf bytes.Buffer
	if err := ivfIndex.WriteCentroidsFile(&centroidsBuf); err != nil {
		return nil, false, fmt.Errorf("failed to serialize centroids: %w", err)
	}
	centroidsKey, err := writer.WriteIVFCentroids(ctx, centroidsBuf.Bytes())
	if err != nil {
		return nil, false, fmt.Errorf("failed to write centroids: %w", err)
	}

	// Write cluster offsets
	var offsetsBuf bytes.Buffer
	if err := ivfIndex.WriteClusterOffsetsFile(&offsetsBuf); err != nil {
		return nil, false, fmt.Errorf("failed to serialize offsets: %w", err)
	}
	offsetsKey, err := writer.WriteIVFClusterOffsets(ctx, offsetsBuf.Bytes())
	if err != nil {
		return nil, false, fmt.Errorf("failed to write offsets: %w", err)
	}

	// Write cluster data (streamed)
	clusterDataKey, err := writer.WriteIVFClusterDataStream(ctx, clusterDataSize, writeClusterData)
	if err != nil {
		return nil, false, fmt.Errorf("failed to write cluster data: %w", err)
	}

	return &IVFKeys{
		CentroidsKey:      centroidsKey,
		ClusterOffsetsKey: offsetsKey,
		ClusterDataKey:    clusterDataKey,
		NClusters:         ivfIndex.NClusters,
		VectorCount:       len(vectors),
	}, c.config.Recluster, nil
}

// intSqrt computes integer square root.
func intSqrt(n int) int {
	if n <= 0 {
		return 0
	}
	x := n
	for {
		y := (x + n/x) / 2
		if y >= x {
			return x
		}
		x = y
	}
}

// BackgroundCompactor manages background compaction for namespaces.
type BackgroundCompactor struct {
	store      objectstore.Store
	lsmConfig  *LSMConfig
	compConfig *CompactorConfig
	stateMan   *namespace.StateManager

	mu        sync.RWMutex
	trees     map[string]*LSMTree
	running   bool
	stopCh    chan struct{}
	stoppedCh chan struct{}

	pollInterval time.Duration
	syncInterval time.Duration
	lastSync     time.Time
}

// NewBackgroundCompactor creates a background compactor.
func NewBackgroundCompactor(store objectstore.Store, lsmConfig *LSMConfig, compConfig *CompactorConfig) *BackgroundCompactor {
	if lsmConfig == nil {
		lsmConfig = DefaultLSMConfig()
	}
	compConfig = normalizeCompactorConfig(compConfig)
	return &BackgroundCompactor{
		store:        store,
		lsmConfig:    lsmConfig,
		compConfig:   compConfig,
		trees:        make(map[string]*LSMTree),
		pollInterval: 5 * time.Second, // Check for compaction work every 5 seconds
		syncInterval: 30 * time.Second,
	}
}

// RegisterNamespace registers a namespace's LSM tree for compaction monitoring.
func (bc *BackgroundCompactor) RegisterNamespace(ns string, tree *LSMTree) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.trees[ns] = tree
}

// SetStateManager configures the state manager used to publish compaction manifests.
func (bc *BackgroundCompactor) SetStateManager(stateMan *namespace.StateManager) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.stateMan = stateMan
}

// UnregisterNamespace removes a namespace from compaction monitoring.
func (bc *BackgroundCompactor) UnregisterNamespace(ns string) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	delete(bc.trees, ns)
}

// Start begins the background compaction loop.
func (bc *BackgroundCompactor) Start() {
	bc.mu.Lock()
	if bc.running {
		bc.mu.Unlock()
		return
	}
	bc.running = true
	bc.stopCh = make(chan struct{})
	bc.stoppedCh = make(chan struct{})
	bc.mu.Unlock()

	go bc.run()
}

// Stop gracefully stops the background compactor.
func (bc *BackgroundCompactor) Stop() {
	bc.mu.Lock()
	if !bc.running {
		bc.mu.Unlock()
		return
	}
	bc.running = false
	close(bc.stopCh)
	bc.mu.Unlock()

	<-bc.stoppedCh
}

// IsRunning returns true if the background compactor is running.
func (bc *BackgroundCompactor) IsRunning() bool {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.running
}

// run is the main background loop.
func (bc *BackgroundCompactor) run() {
	defer close(bc.stoppedCh)

	ticker := time.NewTicker(bc.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bc.stopCh:
			return
		case <-ticker.C:
			bc.checkAndCompact()
		}
	}
}

// checkAndCompact checks all namespaces for compaction needs.
func (bc *BackgroundCompactor) checkAndCompact() {
	bc.syncNamespaces()

	bc.mu.RLock()
	namespaces := make([]string, 0, len(bc.trees))
	for ns := range bc.trees {
		namespaces = append(namespaces, ns)
	}
	bc.mu.RUnlock()

	for _, ns := range namespaces {
		bc.mu.RLock()
		tree, ok := bc.trees[ns]
		bc.mu.RUnlock()
		if !ok {
			continue
		}

		// Try L0->L1 compaction first
		if tree.NeedsL0Compaction() {
			if err := bc.compactL0(ns, tree); err != nil {
				log.Printf("[compaction] ns=%s level=l0_l1 error=%v", ns, err)
				continue
			}
		}

		// Then check L1->L2 compaction
		if tree.NeedsL1Compaction() {
			if err := bc.compactL1(ns, tree); err != nil {
				log.Printf("[compaction] ns=%s level=l1_l2 error=%v", ns, err)
				continue
			}
		}
	}
}

func (bc *BackgroundCompactor) syncNamespaces() {
	bc.mu.RLock()
	store := bc.store
	stateMan := bc.stateMan
	lsmConfig := bc.lsmConfig
	interval := bc.syncInterval
	lastSync := bc.lastSync
	bc.mu.RUnlock()

	if store == nil || stateMan == nil {
		return
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if !lastSync.IsZero() && time.Since(lastSync) < interval {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	namespaces, err := listNamespacesFromStore(ctx, store)
	if err != nil {
		log.Printf("[compaction] namespace sync error: %v", err)
		return
	}

	for _, ns := range namespaces {
		loaded, err := stateMan.Load(ctx, ns)
		if err != nil {
			continue
		}
		seq := loaded.State.Index.ManifestSeq
		if seq == 0 {
			continue
		}
		manifest, err := LoadManifest(ctx, store, ns, seq)
		if err != nil || manifest == nil {
			continue
		}
		tree := LoadLSMTree(ns, store, manifest, lsmConfig)
		bc.RegisterNamespace(ns, tree)
	}

	bc.mu.Lock()
	bc.lastSync = time.Now()
	bc.mu.Unlock()
}

func listNamespacesFromStore(ctx context.Context, store objectstore.Store) ([]string, error) {
	if store == nil {
		return nil, nil
	}

	namespaces := make(map[string]struct{})
	marker := ""
	for {
		result, err := store.List(ctx, &objectstore.ListOptions{
			Prefix:  "vex/namespaces/",
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Objects {
			ns := extractNamespaceFromKey(obj.Key)
			if ns == "" {
				continue
			}
			namespaces[ns] = struct{}{}
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	result := make([]string, 0, len(namespaces))
	for ns := range namespaces {
		result = append(result, ns)
	}
	sort.Strings(result)
	return result, nil
}

func extractNamespaceFromKey(key string) string {
	const base = "vex/namespaces/"
	if !strings.HasPrefix(key, base) {
		return ""
	}
	rest := strings.TrimPrefix(key, base)
	if strings.Count(rest, "/") == 1 && strings.HasSuffix(rest, "/") {
		return strings.TrimSuffix(rest, "/")
	}
	if idx := strings.Index(rest, "/"); idx > 0 {
		return rest[:idx]
	}
	return ""
}

// compactL0 performs L0->L1 compaction for a namespace.
func (bc *BackgroundCompactor) compactL0(ns string, tree *LSMTree) error {
	level := "l0_l1"
	start := time.Now()
	metrics.IncCompactionInProgress(ns, level)
	var resultErr error
	defer func() {
		metrics.DecCompactionInProgress(ns, level)
		metrics.ObserveCompaction(ns, level, time.Since(start).Seconds(), resultErr)
		if resultErr == nil {
			log.Printf("[compaction] ns=%s level=%s success dur=%s", ns, level, time.Since(start))
		} else {
			log.Printf("[compaction] ns=%s level=%s failed dur=%s err=%v", ns, level, time.Since(start), resultErr)
		}
	}()

	// Mark compaction in progress
	if err := tree.SetCompacting(true); err != nil {
		resultErr = err
		return err
	}
	defer tree.SetCompacting(false)

	// Plan compaction
	plan, err := tree.PlanL0Compaction()
	if err != nil {
		resultErr = err
		return err
	}
	log.Printf("[compaction] ns=%s level=%s segments=%d bytes=%d rows=%d", ns, level, len(plan.SourceSegments), plan.TotalBytes, plan.TotalRows)
	if exists, err := bc.compactionRequestExists(context.Background(), ns, plan); err != nil {
		resultErr = err
		return err
	} else if exists {
		log.Printf("[compaction] ns=%s level=%s request already exists", ns, level)
		return nil
	}

	// Execute compaction
	compactor := NewFullCompactor(bc.store, ns, bc.compConfig)
	result, err := compactor.Compact(context.Background(), plan)
	if err != nil {
		resultErr = err
		return fmt.Errorf("L0->L1 compaction failed: %w", err)
	}

	enqueued, err := bc.enqueueCompactionRequest(context.Background(), ns, plan, *result.NewSegment)
	if err != nil {
		resultErr = err
		return err
	}
	if enqueued {
		log.Printf("[compaction] ns=%s level=%s enqueued request for %d segments", ns, level, len(plan.SourceSegments))
	} else {
		log.Printf("[compaction] ns=%s level=%s request already exists", ns, level)
	}

	return nil
}

// compactL1 performs L1->L2 compaction for a namespace.
func (bc *BackgroundCompactor) compactL1(ns string, tree *LSMTree) error {
	level := "l1_l2"
	start := time.Now()
	metrics.IncCompactionInProgress(ns, level)
	var resultErr error
	defer func() {
		metrics.DecCompactionInProgress(ns, level)
		metrics.ObserveCompaction(ns, level, time.Since(start).Seconds(), resultErr)
		if resultErr == nil {
			log.Printf("[compaction] ns=%s level=%s success dur=%s", ns, level, time.Since(start))
		} else {
			log.Printf("[compaction] ns=%s level=%s failed dur=%s err=%v", ns, level, time.Since(start), resultErr)
		}
	}()

	if err := tree.SetCompacting(true); err != nil {
		resultErr = err
		return err
	}
	defer tree.SetCompacting(false)

	plan, err := tree.PlanL1Compaction()
	if err != nil {
		resultErr = err
		return err
	}
	log.Printf("[compaction] ns=%s level=%s segments=%d bytes=%d rows=%d", ns, level, len(plan.SourceSegments), plan.TotalBytes, plan.TotalRows)
	if exists, err := bc.compactionRequestExists(context.Background(), ns, plan); err != nil {
		resultErr = err
		return err
	} else if exists {
		log.Printf("[compaction] ns=%s level=%s request already exists", ns, level)
		return nil
	}

	// Enable reclustering for L1->L2 compaction
	config := *bc.compConfig
	config.Recluster = true

	compactor := NewFullCompactor(bc.store, ns, &config)
	result, err := compactor.Compact(context.Background(), plan)
	if err != nil {
		resultErr = err
		return fmt.Errorf("L1->L2 compaction failed: %w", err)
	}

	enqueued, err := bc.enqueueCompactionRequest(context.Background(), ns, plan, *result.NewSegment)
	if err != nil {
		resultErr = err
		return err
	}
	if enqueued {
		log.Printf("[compaction] ns=%s level=%s enqueued request for %d segments", ns, level, len(plan.SourceSegments))
	} else {
		log.Printf("[compaction] ns=%s level=%s request already exists", ns, level)
	}

	return nil
}

func (bc *BackgroundCompactor) enqueueCompactionRequest(ctx context.Context, ns string, plan *CompactionPlan, newSegment Segment) (bool, error) {
	bc.mu.RLock()
	stateMan := bc.stateMan
	bc.mu.RUnlock()
	if stateMan == nil {
		return false, fmt.Errorf("compaction state manager not configured")
	}

	requestID := CompactionRequestID(ns, plan.TargetLevel, plan.SourceSegments)
	requestKey := CompactionRequestKey(ns, requestID)
	if _, err := bc.store.Head(ctx, requestKey); err == nil {
		return false, nil
	} else if err != nil && !objectstore.IsNotFoundError(err) {
		return false, fmt.Errorf("failed to check compaction request: %w", err)
	}

	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		return false, fmt.Errorf("failed to load state: %w", err)
	}
	if loaded.State.Index.ManifestSeq == 0 || loaded.State.Index.ManifestKey == "" {
		return false, fmt.Errorf("namespace %s has no manifest", ns)
	}

	req := NewCompactionRequest(ns, loaded.State.Index.ManifestSeq, loaded.State.Index.ManifestKey, plan.SourceSegments, newSegment)
	req.ID = requestID
	_, created, err := WriteCompactionRequest(ctx, bc.store, req)
	if err != nil {
		return false, err
	}
	return created, nil
}

func (bc *BackgroundCompactor) compactionRequestExists(ctx context.Context, ns string, plan *CompactionPlan) (bool, error) {
	if bc.store == nil {
		return false, fmt.Errorf("object store not configured")
	}
	requestID := CompactionRequestID(ns, plan.TargetLevel, plan.SourceSegments)
	requestKey := CompactionRequestKey(ns, requestID)
	if _, err := bc.store.Head(ctx, requestKey); err == nil {
		return true, nil
	} else if objectstore.IsNotFoundError(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (bc *BackgroundCompactor) updateStateForManifest(ctx context.Context, ns, manifestKey string, manifestSeq uint64, indexedWALSeq uint64) error {
	loaded, err := bc.stateMan.Load(ctx, ns)
	if err != nil {
		return err
	}
	if loaded.State.Index.ManifestSeq > manifestSeq {
		return fmt.Errorf("%w: state manifest seq %d ahead of %d", ErrCompactionStale, loaded.State.Index.ManifestSeq, manifestSeq)
	}
	if loaded.State.Index.ManifestSeq == manifestSeq {
		if loaded.State.Index.ManifestKey == manifestKey {
			return nil
		}
		return fmt.Errorf("%w: state manifest key mismatch for seq %d", ErrCompactionStale, manifestSeq)
	}

	_, err = bc.stateMan.UpdateIndexManifest(ctx, ns, loaded.ETag, manifestKey, manifestSeq, indexedWALSeq)
	if err != nil {
		if errors.Is(err, namespace.ErrCASRetryExhausted) {
			current, loadErr := bc.stateMan.Load(ctx, ns)
			if loadErr == nil && current.State.Index.ManifestSeq >= manifestSeq {
				return fmt.Errorf("%w: state manifest seq advanced to %d (target %d)", ErrCompactionStale, current.State.Index.ManifestSeq, manifestSeq)
			}
		}
		return err
	}
	return nil
}

func (bc *BackgroundCompactor) updateStateForManifestWithRetry(ctx context.Context, ns, manifestKey string, manifestSeq uint64, indexedWALSeq uint64) error {
	const maxAttempts = 10
	baseDelay := 50 * time.Millisecond
	maxDelay := 500 * time.Millisecond
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := bc.updateStateForManifest(ctx, ns, manifestKey, manifestSeq, indexedWALSeq)
		if err == nil || errors.Is(err, ErrCompactionStale) {
			return err
		}
		lastErr = err
		sleepWithBackoff(ctx, attempt, baseDelay, maxDelay)
	}

	if lastErr != nil {
		return fmt.Errorf("%w: state update retries exhausted: %v", ErrCompactionStale, lastErr)
	}
	return fmt.Errorf("%w: state update retries exhausted", ErrCompactionStale)
}

func sleepWithBackoff(ctx context.Context, attempt int, baseDelay, maxDelay time.Duration) {
	delay := baseDelay * time.Duration(attempt+1)
	if delay > maxDelay {
		delay = maxDelay
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(delay):
	}
}

func (bc *BackgroundCompactor) cleanupObsoleteSegments(ctx context.Context, segments []Segment) error {
	retention := bc.compConfig.RetentionTime
	now := time.Now()
	var lastErr error

	for _, seg := range segments {
		age, ok := bc.segmentAge(ctx, seg, now)
		if !ok {
			continue
		}
		if age < retention {
			continue
		}
		for _, key := range segmentObjectKeys(seg) {
			if key == "" {
				continue
			}
			if err := bc.store.Delete(ctx, key); err != nil && !objectstore.IsNotFoundError(err) {
				lastErr = err
			}
		}
	}

	return lastErr
}

func (bc *BackgroundCompactor) segmentAge(ctx context.Context, seg Segment, now time.Time) (time.Duration, bool) {
	if !seg.CreatedAt.IsZero() {
		return now.Sub(seg.CreatedAt), true
	}
	keys := segmentObjectKeys(seg)
	for _, key := range keys {
		if key == "" {
			continue
		}
		info, err := bc.store.Head(ctx, key)
		if err != nil {
			if objectstore.IsNotFoundError(err) {
				continue
			}
			return 0, false
		}
		return now.Sub(info.LastModified), true
	}
	return 0, false
}

func segmentObjectKeys(seg Segment) []string {
	var keys []string
	if seg.DocsKey != "" {
		keys = append(keys, seg.DocsKey)
	}
	if seg.VectorsKey != "" {
		keys = append(keys, seg.VectorsKey)
	}
	keys = append(keys, seg.IVFKeys.AllKeys()...)
	keys = append(keys, seg.FilterKeys...)
	keys = append(keys, seg.FTSKeys...)
	return keys
}

// TriggerCompaction forces a compaction check for a namespace.
// This is useful for testing or when immediate compaction is needed.
func (bc *BackgroundCompactor) TriggerCompaction(ns string) error {
	bc.mu.RLock()
	tree, ok := bc.trees[ns]
	bc.mu.RUnlock()

	if !ok {
		return fmt.Errorf("namespace %s not registered", ns)
	}

	if tree.NeedsL0Compaction() {
		return bc.compactL0(ns, tree)
	}

	if tree.NeedsL1Compaction() {
		return bc.compactL1(ns, tree)
	}

	return ErrNoSegmentsToCompact
}
