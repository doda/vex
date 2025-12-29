// Package indexer implements the indexer process that watches WAL and builds indexes.
package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// L0SegmentBuilderConfig configures the L0 segment builder.
type L0SegmentBuilderConfig struct {
	// TargetSizeBytes is the approximate target size for L0 segments.
	// Default: 1MB
	TargetSizeBytes int64

	// NClusters is the number of IVF clusters to create.
	// Default: sqrt(n) or 256, whichever is smaller.
	NClusters int

	// Metric is the distance metric to use for the IVF index.
	// Default: cosine_distance
	Metric vector.DistanceMetric
}

// DefaultL0Config returns the default L0 segment builder configuration.
func DefaultL0Config() *L0SegmentBuilderConfig {
	return &L0SegmentBuilderConfig{
		TargetSizeBytes: index.DefaultL0TargetSizeBytes,
		NClusters:       0, // Auto-calculate based on vector count
		Metric:          vector.MetricCosineDistance,
	}
}

// L0SegmentProcessor builds L0 segments with IVF indexes from WAL entries.
type L0SegmentProcessor struct {
	store    objectstore.Store
	stateMan *namespace.StateManager
	config   *L0SegmentBuilderConfig
	indexer  *Indexer
}

// NewL0SegmentProcessor creates a new L0 segment processor.
func NewL0SegmentProcessor(store objectstore.Store, stateMan *namespace.StateManager, config *L0SegmentBuilderConfig, indexer *Indexer) *L0SegmentProcessor {
	if config == nil {
		config = DefaultL0Config()
	}
	return &L0SegmentProcessor{
		store:    store,
		stateMan: stateMan,
		config:   config,
		indexer:  indexer,
	}
}

// AsWALProcessor returns a WALProcessor function that wraps this L0SegmentProcessor.
func (p *L0SegmentProcessor) AsWALProcessor() WALProcessor {
	return func(ctx context.Context, ns string, startSeq, endSeq uint64, state *namespace.State, etag string) (*WALProcessResult, error) {
		return p.ProcessWAL(ctx, ns, startSeq, endSeq, state, etag)
	}
}

// ProcessWAL builds L0 segments with IVF indexes from WAL entries.
// The etag parameter is the current namespace state ETag for optimistic locking.
func (p *L0SegmentProcessor) ProcessWAL(ctx context.Context, ns string, startSeq, endSeq uint64, state *namespace.State, etag string) (*WALProcessResult, error) {
	// Read WAL entries
	entries, totalBytes, lastSeq, err := p.indexer.ProcessWALRange(ctx, ns, startSeq, endSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL range: %w", err)
	}

	if len(entries) == 0 {
		return &WALProcessResult{
			BytesIndexed:       0,
			ProcessedWALSeq:    lastSeq,
			ProcessedWALSeqSet: true,
		}, nil
	}

	// Extract documents with vectors from WAL entries
	docs, dims := extractVectorDocuments(entries)
	if len(docs) == 0 {
		// No vectors or tombstones in this WAL range, just track the bytes processed
		return &WALProcessResult{
			BytesIndexed:       totalBytes,
			ProcessedWALSeq:    lastSeq,
			ProcessedWALSeqSet: true,
		}, nil
	}

	dtype := vector.DTypeF32
	if state.Vector != nil {
		candidate := vector.DType(state.Vector.DType)
		if candidate.IsValid() {
			dtype = candidate
		}
	}

	// Build L0 segment with IVF index
	segment, err := p.buildL0Segment(ctx, ns, startSeq+1, lastSeq, docs, dims, dtype)
	if err != nil {
		return nil, fmt.Errorf("failed to build L0 segment: %w", err)
	}

	if segment != nil {
		// Publish the new manifest with the L0 segment and update state
		result, err := p.publishSegment(ctx, ns, segment, state, etag)
		if err != nil {
			return nil, fmt.Errorf("failed to publish segment: %w", err)
		}
		result.BytesIndexed = totalBytes
		return result, nil
	}

	return &WALProcessResult{
		BytesIndexed:       totalBytes,
		ProcessedWALSeq:    lastSeq,
		ProcessedWALSeqSet: true,
	}, nil
}

// vectorDocument represents a document with vector data for indexing.
type vectorDocument struct {
	id        document.ID
	idKey     string
	numericID uint64
	walSeq    uint64
	vec       []float32
	attrs     map[string]any
	deleted   bool
}

// extractVectorDocuments extracts documents with vectors from WAL entries.
// Returns the deduped documents and the vector dimensions.
func extractVectorDocuments(entries []*wal.WalEntry) ([]vectorDocument, int) {
	// Track latest version of each document (last-write-wins)
	docMap := make(map[string]*vectorDocument)
	var dims int

	for _, entry := range entries {
		for _, batch := range entry.SubBatches {
			for _, mutation := range batch.Mutations {
				if mutation.Id == nil {
					continue
				}
				docID, err := wal.DocumentIDToID(mutation.Id)
				if err != nil {
					continue
				}
				docKey := documentIDKey(mutation.Id)
				if docKey == "" {
					continue
				}

				switch mutation.Type {
				case wal.MutationType_MUTATION_TYPE_DELETE:
					docMap[docKey] = &vectorDocument{
						id:      docID,
						idKey:   docKey,
						walSeq:  entry.Seq,
						deleted: true,
					}

				case wal.MutationType_MUTATION_TYPE_UPSERT:
					if len(mutation.Vector) > 0 && mutation.VectorDims > 0 {
						vec := decodeVector(mutation.Vector, mutation.VectorDims)
						if vec != nil {
							if dims == 0 {
								dims = int(mutation.VectorDims)
							}
							attrs := make(map[string]any, len(mutation.Attributes))
							for name, value := range mutation.Attributes {
								attrs[name] = attributeValueToAny(value)
							}
							docMap[docKey] = &vectorDocument{
								id:      docID,
								idKey:   docKey,
								walSeq:  entry.Seq,
								vec:     vec,
								attrs:   attrs,
								deleted: false,
							}
						}
					}
				}
			}
		}
	}

	// Convert map to slice
	docs := make([]vectorDocument, 0, len(docMap))
	for _, doc := range docMap {
		docs = append(docs, *doc)
	}

	sort.Slice(docs, func(i, j int) bool {
		if docs[i].walSeq != docs[j].walSeq {
			return docs[i].walSeq < docs[j].walSeq
		}
		return docs[i].idKey < docs[j].idKey
	})
	for i := range docs {
		docs[i].numericID = numericIDForKey(docs[i].idKey)
	}

	return docs, dims
}

// decodeVector decodes a raw byte slice into float32 vector.
func decodeVector(data []byte, dims uint32) []float32 {
	if len(data) == 0 {
		return nil
	}

	// float32 format
	if uint32(len(data)) == dims*4 {
		vec := make([]float32, dims)
		for i := uint32(0); i < dims; i++ {
			bits := uint32(data[i*4]) | uint32(data[i*4+1])<<8 |
				uint32(data[i*4+2])<<16 | uint32(data[i*4+3])<<24
			vec[i] = float32frombits(bits)
		}
		return vec
	}

	// float16 format
	if uint32(len(data)) == dims*2 {
		vec := make([]float32, dims)
		for i := uint32(0); i < dims; i++ {
			bits := uint16(data[i*2]) | uint16(data[i*2+1])<<8
			vec[i] = float16ToFloat32(bits)
		}
		return vec
	}

	return nil
}

// float32frombits converts bits to float32.
func float32frombits(b uint32) float32 {
	return math.Float32frombits(b)
}

// float16ToFloat32 converts float16 to float32.
func float16ToFloat32(h uint16) float32 {
	sign := uint32((h >> 15) & 1)
	exp := int32((h >> 10) & 0x1F)
	mant := uint32(h & 0x3FF)

	var f uint32

	if exp == 0 {
		if mant == 0 {
			f = sign << 31
		} else {
			for mant&0x400 == 0 {
				mant <<= 1
				exp--
			}
			exp++
			mant &= 0x3FF
			f = (sign << 31) | (uint32(exp+127-15) << 23) | (mant << 13)
		}
	} else if exp == 31 {
		f = (sign << 31) | 0x7F800000 | (mant << 13)
	} else {
		f = (sign << 31) | (uint32(exp+127-15) << 23) | (mant << 13)
	}

	return float32frombits(f)
}

func documentIDKey(id *wal.DocumentID) string {
	if id == nil {
		return ""
	}
	switch v := id.GetId().(type) {
	case *wal.DocumentID_U64:
		return fmt.Sprintf("u64:%d", v.U64)
	case *wal.DocumentID_Uuid:
		return fmt.Sprintf("uuid:%x", v.Uuid)
	case *wal.DocumentID_Str:
		return fmt.Sprintf("str:%s", v.Str)
	default:
		return ""
	}
}

func attributeValueToAny(v *wal.AttributeValue) any {
	if v == nil {
		return nil
	}
	switch val := v.GetValue().(type) {
	case *wal.AttributeValue_StringVal:
		return val.StringVal
	case *wal.AttributeValue_IntVal:
		return val.IntVal
	case *wal.AttributeValue_UintVal:
		return val.UintVal
	case *wal.AttributeValue_FloatVal:
		return val.FloatVal
	case *wal.AttributeValue_DatetimeVal:
		return val.DatetimeVal
	case *wal.AttributeValue_BoolVal:
		return val.BoolVal
	case *wal.AttributeValue_NullVal:
		return nil
	case *wal.AttributeValue_UuidVal:
		return val.UuidVal
	case *wal.AttributeValue_StringArray:
		if val.StringArray != nil {
			return val.StringArray.Values
		}
		return nil
	case *wal.AttributeValue_IntArray:
		if val.IntArray != nil {
			return val.IntArray.Values
		}
		return nil
	case *wal.AttributeValue_UintArray:
		if val.UintArray != nil {
			return val.UintArray.Values
		}
		return nil
	case *wal.AttributeValue_FloatArray:
		if val.FloatArray != nil {
			return val.FloatArray.Values
		}
		return nil
	case *wal.AttributeValue_DatetimeArray:
		if val.DatetimeArray != nil {
			return val.DatetimeArray.Values
		}
		return nil
	case *wal.AttributeValue_BoolArray:
		if val.BoolArray != nil {
			return val.BoolArray.Values
		}
		return nil
	default:
		return nil
	}
}

func numericIDForKey(key string) uint64 {
	if key == "" {
		return 0
	}
	id := xxhash.Sum64String(key)
	if id == 0 {
		return 1
	}
	return id
}

type vectorDocColumn struct {
	ID         string
	NumericID  uint64
	WALSeq     uint64
	Deleted    bool
	Attributes map[string]any
	Vector     []float32
}

// buildL0Segment builds an L0 segment with IVF index from the documents.
func (p *L0SegmentProcessor) buildL0Segment(ctx context.Context, ns string, startSeq, endSeq uint64, docs []vectorDocument, dims int, dtype vector.DType) (*index.Segment, error) {
	if len(docs) == 0 {
		return nil, nil
	}

	var vectorDocs []vectorDocument
	tombstoneCount := int64(0)
	liveCount := int64(0)
	for _, doc := range docs {
		if doc.deleted {
			tombstoneCount++
			continue
		}
		if len(doc.vec) == 0 {
			continue
		}
		liveCount++
		vectorDocs = append(vectorDocs, doc)
	}

	if len(vectorDocs) > 0 && dims == 0 {
		return nil, fmt.Errorf("missing vector dimensions for segment build")
	}

	// Determine number of clusters
	nClusters := p.config.NClusters
	if nClusters <= 0 && len(vectorDocs) > 0 {
		// Auto-calculate: sqrt(n), capped at 256
		nClusters = intSqrt(len(vectorDocs))
		if nClusters < 1 {
			nClusters = 1
		}
		if nClusters > 256 {
			nClusters = 256
		}
	}
	if len(vectorDocs) > 0 && nClusters > len(vectorDocs) {
		nClusters = len(vectorDocs)
	}

	// Generate segment ID and write IVF files
	segID := index.GenerateSegmentID()
	writer := index.NewSegmentWriter(p.store, ns, segID)

	// Write centroids (small file, cacheable in RAM)
	var centroidsBuf bytes.Buffer
	var offsetsBuf bytes.Buffer
	var ivfIndex *vector.IVFIndex
	var centroidsKey string
	var offsetsKey string
	var clusterDataKey string
	if len(vectorDocs) > 0 {
		// Build IVF index
		builder := vector.NewIVFBuilderWithDType(dims, dtype, p.config.Metric, nClusters)
		for _, doc := range vectorDocs {
			if err := builder.AddVector(doc.numericID, doc.vec); err != nil {
				return nil, fmt.Errorf("failed to add vector %s: %w", doc.id.String(), err)
			}
		}

		var err error
		ivfIndex, err = builder.Build()
		if err != nil {
			return nil, fmt.Errorf("failed to build IVF index: %w", err)
		}

		if err := ivfIndex.WriteCentroidsFile(&centroidsBuf); err != nil {
			return nil, fmt.Errorf("failed to serialize centroids: %w", err)
		}
		centroidsKey, err = writer.WriteIVFCentroids(ctx, centroidsBuf.Bytes())
		if err != nil {
			return nil, fmt.Errorf("failed to write centroids: %w", err)
		}

		// Write cluster offsets (small file, cacheable)
		if err := ivfIndex.WriteClusterOffsetsFile(&offsetsBuf); err != nil {
			return nil, fmt.Errorf("failed to serialize cluster offsets: %w", err)
		}
		offsetsKey, err = writer.WriteIVFClusterOffsets(ctx, offsetsBuf.Bytes())
		if err != nil {
			return nil, fmt.Errorf("failed to write cluster offsets: %w", err)
		}

		// Write cluster data (large file, range-read)
		clusterDataKey, err = writer.WriteIVFClusterData(ctx, ivfIndex.GetClusterDataBytes())
		if err != nil {
			return nil, fmt.Errorf("failed to write cluster data: %w", err)
		}
	}

	docColumns := make([]vectorDocColumn, 0, len(docs))
	for _, doc := range docs {
		docColumns = append(docColumns, vectorDocColumn{
			ID:         doc.id.String(),
			NumericID:  doc.numericID,
			WALSeq:     doc.walSeq,
			Deleted:    doc.deleted,
			Attributes: doc.attrs,
			Vector:     doc.vec,
		})
	}
	docsData, err := json.Marshal(docColumns)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize docs: %w", err)
	}
	docsKey, err := writer.WriteDocsData(ctx, docsData)
	if err != nil {
		return nil, fmt.Errorf("failed to write docs: %w", err)
	}

	writer.Seal()

	// Calculate segment stats
	bytesPerElement := dtype.BytesPerElement()
	if bytesPerElement == 0 {
		bytesPerElement = 4
	}
	vectorBytes := int64(len(vectorDocs) * dims * bytesPerElement)
	totalBytes := int64(len(docsData))
	if ivfIndex != nil {
		totalBytes += int64(centroidsBuf.Len()) + int64(offsetsBuf.Len()) + int64(len(ivfIndex.GetClusterDataBytes()))
	}

	segment := &index.Segment{
		ID:          segID,
		Level:       index.L0,
		StartWALSeq: startSeq,
		EndWALSeq:   endSeq,
		DocsKey:     docsKey,
		IVFKeys: func() *index.IVFKeys {
			if ivfIndex == nil {
				return nil
			}
			return &index.IVFKeys{
				CentroidsKey:      centroidsKey,
				ClusterOffsetsKey: offsetsKey,
				ClusterDataKey:    clusterDataKey,
				NClusters:         ivfIndex.NClusters,
				VectorCount:       len(vectorDocs),
			}
		}(),
		Stats: index.SegmentStats{
			RowCount:       liveCount,
			LogicalBytes:   vectorBytes + totalBytes,
			TombstoneCount: tombstoneCount,
		},
	}

	return segment, nil
}

// publishSegment writes the manifest with the new segment and updates namespace state.
// Returns a WALProcessResult with manifest info to signal to the caller that state was updated.
func (p *L0SegmentProcessor) publishSegment(ctx context.Context, ns string, segment *index.Segment, state *namespace.State, etag string) (*WALProcessResult, error) {
	// Load existing manifest if there is one
	var manifest *index.Manifest
	if state.Index.ManifestKey != "" {
		existingManifest, err := p.loadManifest(ctx, state.Index.ManifestKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load existing manifest: %w", err)
		}
		if existingManifest != nil {
			manifest = existingManifest
		}
	}

	// Create new manifest if none exists
	if manifest == nil {
		manifest = index.NewManifest(ns)
	}

	// Add the new L0 segment
	manifest.AddSegment(*segment)
	manifest.UpdateIndexedWALSeq()

	// Serialize manifest
	manifestData, err := manifest.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize manifest: %w", err)
	}

	if err := index.VerifyManifestReferences(ctx, p.store, manifest); err != nil {
		return nil, fmt.Errorf("manifest references missing objects: %w", err)
	}

	// Write manifest to object storage with new sequence number
	newManifestSeq := state.Index.ManifestSeq + 1
	newManifestKey := index.ManifestKey(ns, newManifestSeq)
	_, err = p.store.PutIfAbsent(ctx, newManifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), &objectstore.PutOptions{
		ContentType: "application/json",
	})
	if err != nil {
		if objectstore.IsConflictError(err) {
			return nil, fmt.Errorf("manifest already exists: %w", err)
		}
		return nil, fmt.Errorf("failed to write manifest: %w", err)
	}

	// Update namespace state via StateManager to record the new indexed_wal_seq and manifest
	_, err = p.stateMan.AdvanceIndex(
		ctx,
		ns,
		etag,
		newManifestKey,
		newManifestSeq,
		segment.EndWALSeq,
		segment.Stats.LogicalBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to advance index state: %w", err)
	}

	return &WALProcessResult{
		ManifestKey:     newManifestKey,
		ManifestSeq:     newManifestSeq,
		IndexedWALSeq:   segment.EndWALSeq,
		ManifestWritten: true,
	}, nil
}

// loadManifest loads a manifest from object storage.
func (p *L0SegmentProcessor) loadManifest(ctx context.Context, manifestKey string) (*index.Manifest, error) {
	if manifestKey == "" {
		return nil, nil
	}

	reader, _, err := p.store.Get(ctx, manifestKey, nil)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	var manifest index.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	return &manifest, nil
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
