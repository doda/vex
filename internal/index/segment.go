// Package index implements the index segment format.
package index

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	// ErrSegmentSealed is returned when attempting to modify a sealed segment.
	ErrSegmentSealed = errors.New("segment is sealed and immutable")

	// ErrSegmentNotSealed is returned when attempting operations that require a sealed segment.
	ErrSegmentNotSealed = errors.New("segment must be sealed before this operation")

	// ErrInvalidWALRange is returned when WAL sequence range is invalid.
	ErrInvalidWALRange = errors.New("invalid WAL sequence range")

	// ErrMissingSegmentID is returned when segment ID is not set.
	ErrMissingSegmentID = errors.New("segment ID is required")

	// ErrMissingDocsKey is returned when docs_key is missing.
	ErrMissingDocsKey = errors.New("docs_key is required for a complete segment")
)

var segmentIDSeq uint64

// SegmentBuilder constructs an immutable Segment.
// Once Build() is called, the segment becomes sealed and immutable.
type SegmentBuilder struct {
	mu      sync.Mutex
	segment Segment
	sealed  bool
}

// NewSegmentBuilder creates a new segment builder with the given ID and WAL range.
func NewSegmentBuilder(id string, startWALSeq, endWALSeq uint64) (*SegmentBuilder, error) {
	if id == "" {
		return nil, ErrMissingSegmentID
	}
	if startWALSeq > endWALSeq {
		return nil, fmt.Errorf("%w: start_wal_seq (%d) > end_wal_seq (%d)",
			ErrInvalidWALRange, startWALSeq, endWALSeq)
	}

	return &SegmentBuilder{
		segment: Segment{
			ID:          id,
			Level:       0,
			StartWALSeq: startWALSeq,
			EndWALSeq:   endWALSeq,
			CreatedAt:   time.Now().UTC(),
		},
	}, nil
}

// SetLevel sets the LSM level for the segment.
func (b *SegmentBuilder) SetLevel(level int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return ErrSegmentSealed
	}
	b.segment.Level = level
	return nil
}

// SetDocsKey sets the object storage key for document column data.
func (b *SegmentBuilder) SetDocsKey(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return ErrSegmentSealed
	}
	b.segment.DocsKey = key
	return nil
}

// SetVectorsKey sets the object storage key for vector data.
// Deprecated: Use SetIVFKeys for the new IVF format.
func (b *SegmentBuilder) SetVectorsKey(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return ErrSegmentSealed
	}
	b.segment.VectorsKey = key
	return nil
}

// SetIVFKeys sets the IVF index keys for this segment.
func (b *SegmentBuilder) SetIVFKeys(keys *IVFKeys) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return ErrSegmentSealed
	}
	// Make a copy to prevent external mutation
	if keys != nil {
		b.segment.IVFKeys = &IVFKeys{
			CentroidsKey:      keys.CentroidsKey,
			ClusterOffsetsKey: keys.ClusterOffsetsKey,
			ClusterDataKey:    keys.ClusterDataKey,
			NClusters:         keys.NClusters,
			VectorCount:       keys.VectorCount,
		}
	} else {
		b.segment.IVFKeys = nil
	}
	return nil
}

// AddFilterKey adds an object storage key for a filter index.
func (b *SegmentBuilder) AddFilterKey(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return ErrSegmentSealed
	}
	b.segment.FilterKeys = append(b.segment.FilterKeys, key)
	return nil
}

// SetFilterKeys sets all filter index keys at once.
func (b *SegmentBuilder) SetFilterKeys(keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return ErrSegmentSealed
	}
	b.segment.FilterKeys = make([]string, len(keys))
	copy(b.segment.FilterKeys, keys)
	return nil
}

// AddFTSKey adds an object storage key for a full-text search index.
func (b *SegmentBuilder) AddFTSKey(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return ErrSegmentSealed
	}
	b.segment.FTSKeys = append(b.segment.FTSKeys, key)
	return nil
}

// SetFTSKeys sets all FTS index keys at once.
func (b *SegmentBuilder) SetFTSKeys(keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return ErrSegmentSealed
	}
	b.segment.FTSKeys = make([]string, len(keys))
	copy(b.segment.FTSKeys, keys)
	return nil
}

// SetStats sets the segment statistics.
func (b *SegmentBuilder) SetStats(stats SegmentStats) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return ErrSegmentSealed
	}
	b.segment.Stats = stats
	return nil
}

// SetCreatedAt sets the creation timestamp.
func (b *SegmentBuilder) SetCreatedAt(t time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return ErrSegmentSealed
	}
	b.segment.CreatedAt = t.UTC()
	return nil
}

// Build finalizes and returns the immutable segment.
// After calling Build(), the segment is sealed and cannot be modified.
func (b *SegmentBuilder) Build() (Segment, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return b.segment, nil
	}

	// Validate the segment before sealing
	if err := b.validate(); err != nil {
		return Segment{}, err
	}

	b.sealed = true
	return b.segment, nil
}

// IsSealed returns true if the segment has been built and sealed.
func (b *SegmentBuilder) IsSealed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sealed
}

// validate checks if the segment is valid.
func (b *SegmentBuilder) validate() error {
	if b.segment.ID == "" {
		return ErrMissingSegmentID
	}
	if b.segment.StartWALSeq > b.segment.EndWALSeq {
		return fmt.Errorf("%w: start_wal_seq (%d) > end_wal_seq (%d)",
			ErrInvalidWALRange, b.segment.StartWALSeq, b.segment.EndWALSeq)
	}
	return nil
}

// ImmutableSegment wraps a Segment to enforce immutability at runtime.
type ImmutableSegment struct {
	segment Segment
}

// NewImmutableSegment creates an immutable wrapper around a segment.
func NewImmutableSegment(seg Segment) (*ImmutableSegment, error) {
	if err := ValidateSegment(seg); err != nil {
		return nil, err
	}
	return &ImmutableSegment{segment: seg}, nil
}

// Segment returns a copy of the underlying segment.
func (s *ImmutableSegment) Segment() Segment {
	seg := s.segment
	if s.segment.FilterKeys != nil {
		seg.FilterKeys = make([]string, len(s.segment.FilterKeys))
		copy(seg.FilterKeys, s.segment.FilterKeys)
	}
	if s.segment.FTSKeys != nil {
		seg.FTSKeys = make([]string, len(s.segment.FTSKeys))
		copy(seg.FTSKeys, s.segment.FTSKeys)
	}
	return seg
}

// ID returns the segment ID.
func (s *ImmutableSegment) ID() string {
	return s.segment.ID
}

// Level returns the LSM level.
func (s *ImmutableSegment) Level() int {
	return s.segment.Level
}

// WALRange returns the WAL sequence range covered by this segment.
func (s *ImmutableSegment) WALRange() (start, end uint64) {
	return s.segment.StartWALSeq, s.segment.EndWALSeq
}

// CoversWALSeq returns true if the segment covers the given WAL sequence.
func (s *ImmutableSegment) CoversWALSeq(seq uint64) bool {
	return seq >= s.segment.StartWALSeq && seq <= s.segment.EndWALSeq
}

// DocsKey returns the object storage key for document data.
func (s *ImmutableSegment) DocsKey() string {
	return s.segment.DocsKey
}

// VectorsKey returns the object storage key for vector data.
func (s *ImmutableSegment) VectorsKey() string {
	return s.segment.VectorsKey
}

// FilterKeys returns a copy of the filter index keys.
func (s *ImmutableSegment) FilterKeys() []string {
	if s.segment.FilterKeys == nil {
		return nil
	}
	result := make([]string, len(s.segment.FilterKeys))
	copy(result, s.segment.FilterKeys)
	return result
}

// FTSKeys returns a copy of the FTS index keys.
func (s *ImmutableSegment) FTSKeys() []string {
	if s.segment.FTSKeys == nil {
		return nil
	}
	result := make([]string, len(s.segment.FTSKeys))
	copy(result, s.segment.FTSKeys)
	return result
}

// IVFKeys returns a copy of the IVF index keys.
func (s *ImmutableSegment) IVFKeys() *IVFKeys {
	if s.segment.IVFKeys == nil {
		return nil
	}
	return &IVFKeys{
		CentroidsKey:      s.segment.IVFKeys.CentroidsKey,
		ClusterOffsetsKey: s.segment.IVFKeys.ClusterOffsetsKey,
		ClusterDataKey:    s.segment.IVFKeys.ClusterDataKey,
		NClusters:         s.segment.IVFKeys.NClusters,
		VectorCount:       s.segment.IVFKeys.VectorCount,
	}
}

// HasIVF returns true if this segment has IVF index data.
func (s *ImmutableSegment) HasIVF() bool {
	return s.segment.IVFKeys.HasIVF()
}

// Stats returns the segment statistics.
func (s *ImmutableSegment) Stats() SegmentStats {
	return s.segment.Stats
}

// CreatedAt returns the creation timestamp.
func (s *ImmutableSegment) CreatedAt() time.Time {
	return s.segment.CreatedAt
}

// AllObjectKeys returns all object storage keys used by this segment.
func (s *ImmutableSegment) AllObjectKeys() []string {
	var keys []string
	if s.segment.DocsKey != "" {
		keys = append(keys, s.segment.DocsKey)
	}
	if s.segment.VectorsKey != "" {
		keys = append(keys, s.segment.VectorsKey)
	}
	// Include IVF keys if present
	keys = append(keys, s.segment.IVFKeys.AllKeys()...)
	keys = append(keys, s.segment.FilterKeys...)
	keys = append(keys, s.segment.FTSKeys...)
	return keys
}

// ValidateSegment validates a segment's structure.
func ValidateSegment(seg Segment) error {
	if seg.ID == "" {
		return ErrMissingSegmentID
	}
	if seg.StartWALSeq > seg.EndWALSeq {
		return fmt.Errorf("%w: start_wal_seq (%d) > end_wal_seq (%d)",
			ErrInvalidWALRange, seg.StartWALSeq, seg.EndWALSeq)
	}
	return nil
}

// SegmentWriter handles writing segment data to object storage.
type SegmentWriter struct {
	store     objectstore.Store
	namespace string
	segmentID string
	checksum  bool

	mu      sync.Mutex
	written map[string]string // key -> etag
	sealed  bool
}

// NewSegmentWriter creates a writer for uploading segment objects.
func NewSegmentWriter(store objectstore.Store, namespace, segmentID string) *SegmentWriter {
	return &SegmentWriter{
		store:     store,
		namespace: namespace,
		segmentID: segmentID,
		checksum:  true,
		written:   make(map[string]string),
	}
}

// SetChecksumEnabled enables or disables checksum verification.
func (w *SegmentWriter) SetChecksumEnabled(enabled bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.checksum = enabled
}

// WriteDocsData writes document column data and returns the object key.
func (w *SegmentWriter) WriteDocsData(ctx context.Context, data []byte) (string, error) {
	w.mu.Lock()
	if w.sealed {
		w.mu.Unlock()
		return "", ErrSegmentSealed
	}
	w.mu.Unlock()

	key := fmt.Sprintf("%s/docs.col.zst", SegmentKey(w.namespace, w.segmentID))
	etag, err := w.writeObject(ctx, key, data)
	if err != nil {
		return "", err
	}

	w.mu.Lock()
	w.written[key] = etag
	w.mu.Unlock()

	return key, nil
}

// WriteVectorsData writes vector index data and returns the object key.
// Deprecated: Use WriteIVFCentroids, WriteIVFClusterOffsets, and WriteIVFClusterData instead.
func (w *SegmentWriter) WriteVectorsData(ctx context.Context, data []byte) (string, error) {
	w.mu.Lock()
	if w.sealed {
		w.mu.Unlock()
		return "", ErrSegmentSealed
	}
	w.mu.Unlock()

	key := fmt.Sprintf("%s/vectors.ivf.zst", SegmentKey(w.namespace, w.segmentID))
	etag, err := w.writeObject(ctx, key, data)
	if err != nil {
		return "", err
	}

	w.mu.Lock()
	w.written[key] = etag
	w.mu.Unlock()

	return key, nil
}

// WriteIVFCentroids writes the IVF centroids file (small, cacheable in RAM).
// Returns the object key for vectors.centroids.bin.
func (w *SegmentWriter) WriteIVFCentroids(ctx context.Context, data []byte) (string, error) {
	w.mu.Lock()
	if w.sealed {
		w.mu.Unlock()
		return "", ErrSegmentSealed
	}
	w.mu.Unlock()

	key := fmt.Sprintf("%s/vectors.centroids.bin", SegmentKey(w.namespace, w.segmentID))
	etag, err := w.writeObject(ctx, key, data)
	if err != nil {
		return "", err
	}

	w.mu.Lock()
	w.written[key] = etag
	w.mu.Unlock()

	return key, nil
}

// WriteIVFClusterOffsets writes the IVF cluster offsets file (small, cacheable).
// Returns the object key for vectors.cluster_offsets.bin.
func (w *SegmentWriter) WriteIVFClusterOffsets(ctx context.Context, data []byte) (string, error) {
	w.mu.Lock()
	if w.sealed {
		w.mu.Unlock()
		return "", ErrSegmentSealed
	}
	w.mu.Unlock()

	key := fmt.Sprintf("%s/vectors.cluster_offsets.bin", SegmentKey(w.namespace, w.segmentID))
	etag, err := w.writeObject(ctx, key, data)
	if err != nil {
		return "", err
	}

	w.mu.Lock()
	w.written[key] = etag
	w.mu.Unlock()

	return key, nil
}

// WriteIVFClusterData writes the IVF packed cluster data file (large).
// Returns the object key for vectors.clusters.pack.
func (w *SegmentWriter) WriteIVFClusterData(ctx context.Context, data []byte) (string, error) {
	w.mu.Lock()
	if w.sealed {
		w.mu.Unlock()
		return "", ErrSegmentSealed
	}
	w.mu.Unlock()

	key := fmt.Sprintf("%s/vectors.clusters.pack", SegmentKey(w.namespace, w.segmentID))
	etag, err := w.writeObject(ctx, key, data)
	if err != nil {
		return "", err
	}

	w.mu.Lock()
	w.written[key] = etag
	w.mu.Unlock()

	return key, nil
}

// WriteFilterData writes filter bitmap data and returns the object key.
func (w *SegmentWriter) WriteFilterData(ctx context.Context, attrName string, data []byte) (string, error) {
	w.mu.Lock()
	if w.sealed {
		w.mu.Unlock()
		return "", ErrSegmentSealed
	}
	w.mu.Unlock()

	key := fmt.Sprintf("%s/filters/%s.bitmap", SegmentKey(w.namespace, w.segmentID), attrName)
	etag, err := w.writeObject(ctx, key, data)
	if err != nil {
		return "", err
	}

	w.mu.Lock()
	w.written[key] = etag
	w.mu.Unlock()

	return key, nil
}

// WriteFTSData writes full-text search index data and returns the object key.
func (w *SegmentWriter) WriteFTSData(ctx context.Context, attrName string, data []byte) (string, error) {
	w.mu.Lock()
	if w.sealed {
		w.mu.Unlock()
		return "", ErrSegmentSealed
	}
	w.mu.Unlock()

	key := fmt.Sprintf("%s/fts/%s.idx", SegmentKey(w.namespace, w.segmentID), attrName)
	etag, err := w.writeObject(ctx, key, data)
	if err != nil {
		return "", err
	}

	w.mu.Lock()
	w.written[key] = etag
	w.mu.Unlock()

	return key, nil
}

// writeObject writes data to object storage with optional checksum.
func (w *SegmentWriter) writeObject(ctx context.Context, key string, data []byte) (string, error) {
	var opts *objectstore.PutOptions
	if w.checksum {
		hash := sha256.Sum256(data)
		checksum := base64.StdEncoding.EncodeToString(hash[:])
		opts = &objectstore.PutOptions{Checksum: checksum}
	}

	result, err := w.store.PutIfAbsent(ctx, key, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		return "", fmt.Errorf("failed to write object %s: %w", key, err)
	}

	return result.ETag, nil
}

// Seal marks the writer as complete, preventing further writes.
func (w *SegmentWriter) Seal() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sealed = true
}

// IsSealed returns true if the writer has been sealed.
func (w *SegmentWriter) IsSealed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sealed
}

// WrittenKeys returns all keys that have been written.
func (w *SegmentWriter) WrittenKeys() map[string]string {
	w.mu.Lock()
	defer w.mu.Unlock()

	result := make(map[string]string, len(w.written))
	for k, v := range w.written {
		result[k] = v
	}
	return result
}

// GenerateSegmentID generates a unique segment ID.
func GenerateSegmentID() string {
	seq := atomic.AddUint64(&segmentIDSeq, 1)
	return fmt.Sprintf("seg_%d_%d", time.Now().UnixNano(), seq)
}

// DocsObjectKey returns the standard object key for docs data.
func DocsObjectKey(namespace, segmentID string) string {
	return fmt.Sprintf("%s/docs.col.zst", SegmentKey(namespace, segmentID))
}

// VectorsObjectKey returns the standard object key for vectors data.
func VectorsObjectKey(namespace, segmentID string) string {
	return fmt.Sprintf("%s/vectors.ivf.zst", SegmentKey(namespace, segmentID))
}

// FilterObjectKey returns the standard object key for a filter bitmap.
func FilterObjectKey(namespace, segmentID, attrName string) string {
	return fmt.Sprintf("%s/filters/%s.bitmap", SegmentKey(namespace, segmentID), attrName)
}

// FTSObjectKey returns the standard object key for a FTS index.
func FTSObjectKey(namespace, segmentID, attrName string) string {
	return fmt.Sprintf("%s/fts/%s.idx", SegmentKey(namespace, segmentID), attrName)
}

// SegmentMetadata contains metadata about a segment stored in object storage.
type SegmentMetadata struct {
	Segment   Segment           `json:"segment"`
	ObjectMap map[string]string `json:"object_map"` // key -> etag
	SealedAt  time.Time         `json:"sealed_at"`
}

// MarshalJSON serializes segment metadata.
func (m *SegmentMetadata) MarshalJSON() ([]byte, error) {
	type alias SegmentMetadata
	return json.Marshal((*alias)(m))
}

// UnmarshalJSON deserializes segment metadata.
func (m *SegmentMetadata) UnmarshalJSON(data []byte) error {
	type alias SegmentMetadata
	return json.Unmarshal(data, (*alias)(m))
}
