// Package index implements the index manifest format and segment metadata.
package index

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

const (
	// CurrentManifestVersion is the current version of the manifest format.
	CurrentManifestVersion = 1
)

// Manifest represents an index manifest stored at index/manifests/<gen>.idx.json.
// It contains all metadata needed to read the indexed data for a namespace.
type Manifest struct {
	// FormatVersion identifies the manifest format version for compatibility.
	FormatVersion int `json:"format_version"`

	// Namespace is the name of the namespace this manifest belongs to.
	Namespace string `json:"namespace"`

	// GeneratedAt is when this manifest was created.
	GeneratedAt time.Time `json:"generated_at"`

	// IndexedWALSeq is the highest WAL sequence number included in this index.
	// Query nodes use this to know which WAL entries are already indexed.
	IndexedWALSeq uint64 `json:"indexed_wal_seq"`

	// SchemaVersionHash is a hash of the schema used to build this index.
	// Used to detect when a schema change requires index rebuild.
	SchemaVersionHash string `json:"schema_version_hash,omitempty"`

	// Segments is the list of all segments in this index.
	Segments []Segment `json:"segments"`

	// Stats contains aggregate statistics about the indexed data.
	Stats Stats `json:"stats"`
}

// Segment represents an immutable index segment.
// Each segment covers a contiguous WAL sequence interval.
type Segment struct {
	// ID is the unique identifier for this segment (e.g., "seg_01H...").
	ID string `json:"id"`

	// Level indicates the LSM level (0 = L0, 1 = L1, etc.).
	// L0 segments are created from WAL batches, higher levels from compaction.
	Level int `json:"level"`

	// StartWALSeq is the first WAL sequence included in this segment.
	StartWALSeq uint64 `json:"start_wal_seq"`

	// EndWALSeq is the last WAL sequence included in this segment.
	EndWALSeq uint64 `json:"end_wal_seq"`

	// DocsKey is the object storage key for the document column data.
	DocsKey string `json:"docs_key,omitempty"`

	// VectorsKey is the object storage key for vector data (IVF index).
	// Deprecated: Use IVFKeys for the new IVF format.
	VectorsKey string `json:"vectors_key,omitempty"`

	// IVFKeys contains the IVF index object keys.
	IVFKeys *IVFKeys `json:"ivf_keys,omitempty"`

	// FilterKeys is a list of object storage keys for filter indexes (roaring bitmaps).
	FilterKeys []string `json:"filter_keys,omitempty"`

	// FTSKeys is a list of object storage keys for full-text search indexes.
	FTSKeys []string `json:"fts_keys,omitempty"`

	// Stats contains statistics about this segment.
	Stats SegmentStats `json:"stats"`

	// CreatedAt is when this segment was created.
	CreatedAt time.Time `json:"created_at"`
}

// IVFKeys contains the object storage keys for IVF index components.
// These follow the file format defined in the spec:
// - centroids: small file containing cluster centroids (cacheable in RAM)
// - cluster_offsets: small file mapping clusters to offset/length in cluster data
// - cluster_data: large file with packed cluster vectors
type IVFKeys struct {
	// CentroidsKey is the object key for vectors.centroids.bin
	CentroidsKey string `json:"centroids_key,omitempty"`

	// ClusterOffsetsKey is the object key for vectors.cluster_offsets.bin
	ClusterOffsetsKey string `json:"cluster_offsets_key,omitempty"`

	// ClusterDataKey is the object key for vectors.clusters.pack
	ClusterDataKey string `json:"cluster_data_key,omitempty"`

	// NClusters is the number of clusters in the index.
	NClusters int `json:"n_clusters,omitempty"`

	// VectorCount is the total number of vectors indexed.
	VectorCount int `json:"vector_count,omitempty"`
}

// HasIVF returns true if IVF keys are present and valid.
func (k *IVFKeys) HasIVF() bool {
	return k != nil && k.CentroidsKey != "" && k.ClusterOffsetsKey != "" && k.ClusterDataKey != ""
}

// AllKeys returns all object storage keys in the IVF structure.
func (k *IVFKeys) AllKeys() []string {
	if k == nil {
		return nil
	}
	var keys []string
	if k.CentroidsKey != "" {
		keys = append(keys, k.CentroidsKey)
	}
	if k.ClusterOffsetsKey != "" {
		keys = append(keys, k.ClusterOffsetsKey)
	}
	if k.ClusterDataKey != "" {
		keys = append(keys, k.ClusterDataKey)
	}
	return keys
}

// SegmentStats contains statistics for a single segment.
type SegmentStats struct {
	// RowCount is the number of rows in this segment.
	RowCount int64 `json:"row_count"`

	// LogicalBytes is the approximate size in bytes of the data in this segment.
	LogicalBytes int64 `json:"logical_bytes"`

	// TombstoneCount is the number of tombstones (deleted rows) in this segment.
	TombstoneCount int64 `json:"tombstone_count,omitempty"`
}

// Stats contains aggregate statistics about the entire index.
type Stats struct {
	// ApproxRowCount is the approximate total number of live rows.
	// This accounts for deduplication across segments.
	ApproxRowCount int64 `json:"approx_row_count"`

	// ApproxLogicalBytes is the approximate total size in bytes.
	ApproxLogicalBytes int64 `json:"approx_logical_bytes"`

	// SegmentCount is the total number of segments.
	SegmentCount int `json:"segment_count"`

	// L0SegmentCount is the number of L0 (uncompacted) segments.
	L0SegmentCount int `json:"l0_segment_count"`
}

// NewManifest creates a new manifest for a namespace.
func NewManifest(namespace string) *Manifest {
	return &Manifest{
		FormatVersion: CurrentManifestVersion,
		Namespace:     namespace,
		GeneratedAt:   time.Now().UTC(),
		Segments:      []Segment{},
		Stats:         Stats{},
	}
}

// AddSegment adds a segment to the manifest and updates stats.
func (m *Manifest) AddSegment(seg Segment) {
	m.Segments = append(m.Segments, seg)
	m.updateStats()
}

// RemoveSegment removes a segment by ID and updates stats.
func (m *Manifest) RemoveSegment(segID string) bool {
	for i, seg := range m.Segments {
		if seg.ID == segID {
			m.Segments = append(m.Segments[:i], m.Segments[i+1:]...)
			m.updateStats()
			return true
		}
	}
	return false
}

// GetSegment returns a segment by ID, or nil if not found.
func (m *Manifest) GetSegment(segID string) *Segment {
	for i := range m.Segments {
		if m.Segments[i].ID == segID {
			return &m.Segments[i]
		}
	}
	return nil
}

// GetSegmentsByLevel returns all segments at a given level.
func (m *Manifest) GetSegmentsByLevel(level int) []Segment {
	var result []Segment
	for _, seg := range m.Segments {
		if seg.Level == level {
			result = append(result, seg)
		}
	}
	return result
}

// SortSegmentsByWALSeq sorts segments by their end WAL sequence (newest first).
// This is used for query execution to process newest data first.
func (m *Manifest) SortSegmentsByWALSeq() {
	sort.Slice(m.Segments, func(i, j int) bool {
		return m.Segments[i].EndWALSeq > m.Segments[j].EndWALSeq
	})
}

// updateStats recalculates aggregate statistics from segments.
func (m *Manifest) updateStats() {
	m.Stats = Stats{}
	for _, seg := range m.Segments {
		m.Stats.SegmentCount++
		if seg.Level == 0 {
			m.Stats.L0SegmentCount++
		}
		m.Stats.ApproxRowCount += seg.Stats.RowCount
		m.Stats.ApproxLogicalBytes += seg.Stats.LogicalBytes
	}
}

// UpdateIndexedWALSeq updates the indexed WAL sequence to the highest segment end.
func (m *Manifest) UpdateIndexedWALSeq() {
	var maxSeq uint64
	for _, seg := range m.Segments {
		if seg.EndWALSeq > maxSeq {
			maxSeq = seg.EndWALSeq
		}
	}
	m.IndexedWALSeq = maxSeq
}

// WALRangeCovered returns true if the given WAL sequence is covered by the index.
func (m *Manifest) WALRangeCovered(seq uint64) bool {
	return seq <= m.IndexedWALSeq
}

// ComputeSchemaHash computes a hash of the schema for versioning.
func ComputeSchemaHash(schema interface{}) (string, error) {
	data, err := json.Marshal(schema)
	if err != nil {
		return "", fmt.Errorf("failed to marshal schema: %w", err)
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:8]), nil
}

// ManifestKey generates the object storage key for a manifest.
func ManifestKey(namespace string, seq uint64) string {
	return fmt.Sprintf("vex/namespaces/%s/index/manifests/%020d.idx.json", namespace, seq)
}

// SegmentKey generates the object storage key prefix for a segment.
func SegmentKey(namespace, segmentID string) string {
	return fmt.Sprintf("vex/namespaces/%s/index/segments/%s", namespace, segmentID)
}

// MarshalJSON serializes manifest to JSON.
func (m *Manifest) MarshalJSON() ([]byte, error) {
	type manifestAlias Manifest
	return json.Marshal((*manifestAlias)(m))
}

// UnmarshalJSON deserializes manifest from JSON.
func (m *Manifest) UnmarshalJSON(data []byte) error {
	type manifestAlias Manifest
	return json.Unmarshal(data, (*manifestAlias)(m))
}

// Clone creates a deep copy of the manifest.
func (m *Manifest) Clone() *Manifest {
	data, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	var clone Manifest
	if err := json.Unmarshal(data, &clone); err != nil {
		return nil
	}
	return &clone
}

// Validate checks if the manifest is valid.
func (m *Manifest) Validate() error {
	if m.FormatVersion < 1 {
		return fmt.Errorf("invalid format_version: %d", m.FormatVersion)
	}
	if m.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if m.GeneratedAt.IsZero() {
		return fmt.Errorf("generated_at is required")
	}

	// Validate segments
	seenIDs := make(map[string]bool)
	for i, seg := range m.Segments {
		if seg.ID == "" {
			return fmt.Errorf("segment %d: id is required", i)
		}
		if seenIDs[seg.ID] {
			return fmt.Errorf("segment %d: duplicate id %s", i, seg.ID)
		}
		seenIDs[seg.ID] = true

		if seg.StartWALSeq > seg.EndWALSeq {
			return fmt.Errorf("segment %s: start_wal_seq (%d) > end_wal_seq (%d)",
				seg.ID, seg.StartWALSeq, seg.EndWALSeq)
		}
	}

	return nil
}

// AllObjectKeys returns all object storage keys referenced by this manifest.
// This is used for GC to determine which objects are still reachable.
func (m *Manifest) AllObjectKeys() []string {
	var keys []string
	for _, seg := range m.Segments {
		if seg.DocsKey != "" {
			keys = append(keys, seg.DocsKey)
		}
		if seg.VectorsKey != "" {
			keys = append(keys, seg.VectorsKey)
		}
		// Include IVF keys if present
		keys = append(keys, seg.IVFKeys.AllKeys()...)
		keys = append(keys, seg.FilterKeys...)
		keys = append(keys, seg.FTSKeys...)
	}
	return keys
}
