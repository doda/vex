// Package index implements atomic index publishing protocol.
package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	ErrPublishAborted       = errors.New("publish was aborted")
	ErrSegmentUploadFailed  = errors.New("failed to upload segment objects")
	ErrManifestUploadFailed = errors.New("failed to upload manifest")
	ErrStateUpdateFailed    = errors.New("failed to update state")
	ErrObjectMissing        = errors.New("segment object missing from storage")
)

// Publisher handles atomic publishing of index segments and manifests.
// It ensures:
// 1. All segment objects are uploaded before the manifest
// 2. The manifest never references missing objects
// 3. state.json is updated only after successful manifest upload
// 4. indexed_wal_seq advances only on successful publish
type Publisher struct {
	store     objectstore.Store
	namespace string

	mu       sync.Mutex
	segments []*SegmentUpload
	manifest *Manifest
	sealed   bool
}

// SegmentUpload tracks segment data to be uploaded.
type SegmentUpload struct {
	Segment    Segment
	DocsData   []byte
	VectorData []byte
	FilterData map[string][]byte // attrName -> data
	FTSData    map[string][]byte // attrName -> data

	uploaded    bool
	uploadedMu  sync.Mutex
	writtenKeys []string
}

// NewPublisher creates a new publisher for atomic index publishing.
func NewPublisher(store objectstore.Store, namespace string) *Publisher {
	return &Publisher{
		store:     store,
		namespace: namespace,
	}
}

// AddSegment adds a segment to be published.
func (p *Publisher) AddSegment(upload *SegmentUpload) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.sealed {
		return ErrPublishAborted
	}

	p.segments = append(p.segments, upload)
	return nil
}

// SetManifest sets the manifest to be published.
func (p *Publisher) SetManifest(manifest *Manifest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.sealed {
		return ErrPublishAborted
	}

	p.manifest = manifest
	return nil
}

// PublishResult contains the result of a successful publish.
type PublishResult struct {
	ManifestKey      string
	ManifestSeq      uint64
	IndexedWALSeq    uint64
	UploadedSegments []string // Segment IDs
	UploadedObjects  []string // Object keys
	BytesUploaded    int64
}

// StateUpdater is called to update state.json after successful manifest upload.
// It receives the manifest key, manifest seq, and new indexed_wal_seq.
// Returns the new ETag if successful.
type StateUpdater func(ctx context.Context, manifestKey string, manifestSeq, indexedWALSeq uint64, bytesIndexed int64) (string, error)

// Publish executes the atomic publish protocol:
// 1. Upload all segment objects
// 2. Verify all objects exist
// 3. Upload manifest
// 4. Update state.json via CAS
func (p *Publisher) Publish(ctx context.Context, manifestSeq uint64, stateUpdater StateUpdater) (*PublishResult, error) {
	p.mu.Lock()
	if p.sealed {
		p.mu.Unlock()
		return nil, ErrPublishAborted
	}
	p.sealed = true
	segments := p.segments
	manifest := p.manifest
	p.mu.Unlock()

	if manifest == nil {
		return nil, errors.New("manifest not set")
	}

	result := &PublishResult{
		ManifestSeq:   manifestSeq,
		IndexedWALSeq: manifest.IndexedWALSeq,
	}

	// Step 1: Upload all segment objects first
	for _, seg := range segments {
		if err := p.uploadSegmentObjects(ctx, seg); err != nil {
			return nil, fmt.Errorf("%w: segment %s: %v", ErrSegmentUploadFailed, seg.Segment.ID, err)
		}
		p.syncManifestSegment(manifest, seg.Segment)
		result.UploadedSegments = append(result.UploadedSegments, seg.Segment.ID)
		result.UploadedObjects = append(result.UploadedObjects, seg.writtenKeys...)
		for _, data := range [][]byte{seg.DocsData, seg.VectorData} {
			result.BytesUploaded += int64(len(data))
		}
		for _, data := range seg.FilterData {
			result.BytesUploaded += int64(len(data))
		}
		for _, data := range seg.FTSData {
			result.BytesUploaded += int64(len(data))
		}
	}

	// Step 2: Verify all segment objects exist before writing manifest
	allKeys := manifest.AllObjectKeys()
	if err := p.verifyObjectsExist(ctx, allKeys); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrObjectMissing, err)
	}

	// Step 3: Upload manifest
	manifestKey := ManifestKey(p.namespace, manifestSeq)
	if err := p.uploadManifest(ctx, manifestKey, manifest); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrManifestUploadFailed, err)
	}
	result.ManifestKey = manifestKey

	// Step 4: Update state.json via CAS (only after manifest is uploaded)
	if stateUpdater != nil {
		_, err := stateUpdater(ctx, manifestKey, manifestSeq, manifest.IndexedWALSeq, result.BytesUploaded)
		if err != nil {
			// State update failed - manifest is orphaned but that's safe
			// (will be GC'd eventually as unreferenced)
			return nil, fmt.Errorf("%w: %v", ErrStateUpdateFailed, err)
		}
	}

	return result, nil
}

// uploadSegmentObjects uploads all data for a segment.
func (p *Publisher) uploadSegmentObjects(ctx context.Context, seg *SegmentUpload) error {
	seg.uploadedMu.Lock()
	defer seg.uploadedMu.Unlock()

	if seg.uploaded {
		return nil
	}

	writer := NewSegmentWriter(p.store, p.namespace, seg.Segment.ID)

	// Upload docs data
	if len(seg.DocsData) > 0 {
		key, err := writer.WriteDocsData(ctx, seg.DocsData)
		if err != nil {
			return fmt.Errorf("failed to upload docs: %w", err)
		}
		seg.Segment.DocsKey = key
		seg.writtenKeys = append(seg.writtenKeys, key)
	}

	// Upload vector data
	if len(seg.VectorData) > 0 {
		key, err := writer.WriteVectorsData(ctx, seg.VectorData)
		if err != nil {
			return fmt.Errorf("failed to upload vectors: %w", err)
		}
		seg.Segment.VectorsKey = key
		seg.writtenKeys = append(seg.writtenKeys, key)
	}

	// Upload filter data
	for attrName, data := range seg.FilterData {
		if len(data) > 0 {
			key, err := writer.WriteFilterData(ctx, attrName, data)
			if err != nil {
				return fmt.Errorf("failed to upload filter %s: %w", attrName, err)
			}
			seg.Segment.FilterKeys = append(seg.Segment.FilterKeys, key)
			seg.writtenKeys = append(seg.writtenKeys, key)
		}
	}

	// Upload FTS data
	for attrName, data := range seg.FTSData {
		if len(data) > 0 {
			key, err := writer.WriteFTSData(ctx, attrName, data)
			if err != nil {
				return fmt.Errorf("failed to upload FTS %s: %w", attrName, err)
			}
			seg.Segment.FTSKeys = append(seg.Segment.FTSKeys, key)
			seg.writtenKeys = append(seg.writtenKeys, key)
		}
	}

	writer.Seal()
	seg.uploaded = true
	return nil
}

// verifyObjectsExist checks that all referenced objects exist in storage.
func (p *Publisher) verifyObjectsExist(ctx context.Context, keys []string) error {
	for _, key := range keys {
		_, err := p.store.Head(ctx, key)
		if err != nil {
			if objectstore.IsNotFoundError(err) {
				return fmt.Errorf("object not found: %s", key)
			}
			return fmt.Errorf("failed to verify object %s: %w", key, err)
		}
	}
	return nil
}

// uploadManifest uploads the manifest to object storage.
func (p *Publisher) uploadManifest(ctx context.Context, key string, manifest *Manifest) error {
	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	_, err = p.store.PutIfAbsent(ctx, key, bytes.NewReader(data), int64(len(data)), &objectstore.PutOptions{
		ContentType: "application/json",
	})
	if err != nil {
		// If already exists, that's okay - idempotent publish
		if !objectstore.IsConflictError(err) {
			return err
		}
	}
	return nil
}

func (p *Publisher) syncManifestSegment(manifest *Manifest, seg Segment) {
	if manifest == nil {
		return
	}

	existing := manifest.GetSegment(seg.ID)
	if existing == nil {
		manifest.AddSegment(seg)
		return
	}

	// Fill in missing object keys without overriding explicitly set ones.
	if existing.DocsKey == "" {
		existing.DocsKey = seg.DocsKey
	}
	if existing.VectorsKey == "" {
		existing.VectorsKey = seg.VectorsKey
	}
	if len(existing.FilterKeys) == 0 {
		existing.FilterKeys = seg.FilterKeys
	}
	if len(existing.FTSKeys) == 0 {
		existing.FTSKeys = seg.FTSKeys
	}
}

// PublishConfig holds configuration for the publish operation.
type PublishConfig struct {
	VerifyObjects bool // Whether to verify objects exist before manifest upload
	SkipStateUpdate bool // Skip state.json update (for testing)
}

// DefaultPublishConfig returns default publish configuration.
func DefaultPublishConfig() *PublishConfig {
	return &PublishConfig{
		VerifyObjects:   true,
		SkipStateUpdate: false,
	}
}

// PublishWithConfig executes publish with custom configuration.
func (p *Publisher) PublishWithConfig(ctx context.Context, manifestSeq uint64, stateUpdater StateUpdater, config *PublishConfig) (*PublishResult, error) {
	if config == nil {
		config = DefaultPublishConfig()
	}

	p.mu.Lock()
	if p.sealed {
		p.mu.Unlock()
		return nil, ErrPublishAborted
	}
	p.sealed = true
	segments := p.segments
	manifest := p.manifest
	p.mu.Unlock()

	if manifest == nil {
		return nil, errors.New("manifest not set")
	}

	result := &PublishResult{
		ManifestSeq:   manifestSeq,
		IndexedWALSeq: manifest.IndexedWALSeq,
	}

	// Step 1: Upload all segment objects first
	for _, seg := range segments {
		if err := p.uploadSegmentObjects(ctx, seg); err != nil {
			return nil, fmt.Errorf("%w: segment %s: %v", ErrSegmentUploadFailed, seg.Segment.ID, err)
		}
		p.syncManifestSegment(manifest, seg.Segment)
		result.UploadedSegments = append(result.UploadedSegments, seg.Segment.ID)
		result.UploadedObjects = append(result.UploadedObjects, seg.writtenKeys...)
		for _, data := range [][]byte{seg.DocsData, seg.VectorData} {
			result.BytesUploaded += int64(len(data))
		}
		for _, data := range seg.FilterData {
			result.BytesUploaded += int64(len(data))
		}
		for _, data := range seg.FTSData {
			result.BytesUploaded += int64(len(data))
		}
	}

	// Step 2: Optionally verify all segment objects exist before writing manifest
	if config.VerifyObjects {
		allKeys := manifest.AllObjectKeys()
		if err := p.verifyObjectsExist(ctx, allKeys); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrObjectMissing, err)
		}
	}

	// Step 3: Upload manifest
	manifestKey := ManifestKey(p.namespace, manifestSeq)
	if err := p.uploadManifest(ctx, manifestKey, manifest); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrManifestUploadFailed, err)
	}
	result.ManifestKey = manifestKey

	// Step 4: Update state.json via CAS (only after manifest is uploaded)
	if stateUpdater != nil && !config.SkipStateUpdate {
		_, err := stateUpdater(ctx, manifestKey, manifestSeq, manifest.IndexedWALSeq, result.BytesUploaded)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrStateUpdateFailed, err)
		}
	}

	return result, nil
}

// TransactionBuilder builds a publish transaction step by step.
type TransactionBuilder struct {
	publisher *Publisher
	manifest  *Manifest
}

// NewTransaction creates a new publish transaction builder.
func NewTransaction(store objectstore.Store, namespace string) *TransactionBuilder {
	return &TransactionBuilder{
		publisher: NewPublisher(store, namespace),
		manifest:  NewManifest(namespace),
	}
}

// WithSegment adds a segment to the transaction.
func (t *TransactionBuilder) WithSegment(seg *SegmentUpload) *TransactionBuilder {
	t.publisher.AddSegment(seg)
	t.manifest.AddSegment(seg.Segment)
	return t
}

// WithExistingSegments adds existing segments to the manifest (not uploaded).
func (t *TransactionBuilder) WithExistingSegments(segments []Segment) *TransactionBuilder {
	for _, seg := range segments {
		t.manifest.AddSegment(seg)
	}
	return t
}

// WithSchemaHash sets the schema version hash.
func (t *TransactionBuilder) WithSchemaHash(hash string) *TransactionBuilder {
	t.manifest.SchemaVersionHash = hash
	return t
}

// Build finalizes the transaction and returns the publisher.
func (t *TransactionBuilder) Build() (*Publisher, *Manifest, error) {
	t.manifest.UpdateIndexedWALSeq()
	if err := t.manifest.Validate(); err != nil {
		return nil, nil, err
	}
	if err := t.publisher.SetManifest(t.manifest); err != nil {
		return nil, nil, err
	}
	return t.publisher, t.manifest, nil
}

// VerifyManifestReferences checks that all objects referenced by a manifest exist.
func VerifyManifestReferences(ctx context.Context, store objectstore.Store, manifest *Manifest) error {
	keys := manifest.AllObjectKeys()
	for _, key := range keys {
		_, err := store.Head(ctx, key)
		if err != nil {
			if objectstore.IsNotFoundError(err) {
				return fmt.Errorf("%w: %s", ErrObjectMissing, key)
			}
			return fmt.Errorf("failed to verify %s: %w", key, err)
		}
	}
	return nil
}

// LoadManifest loads a manifest from object storage.
func LoadManifest(ctx context.Context, store objectstore.Store, namespace string, seq uint64) (*Manifest, error) {
	key := ManifestKey(namespace, seq)
	reader, _, err := store.Get(ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}
	defer reader.Close()

	var manifest Manifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("failed to decode manifest: %w", err)
	}

	return &manifest, nil
}
