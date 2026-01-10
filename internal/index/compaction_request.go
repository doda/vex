// Package index implements compaction request handling for single-writer manifest updates.
package index

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// CompactionRequest captures the output of a compaction so a single writer can publish it.
type CompactionRequest struct {
	ID               string    `json:"id"`
	Namespace        string    `json:"namespace"`
	CreatedAt        time.Time `json:"created_at"`
	BaseManifestSeq  uint64    `json:"base_manifest_seq"`
	BaseManifestKey  string    `json:"base_manifest_key"`
	SourceSegmentIDs []string  `json:"source_segment_ids"`
	NewSegment       Segment   `json:"new_segment"`
}

type CompactionApplyOptions struct {
	RetentionTime time.Duration
	MaxAttempts   int
}

type CompactionApplyResult struct {
	Applied      bool
	Superseded   bool
	ManifestSeq  uint64
	ManifestKey  string
	RequestKey   string
	SourceCount  int
	SegmentLevel int
}

// CompactionRequestID creates a deterministic request ID from a plan.
func CompactionRequestID(namespace string, targetLevel int, sourceSegments []Segment) string {
	ids := make([]string, 0, len(sourceSegments))
	for _, seg := range sourceSegments {
		if seg.ID != "" {
			ids = append(ids, seg.ID)
		}
	}
	sort.Strings(ids)
	payload := fmt.Sprintf("%s|%d|%s", namespace, targetLevel, strings.Join(ids, ","))
	hash := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(hash[:])
}

// CompactionRequestKey returns the object storage key for a compaction request.
func CompactionRequestKey(namespace, id string) string {
	return fmt.Sprintf("vex/namespaces/%s/index/compactions/%s.json", namespace, id)
}

// CompactionRequestPrefix returns the object storage prefix for compaction requests.
func CompactionRequestPrefix(namespace string) string {
	return fmt.Sprintf("vex/namespaces/%s/index/compactions/", namespace)
}

// NewCompactionRequest creates a compaction request payload.
func NewCompactionRequest(namespace string, baseSeq uint64, baseKey string, sourceSegments []Segment, newSegment Segment) *CompactionRequest {
	sourceIDs := make([]string, 0, len(sourceSegments))
	for _, seg := range sourceSegments {
		if seg.ID != "" {
			sourceIDs = append(sourceIDs, seg.ID)
		}
	}
	req := &CompactionRequest{
		ID:               CompactionRequestID(namespace, newSegment.Level, sourceSegments),
		Namespace:        namespace,
		CreatedAt:        time.Now().UTC(),
		BaseManifestSeq:  baseSeq,
		BaseManifestKey:  baseKey,
		SourceSegmentIDs: sourceIDs,
		NewSegment:       newSegment,
	}
	return req
}

// WriteCompactionRequest writes a compaction request to object storage.
// Returns the request key and whether it was newly written.
func WriteCompactionRequest(ctx context.Context, store objectstore.Store, req *CompactionRequest) (string, bool, error) {
	if store == nil {
		return "", false, errors.New("object store is nil")
	}
	if req == nil {
		return "", false, errors.New("compaction request is nil")
	}
	if req.ID == "" {
		return "", false, errors.New("compaction request missing ID")
	}
	if req.Namespace == "" {
		return "", false, errors.New("compaction request missing namespace")
	}
	key := CompactionRequestKey(req.Namespace, req.ID)
	data, err := json.Marshal(req)
	if err != nil {
		return "", false, fmt.Errorf("failed to serialize compaction request: %w", err)
	}
	_, err = store.PutIfAbsent(ctx, key, bytes.NewReader(data), int64(len(data)), &objectstore.PutOptions{
		ContentType: "application/json",
	})
	if err != nil {
		if objectstore.IsConflictError(err) {
			return key, false, nil
		}
		return "", false, fmt.Errorf("failed to write compaction request: %w", err)
	}
	return key, true, nil
}

// LoadCompactionRequest loads a compaction request from object storage.
func LoadCompactionRequest(ctx context.Context, store objectstore.Store, key string) (*CompactionRequest, error) {
	if store == nil {
		return nil, errors.New("object store is nil")
	}
	reader, _, err := store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var req CompactionRequest
	if err := json.NewDecoder(reader).Decode(&req); err != nil {
		return nil, fmt.Errorf("failed to decode compaction request: %w", err)
	}
	return &req, nil
}

// ListCompactionRequestKeys lists compaction request object keys for a namespace.
func ListCompactionRequestKeys(ctx context.Context, store objectstore.Store, namespace string, limit int) ([]string, error) {
	if store == nil {
		return nil, errors.New("object store is nil")
	}
	if limit <= 0 {
		limit = 100
	}

	prefix := CompactionRequestPrefix(namespace)
	var keys []string
	marker := ""
	for len(keys) < limit {
		result, err := store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: limit - len(keys),
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range result.Objects {
			if obj.Key == "" {
				continue
			}
			if strings.HasSuffix(obj.Key, "/") || !strings.HasSuffix(obj.Key, ".json") {
				continue
			}
			keys = append(keys, obj.Key)
			if len(keys) >= limit {
				break
			}
		}
		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}
	return keys, nil
}

// ApplyCompactionRequest applies a compaction request using state CAS and updates the manifest pointer.
func ApplyCompactionRequest(ctx context.Context, store objectstore.Store, stateMan *namespace.StateManager, req *CompactionRequest, opts CompactionApplyOptions) (*CompactionApplyResult, error) {
	if store == nil {
		return nil, errors.New("object store is nil")
	}
	if stateMan == nil {
		return nil, errors.New("state manager is nil")
	}
	if req == nil {
		return nil, errors.New("compaction request is nil")
	}
	if req.Namespace == "" {
		return nil, errors.New("compaction request missing namespace")
	}
	if req.ID == "" {
		return nil, errors.New("compaction request missing ID")
	}
	if len(req.SourceSegmentIDs) == 0 {
		return nil, errors.New("compaction request missing source segments")
	}
	if req.NewSegment.ID == "" {
		return nil, errors.New("compaction request missing new segment ID")
	}

	if opts.RetentionTime == 0 {
		opts.RetentionTime = DefaultCompactorConfig().RetentionTime
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}

	requestKey := CompactionRequestKey(req.Namespace, req.ID)
	result := &CompactionApplyResult{
		RequestKey:   requestKey,
		SourceCount:  len(req.SourceSegmentIDs),
		SegmentLevel: req.NewSegment.Level,
	}

	var lastErr error
	for attempt := 0; attempt < opts.MaxAttempts; attempt++ {
		loaded, err := stateMan.Load(ctx, req.Namespace)
		if err != nil {
			return nil, err
		}
		if loaded.State.Index.ManifestSeq == 0 || loaded.State.Index.ManifestKey == "" {
			return nil, fmt.Errorf("namespace %s has no manifest", req.Namespace)
		}

		manifest, err := LoadManifest(ctx, store, req.Namespace, loaded.State.Index.ManifestSeq)
		if err != nil {
			return nil, err
		}
		if manifest == nil {
			return nil, errors.New("manifest is nil")
		}

		if manifest.GetSegment(req.NewSegment.ID) != nil && !manifestHasSegmentIDs(manifest, req.SourceSegmentIDs) {
			result.Applied = true
			result.ManifestSeq = loaded.State.Index.ManifestSeq
			result.ManifestKey = loaded.State.Index.ManifestKey
			_ = store.Delete(ctx, requestKey)
			return result, nil
		}

		if !manifestHasSegmentIDs(manifest, req.SourceSegmentIDs) {
			result.Superseded = true
			result.ManifestSeq = loaded.State.Index.ManifestSeq
			result.ManifestKey = loaded.State.Index.ManifestKey
			_ = store.Delete(ctx, requestKey)
			if manifest.GetSegment(req.NewSegment.ID) == nil {
				_ = deleteSegmentObjects(ctx, store, req.NewSegment)
			}
			return result, nil
		}

		updated := manifest.Clone()
		for _, segID := range req.SourceSegmentIDs {
			updated.RemoveSegment(segID)
		}
		updated.AddSegment(req.NewSegment)
		updated.UpdateIndexedWALSeq()
		updated.GeneratedAt = time.Now().UTC()

		if err := updated.Validate(); err != nil {
			return nil, fmt.Errorf("manifest invalid after compaction apply: %w", err)
		}
		if err := VerifyManifestReferences(ctx, store, updated); err != nil {
			return nil, fmt.Errorf("manifest references missing objects: %w", err)
		}

		manifestData, err := updated.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize manifest: %w", err)
		}
		seq := loaded.State.Index.ManifestSeq + 1
		const maxSeqBumps = 5
		for bump := 0; bump < maxSeqBumps; bump++ {
			newManifestSeq := seq
			newManifestKey := ManifestKey(req.Namespace, newManifestSeq)

			_, err = store.PutIfAbsent(ctx, newManifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), &objectstore.PutOptions{
				ContentType: "application/json",
			})
			if err != nil {
				if objectstore.IsConflictError(err) {
					existing, loadErr := LoadManifest(ctx, store, req.Namespace, newManifestSeq)
					if loadErr != nil {
						lastErr = loadErr
						seq++
						continue
					}
					if existing != nil && manifestMatchesCompactionRequest(existing, req) {
						if _, err := stateMan.UpdateIndexManifest(ctx, req.Namespace, loaded.ETag, newManifestKey, newManifestSeq, existing.IndexedWALSeq); err != nil {
							if errors.Is(err, namespace.ErrCASRetryExhausted) {
								lastErr = err
								seq++
								continue
							}
							return nil, err
						}
						result.Applied = true
						result.ManifestSeq = newManifestSeq
						result.ManifestKey = newManifestKey
						_ = store.Delete(ctx, requestKey)
						_ = cleanupSegmentsAfterApply(ctx, store, req.SourceSegmentIDs, manifest, opts.RetentionTime)
						return result, nil
					}
					lastErr = err
					seq++
					continue
				}
				return nil, fmt.Errorf("failed to write manifest: %w", err)
			}

			if _, err := stateMan.UpdateIndexManifest(ctx, req.Namespace, loaded.ETag, newManifestKey, newManifestSeq, updated.IndexedWALSeq); err != nil {
				if errors.Is(err, namespace.ErrCASRetryExhausted) {
					lastErr = err
					seq++
					continue
				}
				return nil, err
			}

			result.Applied = true
			result.ManifestSeq = newManifestSeq
			result.ManifestKey = newManifestKey
			_ = store.Delete(ctx, requestKey)
			_ = cleanupSegmentsAfterApply(ctx, store, req.SourceSegmentIDs, manifest, opts.RetentionTime)
			return result, nil
		}
	}

	if lastErr != nil {
		return result, fmt.Errorf("compaction apply attempts exhausted for %s: %v", req.Namespace, lastErr)
	}
	return result, fmt.Errorf("compaction apply attempts exhausted for %s", req.Namespace)
}

func manifestHasSegmentIDs(manifest *Manifest, segmentIDs []string) bool {
	for _, segID := range segmentIDs {
		if segID == "" {
			continue
		}
		if manifest.GetSegment(segID) == nil {
			return false
		}
	}
	return true
}

func manifestMatchesCompactionRequest(manifest *Manifest, req *CompactionRequest) bool {
	if manifest.GetSegment(req.NewSegment.ID) == nil {
		return false
	}
	for _, segID := range req.SourceSegmentIDs {
		if segID == "" {
			continue
		}
		if manifest.GetSegment(segID) != nil {
			return false
		}
	}
	return true
}

func cleanupSegmentsAfterApply(ctx context.Context, store objectstore.Store, segmentIDs []string, manifest *Manifest, retention time.Duration) error {
	if manifest == nil {
		return nil
	}
	now := time.Now()
	for _, segID := range segmentIDs {
		seg := manifest.GetSegment(segID)
		if seg == nil {
			continue
		}
		if !segmentReadyForCleanup(ctx, store, *seg, now, retention) {
			continue
		}
		_ = deleteSegmentObjects(ctx, store, *seg)
	}
	return nil
}

func segmentReadyForCleanup(ctx context.Context, store objectstore.Store, seg Segment, now time.Time, retention time.Duration) bool {
	if retention <= 0 {
		return true
	}
	if !seg.CreatedAt.IsZero() {
		return now.Sub(seg.CreatedAt) >= retention
	}
	for _, key := range segmentObjectKeys(seg) {
		if key == "" {
			continue
		}
		info, err := store.Head(ctx, key)
		if err != nil {
			continue
		}
		return now.Sub(info.LastModified) >= retention
	}
	return false
}

func deleteSegmentObjects(ctx context.Context, store objectstore.Store, seg Segment) error {
	for _, key := range segmentObjectKeys(seg) {
		if key == "" {
			continue
		}
		if err := store.Delete(ctx, key); err != nil && !objectstore.IsNotFoundError(err) {
			return err
		}
	}
	return nil
}
