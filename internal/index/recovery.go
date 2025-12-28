// Package index implements index recovery for partial builds.
package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	ErrRecoveryInProgress = errors.New("recovery is already in progress")
	ErrNoRecoveryNeeded   = errors.New("no recovery needed")
)

// RecoveryConfig configures the index recovery process.
type RecoveryConfig struct {
	// MinOrphanAge is the minimum age an object must have before being considered orphaned.
	// This prevents deleting objects that are still being referenced by in-flight operations.
	MinOrphanAge time.Duration

	// DryRun if true, only reports orphans without deleting them.
	DryRun bool

	// MaxObjectsPerScan limits how many objects to scan in one pass.
	MaxObjectsPerScan int
}

// DefaultRecoveryConfig returns the default recovery configuration.
func DefaultRecoveryConfig() *RecoveryConfig {
	return &RecoveryConfig{
		MinOrphanAge:      24 * time.Hour, // 24h retention before GC
		DryRun:            false,
		MaxObjectsPerScan: 10000,
	}
}

// RecoveryResult contains the result of a recovery operation.
type RecoveryResult struct {
	// Namespace is the namespace that was checked.
	Namespace string

	// OrphanSegmentObjects are segment objects not referenced by the active manifest.
	OrphanSegmentObjects []string

	// OrphanManifests are manifest files not referenced by state.json.
	OrphanManifests []string

	// DeletedObjects are objects that were deleted (if not DryRun).
	DeletedObjects []string

	// Errors contains any errors encountered during recovery.
	Errors []error

	// ScannedObjects is the total number of objects scanned.
	ScannedObjects int

	// ActiveManifestSeq is the sequence number of the active manifest.
	ActiveManifestSeq uint64

	// Duration is how long the recovery took.
	Duration time.Duration
}

// Recovery handles detection and cleanup of orphaned index objects.
type Recovery struct {
	store  objectstore.Store
	config *RecoveryConfig
}

// NewRecovery creates a new recovery handler.
func NewRecovery(store objectstore.Store, config *RecoveryConfig) *Recovery {
	if config == nil {
		config = DefaultRecoveryConfig()
	}
	return &Recovery{
		store:  store,
		config: config,
	}
}

// FindOrphanedObjects scans for orphaned index objects in a namespace.
// Objects are considered orphaned if:
// 1. They are segment objects not referenced by the active manifest
// 2. They are manifest files not referenced by state.json
// 3. They are older than MinOrphanAge
func (r *Recovery) FindOrphanedObjects(ctx context.Context, namespace string) (*RecoveryResult, error) {
	start := time.Now()
	result := &RecoveryResult{
		Namespace: namespace,
	}

	// Load the current state to get the active manifest
	activeManifest, activeSeq, err := r.loadActiveManifest(ctx, namespace)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			// No state file - namespace may be deleted or never existed
			// Scan for any orphaned objects anyway
			activeManifest = nil
		} else {
			return nil, fmt.Errorf("failed to load active manifest: %w", err)
		}
	}
	result.ActiveManifestSeq = activeSeq

	// Get all object keys referenced by the active manifest
	referencedKeys := make(map[string]bool)
	if activeManifest != nil {
		for _, key := range activeManifest.AllObjectKeys() {
			referencedKeys[key] = true
		}
		// Also mark the active manifest itself as referenced
		referencedKeys[ManifestKey(namespace, activeSeq)] = true
	}

	// Scan for segment objects
	segmentPrefix := fmt.Sprintf("vex/namespaces/%s/index/segments/", namespace)
	orphanSegments, scanned, err := r.scanForOrphans(ctx, segmentPrefix, referencedKeys)
	if err != nil {
		result.Errors = append(result.Errors, err)
	}
	result.OrphanSegmentObjects = orphanSegments
	result.ScannedObjects += scanned

	// Scan for manifest files
	manifestPrefix := fmt.Sprintf("vex/namespaces/%s/index/manifests/", namespace)
	orphanManifests, scanned, err := r.scanForOrphanManifests(ctx, manifestPrefix, activeSeq)
	if err != nil {
		result.Errors = append(result.Errors, err)
	}
	result.OrphanManifests = orphanManifests
	result.ScannedObjects += scanned

	result.Duration = time.Since(start)
	return result, nil
}

// CleanOrphanedObjects removes orphaned objects older than MinOrphanAge.
func (r *Recovery) CleanOrphanedObjects(ctx context.Context, namespace string) (*RecoveryResult, error) {
	result, err := r.FindOrphanedObjects(ctx, namespace)
	if err != nil {
		return nil, err
	}

	if r.config.DryRun {
		return result, nil
	}

	// Delete orphaned segment objects
	for _, key := range result.OrphanSegmentObjects {
		if err := r.store.Delete(ctx, key); err != nil {
			if !objectstore.IsNotFoundError(err) {
				result.Errors = append(result.Errors, fmt.Errorf("failed to delete %s: %w", key, err))
				continue
			}
		}
		result.DeletedObjects = append(result.DeletedObjects, key)
	}

	// Delete orphaned manifests (oldest first to maintain consistency)
	sort.Strings(result.OrphanManifests)
	for _, key := range result.OrphanManifests {
		if err := r.store.Delete(ctx, key); err != nil {
			if !objectstore.IsNotFoundError(err) {
				result.Errors = append(result.Errors, fmt.Errorf("failed to delete %s: %w", key, err))
				continue
			}
		}
		result.DeletedObjects = append(result.DeletedObjects, key)
	}

	return result, nil
}

// loadActiveManifest loads the manifest referenced by state.json.
func (r *Recovery) loadActiveManifest(ctx context.Context, namespace string) (*Manifest, uint64, error) {
	// Load state.json
	stateKey := fmt.Sprintf("vex/namespaces/%s/meta/state.json", namespace)
	reader, _, err := r.store.Get(ctx, stateKey, nil)
	if err != nil {
		return nil, 0, err
	}
	defer reader.Close()

	var state struct {
		Index struct {
			ManifestSeq uint64 `json:"manifest_seq"`
			ManifestKey string `json:"manifest_key"`
		} `json:"index"`
	}
	if err := json.NewDecoder(reader).Decode(&state); err != nil {
		return nil, 0, fmt.Errorf("failed to decode state: %w", err)
	}

	if state.Index.ManifestSeq == 0 || state.Index.ManifestKey == "" {
		// No manifest yet
		return nil, 0, nil
	}

	// Load the manifest
	manifest, err := LoadManifest(ctx, r.store, namespace, state.Index.ManifestSeq)
	if err != nil {
		return nil, state.Index.ManifestSeq, err
	}

	return manifest, state.Index.ManifestSeq, nil
}

// scanForOrphans scans objects with the given prefix and finds those not in the referenced set.
func (r *Recovery) scanForOrphans(ctx context.Context, prefix string, referenced map[string]bool) ([]string, int, error) {
	var orphans []string
	var scanned int
	var marker string
	now := time.Now()

	for {
		result, err := r.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return orphans, scanned, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range result.Objects {
			scanned++
			if scanned > r.config.MaxObjectsPerScan {
				break
			}

			// Check if object is referenced
			if referenced[obj.Key] {
				continue
			}

			// Check if object is old enough to be considered orphaned
			age := now.Sub(obj.LastModified)
			if age < r.config.MinOrphanAge {
				continue
			}

			orphans = append(orphans, obj.Key)
		}

		if !result.IsTruncated || result.NextMarker == "" || scanned > r.config.MaxObjectsPerScan {
			break
		}
		marker = result.NextMarker
	}

	return orphans, scanned, nil
}

// scanForOrphanManifests finds manifests that are not the active one.
func (r *Recovery) scanForOrphanManifests(ctx context.Context, prefix string, activeSeq uint64) ([]string, int, error) {
	var orphans []string
	var scanned int
	var marker string
	now := time.Now()
	activeKey := ""
	if activeSeq > 0 {
		activeKey = fmt.Sprintf("%020d.idx.json", activeSeq)
	}

	for {
		result, err := r.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return orphans, scanned, fmt.Errorf("failed to list manifests: %w", err)
		}

		for _, obj := range result.Objects {
			scanned++
			if scanned > r.config.MaxObjectsPerScan {
				break
			}

			// Check if this is the active manifest
			if activeKey != "" && strings.HasSuffix(obj.Key, activeKey) {
				continue
			}

			// Check if manifest is old enough
			age := now.Sub(obj.LastModified)
			if age < r.config.MinOrphanAge {
				continue
			}

			orphans = append(orphans, obj.Key)
		}

		if !result.IsTruncated || result.NextMarker == "" || scanned > r.config.MaxObjectsPerScan {
			break
		}
		marker = result.NextMarker
	}

	return orphans, scanned, nil
}

// IsOrphanedSegmentObject checks if a segment object key is orphaned.
// This is a quick check that doesn't scan all objects.
func (r *Recovery) IsOrphanedSegmentObject(ctx context.Context, namespace string, segmentKey string) (bool, error) {
	// Load active manifest
	manifest, _, err := r.loadActiveManifest(ctx, namespace)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return true, nil // No state = all objects are orphaned
		}
		return false, err
	}

	if manifest == nil {
		return true, nil // No manifest = segment is orphaned
	}

	// Check if key is referenced
	for _, key := range manifest.AllObjectKeys() {
		if key == segmentKey {
			return false, nil
		}
	}

	return true, nil
}

// RecoverFromCrash attempts to recover from a partial index build.
// It verifies the current state is consistent and cleans up any partial work.
func (r *Recovery) RecoverFromCrash(ctx context.Context, namespace string) (*RecoveryResult, error) {
	result := &RecoveryResult{
		Namespace: namespace,
	}
	start := time.Now()

	// First, verify the active manifest (if any) is consistent
	manifest, seq, err := r.loadActiveManifest(ctx, namespace)
	if err != nil && !objectstore.IsNotFoundError(err) {
		return nil, fmt.Errorf("failed to load manifest for recovery: %w", err)
	}
	result.ActiveManifestSeq = seq

	if manifest != nil {
		// Verify all referenced objects exist
		if err := VerifyManifestReferences(ctx, r.store, manifest); err != nil {
			// Active manifest references missing objects - this is a critical error
			// In production, this would trigger an alert
			result.Errors = append(result.Errors, fmt.Errorf("active manifest inconsistent: %w", err))
		}
	}

	// Find and optionally clean orphaned objects
	findResult, err := r.FindOrphanedObjects(ctx, namespace)
	if err != nil {
		result.Errors = append(result.Errors, err)
	} else {
		result.OrphanSegmentObjects = findResult.OrphanSegmentObjects
		result.OrphanManifests = findResult.OrphanManifests
		result.ScannedObjects = findResult.ScannedObjects
	}

	result.Duration = time.Since(start)
	return result, nil
}

// SegmentObjectAgeCheck verifies that segment objects are old enough before cleanup.
// This is a safety check to prevent deleting objects from in-flight operations.
func SegmentObjectAgeCheck(lastModified time.Time, minAge time.Duration) bool {
	return time.Since(lastModified) >= minAge
}

// OrphanScanner provides incremental scanning for orphaned objects.
type OrphanScanner struct {
	recovery  *Recovery
	namespace string
	marker    string
	done      bool
}

// NewOrphanScanner creates a scanner for incremental orphan detection.
func NewOrphanScanner(recovery *Recovery, namespace string) *OrphanScanner {
	return &OrphanScanner{
		recovery:  recovery,
		namespace: namespace,
	}
}

// ScanBatch scans the next batch of objects for orphans.
// Returns true if there are more objects to scan.
func (s *OrphanScanner) ScanBatch(ctx context.Context, batchSize int) ([]string, bool, error) {
	if s.done {
		return nil, false, nil
	}

	// Load referenced objects
	manifest, _, err := s.recovery.loadActiveManifest(ctx, s.namespace)
	if err != nil && !objectstore.IsNotFoundError(err) {
		return nil, false, err
	}

	referenced := make(map[string]bool)
	if manifest != nil {
		for _, key := range manifest.AllObjectKeys() {
			referenced[key] = true
		}
	}

	// Scan next batch
	prefix := fmt.Sprintf("vex/namespaces/%s/index/segments/", s.namespace)
	result, err := s.recovery.store.List(ctx, &objectstore.ListOptions{
		Prefix:  prefix,
		Marker:  s.marker,
		MaxKeys: batchSize,
	})
	if err != nil {
		return nil, false, err
	}

	now := time.Now()
	var orphans []string
	for _, obj := range result.Objects {
		if !referenced[obj.Key] && now.Sub(obj.LastModified) >= s.recovery.config.MinOrphanAge {
			orphans = append(orphans, obj.Key)
		}
	}

	if !result.IsTruncated || result.NextMarker == "" {
		s.done = true
		return orphans, false, nil
	}

	s.marker = result.NextMarker
	return orphans, true, nil
}

// IsDone returns true if scanning is complete.
func (s *OrphanScanner) IsDone() bool {
	return s.done
}
