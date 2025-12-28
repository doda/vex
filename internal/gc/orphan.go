// Package gc implements garbage collection for orphan objects.
package gc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/pkg/objectstore"
)

const (
	// OrphanScanMarkerKey is the object key for the scan progress marker.
	OrphanScanMarkerKey = "vex/gc/orphan_scan_marker.json"

	// DefaultOrphanRetentionTime is the minimum age an object must have before
	// being considered orphaned (24 hours).
	DefaultOrphanRetentionTime = 24 * time.Hour
)

// OrphanScanMarker tracks the progress of orphan object scanning.
// This is persisted to object storage to resume scanning across restarts.
type OrphanScanMarker struct {
	// LastScanStarted is when the current or last scan started.
	LastScanStarted time.Time `json:"last_scan_started"`

	// LastScanCompleted is when the last full scan completed.
	LastScanCompleted *time.Time `json:"last_scan_completed,omitempty"`

	// CurrentNamespace is the namespace currently being scanned.
	// Empty string means the scan hasn't started or is between namespaces.
	CurrentNamespace string `json:"current_namespace,omitempty"`

	// CurrentMarker is the pagination marker within the current namespace.
	CurrentMarker string `json:"current_marker,omitempty"`

	// ObjectsScanned is the total number of objects scanned in this pass.
	ObjectsScanned int64 `json:"objects_scanned"`

	// OrphansFound is the number of orphans found in this pass.
	OrphansFound int64 `json:"orphans_found"`

	// OrphansDeleted is the number of orphans deleted in this pass.
	OrphansDeleted int64 `json:"orphans_deleted"`
}

// OrphanConfig configures the orphan collector.
type OrphanConfig struct {
	// ScanInterval is how often to scan for orphans.
	ScanInterval time.Duration

	// RetentionTime is the minimum age before deletion (default 24h).
	RetentionTime time.Duration

	// BatchSize is how many objects to scan per batch.
	BatchSize int

	// DryRun if true, only reports orphans without deleting them.
	DryRun bool
}

// DefaultOrphanConfig returns sensible defaults for orphan collection.
func DefaultOrphanConfig() *OrphanConfig {
	return &OrphanConfig{
		ScanInterval:  6 * time.Hour, // Scan every 6 hours
		RetentionTime: DefaultOrphanRetentionTime,
		BatchSize:     1000,
		DryRun:        false,
	}
}

// OrphanResult contains the result of an orphan GC operation.
type OrphanResult struct {
	Namespace string

	// Objects that were identified as orphans.
	OrphanObjects []string

	// Objects that were deleted (if not DryRun).
	DeletedObjects []string

	// Errors encountered during the operation.
	Errors []error

	// ObjectsScanned is the number of objects examined.
	ObjectsScanned int

	// Duration of the operation.
	Duration time.Duration
}

// OrphanCollector scans for and removes orphan objects.
type OrphanCollector struct {
	store  objectstore.Store
	config *OrphanConfig

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewOrphanCollector creates a new orphan collector.
func NewOrphanCollector(store objectstore.Store, config *OrphanConfig) *OrphanCollector {
	if config == nil {
		config = DefaultOrphanConfig()
	}
	return &OrphanCollector{
		store:  store,
		config: config,
	}
}

// Start begins the background orphan collection loop.
func (c *OrphanCollector) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return ErrGCInProgress
	}
	c.running = true
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	c.mu.Unlock()

	go c.runLoop(ctx)
	return nil
}

// Stop stops the background orphan collection loop.
func (c *OrphanCollector) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	close(c.stopCh)
	<-c.doneCh

	c.mu.Lock()
	c.running = false
	c.mu.Unlock()
}

// runLoop is the main orphan collection loop.
func (c *OrphanCollector) runLoop(ctx context.Context) {
	defer close(c.doneCh)

	ticker := time.NewTicker(c.config.ScanInterval)
	defer ticker.Stop()

	// Run immediately on start
	c.scanAllNamespaces(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.scanAllNamespaces(ctx)
		}
	}
}

// scanAllNamespaces scans all namespaces for orphan objects.
func (c *OrphanCollector) scanAllNamespaces(ctx context.Context) {
	// Load or create scan marker
	marker, _ := c.loadScanMarker(ctx)
	if marker == nil {
		marker = &OrphanScanMarker{
			LastScanStarted: time.Now(),
		}
	} else if marker.LastScanCompleted != nil {
		// Start a new scan pass
		marker = &OrphanScanMarker{
			LastScanStarted: time.Now(),
		}
	}

	// Find all namespaces
	namespaces, err := c.listNamespaces(ctx)
	if err != nil {
		return
	}

	// Resume from last position if applicable
	startIdx := 0
	if marker.CurrentNamespace != "" {
		for i, ns := range namespaces {
			if ns == marker.CurrentNamespace {
				startIdx = i
				break
			}
		}
	}

	// Scan each namespace
	for i := startIdx; i < len(namespaces); i++ {
		ns := namespaces[i]

		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		marker.CurrentNamespace = ns
		c.saveScanMarker(ctx, marker)

		result, _ := c.CollectNamespaceOrphans(ctx, ns)
		if result != nil {
			marker.ObjectsScanned += int64(result.ObjectsScanned)
			marker.OrphansFound += int64(len(result.OrphanObjects))
			marker.OrphansDeleted += int64(len(result.DeletedObjects))
		}
	}

	// Mark scan as complete
	now := time.Now()
	marker.LastScanCompleted = &now
	marker.CurrentNamespace = ""
	marker.CurrentMarker = ""
	c.saveScanMarker(ctx, marker)
}

// listNamespaces lists all namespaces in the object store.
func (c *OrphanCollector) listNamespaces(ctx context.Context) ([]string, error) {
	var namespaces []string
	marker := ""
	seen := make(map[string]bool)

	for {
		result, err := c.store.List(ctx, &objectstore.ListOptions{
			Prefix:  "vex/namespaces/",
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list namespaces: %w", err)
		}

		for _, obj := range result.Objects {
			// Extract namespace from key: vex/namespaces/<namespace>/...
			parts := strings.Split(obj.Key, "/")
			if len(parts) >= 3 {
				ns := parts[2]
				if !seen[ns] {
					seen[ns] = true
					namespaces = append(namespaces, ns)
				}
			}
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	return namespaces, nil
}

// CollectNamespaceOrphans scans a namespace for orphan objects and removes them.
func (c *OrphanCollector) CollectNamespaceOrphans(ctx context.Context, namespace string) (*OrphanResult, error) {
	start := time.Now()
	result := &OrphanResult{
		Namespace: namespace,
	}

	// Check if namespace is tombstoned (skip if so - handled by namespace GC)
	if c.isNamespaceTombstoned(ctx, namespace) {
		return result, nil
	}

	// Load the active manifest to get referenced objects
	referencedKeys, err := c.getReferencedKeys(ctx, namespace)
	if err != nil {
		// If we can't safely determine references, avoid deleting anything.
		result.Errors = append(result.Errors, err)
		result.Duration = time.Since(start)
		return result, err
	}

	// Scan segment objects
	segmentPrefix := fmt.Sprintf("vex/namespaces/%s/index/segments/", namespace)
	orphanSegments, scanned, scanErrs := c.scanForOrphans(ctx, segmentPrefix, referencedKeys)
	result.OrphanObjects = append(result.OrphanObjects, orphanSegments...)
	result.ObjectsScanned += scanned
	result.Errors = append(result.Errors, scanErrs...)

	// Scan manifest objects (find old/orphaned manifests)
	manifestPrefix := fmt.Sprintf("vex/namespaces/%s/index/manifests/", namespace)
	orphanManifests, scanned, scanErrs := c.scanForOrphanManifests(ctx, namespace, manifestPrefix)
	result.OrphanObjects = append(result.OrphanObjects, orphanManifests...)
	result.ObjectsScanned += scanned
	result.Errors = append(result.Errors, scanErrs...)

	// Delete orphans if not dry run
	if !c.config.DryRun {
		for _, key := range result.OrphanObjects {
			if err := c.store.Delete(ctx, key); err != nil {
				if !objectstore.IsNotFoundError(err) {
					result.Errors = append(result.Errors, fmt.Errorf("failed to delete %s: %w", key, err))
					continue
				}
			}
			result.DeletedObjects = append(result.DeletedObjects, key)
		}
	}

	result.Duration = time.Since(start)
	return result, nil
}

// isNamespaceTombstoned checks if a namespace has been deleted.
func (c *OrphanCollector) isNamespaceTombstoned(ctx context.Context, namespace string) bool {
	key := fmt.Sprintf("vex/namespaces/%s/meta/tombstone.json", namespace)
	_, err := c.store.Head(ctx, key)
	return err == nil
}

// getReferencedKeys returns all object keys referenced by the active manifest.
func (c *OrphanCollector) getReferencedKeys(ctx context.Context, namespace string) (map[string]bool, error) {
	// Load state.json to get active manifest
	stateKey := fmt.Sprintf("vex/namespaces/%s/meta/state.json", namespace)
	reader, _, err := c.store.Get(ctx, stateKey, nil)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return make(map[string]bool), nil
		}
		return nil, err
	}
	defer reader.Close()

	var state struct {
		Index struct {
			ManifestSeq uint64 `json:"manifest_seq"`
			ManifestKey string `json:"manifest_key"`
		} `json:"index"`
	}
	if err := json.NewDecoder(reader).Decode(&state); err != nil {
		return nil, err
	}

	if state.Index.ManifestSeq == 0 {
		return make(map[string]bool), nil
	}

	// Load the manifest
	manifest, err := index.LoadManifest(ctx, c.store, namespace, state.Index.ManifestSeq)
	if err != nil {
		return nil, err
	}

	// Build set of referenced keys
	keys := make(map[string]bool)
	for _, key := range manifest.AllObjectKeys() {
		keys[key] = true
	}
	// Also mark the active manifest itself as referenced
	keys[index.ManifestKey(namespace, state.Index.ManifestSeq)] = true

	return keys, nil
}

// scanForOrphans scans objects with the given prefix and finds orphans.
func (c *OrphanCollector) scanForOrphans(ctx context.Context, prefix string, referenced map[string]bool) ([]string, int, []error) {
	var orphans []string
	var errors []error
	var scanned int
	marker := ""
	now := time.Now()

	for {
		result, err := c.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: c.config.BatchSize,
		})
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to list %s: %w", prefix, err))
			break
		}

		for _, obj := range result.Objects {
			scanned++

			// Check if referenced by active manifest
			if referenced[obj.Key] {
				continue
			}

			// Check retention time (24h minimum)
			age := now.Sub(obj.LastModified)
			if age < c.config.RetentionTime {
				continue
			}

			orphans = append(orphans, obj.Key)
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	return orphans, scanned, errors
}

// scanForOrphanManifests finds manifests that are not the active one.
func (c *OrphanCollector) scanForOrphanManifests(ctx context.Context, namespace, prefix string) ([]string, int, []error) {
	var orphans []string
	var errors []error
	var scanned int
	marker := ""
	now := time.Now()

	// Get active manifest seq
	activeSeq := c.getActiveManifestSeq(ctx, namespace)
	activeKey := ""
	if activeSeq > 0 {
		activeKey = index.ManifestKey(namespace, activeSeq)
	}

	for {
		result, err := c.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: c.config.BatchSize,
		})
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to list manifests: %w", err))
			break
		}

		for _, obj := range result.Objects {
			scanned++

			// Skip active manifest
			if obj.Key == activeKey {
				continue
			}

			// Check retention time
			age := now.Sub(obj.LastModified)
			if age < c.config.RetentionTime {
				continue
			}

			orphans = append(orphans, obj.Key)
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	return orphans, scanned, errors
}

// getActiveManifestSeq returns the active manifest sequence for a namespace.
func (c *OrphanCollector) getActiveManifestSeq(ctx context.Context, namespace string) uint64 {
	stateKey := fmt.Sprintf("vex/namespaces/%s/meta/state.json", namespace)
	reader, _, err := c.store.Get(ctx, stateKey, nil)
	if err != nil {
		return 0
	}
	defer reader.Close()

	var state struct {
		Index struct {
			ManifestSeq uint64 `json:"manifest_seq"`
		} `json:"index"`
	}
	if err := json.NewDecoder(reader).Decode(&state); err != nil {
		return 0
	}
	return state.Index.ManifestSeq
}

// loadScanMarker loads the orphan scan marker from object storage.
func (c *OrphanCollector) loadScanMarker(ctx context.Context) (*OrphanScanMarker, error) {
	reader, _, err := c.store.Get(ctx, OrphanScanMarkerKey, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var marker OrphanScanMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return nil, err
	}

	return &marker, nil
}

// saveScanMarker saves the orphan scan marker to object storage.
func (c *OrphanCollector) saveScanMarker(ctx context.Context, marker *OrphanScanMarker) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	_, err = c.store.Put(ctx, OrphanScanMarkerKey, bytes.NewReader(data), int64(len(data)), nil)
	return err
}

// LoadOrphanScanMarker loads the scan marker from the given store.
// This is exported for testing and external monitoring.
func LoadOrphanScanMarker(ctx context.Context, store objectstore.Store) (*OrphanScanMarker, error) {
	reader, _, err := store.Get(ctx, OrphanScanMarkerKey, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var marker OrphanScanMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return nil, err
	}

	return &marker, nil
}

// GetScanMarker returns the current scan marker.
func (c *OrphanCollector) GetScanMarker(ctx context.Context) (*OrphanScanMarker, error) {
	return c.loadScanMarker(ctx)
}
