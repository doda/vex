// Package gc implements background garbage collection for deleted namespaces.
package gc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	ErrGCInProgress = errors.New("garbage collection already in progress")
	ErrGCStopped    = errors.New("garbage collection stopped")
)

// Config configures the garbage collector.
type Config struct {
	// ScanInterval is how often to scan for deleted namespaces.
	ScanInterval time.Duration

	// MinTombstoneAge is the minimum age a tombstone must have before GC runs.
	// This allows time for in-flight operations to complete.
	MinTombstoneAge time.Duration

	// ObjectBatchSize is how many objects to delete per batch.
	ObjectBatchSize int

	// PreserveTombstone if true, keeps the tombstone.json file after GC.
	// This is used for fast rejection of requests to deleted namespaces.
	PreserveTombstone bool

	// OldFormatRetentionDays is how long to retain segments with old format versions.
	// After this period, old format segments are eligible for GC during upgrades.
	// Default is 7 days, allowing ample time for rolling upgrades to complete.
	OldFormatRetentionDays int
}

// DefaultConfig returns a sensible default configuration.
func DefaultConfig() *Config {
	return &Config{
		ScanInterval:           5 * time.Minute,
		MinTombstoneAge:        24 * time.Hour,
		ObjectBatchSize:        100,
		PreserveTombstone:      true, // Keep tombstone for fast rejection
		OldFormatRetentionDays: 7,    // 7 days retention for old format segments
	}
}

// Result contains the result of a GC operation for a single namespace.
type Result struct {
	Namespace string

	// Objects deleted categorized by type
	WALObjectsDeleted   []string
	IndexObjectsDeleted []string
	MetaObjectsDeleted  []string

	// TotalDeleted is the total number of objects deleted.
	TotalDeleted int

	// Errors encountered during GC.
	Errors []error

	// Duration of the GC operation.
	Duration time.Duration

	// TombstonePreserved indicates if the tombstone was kept.
	TombstonePreserved bool
}

// Collector is the garbage collector that cleans up deleted namespaces.
type Collector struct {
	store  objectstore.Store
	config *Config

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewCollector creates a new garbage collector.
func NewCollector(store objectstore.Store, config *Config) *Collector {
	if config == nil {
		config = DefaultConfig()
	}
	return &Collector{
		store:  store,
		config: config,
	}
}

// Start begins the background GC loop.
func (c *Collector) Start(ctx context.Context) error {
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

// Stop stops the background GC loop.
func (c *Collector) Stop() {
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

// runLoop is the main GC loop.
func (c *Collector) runLoop(ctx context.Context) {
	defer close(c.doneCh)

	ticker := time.NewTicker(c.config.ScanInterval)
	defer ticker.Stop()

	// Run immediately on start
	c.scanAndCollect(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.scanAndCollect(ctx)
		}
	}
}

// scanAndCollect scans for deleted namespaces and runs GC on them.
func (c *Collector) scanAndCollect(ctx context.Context) {
	tombstones, err := c.findTombstonedNamespaces(ctx)
	if err != nil {
		return
	}

	for _, ns := range tombstones {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		// Check if tombstone is old enough
		if time.Since(ns.DeletedAt) < c.config.MinTombstoneAge {
			continue
		}

		_, _ = c.CollectNamespace(ctx, ns.Namespace)
	}
}

// tombstoneInfo contains info about a tombstoned namespace.
type tombstoneInfo struct {
	Namespace string
	DeletedAt time.Time
}

// findTombstonedNamespaces scans for namespaces with tombstone.json.
func (c *Collector) findTombstonedNamespaces(ctx context.Context) ([]tombstoneInfo, error) {
	var tombstones []tombstoneInfo
	marker := ""
	prefix := "vex/namespaces/"

	for {
		result, err := c.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list namespaces: %w", err)
		}

		// Look for tombstone.json files
		for _, obj := range result.Objects {
			if !strings.HasSuffix(obj.Key, "/meta/tombstone.json") {
				continue
			}

			// Extract namespace from key
			parts := strings.Split(obj.Key, "/")
			if len(parts) < 4 {
				continue
			}
			ns := parts[2] // vex/namespaces/<namespace>/meta/tombstone.json

			// Load tombstone to get deletion time
			ts, err := c.loadTombstone(ctx, ns)
			if err != nil {
				continue
			}

			tombstones = append(tombstones, tombstoneInfo{
				Namespace: ns,
				DeletedAt: ts.DeletedAt,
			})
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	return tombstones, nil
}

// loadTombstone loads a namespace's tombstone.
func (c *Collector) loadTombstone(ctx context.Context, ns string) (*namespace.Tombstone, error) {
	key := namespace.TombstoneKey(ns)
	reader, _, err := c.store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var ts namespace.Tombstone
	if err := json.Unmarshal(data, &ts); err != nil {
		return nil, err
	}

	return &ts, nil
}

// CollectNamespace performs GC on a single namespace.
func (c *Collector) CollectNamespace(ctx context.Context, ns string) (*Result, error) {
	start := time.Now()
	result := &Result{
		Namespace: ns,
	}

	// Verify namespace is actually tombstoned
	ts, err := c.loadTombstone(ctx, ns)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return nil, fmt.Errorf("namespace %s has no tombstone", ns)
		}
		return nil, fmt.Errorf("failed to load tombstone: %w", err)
	}

	// Check tombstone age
	if time.Since(ts.DeletedAt) < c.config.MinTombstoneAge {
		return nil, fmt.Errorf("tombstone too recent: %v old, need %v", time.Since(ts.DeletedAt), c.config.MinTombstoneAge)
	}

	// Delete WAL objects
	walDeleted, walErrs := c.deleteObjectsWithPrefix(ctx, fmt.Sprintf("vex/namespaces/%s/wal/", ns))
	result.WALObjectsDeleted = walDeleted
	result.Errors = append(result.Errors, walErrs...)

	// Delete index objects (segments, manifests)
	indexDeleted, indexErrs := c.deleteObjectsWithPrefix(ctx, fmt.Sprintf("vex/namespaces/%s/index/", ns))
	result.IndexObjectsDeleted = indexDeleted
	result.Errors = append(result.Errors, indexErrs...)

	// Delete meta objects (except tombstone if configured to preserve)
	metaDeleted, metaErrs := c.deleteMetaObjects(ctx, ns)
	result.MetaObjectsDeleted = metaDeleted
	result.Errors = append(result.Errors, metaErrs...)

	if err := c.deleteCatalogEntry(ctx, ns); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("failed to delete catalog entry: %w", err))
	}

	result.TotalDeleted = len(walDeleted) + len(indexDeleted) + len(metaDeleted)
	result.TombstonePreserved = c.config.PreserveTombstone
	result.Duration = time.Since(start)

	return result, nil
}

// deleteObjectsWithPrefix deletes all objects with the given prefix.
func (c *Collector) deleteObjectsWithPrefix(ctx context.Context, prefix string) ([]string, []error) {
	var deleted []string
	var errs []error
	marker := ""

	for {
		result, err := c.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: c.config.ObjectBatchSize,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to list %s: %w", prefix, err))
			break
		}

		for _, obj := range result.Objects {
			if err := c.store.Delete(ctx, obj.Key); err != nil {
				if !objectstore.IsNotFoundError(err) {
					errs = append(errs, fmt.Errorf("failed to delete %s: %w", obj.Key, err))
				}
			} else {
				deleted = append(deleted, obj.Key)
			}
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	return deleted, errs
}

// deleteMetaObjects deletes meta objects, optionally preserving tombstone.
func (c *Collector) deleteMetaObjects(ctx context.Context, ns string) ([]string, []error) {
	var deleted []string
	var errs []error
	prefix := fmt.Sprintf("vex/namespaces/%s/meta/", ns)
	tombstoneKey := namespace.TombstoneKey(ns)
	marker := ""

	for {
		result, err := c.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: c.config.ObjectBatchSize,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to list meta: %w", err))
			break
		}

		for _, obj := range result.Objects {
			// Skip tombstone if preserving
			if c.config.PreserveTombstone && obj.Key == tombstoneKey {
				continue
			}

			if err := c.store.Delete(ctx, obj.Key); err != nil {
				if !objectstore.IsNotFoundError(err) {
					errs = append(errs, fmt.Errorf("failed to delete %s: %w", obj.Key, err))
				}
			} else {
				deleted = append(deleted, obj.Key)
			}
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	return deleted, errs
}

func (c *Collector) deleteCatalogEntry(ctx context.Context, ns string) error {
	key := namespace.CatalogKey(ns)
	if err := c.store.Delete(ctx, key); err != nil {
		if objectstore.IsNotFoundError(err) {
			return nil
		}
		return err
	}
	return nil
}

// CleanupResult contains the result of a full GC pass.
type CleanupResult struct {
	NamespacesProcessed int
	TotalObjectsDeleted int
	Results             []*Result
	Duration            time.Duration
}

// RunFullCleanup performs a complete GC pass over all tombstoned namespaces.
func (c *Collector) RunFullCleanup(ctx context.Context) (*CleanupResult, error) {
	start := time.Now()
	result := &CleanupResult{}

	tombstones, err := c.findTombstonedNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	// Sort by deletion time (oldest first)
	sort.Slice(tombstones, func(i, j int) bool {
		return tombstones[i].DeletedAt.Before(tombstones[j].DeletedAt)
	})

	for _, ts := range tombstones {
		if time.Since(ts.DeletedAt) < c.config.MinTombstoneAge {
			continue
		}

		nsResult, err := c.CollectNamespace(ctx, ts.Namespace)
		if err != nil {
			continue
		}

		result.Results = append(result.Results, nsResult)
		result.TotalObjectsDeleted += nsResult.TotalDeleted
		result.NamespacesProcessed++
	}

	result.Duration = time.Since(start)
	return result, nil
}

// IsTombstonePreserved checks if a namespace's tombstone is preserved after GC.
func IsTombstonePreserved(ctx context.Context, store objectstore.Store, ns string) (bool, error) {
	key := namespace.TombstoneKey(ns)
	_, err := store.Head(ctx, key)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// HasRemainingObjects checks if a namespace has any remaining objects after GC.
func HasRemainingObjects(ctx context.Context, store objectstore.Store, ns string) (bool, error) {
	prefix := fmt.Sprintf("vex/namespaces/%s/", ns)
	tombstoneKey := namespace.TombstoneKey(ns)

	result, err := store.List(ctx, &objectstore.ListOptions{
		Prefix:  prefix,
		MaxKeys: 10,
	})
	if err != nil {
		return false, err
	}

	// Check if any objects exist besides the tombstone
	for _, obj := range result.Objects {
		if obj.Key != tombstoneKey {
			return true, nil
		}
	}

	return false, nil
}
