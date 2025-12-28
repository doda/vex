// Package gc implements garbage collection for deleted namespaces and old format segments.
package gc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// FormatGCResult contains the result of old format segment GC for a namespace.
type FormatGCResult struct {
	Namespace string

	// Manifests with old format versions found.
	OldFormatManifests int

	// Segments deleted due to old format version.
	SegmentsDeleted []string

	// ManifestsDeleted is the list of old manifests that were deleted.
	ManifestsDeleted []string

	// SegmentsRetained is the count of segments retained (not yet old enough).
	SegmentsRetained int

	// Errors encountered during GC.
	Errors []error

	// Duration of the GC operation.
	Duration time.Duration
}

// SegmentFormatInfo contains version and age info for a segment.
type SegmentFormatInfo struct {
	ManifestKey     string
	ManifestVersion int
	SegmentID       string
	SegmentKeys     []string
	CreatedAt       time.Time
}

// CollectOldFormatSegments scans a namespace for old format segments and deletes
// those that have exceeded the retention window. This implements the upgrade
// strategy step 3: GC old format segments after retention window.
func (c *Collector) CollectOldFormatSegments(ctx context.Context, ns string) (*FormatGCResult, error) {
	start := time.Now()
	result := &FormatGCResult{
		Namespace: ns,
	}

	// Find all manifests for this namespace
	manifests, err := c.findNamespaceManifests(ctx, ns)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("failed to list manifests: %w", err))
		result.Duration = time.Since(start)
		return result, nil
	}

	currentVersion := index.CurrentManifestVersion
	retentionDays := c.config.OldFormatRetentionDays
	if retentionDays <= 0 {
		retentionDays = 7
	}
	retentionDuration := time.Duration(retentionDays) * 24 * time.Hour

	for _, manifestKey := range manifests {
		m, err := c.loadManifest(ctx, manifestKey)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("failed to load manifest %s: %w", manifestKey, err))
			continue
		}

		// Only process manifests with old format versions
		if m.FormatVersion >= currentVersion {
			continue
		}

		result.OldFormatManifests++

		// Check if manifest is old enough to be GCed
		age := time.Since(m.GeneratedAt)
		if age < retentionDuration {
			// Count segments that are retained
			result.SegmentsRetained += len(m.Segments)
			continue
		}

		// Manifest is old enough, delete all its segments
		for _, seg := range m.Segments {
			segKeys := collectSegmentKeys(seg)
			for _, key := range segKeys {
				if err := c.store.Delete(ctx, key); err != nil {
					if !objectstore.IsNotFoundError(err) {
						result.Errors = append(result.Errors, fmt.Errorf("failed to delete segment object %s: %w", key, err))
					}
				} else {
					result.SegmentsDeleted = append(result.SegmentsDeleted, key)
				}
			}
		}

		// Delete the old manifest itself
		if err := c.store.Delete(ctx, manifestKey); err != nil {
			if !objectstore.IsNotFoundError(err) {
				result.Errors = append(result.Errors, fmt.Errorf("failed to delete manifest %s: %w", manifestKey, err))
			}
		} else {
			result.ManifestsDeleted = append(result.ManifestsDeleted, manifestKey)
		}
	}

	result.Duration = time.Since(start)
	return result, nil
}

// findNamespaceManifests lists all manifest keys for a namespace.
func (c *Collector) findNamespaceManifests(ctx context.Context, ns string) ([]string, error) {
	var manifests []string
	prefix := fmt.Sprintf("vex/namespaces/%s/index/manifests/", ns)
	marker := ""

	for {
		result, err := c.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: c.config.ObjectBatchSize,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Objects {
			if strings.HasSuffix(obj.Key, ".idx.json") {
				manifests = append(manifests, obj.Key)
			}
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	return manifests, nil
}

// loadManifest reads and parses a manifest from object storage.
func (c *Collector) loadManifest(ctx context.Context, key string) (*index.Manifest, error) {
	reader, _, err := c.store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var m index.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}

	return &m, nil
}

// collectSegmentKeys returns all object storage keys for a segment.
func collectSegmentKeys(seg index.Segment) []string {
	var keys []string

	if seg.DocsKey != "" {
		keys = append(keys, seg.DocsKey)
	}
	if seg.VectorsKey != "" {
		keys = append(keys, seg.VectorsKey)
	}
	if seg.IVFKeys != nil {
		keys = append(keys, seg.IVFKeys.AllKeys()...)
	}
	keys = append(keys, seg.FilterKeys...)
	keys = append(keys, seg.FTSKeys...)

	return keys
}

// IsOldFormatSegmentEligibleForGC checks if a segment with an old format version
// is eligible for garbage collection based on the retention window.
func IsOldFormatSegmentEligibleForGC(segmentCreatedAt time.Time, retentionDays int) bool {
	if retentionDays <= 0 {
		retentionDays = 7
	}
	retentionDuration := time.Duration(retentionDays) * 24 * time.Hour
	return time.Since(segmentCreatedAt) >= retentionDuration
}

// GetOldFormatRetentionDuration returns the retention duration for old format segments.
func GetOldFormatRetentionDuration(retentionDays int) time.Duration {
	if retentionDays <= 0 {
		retentionDays = 7
	}
	return time.Duration(retentionDays) * 24 * time.Hour
}

// FormatRetentionPolicy represents the policy for retaining old format segments.
type FormatRetentionPolicy struct {
	// RetentionDays is how many days to retain old format segments.
	RetentionDays int

	// CurrentManifestVersion is the current manifest format version.
	CurrentManifestVersion int
}

// DefaultFormatRetentionPolicy returns the default retention policy.
func DefaultFormatRetentionPolicy() FormatRetentionPolicy {
	return FormatRetentionPolicy{
		RetentionDays:          7,
		CurrentManifestVersion: index.CurrentManifestVersion,
	}
}

// CanGCManifest returns true if a manifest with the given version and creation
// time is eligible for garbage collection.
func (p FormatRetentionPolicy) CanGCManifest(version int, createdAt time.Time) bool {
	// Only GC old format manifests
	if version >= p.CurrentManifestVersion {
		return false
	}

	// Check retention period
	retention := GetOldFormatRetentionDuration(p.RetentionDays)
	return time.Since(createdAt) >= retention
}
