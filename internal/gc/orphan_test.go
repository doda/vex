package gc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// createNamespaceWithManifest creates a namespace with state.json and a manifest.
func createNamespaceWithManifest(t *testing.T, store objectstore.Store, ns string, manifestSeq uint64, segments []index.Segment) {
	ctx := context.Background()

	// Create the manifest
	manifest := index.NewManifest(ns)
	manifest.IndexedWALSeq = manifestSeq
	for _, seg := range segments {
		manifest.AddSegment(seg)
	}

	manifestKey := index.ManifestKey(ns, manifestSeq)
	manifestData, _ := json.Marshal(manifest)
	store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)

	// Create state.json pointing to the manifest
	state := namespace.NewState(ns)
	state.Index.ManifestSeq = manifestSeq
	state.Index.ManifestKey = manifestKey
	state.Index.IndexedWALSeq = manifestSeq
	stateData, _ := json.Marshal(state)
	stateKey := namespace.StateKey(ns)
	store.Put(ctx, stateKey, bytes.NewReader(stateData), int64(len(stateData)), nil)
}

// createOrphanSegmentObjects creates segment objects that are NOT referenced by any manifest.
func createOrphanSegmentObjects(t *testing.T, store objectstore.Store, ns string, keys []string) {
	ctx := context.Background()
	for _, key := range keys {
		data := []byte("orphan-segment-data")
		store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
	}
}

// createReferencedSegmentObjects creates segment objects referenced by the manifest.
func createReferencedSegmentObjects(t *testing.T, store objectstore.Store, keys []string) {
	ctx := context.Background()
	for _, key := range keys {
		data := []byte("referenced-segment-data")
		store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
	}
}

// TestOrphanGC_DetectsOrphanSegments verifies orphan segment objects are detected.
func TestOrphanGC_DetectsOrphanSegments(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "orphan-test-ns"

	// Create referenced segment keys
	referencedKeys := []string{
		fmt.Sprintf("vex/namespaces/%s/index/segments/seg-001.idx", ns),
		fmt.Sprintf("vex/namespaces/%s/index/segments/seg-002.idx", ns),
	}

	// Create a manifest that references these segments
	segments := []index.Segment{
		{ID: "seg-001", DocsKey: referencedKeys[0], StartWALSeq: 1, EndWALSeq: 5},
		{ID: "seg-002", DocsKey: referencedKeys[1], StartWALSeq: 6, EndWALSeq: 10},
	}
	createNamespaceWithManifest(t, store, ns, 1, segments)
	createReferencedSegmentObjects(t, store, referencedKeys)

	// Create orphan segment objects (not referenced by manifest)
	orphanKeys := []string{
		fmt.Sprintf("vex/namespaces/%s/index/segments/orphan-seg-001.idx", ns),
		fmt.Sprintf("vex/namespaces/%s/index/segments/orphan-seg-002.idx", ns),
	}
	createOrphanSegmentObjects(t, store, ns, orphanKeys)

	// Configure collector with zero retention time for testing
	config := &OrphanConfig{
		RetentionTime: 0, // No retention for testing
		BatchSize:     100,
		DryRun:        true, // Only detect, don't delete
	}
	collector := NewOrphanCollector(store, config)

	result, err := collector.CollectNamespaceOrphans(ctx, ns)
	if err != nil {
		t.Fatalf("CollectNamespaceOrphans failed: %v", err)
	}

	// Verify orphans were detected
	if len(result.OrphanObjects) != 2 {
		t.Errorf("expected 2 orphan objects, got %d", len(result.OrphanObjects))
	}

	// Verify correct objects were identified as orphans
	orphanSet := make(map[string]bool)
	for _, key := range result.OrphanObjects {
		orphanSet[key] = true
	}
	for _, key := range orphanKeys {
		if !orphanSet[key] {
			t.Errorf("expected %s to be detected as orphan", key)
		}
	}
	for _, key := range referencedKeys {
		if orphanSet[key] {
			t.Errorf("referenced key %s should not be detected as orphan", key)
		}
	}
}

// TestOrphanGC_RespectsRetentionTime verifies 24h retention before deletion.
func TestOrphanGC_RespectsRetentionTime(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "retention-test-ns"

	// Create state without a manifest (all segments are orphans)
	state := namespace.NewState(ns)
	stateData, _ := json.Marshal(state)
	stateKey := namespace.StateKey(ns)
	store.Put(ctx, stateKey, bytes.NewReader(stateData), int64(len(stateData)), nil)

	// Create orphan segment (will be "recent" due to just being created)
	orphanKey := fmt.Sprintf("vex/namespaces/%s/index/segments/recent-orphan.idx", ns)
	data := []byte("orphan-data")
	store.Put(ctx, orphanKey, bytes.NewReader(data), int64(len(data)), nil)

	// Configure collector with 24h retention
	config := &OrphanConfig{
		RetentionTime: DefaultOrphanRetentionTime, // 24h
		BatchSize:     100,
		DryRun:        true,
	}
	collector := NewOrphanCollector(store, config)

	result, err := collector.CollectNamespaceOrphans(ctx, ns)
	if err != nil {
		t.Fatalf("CollectNamespaceOrphans failed: %v", err)
	}

	// Orphan should NOT be detected because it's too recent
	if len(result.OrphanObjects) != 0 {
		t.Errorf("expected 0 orphan objects (too recent), got %d", len(result.OrphanObjects))
	}

	// Now test with zero retention
	config.RetentionTime = 0
	collector = NewOrphanCollector(store, config)

	result, err = collector.CollectNamespaceOrphans(ctx, ns)
	if err != nil {
		t.Fatalf("CollectNamespaceOrphans failed: %v", err)
	}

	// Orphan should be detected with zero retention
	if len(result.OrphanObjects) != 1 {
		t.Errorf("expected 1 orphan object, got %d", len(result.OrphanObjects))
	}
}

// TestOrphanGC_DeletesOrphans verifies orphan objects are actually deleted.
func TestOrphanGC_DeletesOrphans(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "delete-orphan-ns"

	// Create state without a manifest
	state := namespace.NewState(ns)
	stateData, _ := json.Marshal(state)
	stateKey := namespace.StateKey(ns)
	store.Put(ctx, stateKey, bytes.NewReader(stateData), int64(len(stateData)), nil)

	// Create orphan segments
	orphanKeys := []string{
		fmt.Sprintf("vex/namespaces/%s/index/segments/orphan-1.idx", ns),
		fmt.Sprintf("vex/namespaces/%s/index/segments/orphan-2.idx", ns),
	}
	for _, key := range orphanKeys {
		data := []byte("orphan-data")
		store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
	}

	// Verify objects exist before GC
	for _, key := range orphanKeys {
		_, err := store.Head(ctx, key)
		if err != nil {
			t.Fatalf("orphan object %s should exist before GC", key)
		}
	}

	// Configure collector with zero retention and NOT dry run
	config := &OrphanConfig{
		RetentionTime: 0,
		BatchSize:     100,
		DryRun:        false, // Actually delete
	}
	collector := NewOrphanCollector(store, config)

	result, err := collector.CollectNamespaceOrphans(ctx, ns)
	if err != nil {
		t.Fatalf("CollectNamespaceOrphans failed: %v", err)
	}

	// Verify orphans were deleted
	if len(result.DeletedObjects) != 2 {
		t.Errorf("expected 2 deleted objects, got %d", len(result.DeletedObjects))
	}

	// Verify objects no longer exist
	for _, key := range orphanKeys {
		_, err := store.Head(ctx, key)
		if !objectstore.IsNotFoundError(err) {
			t.Errorf("orphan object %s should be deleted after GC", key)
		}
	}
}

// TestOrphanGC_ScanMarkerTracking verifies gc/orphan_scan_marker.json tracking.
func TestOrphanGC_ScanMarkerTracking(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create a few namespaces with state
	for _, ns := range []string{"ns-1", "ns-2", "ns-3"} {
		state := namespace.NewState(ns)
		stateData, _ := json.Marshal(state)
		stateKey := namespace.StateKey(ns)
		store.Put(ctx, stateKey, bytes.NewReader(stateData), int64(len(stateData)), nil)
	}

	// Configure collector
	config := &OrphanConfig{
		RetentionTime: 0,
		BatchSize:     100,
		DryRun:        true,
		ScanInterval:  time.Hour, // Long interval for test control
	}
	collector := NewOrphanCollector(store, config)

	// Run a scan
	collector.scanAllNamespaces(ctx)

	// Verify scan marker was created
	marker, err := LoadOrphanScanMarker(ctx, store)
	if err != nil {
		t.Fatalf("failed to load scan marker: %v", err)
	}

	// Marker should indicate scan completed
	if marker.LastScanCompleted == nil {
		t.Error("expected LastScanCompleted to be set")
	}

	if marker.LastScanStarted.IsZero() {
		t.Error("expected LastScanStarted to be set")
	}

	// After completion, current namespace should be empty
	if marker.CurrentNamespace != "" {
		t.Errorf("expected empty CurrentNamespace after completion, got %s", marker.CurrentNamespace)
	}

	// Verify marker key is correct
	_, err = store.Head(ctx, OrphanScanMarkerKey)
	if err != nil {
		t.Errorf("expected scan marker at %s", OrphanScanMarkerKey)
	}
}

// TestOrphanGC_SkipsTombstonedNamespaces verifies tombstoned namespaces are skipped.
func TestOrphanGC_SkipsTombstonedNamespaces(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "tombstoned-ns"

	// Create tombstoned namespace
	createTestNamespace(t, store, ns, true, 25*time.Hour)

	// Add orphan segments
	orphanKey := fmt.Sprintf("vex/namespaces/%s/index/segments/orphan.idx", ns)
	data := []byte("orphan-data")
	store.Put(ctx, orphanKey, bytes.NewReader(data), int64(len(data)), nil)

	// Configure collector
	config := &OrphanConfig{
		RetentionTime: 0,
		BatchSize:     100,
		DryRun:        true,
	}
	collector := NewOrphanCollector(store, config)

	result, err := collector.CollectNamespaceOrphans(ctx, ns)
	if err != nil {
		t.Fatalf("CollectNamespaceOrphans failed: %v", err)
	}

	// Tombstoned namespace should be skipped (orphan collector leaves it to namespace GC)
	if len(result.OrphanObjects) != 0 {
		t.Errorf("expected 0 orphan objects for tombstoned namespace, got %d", len(result.OrphanObjects))
	}
}

// TestOrphanGC_DetectsOrphanManifests verifies orphan manifests are detected.
func TestOrphanGC_DetectsOrphanManifests(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "orphan-manifest-ns"

	// Create namespace with manifest seq 5
	segments := []index.Segment{
		{ID: "seg-001", DocsKey: fmt.Sprintf("vex/namespaces/%s/index/segments/seg-001.idx", ns), StartWALSeq: 1, EndWALSeq: 10},
	}
	createNamespaceWithManifest(t, store, ns, 5, segments)

	// Create older manifests that are now orphaned
	for seq := uint64(1); seq <= 4; seq++ {
		manifest := index.NewManifest(ns)
		manifest.IndexedWALSeq = seq
		manifestKey := index.ManifestKey(ns, seq)
		manifestData, _ := json.Marshal(manifest)
		store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)
	}

	// Configure collector with zero retention
	config := &OrphanConfig{
		RetentionTime: 0,
		BatchSize:     100,
		DryRun:        true,
	}
	collector := NewOrphanCollector(store, config)

	result, err := collector.CollectNamespaceOrphans(ctx, ns)
	if err != nil {
		t.Fatalf("CollectNamespaceOrphans failed: %v", err)
	}

	// Should detect 4 orphan manifests (seq 1-4)
	orphanManifestCount := 0
	for _, key := range result.OrphanObjects {
		if isManifestKey(key) {
			orphanManifestCount++
		}
	}
	if orphanManifestCount != 4 {
		t.Errorf("expected 4 orphan manifests, got %d", orphanManifestCount)
	}

	// Active manifest (seq 5) should NOT be detected
	activeKey := index.ManifestKey(ns, 5)
	for _, key := range result.OrphanObjects {
		if key == activeKey {
			t.Errorf("active manifest %s should not be detected as orphan", activeKey)
		}
	}
}

// TestOrphanGC_BackgroundLoopStartStop verifies background loop lifecycle.
func TestOrphanGC_BackgroundLoopStartStop(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &OrphanConfig{
		ScanInterval:  100 * time.Millisecond,
		RetentionTime: 0,
		BatchSize:     100,
		DryRun:        true,
	}
	collector := NewOrphanCollector(store, config)

	// Start background loop
	if err := collector.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Can't start twice
	if err := collector.Start(ctx); err != ErrGCInProgress {
		t.Errorf("expected ErrGCInProgress, got %v", err)
	}

	// Wait for a scan
	time.Sleep(150 * time.Millisecond)

	// Stop
	collector.Stop()

	// Stop is idempotent
	collector.Stop()
}

// TestOrphanGC_HandlesNoManifest verifies behavior when namespace has no manifest.
func TestOrphanGC_HandlesNoManifest(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "no-manifest-ns"

	// Create state without manifest
	state := namespace.NewState(ns)
	stateData, _ := json.Marshal(state)
	stateKey := namespace.StateKey(ns)
	store.Put(ctx, stateKey, bytes.NewReader(stateData), int64(len(stateData)), nil)

	// Create segment objects (all are orphans since no manifest)
	orphanKeys := []string{
		fmt.Sprintf("vex/namespaces/%s/index/segments/seg-001.idx", ns),
		fmt.Sprintf("vex/namespaces/%s/index/segments/seg-002.idx", ns),
	}
	for _, key := range orphanKeys {
		data := []byte("orphan-data")
		store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
	}

	config := &OrphanConfig{
		RetentionTime: 0,
		BatchSize:     100,
		DryRun:        true,
	}
	collector := NewOrphanCollector(store, config)

	result, err := collector.CollectNamespaceOrphans(ctx, ns)
	if err != nil {
		t.Fatalf("CollectNamespaceOrphans failed: %v", err)
	}

	// All segments should be detected as orphans
	if len(result.OrphanObjects) != 2 {
		t.Errorf("expected 2 orphan objects, got %d", len(result.OrphanObjects))
	}
}

// TestOrphanGC_DefaultRetentionIs24Hours verifies default retention is 24h.
func TestOrphanGC_DefaultRetentionIs24Hours(t *testing.T) {
	config := DefaultOrphanConfig()
	if config.RetentionTime != 24*time.Hour {
		t.Errorf("expected default retention of 24h, got %v", config.RetentionTime)
	}
}

// isManifestKey checks if a key is a manifest key.
func isManifestKey(key string) bool {
	return len(key) > 0 && key[len(key)-9:] == ".idx.json"
}

// TestOrphanScanMarkerKey verifies the marker key is correct.
func TestOrphanScanMarkerKey(t *testing.T) {
	expected := "vex/gc/orphan_scan_marker.json"
	if OrphanScanMarkerKey != expected {
		t.Errorf("expected scan marker key %s, got %s", expected, OrphanScanMarkerKey)
	}
}

// TestOrphanGC_ScanMarkerPersistence verifies marker survives restarts.
func TestOrphanGC_ScanMarkerPersistence(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create first collector and run a scan
	config := &OrphanConfig{
		RetentionTime: 0,
		BatchSize:     100,
		DryRun:        true,
		ScanInterval:  time.Hour,
	}
	collector1 := NewOrphanCollector(store, config)
	collector1.scanAllNamespaces(ctx)

	// Get the marker
	marker1, err := LoadOrphanScanMarker(ctx, store)
	if err != nil {
		t.Fatalf("failed to load marker: %v", err)
	}

	// Create new collector (simulates restart)
	collector2 := NewOrphanCollector(store, config)
	marker2, err := collector2.GetScanMarker(ctx)
	if err != nil {
		t.Fatalf("failed to get marker: %v", err)
	}

	// Markers should match
	if !marker1.LastScanStarted.Equal(marker2.LastScanStarted) {
		t.Error("marker should persist across collector instances")
	}
}
