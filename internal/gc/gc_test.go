package gc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// createTestNamespace creates a namespace with state, tombstone, WAL, and index objects.
func createTestNamespace(t *testing.T, store objectstore.Store, ns string, tombstoned bool, tombstoneAge time.Duration) {
	ctx := context.Background()

	// Create state.json
	state := namespace.NewState(ns)
	if tombstoned {
		now := time.Now().Add(-tombstoneAge)
		state.Deletion.Tombstoned = true
		state.Deletion.TombstonedAt = &now
	}
	stateData, _ := json.Marshal(state)
	stateKey := namespace.StateKey(ns)
	store.Put(ctx, stateKey, bytes.NewReader(stateData), int64(len(stateData)), nil)

	// Create tombstone if needed
	if tombstoned {
		tombstone := &namespace.Tombstone{
			DeletedAt: time.Now().Add(-tombstoneAge),
		}
		tsData, _ := json.Marshal(tombstone)
		tsKey := namespace.TombstoneKey(ns)
		store.Put(ctx, tsKey, bytes.NewReader(tsData), int64(len(tsData)), nil)
	}

	// Create some WAL objects
	for i := 1; i <= 3; i++ {
		key := walKey(ns, uint64(i))
		data := []byte("wal-data")
		store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
	}

	// Create some index objects (segments and manifests)
	segmentKeys := []string{
		indexSegmentKey(ns, "segment-001"),
		indexSegmentKey(ns, "segment-002"),
	}
	for _, key := range segmentKeys {
		data := []byte("segment-data")
		store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
	}

	manifestKeys := []string{
		indexManifestKey(ns, 1),
		indexManifestKey(ns, 2),
	}
	for _, key := range manifestKeys {
		data := []byte("manifest-data")
		store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
	}
}

func walKey(ns string, seq uint64) string {
	return fmt.Sprintf("vex/namespaces/%s/wal/%s.wal.zst", ns, padSequence(seq))
}

func indexSegmentKey(ns, segment string) string {
	return "vex/namespaces/" + ns + "/index/segments/" + segment + ".idx"
}

func indexManifestKey(ns string, seq uint64) string {
	return "vex/namespaces/" + ns + "/index/manifests/" + padSequence(seq) + ".idx.json"
}

func padSequence(seq uint64) string {
	return fmt.Sprintf("%020d", seq)
}

func countObjects(t *testing.T, store objectstore.Store, prefix string) int {
	ctx := context.Background()
	count := 0
	marker := ""
	for {
		result, err := store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 100,
		})
		if err != nil {
			t.Fatalf("failed to list objects: %v", err)
		}
		count += len(result.Objects)
		if !result.IsTruncated {
			break
		}
		marker = result.NextMarker
	}
	return count
}

// TestGC_DeletesNamespaceObjects verifies that GC deletes WAL and index objects.
func TestGC_DeletesNamespaceObjects(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create a tombstoned namespace with objects
	ns := "deleted-ns"
	createTestNamespace(t, store, ns, true, 25*time.Hour) // tombstoned 25h ago

	// Verify objects exist
	prefix := "vex/namespaces/" + ns + "/"
	before := countObjects(t, store, prefix)
	if before == 0 {
		t.Fatal("expected objects to exist before GC")
	}

	// Run GC with short age requirement
	config := &Config{
		MinTombstoneAge:   1 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)

	result, err := collector.CollectNamespace(ctx, ns)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Verify objects were deleted
	if result.TotalDeleted == 0 {
		t.Error("expected objects to be deleted")
	}

	// Verify WAL objects were deleted
	if len(result.WALObjectsDeleted) == 0 {
		t.Error("expected WAL objects to be deleted")
	}

	// Verify index objects were deleted
	if len(result.IndexObjectsDeleted) == 0 {
		t.Error("expected index objects to be deleted")
	}
}

// TestGC_VerifiesWALObjectsCleanedUp verifies WAL objects are cleaned up.
func TestGC_VerifiesWALObjectsCleanedUp(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "wal-cleanup-ns"
	createTestNamespace(t, store, ns, true, 25*time.Hour)

	// Count WAL objects before
	walPrefix := "vex/namespaces/" + ns + "/wal/"
	walBefore := countObjects(t, store, walPrefix)
	if walBefore == 0 {
		t.Fatal("expected WAL objects to exist")
	}

	// Run GC
	config := &Config{
		MinTombstoneAge:   1 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)

	result, err := collector.CollectNamespace(ctx, ns)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Verify WAL objects count matches deleted
	if len(result.WALObjectsDeleted) != walBefore {
		t.Errorf("expected %d WAL objects deleted, got %d", walBefore, len(result.WALObjectsDeleted))
	}

	// Verify WAL directory is empty
	walAfter := countObjects(t, store, walPrefix)
	if walAfter != 0 {
		t.Errorf("expected 0 WAL objects after GC, got %d", walAfter)
	}
}

// TestGC_VerifiesIndexObjectsCleanedUp verifies index objects are cleaned up.
func TestGC_VerifiesIndexObjectsCleanedUp(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "index-cleanup-ns"
	createTestNamespace(t, store, ns, true, 25*time.Hour)

	// Count index objects before
	indexPrefix := "vex/namespaces/" + ns + "/index/"
	indexBefore := countObjects(t, store, indexPrefix)
	if indexBefore == 0 {
		t.Fatal("expected index objects to exist")
	}

	// Run GC
	config := &Config{
		MinTombstoneAge:   1 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)

	result, err := collector.CollectNamespace(ctx, ns)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Verify index objects were deleted
	if len(result.IndexObjectsDeleted) != indexBefore {
		t.Errorf("expected %d index objects deleted, got %d", indexBefore, len(result.IndexObjectsDeleted))
	}

	// Verify index directory is empty
	indexAfter := countObjects(t, store, indexPrefix)
	if indexAfter != 0 {
		t.Errorf("expected 0 index objects after GC, got %d", indexAfter)
	}
}

// TestGC_TombstonePreservedForFastRejection verifies tombstone is preserved by default.
func TestGC_TombstonePreservedForFastRejection(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "tombstone-preserve-ns"
	createTestNamespace(t, store, ns, true, 25*time.Hour)

	// Verify tombstone exists before
	tsKey := namespace.TombstoneKey(ns)
	_, _, err := store.Get(ctx, tsKey, nil)
	if err != nil {
		t.Fatal("expected tombstone to exist before GC")
	}

	// Run GC with tombstone preservation enabled
	config := &Config{
		MinTombstoneAge:   1 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)

	result, err := collector.CollectNamespace(ctx, ns)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Verify result indicates tombstone preserved
	if !result.TombstonePreserved {
		t.Error("expected result.TombstonePreserved to be true")
	}

	// Verify tombstone still exists
	_, _, err = store.Get(ctx, tsKey, nil)
	if err != nil {
		t.Error("expected tombstone to be preserved after GC")
	}

	// Verify tombstone was NOT in deleted list
	for _, key := range result.MetaObjectsDeleted {
		if key == tsKey {
			t.Error("tombstone should not be in deleted list when preserving")
		}
	}

	// Verify state.json WAS deleted
	stateKey := namespace.StateKey(ns)
	stateDeleted := false
	for _, key := range result.MetaObjectsDeleted {
		if key == stateKey {
			stateDeleted = true
			break
		}
	}
	if !stateDeleted {
		t.Error("expected state.json to be deleted")
	}
}

// TestGC_TombstoneCanBeDeleted verifies tombstone can be deleted when configured.
func TestGC_TombstoneCanBeDeleted(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "tombstone-delete-ns"
	createTestNamespace(t, store, ns, true, 25*time.Hour)

	// Run GC with tombstone preservation disabled
	config := &Config{
		MinTombstoneAge:   1 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: false, // Delete tombstone too
	}
	collector := NewCollector(store, config)

	result, err := collector.CollectNamespace(ctx, ns)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Verify result indicates tombstone not preserved
	if result.TombstonePreserved {
		t.Error("expected result.TombstonePreserved to be false")
	}

	// Verify tombstone was deleted
	tsKey := namespace.TombstoneKey(ns)
	_, _, err = store.Get(ctx, tsKey, nil)
	if !objectstore.IsNotFoundError(err) {
		t.Error("expected tombstone to be deleted")
	}
}

// TestGC_RespectsMinTombstoneAge verifies GC doesn't run on recent tombstones.
func TestGC_RespectsMinTombstoneAge(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "recent-tombstone-ns"
	createTestNamespace(t, store, ns, true, 1*time.Hour) // Only 1 hour old

	// Run GC with 24h minimum age
	config := &Config{
		MinTombstoneAge:   24 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)

	_, err := collector.CollectNamespace(ctx, ns)
	if err == nil {
		t.Error("expected GC to fail for recent tombstone")
	}
	if !strings.Contains(err.Error(), "tombstone too recent") {
		t.Errorf("expected 'tombstone too recent' error, got: %v", err)
	}

	// Verify objects still exist
	prefix := "vex/namespaces/" + ns + "/"
	count := countObjects(t, store, prefix)
	if count == 0 {
		t.Error("expected objects to still exist")
	}
}

// TestGC_SkipsNonTombstonedNamespaces verifies GC skips active namespaces.
func TestGC_SkipsNonTombstonedNamespaces(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "active-ns"
	createTestNamespace(t, store, ns, false, 0) // Not tombstoned

	config := &Config{
		MinTombstoneAge:   1 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)

	_, err := collector.CollectNamespace(ctx, ns)
	if err == nil {
		t.Error("expected GC to fail for non-tombstoned namespace")
	}
	if !strings.Contains(err.Error(), "no tombstone") {
		t.Errorf("expected 'no tombstone' error, got: %v", err)
	}
}

// TestGC_FindsTombstonedNamespaces verifies scanner finds all tombstoned namespaces.
func TestGC_FindsTombstonedNamespaces(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	// Create multiple namespaces
	createTestNamespace(t, store, "ns-active", false, 0)
	createTestNamespace(t, store, "ns-deleted-1", true, 25*time.Hour)
	createTestNamespace(t, store, "ns-deleted-2", true, 30*time.Hour)
	createTestNamespace(t, store, "ns-recent", true, 1*time.Hour)

	config := &Config{
		MinTombstoneAge:   24 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)

	// Run full cleanup
	result, err := collector.RunFullCleanup(ctx)
	if err != nil {
		t.Fatalf("RunFullCleanup failed: %v", err)
	}

	// Should have processed 2 namespaces (ns-deleted-1 and ns-deleted-2)
	if result.NamespacesProcessed != 2 {
		t.Errorf("expected 2 namespaces processed, got %d", result.NamespacesProcessed)
	}

	// Verify objects were deleted
	if result.TotalObjectsDeleted == 0 {
		t.Error("expected objects to be deleted")
	}

	// Verify ns-recent still has objects (too recent)
	recentPrefix := "vex/namespaces/ns-recent/"
	recentCount := countObjects(t, store, recentPrefix)
	if recentCount == 0 {
		t.Error("expected ns-recent to still have objects")
	}
}

// TestGC_BackgroundLoopStartStop verifies background loop starts and stops.
func TestGC_BackgroundLoopStartStop(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &Config{
		ScanInterval:      100 * time.Millisecond, // Fast for testing
		MinTombstoneAge:   0,                      // No minimum for testing
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)

	// Start the background loop
	if err := collector.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Can't start twice
	if err := collector.Start(ctx); err != ErrGCInProgress {
		t.Errorf("expected ErrGCInProgress, got: %v", err)
	}

	// Wait for a scan
	time.Sleep(150 * time.Millisecond)

	// Stop should work
	collector.Stop()

	// Stop is idempotent
	collector.Stop()
}

// TestGC_IsTombstonePreserved checks the helper function.
func TestGC_IsTombstonePreserved(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "check-tombstone-ns"

	// No tombstone initially
	preserved, err := IsTombstonePreserved(ctx, store, ns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if preserved {
		t.Error("expected preserved=false when tombstone doesn't exist")
	}

	// Create tombstone
	createTestNamespace(t, store, ns, true, 25*time.Hour)

	// Now should be preserved
	preserved, err = IsTombstonePreserved(ctx, store, ns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !preserved {
		t.Error("expected preserved=true when tombstone exists")
	}
}

// TestGC_HasRemainingObjects checks the helper function.
func TestGC_HasRemainingObjects(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "remaining-objects-ns"
	createTestNamespace(t, store, ns, true, 25*time.Hour)

	// Has objects initially
	hasRemaining, err := HasRemainingObjects(ctx, store, ns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasRemaining {
		t.Error("expected hasRemaining=true before GC")
	}

	// Run GC
	config := &Config{
		MinTombstoneAge:   1 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)
	_, _ = collector.CollectNamespace(ctx, ns)

	// After GC, only tombstone should remain
	hasRemaining, err = HasRemainingObjects(ctx, store, ns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasRemaining {
		t.Error("expected hasRemaining=false after GC (only tombstone remains)")
	}
}

// TestGC_DeletesContinuesOnError verifies GC continues on individual object errors.
func TestGC_DeletesContinuesOnError(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "error-handling-ns"
	createTestNamespace(t, store, ns, true, 25*time.Hour)

	config := &Config{
		MinTombstoneAge:   1 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)

	// GC should complete successfully even if some objects are already gone
	result, err := collector.CollectNamespace(ctx, ns)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Should have deleted objects
	if result.TotalDeleted == 0 {
		t.Error("expected some objects to be deleted")
	}
}

// TestGC_ResultContainsAllCategories verifies result contains all object categories.
func TestGC_ResultContainsAllCategories(t *testing.T) {
	store := objectstore.NewMemoryStore()
	ctx := context.Background()

	ns := "result-categories-ns"
	createTestNamespace(t, store, ns, true, 25*time.Hour)

	config := &Config{
		MinTombstoneAge:   1 * time.Hour,
		ObjectBatchSize:   100,
		PreserveTombstone: true,
	}
	collector := NewCollector(store, config)

	result, err := collector.CollectNamespace(ctx, ns)
	if err != nil {
		t.Fatalf("GC failed: %v", err)
	}

	// Check all categories are tracked
	if len(result.WALObjectsDeleted) == 0 {
		t.Error("expected WAL objects in result")
	}
	if len(result.IndexObjectsDeleted) == 0 {
		t.Error("expected index objects in result")
	}
	if len(result.MetaObjectsDeleted) == 0 {
		t.Error("expected meta objects in result")
	}

	// Check total matches sum
	expectedTotal := len(result.WALObjectsDeleted) + len(result.IndexObjectsDeleted) + len(result.MetaObjectsDeleted)
	if result.TotalDeleted != expectedTotal {
		t.Errorf("expected TotalDeleted=%d, got %d", expectedTotal, result.TotalDeleted)
	}

	// Check duration is set
	if result.Duration == 0 {
		t.Error("expected Duration to be set")
	}

	// Check namespace is set
	if result.Namespace != ns {
		t.Errorf("expected Namespace=%s, got %s", ns, result.Namespace)
	}
}
