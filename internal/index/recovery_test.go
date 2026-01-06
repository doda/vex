package index

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vexsearch/vex/pkg/objectstore"
)

// recoveryMockStore is a test object store that tracks operations.
type recoveryMockStore struct {
	mu      sync.Mutex
	objects map[string]*mockObject
	calls   []string
}

type mockObject struct {
	data         []byte
	lastModified time.Time
	etag         string
}

func newRecoveryMockStore() *recoveryMockStore {
	return &recoveryMockStore{
		objects: make(map[string]*mockObject),
	}
}

func (m *recoveryMockStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "Get:"+key)

	obj, ok := m.objects[key]
	if !ok {
		return nil, nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(obj.data)), &objectstore.ObjectInfo{
		Key:          key,
		Size:         int64(len(obj.data)),
		ETag:         obj.etag,
		LastModified: obj.lastModified,
	}, nil
}

func (m *recoveryMockStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "Head:"+key)

	obj, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return &objectstore.ObjectInfo{
		Key:          key,
		Size:         int64(len(obj.data)),
		ETag:         obj.etag,
		LastModified: obj.lastModified,
	}, nil
}

func (m *recoveryMockStore) Put(ctx context.Context, key string, r io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "Put:"+key)

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	m.objects[key] = &mockObject{
		data:         data,
		lastModified: time.Now(),
		etag:         "etag-" + key,
	}

	return &objectstore.ObjectInfo{
		Key:  key,
		Size: int64(len(data)),
		ETag: "etag-" + key,
	}, nil
}

func (m *recoveryMockStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "PutIfAbsent:"+key)

	if _, exists := m.objects[key]; exists {
		return nil, objectstore.ErrAlreadyExists
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	m.objects[key] = &mockObject{
		data:         data,
		lastModified: time.Now(),
		etag:         "etag-" + key,
	}

	return &objectstore.ObjectInfo{
		Key:  key,
		Size: int64(len(data)),
		ETag: "etag-" + key,
	}, nil
}

func (m *recoveryMockStore) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	return m.Put(ctx, key, r, size, opts)
}

func (m *recoveryMockStore) List(ctx context.Context, opts *objectstore.ListOptions) (*objectstore.ListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "List:"+opts.Prefix)

	var objects []objectstore.ObjectInfo
	for key, obj := range m.objects {
		if strings.HasPrefix(key, opts.Prefix) {
			if opts.Marker != "" && key <= opts.Marker {
				continue
			}
			objects = append(objects, objectstore.ObjectInfo{
				Key:          key,
				Size:         int64(len(obj.data)),
				ETag:         obj.etag,
				LastModified: obj.lastModified,
			})
		}
	}

	// Sort for consistent ordering
	objectstore.SortObjects(objects)

	// Apply max keys
	truncated := false
	nextMarker := ""
	if opts.MaxKeys > 0 && len(objects) > opts.MaxKeys {
		objects = objects[:opts.MaxKeys]
		truncated = true
		nextMarker = objects[len(objects)-1].Key
	}

	return &objectstore.ListResult{
		Objects:     objects,
		IsTruncated: truncated,
		NextMarker:  nextMarker,
	}, nil
}

func (m *recoveryMockStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "Delete:"+key)
	delete(m.objects, key)
	return nil
}

// hasObject checks if an object exists.
func (m *recoveryMockStore) hasObject(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.objects[key]
	return exists
}

// Test: Crash after segment upload but before manifest is safe
func TestRecovery_CrashAfterSegmentUploadBeforeManifest(t *testing.T) {
	store := newRecoveryMockStore()
	namespace := "test-ns"

	// Simulate a crash scenario:
	// 1. Segment objects were uploaded
	// 2. Manifest was NOT uploaded
	// 3. State.json was NOT updated

	// Create segment objects (orphaned - no manifest references them)
	segmentID := "seg_orphan_123"
	docsKey := DocsObjectKey(namespace, segmentID)
	vectorsKey := VectorsObjectKey(namespace, segmentID)

	store.objects[docsKey] = &mockObject{
		data:         []byte("orphaned docs data"),
		lastModified: time.Now().Add(-25 * time.Hour), // Old enough to be GC'd
		etag:         "etag-docs",
	}
	store.objects[vectorsKey] = &mockObject{
		data:         []byte("orphaned vectors data"),
		lastModified: time.Now().Add(-25 * time.Hour),
		etag:         "etag-vectors",
	}

	// Create an empty state.json (no manifest reference)
	stateKey := "vex/namespaces/" + namespace + "/meta/state.json"
	stateData, _ := json.Marshal(map[string]interface{}{
		"namespace": namespace,
		"index": map[string]interface{}{
			"manifest_seq": 0,
			"manifest_key": "",
		},
	})
	store.objects[stateKey] = &mockObject{
		data:         stateData,
		lastModified: time.Now(),
		etag:         "etag-state",
	}

	// Run recovery
	config := &RecoveryConfig{
		MinOrphanAge:      24 * time.Hour,
		DryRun:            false,
		MaxObjectsPerScan: 10000,
	}
	recovery := NewRecovery(store, config)

	result, err := recovery.CleanOrphanedObjects(context.Background(), namespace)
	if err != nil {
		t.Fatalf("CleanOrphanedObjects failed: %v", err)
	}

	// Verify orphaned segments were found
	if len(result.OrphanSegmentObjects) != 2 {
		t.Errorf("Expected 2 orphan segment objects, got %d: %v", len(result.OrphanSegmentObjects), result.OrphanSegmentObjects)
	}

	// Verify orphans were deleted
	if len(result.DeletedObjects) != 2 {
		t.Errorf("Expected 2 deleted objects, got %d: %v", len(result.DeletedObjects), result.DeletedObjects)
	}

	// Verify objects are gone
	if store.hasObject(docsKey) {
		t.Errorf("Orphaned docs object should have been deleted")
	}
	if store.hasObject(vectorsKey) {
		t.Errorf("Orphaned vectors object should have been deleted")
	}
}

// Test: Crash after manifest upload but before state.json update
func TestRecovery_CrashAfterManifestUploadBeforeStateUpdate(t *testing.T) {
	store := newRecoveryMockStore()
	namespace := "test-ns"

	// Simulate a crash scenario:
	// 1. Segment objects were uploaded
	// 2. Manifest was uploaded
	// 3. State.json was NOT updated (still points to old manifest)

	// Create segment objects referenced by the new manifest
	segmentID := "seg_new_123"
	docsKey := DocsObjectKey(namespace, segmentID)
	store.objects[docsKey] = &mockObject{
		data:         []byte("new segment docs"),
		lastModified: time.Now().Add(-25 * time.Hour),
		etag:         "etag-new-docs",
	}

	// Create the orphaned manifest (not referenced by state.json)
	orphanManifestSeq := uint64(10)
	orphanManifestKey := ManifestKey(namespace, orphanManifestSeq)
	orphanManifest := NewManifest(namespace)
	orphanManifest.IndexedWALSeq = 100
	orphanManifest.AddSegment(Segment{
		ID:          segmentID,
		DocsKey:     docsKey,
		StartWALSeq: 1,
		EndWALSeq:   100,
	})
	orphanManifestData, _ := json.Marshal(orphanManifest)
	store.objects[orphanManifestKey] = &mockObject{
		data:         orphanManifestData,
		lastModified: time.Now().Add(-25 * time.Hour),
		etag:         "etag-orphan-manifest",
	}

	// Create state.json pointing to an older manifest (seq=5)
	activeManifestSeq := uint64(5)
	activeManifestKey := ManifestKey(namespace, activeManifestSeq)
	activeSegmentID := "seg_active_456"
	activeDocsKey := DocsObjectKey(namespace, activeSegmentID)

	// Create active segment object
	store.objects[activeDocsKey] = &mockObject{
		data:         []byte("active segment docs"),
		lastModified: time.Now(),
		etag:         "etag-active-docs",
	}

	// Create active manifest
	activeManifest := NewManifest(namespace)
	activeManifest.IndexedWALSeq = 50
	activeManifest.AddSegment(Segment{
		ID:          activeSegmentID,
		DocsKey:     activeDocsKey,
		StartWALSeq: 1,
		EndWALSeq:   50,
	})
	activeManifestData, _ := json.Marshal(activeManifest)
	store.objects[activeManifestKey] = &mockObject{
		data:         activeManifestData,
		lastModified: time.Now(),
		etag:         "etag-active-manifest",
	}

	// State.json points to the active manifest
	stateKey := "vex/namespaces/" + namespace + "/meta/state.json"
	stateData, _ := json.Marshal(map[string]interface{}{
		"namespace": namespace,
		"index": map[string]interface{}{
			"manifest_seq": activeManifestSeq,
			"manifest_key": activeManifestKey,
		},
	})
	store.objects[stateKey] = &mockObject{
		data:         stateData,
		lastModified: time.Now(),
		etag:         "etag-state",
	}

	// Run recovery
	config := &RecoveryConfig{
		MinOrphanAge:      24 * time.Hour,
		DryRun:            false,
		MaxObjectsPerScan: 10000,
	}
	recovery := NewRecovery(store, config)

	result, err := recovery.CleanOrphanedObjects(context.Background(), namespace)
	if err != nil {
		t.Fatalf("CleanOrphanedObjects failed: %v", err)
	}

	// Verify orphaned manifest was found
	if len(result.OrphanManifests) != 1 {
		t.Errorf("Expected 1 orphan manifest, got %d: %v", len(result.OrphanManifests), result.OrphanManifests)
	}

	// Verify new segment (referenced by orphan manifest but not active manifest) is orphaned
	if len(result.OrphanSegmentObjects) != 1 {
		t.Errorf("Expected 1 orphan segment object, got %d: %v", len(result.OrphanSegmentObjects), result.OrphanSegmentObjects)
	}

	// Verify active manifest and its segment are NOT deleted
	if !store.hasObject(activeManifestKey) {
		t.Errorf("Active manifest should NOT have been deleted")
	}
	if !store.hasObject(activeDocsKey) {
		t.Errorf("Active segment docs should NOT have been deleted")
	}

	// Verify orphaned objects were deleted
	if store.hasObject(orphanManifestKey) {
		t.Errorf("Orphan manifest should have been deleted")
	}
	if store.hasObject(docsKey) {
		t.Errorf("Orphan segment docs should have been deleted")
	}
}

// Test: Objects too young are not deleted
func TestRecovery_YoungObjectsNotDeleted(t *testing.T) {
	store := newRecoveryMockStore()
	namespace := "test-ns"

	// Create a young orphaned segment (less than 24h old)
	segmentID := "seg_young_123"
	docsKey := DocsObjectKey(namespace, segmentID)
	store.objects[docsKey] = &mockObject{
		data:         []byte("young orphaned docs"),
		lastModified: time.Now().Add(-1 * time.Hour), // Only 1 hour old
		etag:         "etag-young-docs",
	}

	// Create state.json with no manifest
	stateKey := "vex/namespaces/" + namespace + "/meta/state.json"
	stateData, _ := json.Marshal(map[string]interface{}{
		"namespace": namespace,
		"index": map[string]interface{}{
			"manifest_seq": 0,
			"manifest_key": "",
		},
	})
	store.objects[stateKey] = &mockObject{
		data:         stateData,
		lastModified: time.Now(),
		etag:         "etag-state",
	}

	// Run recovery with 24h min age
	config := &RecoveryConfig{
		MinOrphanAge:      24 * time.Hour,
		DryRun:            false,
		MaxObjectsPerScan: 10000,
	}
	recovery := NewRecovery(store, config)

	result, err := recovery.CleanOrphanedObjects(context.Background(), namespace)
	if err != nil {
		t.Fatalf("CleanOrphanedObjects failed: %v", err)
	}

	// Young object should NOT be in orphans list
	if len(result.OrphanSegmentObjects) != 0 {
		t.Errorf("Expected 0 orphan segment objects (too young), got %d", len(result.OrphanSegmentObjects))
	}

	// Object should still exist
	if !store.hasObject(docsKey) {
		t.Errorf("Young object should NOT have been deleted")
	}
}

// Test: Dry run mode
func TestRecovery_DryRun(t *testing.T) {
	store := newRecoveryMockStore()
	namespace := "test-ns"

	// Create old orphaned segment
	segmentID := "seg_orphan_dryrun"
	docsKey := DocsObjectKey(namespace, segmentID)
	store.objects[docsKey] = &mockObject{
		data:         []byte("orphaned docs for dry run"),
		lastModified: time.Now().Add(-25 * time.Hour),
		etag:         "etag-dryrun",
	}

	// Create state.json with no manifest
	stateKey := "vex/namespaces/" + namespace + "/meta/state.json"
	stateData, _ := json.Marshal(map[string]interface{}{
		"namespace": namespace,
		"index": map[string]interface{}{
			"manifest_seq": 0,
			"manifest_key": "",
		},
	})
	store.objects[stateKey] = &mockObject{
		data:         stateData,
		lastModified: time.Now(),
		etag:         "etag-state",
	}

	// Run recovery in dry run mode
	config := &RecoveryConfig{
		MinOrphanAge:      24 * time.Hour,
		DryRun:            true, // DRY RUN
		MaxObjectsPerScan: 10000,
	}
	recovery := NewRecovery(store, config)

	result, err := recovery.CleanOrphanedObjects(context.Background(), namespace)
	if err != nil {
		t.Fatalf("CleanOrphanedObjects failed: %v", err)
	}

	// Orphan should be found
	if len(result.OrphanSegmentObjects) != 1 {
		t.Errorf("Expected 1 orphan segment object, got %d", len(result.OrphanSegmentObjects))
	}

	// But nothing should be deleted
	if len(result.DeletedObjects) != 0 {
		t.Errorf("Dry run should not delete objects, got %d deleted", len(result.DeletedObjects))
	}

	// Object should still exist
	if !store.hasObject(docsKey) {
		t.Errorf("Dry run should NOT delete objects")
	}
}

// Test: IsOrphanedSegmentObject helper
func TestRecovery_IsOrphanedSegmentObject(t *testing.T) {
	store := newRecoveryMockStore()
	namespace := "test-ns"

	// Create a manifest with one segment
	activeSegmentID := "seg_active"
	activeDocsKey := DocsObjectKey(namespace, activeSegmentID)

	store.objects[activeDocsKey] = &mockObject{
		data:         []byte("active docs"),
		lastModified: time.Now(),
		etag:         "etag-active",
	}

	manifest := NewManifest(namespace)
	manifest.AddSegment(Segment{
		ID:      activeSegmentID,
		DocsKey: activeDocsKey,
	})
	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey(namespace, 1)
	store.objects[manifestKey] = &mockObject{
		data:         manifestData,
		lastModified: time.Now(),
		etag:         "etag-manifest",
	}

	stateKey := "vex/namespaces/" + namespace + "/meta/state.json"
	stateData, _ := json.Marshal(map[string]interface{}{
		"namespace": namespace,
		"index": map[string]interface{}{
			"manifest_seq": 1,
			"manifest_key": manifestKey,
		},
	})
	store.objects[stateKey] = &mockObject{
		data:         stateData,
		lastModified: time.Now(),
		etag:         "etag-state",
	}

	recovery := NewRecovery(store, nil)

	// Active segment should NOT be orphaned
	orphaned, err := recovery.IsOrphanedSegmentObject(context.Background(), namespace, activeDocsKey)
	if err != nil {
		t.Fatalf("IsOrphanedSegmentObject failed: %v", err)
	}
	if orphaned {
		t.Error("Active segment should NOT be reported as orphaned")
	}

	// Unknown segment should be orphaned
	orphanKey := DocsObjectKey(namespace, "seg_unknown")
	orphaned, err = recovery.IsOrphanedSegmentObject(context.Background(), namespace, orphanKey)
	if err != nil {
		t.Fatalf("IsOrphanedSegmentObject failed: %v", err)
	}
	if !orphaned {
		t.Error("Unknown segment should be reported as orphaned")
	}
}

// Test: RecoverFromCrash detects inconsistent manifest
func TestRecovery_RecoverFromCrashDetectsInconsistentManifest(t *testing.T) {
	store := newRecoveryMockStore()
	namespace := "test-ns"

	// Create a manifest that references a missing segment object
	missingDocsKey := DocsObjectKey(namespace, "seg_missing")
	manifest := NewManifest(namespace)
	manifest.AddSegment(Segment{
		ID:      "seg_missing",
		DocsKey: missingDocsKey, // This object doesn't exist!
	})
	manifestData, _ := json.Marshal(manifest)
	manifestKey := ManifestKey(namespace, 1)
	store.objects[manifestKey] = &mockObject{
		data:         manifestData,
		lastModified: time.Now(),
		etag:         "etag-manifest",
	}

	stateKey := "vex/namespaces/" + namespace + "/meta/state.json"
	stateData, _ := json.Marshal(map[string]interface{}{
		"namespace": namespace,
		"index": map[string]interface{}{
			"manifest_seq": 1,
			"manifest_key": manifestKey,
		},
	})
	store.objects[stateKey] = &mockObject{
		data:         stateData,
		lastModified: time.Now(),
		etag:         "etag-state",
	}

	recovery := NewRecovery(store, nil)

	result, err := recovery.RecoverFromCrash(context.Background(), namespace)
	if err != nil {
		t.Fatalf("RecoverFromCrash failed: %v", err)
	}

	// Should report the inconsistency as an error
	if len(result.Errors) == 0 {
		t.Error("Expected error about inconsistent manifest, got none")
	}

	// Check that error mentions missing object
	foundError := false
	for _, e := range result.Errors {
		if strings.Contains(e.Error(), "active manifest inconsistent") {
			foundError = true
			break
		}
	}
	if !foundError {
		t.Errorf("Expected error about active manifest inconsistent, got: %v", result.Errors)
	}
}

// Test: OrphanScanner incremental scanning
func TestRecovery_OrphanScanner(t *testing.T) {
	store := newRecoveryMockStore()
	namespace := "test-ns"

	// Create multiple orphaned segments
	oldTime := time.Now().Add(-25 * time.Hour)
	for i := 0; i < 5; i++ {
		segmentID := "seg_orphan_" + string(rune('a'+i))
		docsKey := DocsObjectKey(namespace, segmentID)
		store.objects[docsKey] = &mockObject{
			data:         []byte("orphan docs " + segmentID),
			lastModified: oldTime,
			etag:         "etag-" + segmentID,
		}
	}

	// Create state.json with no manifest
	stateKey := "vex/namespaces/" + namespace + "/meta/state.json"
	stateData, _ := json.Marshal(map[string]interface{}{
		"namespace": namespace,
		"index": map[string]interface{}{
			"manifest_seq": 0,
			"manifest_key": "",
		},
	})
	store.objects[stateKey] = &mockObject{
		data:         stateData,
		lastModified: time.Now(),
		etag:         "etag-state",
	}

	config := &RecoveryConfig{
		MinOrphanAge:      24 * time.Hour,
		MaxObjectsPerScan: 10000,
	}
	recovery := NewRecovery(store, config)
	scanner := NewOrphanScanner(recovery, namespace)

	// Scan in batches
	var allOrphans []string
	hasMore := true
	for hasMore {
		var err error
		var orphans []string
		orphans, hasMore, err = scanner.ScanBatch(context.Background(), 2)
		if err != nil {
			t.Fatalf("ScanBatch failed: %v", err)
		}
		allOrphans = append(allOrphans, orphans...)
	}

	// Should find all 5 orphans
	if len(allOrphans) != 5 {
		t.Errorf("Expected 5 orphans, got %d", len(allOrphans))
	}

	// Scanner should be done
	if !scanner.IsDone() {
		t.Error("Scanner should be marked as done")
	}
}

// Test: No state file means all objects are orphaned
func TestRecovery_NoStateFile(t *testing.T) {
	store := newRecoveryMockStore()
	namespace := "test-ns"

	// Create segment objects with no state file at all
	oldTime := time.Now().Add(-25 * time.Hour)
	segmentID := "seg_no_state"
	docsKey := DocsObjectKey(namespace, segmentID)
	store.objects[docsKey] = &mockObject{
		data:         []byte("orphan docs no state"),
		lastModified: oldTime,
		etag:         "etag-no-state",
	}

	config := &RecoveryConfig{
		MinOrphanAge:      24 * time.Hour,
		DryRun:            false,
		MaxObjectsPerScan: 10000,
	}
	recovery := NewRecovery(store, config)

	result, err := recovery.CleanOrphanedObjects(context.Background(), namespace)
	if err != nil {
		t.Fatalf("CleanOrphanedObjects failed: %v", err)
	}

	// Should find the orphan
	if len(result.OrphanSegmentObjects) != 1 {
		t.Errorf("Expected 1 orphan, got %d", len(result.OrphanSegmentObjects))
	}

	// Should delete it
	if len(result.DeletedObjects) != 1 {
		t.Errorf("Expected 1 deleted, got %d", len(result.DeletedObjects))
	}
}

// Test: Multiple segment types (docs, vectors, filters, fts)
func TestRecovery_MultipleSegmentTypes(t *testing.T) {
	store := newRecoveryMockStore()
	namespace := "test-ns"

	oldTime := time.Now().Add(-25 * time.Hour)
	segmentID := "seg_multi"

	// Create all types of segment objects
	keys := []string{
		DocsObjectKey(namespace, segmentID),
		VectorsObjectKey(namespace, segmentID),
		FilterObjectKey(namespace, segmentID, "color"),
		FTSObjectKey(namespace, segmentID, "title"),
	}

	for _, key := range keys {
		store.objects[key] = &mockObject{
			data:         []byte("data for " + key),
			lastModified: oldTime,
			etag:         "etag-" + key,
		}
	}

	// Create state.json with no manifest
	stateKey := "vex/namespaces/" + namespace + "/meta/state.json"
	stateData, _ := json.Marshal(map[string]interface{}{
		"namespace": namespace,
		"index": map[string]interface{}{
			"manifest_seq": 0,
			"manifest_key": "",
		},
	})
	store.objects[stateKey] = &mockObject{
		data:         stateData,
		lastModified: time.Now(),
		etag:         "etag-state",
	}

	config := &RecoveryConfig{
		MinOrphanAge:      24 * time.Hour,
		DryRun:            false,
		MaxObjectsPerScan: 10000,
	}
	recovery := NewRecovery(store, config)

	result, err := recovery.CleanOrphanedObjects(context.Background(), namespace)
	if err != nil {
		t.Fatalf("CleanOrphanedObjects failed: %v", err)
	}

	// Should find all 4 orphans
	if len(result.OrphanSegmentObjects) != 4 {
		t.Errorf("Expected 4 orphan segment objects, got %d: %v", len(result.OrphanSegmentObjects), result.OrphanSegmentObjects)
	}

	// Should delete all
	if len(result.DeletedObjects) != 4 {
		t.Errorf("Expected 4 deleted objects, got %d", len(result.DeletedObjects))
	}

	// Verify all gone
	for _, key := range keys {
		if store.hasObject(key) {
			t.Errorf("Object %s should have been deleted", key)
		}
	}
}
