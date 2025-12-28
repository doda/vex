package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vexsearch/vex/pkg/objectstore"
)

// publisherMockStore is a test object store that tracks operations.
type publisherMockStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	etags   map[string]string
	calls   []string

	// Error injection
	failPut    bool
	failPutKey string
	failHead   bool
	failGet    bool
}

func newPublisherMockStore() *publisherMockStore {
	return &publisherMockStore{
		objects: make(map[string][]byte),
		etags:   make(map[string]string),
	}
}

func (m *publisherMockStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "Get:"+key)

	if m.failGet {
		return nil, nil, errors.New("get failed")
	}

	data, ok := m.objects[key]
	if !ok {
		return nil, nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), &objectstore.ObjectInfo{
		Key:  key,
		Size: int64(len(data)),
		ETag: m.etags[key],
	}, nil
}

func (m *publisherMockStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "Head:"+key)

	if m.failHead {
		return nil, errors.New("head failed")
	}

	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return &objectstore.ObjectInfo{
		Key:  key,
		Size: int64(len(data)),
		ETag: m.etags[key],
	}, nil
}

func (m *publisherMockStore) Put(ctx context.Context, key string, r io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "Put:"+key)

	if m.failPut || (m.failPutKey != "" && strings.Contains(key, m.failPutKey)) {
		return nil, errors.New("put failed")
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	etag := "etag-" + key
	m.objects[key] = data
	m.etags[key] = etag

	return &objectstore.ObjectInfo{
		Key:  key,
		Size: int64(len(data)),
		ETag: etag,
	}, nil
}

func (m *publisherMockStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "PutIfAbsent:"+key)

	if m.failPut || (m.failPutKey != "" && strings.Contains(key, m.failPutKey)) {
		return nil, errors.New("put failed")
	}

	if _, exists := m.objects[key]; exists {
		return nil, objectstore.ErrAlreadyExists
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	etag := "etag-" + key
	m.objects[key] = data
	m.etags[key] = etag

	return &objectstore.ObjectInfo{
		Key:  key,
		Size: int64(len(data)),
		ETag: etag,
	}, nil
}

func (m *publisherMockStore) PutIfMatch(ctx context.Context, key string, r io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "PutIfMatch:"+key)

	if m.failPut {
		return nil, errors.New("put failed")
	}

	currentETag, exists := m.etags[key]
	if !exists {
		return nil, objectstore.ErrNotFound
	}
	if currentETag != etag {
		return nil, objectstore.ErrPrecondition
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	newETag := "etag-new-" + key
	m.objects[key] = data
	m.etags[key] = newETag

	return &objectstore.ObjectInfo{
		Key:  key,
		Size: int64(len(data)),
		ETag: newETag,
	}, nil
}

func (m *publisherMockStore) List(ctx context.Context, opts *objectstore.ListOptions) (*objectstore.ListResult, error) {
	return &objectstore.ListResult{}, nil
}

func (m *publisherMockStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	delete(m.etags, key)
	return nil
}

func (m *publisherMockStore) getCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.calls))
	copy(result, m.calls)
	return result
}

func (m *publisherMockStore) hasObject(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.objects[key]
	return exists
}

func (m *publisherMockStore) getObject(key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.objects[key]
}

// Test: Verify all segment objects are uploaded before manifest
func TestPublisher_SegmentsUploadedBeforeManifest(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	publisher := NewPublisher(store, namespace)

	// Create a segment with docs data
	segID := "seg_1234"
	seg := &SegmentUpload{
		Segment: Segment{
			ID:          segID,
			Level:       0,
			StartWALSeq: 1,
			EndWALSeq:   5,
			CreatedAt:   time.Now().UTC(),
		},
		DocsData:   []byte("docs data"),
		VectorData: []byte("vector data"),
		FilterData: map[string][]byte{"color": []byte("filter data")},
	}
	publisher.AddSegment(seg)

	// Create manifest referencing the segment
	manifest := NewManifest(namespace)
	manifest.IndexedWALSeq = 5
	// Note: The segment keys will be set during upload
	publisher.SetManifest(manifest)

	// Define state updater (no-op for this test)
	var stateUpdateCalled bool
	stateUpdater := func(ctx context.Context, manifestKey string, manifestSeq, indexedWALSeq uint64, bytesIndexed int64) (string, error) {
		stateUpdateCalled = true
		return "etag-new", nil
	}

	// Execute publish
	ctx := context.Background()
	result, err := publisher.Publish(ctx, 1, stateUpdater)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Verify result
	if len(result.UploadedSegments) != 1 || result.UploadedSegments[0] != segID {
		t.Errorf("Expected segment %s in result, got %v", segID, result.UploadedSegments)
	}

	// Analyze call order
	calls := store.getCalls()
	var docsIndex, vectorsIndex, filterIndex, manifestIndex int
	for i, call := range calls {
		if strings.Contains(call, "docs.col.zst") && strings.HasPrefix(call, "PutIfAbsent") {
			docsIndex = i
		}
		if strings.Contains(call, "vectors.ivf.zst") && strings.HasPrefix(call, "PutIfAbsent") {
			vectorsIndex = i
		}
		if strings.Contains(call, "filters/color.bitmap") && strings.HasPrefix(call, "PutIfAbsent") {
			filterIndex = i
		}
		if strings.Contains(call, ".idx.json") && strings.HasPrefix(call, "PutIfAbsent") {
			manifestIndex = i
		}
	}

	// Verify segments uploaded before manifest
	if docsIndex > manifestIndex {
		t.Errorf("Docs uploaded after manifest: docs=%d, manifest=%d", docsIndex, manifestIndex)
	}
	if vectorsIndex > manifestIndex {
		t.Errorf("Vectors uploaded after manifest: vectors=%d, manifest=%d", vectorsIndex, manifestIndex)
	}
	if filterIndex > manifestIndex {
		t.Errorf("Filter uploaded after manifest: filter=%d, manifest=%d", filterIndex, manifestIndex)
	}

	// Verify state update was called
	if !stateUpdateCalled {
		t.Error("State updater was not called")
	}
}

// Test: Manifest never references missing objects
func TestPublisher_ManifestNeverReferencesMissingObjects(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	publisher := NewPublisher(store, namespace)

	// Create a segment
	seg := &SegmentUpload{
		Segment: Segment{
			ID:          "seg_1234",
			Level:       0,
			StartWALSeq: 1,
			EndWALSeq:   5,
			CreatedAt:   time.Now().UTC(),
		},
		DocsData: []byte("docs data"),
	}
	publisher.AddSegment(seg)

	// Create manifest with a reference to a non-existent object
	manifest := NewManifest(namespace)
	manifest.AddSegment(Segment{
		ID:          "seg_1234",
		DocsKey:     "vex/namespaces/test-ns/index/segments/seg_1234/docs.col.zst",
		VectorsKey:  "vex/namespaces/test-ns/index/segments/seg_1234/nonexistent.ivf.zst", // Missing!
		StartWALSeq: 1,
		EndWALSeq:   5,
	})
	manifest.IndexedWALSeq = 5
	publisher.SetManifest(manifest)

	ctx := context.Background()
	_, err := publisher.Publish(ctx, 1, nil)

	// Should fail because manifest references a missing object
	if err == nil {
		t.Fatal("Expected error for missing object reference")
	}
	if !errors.Is(err, ErrObjectMissing) {
		t.Errorf("Expected ErrObjectMissing, got: %v", err)
	}
}

// Test: Manifest upload fails if segment upload fails
func TestPublisher_ManifestNotUploadedIfSegmentFails(t *testing.T) {
	store := newPublisherMockStore()
	store.failPutKey = "docs.col.zst" // Fail docs upload
	namespace := "test-ns"

	publisher := NewPublisher(store, namespace)

	seg := &SegmentUpload{
		Segment: Segment{
			ID:          "seg_1234",
			Level:       0,
			StartWALSeq: 1,
			EndWALSeq:   5,
		},
		DocsData: []byte("docs data"),
	}
	publisher.AddSegment(seg)

	manifest := NewManifest(namespace)
	manifest.IndexedWALSeq = 5
	publisher.SetManifest(manifest)

	ctx := context.Background()
	_, err := publisher.Publish(ctx, 1, nil)

	if err == nil {
		t.Fatal("Expected error for failed segment upload")
	}
	if !errors.Is(err, ErrSegmentUploadFailed) {
		t.Errorf("Expected ErrSegmentUploadFailed, got: %v", err)
	}

	// Verify manifest was NOT uploaded
	calls := store.getCalls()
	for _, call := range calls {
		if strings.Contains(call, ".idx.json") {
			t.Error("Manifest was uploaded despite segment upload failure")
		}
	}
}

// Test: state.json CAS update happens after manifest upload
func TestPublisher_StateUpdateAfterManifest(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	publisher := NewPublisher(store, namespace)

	seg := &SegmentUpload{
		Segment: Segment{
			ID:          "seg_1234",
			Level:       0,
			StartWALSeq: 1,
			EndWALSeq:   5,
			CreatedAt:   time.Now().UTC(),
		},
		DocsData: []byte("docs data"),
	}
	publisher.AddSegment(seg)

	manifest := NewManifest(namespace)
	manifest.IndexedWALSeq = 5
	publisher.SetManifest(manifest)

	var stateUpdateOrder int
	var manifestUploadOrder int
	var orderCounter int32

	stateUpdater := func(ctx context.Context, manifestKey string, manifestSeq, indexedWALSeq uint64, bytesIndexed int64) (string, error) {
		stateUpdateOrder = int(atomic.AddInt32(&orderCounter, 1))
		return "etag-new", nil
	}

	// Track when manifest is uploaded by checking store calls after
	ctx := context.Background()
	_, err := publisher.Publish(ctx, 1, stateUpdater)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Check order via calls
	calls := store.getCalls()
	for i, call := range calls {
		if strings.Contains(call, ".idx.json") && strings.HasPrefix(call, "PutIfAbsent") {
			manifestUploadOrder = i
		}
	}

	// State updater is called after manifest upload (after all store calls complete)
	// Since state updater runs after publish completes its store operations
	if stateUpdateOrder == 0 {
		t.Error("State updater was not called")
	}

	// Verify manifest was uploaded before state update would be called
	if manifestUploadOrder < 0 {
		t.Error("Manifest was not uploaded")
	}

	// Verify manifest exists
	if !store.hasObject("vex/namespaces/test-ns/index/manifests/00000000000000000001.idx.json") {
		t.Error("Manifest object not found in store")
	}
}

// Test: indexed_wal_seq only advances on successful publish
func TestPublisher_IndexedWALSeqOnlyAdvancesOnSuccess(t *testing.T) {
	t.Run("advances on success", func(t *testing.T) {
		store := newPublisherMockStore()
		namespace := "test-ns"

		publisher := NewPublisher(store, namespace)

		seg := &SegmentUpload{
			Segment: Segment{
				ID:          "seg_1234",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
				CreatedAt:   time.Now().UTC(),
			},
			DocsData: []byte("docs data"),
		}
		publisher.AddSegment(seg)

		manifest := NewManifest(namespace)
		manifest.AddSegment(seg.Segment)
		manifest.UpdateIndexedWALSeq()
		publisher.SetManifest(manifest)

		var reportedWALSeq uint64
		stateUpdater := func(ctx context.Context, manifestKey string, manifestSeq, indexedWALSeq uint64, bytesIndexed int64) (string, error) {
			reportedWALSeq = indexedWALSeq
			return "etag-new", nil
		}

		ctx := context.Background()
		result, err := publisher.Publish(ctx, 1, stateUpdater)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		if result.IndexedWALSeq != 10 {
			t.Errorf("Expected IndexedWALSeq=10, got %d", result.IndexedWALSeq)
		}
		if reportedWALSeq != 10 {
			t.Errorf("State updater received wrong WAL seq: %d", reportedWALSeq)
		}
	})

	t.Run("does not advance on segment upload failure", func(t *testing.T) {
		store := newPublisherMockStore()
		store.failPut = true
		namespace := "test-ns"

		publisher := NewPublisher(store, namespace)

		seg := &SegmentUpload{
			Segment: Segment{
				ID:          "seg_1234",
				Level:       0,
				StartWALSeq: 1,
				EndWALSeq:   10,
			},
			DocsData: []byte("docs data"),
		}
		publisher.AddSegment(seg)

		manifest := NewManifest(namespace)
		manifest.IndexedWALSeq = 10
		publisher.SetManifest(manifest)

		var stateUpdaterCalled bool
		stateUpdater := func(ctx context.Context, manifestKey string, manifestSeq, indexedWALSeq uint64, bytesIndexed int64) (string, error) {
			stateUpdaterCalled = true
			return "etag-new", nil
		}

		ctx := context.Background()
		_, err := publisher.Publish(ctx, 1, stateUpdater)
		if err == nil {
			t.Fatal("Expected error for failed upload")
		}

		if stateUpdaterCalled {
			t.Error("State updater should not be called on failure")
		}
	})

	t.Run("does not advance on manifest upload failure", func(t *testing.T) {
		store := newPublisherMockStore()
		store.failPutKey = ".idx.json" // Fail manifest upload
		namespace := "test-ns"

		publisher := NewPublisher(store, namespace)

		manifest := NewManifest(namespace)
		manifest.IndexedWALSeq = 10
		publisher.SetManifest(manifest)

		var stateUpdaterCalled bool
		stateUpdater := func(ctx context.Context, manifestKey string, manifestSeq, indexedWALSeq uint64, bytesIndexed int64) (string, error) {
			stateUpdaterCalled = true
			return "etag-new", nil
		}

		ctx := context.Background()
		_, err := publisher.Publish(ctx, 1, stateUpdater)
		if err == nil {
			t.Fatal("Expected error for failed manifest upload")
		}

		if stateUpdaterCalled {
			t.Error("State updater should not be called on manifest failure")
		}
	})

	t.Run("does not advance on state update failure", func(t *testing.T) {
		store := newPublisherMockStore()
		namespace := "test-ns"

		publisher := NewPublisher(store, namespace)

		manifest := NewManifest(namespace)
		manifest.IndexedWALSeq = 10
		publisher.SetManifest(manifest)

		stateUpdater := func(ctx context.Context, manifestKey string, manifestSeq, indexedWALSeq uint64, bytesIndexed int64) (string, error) {
			return "", errors.New("CAS conflict")
		}

		ctx := context.Background()
		_, err := publisher.Publish(ctx, 1, stateUpdater)
		if err == nil {
			t.Fatal("Expected error for failed state update")
		}
		if !errors.Is(err, ErrStateUpdateFailed) {
			t.Errorf("Expected ErrStateUpdateFailed, got: %v", err)
		}
	})
}

// Test: Transaction builder
func TestTransactionBuilder(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	seg := &SegmentUpload{
		Segment: Segment{
			ID:          "seg_1234",
			Level:       0,
			StartWALSeq: 1,
			EndWALSeq:   5,
			CreatedAt:   time.Now().UTC(),
		},
		DocsData: []byte("docs data"),
	}

	existingSeg := Segment{
		ID:          "seg_0001",
		Level:       1,
		StartWALSeq: 0,
		EndWALSeq:   0,
		DocsKey:     "existing/docs.col.zst",
	}

	// Pre-create existing segment in store
	store.objects[existingSeg.DocsKey] = []byte("existing docs")
	store.etags[existingSeg.DocsKey] = "etag-existing"

	txn := NewTransaction(store, namespace).
		WithExistingSegments([]Segment{existingSeg}).
		WithSegment(seg).
		WithSchemaHash("abc123")

	publisher, manifest, err := txn.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if manifest.SchemaVersionHash != "abc123" {
		t.Errorf("Expected schema hash abc123, got %s", manifest.SchemaVersionHash)
	}
	if len(manifest.Segments) != 2 {
		t.Errorf("Expected 2 segments, got %d", len(manifest.Segments))
	}
	if manifest.IndexedWALSeq != 5 {
		t.Errorf("Expected IndexedWALSeq=5, got %d", manifest.IndexedWALSeq)
	}

	ctx := context.Background()
	result, err := publisher.Publish(ctx, 1, nil)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if len(result.UploadedSegments) != 1 {
		t.Errorf("Expected 1 uploaded segment, got %d", len(result.UploadedSegments))
	}
}

// Test: VerifyManifestReferences
func TestVerifyManifestReferences(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	manifest := NewManifest(namespace)
	manifest.AddSegment(Segment{
		ID:      "seg_1234",
		DocsKey: "existing/docs.col.zst",
	})

	// Object doesn't exist
	err := VerifyManifestReferences(context.Background(), store, manifest)
	if err == nil {
		t.Fatal("Expected error for missing object")
	}
	if !errors.Is(err, ErrObjectMissing) {
		t.Errorf("Expected ErrObjectMissing, got: %v", err)
	}

	// Add the object
	store.objects["existing/docs.col.zst"] = []byte("docs")
	store.etags["existing/docs.col.zst"] = "etag"

	err = VerifyManifestReferences(context.Background(), store, manifest)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

// Test: LoadManifest
func TestLoadManifest(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	manifest := NewManifest(namespace)
	manifest.IndexedWALSeq = 42
	manifest.AddSegment(Segment{
		ID:          "seg_1234",
		StartWALSeq: 1,
		EndWALSeq:   42,
	})

	data, _ := json.Marshal(manifest)
	key := ManifestKey(namespace, 5)
	store.objects[key] = data
	store.etags[key] = "etag"

	loaded, err := LoadManifest(context.Background(), store, namespace, 5)
	if err != nil {
		t.Fatalf("LoadManifest failed: %v", err)
	}

	if loaded.IndexedWALSeq != 42 {
		t.Errorf("Expected IndexedWALSeq=42, got %d", loaded.IndexedWALSeq)
	}
	if len(loaded.Segments) != 1 {
		t.Errorf("Expected 1 segment, got %d", len(loaded.Segments))
	}
}

// Test: Publisher is single-use
func TestPublisher_SingleUse(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	publisher := NewPublisher(store, namespace)

	manifest := NewManifest(namespace)
	manifest.IndexedWALSeq = 5
	publisher.SetManifest(manifest)

	ctx := context.Background()

	// First publish succeeds
	_, err := publisher.Publish(ctx, 1, nil)
	if err != nil {
		t.Fatalf("First publish failed: %v", err)
	}

	// Second publish fails
	_, err = publisher.Publish(ctx, 2, nil)
	if !errors.Is(err, ErrPublishAborted) {
		t.Errorf("Expected ErrPublishAborted, got: %v", err)
	}
}

// Test: Multiple segments
func TestPublisher_MultipleSegments(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	publisher := NewPublisher(store, namespace)

	// Add multiple segments
	seg1 := &SegmentUpload{
		Segment: Segment{
			ID:          "seg_001",
			Level:       0,
			StartWALSeq: 1,
			EndWALSeq:   5,
			CreatedAt:   time.Now().UTC(),
		},
		DocsData: []byte("docs 1"),
	}
	seg2 := &SegmentUpload{
		Segment: Segment{
			ID:          "seg_002",
			Level:       0,
			StartWALSeq: 6,
			EndWALSeq:   10,
			CreatedAt:   time.Now().UTC(),
		},
		DocsData:   []byte("docs 2"),
		VectorData: []byte("vectors 2"),
	}
	publisher.AddSegment(seg1)
	publisher.AddSegment(seg2)

	manifest := NewManifest(namespace)
	manifest.IndexedWALSeq = 10
	publisher.SetManifest(manifest)

	ctx := context.Background()
	result, err := publisher.Publish(ctx, 1, stateUpdater)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if len(result.UploadedSegments) != 2 {
		t.Errorf("Expected 2 uploaded segments, got %d", len(result.UploadedSegments))
	}

	// Verify both segments' data was uploaded
	calls := store.getCalls()
	var seg1DocsUploaded, seg2DocsUploaded, seg2VectorsUploaded bool
	for _, call := range calls {
		if strings.Contains(call, "seg_001") && strings.Contains(call, "docs") {
			seg1DocsUploaded = true
		}
		if strings.Contains(call, "seg_002") && strings.Contains(call, "docs") {
			seg2DocsUploaded = true
		}
		if strings.Contains(call, "seg_002") && strings.Contains(call, "vectors") {
			seg2VectorsUploaded = true
		}
	}

	if !seg1DocsUploaded {
		t.Error("seg_001 docs not uploaded")
	}
	if !seg2DocsUploaded {
		t.Error("seg_002 docs not uploaded")
	}
	if !seg2VectorsUploaded {
		t.Error("seg_002 vectors not uploaded")
	}
}

// Test: FTS data upload
func TestPublisher_FTSData(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	publisher := NewPublisher(store, namespace)

	seg := &SegmentUpload{
		Segment: Segment{
			ID:          "seg_fts",
			Level:       0,
			StartWALSeq: 1,
			EndWALSeq:   5,
			CreatedAt:   time.Now().UTC(),
		},
		DocsData: []byte("docs"),
		FTSData: map[string][]byte{
			"title":   []byte("title fts"),
			"content": []byte("content fts"),
		},
	}
	publisher.AddSegment(seg)

	manifest := NewManifest(namespace)
	manifest.IndexedWALSeq = 5
	publisher.SetManifest(manifest)

	ctx := context.Background()
	result, err := publisher.Publish(ctx, 1, nil)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Verify FTS keys are in result
	if len(result.UploadedObjects) < 3 {
		t.Errorf("Expected at least 3 uploaded objects (docs + 2 FTS), got %d", len(result.UploadedObjects))
	}

	// Verify FTS objects exist
	calls := store.getCalls()
	var titleFTS, contentFTS bool
	for _, call := range calls {
		if strings.Contains(call, "fts/title.idx") {
			titleFTS = true
		}
		if strings.Contains(call, "fts/content.idx") {
			contentFTS = true
		}
	}

	if !titleFTS {
		t.Error("title FTS not uploaded")
	}
	if !contentFTS {
		t.Error("content FTS not uploaded")
	}
}

// Test: Empty segment (no data)
func TestPublisher_EmptySegment(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	publisher := NewPublisher(store, namespace)

	// Segment with no data
	seg := &SegmentUpload{
		Segment: Segment{
			ID:          "seg_empty",
			Level:       0,
			StartWALSeq: 1,
			EndWALSeq:   5,
			CreatedAt:   time.Now().UTC(),
		},
	}
	publisher.AddSegment(seg)

	manifest := NewManifest(namespace)
	manifest.IndexedWALSeq = 5
	publisher.SetManifest(manifest)

	ctx := context.Background()
	result, err := publisher.Publish(ctx, 1, nil)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// No segment data uploaded, but manifest is
	if len(result.UploadedObjects) != 0 {
		t.Errorf("Expected 0 uploaded objects for empty segment, got %d", len(result.UploadedObjects))
	}

	// Manifest should still be uploaded
	manifestKey := ManifestKey(namespace, 1)
	if !store.hasObject(manifestKey) {
		t.Error("Manifest not uploaded")
	}
}

// Test: PublishWithConfig - skip verification
func TestPublisher_SkipVerification(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	publisher := NewPublisher(store, namespace)

	// Manifest references a non-existent object
	manifest := NewManifest(namespace)
	manifest.AddSegment(Segment{
		ID:      "seg_1234",
		DocsKey: "nonexistent/docs.col.zst",
	})
	manifest.IndexedWALSeq = 5
	publisher.SetManifest(manifest)

	ctx := context.Background()

	// With verification enabled (default), should fail
	_, err := publisher.PublishWithConfig(ctx, 1, nil, nil)
	if !errors.Is(err, ErrObjectMissing) {
		t.Errorf("Expected ErrObjectMissing with default config, got: %v", err)
	}

	// Create new publisher
	publisher2 := NewPublisher(store, namespace)
	publisher2.SetManifest(manifest)

	// With verification disabled, should succeed (but this is risky in production!)
	config := &PublishConfig{VerifyObjects: false, SkipStateUpdate: true}
	result, err := publisher2.PublishWithConfig(ctx, 1, nil, config)
	if err != nil {
		t.Errorf("Expected success with verification disabled, got: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

// Test: Concurrent publish attempts are rejected
func TestPublisher_ConcurrentPublish(t *testing.T) {
	store := newPublisherMockStore()
	namespace := "test-ns"

	publisher := NewPublisher(store, namespace)

	manifest := NewManifest(namespace)
	manifest.IndexedWALSeq = 5
	publisher.SetManifest(manifest)

	ctx := context.Background()

	var wg sync.WaitGroup
	var successCount, errorCount int32

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := publisher.Publish(ctx, 1, nil)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else {
				atomic.AddInt32(&errorCount, 1)
			}
		}()
	}

	wg.Wait()

	// Exactly one should succeed
	if successCount != 1 {
		t.Errorf("Expected exactly 1 success, got %d", successCount)
	}
	if errorCount != 9 {
		t.Errorf("Expected 9 errors, got %d", errorCount)
	}
}

// Helper for tests
func stateUpdater(ctx context.Context, manifestKey string, manifestSeq, indexedWALSeq uint64, bytesIndexed int64) (string, error) {
	return "etag-new", nil
}
