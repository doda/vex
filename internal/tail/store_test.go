package tail

import (
	"bytes"
	"context"
	"io"
	"math"
	"testing"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// mockObjectStore implements a simple in-memory object store for testing.
type mockObjectStore struct {
	objects map[string][]byte
}

func newMockStore() *mockObjectStore {
	return &mockObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *mockObjectStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), &objectstore.ObjectInfo{Key: key, Size: int64(len(data))}, nil
}

func (m *mockObjectStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return &objectstore.ObjectInfo{Key: key, Size: int64(len(data))}, nil
}

func (m *mockObjectStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	m.objects[key] = data
	return &objectstore.ObjectInfo{Key: key, Size: int64(len(data))}, nil
}

func (m *mockObjectStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if _, ok := m.objects[key]; ok {
		return nil, objectstore.ErrAlreadyExists
	}
	return m.Put(ctx, key, body, size, opts)
}

func (m *mockObjectStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	return m.Put(ctx, key, body, size, opts)
}

func (m *mockObjectStore) Delete(ctx context.Context, key string) error {
	delete(m.objects, key)
	return nil
}

func (m *mockObjectStore) List(ctx context.Context, opts *objectstore.ListOptions) (*objectstore.ListResult, error) {
	var objects []objectstore.ObjectInfo
	for key, data := range m.objects {
		objects = append(objects, objectstore.ObjectInfo{Key: key, Size: int64(len(data))})
	}
	return &objectstore.ListResult{Objects: objects}, nil
}

func createTestWALEntry(namespace string, seq uint64, docs []testDoc) (*wal.WalEntry, []byte) {
	entry := wal.NewWalEntry(namespace, seq)
	batch := wal.NewWriteSubBatch("test-request")

	for _, doc := range docs {
		attrs := make(map[string]*wal.AttributeValue)
		for k, v := range doc.attrs {
			switch val := v.(type) {
			case string:
				attrs[k] = wal.StringValue(val)
			case int:
				attrs[k] = wal.IntValue(int64(val))
			case int64:
				attrs[k] = wal.IntValue(val)
			case float64:
				attrs[k] = wal.FloatValue(val)
			case bool:
				attrs[k] = wal.BoolValue(val)
			}
		}

		var vectorBytes []byte
		var dims uint32
		if doc.vector != nil {
			dims = uint32(len(doc.vector))
			vectorBytes = encodeVector(doc.vector)
		}

		protoID := &wal.DocumentID{Id: &wal.DocumentID_U64{U64: doc.id}}

		if doc.deleted {
			batch.AddDelete(protoID)
		} else {
			batch.AddUpsert(protoID, attrs, vectorBytes, dims)
		}
	}

	entry.SubBatches = append(entry.SubBatches, batch)

	encoder, _ := wal.NewEncoder()
	defer encoder.Close()
	result, _ := encoder.Encode(entry)

	return entry, result.Data
}

type testDoc struct {
	id      uint64
	attrs   map[string]any
	vector  []float32
	deleted bool
}

func encodeVector(vec []float32) []byte {
	buf := make([]byte, len(vec)*4)
	for i, v := range vec {
		bits := math.Float32bits(v)
		buf[i*4] = byte(bits)
		buf[i*4+1] = byte(bits >> 8)
		buf[i*4+2] = byte(bits >> 16)
		buf[i*4+3] = byte(bits >> 24)
	}
	return buf
}

func TestTailStore_AddWALEntry(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	entry := wal.NewWalEntry("test-namespace", 1)
	batch := wal.NewWriteSubBatch("req1")
	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{"name": wal.StringValue("doc1")},
		nil, 0,
	)
	entry.SubBatches = append(entry.SubBatches, batch)

	ts.AddWALEntry("test-namespace", entry)

	// Verify document is in tail
	ctx := context.Background()
	doc, err := ts.GetDocument(ctx, "test-namespace", document.NewU64ID(1))
	if err != nil {
		t.Fatalf("GetDocument failed: %v", err)
	}
	if doc == nil {
		t.Fatal("expected document, got nil")
	}
	if doc.Attributes["name"] != "doc1" {
		t.Errorf("expected name=doc1, got %v", doc.Attributes["name"])
	}
}

func TestTailStore_RefreshFromObjectStorage(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Create WAL entry and store in object store
	_, data := createTestWALEntry("test-ns", 1, []testDoc{
		{id: 1, attrs: map[string]any{"category": "A"}},
		{id: 2, attrs: map[string]any{"category": "B"}},
	})
	store.objects["test-ns/wal/1.wal.zst"] = data

	_, data2 := createTestWALEntry("test-ns", 2, []testDoc{
		{id: 3, attrs: map[string]any{"category": "C"}},
	})
	store.objects["test-ns/wal/2.wal.zst"] = data2

	// Refresh tail
	ctx := context.Background()
	err := ts.Refresh(ctx, "test-ns", 0, 2)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Verify all documents are available
	docs, err := ts.Scan(ctx, "test-ns", nil)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(docs) != 3 {
		t.Errorf("expected 3 docs, got %d", len(docs))
	}
}

func TestTailStore_RAMTier(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Add entries directly to RAM tier
	for i := uint64(1); i <= 5; i++ {
		entry := wal.NewWalEntry("test-ns", i)
		batch := wal.NewWriteSubBatch("req")
		batch.AddUpsert(
			&wal.DocumentID{Id: &wal.DocumentID_U64{U64: i}},
			map[string]*wal.AttributeValue{"seq": wal.IntValue(int64(i))},
			nil, 0,
		)
		entry.SubBatches = append(entry.SubBatches, batch)
		ts.AddWALEntry("test-ns", entry)
	}

	// Verify RAM tier contains all entries
	nt := ts.getNamespace("test-ns")
	if nt == nil {
		t.Fatal("namespace not found")
	}
	if len(nt.ramEntries) != 5 {
		t.Errorf("expected 5 RAM entries, got %d", len(nt.ramEntries))
	}
	if len(nt.documents) != 5 {
		t.Errorf("expected 5 documents, got %d", len(nt.documents))
	}
}

func TestTailStore_NVMeTier(t *testing.T) {
	store := newMockStore()
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: t.TempDir(),
		MaxBytes: 100 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	ts := New(DefaultConfig(), store, diskCache, nil)
	defer ts.Close()

	// Create WAL entry and store in object store
	_, data := createTestWALEntry("test-ns", 1, []testDoc{
		{id: 1, attrs: map[string]any{"value": 42}},
	})
	store.objects["test-ns/wal/1.wal.zst"] = data

	// Refresh should load from object store and cache to NVMe
	ctx := context.Background()
	err = ts.Refresh(ctx, "test-ns", 0, 1)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Verify document is accessible
	doc, err := ts.GetDocument(ctx, "test-ns", document.NewU64ID(1))
	if err != nil {
		t.Fatalf("GetDocument failed: %v", err)
	}
	if doc == nil {
		t.Fatal("expected document, got nil")
	}

	// Verify data was cached to disk
	cacheKey := cache.CacheKey{
		ObjectKey: "test-ns/wal/1.wal.zst",
		ETag:      "",
	}
	if !diskCache.Contains(cacheKey) {
		t.Error("expected WAL entry to be cached to disk")
	}
}

func TestTailStore_VectorScan(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Add documents with vectors
	_, data := createTestWALEntry("test-ns", 1, []testDoc{
		{id: 1, attrs: map[string]any{"name": "close"}, vector: []float32{1.0, 0.0, 0.0}},
		{id: 2, attrs: map[string]any{"name": "medium"}, vector: []float32{0.7, 0.7, 0.0}},
		{id: 3, attrs: map[string]any{"name": "far"}, vector: []float32{0.0, 1.0, 0.0}},
	})
	store.objects["test-ns/wal/1.wal.zst"] = data

	ctx := context.Background()
	err := ts.Refresh(ctx, "test-ns", 0, 1)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Search for vectors close to [1, 0, 0]
	query := []float32{1.0, 0.0, 0.0}
	results, err := ts.VectorScan(ctx, "test-ns", query, 2, MetricCosineDistance, nil)
	if err != nil {
		t.Fatalf("VectorScan failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// First result should be the closest (id=1, distance=0)
	if results[0].Doc.ID.U64() != 1 {
		t.Errorf("expected first result id=1, got %d", results[0].Doc.ID.U64())
	}
	if results[0].Distance > 0.01 {
		t.Errorf("expected distance ~0, got %f", results[0].Distance)
	}

	// Second result should be medium distance (id=2)
	if results[1].Doc.ID.U64() != 2 {
		t.Errorf("expected second result id=2, got %d", results[1].Doc.ID.U64())
	}
}

func TestTailStore_VectorScanEuclidean(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Add documents with vectors
	_, data := createTestWALEntry("test-ns", 1, []testDoc{
		{id: 1, vector: []float32{0.0, 0.0}},
		{id: 2, vector: []float32{1.0, 0.0}},
		{id: 3, vector: []float32{3.0, 4.0}}, // distance 5 from origin
	})
	store.objects["test-ns/wal/1.wal.zst"] = data

	ctx := context.Background()
	err := ts.Refresh(ctx, "test-ns", 0, 1)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	query := []float32{0.0, 0.0}
	results, err := ts.VectorScan(ctx, "test-ns", query, 3, MetricEuclideanSquared, nil)
	if err != nil {
		t.Fatalf("VectorScan failed: %v", err)
	}

	// Results should be sorted by euclidean squared distance
	if results[0].Doc.ID.U64() != 1 {
		t.Errorf("expected closest=1, got %d", results[0].Doc.ID.U64())
	}
	if results[0].Distance != 0 {
		t.Errorf("expected distance=0, got %f", results[0].Distance)
	}

	if results[1].Doc.ID.U64() != 2 {
		t.Errorf("expected second=2, got %d", results[1].Doc.ID.U64())
	}
	if results[1].Distance != 1.0 {
		t.Errorf("expected distance=1, got %f", results[1].Distance)
	}

	if results[2].Doc.ID.U64() != 3 {
		t.Errorf("expected third=3, got %d", results[2].Doc.ID.U64())
	}
	if results[2].Distance != 25.0 {
		t.Errorf("expected distance=25, got %f", results[2].Distance)
	}
}

func TestTailStore_FilterEvaluation(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Add documents with different attributes
	_, data := createTestWALEntry("test-ns", 1, []testDoc{
		{id: 1, attrs: map[string]any{"category": "A", "score": int64(10)}},
		{id: 2, attrs: map[string]any{"category": "B", "score": int64(20)}},
		{id: 3, attrs: map[string]any{"category": "A", "score": int64(30)}},
	})
	store.objects["test-ns/wal/1.wal.zst"] = data

	ctx := context.Background()
	err := ts.Refresh(ctx, "test-ns", 0, 1)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Filter by category
	f, err := filter.Parse([]any{"category", "Eq", "A"})
	if err != nil {
		t.Fatalf("Parse filter failed: %v", err)
	}

	docs, err := ts.Scan(ctx, "test-ns", f)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(docs) != 2 {
		t.Errorf("expected 2 docs with category=A, got %d", len(docs))
	}

	// Verify all results have category=A
	for _, doc := range docs {
		if doc.Attributes["category"] != "A" {
			t.Errorf("expected category=A, got %v", doc.Attributes["category"])
		}
	}
}

func TestTailStore_VectorScanWithFilter(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Add documents with vectors and categories
	_, data := createTestWALEntry("test-ns", 1, []testDoc{
		{id: 1, attrs: map[string]any{"category": "A"}, vector: []float32{1.0, 0.0}},
		{id: 2, attrs: map[string]any{"category": "B"}, vector: []float32{0.9, 0.1}},
		{id: 3, attrs: map[string]any{"category": "A"}, vector: []float32{0.5, 0.5}},
	})
	store.objects["test-ns/wal/1.wal.zst"] = data

	ctx := context.Background()
	err := ts.Refresh(ctx, "test-ns", 0, 1)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Filter by category=A, search for vectors close to [1, 0]
	f, _ := filter.Parse([]any{"category", "Eq", "A"})
	query := []float32{1.0, 0.0}
	results, err := ts.VectorScan(ctx, "test-ns", query, 10, MetricCosineDistance, f)
	if err != nil {
		t.Fatalf("VectorScan failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results (only category=A), got %d", len(results))
	}

	// First should be id=1 (perfect match)
	if results[0].Doc.ID.U64() != 1 {
		t.Errorf("expected first=1, got %d", results[0].Doc.ID.U64())
	}

	// All results should have category=A
	for _, r := range results {
		if r.Doc.Attributes["category"] != "A" {
			t.Errorf("expected category=A, got %v", r.Doc.Attributes["category"])
		}
	}
}

func TestTailStore_DeletedDocuments(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Add documents
	_, data := createTestWALEntry("test-ns", 1, []testDoc{
		{id: 1, attrs: map[string]any{"name": "doc1"}},
		{id: 2, attrs: map[string]any{"name": "doc2"}},
	})
	store.objects["test-ns/wal/1.wal.zst"] = data

	// Delete one document
	_, data2 := createTestWALEntry("test-ns", 2, []testDoc{
		{id: 1, deleted: true},
	})
	store.objects["test-ns/wal/2.wal.zst"] = data2

	ctx := context.Background()
	err := ts.Refresh(ctx, "test-ns", 0, 2)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Scan should not return deleted document
	docs, err := ts.Scan(ctx, "test-ns", nil)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(docs) != 1 {
		t.Errorf("expected 1 non-deleted doc, got %d", len(docs))
	}
	if docs[0].ID.U64() != 2 {
		t.Errorf("expected id=2, got %d", docs[0].ID.U64())
	}

	// GetDocument should return nil for deleted document
	doc, err := ts.GetDocument(ctx, "test-ns", document.NewU64ID(1))
	if err != nil {
		t.Fatalf("GetDocument failed: %v", err)
	}
	if doc != nil {
		t.Error("expected nil for deleted document")
	}
}

func TestTailStore_LastWriteWins(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Add document with initial value
	_, data := createTestWALEntry("test-ns", 1, []testDoc{
		{id: 1, attrs: map[string]any{"version": int64(1)}},
	})
	store.objects["test-ns/wal/1.wal.zst"] = data

	// Update document
	_, data2 := createTestWALEntry("test-ns", 2, []testDoc{
		{id: 1, attrs: map[string]any{"version": int64(2)}},
	})
	store.objects["test-ns/wal/2.wal.zst"] = data2

	ctx := context.Background()
	err := ts.Refresh(ctx, "test-ns", 0, 2)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Should get latest version
	doc, err := ts.GetDocument(ctx, "test-ns", document.NewU64ID(1))
	if err != nil {
		t.Fatalf("GetDocument failed: %v", err)
	}
	if doc == nil {
		t.Fatal("expected document, got nil")
	}
	if doc.Attributes["version"] != int64(2) {
		t.Errorf("expected version=2 (last write wins), got %v", doc.Attributes["version"])
	}
}

func TestTailStore_TailBytes(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Initially zero
	if bytes := ts.TailBytes("test-ns"); bytes != 0 {
		t.Errorf("expected 0 bytes initially, got %d", bytes)
	}

	// Add WAL entry
	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req")
	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{"data": wal.StringValue("some data here")},
		nil, 0,
	)
	entry.SubBatches = append(entry.SubBatches, batch)
	ts.AddWALEntry("test-ns", entry)

	// Should have non-zero bytes
	if bytes := ts.TailBytes("test-ns"); bytes == 0 {
		t.Error("expected non-zero tail bytes after adding entry")
	}
}

func TestTailStore_Clear(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Add data
	entry := wal.NewWalEntry("test-ns", 1)
	batch := wal.NewWriteSubBatch("req")
	batch.AddUpsert(
		&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
		map[string]*wal.AttributeValue{},
		nil, 0,
	)
	entry.SubBatches = append(entry.SubBatches, batch)
	ts.AddWALEntry("test-ns", entry)

	// Clear
	ts.Clear("test-ns")

	// Verify cleared
	ctx := context.Background()
	docs, _ := ts.Scan(ctx, "test-ns", nil)
	if len(docs) != 0 {
		t.Errorf("expected 0 docs after clear, got %d", len(docs))
	}

	if bytes := ts.TailBytes("test-ns"); bytes != 0 {
		t.Errorf("expected 0 bytes after clear, got %d", bytes)
	}
}

func TestVectorDistanceMetrics(t *testing.T) {
	a := []float32{1.0, 0.0, 0.0}
	b := []float32{0.0, 1.0, 0.0}

	// Cosine distance: orthogonal vectors have distance 1
	cosDist := computeDistance(a, b, MetricCosineDistance)
	if math.Abs(cosDist-1.0) > 0.001 {
		t.Errorf("expected cosine distance ~1.0, got %f", cosDist)
	}

	// Same vector has distance 0
	cosDist = computeDistance(a, a, MetricCosineDistance)
	if math.Abs(cosDist) > 0.001 {
		t.Errorf("expected cosine distance ~0, got %f", cosDist)
	}

	// Euclidean squared
	eucDist := computeDistance(a, b, MetricEuclideanSquared)
	if math.Abs(eucDist-2.0) > 0.001 {
		t.Errorf("expected euclidean squared ~2.0, got %f", eucDist)
	}

	// Dot product (negative for distance)
	dotDist := computeDistance(a, b, MetricDotProduct)
	if math.Abs(dotDist) > 0.001 {
		t.Errorf("expected dot product distance ~0, got %f", dotDist)
	}
}

func TestFloat16Conversion(t *testing.T) {
	testCases := []struct {
		name     string
		f16      uint16
		expected float32
	}{
		{"zero", 0x0000, 0.0},
		{"one", 0x3C00, 1.0},
		{"half", 0x3800, 0.5},
		{"two", 0x4000, 2.0},
		{"negative one", 0xBC00, -1.0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := float16ToFloat32(tc.f16)
			if math.Abs(float64(result-tc.expected)) > 0.001 {
				t.Errorf("float16ToFloat32(0x%04X) = %f, expected %f", tc.f16, result, tc.expected)
			}
		})
	}
}

func TestTailStore_ScanWithByteLimit(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Add multiple WAL entries with different sequences
	// Each entry has known size, allowing us to test byte limiting

	// Entry 1: oldest (seq=1)
	_, data1 := createTestWALEntry("test-ns", 1, []testDoc{
		{id: 1, attrs: map[string]any{"seq": int64(1)}},
	})
	store.objects["test-ns/wal/1.wal.zst"] = data1

	// Entry 2: middle (seq=2)
	_, data2 := createTestWALEntry("test-ns", 2, []testDoc{
		{id: 2, attrs: map[string]any{"seq": int64(2)}},
	})
	store.objects["test-ns/wal/2.wal.zst"] = data2

	// Entry 3: newest (seq=3)
	_, data3 := createTestWALEntry("test-ns", 3, []testDoc{
		{id: 3, attrs: map[string]any{"seq": int64(3)}},
	})
	store.objects["test-ns/wal/3.wal.zst"] = data3

	ctx := context.Background()
	err := ts.Refresh(ctx, "test-ns", 0, 3)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	// Get the namespace to understand entry sizes
	nt := ts.getNamespace("test-ns")
	if nt == nil {
		t.Fatal("namespace not found")
	}

	// Verify all documents are present without limit
	allDocs, err := ts.ScanWithByteLimit(ctx, "test-ns", nil, 0)
	if err != nil {
		t.Fatalf("ScanWithByteLimit(0) failed: %v", err)
	}
	if len(allDocs) != 3 {
		t.Errorf("expected 3 docs with no limit, got %d", len(allDocs))
	}

	// Calculate size of newest entry only
	newestEntrySize := nt.ramEntries[3].sizeBytes

	t.Run("byte limit includes only newest entry", func(t *testing.T) {
		// Limit to only fit the newest entry
		docs, err := ts.ScanWithByteLimit(ctx, "test-ns", nil, newestEntrySize)
		if err != nil {
			t.Fatalf("ScanWithByteLimit failed: %v", err)
		}

		// Should only return the newest document (id=3)
		if len(docs) != 1 {
			t.Errorf("expected 1 doc (newest only), got %d", len(docs))
		}
		if len(docs) > 0 && docs[0].ID.U64() != 3 {
			t.Errorf("expected doc id=3 (newest), got %d", docs[0].ID.U64())
		}
	})

	t.Run("byte limit uses newest first ordering", func(t *testing.T) {
		// Limit to fit 2 entries (should include seq=3 and seq=2, but not seq=1)
		twoEntriesSize := nt.ramEntries[3].sizeBytes + nt.ramEntries[2].sizeBytes

		docs, err := ts.ScanWithByteLimit(ctx, "test-ns", nil, twoEntriesSize)
		if err != nil {
			t.Fatalf("ScanWithByteLimit failed: %v", err)
		}

		// Should return docs from newest entries (id=3, id=2) but not oldest (id=1)
		if len(docs) != 2 {
			t.Errorf("expected 2 docs (newest 2), got %d", len(docs))
		}

		// Check that id=1 (oldest) is NOT included
		for _, doc := range docs {
			if doc.ID.U64() == 1 {
				t.Error("expected oldest doc (id=1) to be excluded due to byte limit")
			}
		}
	})
}

func TestTailStore_VectorScanWithByteLimit(t *testing.T) {
	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Add multiple WAL entries with different sequences and vectors

	// Entry 1: oldest (seq=1)
	_, data1 := createTestWALEntry("test-ns", 1, []testDoc{
		{id: 1, attrs: map[string]any{"seq": int64(1)}, vector: []float32{0.0, 1.0}},
	})
	store.objects["test-ns/wal/1.wal.zst"] = data1

	// Entry 2: middle (seq=2) - closest to query
	_, data2 := createTestWALEntry("test-ns", 2, []testDoc{
		{id: 2, attrs: map[string]any{"seq": int64(2)}, vector: []float32{1.0, 0.0}},
	})
	store.objects["test-ns/wal/2.wal.zst"] = data2

	// Entry 3: newest (seq=3)
	_, data3 := createTestWALEntry("test-ns", 3, []testDoc{
		{id: 3, attrs: map[string]any{"seq": int64(3)}, vector: []float32{0.5, 0.5}},
	})
	store.objects["test-ns/wal/3.wal.zst"] = data3

	ctx := context.Background()
	err := ts.Refresh(ctx, "test-ns", 0, 3)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	nt := ts.getNamespace("test-ns")
	if nt == nil {
		t.Fatal("namespace not found")
	}

	// Query close to [1, 0] - id=2 is closest
	query := []float32{1.0, 0.0}

	t.Run("vector scan with no limit returns all", func(t *testing.T) {
		results, err := ts.VectorScanWithByteLimit(ctx, "test-ns", query, 10, MetricCosineDistance, nil, 0)
		if err != nil {
			t.Fatalf("VectorScanWithByteLimit(0) failed: %v", err)
		}
		if len(results) != 3 {
			t.Errorf("expected 3 results with no limit, got %d", len(results))
		}
	})

	t.Run("vector scan with byte limit excludes oldest entries", func(t *testing.T) {
		// Limit to only the newest entry (seq=3)
		newestEntrySize := nt.ramEntries[3].sizeBytes

		results, err := ts.VectorScanWithByteLimit(ctx, "test-ns", query, 10, MetricCosineDistance, nil, newestEntrySize)
		if err != nil {
			t.Fatalf("VectorScanWithByteLimit failed: %v", err)
		}

		// Should only include doc from newest entry
		if len(results) != 1 {
			t.Errorf("expected 1 result (newest only), got %d", len(results))
		}
		if len(results) > 0 && results[0].Doc.ID.U64() != 3 {
			t.Errorf("expected result id=3, got %d", results[0].Doc.ID.U64())
		}
	})
}

func TestTailStore_ByteLimitNewestFirstOrdering(t *testing.T) {
	// This test specifically verifies that the 128 MiB window uses newest WAL entries first
	// per the spec: "Order unindexed WAL entries by seq descending (newest first)"

	store := newMockStore()
	ts := New(DefaultConfig(), store, nil, nil)
	defer ts.Close()

	// Create 5 entries with different sequences using the correct key format
	for i := uint64(1); i <= 5; i++ {
		_, data := createTestWALEntry("test-ns", i, []testDoc{
			{id: i, attrs: map[string]any{"seq": int64(i)}},
		})
		// KeyForSeq returns "wal/N.wal.zst", so the full key is "test-ns/" + KeyForSeq(i)
		store.objects["test-ns/"+wal.KeyForSeq(i)] = data
	}

	ctx := context.Background()
	err := ts.Refresh(ctx, "test-ns", 0, 5)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	nt := ts.getNamespace("test-ns")
	if nt == nil {
		t.Fatal("namespace not found")
	}

	// Verify we have all 5 entries
	if len(nt.ramEntries) != 5 {
		t.Fatalf("expected 5 RAM entries, got %d", len(nt.ramEntries))
	}

	// Calculate size to include only 2 newest entries (seq=5 and seq=4)
	twoNewestSize := nt.ramEntries[5].sizeBytes + nt.ramEntries[4].sizeBytes

	docs, err := ts.ScanWithByteLimit(ctx, "test-ns", nil, twoNewestSize)
	if err != nil {
		t.Fatalf("ScanWithByteLimit failed: %v", err)
	}

	// Should include only docs from seq=5 and seq=4 (newest first)
	if len(docs) != 2 {
		t.Fatalf("expected 2 docs (newest 2 entries), got %d", len(docs))
	}

	// Verify docs are from newest entries
	foundIDs := map[uint64]bool{}
	for _, doc := range docs {
		foundIDs[doc.ID.U64()] = true
	}

	if !foundIDs[5] {
		t.Error("expected doc id=5 (newest) to be included")
	}
	if !foundIDs[4] {
		t.Error("expected doc id=4 (second newest) to be included")
	}
	if foundIDs[1] || foundIDs[2] || foundIDs[3] {
		t.Error("expected older docs (id=1,2,3) to be excluded")
	}
}
