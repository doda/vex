package indexer

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// encodeVectorForTest encodes a float32 vector to bytes.
func encodeVectorForTest(vec []float32) []byte {
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

// createVectorWALEntry creates a WAL entry with vector documents.
func createVectorWALEntry(namespace string, seq uint64, docs []vectorTestDoc) (*wal.WalEntry, []byte) {
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
			}
		}

		var vectorBytes []byte
		var dims uint32
		if doc.vector != nil {
			dims = uint32(len(doc.vector))
			vectorBytes = encodeVectorForTest(doc.vector)
		}

		protoID := &wal.DocumentID{Id: &wal.DocumentID_U64{U64: doc.id}}
		batch.AddUpsert(protoID, attrs, vectorBytes, dims)
	}

	entry.SubBatches = append(entry.SubBatches, batch)

	encoder, _ := wal.NewEncoder()
	defer encoder.Close()
	result, _ := encoder.Encode(entry)

	return entry, result.Data
}

type vectorTestDoc struct {
	id     uint64
	attrs  map[string]any
	vector []float32
}

// tailObjectStore wraps mockStore to implement the simple interface tail expects.
type tailObjectStore struct {
	*mockStore
}

func (t *tailObjectStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (interface {
	Read([]byte) (int, error)
	Close() error
}, *objectstore.ObjectInfo, error) {
	return t.mockStore.Get(ctx, key, opts)
}

// TestVectorIncrementalUpdates_Step1_TailExhaustiveScan verifies:
// Step 1: Test new vectors in tail searchable by exhaustive scan
func TestVectorIncrementalUpdates_Step1_TailExhaustiveScan(t *testing.T) {
	store := newMockStore()
	tailStore := tail.New(tail.DefaultConfig(), store, nil, nil)
	defer tailStore.Close()

	// Add documents with vectors via WAL
	docs := []vectorTestDoc{
		{id: 1, attrs: map[string]any{"category": "A"}, vector: []float32{1.0, 0.0, 0.0, 0.0}},
		{id: 2, attrs: map[string]any{"category": "B"}, vector: []float32{0.0, 1.0, 0.0, 0.0}},
		{id: 3, attrs: map[string]any{"category": "A"}, vector: []float32{0.5, 0.5, 0.0, 0.0}},
		{id: 4, attrs: map[string]any{"category": "C"}, vector: []float32{0.0, 0.0, 1.0, 0.0}},
		{id: 5, attrs: map[string]any{"category": "A"}, vector: []float32{0.9, 0.1, 0.0, 0.0}},
	}
	_, data := createVectorWALEntry("test-ns", 1, docs)
	store.mu.Lock()
	store.objects["vex/namespaces/test-ns/wal/00000000000000000001.wal.zst"] = mockObject{data: data, etag: "etag1"}
	store.mu.Unlock()

	// Refresh tail to load WAL
	ctx := context.Background()
	err := tailStore.Refresh(ctx, "test-ns", 0, 1)
	if err != nil {
		t.Fatalf("Refresh failed: %v", err)
	}

	t.Run("VectorsSearchableByExhaustiveScan", func(t *testing.T) {
		// Query for vectors close to [1, 0, 0, 0]
		query := []float32{1.0, 0.0, 0.0, 0.0}
		results, err := tailStore.VectorScan(ctx, "test-ns", query, 3, tail.MetricCosineDistance, nil)
		if err != nil {
			t.Fatalf("VectorScan failed: %v", err)
		}

		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}

		// First result should be id=1 (exact match, distance=0)
		if results[0].Doc.ID.U64() != 1 {
			t.Errorf("expected first result id=1, got %d", results[0].Doc.ID.U64())
		}
		if results[0].Distance > 0.01 {
			t.Errorf("expected distance ~0, got %f", results[0].Distance)
		}

		// Second should be id=5 (closest after exact match)
		if results[1].Doc.ID.U64() != 5 {
			t.Errorf("expected second result id=5, got %d", results[1].Doc.ID.U64())
		}

		// Third should be id=3
		if results[2].Doc.ID.U64() != 3 {
			t.Errorf("expected third result id=3, got %d", results[2].Doc.ID.U64())
		}
	})

	t.Run("IncrementalUpdatesSearchable", func(t *testing.T) {
		// Add more vectors in a new WAL entry
		newDocs := []vectorTestDoc{
			{id: 6, attrs: map[string]any{"category": "D"}, vector: []float32{0.95, 0.05, 0.0, 0.0}},
			{id: 7, attrs: map[string]any{"category": "D"}, vector: []float32{-1.0, 0.0, 0.0, 0.0}},
		}
		_, data2 := createVectorWALEntry("test-ns", 2, newDocs)
		store.mu.Lock()
		store.objects["vex/namespaces/test-ns/wal/00000000000000000002.wal.zst"] = mockObject{data: data2, etag: "etag2"}
		store.mu.Unlock()

		// Refresh to include new entry
		err := tailStore.Refresh(ctx, "test-ns", 1, 2)
		if err != nil {
			t.Fatalf("Refresh failed: %v", err)
		}

		// Search again - new vector id=6 should appear
		query := []float32{1.0, 0.0, 0.0, 0.0}
		results, err := tailStore.VectorScan(ctx, "test-ns", query, 3, tail.MetricCosineDistance, nil)
		if err != nil {
			t.Fatalf("VectorScan failed: %v", err)
		}

		// id=6 should be second closest now
		found := false
		for _, r := range results {
			if r.Doc.ID.U64() == 6 {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected new vector id=6 to be in top 3 results")
		}
	})

	t.Run("AllVectorsDiscovered", func(t *testing.T) {
		// Scan all - should find all 7 vectors
		allDocs, err := tailStore.Scan(ctx, "test-ns", nil)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		vectorCount := 0
		for _, doc := range allDocs {
			if doc.Vector != nil && len(doc.Vector) > 0 {
				vectorCount++
			}
		}

		if vectorCount != 7 {
			t.Errorf("expected 7 vectors, got %d", vectorCount)
		}
	})
}

// TestVectorIncrementalUpdates_Step2_IndexerFoldsTailToIVF verifies:
// Step 2: Verify indexer folds tail vectors into IVF segments
func TestVectorIncrementalUpdates_Step2_IndexerFoldsTailToIVF(t *testing.T) {
	store := newMockStore()
	stateMan := namespace.NewStateManager(store)

	// Create initial namespace state
	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Create WAL entries with vector documents
	docs := []vectorTestDoc{
		{id: 1, vector: []float32{1.0, 0.0, 0.0, 0.0}},
		{id: 2, vector: []float32{0.0, 1.0, 0.0, 0.0}},
		{id: 3, vector: []float32{0.5, 0.5, 0.0, 0.0}},
		{id: 4, vector: []float32{0.0, 0.0, 1.0, 0.0}},
		{id: 5, vector: []float32{0.0, 0.0, 0.0, 1.0}},
	}
	_, data := createVectorWALEntry("test-ns", 1, docs)
	store.mu.Lock()
	store.objects["vex/namespaces/test-ns/wal/00000000000000000001.wal.zst"] = mockObject{data: data, etag: "etag1"}
	store.mu.Unlock()

	// Advance WAL head_seq to 1 to match our WAL entry
	loaded, err := stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("Failed to load namespace: %v", err)
	}
	loaded, err = stateMan.AdvanceWAL(ctx, "test-ns", loaded.ETag, "vex/namespaces/test-ns/wal/00000000000000000001.wal.zst", int64(len(data)), nil)
	if err != nil {
		t.Fatalf("Failed to advance WAL: %v", err)
	}
	state := loaded.State

	// Create L0 segment processor and indexer
	idxer := New(store, stateMan, DefaultConfig(), nil)
	processor := NewL0SegmentProcessor(store, stateMan, nil, idxer)

	t.Run("IndexerBuildsIVFFromWAL", func(t *testing.T) {
		// Process WAL to build IVF segment
		result, err := processor.ProcessWAL(ctx, "test-ns", 0, 1, state, loaded.ETag)
		if err != nil {
			t.Fatalf("ProcessWAL failed: %v", err)
		}

		if result == nil || result.BytesIndexed == 0 {
			t.Error("expected non-zero bytes indexed")
		}

		// Check that IVF files were created
		foundCentroids := false
		foundOffsets := false
		foundClusters := false

		store.mu.RLock()
		for key := range store.objects {
			if bytes.Contains([]byte(key), []byte("centroids.bin")) {
				foundCentroids = true
			}
			if bytes.Contains([]byte(key), []byte("cluster_offsets.bin")) {
				foundOffsets = true
			}
			if bytes.Contains([]byte(key), []byte("clusters.pack")) {
				foundClusters = true
			}
		}
		store.mu.RUnlock()

		if !foundCentroids {
			t.Error("expected vectors.centroids.bin to be created")
		}
		if !foundOffsets {
			t.Error("expected vectors.cluster_offsets.bin to be created")
		}
		if !foundClusters {
			t.Error("expected vectors.clusters.pack to be created")
		}
	})

	t.Run("IVFFilesHaveCorrectFormat", func(t *testing.T) {
		// Find and validate centroids file
		store.mu.RLock()
		defer store.mu.RUnlock()
		for key, obj := range store.objects {
			if bytes.Contains([]byte(key), []byte("centroids.bin")) {
				// Check magic number
				if len(obj.data) < 4 {
					t.Fatal("centroids file too short")
				}
				magic := uint32(obj.data[0]) | uint32(obj.data[1])<<8 | uint32(obj.data[2])<<16 | uint32(obj.data[3])<<24
				if magic != vector.IVFMagic {
					t.Errorf("expected IVF magic 0x%X, got 0x%X", vector.IVFMagic, magic)
				}

				// Check version
				version := uint32(obj.data[4]) | uint32(obj.data[5])<<8 | uint32(obj.data[6])<<16 | uint32(obj.data[7])<<24
				if version != vector.IVFVersion {
					t.Errorf("expected IVF version %d, got %d", vector.IVFVersion, version)
				}
			}
		}
	})
}

// TestVectorIncrementalUpdates_Step3_L0SegmentsBuiltQuickly verifies:
// Step 3: Test L0 segments built quickly from WAL
func TestVectorIncrementalUpdates_Step3_L0SegmentsBuiltQuickly(t *testing.T) {
	store := newMockStore()
	stateMan := namespace.NewStateManager(store)

	ctx := context.Background()
	_, err := stateMan.Create(ctx, "test-ns")
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Create a larger set of vectors for performance testing
	docs := make([]vectorTestDoc, 100)
	for i := range docs {
		vec := make([]float32, 16) // 16-dimensional vectors
		for j := range vec {
			vec[j] = float32(i*16+j) / 1000.0
		}
		docs[i] = vectorTestDoc{id: uint64(i + 1), vector: vec}
	}
	_, data := createVectorWALEntry("test-ns", 1, docs)
	store.mu.Lock()
	store.objects["vex/namespaces/test-ns/wal/00000000000000000001.wal.zst"] = mockObject{data: data, etag: "etag1"}
	store.mu.Unlock()

	// Advance WAL head_seq to 1 to match our WAL entry
	loaded, err := stateMan.Load(ctx, "test-ns")
	if err != nil {
		t.Fatalf("Failed to load namespace: %v", err)
	}
	loaded, err = stateMan.AdvanceWAL(ctx, "test-ns", loaded.ETag, "vex/namespaces/test-ns/wal/00000000000000000001.wal.zst", int64(len(data)), nil)
	if err != nil {
		t.Fatalf("Failed to advance WAL: %v", err)
	}
	state := loaded.State

	idxer := New(store, stateMan, DefaultConfig(), nil)
	processor := NewL0SegmentProcessor(store, stateMan, &L0SegmentBuilderConfig{
		NClusters: 4, // Small number for fast building
		Metric:    vector.MetricCosineDistance,
	}, idxer)

	t.Run("L0SegmentBuildsFast", func(t *testing.T) {
		start := time.Now()

		result, err := processor.ProcessWAL(ctx, "test-ns", 0, 1, state, loaded.ETag)
		if err != nil {
			t.Fatalf("ProcessWAL failed: %v", err)
		}

		elapsed := time.Since(start)

		if result == nil || result.BytesIndexed == 0 {
			t.Error("expected non-zero bytes indexed")
		}

		// L0 segments should build in under 1 second for 100 vectors
		if elapsed > time.Second {
			t.Errorf("L0 segment build took too long: %v (expected < 1s)", elapsed)
		}

		t.Logf("Built L0 segment with 100 16-dim vectors in %v", elapsed)
	})

	t.Run("L0SegmentMetadataCorrect", func(t *testing.T) {
		// Find manifest file
		manifestFound := false
		store.mu.RLock()
		for key, obj := range store.objects {
			if bytes.Contains([]byte(key), []byte(".idx.json")) {
				manifestFound = true

				var manifest index.Manifest
				if err := manifest.UnmarshalJSON(obj.data); err != nil {
					store.mu.RUnlock()
					t.Fatalf("Failed to unmarshal manifest: %v", err)
				}

				if len(manifest.Segments) != 1 {
					t.Errorf("expected 1 segment, got %d", len(manifest.Segments))
				}

				if len(manifest.Segments) > 0 {
					seg := manifest.Segments[0]
					if seg.Level != index.L0 {
						t.Errorf("expected L0 segment, got level %d", seg.Level)
					}
					if seg.IVFKeys == nil {
						t.Error("expected IVF keys to be set")
					}
					if seg.IVFKeys != nil && seg.IVFKeys.VectorCount != 100 {
						t.Errorf("expected 100 vectors, got %d", seg.IVFKeys.VectorCount)
					}
					if seg.StartWALSeq != 1 || seg.EndWALSeq != 1 {
						t.Errorf("expected WAL range [1,1], got [%d,%d]", seg.StartWALSeq, seg.EndWALSeq)
					}
				}
				break
			}
		}
		store.mu.RUnlock()

		if !manifestFound {
			t.Error("manifest file not found")
		}
	})
}

// TestExtractVectorDocuments tests the document extraction logic.
func TestExtractVectorDocuments(t *testing.T) {
	t.Run("ExtractsVectorsFromWAL", func(t *testing.T) {
		entry := wal.NewWalEntry("test", 1)
		batch := wal.NewWriteSubBatch("req")

		// Add document with vector
		batch.AddUpsert(
			&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
			nil,
			encodeVectorForTest([]float32{1.0, 2.0, 3.0}),
			3,
		)
		entry.SubBatches = append(entry.SubBatches, batch)

		docs, dims := extractVectorDocuments([]*wal.WalEntry{entry})

		if len(docs) != 1 {
			t.Fatalf("expected 1 doc, got %d", len(docs))
		}
		if dims != 3 {
			t.Errorf("expected 3 dims, got %d", dims)
		}
		if docs[0].id != 1 {
			t.Errorf("expected id=1, got %d", docs[0].id)
		}
	})

	t.Run("DeduplicatesLastWriteWins", func(t *testing.T) {
		entry1 := wal.NewWalEntry("test", 1)
		batch1 := wal.NewWriteSubBatch("req")
		batch1.AddUpsert(
			&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
			nil,
			encodeVectorForTest([]float32{1.0, 0.0}),
			2,
		)
		entry1.SubBatches = append(entry1.SubBatches, batch1)

		entry2 := wal.NewWalEntry("test", 2)
		batch2 := wal.NewWriteSubBatch("req")
		batch2.AddUpsert(
			&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
			nil,
			encodeVectorForTest([]float32{0.0, 1.0}),
			2,
		)
		entry2.SubBatches = append(entry2.SubBatches, batch2)

		docs, _ := extractVectorDocuments([]*wal.WalEntry{entry1, entry2})

		if len(docs) != 1 {
			t.Fatalf("expected 1 doc after dedup, got %d", len(docs))
		}
		// Last write should win - vector should be [0, 1]
		if docs[0].vec[0] != 0.0 || docs[0].vec[1] != 1.0 {
			t.Errorf("expected last-write-wins vector [0,1], got %v", docs[0].vec)
		}
	})

	t.Run("HandlesDeletes", func(t *testing.T) {
		entry1 := wal.NewWalEntry("test", 1)
		batch1 := wal.NewWriteSubBatch("req")
		batch1.AddUpsert(
			&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}},
			nil,
			encodeVectorForTest([]float32{1.0, 0.0}),
			2,
		)
		entry1.SubBatches = append(entry1.SubBatches, batch1)

		entry2 := wal.NewWalEntry("test", 2)
		batch2 := wal.NewWriteSubBatch("req")
		batch2.AddDelete(&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}})
		entry2.SubBatches = append(entry2.SubBatches, batch2)

		docs, _ := extractVectorDocuments([]*wal.WalEntry{entry1, entry2})

		if len(docs) != 0 {
			t.Errorf("expected 0 docs after delete, got %d", len(docs))
		}
	})
}
