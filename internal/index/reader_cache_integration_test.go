package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/fts"
	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/pkg/objectstore"
)

type readerCacheFixture struct {
	ctx         context.Context
	store       objectstore.Store
	reader      *Reader
	namespace   string
	segment     Segment
	manifest    *Manifest
	manifestKey string
}

func TestReaderCacheInvalidatedOnManifestAdvance(t *testing.T) {
	fixture := setupReaderCacheFixture(t)

	if len(fixture.reader.readers) == 0 {
		t.Fatal("expected IVF reader cache to be populated")
	}
	if len(fixture.reader.ftsCache) == 0 {
		t.Fatal("expected FTS cache to be populated")
	}
	if len(fixture.reader.ftsTerms) == 0 {
		t.Fatal("expected FTS terms cache to be populated")
	}
	if len(fixture.reader.docOffsetsCache) == 0 {
		t.Fatal("expected doc offsets cache to be populated")
	}
	if len(fixture.reader.docIDRowCache) == 0 {
		t.Fatal("expected doc ID row cache to be populated")
	}

	manifestKey2 := ManifestKey(fixture.namespace, 2)
	manifestData, err := json.Marshal(fixture.manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	putObject(t, fixture.ctx, fixture.store, manifestKey2, manifestData)

	if _, err := fixture.reader.LoadManifest(fixture.ctx, manifestKey2); err != nil {
		t.Fatalf("LoadManifest() error = %v", err)
	}

	if len(fixture.reader.readers) != 0 {
		t.Errorf("expected IVF reader cache to be cleared, found %d", len(fixture.reader.readers))
	}
	if len(fixture.reader.ftsCache) != 0 {
		t.Errorf("expected FTS cache to be cleared, found %d", len(fixture.reader.ftsCache))
	}
	if len(fixture.reader.ftsTerms) != 0 {
		t.Errorf("expected FTS terms cache to be cleared, found %d", len(fixture.reader.ftsTerms))
	}
	if len(fixture.reader.docOffsetsCache) != 0 {
		t.Errorf("expected doc offsets cache to be cleared, found %d", len(fixture.reader.docOffsetsCache))
	}
	if len(fixture.reader.docIDRowCache) != 0 {
		t.Errorf("expected doc ID row cache to be cleared, found %d", len(fixture.reader.docIDRowCache))
	}
	if seq := fixture.reader.lastManifestSeq[fixture.namespace]; seq != 2 {
		t.Errorf("expected last manifest seq to be 2, got %d", seq)
	}
}

func TestReaderClearRemovesDocIDRowCache(t *testing.T) {
	fixture := setupReaderCacheFixture(t)

	if len(fixture.reader.docIDRowCache) == 0 {
		t.Fatal("expected doc ID row cache to be populated")
	}

	fixture.reader.Clear(fixture.namespace)

	if len(fixture.reader.docIDRowCache) != 0 {
		t.Errorf("expected doc ID row cache to be cleared, found %d", len(fixture.reader.docIDRowCache))
	}
}

func TestReaderCloseClearsDocIDRowCache(t *testing.T) {
	fixture := setupReaderCacheFixture(t)

	if len(fixture.reader.docIDRowCache) == 0 {
		t.Fatal("expected doc ID row cache to be populated")
	}

	if err := fixture.reader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if len(fixture.reader.docIDRowCache) != 0 {
		t.Errorf("expected doc ID row cache to be cleared, found %d", len(fixture.reader.docIDRowCache))
	}
	if len(fixture.reader.readers) != 0 {
		t.Errorf("expected IVF reader cache to be cleared, found %d", len(fixture.reader.readers))
	}
}

func setupReaderCacheFixture(t *testing.T) readerCacheFixture {
	t.Helper()

	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	namespace := "test-ns"
	segmentID := "seg_001"

	docsKey := fmt.Sprintf("vex/namespaces/%s/index/segments/%s/docs.col.zst", namespace, segmentID)
	centroidsKey := fmt.Sprintf("vex/namespaces/%s/index/segments/%s/vectors.centroids.bin", namespace, segmentID)
	clusterOffsetsKey := fmt.Sprintf("vex/namespaces/%s/index/segments/%s/vectors.cluster_offsets.bin", namespace, segmentID)
	clusterDataKey := fmt.Sprintf("vex/namespaces/%s/index/segments/%s/vectors.clusters.pack", namespace, segmentID)
	ftsKey := fmt.Sprintf("vex/namespaces/%s/index/segments/%s/fts.body.bm25", namespace, segmentID)
	ftsTermsKey := fmt.Sprintf("vex/namespaces/%s/index/segments/%s/fts.body.terms", namespace, segmentID)

	offsets := []vector.ClusterOffset{
		{Offset: 0, Length: uint64(8 + 2*4), DocCount: 1},
	}
	centroidsData := createTestCentroidsFile(t, 2, 1, vector.MetricCosineDistance)
	offsetsData := createTestOffsetsFile(t, offsets)
	clusterData := createTestClusterData(t, 2, offsets)

	putObject(t, ctx, store, centroidsKey, centroidsData)
	putObject(t, ctx, store, clusterOffsetsKey, offsetsData)
	putObject(t, ctx, store, clusterDataKey, clusterData)

	ftsIndex := fts.NewIndex("body", nil)
	ftsIndex.AddDocument(0, "hello world")
	ftsData, err := ftsIndex.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize FTS index: %v", err)
	}
	ftsTermsData, err := EncodeFTSTerms([]string{"hello", "world"})
	if err != nil {
		t.Fatalf("failed to encode FTS terms: %v", err)
	}
	putObject(t, ctx, store, ftsKey, ftsData)
	putObject(t, ctx, store, ftsTermsKey, ftsTermsData)

	offsetsKey := DocsOffsetsKey(docsKey)
	offsetsBytes := createTestDocOffsets(t, []uint64{0, 10})
	putObject(t, ctx, store, offsetsKey, offsetsBytes)

	idMapKey := DocsIDMapKey(docsKey)
	idMapData, err := EncodeDocIDMap([]uint64{1})
	if err != nil {
		t.Fatalf("failed to encode doc ID map: %v", err)
	}
	putObject(t, ctx, store, idMapKey, idMapData)

	segment := Segment{
		ID:          segmentID,
		Level:       0,
		StartWALSeq:  1,
		EndWALSeq:   1,
		DocsKey:     docsKey,
		IVFKeys: &IVFKeys{
			CentroidsKey:      centroidsKey,
			ClusterOffsetsKey: clusterOffsetsKey,
			ClusterDataKey:    clusterDataKey,
			NClusters:         1,
			VectorCount:       1,
		},
		FTSKeys:     []string{ftsKey},
		FTSTermKeys: []string{ftsTermsKey},
	}
	manifest := &Manifest{
		FormatVersion: 1,
		Namespace:     namespace,
		GeneratedAt:   time.Now().UTC(),
		IndexedWALSeq: 1,
		Segments:      []Segment{segment},
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal manifest: %v", err)
	}
	manifestKey := ManifestKey(namespace, 1)
	putObject(t, ctx, store, manifestKey, manifestData)

	reader := NewReader(store, nil, nil)
	if _, _, err := reader.GetIVFReader(ctx, namespace, manifestKey, 1); err != nil {
		t.Fatalf("GetIVFReader() error = %v", err)
	}
	if _, err := reader.LoadFTSIndexesForField(ctx, manifestKey, "body"); err != nil {
		t.Fatalf("LoadFTSIndexesForField() error = %v", err)
	}
	if _, err := reader.LoadFTSTermsForField(ctx, manifestKey, "body"); err != nil {
		t.Fatalf("LoadFTSTermsForField() error = %v", err)
	}
	if _, err := reader.getDocIDRowMap(ctx, segment); err != nil {
		t.Fatalf("getDocIDRowMap() error = %v", err)
	}

	return readerCacheFixture{
		ctx:         ctx,
		store:       store,
		reader:      reader,
		namespace:   namespace,
		segment:     segment,
		manifest:    manifest,
		manifestKey: manifestKey,
	}
}

func createTestDocOffsets(t *testing.T, offsets []uint64) []byte {
	t.Helper()

	if len(offsets) < 2 {
		t.Fatalf("need at least 2 offsets, got %d", len(offsets))
	}

	var buf bytes.Buffer
	buf.WriteString("DOFF")
	if err := binary.Write(&buf, binary.LittleEndian, uint32(1)); err != nil {
		t.Fatalf("failed to write doc offsets version: %v", err)
	}
	count := uint64(len(offsets) - 1)
	if err := binary.Write(&buf, binary.LittleEndian, count); err != nil {
		t.Fatalf("failed to write doc offsets count: %v", err)
	}
	for _, offset := range offsets {
		if err := binary.Write(&buf, binary.LittleEndian, offset); err != nil {
			t.Fatalf("failed to write doc offset: %v", err)
		}
	}
	return buf.Bytes()
}

func putObject(t *testing.T, ctx context.Context, store objectstore.Store, key string, data []byte) {
	t.Helper()

	if _, err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil); err != nil {
		t.Fatalf("failed to put object %s: %v", key, err)
	}
}
