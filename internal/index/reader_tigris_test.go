//go:build integration
// +build integration

package index

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// prefixedStore wraps a store and adds a prefix to all keys
type prefixedStore struct {
	objectstore.Store
	prefix string
}

func (p *prefixedStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	return p.Store.Get(ctx, p.prefix+key, opts)
}

func (p *prefixedStore) Put(ctx context.Context, key string, reader io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	return p.Store.Put(ctx, p.prefix+key, reader, size, opts)
}

func (p *prefixedStore) PutIfAbsent(ctx context.Context, key string, reader io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	return p.Store.PutIfAbsent(ctx, p.prefix+key, reader, size, opts)
}

func (p *prefixedStore) PutIfMatch(ctx context.Context, key string, reader io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	return p.Store.PutIfMatch(ctx, p.prefix+key, reader, size, etag, opts)
}

func (p *prefixedStore) Delete(ctx context.Context, key string) error {
	return p.Store.Delete(ctx, p.prefix+key)
}

func (p *prefixedStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	return p.Store.Head(ctx, p.prefix+key)
}

func (p *prefixedStore) List(ctx context.Context, opts *objectstore.ListOptions) (*objectstore.ListResult, error) {
	if opts == nil {
		opts = &objectstore.ListOptions{}
	}
	newOpts := *opts
	newOpts.Prefix = p.prefix + opts.Prefix
	return p.Store.List(ctx, &newOpts)
}

// TestTigrisQueryPerformance tests query performance against real Tigris data.
// Run with: go test -tags=integration -run TestTigrisQueryPerformance -v
func TestTigrisQueryPerformance(t *testing.T) {
	endpoint := os.Getenv("TIGRIS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "https://t3.storage.dev"
	}
	accessKey := os.Getenv("TIGRIS_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = "tid_ewfaVYOPQFlHfiqyBoHPpWjpFbEHQNPFXhTZipLeupwQsrjTWe"
	}
	secretKey := os.Getenv("TIGRIS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = "tsec_TJ91jeZhVMv4dE3GrnV8W7PEFZkpev4byX4I_pYDoAPZTuXDxVf5O-q06r-AmYP9ZvAhoP"
	}
	bucket := os.Getenv("TIGRIS_BUCKET")
	if bucket == "" {
		bucket = "getajobdemo"
	}

	cfg := objectstore.S3Config{
		Endpoint:  endpoint,
		Bucket:    bucket,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    "auto",
		UseSSL:    true,
	}

	t.Logf("Connecting to Tigris: %s bucket=%s", endpoint, bucket)

	store, err := objectstore.NewS3Store(cfg)
	if err != nil {
		t.Fatalf("Failed to create S3 store: %v", err)
	}

	// Don't add prefix - ManifestKey already includes "vex/" prefix
	ctx := context.Background()

	// Create disk cache for testing
	cacheDir := "/tmp/vex-test-cache"
	os.MkdirAll(cacheDir, 0755)
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: cacheDir,
		MaxBytes: 1024 * 1024 * 1024, // 1GB
	})
	if err != nil {
		t.Logf("Warning: could not create disk cache: %v", err)
	}

	reader := NewReader(store, diskCache, nil)

	// Find the manifest
	namespace := "bsky_posts"
	manifestKey := ManifestKey(namespace, 1)
	t.Logf("Looking for manifest: %s", manifestKey)

	// Time manifest loading
	start := time.Now()
	manifest, err := reader.LoadManifest(ctx, manifestKey)
	manifestTime := time.Since(start)
	if err != nil {
		t.Fatalf("Failed to load manifest: %v", err)
	}
	if manifest == nil {
		// Try to find any manifest
		t.Log("Manifest not found at seq 1, trying to list objects...")
		listStart := time.Now()
		result, err := store.List(ctx, &objectstore.ListOptions{Prefix: "vex/namespaces/" + namespace + "/index/manifest"})
		listTime := time.Since(listStart)
		t.Logf("List took %v", listTime)
		if err != nil {
			t.Fatalf("Failed to list: %v", err)
		}
		for _, obj := range result.Objects {
			t.Logf("Found: %s", obj.Key)
		}
		t.Fatal("No manifest found")
	}

	t.Logf("Manifest loaded in %v", manifestTime)
	t.Logf("  Namespace: %s", manifest.Namespace)
	t.Logf("  Segments: %d", len(manifest.Segments))
	t.Logf("  IndexedWALSeq: %d", manifest.IndexedWALSeq)
	t.Logf("  ApproxRowCount: %d", manifest.Stats.ApproxRowCount)

	for i, seg := range manifest.Segments {
		t.Logf("  Segment %d: ID=%s Level=%d WAL=[%d-%d] Rows=%d",
			i, seg.ID, seg.Level, seg.StartWALSeq, seg.EndWALSeq, seg.Stats.RowCount)
		if seg.IVFKeys != nil {
			t.Logf("    IVF: clusters=%d vectors=%d", seg.IVFKeys.NClusters, seg.IVFKeys.VectorCount)
			t.Logf("    CentroidsKey: %s", seg.IVFKeys.CentroidsKey)
			t.Logf("    ClusterOffsetsKey: %s", seg.IVFKeys.ClusterOffsetsKey)
			t.Logf("    ClusterDataKey: %s", seg.IVFKeys.ClusterDataKey)
		}
	}

	// Time IVF reader creation
	start = time.Now()
	ivfReader, clusterDataKey, err := reader.GetIVFReader(ctx, namespace, manifestKey, 1)
	ivfTime := time.Since(start)
	if err != nil {
		t.Fatalf("Failed to get IVF reader: %v", err)
	}
	if ivfReader == nil {
		t.Fatal("IVF reader is nil")
	}
	t.Logf("IVF reader created in %v", ivfTime)
	t.Logf("  Dims: %d", ivfReader.Dims)
	t.Logf("  NClusters: %d", ivfReader.NClusters)
	t.Logf("  ClusterDataKey: %s", clusterDataKey)

	// Create a test query vector (768 dims for nomic-embed-text)
	dims := ivfReader.Dims
	queryVec := make([]float32, dims)
	for i := range queryVec {
		queryVec[i] = 0.1
	}

	// Run multiple queries to test caching
	topK := 10
	nProbe := 8 // Default

	t.Log("\n=== Query Performance Tests ===")

	for i := 0; i < 5; i++ {
		start = time.Now()
		results, err := reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, topK, nProbe)
		queryTime := time.Since(start)
		if err != nil {
			t.Errorf("Query %d failed: %v", i+1, err)
			continue
		}
		t.Logf("Query %d: %v - returned %d results", i+1, queryTime, len(results))
		if len(results) > 0 {
			t.Logf("  First result: DocID=%d Distance=%.4f", results[0].DocID, results[0].Distance)
		}
	}

	// Test with different nProbe values
	t.Log("\n=== nProbe Comparison ===")
	for _, np := range []int{1, 4, 8, 16, 32} {
		start = time.Now()
		results, err := reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, topK, np)
		queryTime := time.Since(start)
		if err != nil {
			t.Errorf("Query nProbe=%d failed: %v", np, err)
			continue
		}
		t.Logf("nProbe=%d: %v - returned %d results", np, queryTime, len(results))
	}
}

// TestTigrisS3RangePerformance tests raw S3 range read performance.
func TestTigrisS3RangePerformance(t *testing.T) {
	endpoint := os.Getenv("TIGRIS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "https://t3.storage.dev"
	}
	accessKey := os.Getenv("TIGRIS_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = "tid_ewfaVYOPQFlHfiqyBoHPpWjpFbEHQNPFXhTZipLeupwQsrjTWe"
	}
	secretKey := os.Getenv("TIGRIS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = "tsec_TJ91jeZhVMv4dE3GrnV8W7PEFZkpev4byX4I_pYDoAPZTuXDxVf5O-q06r-AmYP9ZvAhoP"
	}
	bucket := os.Getenv("TIGRIS_BUCKET")
	if bucket == "" {
		bucket = "getajobdemo"
	}

	cfg := objectstore.S3Config{
		Endpoint:  endpoint,
		Bucket:    bucket,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    "auto",
		UseSSL:    true,
	}

	store, err := objectstore.NewS3Store(cfg)
	if err != nil {
		t.Fatalf("Failed to create S3 store: %v", err)
	}
	ctx := context.Background()

	// List objects to find cluster data files
	t.Log("Listing cluster data files...")
	result, err := store.List(ctx, &objectstore.ListOptions{Prefix: "vex/namespaces/bsky_posts/index/segments"})
	if err != nil {
		t.Fatalf("Failed to list: %v", err)
	}

	var clusterDataKey string
	for _, obj := range result.Objects {
		t.Logf("Found: %s (size: %d)", obj.Key, obj.Size)
		if len(obj.Key) > 0 && obj.Size > 1000000 { // Find a large file
			clusterDataKey = obj.Key
		}
	}

	if clusterDataKey == "" {
		t.Fatal("No cluster data file found")
	}

	t.Logf("\nTesting range reads on: %s", clusterDataKey)

	// Test various range sizes
	rangeSizes := []int64{1024, 10240, 102400, 1024000, 5242880} // 1KB, 10KB, 100KB, 1MB, 5MB

	for _, size := range rangeSizes {
		opts := &objectstore.GetOptions{
			Range: &objectstore.ByteRange{
				Start: 0,
				End:   size - 1,
			},
		}

		start := time.Now()
		rdr, _, err := store.Get(ctx, clusterDataKey, opts)
		if err != nil {
			t.Errorf("Range read %d bytes failed: %v", size, err)
			continue
		}
		data := make([]byte, size)
		n, _ := io.ReadFull(rdr, data)
		rdr.Close()
		elapsed := time.Since(start)

		mbps := float64(n) / elapsed.Seconds() / 1024 / 1024
		t.Logf("Range %7d bytes: %v (%.1f MB/s)", size, elapsed, mbps)
	}

	// Test parallel range reads
	t.Log("\n=== Parallel Range Reads ===")
	numRanges := 8
	rangeSize := int64(1024 * 1024) // 1MB each

	start := time.Now()
	results := make(chan error, numRanges)
	for i := 0; i < numRanges; i++ {
		go func(idx int) {
			opts := &objectstore.GetOptions{
				Range: &objectstore.ByteRange{
					Start: int64(idx) * rangeSize,
					End:   int64(idx+1)*rangeSize - 1,
				},
			}
			rdr, _, err := store.Get(ctx, clusterDataKey, opts)
			if err != nil {
				results <- err
				return
			}
			data := make([]byte, rangeSize)
			_, err = io.ReadFull(rdr, data)
			rdr.Close()
			results <- err
		}(i)
	}

	for i := 0; i < numRanges; i++ {
		if err := <-results; err != nil {
			t.Errorf("Parallel range %d failed: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	totalBytes := int64(numRanges) * rangeSize
	mbps := float64(totalBytes) / elapsed.Seconds() / 1024 / 1024
	t.Logf("Parallel %d x %d bytes: %v (%.1f MB/s total)", numRanges, rangeSize, elapsed, mbps)
}

// BenchmarkTigrisANNQuery benchmarks ANN queries against Tigris.
func BenchmarkTigrisANNQuery(b *testing.B) {
	endpoint := os.Getenv("TIGRIS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "https://t3.storage.dev"
	}
	accessKey := os.Getenv("TIGRIS_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = "tid_ewfaVYOPQFlHfiqyBoHPpWjpFbEHQNPFXhTZipLeupwQsrjTWe"
	}
	secretKey := os.Getenv("TIGRIS_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = "tsec_TJ91jeZhVMv4dE3GrnV8W7PEFZkpev4byX4I_pYDoAPZTuXDxVf5O-q06r-AmYP9ZvAhoP"
	}
	bucket := os.Getenv("TIGRIS_BUCKET")
	if bucket == "" {
		bucket = "getajobdemo"
	}

	cfg := objectstore.S3Config{
		Endpoint:  endpoint,
		Bucket:    bucket,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    "auto",
		UseSSL:    true,
	}

	store, err := objectstore.NewS3Store(cfg)
	if err != nil {
		b.Fatalf("Failed to create S3 store: %v", err)
	}
	ctx := context.Background()

	cacheDir := "/tmp/vex-bench-cache"
	os.MkdirAll(cacheDir, 0755)
	diskCache, _ := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: cacheDir,
		MaxBytes: 1024 * 1024 * 1024, // 1GB
	})

	reader := NewReader(store, diskCache, nil)

	namespace := "bsky_posts"
	manifestKey := ManifestKey(namespace, 1)

	ivfReader, clusterDataKey, err := reader.GetIVFReader(ctx, namespace, manifestKey, 1)
	if err != nil || ivfReader == nil {
		b.Fatalf("Failed to get IVF reader: %v", err)
	}

	dims := ivfReader.Dims
	queryVec := make([]float32, dims)
	for i := range queryVec {
		queryVec[i] = 0.1
	}

	topK := 10
	nProbe := 8

	// Warm up cache
	reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, topK, nProbe)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := reader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, queryVec, topK, nProbe)
		if err != nil {
			b.Fatal(err)
		}
	}
}
