package cache_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// CachingLoader simulates the pattern used throughout Vex where we try cache
// first and fall back to object storage on cache miss.
type CachingLoader struct {
	diskCache *cache.DiskCache
	store     objectstore.Store
	loads     int
	mu        sync.Mutex
}

// Load attempts to load an object, using cache if available.
// This mirrors the pattern in internal/index/reader.go and internal/tail/store.go.
func (cl *CachingLoader) Load(ctx context.Context, key string, etag string) ([]byte, error) {
	cacheKey := cache.CacheKey{ObjectKey: key, ETag: etag}

	// Try cache first
	if cl.diskCache != nil {
		if reader, err := cl.diskCache.GetReader(cacheKey); err == nil {
			data, err := io.ReadAll(reader)
			reader.Close()
			if err == nil {
				return data, nil
			}
		}
	}

	// Cache miss: load from object storage (cold path)
	cl.mu.Lock()
	cl.loads++
	cl.mu.Unlock()

	reader, _, err := cl.store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// Cache the data for future use
	if cl.diskCache != nil {
		cl.diskCache.PutBytes(cacheKey, data)
	}

	return data, nil
}

// GetLoadCount returns the number of object storage loads.
func (cl *CachingLoader) GetLoadCount() int {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.loads
}

// TestMissingCachedObjectFetchedFromStorage verifies that when an object is not
// in cache, it is correctly fetched from object storage.
func TestMissingCachedObjectFetchedFromStorage(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create object store with test data
	store := objectstore.NewMemoryStore()
	testKey := "ns/test-object.bin"
	testData := []byte("test object data for cache correctness")
	if _, err := store.Put(ctx, testKey, bytes.NewReader(testData), int64(len(testData)), nil); err != nil {
		t.Fatalf("failed to put test data: %v", err)
	}

	// Create disk cache
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	// Create caching loader
	loader := &CachingLoader{
		diskCache: diskCache,
		store:     store,
	}

	// First load should be a cache miss and fetch from storage
	data1, err := loader.Load(ctx, testKey, "v1")
	if err != nil {
		t.Fatalf("first load failed: %v", err)
	}
	if !bytes.Equal(data1, testData) {
		t.Errorf("data mismatch: got %q, want %q", data1, testData)
	}
	if loader.GetLoadCount() != 1 {
		t.Errorf("expected 1 storage load, got %d", loader.GetLoadCount())
	}

	// Second load should hit the cache
	data2, err := loader.Load(ctx, testKey, "v1")
	if err != nil {
		t.Fatalf("second load failed: %v", err)
	}
	if !bytes.Equal(data2, testData) {
		t.Errorf("data mismatch on cache hit: got %q, want %q", data2, testData)
	}
	if loader.GetLoadCount() != 1 {
		t.Errorf("expected still 1 storage load (cache hit), got %d", loader.GetLoadCount())
	}
}

// TestColdPathAlwaysWorks verifies that after cache eviction, the cold path
// (fetching from object storage) continues to work correctly.
func TestColdPathAlwaysWorks(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create object store with test data
	store := objectstore.NewMemoryStore()
	objects := map[string][]byte{
		"obj1": bytes.Repeat([]byte("a"), 100),
		"obj2": bytes.Repeat([]byte("b"), 100),
		"obj3": bytes.Repeat([]byte("c"), 100),
		"obj4": bytes.Repeat([]byte("d"), 100),
	}
	for key, data := range objects {
		if _, err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil); err != nil {
			t.Fatalf("failed to put %s: %v", key, err)
		}
	}

	// Create a very small disk cache that can only hold 2 objects
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 250, // Only holds ~2 objects of 100 bytes each
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	loader := &CachingLoader{
		diskCache: diskCache,
		store:     store,
	}

	// Load obj1 and obj2 (fills the cache)
	for _, key := range []string{"obj1", "obj2"} {
		data, err := loader.Load(ctx, key, "v1")
		if err != nil {
			t.Fatalf("load %s failed: %v", key, err)
		}
		if !bytes.Equal(data, objects[key]) {
			t.Errorf("%s data mismatch", key)
		}
	}
	initialLoads := loader.GetLoadCount()
	if initialLoads != 2 {
		t.Fatalf("expected 2 initial loads, got %d", initialLoads)
	}

	// Load obj3 and obj4 (causes eviction of obj1 and obj2)
	for _, key := range []string{"obj3", "obj4"} {
		data, err := loader.Load(ctx, key, "v1")
		if err != nil {
			t.Fatalf("load %s failed: %v", key, err)
		}
		if !bytes.Equal(data, objects[key]) {
			t.Errorf("%s data mismatch", key)
		}
	}
	loadsAfterEviction := loader.GetLoadCount()
	if loadsAfterEviction != 4 {
		t.Fatalf("expected 4 loads after eviction, got %d", loadsAfterEviction)
	}

	// CRITICAL TEST: Load obj1 again - it was evicted, so cold path must work
	data1, err := loader.Load(ctx, "obj1", "v1")
	if err != nil {
		t.Fatalf("load evicted obj1 failed: %v", err)
	}
	if !bytes.Equal(data1, objects["obj1"]) {
		t.Errorf("evicted obj1 data mismatch: got %q, want %q", data1, objects["obj1"])
	}

	// Verify it was fetched from storage (not cache)
	if loader.GetLoadCount() != 5 {
		t.Errorf("expected 5 loads (obj1 refetched), got %d", loader.GetLoadCount())
	}

	// Load obj2 again - also evicted, cold path must work
	data2, err := loader.Load(ctx, "obj2", "v1")
	if err != nil {
		t.Fatalf("load evicted obj2 failed: %v", err)
	}
	if !bytes.Equal(data2, objects["obj2"]) {
		t.Errorf("evicted obj2 data mismatch: got %q, want %q", data2, objects["obj2"])
	}
}

// TestColdPathWithCacheDisabled verifies the cold path works when cache is nil.
func TestColdPathWithCacheDisabled(t *testing.T) {
	ctx := context.Background()

	store := objectstore.NewMemoryStore()
	testKey := "test/object.bin"
	testData := []byte("data without cache")
	if _, err := store.Put(ctx, testKey, bytes.NewReader(testData), int64(len(testData)), nil); err != nil {
		t.Fatalf("failed to put test data: %v", err)
	}

	// No cache - should still work via cold path
	loader := &CachingLoader{
		diskCache: nil,
		store:     store,
	}

	// Every load should go to storage
	for i := 0; i < 3; i++ {
		data, err := loader.Load(ctx, testKey, "v1")
		if err != nil {
			t.Fatalf("load %d failed: %v", i, err)
		}
		if !bytes.Equal(data, testData) {
			t.Errorf("load %d data mismatch", i)
		}
	}

	// All 3 loads should have gone to storage
	if loader.GetLoadCount() != 3 {
		t.Errorf("expected 3 storage loads, got %d", loader.GetLoadCount())
	}
}

// TestColdPathWithDifferentETags verifies content-addressable caching works
// correctly with different versions (ETags).
func TestColdPathWithDifferentETags(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	store := objectstore.NewMemoryStore()
	testKey := "versioned/object.bin"

	// Put first version
	v1Data := []byte("version 1 data")
	if _, err := store.Put(ctx, testKey, bytes.NewReader(v1Data), int64(len(v1Data)), nil); err != nil {
		t.Fatalf("failed to put v1: %v", err)
	}

	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	loader := &CachingLoader{
		diskCache: diskCache,
		store:     store,
	}

	// Load v1 - should cache it
	data1, err := loader.Load(ctx, testKey, "etag-v1")
	if err != nil {
		t.Fatalf("load v1 failed: %v", err)
	}
	if !bytes.Equal(data1, v1Data) {
		t.Errorf("v1 data mismatch")
	}

	// Update object in storage to v2
	v2Data := []byte("version 2 data - updated")
	if _, err := store.Put(ctx, testKey, bytes.NewReader(v2Data), int64(len(v2Data)), nil); err != nil {
		t.Fatalf("failed to put v2: %v", err)
	}

	// Load with different ETag - should be a cache miss and get v2
	data2, err := loader.Load(ctx, testKey, "etag-v2")
	if err != nil {
		t.Fatalf("load v2 failed: %v", err)
	}
	if !bytes.Equal(data2, v2Data) {
		t.Errorf("v2 data mismatch: got %q, want %q", data2, v2Data)
	}

	// Both loads should have gone to storage (different ETags = different cache keys)
	if loader.GetLoadCount() != 2 {
		t.Errorf("expected 2 storage loads, got %d", loader.GetLoadCount())
	}

	// Loading v1 etag should still return v1 from cache (different cache entries)
	data1Again, err := loader.Load(ctx, testKey, "etag-v1")
	if err != nil {
		t.Fatalf("load v1 again failed: %v", err)
	}
	if !bytes.Equal(data1Again, v1Data) {
		t.Errorf("v1 again data mismatch: got %q, want %q", data1Again, v1Data)
	}
	// Should not have caused another storage load (cache hit)
	if loader.GetLoadCount() != 2 {
		t.Errorf("expected still 2 storage loads, got %d", loader.GetLoadCount())
	}
}

// TestConcurrentColdPath verifies concurrent cold path accesses work correctly.
func TestConcurrentColdPath(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	store := objectstore.NewMemoryStore()

	// Create multiple objects
	numObjects := 10
	for i := 0; i < numObjects; i++ {
		key := keyForIndex(i)
		data := bytes.Repeat([]byte{byte(i)}, 100)
		if _, err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil); err != nil {
			t.Fatalf("failed to put %s: %v", key, err)
		}
	}

	// Small cache to force evictions
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 350, // Only holds ~3 objects
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	loader := &CachingLoader{
		diskCache: diskCache,
		store:     store,
	}

	// Concurrent access to all objects (causes evictions)
	var wg sync.WaitGroup
	errors := make(chan error, numObjects*3)

	for round := 0; round < 3; round++ {
		for i := 0; i < numObjects; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := keyForIndex(idx)
				expected := bytes.Repeat([]byte{byte(idx)}, 100)

				data, err := loader.Load(ctx, key, "v1")
				if err != nil {
					errors <- err
					return
				}
				if !bytes.Equal(data, expected) {
					errors <- bytes.ErrTooLarge // Just need some error
				}
			}(i)
		}
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent load failed: %v", err)
	}
}

func keyForIndex(i int) string {
	return "obj" + string(rune('0'+i))
}

// TestRAMCacheMissWithDiskCacheFallback tests the two-tier cache behavior
// where RAM cache miss falls back to disk cache, then to object storage.
func TestRAMCacheMissWithDiskCacheFallback(t *testing.T) {
	tmpDir := t.TempDir()

	// Create disk cache
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	// Create RAM cache (small)
	ramCache := cache.NewMemoryCache(cache.MemoryCacheConfig{
		MaxBytes: 200, // Very small
	})

	// Put data in disk cache
	diskKey := cache.CacheKey{ObjectKey: "test/data.bin", ETag: "v1"}
	testData := []byte("test data for two-tier cache")
	_, err = diskCache.PutBytes(diskKey, testData)
	if err != nil {
		t.Fatalf("failed to put in disk cache: %v", err)
	}

	// RAM cache doesn't have it
	ramKey := cache.MemoryCacheKey{
		Namespace: "test",
		ShardID:   "shard1",
		ItemID:    "data.bin",
		ItemType:  cache.TypeCentroid,
	}
	if ramCache.Contains(ramKey) {
		t.Error("RAM cache should not contain the key initially")
	}

	// Disk cache should have it
	if !diskCache.Contains(diskKey) {
		t.Error("disk cache should contain the key")
	}

	// Simulate the pattern: try RAM, fall back to disk
	reader, err := diskCache.GetReader(diskKey)
	if err != nil {
		t.Fatalf("disk cache read failed: %v", err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if !bytes.Equal(data, testData) {
		t.Errorf("data mismatch: got %q, want %q", data, testData)
	}

	// Promote to RAM cache
	err = ramCache.Put(ramKey, data)
	if err != nil {
		t.Fatalf("failed to put in RAM cache: %v", err)
	}

	// Now RAM cache should have it
	if !ramCache.Contains(ramKey) {
		t.Error("RAM cache should contain the key after promotion")
	}

	// Getting from RAM should return the same data
	ramData, err := ramCache.Get(ramKey)
	if err != nil {
		t.Fatalf("RAM cache get failed: %v", err)
	}
	if !bytes.Equal(ramData, testData) {
		t.Errorf("RAM data mismatch: got %q, want %q", ramData, testData)
	}
}

// TestCacheEvictionDoesNotCorruptData verifies that partial eviction
// doesn't corrupt the remaining cached data.
func TestCacheEvictionDoesNotCorruptData(t *testing.T) {
	tmpDir := t.TempDir()

	// Cache that holds exactly 2 entries
	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 250,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	// Create distinct data patterns for verification
	data1 := bytes.Repeat([]byte{0xAA}, 100)
	data2 := bytes.Repeat([]byte{0xBB}, 100)
	data3 := bytes.Repeat([]byte{0xCC}, 100)

	key1 := cache.CacheKey{ObjectKey: "obj1", ETag: "v1"}
	key2 := cache.CacheKey{ObjectKey: "obj2", ETag: "v1"}
	key3 := cache.CacheKey{ObjectKey: "obj3", ETag: "v1"}

	// Fill cache with obj1 and obj2
	if _, err := diskCache.PutBytes(key1, data1); err != nil {
		t.Fatalf("failed to put obj1: %v", err)
	}
	if _, err := diskCache.PutBytes(key2, data2); err != nil {
		t.Fatalf("failed to put obj2: %v", err)
	}

	// Verify both are correct
	got1 := readCacheData(t, diskCache, key1)
	if !bytes.Equal(got1, data1) {
		t.Errorf("obj1 corrupted before eviction")
	}

	got2 := readCacheData(t, diskCache, key2)
	if !bytes.Equal(got2, data2) {
		t.Errorf("obj2 corrupted before eviction")
	}

	// Add obj3, causing eviction
	if _, err := diskCache.PutBytes(key3, data3); err != nil {
		t.Fatalf("failed to put obj3: %v", err)
	}

	// obj2 should still be correct (not evicted - was accessed more recently)
	if diskCache.Contains(key2) {
		got2 = readCacheData(t, diskCache, key2)
		if !bytes.Equal(got2, data2) {
			t.Errorf("obj2 corrupted after eviction")
		}
	}

	// obj3 should be correct
	got3 := readCacheData(t, diskCache, key3)
	if !bytes.Equal(got3, data3) {
		t.Errorf("obj3 corrupted after eviction")
	}
}

// TestColdPathWithEmptyObject verifies cold path handles empty objects correctly.
func TestColdPathWithEmptyObject(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	store := objectstore.NewMemoryStore()
	testKey := "empty/object.bin"
	testData := []byte{} // Empty

	if _, err := store.Put(ctx, testKey, bytes.NewReader(testData), 0, nil); err != nil {
		t.Fatalf("failed to put empty object: %v", err)
	}

	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	loader := &CachingLoader{
		diskCache: diskCache,
		store:     store,
	}

	// Load empty object
	data, err := loader.Load(ctx, testKey, "v1")
	if err != nil {
		t.Fatalf("load empty object failed: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected empty data, got %d bytes", len(data))
	}
}

// TestColdPathNonExistentObject verifies cold path correctly handles
// objects that don't exist in storage.
func TestColdPathNonExistentObject(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	store := objectstore.NewMemoryStore()

	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	loader := &CachingLoader{
		diskCache: diskCache,
		store:     store,
	}

	// Try to load non-existent object
	_, err = loader.Load(ctx, "nonexistent/object.bin", "v1")
	if err == nil {
		t.Error("expected error loading non-existent object")
	}
	if !objectstore.IsNotFoundError(err) {
		t.Errorf("expected not found error, got: %v", err)
	}
}

func readCacheData(t *testing.T, diskCache *cache.DiskCache, key cache.CacheKey) []byte {
	t.Helper()

	reader, err := diskCache.GetReader(key)
	if err != nil {
		t.Fatalf("cache read failed for %s: %v", key.ObjectKey, err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("cache read failed for %s: %v", key.ObjectKey, err)
	}

	return data
}
