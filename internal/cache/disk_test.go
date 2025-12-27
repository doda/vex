package cache

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewDiskCache(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DiskCacheConfig{
		RootPath:  tmpDir,
		MaxBytes:  1024 * 1024, // 1MB
		BudgetPct: 95,
	}

	dc, err := NewDiskCache(cfg)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	stats := dc.Stats()
	if stats.MaxBytes != cfg.MaxBytes {
		t.Errorf("MaxBytes = %d, want %d", stats.MaxBytes, cfg.MaxBytes)
	}
	if stats.BudgetPct != cfg.BudgetPct {
		t.Errorf("BudgetPct = %d, want %d", stats.BudgetPct, cfg.BudgetPct)
	}
	if stats.UsedBytes != 0 {
		t.Errorf("UsedBytes = %d, want 0", stats.UsedBytes)
	}
}

func TestDiskCache_PutGet(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	key := CacheKey{ObjectKey: "test/object.bin", ETag: "abc123"}
	data := []byte("hello world")

	// Put data
	path, err := dc.PutBytes(key, data)
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("cached file not found: %v", err)
	}

	// Get should return same path
	gotPath, err := dc.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if gotPath != path {
		t.Errorf("Get returned %s, want %s", gotPath, path)
	}

	// Read and verify content
	content, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("content = %s, want %s", content, data)
	}

	// Stats should reflect the entry
	stats := dc.Stats()
	if stats.EntryCount != 1 {
		t.Errorf("EntryCount = %d, want 1", stats.EntryCount)
	}
	if stats.UsedBytes != int64(len(data)) {
		t.Errorf("UsedBytes = %d, want %d", stats.UsedBytes, len(data))
	}
}

func TestDiskCache_ContentAddressable(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Same object key but different etags should be different cache entries
	key1 := CacheKey{ObjectKey: "test/object.bin", ETag: "v1"}
	key2 := CacheKey{ObjectKey: "test/object.bin", ETag: "v2"}

	data1 := []byte("version 1")
	data2 := []byte("version 2")

	path1, err := dc.PutBytes(key1, data1)
	if err != nil {
		t.Fatalf("PutBytes key1 failed: %v", err)
	}

	path2, err := dc.PutBytes(key2, data2)
	if err != nil {
		t.Fatalf("PutBytes key2 failed: %v", err)
	}

	if path1 == path2 {
		t.Error("different etags should produce different cache paths")
	}

	stats := dc.Stats()
	if stats.EntryCount != 2 {
		t.Errorf("EntryCount = %d, want 2", stats.EntryCount)
	}

	// Verify content is different
	content1, _ := os.ReadFile(path1)
	content2, _ := os.ReadFile(path2)

	if !bytes.Equal(content1, data1) {
		t.Errorf("content1 = %s, want %s", content1, data1)
	}
	if !bytes.Equal(content2, data2) {
		t.Errorf("content2 = %s, want %s", content2, data2)
	}
}

func TestDiskCache_Miss(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	key := CacheKey{ObjectKey: "nonexistent", ETag: "xxx"}
	_, err = dc.Get(key)
	if err != ErrCacheMiss {
		t.Errorf("Get nonexistent = %v, want ErrCacheMiss", err)
	}
}

func TestDiskCache_LRUEviction(t *testing.T) {
	tmpDir := t.TempDir()

	// Small cache that can hold only 2 entries of 100 bytes each
	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 250,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	data := make([]byte, 100)

	// Insert 3 entries - the first should be evicted
	key1 := CacheKey{ObjectKey: "obj1", ETag: "e1"}
	key2 := CacheKey{ObjectKey: "obj2", ETag: "e2"}
	key3 := CacheKey{ObjectKey: "obj3", ETag: "e3"}

	dc.PutBytes(key1, data)
	dc.PutBytes(key2, data)
	dc.PutBytes(key3, data)

	// key1 should be evicted
	if dc.Contains(key1) {
		t.Error("key1 should have been evicted")
	}
	if !dc.Contains(key2) {
		t.Error("key2 should still be cached")
	}
	if !dc.Contains(key3) {
		t.Error("key3 should still be cached")
	}
}

func TestDiskCache_LRUAccessTimeUpdate(t *testing.T) {
	tmpDir := t.TempDir()

	// Cache that can hold only 2 entries
	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 250,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	data := make([]byte, 100)

	key1 := CacheKey{ObjectKey: "obj1", ETag: "e1"}
	key2 := CacheKey{ObjectKey: "obj2", ETag: "e2"}
	key3 := CacheKey{ObjectKey: "obj3", ETag: "e3"}

	dc.PutBytes(key1, data)
	dc.PutBytes(key2, data)

	// Access key1 to make it recently used
	dc.Get(key1)

	// Insert key3 - key2 should be evicted (it's the LRU)
	dc.PutBytes(key3, data)

	if !dc.Contains(key1) {
		t.Error("key1 should still be cached (was accessed recently)")
	}
	if dc.Contains(key2) {
		t.Error("key2 should have been evicted (LRU)")
	}
	if !dc.Contains(key3) {
		t.Error("key3 should still be cached")
	}
}

func TestDiskCache_Pin(t *testing.T) {
	tmpDir := t.TempDir()

	// Small cache
	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 250,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	data := make([]byte, 100)

	// Insert entries with namespace prefixes
	key1 := CacheKey{ObjectKey: "ns1/obj1", ETag: "e1"}
	key2 := CacheKey{ObjectKey: "ns2/obj2", ETag: "e2"}

	dc.PutBytes(key1, data)
	dc.PutBytes(key2, data)

	// Pin ns1
	dc.Pin("ns1/")

	if !dc.IsPinned("ns1/") {
		t.Error("ns1/ should be pinned")
	}

	// Insert a third entry - only ns2 can be evicted
	key3 := CacheKey{ObjectKey: "ns3/obj3", ETag: "e3"}
	dc.PutBytes(key3, data)

	// ns1/obj1 should still be present (pinned)
	if !dc.Contains(key1) {
		t.Error("key1 should still be cached (pinned namespace)")
	}

	// ns2/obj2 should be evicted
	if dc.Contains(key2) {
		t.Error("key2 should have been evicted")
	}

	// Unpin and verify
	dc.Unpin("ns1/")
	if dc.IsPinned("ns1/") {
		t.Error("ns1/ should not be pinned after Unpin")
	}
}

func TestDiskCache_Delete(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	key := CacheKey{ObjectKey: "test", ETag: "e1"}
	data := []byte("test data")

	path, _ := dc.PutBytes(key, data)
	if !dc.Contains(key) {
		t.Error("key should be in cache after Put")
	}

	dc.Delete(key)

	if dc.Contains(key) {
		t.Error("key should not be in cache after Delete")
	}

	// File should be removed from disk
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("cached file should be deleted from disk")
	}
}

func TestDiskCache_Clear(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Add some entries
	for i := 0; i < 5; i++ {
		key := CacheKey{ObjectKey: "test", ETag: string(rune('a' + i))}
		dc.PutBytes(key, []byte("data"))
	}

	stats := dc.Stats()
	if stats.EntryCount != 5 {
		t.Errorf("EntryCount = %d, want 5", stats.EntryCount)
	}

	dc.Clear()

	stats = dc.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("EntryCount after Clear = %d, want 0", stats.EntryCount)
	}
	if stats.UsedBytes != 0 {
		t.Errorf("UsedBytes after Clear = %d, want 0", stats.UsedBytes)
	}
}

func TestDiskCache_LoadExisting(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first cache and add data
	dc1, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	key := CacheKey{ObjectKey: "persistent", ETag: "v1"}
	data := []byte("persistent data")
	_, err = dc1.PutBytes(key, data)
	if err != nil {
		t.Fatalf("PutBytes failed: %v", err)
	}

	// Create second cache pointing to same directory
	dc2, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache (reload) failed: %v", err)
	}

	stats := dc2.Stats()
	if stats.EntryCount != 1 {
		t.Errorf("EntryCount after reload = %d, want 1", stats.EntryCount)
	}
	if stats.UsedBytes != int64(len(data)) {
		t.Errorf("UsedBytes after reload = %d, want %d", stats.UsedBytes, len(data))
	}
}

func TestDiskCache_DefaultBudgetPct(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		// BudgetPct defaults to 95
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	stats := dc.Stats()
	if stats.BudgetPct != 95 {
		t.Errorf("default BudgetPct = %d, want 95", stats.BudgetPct)
	}
}

func TestDiskCache_GetReader(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	key := CacheKey{ObjectKey: "test", ETag: "e1"}
	data := []byte("reader test data")
	dc.PutBytes(key, data)

	reader, err := dc.GetReader(key)
	if err != nil {
		t.Fatalf("GetReader failed: %v", err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("content = %s, want %s", content, data)
	}
}

func TestDiskCache_Put_WithReader(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	key := CacheKey{ObjectKey: "test", ETag: "e1"}
	data := []byte("reader input data")

	path, err := dc.Put(key, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("content = %s, want %s", content, data)
	}
}

func TestDiskCache_Contains(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	key := CacheKey{ObjectKey: "test", ETag: "e1"}

	if dc.Contains(key) {
		t.Error("Contains should return false for nonexistent key")
	}

	dc.PutBytes(key, []byte("data"))

	if !dc.Contains(key) {
		t.Error("Contains should return true after Put")
	}
}

func TestDiskCache_Concurrent(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 10 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Concurrent puts and gets
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(i int) {
			key := CacheKey{ObjectKey: "obj", ETag: string(rune('a' + i))}
			data := make([]byte, 1000)

			dc.PutBytes(key, data)
			dc.Get(key)
			dc.Contains(key)

			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	stats := dc.Stats()
	if stats.EntryCount != 10 {
		t.Errorf("EntryCount = %d, want 10", stats.EntryCount)
	}
}

func TestDiskCache_DuplicatePut(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	key := CacheKey{ObjectKey: "test", ETag: "e1"}
	data := []byte("original data")

	path1, _ := dc.PutBytes(key, data)
	path2, _ := dc.PutBytes(key, []byte("should be ignored"))

	// Should return same path (no re-write)
	if path1 != path2 {
		t.Errorf("duplicate Put returned different path: %s vs %s", path1, path2)
	}

	// Content should be original
	content, _ := os.ReadFile(path1)
	if !bytes.Equal(content, data) {
		t.Errorf("content = %s, want %s", content, data)
	}

	// Only one entry
	stats := dc.Stats()
	if stats.EntryCount != 1 {
		t.Errorf("EntryCount = %d, want 1", stats.EntryCount)
	}
}

func TestCacheKeyToPath(t *testing.T) {
	key1 := CacheKey{ObjectKey: "ns/obj", ETag: "v1"}
	key2 := CacheKey{ObjectKey: "ns/obj", ETag: "v2"}
	key3 := CacheKey{ObjectKey: "ns/obj", ETag: "v1"}

	path1 := cacheKeyToPath(key1)
	path2 := cacheKeyToPath(key2)
	path3 := cacheKeyToPath(key3)

	if path1 == path2 {
		t.Error("different etags should produce different paths")
	}
	if path1 != path3 {
		t.Error("same key should produce same path")
	}
}

func TestDiskCache_PinnedEvictionOrder(t *testing.T) {
	tmpDir := t.TempDir()

	// Very small cache - can only hold 2 entries
	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 250,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	data := make([]byte, 100)

	// Insert and pin first namespace
	key1 := CacheKey{ObjectKey: "warm-ns/obj1", ETag: "e1"}
	key2 := CacheKey{ObjectKey: "cold-ns/obj2", ETag: "e2"}

	dc.PutBytes(key1, data)
	dc.Pin("warm-ns/")

	dc.PutBytes(key2, data)

	// Now insert third - only cold-ns can be evicted
	key3 := CacheKey{ObjectKey: "another/obj3", ETag: "e3"}
	dc.PutBytes(key3, data)

	// warm-ns should still exist
	if !dc.Contains(key1) {
		t.Error("pinned entry should not be evicted")
	}
	// cold-ns should be evicted
	if dc.Contains(key2) {
		t.Error("unpinned LRU entry should be evicted")
	}
}

func TestDiskCache_AccessTimeTracking(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	key := CacheKey{ObjectKey: "test", ETag: "e1"}
	dc.PutBytes(key, []byte("data"))

	// Get the file's mtime before access
	hash := cacheKeyToPath(key)
	path := filepath.Join(tmpDir, hash)
	info1, _ := os.Stat(path)
	mtime1 := info1.ModTime()

	// Wait a bit and access
	time.Sleep(10 * time.Millisecond)
	dc.Get(key)

	// Check mtime was updated
	info2, _ := os.Stat(path)
	mtime2 := info2.ModTime()

	if !mtime2.After(mtime1) {
		t.Error("access time should be updated after Get")
	}
}

// TestDiskCache_EvictLoadedEntries verifies that entries loaded via loadExisting
// can be properly evicted (Issue 1 fix: hash stored in entry struct).
func TestDiskCache_EvictLoadedEntries(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first cache and add data
	dc1, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 250, // Small cache - can hold 2 entries of 100 bytes
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Add two entries
	key1 := CacheKey{ObjectKey: "obj1", ETag: "v1"}
	key2 := CacheKey{ObjectKey: "obj2", ETag: "v2"}
	data := make([]byte, 100)

	dc1.PutBytes(key1, data)
	dc1.PutBytes(key2, data)

	stats1 := dc1.Stats()
	if stats1.EntryCount != 2 {
		t.Fatalf("EntryCount = %d, want 2", stats1.EntryCount)
	}

	// Create second cache (simulates restart - entries loaded via loadExisting)
	dc2, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 250, // Same small size
	})
	if err != nil {
		t.Fatalf("NewDiskCache (reload) failed: %v", err)
	}

	stats2 := dc2.Stats()
	if stats2.EntryCount != 2 {
		t.Fatalf("EntryCount after reload = %d, want 2", stats2.EntryCount)
	}

	// Now add a third entry - this should trigger eviction of loaded entries
	key3 := CacheKey{ObjectKey: "obj3", ETag: "v3"}
	_, err = dc2.PutBytes(key3, data)
	if err != nil {
		t.Fatalf("PutBytes key3 failed: %v", err)
	}

	stats3 := dc2.Stats()
	// Should have 2 entries (one evicted to make room for new one)
	if stats3.EntryCount != 2 {
		t.Errorf("EntryCount after eviction = %d, want 2", stats3.EntryCount)
	}

	// Verify usedBytes is correct (not negative or inflated due to bad eviction)
	expectedBytes := int64(200) // 2 entries of 100 bytes each
	if stats3.UsedBytes != expectedBytes {
		t.Errorf("UsedBytes = %d, want %d", stats3.UsedBytes, expectedBytes)
	}

	// Verify the new entry is actually cached
	if !dc2.Contains(key3) {
		t.Error("key3 should be in cache after Put")
	}
}

// TestDiskCache_AllPinnedTerminates verifies that eviction terminates when all
// entries are pinned (Issue 2 fix: proper termination condition).
func TestDiskCache_AllPinnedTerminates(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 250, // Can hold 2 entries of 100 bytes
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	data := make([]byte, 100)

	// Add two entries from different namespaces
	key1 := CacheKey{ObjectKey: "ns1/obj1", ETag: "e1"}
	key2 := CacheKey{ObjectKey: "ns2/obj2", ETag: "e2"}

	dc.PutBytes(key1, data)
	dc.PutBytes(key2, data)

	// Pin both namespaces
	dc.Pin("ns1/")
	dc.Pin("ns2/")

	// Try to add a third entry - should fail with ErrCacheFull
	// (and NOT spin forever)
	key3 := CacheKey{ObjectKey: "ns3/obj3", ETag: "e3"}

	// Use a channel with timeout to detect infinite loop
	done := make(chan error, 1)
	go func() {
		_, err := dc.PutBytes(key3, data)
		done <- err
	}()

	select {
	case err := <-done:
		if err != ErrCacheFull {
			t.Errorf("expected ErrCacheFull, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("eviction loop did not terminate - infinite loop detected")
	}

	// Both original entries should still be present
	if !dc.Contains(key1) {
		t.Error("key1 should still be cached (pinned)")
	}
	if !dc.Contains(key2) {
		t.Error("key2 should still be cached (pinned)")
	}
}

// TestDiskCache_MultiplePinnedEviction tests eviction when multiple entries are
// pinned but some are evictable.
func TestDiskCache_MultiplePinnedEviction(t *testing.T) {
	tmpDir := t.TempDir()

	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: tmpDir,
		MaxBytes: 350, // Can hold 3 entries of 100 bytes
	})
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	data := make([]byte, 100)

	// Add three entries
	key1 := CacheKey{ObjectKey: "pinned1/obj", ETag: "e1"}
	key2 := CacheKey{ObjectKey: "unpinned/obj", ETag: "e2"}
	key3 := CacheKey{ObjectKey: "pinned2/obj", ETag: "e3"}

	dc.PutBytes(key1, data)
	dc.PutBytes(key2, data)
	dc.PutBytes(key3, data)

	// Pin two namespaces, leave one unpinned
	dc.Pin("pinned1/")
	dc.Pin("pinned2/")

	// Add fourth entry - should evict the unpinned one
	key4 := CacheKey{ObjectKey: "new/obj", ETag: "e4"}
	_, err = dc.PutBytes(key4, data)
	if err != nil {
		t.Fatalf("PutBytes key4 failed: %v", err)
	}

	// Pinned entries should remain
	if !dc.Contains(key1) {
		t.Error("key1 (pinned1/) should still be cached")
	}
	if !dc.Contains(key3) {
		t.Error("key3 (pinned2/) should still be cached")
	}
	// Unpinned entry should be evicted
	if dc.Contains(key2) {
		t.Error("key2 (unpinned/) should have been evicted")
	}
	// New entry should be present
	if !dc.Contains(key4) {
		t.Error("key4 should be cached")
	}
}
