package cache

import (
	"bytes"
	"testing"
)

func TestNewMemoryCache(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{
		MaxBytes:      1024 * 1024, // 1MB
		DefaultCapPct: 50,
	})

	if mc == nil {
		t.Fatal("NewMemoryCache returned nil")
	}

	stats := mc.Stats()
	if stats.MaxBytes != 1024*1024 {
		t.Errorf("expected MaxBytes=1048576, got %d", stats.MaxBytes)
	}
	if stats.UsedBytes != 0 {
		t.Errorf("expected UsedBytes=0, got %d", stats.UsedBytes)
	}
	if stats.DefaultCapPct != 50 {
		t.Errorf("expected DefaultCapPct=50, got %d", stats.DefaultCapPct)
	}
}

func TestMemoryCache_PutGet(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	key := MemoryCacheKey{
		Namespace: "test-ns",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}
	data := []byte("test data")

	// Put
	err := mc.Put(key, data)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get
	result, err := mc.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Errorf("expected %q, got %q", data, result)
	}

	// Verify stats
	stats := mc.Stats()
	if stats.EntryCount != 1 {
		t.Errorf("expected EntryCount=1, got %d", stats.EntryCount)
	}
	if stats.ShardCount != 1 {
		t.Errorf("expected ShardCount=1, got %d", stats.ShardCount)
	}
}

func TestMemoryCache_Miss(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	key := MemoryCacheKey{
		Namespace: "test-ns",
		ShardID:   "shard-1",
		ItemID:    "nonexistent",
		ItemType:  TypeCentroid,
	}

	_, err := mc.Get(key)
	if err != ErrRAMCacheMiss {
		t.Errorf("expected ErrRAMCacheMiss, got %v", err)
	}

	// Verify miss is counted
	stats := mc.Stats()
	if stats.Misses != 1 {
		t.Errorf("expected Misses=1, got %d", stats.Misses)
	}
}

func TestMemoryCache_CentroidsInRAM(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	// Test that ANN centroids can be cached in RAM
	centroids := make([]byte, 1024) // 1KB of centroid data
	for i := range centroids {
		centroids[i] = byte(i % 256)
	}

	key := MemoryCacheKey{
		Namespace: "vectors-ns",
		ShardID:   "segment-0",
		ItemID:    "centroids",
		ItemType:  TypeCentroid,
	}

	err := mc.Put(key, centroids)
	if err != nil {
		t.Fatalf("Put centroids failed: %v", err)
	}

	// Verify centroids are cached
	result, err := mc.Get(key)
	if err != nil {
		t.Fatalf("Get centroids failed: %v", err)
	}

	if !bytes.Equal(result, centroids) {
		t.Error("centroids data mismatch")
	}

	// Verify hit is counted
	stats := mc.Stats()
	if stats.Hits != 1 {
		t.Errorf("expected Hits=1, got %d", stats.Hits)
	}
}

func TestMemoryCache_PostingDictAndBitmapShards(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	// Test posting dictionaries
	postingDict := []byte("term1:1,5,10|term2:2,7,15")
	dictKey := MemoryCacheKey{
		Namespace: "fts-ns",
		ShardID:   "segment-0",
		ItemID:    "posting-dict",
		ItemType:  TypePostingDict,
	}

	err := mc.Put(dictKey, postingDict)
	if err != nil {
		t.Fatalf("Put posting dict failed: %v", err)
	}

	// Test bitmap shards
	bitmap := []byte{0xFF, 0x00, 0xAA, 0x55} // sample bitmap data
	bitmapKey := MemoryCacheKey{
		Namespace: "fts-ns",
		ShardID:   "segment-0",
		ItemID:    "bitmap-status",
		ItemType:  TypeFilterBitmap,
	}

	err = mc.Put(bitmapKey, bitmap)
	if err != nil {
		t.Fatalf("Put bitmap failed: %v", err)
	}

	// Verify both are cached
	if !mc.Contains(dictKey) {
		t.Error("posting dict not in cache")
	}
	if !mc.Contains(bitmapKey) {
		t.Error("bitmap not in cache")
	}

	stats := mc.Stats()
	if stats.EntryCount != 2 {
		t.Errorf("expected EntryCount=2, got %d", stats.EntryCount)
	}
}

func TestMemoryCache_ShardAwareLRUEviction(t *testing.T) {
	// Small cache to trigger eviction
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 300})

	// Add shard 1 with one entry (100 bytes)
	key1 := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeDocColumn,
	}
	err := mc.Put(key1, make([]byte, 100))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Add shard 2 with one entry (100 bytes)
	key2 := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-2",
		ItemID:    "item-1",
		ItemType:  TypeDocColumn,
	}
	err = mc.Put(key2, make([]byte, 100))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Access shard 1 to make it more recently used
	_, _ = mc.Get(key1)

	// Add shard 3 with one entry (150 bytes) - should evict shard 2
	key3 := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-3",
		ItemID:    "item-1",
		ItemType:  TypeDocColumn,
	}
	err = mc.Put(key3, make([]byte, 150))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Shard 2 should be evicted (LRU)
	if mc.Contains(key2) {
		t.Error("shard-2 should have been evicted")
	}

	// Shard 1 should still be in cache (more recently accessed)
	if !mc.Contains(key1) {
		t.Error("shard-1 should still be in cache")
	}

	// Shard 3 should be in cache
	if !mc.Contains(key3) {
		t.Error("shard-3 should be in cache")
	}
}

func TestMemoryCache_ShardAwarePriorityEviction(t *testing.T) {
	// Small cache to trigger eviction
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 250})

	// Add low priority entry (DocColumn)
	keyLow := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-low",
		ItemID:    "item-1",
		ItemType:  TypeDocColumn, // lowest priority
	}
	err := mc.Put(keyLow, make([]byte, 100))
	if err != nil {
		t.Fatalf("Put low priority failed: %v", err)
	}

	// Add high priority entry (TailData) - accessed first to make it LRU
	keyHigh := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-high",
		ItemID:    "item-1",
		ItemType:  TypeTailData, // highest priority
	}
	err = mc.Put(keyHigh, make([]byte, 100))
	if err != nil {
		t.Fatalf("Put high priority failed: %v", err)
	}

	// Now add another entry that forces eviction
	keyNew := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-new",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}
	err = mc.Put(keyNew, make([]byte, 100))
	if err != nil {
		t.Fatalf("Put new failed: %v", err)
	}

	// Low priority shard should be evicted (even if it was added later)
	if mc.Contains(keyLow) {
		t.Error("low priority shard should have been evicted")
	}

	// High priority shard should still be in cache
	if !mc.Contains(keyHigh) {
		t.Error("high priority shard should still be in cache")
	}
}

func TestMemoryCache_PerNamespaceBudgetCaps(t *testing.T) {
	// 1000 bytes total, 40% per namespace = 400 bytes per namespace
	mc := NewMemoryCache(MemoryCacheConfig{
		MaxBytes:      1000,
		DefaultCapPct: 40,
	})

	// Fill namespace A with 350 bytes
	keyA1 := MemoryCacheKey{
		Namespace: "ns-a",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}
	err := mc.Put(keyA1, make([]byte, 350))
	if err != nil {
		t.Fatalf("Put ns-a failed: %v", err)
	}

	// Try to add 100 more bytes to namespace A (would exceed 400 cap)
	keyA2 := MemoryCacheKey{
		Namespace: "ns-a",
		ShardID:   "shard-2",
		ItemID:    "item-1",
		ItemType:  TypeDocColumn, // lower priority, should be evicted
	}
	err = mc.Put(keyA2, make([]byte, 100))
	if err != nil {
		t.Fatalf("Put ns-a shard-2 failed: %v", err)
	}

	// Should have evicted shard-1 to make room within the namespace cap
	if mc.Contains(keyA1) {
		t.Error("shard-1 should have been evicted to stay under namespace cap")
	}

	// Namespace B should be independent
	keyB := MemoryCacheKey{
		Namespace: "ns-b",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}
	err = mc.Put(keyB, make([]byte, 350))
	if err != nil {
		t.Fatalf("Put ns-b failed: %v", err)
	}

	// Verify usage
	usageA := mc.GetNamespaceUsage("ns-a")
	usageB := mc.GetNamespaceUsage("ns-b")

	if usageA > 400 {
		t.Errorf("namespace A usage %d exceeds cap 400", usageA)
	}
	if usageB > 400 {
		t.Errorf("namespace B usage %d exceeds cap 400", usageB)
	}
}

func TestMemoryCache_CustomNamespaceCap(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{
		MaxBytes:      10000,
		DefaultCapPct: 50, // 5000 bytes default
	})

	// Set custom cap for namespace A (only 200 bytes)
	mc.SetNamespaceCap("ns-a", 200)

	// Try to add 300 bytes - should fail or evict
	key := MemoryCacheKey{
		Namespace: "ns-a",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}
	err := mc.Put(key, make([]byte, 300))
	if err != ErrNamespaceBudgetCap {
		t.Errorf("expected ErrNamespaceBudgetCap, got %v", err)
	}

	// Add something that fits
	err = mc.Put(key, make([]byte, 150))
	if err != nil {
		t.Fatalf("Put 150 bytes failed: %v", err)
	}

	// Verify it's in cache
	if !mc.Contains(key) {
		t.Error("entry should be in cache")
	}

	// Namespace B should use default cap
	keyB := MemoryCacheKey{
		Namespace: "ns-b",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}
	err = mc.Put(keyB, make([]byte, 4000))
	if err != nil {
		t.Fatalf("Put ns-b 4000 bytes failed: %v", err)
	}
}

func TestMemoryCache_MultiTenancy(t *testing.T) {
	// Test that multiple namespaces coexist with budget caps
	mc := NewMemoryCache(MemoryCacheConfig{
		MaxBytes:      1000,
		DefaultCapPct: 30, // 300 bytes per namespace
	})

	// Add entries from 3 different namespaces
	for i := 0; i < 3; i++ {
		ns := string(rune('A' + i))
		key := MemoryCacheKey{
			Namespace: "tenant-" + ns,
			ShardID:   "shard-1",
			ItemID:    "item-1",
			ItemType:  TypeCentroid,
		}
		err := mc.Put(key, make([]byte, 250))
		if err != nil {
			t.Fatalf("Put tenant-%s failed: %v", ns, err)
		}
	}

	// All 3 should be in cache
	stats := mc.Stats()
	if stats.NamespaceCount != 3 {
		t.Errorf("expected 3 namespaces, got %d", stats.NamespaceCount)
	}

	// Each namespace should be under its cap
	for i := 0; i < 3; i++ {
		ns := "tenant-" + string(rune('A'+i))
		usage := mc.GetNamespaceUsage(ns)
		if usage > 300 {
			t.Errorf("namespace %s usage %d exceeds cap 300", ns, usage)
		}
	}
}

func TestMemoryCache_Delete(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	key := MemoryCacheKey{
		Namespace: "test-ns",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}

	err := mc.Put(key, make([]byte, 100))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if !mc.Contains(key) {
		t.Error("entry should be in cache")
	}

	mc.Delete(key)

	if mc.Contains(key) {
		t.Error("entry should have been deleted")
	}

	// Verify stats updated
	stats := mc.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("expected EntryCount=0, got %d", stats.EntryCount)
	}
	if stats.UsedBytes != 0 {
		t.Errorf("expected UsedBytes=0, got %d", stats.UsedBytes)
	}
}

func TestMemoryCache_DeleteShard(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	// Add multiple entries to same shard
	for i := 0; i < 5; i++ {
		key := MemoryCacheKey{
			Namespace: "test-ns",
			ShardID:   "shard-1",
			ItemID:    "item-" + string(rune('0'+i)),
			ItemType:  TypeCentroid,
		}
		err := mc.Put(key, make([]byte, 100))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	stats := mc.Stats()
	if stats.EntryCount != 5 {
		t.Errorf("expected EntryCount=5, got %d", stats.EntryCount)
	}

	// Delete entire shard
	mc.DeleteShard("test-ns", "shard-1")

	stats = mc.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("expected EntryCount=0 after DeleteShard, got %d", stats.EntryCount)
	}
	if stats.ShardCount != 0 {
		t.Errorf("expected ShardCount=0, got %d", stats.ShardCount)
	}
}

func TestMemoryCache_DeleteNamespace(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	// Add entries to multiple shards in same namespace
	for s := 0; s < 3; s++ {
		for i := 0; i < 3; i++ {
			key := MemoryCacheKey{
				Namespace: "target-ns",
				ShardID:   "shard-" + string(rune('0'+s)),
				ItemID:    "item-" + string(rune('0'+i)),
				ItemType:  TypeCentroid,
			}
			err := mc.Put(key, make([]byte, 50))
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
	}

	// Add entries to another namespace
	key := MemoryCacheKey{
		Namespace: "other-ns",
		ShardID:   "shard-0",
		ItemID:    "item-0",
		ItemType:  TypeCentroid,
	}
	err := mc.Put(key, make([]byte, 50))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete target namespace
	mc.DeleteNamespace("target-ns")

	// Verify target namespace is gone
	if mc.GetNamespaceUsage("target-ns") != 0 {
		t.Error("target-ns should have no usage")
	}

	// Other namespace should still exist
	if !mc.Contains(key) {
		t.Error("other-ns entry should still exist")
	}
}

func TestMemoryCache_Clear(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	// Add some entries
	for i := 0; i < 10; i++ {
		key := MemoryCacheKey{
			Namespace: "ns-" + string(rune('0'+i%3)),
			ShardID:   "shard-" + string(rune('0'+i%2)),
			ItemID:    "item-" + string(rune('0'+i)),
			ItemType:  TypeCentroid,
		}
		err := mc.Put(key, make([]byte, 100))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	mc.Clear()

	stats := mc.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("expected EntryCount=0, got %d", stats.EntryCount)
	}
	if stats.ShardCount != 0 {
		t.Errorf("expected ShardCount=0, got %d", stats.ShardCount)
	}
	if stats.UsedBytes != 0 {
		t.Errorf("expected UsedBytes=0, got %d", stats.UsedBytes)
	}
}

func TestMemoryCache_HitRatio(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	key := MemoryCacheKey{
		Namespace: "test-ns",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}
	err := mc.Put(key, make([]byte, 100))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2 hits
	_, _ = mc.Get(key)
	_, _ = mc.Get(key)

	// 1 miss
	missingKey := MemoryCacheKey{
		Namespace: "test-ns",
		ShardID:   "shard-1",
		ItemID:    "nonexistent",
		ItemType:  TypeCentroid,
	}
	_, _ = mc.Get(missingKey)

	stats := mc.Stats()
	if stats.Hits != 2 {
		t.Errorf("expected Hits=2, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("expected Misses=1, got %d", stats.Misses)
	}

	expectedRatio := float64(2) / float64(3)
	if stats.HitRatio < expectedRatio-0.01 || stats.HitRatio > expectedRatio+0.01 {
		t.Errorf("expected HitRatio≈%.2f, got %.2f", expectedRatio, stats.HitRatio)
	}
}

func TestMemoryCache_GetShardInfo(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	// Add multiple entries to same shard
	for i := 0; i < 5; i++ {
		key := MemoryCacheKey{
			Namespace: "test-ns",
			ShardID:   "shard-1",
			ItemID:    "item-" + string(rune('0'+i)),
			ItemType:  TypeCentroid,
		}
		err := mc.Put(key, make([]byte, 100))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	size, count, exists := mc.GetShardInfo("test-ns", "shard-1")
	if !exists {
		t.Error("shard should exist")
	}
	if count != 5 {
		t.Errorf("expected count=5, got %d", count)
	}
	if size != 500 {
		t.Errorf("expected size=500, got %d", size)
	}

	// Non-existent shard
	_, _, exists = mc.GetShardInfo("test-ns", "nonexistent")
	if exists {
		t.Error("nonexistent shard should not exist")
	}
}

func TestCacheItemType_Priority(t *testing.T) {
	// Verify priority order: DocColumn < FilterBitmap < Centroid < PostingDict < TailData
	if TypeDocColumn.Priority() >= TypeFilterBitmap.Priority() {
		t.Error("DocColumn should have lower priority than FilterBitmap")
	}
	if TypeFilterBitmap.Priority() >= TypeCentroid.Priority() {
		t.Error("FilterBitmap should have lower priority than Centroid")
	}
	if TypeCentroid.Priority() >= TypePostingDict.Priority() {
		t.Error("Centroid should have lower priority than PostingDict")
	}
	if TypePostingDict.Priority() >= TypeTailData.Priority() {
		t.Error("PostingDict should have lower priority than TailData")
	}
}

func TestCacheItemType_String(t *testing.T) {
	tests := []struct {
		typ      CacheItemType
		expected string
	}{
		{TypeDocColumn, "doc_column"},
		{TypeFilterBitmap, "filter_bitmap"},
		{TypeCentroid, "centroid"},
		{TypePostingDict, "posting_dict"},
		{TypeTailData, "tail_data"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.expected {
			t.Errorf("%d.String() = %q, want %q", tt.typ, got, tt.expected)
		}
	}
}

func TestMemoryCache_UpdateExisting(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	key := MemoryCacheKey{
		Namespace: "test-ns",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}

	// Initial put
	err := mc.Put(key, []byte("original"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Update
	err = mc.Put(key, []byte("updated data"))
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify updated
	result, err := mc.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(result, []byte("updated data")) {
		t.Errorf("expected 'updated data', got %q", result)
	}

	// Verify only one entry exists
	stats := mc.Stats()
	if stats.EntryCount != 1 {
		t.Errorf("expected EntryCount=1, got %d", stats.EntryCount)
	}
}

func TestMemoryCache_DataIsolation(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	key := MemoryCacheKey{
		Namespace: "test-ns",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}

	original := []byte("original data")
	err := mc.Put(key, original)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Modify original after put
	original[0] = 'X'

	// Get should return unmodified data
	result, err := mc.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result[0] != 'o' {
		t.Error("cache data should be isolated from original")
	}

	// Modify returned data
	result[0] = 'Y'

	// Get again should return unmodified data
	result2, err := mc.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if result2[0] != 'o' {
		t.Error("cache data should be isolated from returned slice")
	}
}

func TestMemoryCache_UpdateExistingTriggersGlobalEviction(t *testing.T) {
	// Issue 1: Updating an existing entry that increases in size should trigger global eviction
	// Cache size: 200 bytes
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 200})

	// Add shard1 with 80 bytes
	key1 := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}
	err := mc.Put(key1, make([]byte, 80))
	if err != nil {
		t.Fatalf("Put shard-1 failed: %v", err)
	}

	// Add shard2 with 80 bytes (total: 160 bytes)
	key2 := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-2",
		ItemID:    "item-1",
		ItemType:  TypeDocColumn, // lower priority, should be evicted first
	}
	err = mc.Put(key2, make([]byte, 80))
	if err != nil {
		t.Fatalf("Put shard-2 failed: %v", err)
	}

	// Now update shard-1 to 150 bytes (160 - 80 + 150 = 230 > 200)
	// This should trigger eviction of shard-2
	err = mc.Put(key1, make([]byte, 150))
	if err != nil {
		t.Fatalf("Update shard-1 failed: %v", err)
	}

	// Verify usedBytes does not exceed maxBytes
	stats := mc.Stats()
	if stats.UsedBytes > stats.MaxBytes {
		t.Errorf("usedBytes (%d) exceeds maxBytes (%d)", stats.UsedBytes, stats.MaxBytes)
	}

	// shard-2 should have been evicted to make room
	if mc.Contains(key2) {
		t.Error("shard-2 should have been evicted")
	}

	// shard-1 should still be in cache with the new value
	if !mc.Contains(key1) {
		t.Error("shard-1 should still be in cache")
	}
}

func TestMemoryCache_UpdateExistingDoesNotEvictOwnShard(t *testing.T) {
	// Issue 2: When evicting to make room for an update, we must not evict the shard containing the entry
	// Cache size: 300 bytes, namespace cap: 200 bytes
	mc := NewMemoryCache(MemoryCacheConfig{
		MaxBytes:      300,
		DefaultCapPct: 70, // 210 bytes per namespace
	})

	// Add two entries to the same shard (total 150 bytes in shard-1)
	key1a := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-1",
		ItemID:    "item-a",
		ItemType:  TypeCentroid,
	}
	err := mc.Put(key1a, make([]byte, 75))
	if err != nil {
		t.Fatalf("Put item-a failed: %v", err)
	}

	key1b := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-1",
		ItemID:    "item-b",
		ItemType:  TypeCentroid,
	}
	err = mc.Put(key1b, make([]byte, 75))
	if err != nil {
		t.Fatalf("Put item-b failed: %v", err)
	}

	// Add shard-2 with 50 bytes (total: 200 bytes)
	key2 := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-2",
		ItemID:    "item-1",
		ItemType:  TypeDocColumn, // lower priority
	}
	err = mc.Put(key2, make([]byte, 50))
	if err != nil {
		t.Fatalf("Put shard-2 failed: %v", err)
	}

	// Now update item-a to 100 bytes (namespace would be at 225, over cap of 210)
	// The namespace eviction should evict shard-2, NOT shard-1 (which contains item-a)
	err = mc.Put(key1a, make([]byte, 100))
	if err != nil {
		t.Fatalf("Update item-a failed: %v", err)
	}

	// item-a should still be in cache with the updated value
	if !mc.Contains(key1a) {
		t.Fatal("item-a should still be in cache")
	}

	// item-b should still be in cache (same shard as item-a)
	if !mc.Contains(key1b) {
		t.Fatal("item-b should still be in cache (same shard)")
	}

	// Verify the updated size
	data, err := mc.Get(key1a)
	if err != nil {
		t.Fatalf("Get item-a failed: %v", err)
	}
	if len(data) != 100 {
		t.Errorf("expected item-a size 100, got %d", len(data))
	}
}

func TestMemoryCache_UpdateExistingWhenCacheFull(t *testing.T) {
	// Test updating when cache is completely full and only the updating shard remains
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 100})

	// Add entry with 80 bytes
	key := MemoryCacheKey{
		Namespace: "ns",
		ShardID:   "shard-1",
		ItemID:    "item-1",
		ItemType:  TypeCentroid,
	}
	err := mc.Put(key, make([]byte, 80))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to update to 150 bytes (exceeds maxBytes, no other shards to evict)
	err = mc.Put(key, make([]byte, 150))
	if err != ErrRAMCacheFull {
		t.Errorf("expected ErrRAMCacheFull, got %v", err)
	}

	// Original entry should still be in cache with original value
	if !mc.Contains(key) {
		t.Fatal("original entry should still be in cache")
	}

	data, err := mc.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(data) != 80 {
		t.Errorf("expected original size 80, got %d", len(data))
	}
}
