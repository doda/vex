package cache

import (
	"testing"

	"github.com/vexsearch/vex/internal/logging"
)

func TestTemperatureTracker_EmptyIsCold(t *testing.T) {
	tracker := NewTemperatureTracker()

	temp := tracker.Temperature()
	if temp != logging.CacheCold {
		t.Errorf("expected cold temperature with no samples, got %s", temp)
	}

	ratio := tracker.HitRatio()
	if ratio != 0 {
		t.Errorf("expected 0 hit ratio with no samples, got %f", ratio)
	}
}

func TestTemperatureTracker_BelowMinSamplesIsCold(t *testing.T) {
	tracker := NewTemperatureTracker()

	// Add fewer samples than MinSamplesForClassification
	for i := 0; i < MinSamplesForClassification-1; i++ {
		tracker.RecordHit("")
	}

	temp := tracker.Temperature()
	if temp != logging.CacheCold {
		t.Errorf("expected cold with %d samples (below min), got %s", MinSamplesForClassification-1, temp)
	}
}

func TestTemperatureTracker_ColdClassification(t *testing.T) {
	tracker := NewTemperatureTracker()

	// 30% hit ratio should be cold (<50%)
	for i := 0; i < 30; i++ {
		tracker.RecordHit("")
	}
	for i := 0; i < 70; i++ {
		tracker.RecordMiss("")
	}

	temp := tracker.Temperature()
	if temp != logging.CacheCold {
		t.Errorf("expected cold with 30%% hit ratio, got %s", temp)
	}

	ratio := tracker.HitRatio()
	if ratio < 0.29 || ratio > 0.31 {
		t.Errorf("expected ~0.30 hit ratio, got %f", ratio)
	}
}

func TestTemperatureTracker_WarmClassification(t *testing.T) {
	tracker := NewTemperatureTracker()

	// 60% hit ratio should be warm (>=50%, <80%)
	for i := 0; i < 60; i++ {
		tracker.RecordHit("")
	}
	for i := 0; i < 40; i++ {
		tracker.RecordMiss("")
	}

	temp := tracker.Temperature()
	if temp != logging.CacheWarm {
		t.Errorf("expected warm with 60%% hit ratio, got %s", temp)
	}

	ratio := tracker.HitRatio()
	if ratio < 0.59 || ratio > 0.61 {
		t.Errorf("expected ~0.60 hit ratio, got %f", ratio)
	}
}

func TestTemperatureTracker_HotClassification(t *testing.T) {
	tracker := NewTemperatureTracker()

	// 85% hit ratio should be hot (>=80%)
	for i := 0; i < 85; i++ {
		tracker.RecordHit("")
	}
	for i := 0; i < 15; i++ {
		tracker.RecordMiss("")
	}

	temp := tracker.Temperature()
	if temp != logging.CacheHot {
		t.Errorf("expected hot with 85%% hit ratio, got %s", temp)
	}

	ratio := tracker.HitRatio()
	if ratio < 0.84 || ratio > 0.86 {
		t.Errorf("expected ~0.85 hit ratio, got %f", ratio)
	}
}

func TestTemperatureTracker_SlidingWindow(t *testing.T) {
	// Use a small window size for testing
	tracker := NewTemperatureTrackerWithSize(20)

	// Fill window with hits (100% hit ratio)
	for i := 0; i < 20; i++ {
		tracker.RecordHit("")
	}

	if tracker.Temperature() != logging.CacheHot {
		t.Error("expected hot with 100% hit ratio")
	}

	// Replace window with misses (should become cold)
	for i := 0; i < 20; i++ {
		tracker.RecordMiss("")
	}

	if tracker.Temperature() != logging.CacheCold {
		t.Error("expected cold after window filled with misses")
	}
}

func TestTemperatureTracker_NamespaceStats(t *testing.T) {
	tracker := NewTemperatureTracker()

	// Record hits for ns1, misses for ns2
	for i := 0; i < 100; i++ {
		tracker.RecordHit("ns1")
	}
	for i := 0; i < 100; i++ {
		tracker.RecordMiss("ns2")
	}

	ns1Temp := tracker.NamespaceTemperature("ns1")
	if ns1Temp != logging.CacheHot {
		t.Errorf("expected ns1 to be hot (100%% hits), got %s", ns1Temp)
	}

	ns2Temp := tracker.NamespaceTemperature("ns2")
	if ns2Temp != logging.CacheCold {
		t.Errorf("expected ns2 to be cold (0%% hits), got %s", ns2Temp)
	}

	// Unknown namespace should be cold
	ns3Temp := tracker.NamespaceTemperature("ns3")
	if ns3Temp != logging.CacheCold {
		t.Errorf("expected unknown namespace to be cold, got %s", ns3Temp)
	}
}

func TestTemperatureTracker_NamespaceHitRatio(t *testing.T) {
	tracker := NewTemperatureTracker()

	for i := 0; i < 75; i++ {
		tracker.RecordHit("test-ns")
	}
	for i := 0; i < 25; i++ {
		tracker.RecordMiss("test-ns")
	}

	ratio := tracker.NamespaceHitRatio("test-ns")
	if ratio < 0.74 || ratio > 0.76 {
		t.Errorf("expected ~0.75 hit ratio for test-ns, got %f", ratio)
	}

	// Unknown namespace should have 0 ratio
	unknownRatio := tracker.NamespaceHitRatio("unknown")
	if unknownRatio != 0 {
		t.Errorf("expected 0 hit ratio for unknown namespace, got %f", unknownRatio)
	}
}

func TestTemperatureTracker_Stats(t *testing.T) {
	tracker := NewTemperatureTracker()

	for i := 0; i < 80; i++ {
		tracker.RecordHit("ns1")
	}
	for i := 0; i < 20; i++ {
		tracker.RecordMiss("ns2")
	}

	stats := tracker.Stats()

	if stats.Temperature != logging.CacheHot {
		t.Errorf("expected hot temperature in stats, got %s", stats.Temperature)
	}

	if stats.TotalHits != 80 {
		t.Errorf("expected 80 total hits, got %d", stats.TotalHits)
	}

	if stats.TotalMisses != 20 {
		t.Errorf("expected 20 total misses, got %d", stats.TotalMisses)
	}

	if stats.NamespaceCount != 2 {
		t.Errorf("expected 2 namespaces, got %d", stats.NamespaceCount)
	}

	if stats.WindowHits != 80 {
		t.Errorf("expected 80 window hits, got %d", stats.WindowHits)
	}

	if stats.WindowMisses != 20 {
		t.Errorf("expected 20 window misses, got %d", stats.WindowMisses)
	}

	if stats.HitRatio < 0.79 || stats.HitRatio > 0.81 {
		t.Errorf("expected ~0.80 hit ratio, got %f", stats.HitRatio)
	}
}

func TestTemperatureTracker_Reset(t *testing.T) {
	tracker := NewTemperatureTracker()

	for i := 0; i < 100; i++ {
		tracker.RecordHit("test")
	}

	tracker.Reset()

	if tracker.Temperature() != logging.CacheCold {
		t.Error("expected cold after reset")
	}

	stats := tracker.Stats()
	if stats.TotalHits != 0 || stats.TotalMisses != 0 {
		t.Error("expected zeroed stats after reset")
	}

	if stats.NamespaceCount != 0 {
		t.Error("expected no namespaces after reset")
	}
}

func TestTemperatureTracker_ResetNamespace(t *testing.T) {
	tracker := NewTemperatureTracker()

	for i := 0; i < 100; i++ {
		tracker.RecordHit("ns1")
		tracker.RecordHit("ns2")
	}

	tracker.ResetNamespace("ns1")

	ns1Temp := tracker.NamespaceTemperature("ns1")
	if ns1Temp != logging.CacheCold {
		t.Errorf("expected ns1 to be cold after reset, got %s", ns1Temp)
	}

	ns2Temp := tracker.NamespaceTemperature("ns2")
	if ns2Temp != logging.CacheHot {
		t.Errorf("expected ns2 to still be hot, got %s", ns2Temp)
	}
}

func TestTemperatureThresholds(t *testing.T) {
	tests := []struct {
		hitRatio float64
		expected logging.CacheTemperature
	}{
		{0.0, logging.CacheCold},
		{0.49, logging.CacheCold},
		{0.50, logging.CacheWarm},
		{0.79, logging.CacheWarm},
		{0.80, logging.CacheHot},
		{1.0, logging.CacheHot},
	}

	for _, tt := range tests {
		result := classifyFromRatio(tt.hitRatio)
		if result != tt.expected {
			t.Errorf("classifyFromRatio(%f) = %s, expected %s", tt.hitRatio, result, tt.expected)
		}
	}
}

func TestMemoryCache_Temperature(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	// Initially cold (no accesses)
	if mc.Temperature() != "cold" {
		t.Errorf("expected cold initially, got %s", mc.Temperature())
	}

	key := MemoryCacheKey{Namespace: "test", ShardID: "s1", ItemID: "item1", ItemType: TypeCentroid}

	// Generate misses (below min samples)
	for i := 0; i < MinSamplesForClassification-1; i++ {
		mc.Get(key) // miss
	}

	if mc.Temperature() != "cold" {
		t.Errorf("expected cold with few samples, got %s", mc.Temperature())
	}

	// Put item and generate lots of hits to fill window
	mc.Put(key, []byte("data"))

	// Fill the entire window with hits to achieve > 80% hit ratio
	for i := 0; i < DefaultWindowSize; i++ {
		mc.Get(key) // hit
	}

	// Should be hot now (window filled with hits)
	temp := mc.Temperature()
	if temp != "hot" {
		t.Errorf("expected hot after filling window with hits, got %s", temp)
	}
}

func TestMemoryCache_NamespaceTemperature(t *testing.T) {
	mc := NewMemoryCache(MemoryCacheConfig{MaxBytes: 1024 * 1024})

	// Add item to hot namespace
	hotKey := MemoryCacheKey{Namespace: "hot-ns", ShardID: "s1", ItemID: "item1", ItemType: TypeCentroid}
	mc.Put(hotKey, []byte("data"))

	for i := 0; i < 100; i++ {
		mc.Get(hotKey) // all hits
	}

	// Add item to cold namespace (all misses)
	coldKey := MemoryCacheKey{Namespace: "cold-ns", ShardID: "s1", ItemID: "nonexistent", ItemType: TypeCentroid}
	for i := 0; i < 100; i++ {
		mc.Get(coldKey) // all misses
	}

	if mc.NamespaceTemperature("hot-ns") != "hot" {
		t.Errorf("expected hot-ns to be hot, got %s", mc.NamespaceTemperature("hot-ns"))
	}

	if mc.NamespaceTemperature("cold-ns") != "cold" {
		t.Errorf("expected cold-ns to be cold, got %s", mc.NamespaceTemperature("cold-ns"))
	}
}

func TestDiskCache_Temperature(t *testing.T) {
	dc, err := NewDiskCache(DiskCacheConfig{
		RootPath: t.TempDir(),
		MaxBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}

	// Initially cold
	if dc.Temperature() != "cold" {
		t.Errorf("expected cold initially, got %s", dc.Temperature())
	}

	// Generate some misses
	for i := 0; i < 50; i++ {
		dc.Get(CacheKey{ObjectKey: "test/missing", ETag: "etag"})
	}

	if dc.Temperature() != "cold" {
		t.Errorf("expected cold after all misses, got %s", dc.Temperature())
	}

	// Put and hit
	key := CacheKey{ObjectKey: "test/item", ETag: "etag1"}
	dc.PutBytes(key, []byte("test data"))

	for i := 0; i < 200; i++ {
		dc.Get(key) // hits
	}

	// Should be hot now
	temp := dc.Temperature()
	if temp != "hot" {
		t.Errorf("expected hot after many hits, got %s", temp)
	}
}

func TestExtractNamespaceFromKey(t *testing.T) {
	tests := []struct {
		key      string
		expected string
	}{
		{"vex/namespaces/alpha/wal/00000000000000000001.wal.zst", "alpha"},
		{"/vex/namespaces/beta/index/segments/001.json", "beta"},
		{"namespace/wal/00000000000000000001.wal.zst", "namespace"},
		{"/namespace/manifest/001.json", "namespace"},
		{"ns", "ns"},
		{"/ns", "ns"},
		{"", ""},
		{"ns/", "ns"},
	}

	for _, tt := range tests {
		result := extractNamespaceFromKey(tt.key)
		if result != tt.expected {
			t.Errorf("extractNamespaceFromKey(%q) = %q, expected %q", tt.key, result, tt.expected)
		}
	}
}
