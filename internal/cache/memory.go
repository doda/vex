package cache

import (
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/vexsearch/vex/internal/metrics"
)

var (
	ErrRAMCacheMiss       = errors.New("RAM cache miss")
	ErrRAMCacheFull       = errors.New("RAM cache full")
	ErrNamespaceBudgetCap = errors.New("namespace budget cap exceeded")
)

// CacheItemType defines the type of cached item for priority-based eviction.
type CacheItemType int

const (
	TypeDocColumn CacheItemType = iota // lowest priority
	TypeFilterBitmap
	TypeCentroid
	TypePostingDict
	TypeTailData // highest priority
)

// String returns the string representation of the item type.
func (t CacheItemType) String() string {
	switch t {
	case TypeDocColumn:
		return "doc_column"
	case TypeFilterBitmap:
		return "filter_bitmap"
	case TypeCentroid:
		return "centroid"
	case TypePostingDict:
		return "posting_dict"
	case TypeTailData:
		return "tail_data"
	default:
		return "unknown"
	}
}

// Priority returns the eviction priority (lower = evict first).
func (t CacheItemType) Priority() int {
	return int(t)
}

// MemoryCacheKey identifies a cached item.
type MemoryCacheKey struct {
	Namespace string        // The namespace this item belongs to
	ShardID   string        // Shard identifier (for shard-aware eviction)
	ItemID    string        // Unique item identifier within the shard
	ItemType  CacheItemType // Type of item for priority-based eviction
}

// memoryEntry represents a cached item in RAM.
type memoryEntry struct {
	key        MemoryCacheKey
	data       []byte
	size       int64
	accessTime time.Time
}

// shard groups all entries belonging to a single shard for shard-aware eviction.
type shard struct {
	id          string
	namespace   string
	entries     map[string]*memoryEntry // itemID -> entry
	totalSize   int64
	minPriority int // minimum priority among entries (for eviction ordering)
	lastAccess  time.Time
	element     *list.Element
}

// MemoryCache implements an in-memory cache with shard-aware LRU eviction
// and per-namespace budget caps for multi-tenancy.
type MemoryCache struct {
	mu sync.RWMutex

	maxBytes  int64
	usedBytes int64

	entries map[string]*memoryEntry // full key hash -> entry
	shards  map[string]*shard       // namespace/shardID -> shard
	lruList *list.List              // LRU list of shards (front = most recently used)

	namespaceUsage map[string]int64 // namespace -> bytes used
	namespaceCaps  map[string]int64 // namespace -> max bytes allowed
	defaultCap     int64            // default per-namespace cap (0 = no limit)
	defaultCapPct  int              // default cap as percentage of total (0 = no limit)

	hits   int64
	misses int64

	// Temperature tracking
	tempTracker *TemperatureTracker
}

// MemoryCacheConfig holds configuration for the memory cache.
type MemoryCacheConfig struct {
	MaxBytes      int64 // Maximum total cache size in bytes
	DefaultCapPct int   // Default per-namespace cap as percentage of total (0 = no limit)
}

// NewMemoryCache creates a new memory cache.
func NewMemoryCache(cfg MemoryCacheConfig) *MemoryCache {
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 1 << 30 // 1GB default
	}

	defaultCap := int64(0)
	if cfg.DefaultCapPct > 0 && cfg.DefaultCapPct <= 100 {
		defaultCap = cfg.MaxBytes * int64(cfg.DefaultCapPct) / 100
	}

	return &MemoryCache{
		maxBytes:       cfg.MaxBytes,
		entries:        make(map[string]*memoryEntry),
		shards:         make(map[string]*shard),
		lruList:        list.New(),
		namespaceUsage: make(map[string]int64),
		namespaceCaps:  make(map[string]int64),
		defaultCap:     defaultCap,
		defaultCapPct:  cfg.DefaultCapPct,
		tempTracker:    NewTemperatureTracker(),
	}
}

// makeEntryKey creates a unique key for an entry.
func makeEntryKey(key MemoryCacheKey) string {
	return key.Namespace + "/" + key.ShardID + "/" + key.ItemID
}

// makeShardKey creates a unique key for a shard.
func makeShardKey(namespace, shardID string) string {
	return namespace + "/" + shardID
}

// Get retrieves a cached item.
func (mc *MemoryCache) Get(key MemoryCacheKey) ([]byte, error) {
	entryKey := makeEntryKey(key)

	mc.mu.Lock()
	defer mc.mu.Unlock()

	ent, ok := mc.entries[entryKey]
	if !ok {
		mc.misses++
		metrics.IncCacheMiss("ram", key.Namespace)
		mc.tempTracker.RecordMiss(key.Namespace)
		mc.updateTemperatureMetrics(key.Namespace)
		return nil, ErrRAMCacheMiss
	}

	mc.hits++
	metrics.IncCacheHit("ram", key.Namespace)
	mc.tempTracker.RecordHit(key.Namespace)
	mc.updateTemperatureMetrics(key.Namespace)

	// Update access time
	now := time.Now()
	ent.accessTime = now

	// Update shard access time and move to front of LRU
	shardKey := makeShardKey(key.Namespace, key.ShardID)
	if s, ok := mc.shards[shardKey]; ok {
		s.lastAccess = now
		mc.lruList.MoveToFront(s.element)
	}

	// Return a copy to prevent mutation
	result := make([]byte, len(ent.data))
	copy(result, ent.data)
	return result, nil
}

// Put stores data in the cache, evicting as needed.
func (mc *MemoryCache) Put(key MemoryCacheKey, data []byte) error {
	entryKey := makeEntryKey(key)
	size := int64(len(data))

	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Check if already cached
	if ent, ok := mc.entries[entryKey]; ok {
		// Update existing entry
		sizeDiff := size - ent.size

		if sizeDiff > 0 {
			// Check namespace cap for size increase, excluding our own shard from eviction
			if err := mc.checkNamespaceCapLockedExcluding(key.Namespace, sizeDiff, key.ShardID); err != nil {
				return err
			}

			// Also need global eviction for size increase
			if err := mc.evictLockedExcluding(sizeDiff, key.Namespace, key.ShardID); err != nil {
				return err
			}
		}

		ent.data = make([]byte, len(data))
		copy(ent.data, data)
		ent.size = size
		ent.accessTime = time.Now()

		mc.usedBytes += sizeDiff
		mc.namespaceUsage[key.Namespace] += sizeDiff

		// Update shard
		shardKey := makeShardKey(key.Namespace, key.ShardID)
		if s, ok := mc.shards[shardKey]; ok {
			s.totalSize += sizeDiff
			s.lastAccess = time.Now()
			mc.lruList.MoveToFront(s.element)
		}

		return nil
	}

	// Check namespace cap for new entry
	if err := mc.checkNamespaceCapLocked(key.Namespace, size); err != nil {
		return err
	}

	// Evict if needed (global eviction)
	if err := mc.evictLocked(size); err != nil {
		return err
	}

	// Create entry
	now := time.Now()
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	ent := &memoryEntry{
		key:        key,
		data:       dataCopy,
		size:       size,
		accessTime: now,
	}

	// Get or create shard
	shardKey := makeShardKey(key.Namespace, key.ShardID)
	s, ok := mc.shards[shardKey]
	if !ok {
		s = &shard{
			id:          key.ShardID,
			namespace:   key.Namespace,
			entries:     make(map[string]*memoryEntry),
			minPriority: key.ItemType.Priority(),
			lastAccess:  now,
		}
		s.element = mc.lruList.PushFront(s)
		mc.shards[shardKey] = s
	} else {
		// Update shard
		s.lastAccess = now
		mc.lruList.MoveToFront(s.element)
		if key.ItemType.Priority() < s.minPriority {
			s.minPriority = key.ItemType.Priority()
		}
	}

	// Add entry
	s.entries[key.ItemID] = ent
	s.totalSize += size
	mc.entries[entryKey] = ent
	mc.usedBytes += size
	mc.namespaceUsage[key.Namespace] += size

	return nil
}

// checkNamespaceCapLocked checks if adding size bytes would exceed namespace cap.
func (mc *MemoryCache) checkNamespaceCapLocked(namespace string, size int64) error {
	cap := mc.getNamespaceCapLocked(namespace)
	if cap <= 0 {
		return nil // no cap
	}

	current := mc.namespaceUsage[namespace]
	if current+size > cap {
		// Try to evict from this namespace first
		if err := mc.evictFromNamespaceLocked(namespace, size); err != nil {
			return ErrNamespaceBudgetCap
		}
	}
	return nil
}

// checkNamespaceCapLockedExcluding checks if adding size bytes would exceed namespace cap,
// excluding a specific shard from eviction. This is used when updating an existing entry.
func (mc *MemoryCache) checkNamespaceCapLockedExcluding(namespace string, size int64, excludeShardID string) error {
	cap := mc.getNamespaceCapLocked(namespace)
	if cap <= 0 {
		return nil // no cap
	}

	current := mc.namespaceUsage[namespace]
	if current+size > cap {
		// Try to evict from this namespace first, excluding our shard
		if err := mc.evictFromNamespaceLockedExcluding(namespace, size, excludeShardID); err != nil {
			return ErrNamespaceBudgetCap
		}
	}
	return nil
}

// getNamespaceCapLocked returns the cap for a namespace.
func (mc *MemoryCache) getNamespaceCapLocked(namespace string) int64 {
	if cap, ok := mc.namespaceCaps[namespace]; ok {
		return cap
	}
	if mc.defaultCap <= 0 {
		return 0
	}
	if len(mc.namespaceUsage) == 0 {
		return mc.maxBytes
	}
	if len(mc.namespaceUsage) == 1 {
		for ns := range mc.namespaceUsage {
			if ns == namespace {
				return mc.maxBytes
			}
		}
	}
	return mc.defaultCap
}

// evictFromNamespaceLocked evicts entries from a specific namespace.
func (mc *MemoryCache) evictFromNamespaceLocked(namespace string, needed int64) error {
	cap := mc.getNamespaceCapLocked(namespace)
	if cap <= 0 {
		return nil
	}

	// Find shards belonging to this namespace, sorted by priority (lowest first)
	var namespaceShard []*shard
	for elem := mc.lruList.Back(); elem != nil; elem = elem.Prev() {
		s := elem.Value.(*shard)
		if s.namespace == namespace {
			namespaceShard = append(namespaceShard, s)
		}
	}

	// Evict shards until we have enough space
	for _, s := range namespaceShard {
		if mc.namespaceUsage[namespace]+needed <= cap {
			break
		}
		mc.evictShardLocked(s)
	}

	if mc.namespaceUsage[namespace]+needed > cap {
		return ErrNamespaceBudgetCap
	}
	return nil
}

// evictFromNamespaceLockedExcluding evicts entries from a specific namespace, excluding a shard.
// This is used when updating an existing entry to prevent evicting the shard we're updating.
func (mc *MemoryCache) evictFromNamespaceLockedExcluding(namespace string, needed int64, excludeShardID string) error {
	cap := mc.getNamespaceCapLocked(namespace)
	if cap <= 0 {
		return nil
	}

	// Find shards belonging to this namespace (excluding the one we're updating)
	var namespaceShard []*shard
	for elem := mc.lruList.Back(); elem != nil; elem = elem.Prev() {
		s := elem.Value.(*shard)
		if s.namespace == namespace && s.id != excludeShardID {
			namespaceShard = append(namespaceShard, s)
		}
	}

	// Evict shards until we have enough space
	for _, s := range namespaceShard {
		if mc.namespaceUsage[namespace]+needed <= cap {
			break
		}
		mc.evictShardLocked(s)
	}

	if mc.namespaceUsage[namespace]+needed > cap {
		return ErrNamespaceBudgetCap
	}
	return nil
}

// evictLocked evicts entries until there's enough space. Must be called with lock held.
func (mc *MemoryCache) evictLocked(needed int64) error {
	for mc.usedBytes+needed > mc.maxBytes {
		// Get least recently used shard with lowest priority
		s := mc.findEvictionCandidateLocked()
		if s == nil {
			return ErrRAMCacheFull
		}
		mc.evictShardLocked(s)
	}
	return nil
}

// evictLockedExcluding evicts entries until there's enough space, but excludes a specific shard.
// This is used when updating an existing entry to prevent evicting the shard we're updating.
func (mc *MemoryCache) evictLockedExcluding(needed int64, excludeNamespace, excludeShardID string) error {
	for mc.usedBytes+needed > mc.maxBytes {
		s := mc.findEvictionCandidateLockedExcluding(excludeNamespace, excludeShardID)
		if s == nil {
			return ErrRAMCacheFull
		}
		mc.evictShardLocked(s)
	}
	return nil
}

// findEvictionCandidateLockedExcluding finds the best shard to evict, excluding a specific shard.
func (mc *MemoryCache) findEvictionCandidateLockedExcluding(excludeNamespace, excludeShardID string) *shard {
	var candidate *shard

	for elem := mc.lruList.Back(); elem != nil; elem = elem.Prev() {
		s := elem.Value.(*shard)
		if s.namespace == excludeNamespace && s.id == excludeShardID {
			continue
		}
		if candidate == nil {
			candidate = s
			continue
		}
		if s.minPriority < candidate.minPriority {
			candidate = s
		}
	}

	return candidate
}

// findEvictionCandidateLocked finds the best shard to evict.
// Priority: lower priority items first, then LRU among same priority.
func (mc *MemoryCache) findEvictionCandidateLocked() *shard {
	var candidate *shard

	// Walk from back (LRU) to front (MRU)
	for elem := mc.lruList.Back(); elem != nil; elem = elem.Prev() {
		s := elem.Value.(*shard)
		if candidate == nil {
			candidate = s
			continue
		}

		// Prefer lower priority shards
		if s.minPriority < candidate.minPriority {
			candidate = s
		}
	}

	return candidate
}

// evictShardLocked removes a shard and all its entries.
func (mc *MemoryCache) evictShardLocked(s *shard) {
	// Remove all entries in shard
	for itemID, ent := range s.entries {
		entryKey := makeEntryKey(ent.key)
		delete(mc.entries, entryKey)
		mc.usedBytes -= ent.size
		mc.namespaceUsage[s.namespace] -= ent.size
		delete(s.entries, itemID)
	}

	// Clean up namespace usage if zero
	if mc.namespaceUsage[s.namespace] <= 0 {
		delete(mc.namespaceUsage, s.namespace)
	}

	// Remove shard
	mc.lruList.Remove(s.element)
	shardKey := makeShardKey(s.namespace, s.id)
	delete(mc.shards, shardKey)
}

// SetNamespaceCap sets a custom cap for a specific namespace.
func (mc *MemoryCache) SetNamespaceCap(namespace string, maxBytes int64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if maxBytes <= 0 {
		delete(mc.namespaceCaps, namespace)
	} else {
		mc.namespaceCaps[namespace] = maxBytes
	}
}

// Delete removes a specific item from the cache.
func (mc *MemoryCache) Delete(key MemoryCacheKey) {
	entryKey := makeEntryKey(key)

	mc.mu.Lock()
	defer mc.mu.Unlock()

	ent, ok := mc.entries[entryKey]
	if !ok {
		return
	}

	// Remove from shard
	shardKey := makeShardKey(key.Namespace, key.ShardID)
	if s, ok := mc.shards[shardKey]; ok {
		delete(s.entries, key.ItemID)
		s.totalSize -= ent.size

		// Remove shard if empty
		if len(s.entries) == 0 {
			mc.lruList.Remove(s.element)
			delete(mc.shards, shardKey)
		} else {
			// Recalculate min priority
			s.minPriority = TypeTailData.Priority()
			for _, e := range s.entries {
				if e.key.ItemType.Priority() < s.minPriority {
					s.minPriority = e.key.ItemType.Priority()
				}
			}
		}
	}

	// Remove entry
	delete(mc.entries, entryKey)
	mc.usedBytes -= ent.size
	mc.namespaceUsage[key.Namespace] -= ent.size

	// Clean up namespace usage if zero
	if mc.namespaceUsage[key.Namespace] <= 0 {
		delete(mc.namespaceUsage, key.Namespace)
	}
}

// DeleteShard removes an entire shard from the cache.
func (mc *MemoryCache) DeleteShard(namespace, shardID string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	shardKey := makeShardKey(namespace, shardID)
	s, ok := mc.shards[shardKey]
	if !ok {
		return
	}

	mc.evictShardLocked(s)
}

// DeleteNamespace removes all entries for a namespace.
func (mc *MemoryCache) DeleteNamespace(namespace string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Find all shards for this namespace
	var toEvict []*shard
	for _, s := range mc.shards {
		if s.namespace == namespace {
			toEvict = append(toEvict, s)
		}
	}

	// Evict all
	for _, s := range toEvict {
		mc.evictShardLocked(s)
	}
}

// Clear removes all entries from the cache.
func (mc *MemoryCache) Clear() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.entries = make(map[string]*memoryEntry)
	mc.shards = make(map[string]*shard)
	mc.lruList = list.New()
	mc.namespaceUsage = make(map[string]int64)
	mc.usedBytes = 0
}

// Contains checks if a key is in the cache without updating access time.
func (mc *MemoryCache) Contains(key MemoryCacheKey) bool {
	entryKey := makeEntryKey(key)

	mc.mu.RLock()
	defer mc.mu.RUnlock()

	_, ok := mc.entries[entryKey]
	return ok
}

// MemoryCacheStats contains cache statistics.
type MemoryCacheStats struct {
	UsedBytes      int64
	MaxBytes       int64
	EntryCount     int
	ShardCount     int
	NamespaceCount int
	Hits           int64
	Misses         int64
	HitRatio       float64
	DefaultCapPct  int
	NamespaceUsage map[string]int64
	NamespaceCaps  map[string]int64
}

// Stats returns cache statistics.
func (mc *MemoryCache) Stats() MemoryCacheStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	hitRatio := float64(0)
	total := mc.hits + mc.misses
	if total > 0 {
		hitRatio = float64(mc.hits) / float64(total)
	}

	// Copy namespace maps
	usageCopy := make(map[string]int64)
	for k, v := range mc.namespaceUsage {
		usageCopy[k] = v
	}
	capsCopy := make(map[string]int64)
	for k, v := range mc.namespaceCaps {
		capsCopy[k] = v
	}

	return MemoryCacheStats{
		UsedBytes:      mc.usedBytes,
		MaxBytes:       mc.maxBytes,
		EntryCount:     len(mc.entries),
		ShardCount:     len(mc.shards),
		NamespaceCount: len(mc.namespaceUsage),
		Hits:           mc.hits,
		Misses:         mc.misses,
		HitRatio:       hitRatio,
		DefaultCapPct:  mc.defaultCapPct,
		NamespaceUsage: usageCopy,
		NamespaceCaps:  capsCopy,
	}
}

// GetNamespaceUsage returns the bytes used by a specific namespace.
func (mc *MemoryCache) GetNamespaceUsage(namespace string) int64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.namespaceUsage[namespace]
}

// GetShardInfo returns information about a specific shard.
func (mc *MemoryCache) GetShardInfo(namespace, shardID string) (size int64, entryCount int, exists bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	shardKey := makeShardKey(namespace, shardID)
	s, ok := mc.shards[shardKey]
	if !ok {
		return 0, 0, false
	}
	return s.totalSize, len(s.entries), true
}

// Temperature returns the overall cache temperature classification.
func (mc *MemoryCache) Temperature() string {
	return string(mc.tempTracker.Temperature())
}

// NamespaceTemperature returns the cache temperature for a specific namespace.
func (mc *MemoryCache) NamespaceTemperature(namespace string) string {
	return string(mc.tempTracker.NamespaceTemperature(namespace))
}

// TemperatureTracker returns the internal temperature tracker for advanced use.
func (mc *MemoryCache) TemperatureTracker() *TemperatureTracker {
	return mc.tempTracker
}

// TemperatureStats returns comprehensive temperature statistics.
func (mc *MemoryCache) TemperatureStats() TemperatureStats {
	return mc.tempTracker.Stats()
}

func (mc *MemoryCache) updateTemperatureMetrics(namespace string) {
	stats := mc.tempTracker.Stats()
	metrics.SetCacheTemperature("ram", string(stats.Temperature))
	metrics.SetCacheHitRatio("ram", stats.HitRatio)
	if namespace != "" {
		metrics.SetNamespaceCacheTemperature(namespace, "ram", string(mc.tempTracker.NamespaceTemperature(namespace)))
	}
}
