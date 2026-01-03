package cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vexsearch/vex/internal/metrics"
)

var (
	ErrCacheMiss    = errors.New("cache miss")
	ErrCacheFull    = errors.New("cache full")
	ErrObjectPinned = errors.New("object is pinned")
)

// CacheKey is a content-addressable key composed of object key and etag.
type CacheKey struct {
	ObjectKey string
	ETag      string
}

const metadataSuffix = ".meta"

// cacheKeyToPath converts a CacheKey to a deterministic file path.
func cacheKeyToPath(key CacheKey) string {
	h := sha256.New()
	h.Write([]byte(key.ObjectKey))
	h.Write([]byte("|"))
	h.Write([]byte(key.ETag))
	return hex.EncodeToString(h.Sum(nil))
}

type cacheMetadata struct {
	ObjectKey string `json:"object_key"`
	ETag      string `json:"etag"`
}

func metadataPath(rootPath, hash string) string {
	return filepath.Join(rootPath, hash+metadataSuffix)
}

func writeMetadata(path string, key CacheKey) error {
	metadata := cacheMetadata{
		ObjectKey: key.ObjectKey,
		ETag:      key.ETag,
	}
	payload, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	return os.WriteFile(path, payload, 0644)
}

func readMetadata(path string) (CacheKey, bool, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return CacheKey{}, false, nil
		}
		return CacheKey{}, false, err
	}
	var metadata cacheMetadata
	if err := json.Unmarshal(payload, &metadata); err != nil {
		return CacheKey{}, false, err
	}
	return CacheKey{ObjectKey: metadata.ObjectKey, ETag: metadata.ETag}, true, nil
}

// entry represents a cached object with LRU metadata.
type entry struct {
	key        CacheKey
	hash       string // The hash used as map key (always set, even for reloaded entries)
	path       string
	size       int64
	accessTime time.Time
	element    *list.Element
}

// DiskCache implements an NVMe SSD cache with LRU eviction.
type DiskCache struct {
	mu sync.RWMutex

	rootPath  string
	maxBytes  int64
	usedBytes int64
	budgetPct int

	entries map[string]*entry // hash -> entry
	lruList *list.List        // LRU list (front = most recently used)

	pinned map[string]struct{} // set of pinned namespace prefixes

	// Temperature tracking
	tempTracker *TemperatureTracker
}

// DiskCacheConfig holds configuration for the disk cache.
type DiskCacheConfig struct {
	RootPath  string // Path to store cached files
	MaxBytes  int64  // Maximum cache size in bytes (0 = use BudgetPct)
	BudgetPct int    // Percentage of disk to use (default 95)
}

// NewDiskCache creates a new disk cache.
func NewDiskCache(cfg DiskCacheConfig) (*DiskCache, error) {
	if cfg.RootPath == "" {
		cfg.RootPath = "/tmp/vex-cache"
	}
	if cfg.BudgetPct <= 0 {
		cfg.BudgetPct = 95
	}
	if cfg.BudgetPct > 100 {
		cfg.BudgetPct = 100
	}

	if err := os.MkdirAll(cfg.RootPath, 0755); err != nil {
		return nil, err
	}

	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		// Calculate from disk size
		diskBytes, err := getDiskSize(cfg.RootPath)
		if err != nil {
			// Fallback to 10GB default
			diskBytes = 10 * 1024 * 1024 * 1024
		}
		maxBytes = diskBytes * int64(cfg.BudgetPct) / 100
	}

	dc := &DiskCache{
		rootPath:    cfg.RootPath,
		maxBytes:    maxBytes,
		budgetPct:   cfg.BudgetPct,
		entries:     make(map[string]*entry),
		lruList:     list.New(),
		pinned:      make(map[string]struct{}),
		tempTracker: NewTemperatureTracker(),
	}

	// Load existing cached files
	if err := dc.loadExisting(); err != nil {
		return nil, err
	}

	return dc, nil
}

// loadExisting scans the cache directory and populates the LRU list.
func (dc *DiskCache) loadExisting() error {
	entries, err := os.ReadDir(dc.rootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == metadataSuffix {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}

		hash := e.Name()
		path := filepath.Join(dc.rootPath, hash)
		accessTime := info.ModTime()
		key, ok, err := readMetadata(metadataPath(dc.rootPath, hash))
		if err != nil || !ok {
			key = CacheKey{}
		}

		ent := &entry{
			key:        key,
			hash:       hash, // Store hash for proper eviction
			path:       path,
			size:       info.Size(),
			accessTime: accessTime,
		}
		ent.element = dc.lruList.PushBack(ent)
		dc.entries[hash] = ent
		dc.usedBytes += info.Size()
	}

	return nil
}

// Get retrieves a cached object. Returns the file path if found.
func (dc *DiskCache) Get(key CacheKey) (string, error) {
	hash := cacheKeyToPath(key)

	dc.mu.Lock()
	defer dc.mu.Unlock()

	ent, ok := dc.entries[hash]
	if !ok {
		namespace := extractNamespaceFromKey(key.ObjectKey)
		metrics.IncCacheMiss("disk", namespace)
		dc.tempTracker.RecordMiss(namespace)
		dc.updateTemperatureMetrics(namespace)
		return "", ErrCacheMiss
	}

	// Update access time and move to front of LRU
	ent.accessTime = time.Now()
	dc.lruList.MoveToFront(ent.element)

	// Touch file to update mtime for persistence
	now := time.Now()
	os.Chtimes(ent.path, now, now)

	namespace := extractNamespaceFromKey(key.ObjectKey)
	metrics.IncCacheHit("disk", namespace)
	dc.tempTracker.RecordHit(namespace)
	dc.updateTemperatureMetrics(namespace)
	return ent.path, nil
}

// extractNamespaceFromKey extracts the namespace from an object key.
// Keys typically have format: namespace/... or /namespace/...
func extractNamespaceFromKey(objectKey string) string {
	if objectKey == "" {
		return ""
	}
	key := strings.TrimPrefix(objectKey, "/")
	const prefix = "vex/namespaces/"
	if strings.HasPrefix(key, prefix) {
		rest := strings.TrimPrefix(key, prefix)
		if rest == "" {
			return ""
		}
		if idx := strings.Index(rest, "/"); idx >= 0 {
			if idx == 0 {
				return ""
			}
			return rest[:idx]
		}
		return rest
	}
	if idx := strings.Index(key, "/"); idx >= 0 {
		return key[:idx]
	}
	return key
}

// GetReader returns an io.ReadCloser for the cached object.
func (dc *DiskCache) GetReader(key CacheKey) (io.ReadCloser, error) {
	path, err := dc.Get(key)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

// Put stores data in the cache, evicting as needed.
func (dc *DiskCache) Put(key CacheKey, data io.Reader, size int64) (string, error) {
	hash := cacheKeyToPath(key)

	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Check if already cached
	if ent, ok := dc.entries[hash]; ok {
		ent.accessTime = time.Now()
		dc.lruList.MoveToFront(ent.element)
		return ent.path, nil
	}

	// Evict if needed
	if err := dc.evictLocked(size); err != nil {
		return "", err
	}

	// Write to disk
	path := filepath.Join(dc.rootPath, hash)
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}

	written, err := io.Copy(f, data)
	if err != nil {
		f.Close()
		os.Remove(path)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", err
	}

	// Add to cache
	ent := &entry{
		key:        key,
		hash:       hash,
		path:       path,
		size:       written,
		accessTime: time.Now(),
	}
	ent.element = dc.lruList.PushFront(ent)
	dc.entries[hash] = ent
	dc.usedBytes += written
	if err := writeMetadata(metadataPath(dc.rootPath, hash), key); err != nil {
		os.Remove(path)
		os.Remove(metadataPath(dc.rootPath, hash))
		dc.lruList.Remove(ent.element)
		delete(dc.entries, hash)
		dc.usedBytes -= written
		return "", err
	}

	return path, nil
}

// PutBytes is a convenience method to store bytes directly.
func (dc *DiskCache) PutBytes(key CacheKey, data []byte) (string, error) {
	hash := cacheKeyToPath(key)
	size := int64(len(data))

	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Check if already cached
	if ent, ok := dc.entries[hash]; ok {
		ent.accessTime = time.Now()
		dc.lruList.MoveToFront(ent.element)
		return ent.path, nil
	}

	// Evict if needed
	if err := dc.evictLocked(size); err != nil {
		return "", err
	}

	// Write to disk
	path := filepath.Join(dc.rootPath, hash)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return "", err
	}

	// Add to cache
	ent := &entry{
		key:        key,
		hash:       hash,
		path:       path,
		size:       size,
		accessTime: time.Now(),
	}
	ent.element = dc.lruList.PushFront(ent)
	dc.entries[hash] = ent
	dc.usedBytes += size
	if err := writeMetadata(metadataPath(dc.rootPath, hash), key); err != nil {
		os.Remove(path)
		os.Remove(metadataPath(dc.rootPath, hash))
		dc.lruList.Remove(ent.element)
		delete(dc.entries, hash)
		dc.usedBytes -= size
		return "", err
	}

	return path, nil
}

// evictLocked evicts entries until there's enough space. Must be called with lock held.
func (dc *DiskCache) evictLocked(needed int64) error {
	// Track how many entries we've checked to detect all-pinned case
	checkedCount := 0
	totalEntries := dc.lruList.Len()

	for dc.usedBytes+needed > dc.maxBytes {
		// Get least recently used entry
		elem := dc.lruList.Back()
		if elem == nil {
			// Cache is empty but still not enough space
			return ErrCacheFull
		}

		ent := elem.Value.(*entry)

		// Check if pinned
		if dc.isEntryPinnedLocked(ent) {
			checkedCount++
			if checkedCount >= totalEntries {
				// We've checked all entries and they're all pinned
				return ErrCacheFull
			}
			// Move to front and try next
			dc.lruList.MoveToFront(elem)
			continue
		}

		// Evict this entry using the stored hash
		os.Remove(ent.path)
		os.Remove(metadataPath(dc.rootPath, ent.hash))
		dc.lruList.Remove(elem)
		delete(dc.entries, ent.hash)
		dc.usedBytes -= ent.size

		// Reset counter since we made progress
		checkedCount = 0
		totalEntries = dc.lruList.Len()
	}
	return nil
}

// isEntryPinnedLocked checks if an entry is pinned. Must be called with lock held.
func (dc *DiskCache) isEntryPinnedLocked(ent *entry) bool {
	for prefix := range dc.pinned {
		if len(ent.key.ObjectKey) >= len(prefix) && ent.key.ObjectKey[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// Pin pins a namespace prefix to prevent eviction during warming.
func (dc *DiskCache) Pin(namespacePrefix string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.pinned[namespacePrefix] = struct{}{}
}

// Unpin removes a namespace prefix from the pinned set.
func (dc *DiskCache) Unpin(namespacePrefix string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	delete(dc.pinned, namespacePrefix)
}

// IsPinned checks if a namespace prefix is pinned.
func (dc *DiskCache) IsPinned(namespacePrefix string) bool {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	_, ok := dc.pinned[namespacePrefix]
	return ok
}

// Delete removes an entry from the cache.
func (dc *DiskCache) Delete(key CacheKey) error {
	hash := cacheKeyToPath(key)

	dc.mu.Lock()
	defer dc.mu.Unlock()

	ent, ok := dc.entries[hash]
	if !ok {
		return nil
	}

	os.Remove(ent.path)
	os.Remove(metadataPath(dc.rootPath, ent.hash))
	dc.lruList.Remove(ent.element)
	delete(dc.entries, hash)
	dc.usedBytes -= ent.size

	return nil
}

// Clear removes all entries from the cache.
func (dc *DiskCache) Clear() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	for _, ent := range dc.entries {
		os.Remove(ent.path)
		os.Remove(metadataPath(dc.rootPath, ent.hash))
	}

	dc.entries = make(map[string]*entry)
	dc.lruList = list.New()
	dc.usedBytes = 0

	return nil
}

// Stats returns cache statistics.
func (dc *DiskCache) Stats() CacheStats {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	return CacheStats{
		UsedBytes:   dc.usedBytes,
		MaxBytes:    dc.maxBytes,
		EntryCount:  len(dc.entries),
		PinnedCount: len(dc.pinned),
		BudgetPct:   dc.budgetPct,
	}
}

// CacheStats contains cache statistics.
type CacheStats struct {
	UsedBytes   int64
	MaxBytes    int64
	EntryCount  int
	PinnedCount int
	BudgetPct   int
}

// Contains checks if a key is in the cache without updating access time.
func (dc *DiskCache) Contains(key CacheKey) bool {
	hash := cacheKeyToPath(key)

	dc.mu.RLock()
	defer dc.mu.RUnlock()

	_, ok := dc.entries[hash]
	return ok
}

// getDiskSize returns the total size of the disk containing the given path.
func getDiskSize(path string) (int64, error) {
	var stat syscallStatfs
	if err := statfs(path, &stat); err != nil {
		return 0, err
	}
	return int64(stat.Blocks) * int64(stat.Bsize), nil
}

// Temperature returns the overall cache temperature classification.
func (dc *DiskCache) Temperature() string {
	return string(dc.tempTracker.Temperature())
}

// NamespaceTemperature returns the cache temperature for a specific namespace.
func (dc *DiskCache) NamespaceTemperature(namespace string) string {
	return string(dc.tempTracker.NamespaceTemperature(namespace))
}

// TemperatureTracker returns the internal temperature tracker for advanced use.
func (dc *DiskCache) TemperatureTracker() *TemperatureTracker {
	return dc.tempTracker
}

// TemperatureStats returns comprehensive temperature statistics.
func (dc *DiskCache) TemperatureStats() TemperatureStats {
	return dc.tempTracker.Stats()
}

func (dc *DiskCache) updateTemperatureMetrics(namespace string) {
	stats := dc.tempTracker.Stats()
	metrics.SetCacheTemperature("disk", string(stats.Temperature))
	metrics.SetCacheHitRatio("disk", stats.HitRatio)
	if namespace != "" {
		metrics.SetNamespaceCacheTemperature(namespace, "disk", string(dc.tempTracker.NamespaceTemperature(namespace)))
	}
}
