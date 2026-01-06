// Package cache provides caching functionality with temperature classification.
package cache

import (
	"sync"

	"github.com/vexsearch/vex/internal/logging"
)

const (
	// Temperature classification thresholds based on hit ratio.
	// Hot: >= 80% hit ratio (well-warmed cache)
	// Warm: >= 50% hit ratio (partially warmed)
	// Cold: < 50% hit ratio (cache warming or not used)
	HotThreshold  = 0.80
	WarmThreshold = 0.50

	// Minimum samples required before classifying (avoid noise from small samples).
	MinSamplesForClassification = 10

	// Window size for sliding window hit ratio calculation.
	DefaultWindowSize = 1000
)

// TemperatureTracker tracks cache hit/miss statistics and classifies temperature.
type TemperatureTracker struct {
	mu sync.RWMutex

	// Rolling window of recent access results (true=hit, false=miss).
	window     []bool
	windowSize int
	head       int // next write position
	count      int // number of valid entries
	windowHits int // number of hits in the window

	// Cumulative statistics.
	totalHits   int64
	totalMisses int64

	// Per-namespace statistics for granular tracking.
	namespaceStats map[string]*namespaceHitStats

	cachedTemp logging.CacheTemperature
}

// namespaceHitStats tracks per-namespace cache statistics.
type namespaceHitStats struct {
	hits   int64
	misses int64
}

// TemperatureStats contains cache temperature statistics for observability.
type TemperatureStats struct {
	Temperature    logging.CacheTemperature
	HitRatio       float64
	WindowHits     int
	WindowMisses   int
	TotalHits      int64
	TotalMisses    int64
	NamespaceCount int
}

// NewTemperatureTracker creates a new temperature tracker.
func NewTemperatureTracker() *TemperatureTracker {
	return NewTemperatureTrackerWithSize(DefaultWindowSize)
}

// NewTemperatureTrackerWithSize creates a new temperature tracker with custom window size.
func NewTemperatureTrackerWithSize(windowSize int) *TemperatureTracker {
	if windowSize <= 0 {
		windowSize = DefaultWindowSize
	}
	return &TemperatureTracker{
		window:         make([]bool, windowSize),
		windowSize:     windowSize,
		namespaceStats: make(map[string]*namespaceHitStats),
		cachedTemp:     logging.CacheCold,
	}
}

// RecordHit records a cache hit.
func (t *TemperatureTracker) RecordHit(namespace string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.recordAccessLocked(true)
	t.totalHits++

	if namespace != "" {
		stats := t.getOrCreateNamespaceStats(namespace)
		stats.hits++
	}
}

// RecordMiss records a cache miss.
func (t *TemperatureTracker) RecordMiss(namespace string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.recordAccessLocked(false)
	t.totalMisses++

	if namespace != "" {
		stats := t.getOrCreateNamespaceStats(namespace)
		stats.misses++
	}
}

// recordAccessLocked adds an access to the sliding window. Must be called with lock held.
func (t *TemperatureTracker) recordAccessLocked(hit bool) {
	if t.count == t.windowSize {
		if t.window[t.head] {
			t.windowHits--
		}
	}
	t.window[t.head] = hit
	if hit {
		t.windowHits++
	}
	t.head = (t.head + 1) % t.windowSize
	if t.count < t.windowSize {
		t.count++
	}
}

// getOrCreateNamespaceStats gets or creates stats for a namespace. Must be called with lock held.
func (t *TemperatureTracker) getOrCreateNamespaceStats(namespace string) *namespaceHitStats {
	stats, ok := t.namespaceStats[namespace]
	if !ok {
		stats = &namespaceHitStats{}
		t.namespaceStats[namespace] = stats
	}
	return stats
}

// Temperature returns the current cache temperature classification.
func (t *TemperatureTracker) Temperature() logging.CacheTemperature {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.classifyTemperatureLocked()
}

// NamespaceTemperature returns the cache temperature for a specific namespace.
func (t *TemperatureTracker) NamespaceTemperature(namespace string) logging.CacheTemperature {
	t.mu.RLock()
	defer t.mu.RUnlock()

	stats, ok := t.namespaceStats[namespace]
	if !ok {
		return logging.CacheCold
	}

	total := stats.hits + stats.misses
	if total < MinSamplesForClassification {
		return logging.CacheCold
	}

	hitRatio := float64(stats.hits) / float64(total)
	return classifyFromRatio(hitRatio)
}

// HitRatio returns the current hit ratio from the sliding window.
func (t *TemperatureTracker) HitRatio() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.hitRatioLocked()
}

// hitRatioLocked calculates hit ratio from the sliding window. Must be called with lock held.
func (t *TemperatureTracker) hitRatioLocked() float64 {
	if t.count == 0 {
		return 0
	}
	return float64(t.windowHits) / float64(t.count)
}

// classifyTemperatureLocked classifies temperature based on hit ratio. Must be called with lock held.
func (t *TemperatureTracker) classifyTemperatureLocked() logging.CacheTemperature {
	if t.count < MinSamplesForClassification {
		return logging.CacheCold
	}

	hitRatio := t.hitRatioLocked()
	return classifyFromRatio(hitRatio)
}

// classifyFromRatio classifies temperature from a hit ratio.
func classifyFromRatio(hitRatio float64) logging.CacheTemperature {
	switch {
	case hitRatio >= HotThreshold:
		return logging.CacheHot
	case hitRatio >= WarmThreshold:
		return logging.CacheWarm
	default:
		return logging.CacheCold
	}
}

// Stats returns comprehensive temperature statistics.
func (t *TemperatureTracker) Stats() TemperatureStats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	hits := t.windowHits
	misses := t.count - t.windowHits

	return TemperatureStats{
		Temperature:    t.classifyTemperatureLocked(),
		HitRatio:       t.hitRatioLocked(),
		WindowHits:     hits,
		WindowMisses:   misses,
		TotalHits:      t.totalHits,
		TotalMisses:    t.totalMisses,
		NamespaceCount: len(t.namespaceStats),
	}
}

// NamespaceHitRatio returns the hit ratio for a specific namespace.
func (t *TemperatureTracker) NamespaceHitRatio(namespace string) float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	stats, ok := t.namespaceStats[namespace]
	if !ok {
		return 0
	}

	total := stats.hits + stats.misses
	if total == 0 {
		return 0
	}

	return float64(stats.hits) / float64(total)
}

// Reset clears all statistics.
func (t *TemperatureTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.window = make([]bool, t.windowSize)
	t.head = 0
	t.count = 0
	t.windowHits = 0
	t.totalHits = 0
	t.totalMisses = 0
	t.namespaceStats = make(map[string]*namespaceHitStats)
	t.cachedTemp = logging.CacheCold
}

// ResetNamespace clears statistics for a specific namespace.
func (t *TemperatureTracker) ResetNamespace(namespace string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.namespaceStats, namespace)
}
