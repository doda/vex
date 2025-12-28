// Package guardrails implements per-namespace resource limits for multi-tenancy.
//
// Guardrails provide:
//   - Demand-loaded per-namespace in-memory state
//   - LRU eviction of cold namespace state
//   - Per-namespace memory caps for tail materialization
//   - Concurrent cold cache fill limiting
package guardrails

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds configuration for guardrails.
type Config struct {
	// MaxNamespaces is the maximum number of namespaces to keep in memory.
	// Older namespaces are evicted when this limit is exceeded.
	// Default: 1000
	MaxNamespaces int

	// MaxTailBytesPerNamespace is the maximum tail bytes per namespace.
	// Default: 256 MB (from tail.Config.MaxRAMBytes)
	MaxTailBytesPerNamespace int64

	// MaxConcurrentColdFills limits parallel cold cache fills.
	// Default: 4
	MaxConcurrentColdFills int

	// IdleEvictionTimeout is how long a namespace can be idle before eviction.
	// 0 means no time-based eviction (only count-based).
	IdleEvictionTimeout time.Duration
}

// DefaultConfig returns the default guardrails configuration.
func DefaultConfig() Config {
	return Config{
		MaxNamespaces:            1000,
		MaxTailBytesPerNamespace: 256 * 1024 * 1024, // 256 MB
		MaxConcurrentColdFills:   4,
		IdleEvictionTimeout:      0, // No time-based eviction by default
	}
}

// NamespaceState holds per-namespace in-memory state.
type NamespaceState struct {
	Namespace  string
	LoadedAt   time.Time
	LastAccess time.Time
	TailBytes  int64

	// Custom data attached by users of the guardrails
	Data any
}

// Manager manages per-namespace guardrails.
type Manager struct {
	mu sync.RWMutex

	cfg Config

	// LRU list of namespaces (front = most recently used)
	lruList *list.List

	// namespace -> list element mapping
	namespaces map[string]*list.Element

	// Cold fill semaphore
	coldFillSem chan struct{}

	// Metrics
	loadsTotal   atomic.Int64
	evictsTotal  atomic.Int64
	coldFillWait atomic.Int64
}

// New creates a new guardrails manager.
func New(cfg Config) *Manager {
	if cfg.MaxNamespaces <= 0 {
		cfg.MaxNamespaces = DefaultConfig().MaxNamespaces
	}
	if cfg.MaxTailBytesPerNamespace <= 0 {
		cfg.MaxTailBytesPerNamespace = DefaultConfig().MaxTailBytesPerNamespace
	}
	if cfg.MaxConcurrentColdFills <= 0 {
		cfg.MaxConcurrentColdFills = DefaultConfig().MaxConcurrentColdFills
	}

	return &Manager{
		cfg:         cfg,
		lruList:     list.New(),
		namespaces:  make(map[string]*list.Element),
		coldFillSem: make(chan struct{}, cfg.MaxConcurrentColdFills),
	}
}

// Get retrieves namespace state, returning nil if not loaded.
func (m *Manager) Get(namespace string) *NamespaceState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	elem, ok := m.namespaces[namespace]
	if !ok {
		return nil
	}

	state := elem.Value.(*NamespaceState)
	return state
}

// Touch updates the last access time for a namespace and moves it to front of LRU.
// Returns false if the namespace is not loaded.
func (m *Manager) Touch(namespace string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	elem, ok := m.namespaces[namespace]
	if !ok {
		return false
	}

	state := elem.Value.(*NamespaceState)
	state.LastAccess = time.Now()
	m.lruList.MoveToFront(elem)
	return true
}

// Load loads namespace state if not already loaded.
// Returns the state and whether it was newly loaded (vs already present).
// The loadFn is called only if the namespace needs to be loaded.
func (m *Manager) Load(namespace string, loadFn func() (any, error)) (*NamespaceState, bool, error) {
	// Fast path: check if already loaded
	m.mu.RLock()
	elem, ok := m.namespaces[namespace]
	if ok {
		state := elem.Value.(*NamespaceState)
		m.mu.RUnlock()
		// Touch under write lock
		m.mu.Lock()
		state.LastAccess = time.Now()
		m.lruList.MoveToFront(elem)
		m.mu.Unlock()
		return state, false, nil
	}
	m.mu.RUnlock()

	// Slow path: need to load
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if elem, ok := m.namespaces[namespace]; ok {
		state := elem.Value.(*NamespaceState)
		state.LastAccess = time.Now()
		m.lruList.MoveToFront(elem)
		return state, false, nil
	}

	// Evict if at capacity
	m.evictIfNeededLocked()

	// Load the data
	var data any
	var err error
	if loadFn != nil {
		data, err = loadFn()
		if err != nil {
			return nil, false, err
		}
	}

	now := time.Now()
	state := &NamespaceState{
		Namespace:  namespace,
		LoadedAt:   now,
		LastAccess: now,
		Data:       data,
	}

	elem = m.lruList.PushFront(state)
	m.namespaces[namespace] = elem
	m.loadsTotal.Add(1)

	return state, true, nil
}

// evictIfNeededLocked evicts namespaces if at capacity. Must be called with lock held.
func (m *Manager) evictIfNeededLocked() {
	for m.lruList.Len() >= m.cfg.MaxNamespaces {
		m.evictOldestLocked()
	}

	// Also evict idle namespaces if configured
	if m.cfg.IdleEvictionTimeout > 0 {
		m.evictIdleLocked()
	}
}

// evictOldestLocked evicts the least recently used namespace. Must be called with lock held.
func (m *Manager) evictOldestLocked() bool {
	back := m.lruList.Back()
	if back == nil {
		return false
	}

	state := back.Value.(*NamespaceState)
	m.lruList.Remove(back)
	delete(m.namespaces, state.Namespace)
	m.evictsTotal.Add(1)
	return true
}

// evictIdleLocked evicts namespaces idle longer than the timeout. Must be called with lock held.
func (m *Manager) evictIdleLocked() {
	if m.cfg.IdleEvictionTimeout <= 0 {
		return
	}

	cutoff := time.Now().Add(-m.cfg.IdleEvictionTimeout)
	var toRemove []*list.Element

	for elem := m.lruList.Back(); elem != nil; elem = elem.Prev() {
		state := elem.Value.(*NamespaceState)
		if state.LastAccess.Before(cutoff) {
			toRemove = append(toRemove, elem)
		} else {
			// Since list is ordered by access time (most recent first),
			// we can stop once we hit a non-idle namespace
			break
		}
	}

	for _, elem := range toRemove {
		state := elem.Value.(*NamespaceState)
		m.lruList.Remove(elem)
		delete(m.namespaces, state.Namespace)
		m.evictsTotal.Add(1)
	}
}

// Evict removes a specific namespace from the cache.
func (m *Manager) Evict(namespace string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	elem, ok := m.namespaces[namespace]
	if !ok {
		return false
	}

	m.lruList.Remove(elem)
	delete(m.namespaces, namespace)
	m.evictsTotal.Add(1)
	return true
}

// SetTailBytes updates the tail bytes for a namespace.
// Returns error if the namespace is not loaded or exceeds the cap.
func (m *Manager) SetTailBytes(namespace string, bytes int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	elem, ok := m.namespaces[namespace]
	if !ok {
		return ErrNamespaceNotLoaded
	}

	if bytes > m.cfg.MaxTailBytesPerNamespace {
		return ErrTailBytesExceeded
	}

	state := elem.Value.(*NamespaceState)
	state.TailBytes = bytes
	return nil
}

// CheckTailBytes checks if adding bytes would exceed the namespace cap.
func (m *Manager) CheckTailBytes(namespace string, additionalBytes int64) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	elem, ok := m.namespaces[namespace]
	if !ok {
		// Not loaded, use 0 as current bytes
		if additionalBytes > m.cfg.MaxTailBytesPerNamespace {
			return ErrTailBytesExceeded
		}
		return nil
	}

	state := elem.Value.(*NamespaceState)
	if state.TailBytes+additionalBytes > m.cfg.MaxTailBytesPerNamespace {
		return ErrTailBytesExceeded
	}
	return nil
}

// AcquireColdFill acquires a slot for cold cache filling.
// Blocks until a slot is available or context is cancelled.
func (m *Manager) AcquireColdFill(ctx context.Context) error {
	m.coldFillWait.Add(1)
	defer m.coldFillWait.Add(-1)

	select {
	case m.coldFillSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReleaseColdFill releases a cold fill slot.
func (m *Manager) ReleaseColdFill() {
	select {
	case <-m.coldFillSem:
	default:
		// This shouldn't happen, but avoid blocking
	}
}

// TryAcquireColdFill tries to acquire a cold fill slot without blocking.
// Returns true if acquired, false if no slots available.
func (m *Manager) TryAcquireColdFill() bool {
	select {
	case m.coldFillSem <- struct{}{}:
		return true
	default:
		return false
	}
}

// Stats returns current guardrails statistics.
type Stats struct {
	LoadedNamespaces int
	MaxNamespaces    int
	LoadsTotal       int64
	EvictsTotal      int64
	ColdFillsActive  int
	ColdFillsWaiting int64
	MaxColdFills     int
}

// Stats returns current guardrails statistics.
func (m *Manager) Stats() Stats {
	m.mu.RLock()
	loadedCount := m.lruList.Len()
	m.mu.RUnlock()

	activeColdFills := len(m.coldFillSem)

	return Stats{
		LoadedNamespaces: loadedCount,
		MaxNamespaces:    m.cfg.MaxNamespaces,
		LoadsTotal:       m.loadsTotal.Load(),
		EvictsTotal:      m.evictsTotal.Load(),
		ColdFillsActive:  activeColdFills,
		ColdFillsWaiting: m.coldFillWait.Load(),
		MaxColdFills:     m.cfg.MaxConcurrentColdFills,
	}
}

// GetConfig returns the current configuration.
func (m *Manager) GetConfig() Config {
	return m.cfg
}

// Clear removes all loaded namespaces.
func (m *Manager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lruList = list.New()
	m.namespaces = make(map[string]*list.Element)
}

// Namespaces returns a list of all loaded namespace names.
func (m *Manager) Namespaces() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.namespaces))
	for name := range m.namespaces {
		names = append(names, name)
	}
	return names
}
