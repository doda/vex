package routing

import (
	"sync"
	"time"
)

// ConsistencyMode determines how queries handle freshness vs performance tradeoffs.
type ConsistencyMode string

const (
	// ConsistencyStrong ensures all committed writes are included in query results.
	// Refreshes cache and reads all WAL entries up to the current head.
	ConsistencyStrong ConsistencyMode = "strong"

	// ConsistencyEventual allows up to 60s staleness and limits tail search to 128MB.
	ConsistencyEventual ConsistencyMode = "eventual"

	// EventualMaxStaleness is the maximum staleness for eventual consistency mode.
	EventualMaxStaleness = 60 * time.Second

	// EventualMaxTailBytes is the maximum tail bytes searched in eventual mode.
	EventualMaxTailBytes = 128 * 1024 * 1024
)

// ChurnEvent represents a change in cluster topology.
type ChurnEvent struct {
	Timestamp  time.Time
	OldNodes   []Node
	NewNodes   []Node
	AddedNodes []Node
	Removed    []Node
}

// ChurnManager tracks cluster topology changes and ensures consistency during transitions.
type ChurnManager struct {
	mu              sync.RWMutex
	router          *Router
	lastChurnTime   time.Time
	churnHistory    []ChurnEvent
	snapshotVersion uint64
}

// NewChurnManager creates a new ChurnManager for the given router.
func NewChurnManager(router *Router) *ChurnManager {
	return &ChurnManager{
		router:          router,
		lastChurnTime:   time.Now(),
		churnHistory:    make([]ChurnEvent, 0),
		snapshotVersion: 1,
	}
}

// UpdateNodes updates the cluster membership and records the churn event.
// Returns true if the topology actually changed.
func (cm *ChurnManager) UpdateNodes(newNodes []Node) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	oldNodes := cm.router.Nodes()
	if nodesEqual(oldNodes, newNodes) {
		return false
	}

	added, removed := diffNodes(oldNodes, newNodes)
	event := ChurnEvent{
		Timestamp:  time.Now(),
		OldNodes:   oldNodes,
		NewNodes:   newNodes,
		AddedNodes: added,
		Removed:    removed,
	}

	cm.churnHistory = append(cm.churnHistory, event)
	cm.lastChurnTime = event.Timestamp
	cm.snapshotVersion++
	cm.router.SetNodes(newNodes)

	if len(cm.churnHistory) > 100 {
		cm.churnHistory = cm.churnHistory[len(cm.churnHistory)-100:]
	}

	return true
}

// LastChurnTime returns when the last topology change occurred.
func (cm *ChurnManager) LastChurnTime() time.Time {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.lastChurnTime
}

// SnapshotVersion returns the current snapshot version (increments on each churn).
func (cm *ChurnManager) SnapshotVersion() uint64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.snapshotVersion
}

// RecentChurn returns true if a churn event occurred within the given duration.
func (cm *ChurnManager) RecentChurn(within time.Duration) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return time.Since(cm.lastChurnTime) < within
}

// ChurnsSince returns all churn events since the given time.
func (cm *ChurnManager) ChurnsSince(since time.Time) []ChurnEvent {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var result []ChurnEvent
	for _, event := range cm.churnHistory {
		if event.Timestamp.After(since) {
			result = append(result, event)
		}
	}
	return result
}

// ConsistencySnapshot holds state needed for consistent query execution.
type ConsistencySnapshot struct {
	Mode            ConsistencyMode
	Version         uint64
	CreatedAt       time.Time
	MustRefresh     bool
	MaxTailBytes    int64
	RoutingSnapshot []Node
}

// NeedsRefresh returns whether the snapshot should be refreshed based on mode and staleness.
func (cs *ConsistencySnapshot) NeedsRefresh() bool {
	if cs.MustRefresh {
		return true
	}
	if cs.Mode == ConsistencyStrong {
		return true
	}
	return time.Since(cs.CreatedAt) > EventualMaxStaleness
}

// CreateSnapshot creates a consistency snapshot for the given mode.
func (cm *ChurnManager) CreateSnapshot(mode ConsistencyMode) *ConsistencySnapshot {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	snapshot := &ConsistencySnapshot{
		Mode:            mode,
		Version:         cm.snapshotVersion,
		CreatedAt:       time.Now(),
		RoutingSnapshot: cm.router.Nodes(),
	}

	switch mode {
	case ConsistencyStrong:
		snapshot.MustRefresh = true
		snapshot.MaxTailBytes = -1 // unlimited
	case ConsistencyEventual:
		snapshot.MustRefresh = false
		snapshot.MaxTailBytes = EventualMaxTailBytes
	default:
		snapshot.MustRefresh = true
		snapshot.MaxTailBytes = -1
	}

	return snapshot
}

// ValidateSnapshot checks if a snapshot is still valid or needs refresh due to churn.
func (cm *ChurnManager) ValidateSnapshot(snapshot *ConsistencySnapshot) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if snapshot.Mode == ConsistencyStrong {
		return snapshot.Version == cm.snapshotVersion
	}

	if time.Since(snapshot.CreatedAt) > EventualMaxStaleness {
		return false
	}

	return true
}

// nodesEqual compares two node slices for equality (order-independent).
func nodesEqual(a, b []Node) bool {
	if len(a) != len(b) {
		return false
	}

	aMap := make(map[string]string)
	for _, n := range a {
		aMap[n.ID] = n.Addr
	}

	for _, n := range b {
		if addr, ok := aMap[n.ID]; !ok || addr != n.Addr {
			return false
		}
	}
	return true
}

// diffNodes returns nodes that were added and removed between old and new sets.
func diffNodes(oldNodes, newNodes []Node) (added, removed []Node) {
	oldMap := make(map[string]Node)
	for _, n := range oldNodes {
		oldMap[n.ID] = n
	}

	newMap := make(map[string]Node)
	for _, n := range newNodes {
		newMap[n.ID] = n
	}

	for id, n := range newMap {
		if _, exists := oldMap[id]; !exists {
			added = append(added, n)
		}
	}

	for id, n := range oldMap {
		if _, exists := newMap[id]; !exists {
			removed = append(removed, n)
		}
	}

	return added, removed
}
