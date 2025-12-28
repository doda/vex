package wal

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	walKeyPattern = regexp.MustCompile(`^wal/(\d+)\.wal\.zst$`)
)

// RepairResult contains the result of a repair operation.
type RepairResult struct {
	Namespace            string
	OriginalHeadSeq      uint64
	NewHeadSeq           uint64
	WALsRepaired         int
	HighestContiguousSeq uint64
	OrphanedWALs         []uint64
}

// Repairer handles detection and repair of partial WAL commits.
// It can detect WAL entries that were uploaded but whose corresponding
// state updates failed, and safely advance the state head_seq.
type Repairer struct {
	store        objectstore.Store
	stateManager *namespace.StateManager
}

// NewRepairer creates a new WAL repairer.
func NewRepairer(store objectstore.Store, stateManager *namespace.StateManager) *Repairer {
	return &Repairer{
		store:        store,
		stateManager: stateManager,
	}
}

// DetectHighestContiguousSeq scans WAL objects and finds the highest contiguous
// sequence number starting from the current state head_seq.
// Returns the highest contiguous seq and a list of any orphaned (gap) WALs.
func (r *Repairer) DetectHighestContiguousSeq(ctx context.Context, ns string) (uint64, []uint64, error) {
	// Load current state
	loaded, err := r.stateManager.Load(ctx, ns)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to load state: %w", err)
	}

	currentHeadSeq := loaded.State.WAL.HeadSeq

	// List all WAL objects for this namespace
	walPrefix := fmt.Sprintf("vex/namespaces/%s/wal/", ns)
	walSeqs, err := r.listWALSequences(ctx, walPrefix)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to list WAL objects: %w", err)
	}

	// Sort sequences
	sort.Slice(walSeqs, func(i, j int) bool {
		return walSeqs[i] < walSeqs[j]
	})

	// Find highest contiguous sequence starting from currentHeadSeq
	highestContiguous := currentHeadSeq
	var orphaned []uint64

	for _, seq := range walSeqs {
		if seq <= currentHeadSeq {
			// Already accounted for in state
			continue
		}
		if seq == highestContiguous+1 {
			// Contiguous
			highestContiguous = seq
		} else if seq > highestContiguous+1 {
			// Gap detected - these are orphaned WALs
			orphaned = append(orphaned, seq)
		}
	}

	return highestContiguous, orphaned, nil
}

// listWALSequences lists all WAL sequence numbers in the given prefix.
func (r *Repairer) listWALSequences(ctx context.Context, prefix string) ([]uint64, error) {
	var seqs []uint64
	var marker string

	for {
		result, err := r.store.List(ctx, &objectstore.ListOptions{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Objects {
			// Extract the filename portion
			relPath := strings.TrimPrefix(obj.Key, prefix)
			relPath = "wal/" + relPath
			matches := walKeyPattern.FindStringSubmatch(relPath)
			if len(matches) == 2 {
				seq, err := strconv.ParseUint(matches[1], 10, 64)
				if err == nil {
					seqs = append(seqs, seq)
				}
			}
		}

		if !result.IsTruncated || result.NextMarker == "" {
			break
		}
		marker = result.NextMarker
	}

	return seqs, nil
}

// Repair detects and repairs partial WAL commits for a namespace.
// It advances the state head_seq to the highest contiguous WAL sequence.
// Returns a RepairResult describing what was done.
func (r *Repairer) Repair(ctx context.Context, ns string) (*RepairResult, error) {
	result := &RepairResult{
		Namespace: ns,
	}

	// Load current state
	loaded, err := r.stateManager.Load(ctx, ns)
	if err != nil {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	result.OriginalHeadSeq = loaded.State.WAL.HeadSeq

	// Detect highest contiguous sequence
	highestContiguous, orphaned, err := r.DetectHighestContiguousSeq(ctx, ns)
	if err != nil {
		return nil, err
	}

	result.HighestContiguousSeq = highestContiguous
	result.OrphanedWALs = orphaned

	// If state is already up to date, nothing to do
	if highestContiguous <= loaded.State.WAL.HeadSeq {
		result.NewHeadSeq = loaded.State.WAL.HeadSeq
		return result, nil
	}

	// Advance state to highest contiguous sequence
	// We do this incrementally to maintain the +1 invariant
	currentETag := loaded.ETag
	currentSeq := loaded.State.WAL.HeadSeq

	for seq := currentSeq + 1; seq <= highestContiguous; seq++ {
		// Verify the WAL entry actually exists before advancing
		walKey := walKeyForNamespace(ns, seq)
		info, err := r.store.Head(ctx, walKey)
		if err != nil {
			if objectstore.IsNotFoundError(err) {
				// WAL doesn't exist, we've hit the real limit
				break
			}
			return nil, fmt.Errorf("failed to verify WAL seq %d: %w", seq, err)
		}

		// Read the WAL to get size for unindexed bytes tracking
		reader, _, err := r.store.Get(ctx, walKey, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL seq %d: %w", seq, err)
		}
		// Read to get size (we don't need the content for repair)
		var bytesWritten int64 = 0
		if info != nil {
			bytesWritten = info.Size
		}
		reader.Close()

		// Update state with this WAL entry
		walKeyRelative := KeyForSeq(seq)
		newLoaded, err := r.stateManager.AdvanceWAL(ctx, ns, currentETag, walKeyRelative, bytesWritten, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to advance state to seq %d: %w", seq, err)
		}

		currentETag = newLoaded.ETag
		result.WALsRepaired++
	}

	result.NewHeadSeq = result.OriginalHeadSeq + uint64(result.WALsRepaired)

	return result, nil
}

// RepairTask runs the repair process periodically for a namespace.
// It implements the staleness bound specified in the task.
type RepairTask struct {
	repairer       *Repairer
	interval       time.Duration
	stalenessBound time.Duration
	stopCh         chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
	lastRepair     map[string]time.Time
	lastRepairSeq  map[string]uint64
}

// NewRepairTask creates a new RepairTask with the given interval.
// The stalenessBound controls how long before a namespace is considered
// potentially stale and in need of repair checking.
func NewRepairTask(repairer *Repairer, interval time.Duration, stalenessBound time.Duration) *RepairTask {
	if stalenessBound == 0 {
		stalenessBound = 60 * time.Second // Default 60s staleness bound per spec
	}
	if interval == 0 {
		interval = 10 * time.Second // Check every 10s by default
	}
	return &RepairTask{
		repairer:       repairer,
		interval:       interval,
		stalenessBound: stalenessBound,
		stopCh:         make(chan struct{}),
		lastRepair:     make(map[string]time.Time),
		lastRepairSeq:  make(map[string]uint64),
	}
}

// Start begins the repair task for the given namespaces.
func (t *RepairTask) Start(ctx context.Context, namespaces []string) {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.run(ctx, namespaces)
	}()
}

// Stop stops the repair task.
func (t *RepairTask) Stop() {
	close(t.stopCh)
	t.wg.Wait()
}

// run is the main repair loop.
func (t *RepairTask) run(ctx context.Context, namespaces []string) {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.stopCh:
			return
		case <-ticker.C:
			for _, ns := range namespaces {
				if err := t.repairIfNeeded(ctx, ns); err != nil {
					// Log error but continue with other namespaces
					continue
				}
			}
		}
	}
}

// repairIfNeeded checks if a namespace needs repair and performs it if so.
func (t *RepairTask) repairIfNeeded(ctx context.Context, ns string) error {
	t.mu.RLock()
	lastTime := t.lastRepair[ns]
	t.mu.RUnlock()

	// Only repair if we haven't checked recently (staleness bound)
	if time.Since(lastTime) < t.stalenessBound {
		return nil
	}

	result, err := t.repairer.Repair(ctx, ns)
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.lastRepair[ns] = time.Now()
	t.lastRepairSeq[ns] = result.NewHeadSeq
	t.mu.Unlock()

	return nil
}

// GetLastRepairTime returns the last repair time for a namespace.
func (t *RepairTask) GetLastRepairTime(ns string) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastRepair[ns]
}

// GetLastRepairSeq returns the last repaired sequence for a namespace.
func (t *RepairTask) GetLastRepairSeq(ns string) uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastRepairSeq[ns]
}

// RepairOnce runs a single repair check for a namespace.
// Useful for on-demand repairs or testing.
func (t *RepairTask) RepairOnce(ctx context.Context, ns string) (*RepairResult, error) {
	result, err := t.repairer.Repair(ctx, ns)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	t.lastRepair[ns] = time.Now()
	t.lastRepairSeq[ns] = result.NewHeadSeq
	t.mu.Unlock()

	return result, nil
}

// StalenessBound returns the configured staleness bound.
func (t *RepairTask) StalenessBound() time.Duration {
	return t.stalenessBound
}
