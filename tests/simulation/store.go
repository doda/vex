// Package simulation implements deterministic simulation tests for Vex.
//
// The simulation framework enables testing of concurrent operations, failure
// scenarios, and invariants in a reproducible way by using:
//   - A deterministic in-memory object store with fault injection
//   - A deterministic scheduler for controlling operation ordering
//   - Simulation harnesses for testing crash/recovery scenarios
package simulation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	ErrInjectedFault    = errors.New("injected fault")
	ErrOperationAborted = errors.New("operation aborted")
)

// FaultType represents the type of fault to inject.
type FaultType int

const (
	FaultNone FaultType = iota
	FaultError          // Return error
	FaultDelay          // Add delay
	FaultHang           // Block indefinitely (until context cancel)
	FaultAbort          // Abort mid-operation (for crash simulation)
)

// FaultConfig configures fault injection for an operation.
type FaultConfig struct {
	Type        FaultType
	Error       error
	Delay       time.Duration
	Probability float64 // 0.0 to 1.0 for random faults
	AfterPhase  string  // For multi-phase ops: "wal_upload", "state_read", "state_write"
}

// OpHook is called before/after operations.
type OpHook struct {
	Op        string // "Get", "Put", "PutIfAbsent", "PutIfMatch", etc.
	Key       string // Object key pattern (empty matches all)
	Pre       func(ctx context.Context, key string) error
	Post      func(ctx context.Context, key string, err error) error
	Fault     *FaultConfig
	OneShot   bool // Remove after first use
	triggered bool
}

// DeterministicStore wraps an object store with deterministic behavior and fault injection.
type DeterministicStore struct {
	inner objectstore.Store

	mu      sync.RWMutex
	hooks   []*OpHook
	history []OpRecord
	clock   *SimClock

	// Counters for determinism
	opCounter atomic.Uint64

	// Crash simulation state
	crashAfterOp string // Crash after this op type
	crashOnKey   string // Crash on this key
	crashed      bool
	crashMu      sync.Mutex
}

// OpRecord records an operation for verification.
type OpRecord struct {
	OpID      uint64
	Op        string
	Key       string
	StartTime time.Time
	EndTime   time.Time
	Error     error
	ETag      string
	Size      int64
}

// NewDeterministicStore creates a deterministic store wrapper.
func NewDeterministicStore(inner objectstore.Store, clock *SimClock) *DeterministicStore {
	if clock == nil {
		clock = NewSimClock()
	}
	return &DeterministicStore{
		inner:   inner,
		clock:   clock,
		history: make([]OpRecord, 0, 1000),
	}
}

// AddHook adds an operation hook.
func (s *DeterministicStore) AddHook(hook *OpHook) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hooks = append(s.hooks, hook)
}

// RemoveHooks removes all hooks matching the operation type.
func (s *DeterministicStore) RemoveHooks(op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var newHooks []*OpHook
	for _, h := range s.hooks {
		if h.Op != op {
			newHooks = append(newHooks, h)
		}
	}
	s.hooks = newHooks
}

// ClearHooks removes all hooks.
func (s *DeterministicStore) ClearHooks() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hooks = nil
}

// GetHistory returns a copy of the operation history.
func (s *DeterministicStore) GetHistory() []OpRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	history := make([]OpRecord, len(s.history))
	copy(history, s.history)
	return history
}

// ClearHistory clears the operation history.
func (s *DeterministicStore) ClearHistory() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.history = s.history[:0]
}

// SetCrashPoint sets a crash point to simulate node crash.
func (s *DeterministicStore) SetCrashPoint(op, key string) {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	s.crashAfterOp = op
	s.crashOnKey = key
	s.crashed = false
}

// IsCrashed returns true if a crash has been triggered.
func (s *DeterministicStore) IsCrashed() bool {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	return s.crashed
}

// Reset clears crash state.
func (s *DeterministicStore) Reset() {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	s.crashed = false
	s.crashAfterOp = ""
	s.crashOnKey = ""
}

func (s *DeterministicStore) runHooks(ctx context.Context, op, key string, pre bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var remaining []*OpHook
	for _, h := range s.hooks {
		if h.triggered && h.OneShot {
			continue
		}
		remaining = append(remaining, h)

		if h.Op != "" && h.Op != op {
			continue
		}
		if h.Key != "" && !matchKey(key, h.Key) {
			continue
		}

		if pre {
			if h.Pre != nil {
				if err := h.Pre(ctx, key); err != nil {
					return err
				}
			}
			if h.Fault != nil && h.Fault.Type != FaultNone {
				h.triggered = true
				switch h.Fault.Type {
				case FaultError:
					return h.Fault.Error
				case FaultDelay:
					s.mu.Unlock()
					select {
					case <-ctx.Done():
						s.mu.Lock()
						return ctx.Err()
					case <-time.After(h.Fault.Delay):
					}
					s.mu.Lock()
				case FaultHang:
					s.mu.Unlock()
					<-ctx.Done()
					s.mu.Lock()
					return ctx.Err()
				case FaultAbort:
					return ErrOperationAborted
				}
			}
		} else {
			if h.Post != nil {
				if err := h.Post(ctx, key, nil); err != nil {
					return err
				}
			}
		}
	}
	s.hooks = remaining
	return nil
}

func (s *DeterministicStore) recordOp(op, key string, err error, etag string, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	s.history = append(s.history, OpRecord{
		OpID:      s.opCounter.Add(1),
		Op:        op,
		Key:       key,
		StartTime: now,
		EndTime:   now,
		Error:     err,
		ETag:      etag,
		Size:      size,
	})
}

func (s *DeterministicStore) checkCrash(op, key string) bool {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()

	if s.crashAfterOp != "" && s.crashAfterOp == op {
		if s.crashOnKey == "" || s.crashOnKey == key {
			s.crashed = true
			return true
		}
	}
	return false
}

func (s *DeterministicStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	if err := s.runHooks(ctx, "Get", key, true); err != nil {
		s.recordOp("Get", key, err, "", 0)
		return nil, nil, err
	}

	reader, info, err := s.inner.Get(ctx, key, opts)

	var etag string
	var size int64
	if info != nil {
		etag = info.ETag
		size = info.Size
	}
	s.recordOp("Get", key, err, etag, size)

	if err != nil {
		return nil, nil, err
	}

	if err := s.runHooks(ctx, "Get", key, false); err != nil {
		return nil, nil, err
	}

	return reader, info, nil
}

func (s *DeterministicStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	if err := s.runHooks(ctx, "Head", key, true); err != nil {
		s.recordOp("Head", key, err, "", 0)
		return nil, err
	}

	info, err := s.inner.Head(ctx, key)

	var etag string
	var size int64
	if info != nil {
		etag = info.ETag
		size = info.Size
	}
	s.recordOp("Head", key, err, etag, size)

	if err != nil {
		return nil, err
	}

	if err := s.runHooks(ctx, "Head", key, false); err != nil {
		return nil, err
	}

	return info, nil
}

func (s *DeterministicStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if err := s.runHooks(ctx, "Put", key, true); err != nil {
		s.recordOp("Put", key, err, "", 0)
		return nil, err
	}

	info, err := s.inner.Put(ctx, key, body, size, opts)

	var etag string
	if info != nil {
		etag = info.ETag
	}
	s.recordOp("Put", key, err, etag, size)

	if err != nil {
		return nil, err
	}

	if s.checkCrash("Put", key) {
		return nil, ErrOperationAborted
	}

	if err := s.runHooks(ctx, "Put", key, false); err != nil {
		return nil, err
	}

	return info, nil
}

func (s *DeterministicStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if err := s.runHooks(ctx, "PutIfAbsent", key, true); err != nil {
		s.recordOp("PutIfAbsent", key, err, "", 0)
		return nil, err
	}

	info, err := s.inner.PutIfAbsent(ctx, key, body, size, opts)

	var etag string
	if info != nil {
		etag = info.ETag
	}
	s.recordOp("PutIfAbsent", key, err, etag, size)

	if err != nil {
		return nil, err
	}

	if s.checkCrash("PutIfAbsent", key) {
		return nil, ErrOperationAborted
	}

	if err := s.runHooks(ctx, "PutIfAbsent", key, false); err != nil {
		return nil, err
	}

	return info, nil
}

func (s *DeterministicStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if err := s.runHooks(ctx, "PutIfMatch", key, true); err != nil {
		s.recordOp("PutIfMatch", key, err, "", 0)
		return nil, err
	}

	info, err := s.inner.PutIfMatch(ctx, key, body, size, etag, opts)

	var resultETag string
	if info != nil {
		resultETag = info.ETag
	}
	s.recordOp("PutIfMatch", key, err, resultETag, size)

	if err != nil {
		return nil, err
	}

	if s.checkCrash("PutIfMatch", key) {
		return nil, ErrOperationAborted
	}

	if err := s.runHooks(ctx, "PutIfMatch", key, false); err != nil {
		return nil, err
	}

	return info, nil
}

func (s *DeterministicStore) Delete(ctx context.Context, key string) error {
	if err := s.runHooks(ctx, "Delete", key, true); err != nil {
		s.recordOp("Delete", key, err, "", 0)
		return err
	}

	err := s.inner.Delete(ctx, key)
	s.recordOp("Delete", key, err, "", 0)

	if err != nil {
		return err
	}

	if err := s.runHooks(ctx, "Delete", key, false); err != nil {
		return err
	}

	return nil
}

func (s *DeterministicStore) List(ctx context.Context, opts *objectstore.ListOptions) (*objectstore.ListResult, error) {
	if err := s.runHooks(ctx, "List", "", true); err != nil {
		s.recordOp("List", "", err, "", 0)
		return nil, err
	}

	result, err := s.inner.List(ctx, opts)
	s.recordOp("List", "", err, "", 0)

	if err != nil {
		return nil, err
	}

	if err := s.runHooks(ctx, "List", "", false); err != nil {
		return nil, err
	}

	return result, nil
}

func matchKey(key, pattern string) bool {
	if pattern == "" {
		return true
	}
	// Handle suffix pattern: *state.json
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(key, strings.TrimPrefix(pattern, "*"))
	}
	// Handle prefix pattern: vex/namespaces/*
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(key, strings.TrimSuffix(pattern, "*"))
	}
	return key == pattern
}

// SimClock provides a deterministic clock for simulation.
type SimClock struct {
	mu   sync.Mutex
	time time.Time
}

// NewSimClock creates a new simulation clock.
func NewSimClock() *SimClock {
	return &SimClock{
		time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

// Now returns the current simulated time.
func (c *SimClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.time
}

// Advance moves time forward by the given duration.
func (c *SimClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time = c.time.Add(d)
}

// Set sets the simulated time to a specific value.
func (c *SimClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time = t
}

// MemoryStoreWithLog wraps MemoryStore with logging for simulation verification.
type MemoryStoreWithLog struct {
	*objectstore.MemoryStore
	mu      sync.Mutex
	objects map[string][]byte // Snapshot of objects for verification
}

// NewMemoryStoreWithLog creates a new in-memory store with logging.
func NewMemoryStoreWithLog() *MemoryStoreWithLog {
	return &MemoryStoreWithLog{
		MemoryStore: objectstore.NewMemoryStore(),
		objects:     make(map[string][]byte),
	}
}

// Snapshot returns a copy of all objects for verification.
func (s *MemoryStoreWithLog) Snapshot() map[string][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := make(map[string][]byte)
	for k, v := range s.objects {
		data := make([]byte, len(v))
		copy(data, v)
		snapshot[k] = data
	}
	return snapshot
}

// GetKeys returns sorted list of all object keys.
func (s *MemoryStoreWithLog) GetKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Override Put to track objects
func (s *MemoryStoreWithLog) Put(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.objects[key] = data
	s.mu.Unlock()

	return s.MemoryStore.Put(ctx, key, bytes.NewReader(data), int64(len(data)), opts)
}

// Override PutIfAbsent to track objects
func (s *MemoryStoreWithLog) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}

	info, err := s.MemoryStore.PutIfAbsent(ctx, key, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.objects[key] = data
	s.mu.Unlock()

	return info, nil
}

// Override PutIfMatch to track objects
func (s *MemoryStoreWithLog) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}

	info, err := s.MemoryStore.PutIfMatch(ctx, key, bytes.NewReader(data), int64(len(data)), etag, opts)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.objects[key] = data
	s.mu.Unlock()

	return info, nil
}

// Override Delete to track objects
func (s *MemoryStoreWithLog) Delete(ctx context.Context, key string) error {
	err := s.MemoryStore.Delete(ctx, key)
	if err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()

	return nil
}

// CrashableStore wraps a store to simulate crashes at specific points.
type CrashableStore struct {
	inner    objectstore.Store
	crashMu  sync.Mutex
	crashed  bool
	crashErr error

	// Crash triggers
	crashAfterPut      bool
	crashAfterPutKey   string
	crashBeforeStateUpdate bool
}

// NewCrashableStore creates a store that can simulate crashes.
func NewCrashableStore(inner objectstore.Store) *CrashableStore {
	return &CrashableStore{
		inner:    inner,
		crashErr: fmt.Errorf("node crashed"),
	}
}

// CrashAfterPut configures a crash to occur after a Put succeeds for the given key.
func (s *CrashableStore) CrashAfterPut(key string) {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	s.crashAfterPut = true
	s.crashAfterPutKey = key
}

// CrashBeforeStateUpdate configures a crash before state.json update.
func (s *CrashableStore) CrashBeforeStateUpdate() {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	s.crashBeforeStateUpdate = true
}

// IsCrashed returns true if the store has crashed.
func (s *CrashableStore) IsCrashed() bool {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	return s.crashed
}

// Reset recovers from crash state.
func (s *CrashableStore) Reset() {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	s.crashed = false
	s.crashAfterPut = false
	s.crashAfterPutKey = ""
	s.crashBeforeStateUpdate = false
}

func (s *CrashableStore) checkCrashed() error {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	if s.crashed {
		return s.crashErr
	}
	return nil
}

func (s *CrashableStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	if err := s.checkCrashed(); err != nil {
		return nil, nil, err
	}
	return s.inner.Get(ctx, key, opts)
}

func (s *CrashableStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	if err := s.checkCrashed(); err != nil {
		return nil, err
	}
	return s.inner.Head(ctx, key)
}

func (s *CrashableStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if err := s.checkCrashed(); err != nil {
		return nil, err
	}

	info, err := s.inner.Put(ctx, key, body, size, opts)
	if err != nil {
		return nil, err
	}

	s.crashMu.Lock()
	if s.crashAfterPut && (s.crashAfterPutKey == "" || s.crashAfterPutKey == key) {
		s.crashed = true
		s.crashMu.Unlock()
		return nil, s.crashErr
	}
	s.crashMu.Unlock()

	return info, nil
}

func (s *CrashableStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if err := s.checkCrashed(); err != nil {
		return nil, err
	}

	info, err := s.inner.PutIfAbsent(ctx, key, body, size, opts)
	if err != nil {
		return nil, err
	}

	s.crashMu.Lock()
	if s.crashAfterPut && (s.crashAfterPutKey == "" || s.crashAfterPutKey == key) {
		s.crashed = true
		s.crashMu.Unlock()
		return nil, s.crashErr
	}
	s.crashMu.Unlock()

	return info, nil
}

func (s *CrashableStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if err := s.checkCrashed(); err != nil {
		return nil, err
	}

	// Check for state update crash point
	s.crashMu.Lock()
	if s.crashBeforeStateUpdate && strings.HasSuffix(key, "state.json") {
		s.crashed = true
		s.crashMu.Unlock()
		return nil, s.crashErr
	}
	s.crashMu.Unlock()

	return s.inner.PutIfMatch(ctx, key, body, size, etag, opts)
}

func (s *CrashableStore) Delete(ctx context.Context, key string) error {
	if err := s.checkCrashed(); err != nil {
		return err
	}
	return s.inner.Delete(ctx, key)
}

func (s *CrashableStore) List(ctx context.Context, opts *objectstore.ListOptions) (*objectstore.ListResult, error) {
	if err := s.checkCrashed(); err != nil {
		return nil, err
	}
	return s.inner.List(ctx, opts)
}
