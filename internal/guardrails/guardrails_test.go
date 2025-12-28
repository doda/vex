package guardrails

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/config"
)

func TestDemandLoad(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	// Initially, no namespace is loaded
	if state := m.Get("ns1"); state != nil {
		t.Error("expected nil for unloaded namespace")
	}

	// Load namespace
	loadCalled := false
	state, isNew, err := m.Load("ns1", func() (any, error) {
		loadCalled = true
		return "ns1-data", nil
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !isNew {
		t.Error("expected isNew=true for first load")
	}
	if !loadCalled {
		t.Error("expected loadFn to be called")
	}
	if state.Namespace != "ns1" {
		t.Errorf("expected namespace ns1, got %s", state.Namespace)
	}
	if state.Data != "ns1-data" {
		t.Errorf("expected data ns1-data, got %v", state.Data)
	}

	// Get should now return the state
	if got := m.Get("ns1"); got == nil {
		t.Error("expected state after load")
	}

	// Loading again should not call loadFn
	loadCalled = false
	state2, isNew2, err := m.Load("ns1", func() (any, error) {
		loadCalled = true
		return "ns1-data-new", nil
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if isNew2 {
		t.Error("expected isNew=false for second load")
	}
	if loadCalled {
		t.Error("loadFn should not be called for already loaded namespace")
	}
	if state2.Data != "ns1-data" {
		t.Errorf("expected original data, got %v", state2.Data)
	}
}

func TestEviction(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            3,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	// Load 3 namespaces
	for i := 0; i < 3; i++ {
		ns := string(rune('a' + i))
		_, _, err := m.Load(ns, nil)
		if err != nil {
			t.Fatalf("Load %s failed: %v", ns, err)
		}
	}

	stats := m.Stats()
	if stats.LoadedNamespaces != 3 {
		t.Errorf("expected 3 loaded, got %d", stats.LoadedNamespaces)
	}

	// Loading 4th namespace should evict the oldest
	_, _, err := m.Load("d", nil)
	if err != nil {
		t.Fatalf("Load d failed: %v", err)
	}

	stats = m.Stats()
	if stats.LoadedNamespaces != 3 {
		t.Errorf("expected 3 loaded after eviction, got %d", stats.LoadedNamespaces)
	}
	if stats.EvictsTotal != 1 {
		t.Errorf("expected 1 eviction, got %d", stats.EvictsTotal)
	}

	// "a" should be evicted (oldest)
	if m.Get("a") != nil {
		t.Error("expected 'a' to be evicted")
	}

	// b, c, d should still be loaded
	for _, ns := range []string{"b", "c", "d"} {
		if m.Get(ns) == nil {
			t.Errorf("expected %s to still be loaded", ns)
		}
	}
}

func TestEvictionLRU(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            3,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	// Load a, b, c in order
	for _, ns := range []string{"a", "b", "c"} {
		_, _, err := m.Load(ns, nil)
		if err != nil {
			t.Fatalf("Load %s failed: %v", ns, err)
		}
	}

	// Touch "a" to make it recently used
	if !m.Touch("a") {
		t.Error("Touch should return true for loaded namespace")
	}

	// Load "d" - should evict "b" (now oldest)
	_, _, err := m.Load("d", nil)
	if err != nil {
		t.Fatalf("Load d failed: %v", err)
	}

	if m.Get("b") != nil {
		t.Error("expected 'b' to be evicted")
	}
	if m.Get("a") == nil {
		t.Error("expected 'a' to still be loaded (was touched)")
	}
}

func TestManualEviction(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	_, _, err := m.Load("ns1", nil)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if m.Get("ns1") == nil {
		t.Error("expected ns1 to be loaded")
	}

	if !m.Evict("ns1") {
		t.Error("Evict should return true for loaded namespace")
	}

	if m.Get("ns1") != nil {
		t.Error("expected ns1 to be evicted")
	}

	if m.Evict("ns1") {
		t.Error("Evict should return false for already evicted namespace")
	}
}

func TestTailBytesEnforcement(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	_, _, err := m.Load("ns1", nil)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Set bytes within limit
	if err := m.SetTailBytes("ns1", 50); err != nil {
		t.Errorf("SetTailBytes should succeed: %v", err)
	}

	// Set bytes at limit
	if err := m.SetTailBytes("ns1", 100); err != nil {
		t.Errorf("SetTailBytes at limit should succeed: %v", err)
	}

	// Set bytes over limit
	if err := m.SetTailBytes("ns1", 101); err != ErrTailBytesExceeded {
		t.Errorf("expected ErrTailBytesExceeded, got %v", err)
	}

	// Check on unloaded namespace
	if err := m.CheckTailBytes("unloaded", 50); err != nil {
		t.Errorf("CheckTailBytes on unloaded should succeed: %v", err)
	}
	if err := m.CheckTailBytes("unloaded", 150); err != ErrTailBytesExceeded {
		t.Errorf("expected ErrTailBytesExceeded for unloaded, got %v", err)
	}

	// Check on loaded namespace with existing bytes
	if err := m.CheckTailBytes("ns1", 1); err != ErrTailBytesExceeded {
		t.Errorf("expected ErrTailBytesExceeded (100+1 > 100), got %v", err)
	}

	// Set to lower value and check again
	m.SetTailBytes("ns1", 50)
	if err := m.CheckTailBytes("ns1", 50); err != nil {
		t.Errorf("CheckTailBytes should succeed (50+50 = 100): %v", err)
	}
	if err := m.CheckTailBytes("ns1", 51); err != ErrTailBytesExceeded {
		t.Errorf("expected ErrTailBytesExceeded (50+51 > 100), got %v", err)
	}
}

func TestColdFillLimiting(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   2,
	})

	// Acquire 2 slots
	ctx := context.Background()
	if err := m.AcquireColdFill(ctx); err != nil {
		t.Fatalf("AcquireColdFill 1 failed: %v", err)
	}
	if err := m.AcquireColdFill(ctx); err != nil {
		t.Fatalf("AcquireColdFill 2 failed: %v", err)
	}

	stats := m.Stats()
	if stats.ColdFillsActive != 2 {
		t.Errorf("expected 2 active cold fills, got %d", stats.ColdFillsActive)
	}

	// Try to acquire 3rd - should fail with timeout
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := m.AcquireColdFill(ctxTimeout); err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}

	// TryAcquire should fail
	if m.TryAcquireColdFill() {
		t.Error("TryAcquireColdFill should return false when at limit")
	}

	// Release one slot
	m.ReleaseColdFill()

	stats = m.Stats()
	if stats.ColdFillsActive != 1 {
		t.Errorf("expected 1 active after release, got %d", stats.ColdFillsActive)
	}

	// Now TryAcquire should succeed
	if !m.TryAcquireColdFill() {
		t.Error("TryAcquireColdFill should return true after release")
	}

	// Clean up
	m.ReleaseColdFill()
	m.ReleaseColdFill()
}

func TestConcurrentColdFillWaiting(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   2,
	})

	ctx := context.Background()

	// Acquire all slots
	m.AcquireColdFill(ctx)
	m.AcquireColdFill(ctx)

	// Start goroutines that will wait
	var waitingCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			waitingCount.Add(1)
			m.AcquireColdFill(ctx)
			waitingCount.Add(-1)
			m.ReleaseColdFill()
		}()
	}

	// Wait a bit for goroutines to start waiting
	time.Sleep(50 * time.Millisecond)

	// Check that we have waiters
	stats := m.Stats()
	if stats.ColdFillsWaiting == 0 {
		t.Error("expected some cold fill waiters")
	}

	// Release slots to unblock waiters
	m.ReleaseColdFill()
	m.ReleaseColdFill()

	// Wait for all to complete
	wg.Wait()

	stats = m.Stats()
	if stats.ColdFillsActive != 0 {
		t.Errorf("expected 0 active after cleanup, got %d", stats.ColdFillsActive)
	}
}

func TestIdleEviction(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
		IdleEvictionTimeout:      50 * time.Millisecond,
	})

	// Load namespace
	_, _, err := m.Load("ns1", nil)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Should still be there
	if m.Get("ns1") == nil {
		t.Error("expected ns1 to be loaded")
	}

	// Wait for idle timeout
	time.Sleep(100 * time.Millisecond)

	// Load another namespace to trigger eviction check
	_, _, err = m.Load("ns2", nil)
	if err != nil {
		t.Fatalf("Load ns2 failed: %v", err)
	}

	// ns1 should be evicted due to idle timeout
	if m.Get("ns1") != nil {
		t.Error("expected ns1 to be evicted due to idle timeout")
	}
}

func TestClear(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	for i := 0; i < 5; i++ {
		ns := string(rune('a' + i))
		m.Load(ns, nil)
	}

	stats := m.Stats()
	if stats.LoadedNamespaces != 5 {
		t.Errorf("expected 5 loaded, got %d", stats.LoadedNamespaces)
	}

	m.Clear()

	stats = m.Stats()
	if stats.LoadedNamespaces != 0 {
		t.Errorf("expected 0 loaded after clear, got %d", stats.LoadedNamespaces)
	}
}

func TestNamespacesList(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	expected := []string{"ns1", "ns2", "ns3"}
	for _, ns := range expected {
		m.Load(ns, nil)
	}

	names := m.Namespaces()
	if len(names) != len(expected) {
		t.Errorf("expected %d namespaces, got %d", len(expected), len(names))
	}

	// Check all expected are present (order not guaranteed)
	found := make(map[string]bool)
	for _, name := range names {
		found[name] = true
	}
	for _, exp := range expected {
		if !found[exp] {
			t.Errorf("expected %s in namespaces list", exp)
		}
	}
}

func TestConcurrentLoads(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            100,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	var wg sync.WaitGroup
	var loadCount atomic.Int32

	// Concurrently load the same namespace
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, isNew, err := m.Load("shared", func() (any, error) {
				loadCount.Add(1)
				return "data", nil
			})
			if err != nil {
				t.Errorf("Load failed: %v", err)
			}
			if isNew {
				// Only one should see isNew=true
			}
		}()
	}

	wg.Wait()

	// loadFn should only be called once
	if loadCount.Load() != 1 {
		t.Errorf("expected loadFn to be called once, called %d times", loadCount.Load())
	}

	stats := m.Stats()
	if stats.LoadedNamespaces != 1 {
		t.Errorf("expected 1 namespace, got %d", stats.LoadedNamespaces)
	}
}

func TestTouchUnloaded(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	if m.Touch("nonexistent") {
		t.Error("Touch should return false for unloaded namespace")
	}
}

func TestSetTailBytesUnloaded(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	err := m.SetTailBytes("nonexistent", 50)
	if err != ErrNamespaceNotLoaded {
		t.Errorf("expected ErrNamespaceNotLoaded, got %v", err)
	}
}

func TestLoadWithNilLoadFn(t *testing.T) {
	m := New(Config{
		MaxNamespaces:            10,
		MaxTailBytesPerNamespace: 100,
		MaxConcurrentColdFills:   4,
	})

	state, isNew, err := m.Load("ns1", nil)
	if err != nil {
		t.Fatalf("Load with nil loadFn failed: %v", err)
	}
	if !isNew {
		t.Error("expected isNew=true")
	}
	if state.Data != nil {
		t.Errorf("expected nil data, got %v", state.Data)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxNamespaces != 1000 {
		t.Errorf("expected MaxNamespaces=1000, got %d", cfg.MaxNamespaces)
	}
	if cfg.MaxTailBytesPerNamespace != 256*1024*1024 {
		t.Errorf("expected MaxTailBytesPerNamespace=256MB, got %d", cfg.MaxTailBytesPerNamespace)
	}
	if cfg.MaxConcurrentColdFills != 4 {
		t.Errorf("expected MaxConcurrentColdFills=4, got %d", cfg.MaxConcurrentColdFills)
	}
}

func TestConfigDefaults(t *testing.T) {
	// Test that zero values are replaced with defaults
	m := New(Config{})

	cfg := m.GetConfig()
	if cfg.MaxNamespaces != 1000 {
		t.Errorf("expected MaxNamespaces=1000, got %d", cfg.MaxNamespaces)
	}
	if cfg.MaxTailBytesPerNamespace != 256*1024*1024 {
		t.Errorf("expected MaxTailBytesPerNamespace=256MB, got %d", cfg.MaxTailBytesPerNamespace)
	}
	if cfg.MaxConcurrentColdFills != 4 {
		t.Errorf("expected MaxConcurrentColdFills=4, got %d", cfg.MaxConcurrentColdFills)
	}
}

func TestFromConfig(t *testing.T) {
	tests := []struct {
		name       string
		configCfg  config.GuardrailsConfig
		expectCfg  Config
	}{
		{
			name:       "zero values use defaults",
			configCfg:  config.GuardrailsConfig{},
			expectCfg:  DefaultConfig(),
		},
		{
			name: "custom values",
			configCfg: config.GuardrailsConfig{
				MaxNamespaces:          500,
				MaxTailBytesMB:         128,
				MaxConcurrentColdFills: 8,
			},
			expectCfg: Config{
				MaxNamespaces:            500,
				MaxTailBytesPerNamespace: 128 * 1024 * 1024,
				MaxConcurrentColdFills:   8,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FromConfig(tt.configCfg)
			if got.MaxNamespaces != tt.expectCfg.MaxNamespaces {
				t.Errorf("MaxNamespaces = %d, want %d", got.MaxNamespaces, tt.expectCfg.MaxNamespaces)
			}
			if got.MaxTailBytesPerNamespace != tt.expectCfg.MaxTailBytesPerNamespace {
				t.Errorf("MaxTailBytesPerNamespace = %d, want %d", got.MaxTailBytesPerNamespace, tt.expectCfg.MaxTailBytesPerNamespace)
			}
			if got.MaxConcurrentColdFills != tt.expectCfg.MaxConcurrentColdFills {
				t.Errorf("MaxConcurrentColdFills = %d, want %d", got.MaxConcurrentColdFills, tt.expectCfg.MaxConcurrentColdFills)
			}
		})
	}
}
