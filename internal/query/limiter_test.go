package query

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewConcurrencyLimiter(t *testing.T) {
	t.Run("default limit", func(t *testing.T) {
		l := NewConcurrencyLimiter(0)
		if l.Limit() != DefaultConcurrencyLimit {
			t.Errorf("expected default limit %d, got %d", DefaultConcurrencyLimit, l.Limit())
		}
	})

	t.Run("custom limit", func(t *testing.T) {
		l := NewConcurrencyLimiter(8)
		if l.Limit() != 8 {
			t.Errorf("expected limit 8, got %d", l.Limit())
		}
	})

	t.Run("negative limit uses default", func(t *testing.T) {
		l := NewConcurrencyLimiter(-5)
		if l.Limit() != DefaultConcurrencyLimit {
			t.Errorf("expected default limit %d, got %d", DefaultConcurrencyLimit, l.Limit())
		}
	})
}

func TestConcurrencyLimiter_Acquire(t *testing.T) {
	t.Run("acquire and release", func(t *testing.T) {
		l := NewConcurrencyLimiter(2)
		ctx := context.Background()

		release1, err := l.Acquire(ctx, "ns1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if l.ActiveCount("ns1") != 1 {
			t.Errorf("expected 1 active, got %d", l.ActiveCount("ns1"))
		}

		release2, err := l.Acquire(ctx, "ns1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if l.ActiveCount("ns1") != 2 {
			t.Errorf("expected 2 active, got %d", l.ActiveCount("ns1"))
		}

		release1()
		if l.ActiveCount("ns1") != 1 {
			t.Errorf("expected 1 active after release, got %d", l.ActiveCount("ns1"))
		}

		release2()
		if l.ActiveCount("ns1") != 0 {
			t.Errorf("expected 0 active after release, got %d", l.ActiveCount("ns1"))
		}
	})

	t.Run("blocks when limit reached", func(t *testing.T) {
		l := NewConcurrencyLimiter(2)
		ctx := context.Background()

		release1, _ := l.Acquire(ctx, "ns1")
		release2, _ := l.Acquire(ctx, "ns1")
		defer release1()
		defer release2()

		// Third acquire should block
		ctx3, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		_, err := l.Acquire(ctx3, "ns1")
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded, got %v", err)
		}
	})

	t.Run("queued queries eventually execute", func(t *testing.T) {
		l := NewConcurrencyLimiter(2)
		ctx := context.Background()

		// Fill up the slots
		release1, _ := l.Acquire(ctx, "ns1")
		release2, _ := l.Acquire(ctx, "ns1")

		// Start a goroutine that will try to acquire
		var acquired atomic.Bool
		go func() {
			release, err := l.Acquire(ctx, "ns1")
			if err == nil {
				acquired.Store(true)
				release()
			}
		}()

		// Give it a moment to start waiting
		time.Sleep(10 * time.Millisecond)
		if acquired.Load() {
			t.Error("should not have acquired yet")
		}

		// Release one slot
		release1()

		// Wait for the queued request to acquire
		time.Sleep(50 * time.Millisecond)
		if !acquired.Load() {
			t.Error("queued query should have executed")
		}

		release2()
	})

	t.Run("different namespaces are independent", func(t *testing.T) {
		l := NewConcurrencyLimiter(2)
		ctx := context.Background()

		// Fill up ns1
		release1, _ := l.Acquire(ctx, "ns1")
		release2, _ := l.Acquire(ctx, "ns1")
		defer release1()
		defer release2()

		// ns2 should still be available
		release3, err := l.Acquire(ctx, "ns2")
		if err != nil {
			t.Errorf("ns2 should be available: %v", err)
		}
		release3()

		if l.ActiveCount("ns1") != 2 {
			t.Errorf("expected 2 active for ns1, got %d", l.ActiveCount("ns1"))
		}
		if l.ActiveCount("ns2") != 0 {
			t.Errorf("expected 0 active for ns2, got %d", l.ActiveCount("ns2"))
		}
	})
}

func TestConcurrencyLimiter_TryAcquire(t *testing.T) {
	t.Run("succeeds when slots available", func(t *testing.T) {
		l := NewConcurrencyLimiter(2)

		release, ok := l.TryAcquire("ns1")
		if !ok {
			t.Error("expected TryAcquire to succeed")
		}
		release()
	})

	t.Run("fails when limit reached", func(t *testing.T) {
		l := NewConcurrencyLimiter(2)
		ctx := context.Background()

		release1, _ := l.Acquire(ctx, "ns1")
		release2, _ := l.Acquire(ctx, "ns1")
		defer release1()
		defer release2()

		_, ok := l.TryAcquire("ns1")
		if ok {
			t.Error("expected TryAcquire to fail when limit reached")
		}
	})
}

func TestConcurrencyLimiter_DefaultLimit(t *testing.T) {
	// Verify the default limit is 16 as per spec
	if DefaultConcurrencyLimit != 16 {
		t.Errorf("DefaultConcurrencyLimit should be 16, got %d", DefaultConcurrencyLimit)
	}

	l := NewConcurrencyLimiter(0)
	if l.Limit() != 16 {
		t.Errorf("expected limit 16, got %d", l.Limit())
	}
}

func TestConcurrencyLimiter_ConcurrentAccess(t *testing.T) {
	l := NewConcurrencyLimiter(16)
	ctx := context.Background()

	var wg sync.WaitGroup
	var maxConcurrent atomic.Int32
	var current atomic.Int32

	// Launch 50 goroutines that each try to do 10 queries
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				release, err := l.Acquire(ctx, "ns1")
				if err != nil {
					continue
				}

				// Track current concurrency
				cur := current.Add(1)
				for {
					max := maxConcurrent.Load()
					if cur <= max || maxConcurrent.CompareAndSwap(max, cur) {
						break
					}
				}

				time.Sleep(time.Microsecond) // Simulate work
				current.Add(-1)
				release()
			}
		}()
	}

	wg.Wait()

	// Verify max concurrency never exceeded limit
	if maxConcurrent.Load() > 16 {
		t.Errorf("max concurrent %d exceeded limit 16", maxConcurrent.Load())
	}
}
