package query

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestHandler_ConcurrencyLimit(t *testing.T) {
	t.Run("queries queue when concurrency exceeds limit", func(t *testing.T) {
		limit := 2
		storeBackend := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(storeBackend)

		// Create namespace state first
		ctx := context.Background()
		stateMan.Create(ctx, "test-ns")

		// Create handler with a low limit for testing
		handler := NewHandlerWithLimit(storeBackend, stateMan, nil, limit)

		// Verify the limit is set correctly
		if handler.Limiter().Limit() != limit {
			t.Errorf("expected limit %d, got %d", limit, handler.Limiter().Limit())
		}

		// Acquire all slots directly via limiter
		release1, _ := handler.Limiter().Acquire(ctx, "test-ns")
		release2, _ := handler.Limiter().Acquire(ctx, "test-ns")
		defer release1()
		defer release2()

		// Verify both slots are taken
		if handler.Limiter().ActiveCount("test-ns") != 2 {
			t.Errorf("expected 2 active, got %d", handler.Limiter().ActiveCount("test-ns"))
		}

		// Third query should block/timeout
		ctx3, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		_, err := handler.Limiter().Acquire(ctx3, "test-ns")
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded when limit exceeded, got %v", err)
		}
	})

	t.Run("queued queries eventually execute", func(t *testing.T) {
		limit := 2
		storeBackend := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(storeBackend)

		// Create namespace state first
		ctx := context.Background()
		stateMan.Create(ctx, "test-ns")

		handler := NewHandlerWithLimit(storeBackend, stateMan, nil, limit)

		// Fill up the slots
		release1, _ := handler.Limiter().Acquire(ctx, "test-ns")
		release2, _ := handler.Limiter().Acquire(ctx, "test-ns")

		// Start a goroutine that will try to acquire
		var acquired atomic.Bool
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := handler.Limiter().Acquire(ctx, "test-ns")
			if err == nil {
				acquired.Store(true)
				release()
			}
		}()

		// Give it a moment to start waiting
		time.Sleep(20 * time.Millisecond)
		if acquired.Load() {
			t.Error("should not have acquired yet while slots are full")
		}

		// Release one slot
		release1()

		// Wait for the queued request to acquire
		wg.Wait()
		if !acquired.Load() {
			t.Error("queued query should have executed after slot was released")
		}

		release2()
	})

	t.Run("default limit is 16", func(t *testing.T) {
		storeBackend := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(storeBackend)

		// Create with default limit
		handler := NewHandler(storeBackend, stateMan, nil)

		if handler.Limiter().Limit() != DefaultConcurrencyLimit {
			t.Errorf("expected default limit %d, got %d", DefaultConcurrencyLimit, handler.Limiter().Limit())
		}
		if handler.Limiter().Limit() != 16 {
			t.Errorf("default limit should be 16, got %d", handler.Limiter().Limit())
		}
	})

	t.Run("different namespaces are independent", func(t *testing.T) {
		limit := 2
		storeBackend := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(storeBackend)

		ctx := context.Background()
		stateMan.Create(ctx, "ns1")
		stateMan.Create(ctx, "ns2")

		handler := NewHandlerWithLimit(storeBackend, stateMan, nil, limit)

		// Fill up ns1
		release1, _ := handler.Limiter().Acquire(ctx, "ns1")
		release2, _ := handler.Limiter().Acquire(ctx, "ns1")
		defer release1()
		defer release2()

		// ns2 should still be available
		release3, err := handler.Limiter().Acquire(ctx, "ns2")
		if err != nil {
			t.Errorf("ns2 should be available: %v", err)
		}
		if release3 != nil {
			release3()
		}

		if handler.Limiter().ActiveCount("ns1") != 2 {
			t.Errorf("expected 2 active for ns1, got %d", handler.Limiter().ActiveCount("ns1"))
		}
		if handler.Limiter().ActiveCount("ns2") != 0 {
			t.Errorf("expected 0 active for ns2, got %d", handler.Limiter().ActiveCount("ns2"))
		}
	})

	t.Run("context cancellation releases slot", func(t *testing.T) {
		limit := 2
		storeBackend := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(storeBackend)

		ctx := context.Background()
		stateMan.Create(ctx, "test-ns")

		handler := NewHandlerWithLimit(storeBackend, stateMan, nil, limit)

		// Fill up all slots
		release1, _ := handler.Limiter().Acquire(ctx, "test-ns")
		release2, _ := handler.Limiter().Acquire(ctx, "test-ns")

		// Start a query with a cancelled context
		ctx3, cancel := context.WithCancel(ctx)

		var queryErr error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, queryErr = handler.Limiter().Acquire(ctx3, "test-ns")
		}()

		// Give it a moment to start waiting
		time.Sleep(10 * time.Millisecond)

		// Cancel the context
		cancel()

		wg.Wait()
		if queryErr != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", queryErr)
		}

		// Slots should still be occupied by the original queries
		if handler.Limiter().ActiveCount("test-ns") != 2 {
			t.Errorf("expected 2 active, got %d", handler.Limiter().ActiveCount("test-ns"))
		}

		release1()
		release2()
	})
}

func TestHandler_QueryWithConcurrency(t *testing.T) {
	t.Run("concurrent queries respect limit", func(t *testing.T) {
		limit := 4
		storeBackend := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(storeBackend)

		ctx := context.Background()
		stateMan.Create(ctx, "test-ns")

		handler := NewHandlerWithLimit(storeBackend, stateMan, nil, limit)

		var wg sync.WaitGroup
		var maxConcurrent atomic.Int32
		var current atomic.Int32
		numQueries := 20

		for i := 0; i < numQueries; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				release, err := handler.Limiter().Acquire(ctx, "test-ns")
				if err != nil {
					return
				}

				// Track concurrency
				cur := current.Add(1)
				for {
					max := maxConcurrent.Load()
					if cur <= max || maxConcurrent.CompareAndSwap(max, cur) {
						break
					}
				}

				// Simulate query work
				time.Sleep(5 * time.Millisecond)

				current.Add(-1)
				release()
			}()
		}

		wg.Wait()

		if maxConcurrent.Load() > int32(limit) {
			t.Errorf("max concurrent %d exceeded limit %d", maxConcurrent.Load(), limit)
		}
		if handler.Limiter().ActiveCount("test-ns") != 0 {
			t.Errorf("expected 0 active after completion, got %d", handler.Limiter().ActiveCount("test-ns"))
		}
	})
}
