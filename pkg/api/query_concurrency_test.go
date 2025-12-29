package api

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestQueryConcurrencyLimit(t *testing.T) {
	t.Run("queries queue when concurrency exceeds 16", func(t *testing.T) {
		cfg := &config.Config{AuthToken: testAuthToken}
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)

		ctx := context.Background()
		stateMan.Create(ctx, "test-ns")

		// Create handler with default limit of 16 (nil tail store is fine for this test)
		queryHandler := query.NewHandler(store, stateMan, nil)

		router := NewRouter(cfg)
		router.setQueryHandler(queryHandler)
		router.SetState(&ServerState{
			Namespaces: map[string]*NamespaceState{
				"test-ns": {Exists: true},
			},
			ObjectStore: ObjectStoreState{Available: true},
		})

		// Verify the limiter has the default limit of 16
		if queryHandler.Limiter().Limit() != 16 {
			t.Errorf("expected default limit 16, got %d", queryHandler.Limiter().Limit())
		}

		// Test that concurrent queries respect the limit
		var wg sync.WaitGroup
		var maxConcurrent atomic.Int32
		var current atomic.Int32
		numQueries := 25

		for i := 0; i < numQueries; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Track concurrent execution within the limiter
				release, err := queryHandler.Limiter().Acquire(ctx, "test-ns")
				if err != nil {
					return
				}

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

		// Verify concurrency was capped at 16
		if maxConcurrent.Load() > 16 {
			t.Errorf("max concurrent %d exceeded limit 16", maxConcurrent.Load())
		}
	})

	t.Run("verify queued queries eventually execute", func(t *testing.T) {
		cfg := &config.Config{AuthToken: testAuthToken}
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)

		ctx := context.Background()
		stateMan.Create(ctx, "test-ns")

		// Use a handler with a small limit for easier testing
		queryHandler := query.NewHandlerWithLimit(store, stateMan, nil, 2)

		router := NewRouter(cfg)
		router.setQueryHandler(queryHandler)
		router.SetState(&ServerState{
			Namespaces: map[string]*NamespaceState{
				"test-ns": {Exists: true},
			},
			ObjectStore: ObjectStoreState{Available: true},
		})

		// Fill up the slots
		release1, _ := queryHandler.Limiter().Acquire(ctx, "test-ns")
		release2, _ := queryHandler.Limiter().Acquire(ctx, "test-ns")

		// Verify slots are full
		if queryHandler.Limiter().ActiveCount("test-ns") != 2 {
			t.Errorf("expected 2 active, got %d", queryHandler.Limiter().ActiveCount("test-ns"))
		}

		// Start a goroutine that will try to acquire
		var acquired atomic.Bool
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := queryHandler.Limiter().Acquire(ctx, "test-ns")
			if err == nil {
				acquired.Store(true)
				release()
			}
		}()

		// Give it a moment to start waiting
		time.Sleep(20 * time.Millisecond)
		if acquired.Load() {
			t.Error("query should be queued, not yet executing")
		}

		// Release one slot
		release1()

		// Wait for the queued query to execute
		wg.Wait()
		if !acquired.Load() {
			t.Error("queued query should have executed after slot was released")
		}

		release2()
	})

	t.Run("default concurrency limit is 16", func(t *testing.T) {
		cfg := &config.Config{AuthToken: testAuthToken}
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)

		ctx := context.Background()
		stateMan.Create(ctx, "test-ns")

		// Create handler with default limit
		queryHandler := query.NewHandler(store, stateMan, nil)

		router := NewRouter(cfg)
		router.setQueryHandler(queryHandler)

		// Verify the default limit is 16 per spec
		if queryHandler.Limiter().Limit() != 16 {
			t.Errorf("default limit should be 16, got %d", queryHandler.Limiter().Limit())
		}

		// Acquire 16 slots
		var releases []func()
		for i := 0; i < 16; i++ {
			release, err := queryHandler.Limiter().Acquire(ctx, "test-ns")
			if err != nil {
				t.Fatalf("failed to acquire slot %d: %v", i+1, err)
			}
			releases = append(releases, release)
		}

		// 17th should block
		ctx17, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		_, err := queryHandler.Limiter().Acquire(ctx17, "test-ns")
		if err != context.DeadlineExceeded {
			t.Errorf("17th query should block when 16 are active, got %v", err)
		}

		// Clean up
		for _, r := range releases {
			r()
		}
	})
}

// setQueryHandler allows tests to set a custom query handler.
func (r *Router) setQueryHandler(h *query.Handler) {
	r.queryHandler = h
}
