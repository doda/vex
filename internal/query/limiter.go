// Package query implements the query execution engine for Vex.
package query

import (
	"context"
	"sync"
)

// DefaultConcurrencyLimit is the maximum concurrent queries per namespace.
const DefaultConcurrencyLimit = 16

// ConcurrencyLimiter limits concurrent query execution per namespace.
// When the limit is reached, additional queries wait until a slot is available.
type ConcurrencyLimiter struct {
	mu       sync.Mutex
	limit    int
	limiters map[string]*namespaceLimiter
}

// namespaceLimiter is a semaphore for a single namespace.
type namespaceLimiter struct {
	ch chan struct{}
}

// NewConcurrencyLimiter creates a new ConcurrencyLimiter.
func NewConcurrencyLimiter(limit int) *ConcurrencyLimiter {
	if limit <= 0 {
		limit = DefaultConcurrencyLimit
	}
	return &ConcurrencyLimiter{
		limit:    limit,
		limiters: make(map[string]*namespaceLimiter),
	}
}

// Acquire blocks until a query slot is available for the given namespace,
// or until the context is cancelled. Returns a release function that must
// be called when the query completes.
func (l *ConcurrencyLimiter) Acquire(ctx context.Context, namespace string) (release func(), err error) {
	nl := l.getLimiter(namespace)

	select {
	case nl.ch <- struct{}{}:
		return func() { <-nl.ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TryAcquire attempts to acquire a query slot without blocking.
// Returns true and a release function if successful, false otherwise.
func (l *ConcurrencyLimiter) TryAcquire(namespace string) (release func(), ok bool) {
	nl := l.getLimiter(namespace)

	select {
	case nl.ch <- struct{}{}:
		return func() { <-nl.ch }, true
	default:
		return nil, false
	}
}

// ActiveCount returns the number of active queries for a namespace.
func (l *ConcurrencyLimiter) ActiveCount(namespace string) int {
	l.mu.Lock()
	nl, exists := l.limiters[namespace]
	l.mu.Unlock()

	if !exists {
		return 0
	}
	return len(nl.ch)
}

// Limit returns the concurrency limit per namespace.
func (l *ConcurrencyLimiter) Limit() int {
	return l.limit
}

// getLimiter returns the limiter for a namespace, creating it if needed.
func (l *ConcurrencyLimiter) getLimiter(namespace string) *namespaceLimiter {
	l.mu.Lock()
	defer l.mu.Unlock()

	nl, exists := l.limiters[namespace]
	if !exists {
		nl = &namespaceLimiter{
			ch: make(chan struct{}, l.limit),
		}
		l.limiters[namespace] = nl
	}
	return nl
}

// QueuedCount returns the number of queries waiting for a slot.
// This is approximate and mainly useful for testing/metrics.
func (l *ConcurrencyLimiter) QueuedCount(namespace string) int {
	// We can't easily track queued queries with a channel-based semaphore.
	// For proper queue counting, we'd need a different implementation.
	// For now, return 0 as we don't have an explicit queue.
	return 0
}
