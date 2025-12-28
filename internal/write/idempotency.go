package write

import (
	"context"
	"sync"
	"time"
)

const (
	// DefaultIdempotencyTTL is the default time-to-live for idempotency records.
	// Duplicate requests within this window return the original response.
	DefaultIdempotencyTTL = 5 * time.Minute

	// DefaultIdempotencyCleanupInterval is the interval for cleaning up expired entries.
	DefaultIdempotencyCleanupInterval = 1 * time.Minute
)

// IdempotencyEntry stores a cached response for a processed request.
type IdempotencyEntry struct {
	Response  *WriteResponse
	ExpiresAt time.Time
}

// inFlightEntry tracks a request that is currently being processed.
// Other concurrent requests with the same request_id will wait on the done channel.
type inFlightEntry struct {
	done chan struct{}  // closed when processing completes
	resp *WriteResponse // set before closing done
	err  error          // set before closing done (if failed)
}

// IdempotencyStore tracks processed request_ids per namespace to enable
// duplicate request detection and response caching. It also tracks in-flight
// requests to prevent concurrent duplicates from being processed twice.
type IdempotencyStore struct {
	mu       sync.Mutex
	entries  map[string]map[string]*IdempotencyEntry // namespace -> request_id -> entry
	inFlight map[string]map[string]*inFlightEntry   // namespace -> request_id -> in-flight entry
	ttl      time.Duration
	stopCh   chan struct{}
	stopped  bool
}

// NewIdempotencyStore creates a new idempotency store with the default TTL.
func NewIdempotencyStore() *IdempotencyStore {
	return NewIdempotencyStoreWithTTL(DefaultIdempotencyTTL)
}

// NewIdempotencyStoreWithTTL creates a new idempotency store with a custom TTL.
func NewIdempotencyStoreWithTTL(ttl time.Duration) *IdempotencyStore {
	s := &IdempotencyStore{
		entries:  make(map[string]map[string]*IdempotencyEntry),
		inFlight: make(map[string]map[string]*inFlightEntry),
		ttl:      ttl,
		stopCh:   make(chan struct{}),
	}

	go s.cleanupLoop()

	return s
}

// Close stops the cleanup goroutine and releases resources.
func (s *IdempotencyStore) Close() error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		close(s.stopCh)
	}
	s.mu.Unlock()
	return nil
}

// ReserveResult indicates the outcome of a Reserve call.
type ReserveResult int

const (
	// ReserveOK means the request_id was successfully reserved for processing.
	ReserveOK ReserveResult = iota
	// ReserveCached means a cached response exists and should be returned.
	ReserveCached
	// ReserveInFlight means another request with the same request_id is in progress.
	ReserveInFlight
)

// Reserve attempts to reserve a request_id for processing.
// Returns:
//   - ReserveOK, nil, nil: The request_id is reserved, proceed with processing
//   - ReserveCached, response, nil: A cached response exists, return it
//   - ReserveInFlight, nil, inflight: Another request is in-flight, use WaitForInFlight
//
// If ReserveOK is returned, the caller MUST call Complete or Release when done.
func (s *IdempotencyStore) Reserve(namespace, requestID string) (ReserveResult, *WriteResponse, *inFlightEntry) {
	if requestID == "" {
		return ReserveOK, nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check for cached response first
	if nsEntries, ok := s.entries[namespace]; ok {
		if entry, ok := nsEntries[requestID]; ok {
			if time.Now().Before(entry.ExpiresAt) {
				return ReserveCached, entry.Response, nil
			}
			// Expired, remove it
			delete(nsEntries, requestID)
		}
	}

	// Check for in-flight request
	if nsInFlight, ok := s.inFlight[namespace]; ok {
		if inflight, ok := nsInFlight[requestID]; ok {
			return ReserveInFlight, nil, inflight
		}
	}

	// Reserve this request_id
	nsInFlight, ok := s.inFlight[namespace]
	if !ok {
		nsInFlight = make(map[string]*inFlightEntry)
		s.inFlight[namespace] = nsInFlight
	}
	nsInFlight[requestID] = &inFlightEntry{
		done: make(chan struct{}),
	}

	return ReserveOK, nil, nil
}

// Complete marks a reserved request_id as complete and caches the response.
// This must be called after Reserve returns ReserveOK and processing succeeds.
func (s *IdempotencyStore) Complete(namespace, requestID string, response *WriteResponse) {
	if requestID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get and remove the in-flight entry
	var inflight *inFlightEntry
	if nsInFlight, ok := s.inFlight[namespace]; ok {
		inflight = nsInFlight[requestID]
		delete(nsInFlight, requestID)
		if len(nsInFlight) == 0 {
			delete(s.inFlight, namespace)
		}
	}

	// Cache the response
	nsEntries, ok := s.entries[namespace]
	if !ok {
		nsEntries = make(map[string]*IdempotencyEntry)
		s.entries[namespace] = nsEntries
	}
	nsEntries[requestID] = &IdempotencyEntry{
		Response:  response,
		ExpiresAt: time.Now().Add(s.ttl),
	}

	// Signal waiters - set resp before closing so waiters can read it
	if inflight != nil {
		inflight.resp = response
		close(inflight.done)
	}
}

// Release removes a reserved request_id without caching a response.
// This must be called after Reserve returns ReserveOK and processing fails.
func (s *IdempotencyStore) Release(namespace, requestID string, err error) {
	if requestID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get and remove the in-flight entry
	var inflight *inFlightEntry
	if nsInFlight, ok := s.inFlight[namespace]; ok {
		inflight = nsInFlight[requestID]
		delete(nsInFlight, requestID)
		if len(nsInFlight) == 0 {
			delete(s.inFlight, namespace)
		}
	}

	// Signal waiters with error - set err before closing so waiters can read it
	if inflight != nil {
		inflight.err = err
		close(inflight.done)
	}
}

// WaitForInFlight waits for an in-flight request to complete and returns its result.
// The inflight entry should be obtained from a Reserve call that returned ReserveInFlight.
// Respects context cancellation and returns ctx.Err() if the context is cancelled.
func (s *IdempotencyStore) WaitForInFlight(ctx context.Context, inflight *inFlightEntry) (*WriteResponse, error) {
	if inflight == nil {
		return nil, nil
	}

	// Wait for the in-flight request to complete or context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-inflight.done:
		// The in-flight entry has resp/err set before done is closed
		if inflight.resp != nil {
			return inflight.resp, nil
		}
		// Return error if set, otherwise nil (caller should retry)
		return nil, inflight.err
	}
}

// Get retrieves a cached response for a request_id in a namespace.
// Returns nil if no entry exists or the entry has expired.
// Deprecated: Use Reserve/Complete/Release for proper concurrent handling.
func (s *IdempotencyStore) Get(namespace, requestID string) *WriteResponse {
	if requestID == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	nsEntries, ok := s.entries[namespace]
	if !ok {
		return nil
	}

	entry, ok := nsEntries[requestID]
	if !ok {
		return nil
	}

	if time.Now().After(entry.ExpiresAt) {
		return nil
	}

	return entry.Response
}

// Put stores a response for a request_id in a namespace.
// Deprecated: Use Complete after Reserve for proper concurrent handling.
func (s *IdempotencyStore) Put(namespace, requestID string, response *WriteResponse) {
	if requestID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	nsEntries, ok := s.entries[namespace]
	if !ok {
		nsEntries = make(map[string]*IdempotencyEntry)
		s.entries[namespace] = nsEntries
	}

	nsEntries[requestID] = &IdempotencyEntry{
		Response:  response,
		ExpiresAt: time.Now().Add(s.ttl),
	}
}

// cleanupLoop periodically removes expired entries.
func (s *IdempotencyStore) cleanupLoop() {
	ticker := time.NewTicker(DefaultIdempotencyCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.cleanup()
		}
	}
}

// cleanup removes all expired entries.
func (s *IdempotencyStore) cleanup() {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for ns, nsEntries := range s.entries {
		for reqID, entry := range nsEntries {
			if now.After(entry.ExpiresAt) {
				delete(nsEntries, reqID)
			}
		}
		if len(nsEntries) == 0 {
			delete(s.entries, ns)
		}
	}
}
