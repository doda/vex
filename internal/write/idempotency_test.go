package write

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestIdempotencyStore_GetPut(t *testing.T) {
	store := NewIdempotencyStoreWithTTL(time.Minute)
	defer store.Close()

	ns := "test-ns"
	reqID := uuid.New().String()

	// Initially should return nil
	resp := store.Get(ns, reqID)
	if resp != nil {
		t.Errorf("expected nil for missing entry, got %+v", resp)
	}

	// Store a response
	expected := &WriteResponse{
		RowsAffected: 5,
		RowsUpserted: 3,
		RowsPatched:  1,
		RowsDeleted:  1,
	}
	store.Put(ns, reqID, expected)

	// Should now return the cached response
	resp = store.Get(ns, reqID)
	if resp == nil {
		t.Fatal("expected cached response, got nil")
	}
	if resp.RowsAffected != expected.RowsAffected {
		t.Errorf("expected RowsAffected=%d, got %d", expected.RowsAffected, resp.RowsAffected)
	}
	if resp.RowsUpserted != expected.RowsUpserted {
		t.Errorf("expected RowsUpserted=%d, got %d", expected.RowsUpserted, resp.RowsUpserted)
	}
}

func TestIdempotencyStore_EmptyRequestID(t *testing.T) {
	store := NewIdempotencyStoreWithTTL(time.Minute)
	defer store.Close()

	ns := "test-ns"

	// Empty request ID should return nil
	resp := store.Get(ns, "")
	if resp != nil {
		t.Errorf("expected nil for empty request ID, got %+v", resp)
	}

	// Put with empty request ID should be no-op
	store.Put(ns, "", &WriteResponse{RowsAffected: 1})
	resp = store.Get(ns, "")
	if resp != nil {
		t.Errorf("expected nil after Put with empty request ID, got %+v", resp)
	}
}

func TestIdempotencyStore_NamespaceIsolation(t *testing.T) {
	store := NewIdempotencyStoreWithTTL(time.Minute)
	defer store.Close()

	reqID := uuid.New().String()
	ns1 := "namespace-1"
	ns2 := "namespace-2"

	resp1 := &WriteResponse{RowsAffected: 1}
	resp2 := &WriteResponse{RowsAffected: 2}

	store.Put(ns1, reqID, resp1)
	store.Put(ns2, reqID, resp2)

	// Should return correct response per namespace
	got1 := store.Get(ns1, reqID)
	if got1 == nil || got1.RowsAffected != 1 {
		t.Errorf("expected RowsAffected=1 for ns1, got %+v", got1)
	}

	got2 := store.Get(ns2, reqID)
	if got2 == nil || got2.RowsAffected != 2 {
		t.Errorf("expected RowsAffected=2 for ns2, got %+v", got2)
	}
}

func TestIdempotencyStore_Expiration(t *testing.T) {
	// Use very short TTL for testing
	store := NewIdempotencyStoreWithTTL(50 * time.Millisecond)
	defer store.Close()

	ns := "test-ns"
	reqID := uuid.New().String()

	store.Put(ns, reqID, &WriteResponse{RowsAffected: 1})

	// Should be present immediately
	resp := store.Get(ns, reqID)
	if resp == nil {
		t.Fatal("expected cached response immediately after put, got nil")
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should be expired
	resp = store.Get(ns, reqID)
	if resp != nil {
		t.Errorf("expected nil after expiration, got %+v", resp)
	}
}

func TestHandlerIdempotency(t *testing.T) {
	memStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(memStore)

	handler, err := NewHandler(memStore, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ctx := context.Background()
	ns := "test-namespace"
	reqID := uuid.New().String()

	// First write
	req := &WriteRequest{
		RequestID: reqID,
		UpsertRows: []map[string]any{
			{"id": uint64(1), "name": "doc1"},
			{"id": uint64(2), "name": "doc2"},
		},
	}

	resp1, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if resp1.RowsUpserted != 2 {
		t.Errorf("expected 2 rows upserted, got %d", resp1.RowsUpserted)
	}

	// Second write with same request_id should return cached response
	resp2, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("duplicate write failed: %v", err)
	}
	if resp2.RowsUpserted != resp1.RowsUpserted {
		t.Errorf("duplicate response mismatch: expected %d, got %d", resp1.RowsUpserted, resp2.RowsUpserted)
	}
	if resp2.RowsAffected != resp1.RowsAffected {
		t.Errorf("duplicate response mismatch: expected %d, got %d", resp1.RowsAffected, resp2.RowsAffected)
	}
}

func TestHandlerIdempotency_DifferentRequestID(t *testing.T) {
	memStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(memStore)

	handler, err := NewHandler(memStore, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ctx := context.Background()
	ns := "test-namespace"

	// First write
	req1 := &WriteRequest{
		RequestID: uuid.New().String(),
		UpsertRows: []map[string]any{
			{"id": uint64(1), "name": "doc1"},
		},
	}

	resp1, err := handler.Handle(ctx, ns, req1)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if resp1.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp1.RowsUpserted)
	}

	// Second write with DIFFERENT request_id should be processed
	req2 := &WriteRequest{
		RequestID: uuid.New().String(),
		UpsertRows: []map[string]any{
			{"id": uint64(2), "name": "doc2"},
		},
	}

	resp2, err := handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	if resp2.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted for new request, got %d", resp2.RowsUpserted)
	}
}

func TestBatcherIdempotency(t *testing.T) {
	memStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(memStore)

	cfg := BatcherConfig{
		BatchWindow:  100 * time.Millisecond,
		MaxBatchSize: DefaultMaxBatchSize,
	}
	batcher, err := NewBatcherWithConfig(memStore, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ctx := context.Background()
	ns := "test-namespace"
	reqID := uuid.New().String()

	// First write
	req := &WriteRequest{
		RequestID: reqID,
		UpsertRows: []map[string]any{
			{"id": uint64(1), "name": "doc1"},
		},
	}

	resp1, err := batcher.Submit(ctx, ns, req)
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if resp1.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp1.RowsUpserted)
	}

	// Second write with same request_id should return cached response
	resp2, err := batcher.Submit(ctx, ns, req)
	if err != nil {
		t.Fatalf("duplicate write failed: %v", err)
	}
	if resp2.RowsUpserted != resp1.RowsUpserted {
		t.Errorf("duplicate response mismatch: expected %d, got %d", resp1.RowsUpserted, resp2.RowsUpserted)
	}
}

func TestParseWriteRequest_RequestIDFromBody(t *testing.T) {
	defaultID := "default-uuid"
	bodyID := "body-uuid"

	// Test request_id from body takes precedence
	body := map[string]any{
		"request_id": bodyID,
		"upsert_rows": []any{
			map[string]any{"id": uint64(1), "name": "test"},
		},
	}

	req, err := ParseWriteRequest(defaultID, body)
	if err != nil {
		t.Fatalf("ParseWriteRequest failed: %v", err)
	}
	if req.RequestID != bodyID {
		t.Errorf("expected RequestID=%q, got %q", bodyID, req.RequestID)
	}
}

func TestParseWriteRequest_RequestIDFromParameter(t *testing.T) {
	defaultID := "default-uuid"

	// Test request_id from parameter is used when not in body
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": uint64(1), "name": "test"},
		},
	}

	req, err := ParseWriteRequest(defaultID, body)
	if err != nil {
		t.Fatalf("ParseWriteRequest failed: %v", err)
	}
	if req.RequestID != defaultID {
		t.Errorf("expected RequestID=%q, got %q", defaultID, req.RequestID)
	}
}

func TestParseWriteRequest_EmptyRequestIDInBody(t *testing.T) {
	defaultID := "default-uuid"

	// Test empty request_id in body uses parameter
	body := map[string]any{
		"request_id": "",
		"upsert_rows": []any{
			map[string]any{"id": uint64(1), "name": "test"},
		},
	}

	req, err := ParseWriteRequest(defaultID, body)
	if err != nil {
		t.Fatalf("ParseWriteRequest failed: %v", err)
	}
	if req.RequestID != defaultID {
		t.Errorf("expected RequestID=%q (fallback to default), got %q", defaultID, req.RequestID)
	}
}

func TestIdempotencyStore_Reserve_InFlight(t *testing.T) {
	store := NewIdempotencyStoreWithTTL(time.Minute)
	defer store.Close()

	ns := "test-ns"
	reqID := uuid.New().String()

	// First reserve should succeed
	result1, cached1, inflight1 := store.Reserve(ns, reqID)
	if result1 != ReserveOK {
		t.Fatalf("expected ReserveOK, got %v", result1)
	}
	if cached1 != nil {
		t.Errorf("expected nil cached response, got %+v", cached1)
	}
	if inflight1 != nil {
		t.Errorf("expected nil inflight, got %v", inflight1)
	}

	// Second reserve for same request_id should return in-flight
	result2, cached2, inflight2 := store.Reserve(ns, reqID)
	if result2 != ReserveInFlight {
		t.Fatalf("expected ReserveInFlight, got %v", result2)
	}
	if cached2 != nil {
		t.Errorf("expected nil cached response, got %+v", cached2)
	}
	if inflight2 == nil {
		t.Fatal("expected non-nil inflight for in-flight request")
	}

	// Complete the first request
	expectedResp := &WriteResponse{RowsAffected: 5}
	store.Complete(ns, reqID, expectedResp)

	// The wait channel should be closed now
	select {
	case <-inflight2.done:
		// Good - channel closed
	default:
		t.Fatal("done channel should be closed after Complete")
	}

	// WaitForInFlight should return the response
	ctx := context.Background()
	resp, err := store.WaitForInFlight(ctx, inflight2)
	if err != nil {
		t.Fatalf("WaitForInFlight returned error: %v", err)
	}
	if resp == nil || resp.RowsAffected != 5 {
		t.Errorf("expected response with RowsAffected=5, got %+v", resp)
	}

	// Now Reserve should return cached
	result3, cached3, _ := store.Reserve(ns, reqID)
	if result3 != ReserveCached {
		t.Fatalf("expected ReserveCached, got %v", result3)
	}
	if cached3 == nil || cached3.RowsAffected != 5 {
		t.Errorf("expected cached response with RowsAffected=5, got %+v", cached3)
	}
}

func TestIdempotencyStore_Reserve_Release(t *testing.T) {
	store := NewIdempotencyStoreWithTTL(time.Minute)
	defer store.Close()

	ns := "test-ns"
	reqID := uuid.New().String()

	// Reserve
	result1, _, _ := store.Reserve(ns, reqID)
	if result1 != ReserveOK {
		t.Fatalf("expected ReserveOK, got %v", result1)
	}

	// Second reserve should be in-flight
	result2, _, inflight := store.Reserve(ns, reqID)
	if result2 != ReserveInFlight {
		t.Fatalf("expected ReserveInFlight, got %v", result2)
	}

	// Release (failure case)
	store.Release(ns, reqID, nil)

	// Wait channel should be closed
	select {
	case <-inflight.done:
		// Good
	default:
		t.Fatal("done channel should be closed after Release")
	}

	// WaitForInFlight should return nil (original failed with no error)
	ctx := context.Background()
	resp, err := store.WaitForInFlight(ctx, inflight)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if resp != nil {
		t.Errorf("expected nil response after release, got %+v", resp)
	}

	// Now Reserve should succeed again (since no response was cached)
	result3, cached3, _ := store.Reserve(ns, reqID)
	if result3 != ReserveOK {
		t.Fatalf("expected ReserveOK after release, got %v", result3)
	}
	if cached3 != nil {
		t.Errorf("expected nil cached response after release, got %+v", cached3)
	}
}

func TestHandlerIdempotency_ConcurrentDuplicates(t *testing.T) {
	memStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(memStore)

	handler, err := NewHandler(memStore, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	ctx := context.Background()
	ns := "test-namespace"
	reqID := uuid.New().String()

	req := &WriteRequest{
		RequestID: reqID,
		UpsertRows: []map[string]any{
			{"id": uint64(1), "name": "doc1"},
		},
	}

	// Launch multiple concurrent requests with the same request_id
	const numConcurrent = 10
	var wg sync.WaitGroup
	responses := make([]*WriteResponse, numConcurrent)
	errors := make([]error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := handler.Handle(ctx, ns, req)
			responses[idx] = resp
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// All requests should succeed with the same response
	var firstResp *WriteResponse
	for i := 0; i < numConcurrent; i++ {
		if errors[i] != nil {
			t.Errorf("request %d failed: %v", i, errors[i])
			continue
		}
		if responses[i] == nil {
			t.Errorf("request %d returned nil response", i)
			continue
		}
		if firstResp == nil {
			firstResp = responses[i]
		} else {
			if responses[i].RowsAffected != firstResp.RowsAffected {
				t.Errorf("request %d: expected RowsAffected=%d, got %d", i, firstResp.RowsAffected, responses[i].RowsAffected)
			}
		}
	}

	// Verify only 1 row was actually upserted (not 10)
	if firstResp != nil && firstResp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", firstResp.RowsUpserted)
	}
}

func TestBatcherIdempotency_ConcurrentDuplicates(t *testing.T) {
	memStore := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(memStore)

	cfg := BatcherConfig{
		BatchWindow:  500 * time.Millisecond,
		MaxBatchSize: DefaultMaxBatchSize,
	}
	batcher, err := NewBatcherWithConfig(memStore, stateMan, nil, cfg)
	if err != nil {
		t.Fatalf("failed to create batcher: %v", err)
	}
	defer batcher.Close()

	ctx := context.Background()
	ns := "test-namespace"
	reqID := uuid.New().String()

	req := &WriteRequest{
		RequestID: reqID,
		UpsertRows: []map[string]any{
			{"id": uint64(1), "name": "doc1"},
		},
	}

	// Launch multiple concurrent requests with the same request_id
	const numConcurrent = 10
	var wg sync.WaitGroup
	responses := make([]*WriteResponse, numConcurrent)
	errors := make([]error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := batcher.Submit(ctx, ns, req)
			responses[idx] = resp
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// All requests should succeed with the same response
	var firstResp *WriteResponse
	for i := 0; i < numConcurrent; i++ {
		if errors[i] != nil {
			t.Errorf("request %d failed: %v", i, errors[i])
			continue
		}
		if responses[i] == nil {
			t.Errorf("request %d returned nil response", i)
			continue
		}
		if firstResp == nil {
			firstResp = responses[i]
		} else {
			if responses[i].RowsAffected != firstResp.RowsAffected {
				t.Errorf("request %d: expected RowsAffected=%d, got %d", i, firstResp.RowsAffected, responses[i].RowsAffected)
			}
		}
	}

	// Verify only 1 row was actually upserted (not 10)
	if firstResp != nil && firstResp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", firstResp.RowsUpserted)
	}
}

func TestIdempotencyStore_ConcurrentReserve(t *testing.T) {
	store := NewIdempotencyStoreWithTTL(time.Minute)
	defer store.Close()

	ns := "test-ns"
	reqID := uuid.New().String()

	// Track how many get ReserveOK vs ReserveInFlight
	var okCount, inFlightCount int64
	var wg sync.WaitGroup
	const numConcurrent = 20

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, _, _ := store.Reserve(ns, reqID)
			switch result {
			case ReserveOK:
				atomic.AddInt64(&okCount, 1)
			case ReserveInFlight:
				atomic.AddInt64(&inFlightCount, 1)
			}
		}()
	}

	wg.Wait()

	// Exactly one should get ReserveOK, rest should get ReserveInFlight
	if okCount != 1 {
		t.Errorf("expected exactly 1 ReserveOK, got %d", okCount)
	}
	if inFlightCount != numConcurrent-1 {
		t.Errorf("expected %d ReserveInFlight, got %d", numConcurrent-1, inFlightCount)
	}
}

func TestIdempotencyStore_WaitForInFlight_ReturnsError(t *testing.T) {
	store := NewIdempotencyStoreWithTTL(time.Minute)
	defer store.Close()

	ns := "test-ns"
	reqID := uuid.New().String()

	// Reserve
	result1, _, _ := store.Reserve(ns, reqID)
	if result1 != ReserveOK {
		t.Fatalf("expected ReserveOK, got %v", result1)
	}

	// Second reserve should be in-flight
	result2, _, inflight := store.Reserve(ns, reqID)
	if result2 != ReserveInFlight {
		t.Fatalf("expected ReserveInFlight, got %v", result2)
	}

	// Release with error
	testErr := context.DeadlineExceeded
	store.Release(ns, reqID, testErr)

	// WaitForInFlight should return the error
	ctx := context.Background()
	resp, err := store.WaitForInFlight(ctx, inflight)
	if err != testErr {
		t.Errorf("expected error %v, got %v", testErr, err)
	}
	if resp != nil {
		t.Errorf("expected nil response, got %+v", resp)
	}
}

func TestIdempotencyStore_WaitForInFlight_ContextCancellation(t *testing.T) {
	store := NewIdempotencyStoreWithTTL(time.Minute)
	defer store.Close()

	ns := "test-ns"
	reqID := uuid.New().String()

	// Reserve
	result1, _, _ := store.Reserve(ns, reqID)
	if result1 != ReserveOK {
		t.Fatalf("expected ReserveOK, got %v", result1)
	}

	// Second reserve should be in-flight
	result2, _, inflight := store.Reserve(ns, reqID)
	if result2 != ReserveInFlight {
		t.Fatalf("expected ReserveInFlight, got %v", result2)
	}

	// Create a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Start WaitForInFlight in a goroutine
	var waitErr error
	var waitResp *WriteResponse
	done := make(chan struct{})
	go func() {
		waitResp, waitErr = store.WaitForInFlight(ctx, inflight)
		close(done)
	}()

	// Cancel the context
	cancel()

	// Wait for WaitForInFlight to return
	select {
	case <-done:
		// Good
	case <-time.After(time.Second):
		t.Fatal("WaitForInFlight did not return after context cancellation")
	}

	// Should return context.Canceled
	if waitErr != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", waitErr)
	}
	if waitResp != nil {
		t.Errorf("expected nil response, got %+v", waitResp)
	}

	// Clean up by completing the original request
	store.Complete(ns, reqID, &WriteResponse{RowsAffected: 1})
}
