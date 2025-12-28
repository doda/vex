// Package api provides HTTP API handlers for Vex.
// This file tests all documented limits as specified in the task.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/internal/schema"
	"github.com/vexsearch/vex/internal/write"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// Limits constants verified against spec
const (
	specMaxBatchSize256MB       = 256 * 1024 * 1024 // 256MB
	specMaxUnindexedBytes2GB    = 2 * 1024 * 1024 * 1024
	specMaxConcurrencyLimit16   = 16
	specMaxMultiQuery16         = 16
	specMaxTopK10000            = 10_000
	specMaxStringIDBytes64      = 64
	specMaxAttributeNameLen128  = 128
)

// TestLimit_MaxUpsertBatch256MB verifies 256MB max upsert batch size.
func TestLimit_MaxUpsertBatch256MB(t *testing.T) {
	t.Run("constant value is correct", func(t *testing.T) {
		if MaxRequestBodySize != specMaxBatchSize256MB {
			t.Errorf("MaxRequestBodySize should be %d (256MB), got %d", specMaxBatchSize256MB, MaxRequestBodySize)
		}
	})

	t.Run("requests under 256MB are accepted", func(t *testing.T) {
		cfg := config.Default()
		router := NewRouter(cfg)
		router.SetState(&ServerState{
			Namespaces:  map[string]*NamespaceState{"test-ns": {Exists: true}},
			ObjectStore: ObjectStoreState{Available: true},
		})

		// A small request should be accepted
		body := `{"upsert_rows":[{"id":1,"data":"test"}]}`
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})

	t.Run("requests over 256MB return 413", func(t *testing.T) {
		cfg := config.Default()
		router := NewRouter(cfg)
		router.SetState(&ServerState{
			Namespaces:  map[string]*NamespaceState{"test-ns": {Exists: true}},
			ObjectStore: ObjectStoreState{Available: true},
		})

		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", nil)
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = MaxRequestBodySize + 1
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("expected 413, got %d", w.Code)
		}
	})
}

// TestLimit_MaxBatchRatePerSecPerNamespace verifies 1 batch/s per namespace rate limit.
func TestLimit_MaxBatchRatePerSecPerNamespace(t *testing.T) {
	t.Run("batcher batches writes within 1 second window", func(t *testing.T) {
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)
		ctx := context.Background()

		// Use a short batch window for testing (simulates the 1/sec concept)
		batcher, err := write.NewBatcherWithConfig(store, stateMan, nil, write.BatcherConfig{
			BatchWindow:  100 * time.Millisecond,
			MaxBatchSize: 100 * 1024 * 1024,
		})
		if err != nil {
			t.Fatalf("failed to create batcher: %v", err)
		}
		defer batcher.Close()

		ns := "test-rate-limit-ns"

		// Submit 3 writes quickly
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				req := &write.WriteRequest{
					RequestID:  fmt.Sprintf("req-%d", idx),
					UpsertRows: []map[string]any{{"id": uint64(idx), "name": "test"}},
				}
				batcher.Submit(ctx, ns, req)
			}(i)
		}
		wg.Wait()

		// All 3 should batch into 1 WAL entry
		state, err := stateMan.Load(ctx, ns)
		if err != nil {
			t.Fatalf("failed to load state: %v", err)
		}
		if state.State.WAL.HeadSeq != 1 {
			t.Errorf("expected 1 WAL entry (batched), got %d", state.State.WAL.HeadSeq)
		}
	})

	t.Run("default batch window is 1 second", func(t *testing.T) {
		if write.DefaultBatchWindow != time.Second {
			t.Errorf("DefaultBatchWindow should be 1 second, got %v", write.DefaultBatchWindow)
		}
	})
}

// TestLimit_2GBUnindexedCap verifies 2GB unindexed data threshold.
func TestLimit_2GBUnindexedCap(t *testing.T) {
	t.Run("constant value is correct", func(t *testing.T) {
		if MaxUnindexedBytes != specMaxUnindexedBytes2GB {
			t.Errorf("MaxUnindexedBytes should be %d (2GB), got %d", specMaxUnindexedBytes2GB, MaxUnindexedBytes)
		}
		if write.MaxUnindexedBytes != specMaxUnindexedBytes2GB {
			t.Errorf("write.MaxUnindexedBytes should be %d (2GB), got %d", specMaxUnindexedBytes2GB, write.MaxUnindexedBytes)
		}
	})

	t.Run("writes return 429 when unindexed data exceeds 2GB", func(t *testing.T) {
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)
		ctx := context.Background()
		ns := "test-backpressure-ns"

		// Create namespace with high unindexed bytes
		loaded, _ := stateMan.Create(ctx, ns)
		stateMan.Update(ctx, ns, loaded.ETag, func(state *namespace.State) error {
			state.WAL.BytesUnindexedEst = MaxUnindexedBytes + 1
			return nil
		})

		handler, _ := write.NewHandler(store, stateMan)
		defer handler.Close()

		req := &write.WriteRequest{
			RequestID:           "req-1",
			UpsertRows:          []map[string]any{{"id": "doc1", "field": "value"}},
			DisableBackpressure: false,
		}

		_, err := handler.Handle(ctx, ns, req)
		if err != write.ErrBackpressure {
			t.Errorf("expected ErrBackpressure, got: %v", err)
		}
	})

	t.Run("disable_backpressure=true allows writes above threshold", func(t *testing.T) {
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)
		ctx := context.Background()
		ns := "test-disable-bp-ns"

		loaded, _ := stateMan.Create(ctx, ns)
		stateMan.Update(ctx, ns, loaded.ETag, func(state *namespace.State) error {
			state.WAL.BytesUnindexedEst = MaxUnindexedBytes + 1
			return nil
		})

		handler, _ := write.NewHandler(store, stateMan)
		defer handler.Close()

		req := &write.WriteRequest{
			RequestID:           "req-2",
			UpsertRows:          []map[string]any{{"id": "doc1", "field": "value"}},
			DisableBackpressure: true,
		}

		resp, err := handler.Handle(ctx, ns, req)
		if err != nil {
			t.Fatalf("expected success with disable_backpressure=true, got: %v", err)
		}
		if resp.RowsUpserted != 1 {
			t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
		}
	})
}

// TestLimit_QueryConcurrency16 verifies query concurrency limit of 16 per namespace.
func TestLimit_QueryConcurrency16(t *testing.T) {
	t.Run("default limit is 16", func(t *testing.T) {
		if query.DefaultConcurrencyLimit != specMaxConcurrencyLimit16 {
			t.Errorf("DefaultConcurrencyLimit should be %d, got %d", specMaxConcurrencyLimit16, query.DefaultConcurrencyLimit)
		}
	})

	t.Run("queries queue when concurrency exceeds 16", func(t *testing.T) {
		limiter := query.NewConcurrencyLimiter(specMaxConcurrencyLimit16)
		ctx := context.Background()
		ns := "test-concurrency-ns"

		// Acquire all 16 slots
		releases := make([]func(), specMaxConcurrencyLimit16)
		for i := 0; i < specMaxConcurrencyLimit16; i++ {
			release, err := limiter.Acquire(ctx, ns)
			if err != nil {
				t.Fatalf("failed to acquire slot %d: %v", i, err)
			}
			releases[i] = release
		}

		// Verify all slots are taken
		if limiter.ActiveCount(ns) != specMaxConcurrencyLimit16 {
			t.Errorf("expected %d active, got %d", specMaxConcurrencyLimit16, limiter.ActiveCount(ns))
		}

		// Next acquire should block/timeout
		ctx17, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		_, err := limiter.Acquire(ctx17, ns)
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded when at limit, got %v", err)
		}

		// Clean up
		for _, release := range releases {
			release()
		}
	})

	t.Run("queued queries eventually execute", func(t *testing.T) {
		limiter := query.NewConcurrencyLimiter(2)
		ctx := context.Background()
		ns := "test-queue-ns"

		release1, _ := limiter.Acquire(ctx, ns)
		release2, _ := limiter.Acquire(ctx, ns)

		var acquired atomic.Bool
		go func() {
			release, err := limiter.Acquire(ctx, ns)
			if err == nil {
				acquired.Store(true)
				release()
			}
		}()

		time.Sleep(10 * time.Millisecond)
		if acquired.Load() {
			t.Error("should not have acquired yet")
		}

		release1()
		time.Sleep(50 * time.Millisecond)

		if !acquired.Load() {
			t.Error("queued query should have executed")
		}

		release2()
	})
}

// TestLimit_MaxMultiQuery16 verifies max 16 subqueries in multi-query.
func TestLimit_MaxMultiQuery16(t *testing.T) {
	t.Run("constant value is correct", func(t *testing.T) {
		if query.MaxMultiQuery != specMaxMultiQuery16 {
			t.Errorf("MaxMultiQuery should be %d, got %d", specMaxMultiQuery16, query.MaxMultiQuery)
		}
	})

	t.Run("exactly 16 subqueries is allowed", func(t *testing.T) {
		cfg := config.Default()
		router := NewRouter(cfg)
		router.SetState(&ServerState{
			Namespaces:  map[string]*NamespaceState{"test-ns": {Exists: true}},
			ObjectStore: ObjectStoreState{Available: true},
		})

		queries := make([]map[string]any, specMaxMultiQuery16)
		for i := 0; i < specMaxMultiQuery16; i++ {
			queries[i] = map[string]any{"rank_by": []any{"id", "asc"}, "limit": 1}
		}
		body, _ := json.Marshal(map[string]any{"queries": queries})

		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for 16 subqueries, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("17 subqueries returns error", func(t *testing.T) {
		cfg := config.Default()
		router := NewRouter(cfg)
		router.SetState(&ServerState{
			Namespaces:  map[string]*NamespaceState{"test-ns": {Exists: true}},
			ObjectStore: ObjectStoreState{Available: true},
		})

		queries := make([]map[string]any, specMaxMultiQuery16+1)
		for i := 0; i < specMaxMultiQuery16+1; i++ {
			queries[i] = map[string]any{"rank_by": []any{"id", "asc"}, "limit": 1}
		}
		body, _ := json.Marshal(map[string]any{"queries": queries})

		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for >16 subqueries, got %d", w.Code)
		}

		// Verify error message
		respBody, _ := io.ReadAll(w.Body)
		if !strings.Contains(string(respBody), "16 subqueries") {
			t.Errorf("expected error message about 16 subqueries, got: %s", respBody)
		}
	})
}

// TestLimit_MaxTopK10000 verifies max limit/top_k of 10,000.
func TestLimit_MaxTopK10000(t *testing.T) {
	t.Run("constant value is correct", func(t *testing.T) {
		if query.MaxTopK != specMaxTopK10000 {
			t.Errorf("MaxTopK should be %d, got %d", specMaxTopK10000, query.MaxTopK)
		}
	})

	t.Run("limit at 10,000 is accepted", func(t *testing.T) {
		cfg := config.Default()
		router := NewRouter(cfg)
		router.SetState(&ServerState{
			Namespaces:  map[string]*NamespaceState{"test-ns": {Exists: true}},
			ObjectStore: ObjectStoreState{Available: true},
		})

		body := fmt.Sprintf(`{"rank_by":["id","asc"],"limit":%d}`, specMaxTopK10000)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for limit=%d, got %d: %s", specMaxTopK10000, w.Code, w.Body.String())
		}
	})

	t.Run("limit over 10,000 returns 400", func(t *testing.T) {
		cfg := config.Default()
		router := NewRouter(cfg)
		router.SetState(&ServerState{
			Namespaces:  map[string]*NamespaceState{"test-ns": {Exists: true}},
			ObjectStore: ObjectStoreState{Available: true},
		})

		body := fmt.Sprintf(`{"rank_by":["id","asc"],"limit":%d}`, specMaxTopK10000+1)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for limit>10,000, got %d", w.Code)
		}

		// Verify error message
		respBody, _ := io.ReadAll(w.Body)
		if !strings.Contains(string(respBody), "10,000") {
			t.Errorf("expected error message about 10,000 limit, got: %s", respBody)
		}
	})

	t.Run("top_k over 10,000 returns 400", func(t *testing.T) {
		cfg := config.Default()
		router := NewRouter(cfg)
		router.SetState(&ServerState{
			Namespaces:  map[string]*NamespaceState{"test-ns": {Exists: true}},
			ObjectStore: ObjectStoreState{Available: true},
		})

		body := fmt.Sprintf(`{"rank_by":["id","asc"],"top_k":%d}`, specMaxTopK10000+1)
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for top_k>10,000, got %d", w.Code)
		}
	})
}

// TestLimit_Max64ByteIDs verifies max 64 byte document IDs.
func TestLimit_Max64ByteIDs(t *testing.T) {
	t.Run("constant value is correct", func(t *testing.T) {
		if document.MaxStringIDBytes != specMaxStringIDBytes64 {
			t.Errorf("MaxStringIDBytes should be %d, got %d", specMaxStringIDBytes64, document.MaxStringIDBytes)
		}
	})

	t.Run("string ID at exactly 64 bytes is accepted", func(t *testing.T) {
		id64 := strings.Repeat("a", specMaxStringIDBytes64)
		_, err := document.ParseID(id64)
		if err != nil {
			t.Errorf("expected 64-byte string ID to be accepted, got error: %v", err)
		}
	})

	t.Run("string ID over 64 bytes is rejected", func(t *testing.T) {
		id65 := strings.Repeat("a", specMaxStringIDBytes64+1)
		_, err := document.ParseID(id65)
		if err == nil {
			t.Error("expected error for 65-byte string ID")
		}
		valErr, ok := err.(*document.ValidationError)
		if !ok {
			t.Errorf("expected ValidationError, got %T", err)
		}
		if !strings.Contains(valErr.Message, "exceeds maximum") {
			t.Errorf("expected 'exceeds maximum' in error, got: %s", valErr.Message)
		}
	})

	t.Run("write handler returns error for oversized string ID", func(t *testing.T) {
		// This test validates the write handler (not the fallback mode)
		// rejects oversized string IDs. The validation happens during
		// document processing in the write handler.
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)
		ctx := context.Background()
		ns := "test-id-limit-ns"

		handler, _ := write.NewHandler(store, stateMan)
		defer handler.Close()

		id65 := strings.Repeat("x", specMaxStringIDBytes64+1)
		req := &write.WriteRequest{
			RequestID:  "test-req",
			UpsertRows: []map[string]any{{"id": id65, "name": "test"}},
		}

		_, err := handler.Handle(ctx, ns, req)
		if err == nil {
			t.Fatal("expected error for oversized ID")
		}
		if !strings.Contains(err.Error(), "invalid document ID") {
			t.Errorf("expected 'invalid document ID' error, got: %v", err)
		}
	})
}

// TestLimit_Max128CharAttributeNames verifies max 128 char attribute names.
func TestLimit_Max128CharAttributeNames(t *testing.T) {
	t.Run("constant value is correct", func(t *testing.T) {
		if schema.MaxAttributeNameLength != specMaxAttributeNameLen128 {
			t.Errorf("MaxAttributeNameLength should be %d, got %d", specMaxAttributeNameLen128, schema.MaxAttributeNameLength)
		}
	})

	t.Run("attribute name at exactly 128 chars is accepted", func(t *testing.T) {
		name128 := strings.Repeat("a", specMaxAttributeNameLen128)
		err := schema.ValidateAttributeName(name128)
		if err != nil {
			t.Errorf("expected 128-char attribute name to be accepted, got error: %v", err)
		}
	})

	t.Run("attribute name over 128 chars is rejected", func(t *testing.T) {
		name129 := strings.Repeat("a", specMaxAttributeNameLen128+1)
		err := schema.ValidateAttributeName(name129)
		if err == nil {
			t.Error("expected error for 129-char attribute name")
		}
		if err != schema.ErrAttributeNameTooLong {
			t.Errorf("expected ErrAttributeNameTooLong, got: %v", err)
		}
	})

	t.Run("write handler returns error for oversized attribute name", func(t *testing.T) {
		// This test validates the write handler (not the fallback mode)
		// rejects oversized attribute names. The validation happens during
		// document processing in the write handler.
		store := objectstore.NewMemoryStore()
		stateMan := namespace.NewStateManager(store)
		ctx := context.Background()
		ns := "test-attr-limit-ns"

		handler, _ := write.NewHandler(store, stateMan)
		defer handler.Close()

		name129 := strings.Repeat("x", specMaxAttributeNameLen128+1)
		req := &write.WriteRequest{
			RequestID:  "test-req",
			UpsertRows: []map[string]any{{"id": 1, name129: "value"}},
		}

		_, err := handler.Handle(ctx, ns, req)
		if err == nil {
			t.Fatal("expected error for oversized attribute name")
		}
		if !strings.Contains(err.Error(), "invalid attribute") {
			t.Errorf("expected 'invalid attribute' error, got: %v", err)
		}
	})
}

// TestLimits_AllDocumentedLimitsEnforced is a summary test verifying all limits.
func TestLimits_AllDocumentedLimitsEnforced(t *testing.T) {
	tests := []struct {
		name     string
		limit    int64
		expected int64
	}{
		{"max upsert batch", MaxRequestBodySize, 256 * 1024 * 1024},
		{"2GB unindexed cap", MaxUnindexedBytes, 2 * 1024 * 1024 * 1024},
		{"query concurrency limit", int64(query.DefaultConcurrencyLimit), 16},
		{"max multi-query", int64(query.MaxMultiQuery), 16},
		{"max limit/top_k", int64(query.MaxTopK), 10_000},
		{"max string ID bytes", int64(document.MaxStringIDBytes), 64},
		{"max attribute name length", int64(schema.MaxAttributeNameLength), 128},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.limit != tt.expected {
				t.Errorf("%s: expected %d, got %d", tt.name, tt.expected, tt.limit)
			}
		})
	}
}
