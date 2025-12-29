package write

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestBackpressure_WritesReturn429WhenUnindexedDataExceeds2GB(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ns := "test-backpressure-ns"

	// Manually create a namespace state with high unindexed bytes
	loaded, err := stateMan.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Set BytesUnindexedEst to just above 2GB threshold
	_, err = stateMan.Update(ctx, ns, loaded.ETag, func(state *namespace.State) error {
		state.WAL.BytesUnindexedEst = MaxUnindexedBytes + 1
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	// Create handler
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Attempt to write without disable_backpressure
	req := &WriteRequest{
		RequestID: "req-1",
		UpsertRows: []map[string]any{
			{"id": "doc1", "field": "value"},
		},
		DisableBackpressure: false,
	}

	_, err = handler.Handle(ctx, ns, req)
	if err == nil {
		t.Fatal("expected backpressure error, got nil")
	}
	if err != ErrBackpressure {
		t.Fatalf("expected ErrBackpressure, got: %v", err)
	}
}

func TestBackpressure_DisableBackpressureAllowsWritesAboveThreshold(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ns := "test-backpressure-disable-ns"

	// Manually create a namespace state with high unindexed bytes
	loaded, err := stateMan.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Set BytesUnindexedEst to just above 2GB threshold
	_, err = stateMan.Update(ctx, ns, loaded.ETag, func(state *namespace.State) error {
		state.WAL.BytesUnindexedEst = MaxUnindexedBytes + 1
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	// Create handler
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Attempt to write WITH disable_backpressure=true
	req := &WriteRequest{
		RequestID: "req-2",
		UpsertRows: []map[string]any{
			{"id": "doc1", "field": "value"},
		},
		DisableBackpressure: true,
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("expected write to succeed with disable_backpressure=true, got error: %v", err)
	}
	if resp.RowsUpserted != 1 {
		t.Errorf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
}

func TestBackpressure_IncludesIncomingWriteSize(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ns := "test-backpressure-incoming-size"

	loaded, err := stateMan.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	_, err = stateMan.Update(ctx, ns, loaded.ETag, func(state *namespace.State) error {
		state.WAL.BytesUnindexedEst = MaxUnindexedBytes - 1
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	req := &WriteRequest{
		RequestID: "req-incoming-1",
		UpsertRows: []map[string]any{
			{"id": "doc1", "field": "value"},
		},
	}

	_, err = handler.Handle(ctx, ns, req)
	if err != ErrBackpressure {
		t.Fatalf("expected ErrBackpressure, got: %v", err)
	}
}

func TestBackpressure_AllowsWritesBelowThresholdWithIncomingSize(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ns := "test-backpressure-incoming-ok"

	loaded, err := stateMan.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	_, err = stateMan.Update(ctx, ns, loaded.ETag, func(state *namespace.State) error {
		state.WAL.BytesUnindexedEst = MaxUnindexedBytes - 10*1024*1024
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	req := &WriteRequest{
		RequestID: "req-incoming-2",
		UpsertRows: []map[string]any{
			{"id": "doc1", "field": "value"},
		},
	}

	resp, err := handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("expected write to succeed, got error: %v", err)
	}
	if resp.RowsUpserted != 1 {
		t.Fatalf("expected 1 row upserted, got %d", resp.RowsUpserted)
	}
}

func TestBackpressure_BytesUnindexedEstIsTrackedInState(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ns := "test-bytes-unindexed-ns"

	// Create handler
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Write some data
	req := &WriteRequest{
		RequestID: "req-3",
		UpsertRows: []map[string]any{
			{"id": "doc1", "field": "value"},
		},
	}

	_, err = handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("failed to handle write: %v", err)
	}

	// Load the state and verify bytes_unindexed_est was incremented
	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if loaded.State.WAL.BytesUnindexedEst <= 0 {
		t.Errorf("expected bytes_unindexed_est > 0, got %d", loaded.State.WAL.BytesUnindexedEst)
	}

	// The bytes_unindexed_est should increase with each write
	initialBytes := loaded.State.WAL.BytesUnindexedEst

	req2 := &WriteRequest{
		RequestID: "req-4",
		UpsertRows: []map[string]any{
			{"id": "doc2", "field": "another-value"},
		},
	}
	_, err = handler.Handle(ctx, ns, req2)
	if err != nil {
		t.Fatalf("failed to handle second write: %v", err)
	}

	loaded2, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state after second write: %v", err)
	}

	if loaded2.State.WAL.BytesUnindexedEst <= initialBytes {
		t.Errorf("expected bytes_unindexed_est to increase after second write, was %d now %d",
			initialBytes, loaded2.State.WAL.BytesUnindexedEst)
	}
}

func TestBackpressure_DisableBackpressureFlagParsedFromRequest(t *testing.T) {
	tests := []struct {
		name     string
		body     map[string]any
		expected bool
	}{
		{
			name:     "not set",
			body:     map[string]any{},
			expected: false,
		},
		{
			name:     "set to true",
			body:     map[string]any{"disable_backpressure": true},
			expected: true,
		},
		{
			name:     "set to false",
			body:     map[string]any{"disable_backpressure": false},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := ParseWriteRequest("req-id", tc.body)
			if err != nil {
				t.Fatalf("failed to parse request: %v", err)
			}
			if req.DisableBackpressure != tc.expected {
				t.Errorf("expected DisableBackpressure=%v, got %v", tc.expected, req.DisableBackpressure)
			}
		})
	}
}

func TestBackpressure_NamespaceFlagSetWhenDisableBackpressureUsedAboveThreshold(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ns := "test-namespace-flag-ns"

	// Create a namespace state with high unindexed bytes
	loaded, err := stateMan.Create(ctx, ns)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Set BytesUnindexedEst to just above 2GB threshold
	loaded, err = stateMan.Update(ctx, ns, loaded.ETag, func(state *namespace.State) error {
		state.WAL.BytesUnindexedEst = MaxUnindexedBytes + 1
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update state: %v", err)
	}

	// Verify the flag is initially false
	if loaded.State.NamespaceFlags.DisableBackpressure {
		t.Error("expected DisableBackpressure flag to be false initially")
	}

	// Create handler
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	// Write with disable_backpressure=true while above threshold
	req := &WriteRequest{
		RequestID: "req-5",
		UpsertRows: []map[string]any{
			{"id": "doc1", "field": "value"},
		},
		DisableBackpressure: true,
	}

	_, err = handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("expected write to succeed: %v", err)
	}

	// Load state and verify the namespace flag was set
	loaded2, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if !loaded2.State.NamespaceFlags.DisableBackpressure {
		t.Error("expected DisableBackpressure namespace flag to be set after writing above threshold")
	}
}

func TestBackpressure_StateJSONContainsBytesUnindexedEst(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ns := "test-state-json-ns"

	// Create handler and write some data
	handler, err := NewHandler(store, stateMan)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	defer handler.Close()

	req := &WriteRequest{
		RequestID: "req-6",
		UpsertRows: []map[string]any{
			{"id": "doc1", "field": "value"},
		},
	}

	_, err = handler.Handle(ctx, ns, req)
	if err != nil {
		t.Fatalf("failed to handle write: %v", err)
	}

	// Read the raw state JSON from object storage
	key := namespace.StateKey(ns)
	reader, _, err := store.Get(ctx, key, nil)
	if err != nil {
		t.Fatalf("failed to read state from object storage: %v", err)
	}
	defer reader.Close()

	var rawState map[string]any
	if err := json.NewDecoder(reader).Decode(&rawState); err != nil {
		t.Fatalf("failed to decode state JSON: %v", err)
	}

	// Check that wal.bytes_unindexed_est is present in the JSON
	walObj, ok := rawState["wal"].(map[string]any)
	if !ok {
		t.Fatal("expected 'wal' object in state JSON")
	}

	bytesUnindexed, ok := walObj["bytes_unindexed_est"]
	if !ok {
		t.Fatal("expected 'bytes_unindexed_est' field in wal object")
	}

	// The value should be > 0 after a write
	if bytesUnindexed.(float64) <= 0 {
		t.Errorf("expected bytes_unindexed_est > 0, got %v", bytesUnindexed)
	}
}
