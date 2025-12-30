package query

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

func writeTestStateWithFlags(ctx context.Context, store objectstore.Store, ns string, walHeadSeq, indexedSeq uint64, unindexedBytes int64, disableBackpressure bool) error {
	state := namespace.NewState(ns)
	state.WAL.HeadSeq = walHeadSeq
	state.Index.IndexedWALSeq = indexedSeq
	state.WAL.BytesUnindexedEst = unindexedBytes
	state.NamespaceFlags.DisableBackpressure = disableBackpressure

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	key := namespace.StateKey(ns)
	_, err = store.PutIfAbsent(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
	if err != nil && !objectstore.IsConflictError(err) {
		return err
	}
	if objectstore.IsConflictError(err) {
		_, err = store.Put(ctx, key, bytes.NewReader(data), int64(len(data)), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestStrongBackpressure(t *testing.T) {
	store := objectstore.NewMemoryStore()
	stateMan := namespace.NewStateManager(store)
	ctx := context.Background()

	if err := writeTestStateWithFlags(ctx, store, "backpressure-ns", 5, 2, MaxUnindexedBytes+1, true); err != nil {
		t.Fatalf("failed to write test state: %v", err)
	}

	handler := NewHandler(store, stateMan, nil)
	req := &QueryRequest{
		RankBy: []any{"id", "asc"},
	}

	_, err := handler.Handle(ctx, "backpressure-ns", req)
	if !errors.Is(err, ErrStrongQueryBackpressure) {
		t.Fatalf("expected ErrStrongQueryBackpressure, got %v", err)
	}
}
