package namespace

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/vexsearch/vex/pkg/objectstore"
)

func TestStateSchema(t *testing.T) {
	t.Run("format_version present", func(t *testing.T) {
		state := NewState("test-ns")
		if state.FormatVersion != CurrentFormatVersion {
			t.Errorf("expected format_version %d, got %d", CurrentFormatVersion, state.FormatVersion)
		}
	})

	t.Run("all required fields present", func(t *testing.T) {
		state := NewState("test-ns")
		data, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("failed to marshal state: %v", err)
		}

		var m map[string]interface{}
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("failed to unmarshal state: %v", err)
		}

		requiredFields := []string{
			"format_version",
			"namespace",
			"created_at",
			"updated_at",
			"wal",
			"index",
			"namespace_flags",
			"deletion",
		}

		for _, field := range requiredFields {
			if _, exists := m[field]; !exists {
				t.Errorf("required field %q not present in state JSON", field)
			}
		}
	})

	t.Run("wal subfields present", func(t *testing.T) {
		state := NewState("test-ns")
		data, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("failed to marshal state: %v", err)
		}

		var m map[string]interface{}
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("failed to unmarshal state: %v", err)
		}

		wal := m["wal"].(map[string]interface{})
		walFields := []string{"head_seq", "head_key", "bytes_unindexed_est", "status"}
		for _, field := range walFields {
			if _, exists := wal[field]; !exists {
				t.Errorf("required wal field %q not present", field)
			}
		}
	})

	t.Run("index subfields present", func(t *testing.T) {
		state := NewState("test-ns")
		data, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("failed to marshal state: %v", err)
		}

		var m map[string]interface{}
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("failed to unmarshal state: %v", err)
		}

		index := m["index"].(map[string]interface{})
		indexFields := []string{"manifest_seq", "indexed_wal_seq", "status"}
		for _, field := range indexFields {
			if _, exists := index[field]; !exists {
				t.Errorf("required index field %q not present", field)
			}
		}
	})
}

func TestCASLoop(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	t.Run("successful update with correct ETag", func(t *testing.T) {
		loaded, err := mgr.Create(ctx, "test-cas-1")
		if err != nil {
			t.Fatalf("failed to create state: %v", err)
		}

		updated, err := mgr.Update(ctx, "test-cas-1", loaded.ETag, func(state *State) error {
			state.WAL.HeadSeq++
			state.WAL.HeadKey = "wal/00000000000000000001.wal.zst"
			return nil
		})
		if err != nil {
			t.Fatalf("failed to update state: %v", err)
		}

		if updated.State.WAL.HeadSeq != 1 {
			t.Errorf("expected head_seq 1, got %d", updated.State.WAL.HeadSeq)
		}
	})

	t.Run("retry on ETag mismatch", func(t *testing.T) {
		loaded, err := mgr.Create(ctx, "test-cas-2")
		if err != nil {
			t.Fatalf("failed to create state: %v", err)
		}

		// Simulate a concurrent update by modifying state directly
		_, err = mgr.Update(ctx, "test-cas-2", loaded.ETag, func(state *State) error {
			state.WAL.HeadSeq++
			state.WAL.HeadKey = "wal/00000000000000000001.wal.zst"
			return nil
		})
		if err != nil {
			t.Fatalf("first update failed: %v", err)
		}

		// Now try to update with stale ETag - should succeed after retry
		updateCount := 0
		updated, err := mgr.Update(ctx, "test-cas-2", loaded.ETag, func(state *State) error {
			updateCount++
			state.WAL.HeadSeq++
			state.WAL.HeadKey = "wal/00000000000000000002.wal.zst"
			return nil
		})
		if err != nil {
			t.Fatalf("second update failed: %v", err)
		}

		if updated.State.WAL.HeadSeq != 2 {
			t.Errorf("expected head_seq 2, got %d", updated.State.WAL.HeadSeq)
		}

		// Verify retry happened
		if updateCount < 1 {
			t.Error("update function should have been called at least once")
		}
	})
}

func TestWALSeqMonotonicIncrease(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	loaded, err := mgr.Create(ctx, "test-wal-seq")
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	t.Run("head_seq increases by exactly 1", func(t *testing.T) {
		for i := uint64(1); i <= 5; i++ {
			loaded, err = mgr.Update(ctx, "test-wal-seq", loaded.ETag, func(state *State) error {
				state.WAL.HeadSeq++
				return nil
			})
			if err != nil {
				t.Fatalf("failed to update at seq %d: %v", i, err)
			}
			if loaded.State.WAL.HeadSeq != i {
				t.Errorf("expected head_seq %d, got %d", i, loaded.State.WAL.HeadSeq)
			}
		}
	})

	t.Run("reject increment by more than 1", func(t *testing.T) {
		_, err := mgr.Update(ctx, "test-wal-seq", loaded.ETag, func(state *State) error {
			state.WAL.HeadSeq += 2 // Skip one
			return nil
		})
		if err == nil {
			t.Error("expected error for non-monotonic increment")
		}
		if err != nil && !containsError(err, ErrWALSeqNotMonotonic) {
			t.Errorf("expected ErrWALSeqNotMonotonic, got %v", err)
		}
	})

	t.Run("reject decrement", func(t *testing.T) {
		_, err := mgr.Update(ctx, "test-wal-seq", loaded.ETag, func(state *State) error {
			state.WAL.HeadSeq--
			return nil
		})
		if err == nil {
			t.Error("expected error for decrement")
		}
	})
}

func TestIndexSeqConstraint(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	loaded, err := mgr.Create(ctx, "test-index-seq")
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	// Advance WAL to seq 10
	for i := 0; i < 10; i++ {
		loaded, err = mgr.Update(ctx, "test-index-seq", loaded.ETag, func(state *State) error {
			state.WAL.HeadSeq++
			return nil
		})
		if err != nil {
			t.Fatalf("failed to advance WAL: %v", err)
		}
	}

	t.Run("indexed_wal_seq can equal head_seq", func(t *testing.T) {
		loaded, err = mgr.Update(ctx, "test-index-seq", loaded.ETag, func(state *State) error {
			state.Index.IndexedWALSeq = 10
			return nil
		})
		if err != nil {
			t.Errorf("should allow indexed_wal_seq == head_seq: %v", err)
		}
	})

	t.Run("indexed_wal_seq cannot exceed head_seq", func(t *testing.T) {
		_, err := mgr.Update(ctx, "test-index-seq", loaded.ETag, func(state *State) error {
			state.Index.IndexedWALSeq = 11
			return nil
		})
		if err == nil {
			t.Error("expected error when indexed_wal_seq > head_seq")
		}
		if err != nil && !containsError(err, ErrIndexSeqExceedsWAL) {
			t.Errorf("expected ErrIndexSeqExceedsWAL, got %v", err)
		}
	})
}

func TestTimestamps(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	t.Run("created_at set on creation", func(t *testing.T) {
		before := time.Now().UTC()
		loaded, err := mgr.Create(ctx, "test-timestamps")
		if err != nil {
			t.Fatalf("failed to create state: %v", err)
		}
		after := time.Now().UTC()

		if loaded.State.CreatedAt.Before(before) || loaded.State.CreatedAt.After(after) {
			t.Errorf("created_at %v not between %v and %v", loaded.State.CreatedAt, before, after)
		}
	})

	t.Run("updated_at changes on update", func(t *testing.T) {
		loaded, err := mgr.Load(ctx, "test-timestamps")
		if err != nil {
			t.Fatalf("failed to load state: %v", err)
		}
		originalUpdatedAt := loaded.State.UpdatedAt

		time.Sleep(10 * time.Millisecond)

		loaded, err = mgr.Update(ctx, "test-timestamps", loaded.ETag, func(state *State) error {
			state.WAL.HeadSeq++
			return nil
		})
		if err != nil {
			t.Fatalf("failed to update state: %v", err)
		}

		if !loaded.State.UpdatedAt.After(originalUpdatedAt) {
			t.Errorf("updated_at not updated: original=%v, new=%v", originalUpdatedAt, loaded.State.UpdatedAt)
		}
	})

	t.Run("created_at does not change on update", func(t *testing.T) {
		loaded, err := mgr.Load(ctx, "test-timestamps")
		if err != nil {
			t.Fatalf("failed to load state: %v", err)
		}
		originalCreatedAt := loaded.State.CreatedAt

		loaded, err = mgr.Update(ctx, "test-timestamps", loaded.ETag, func(state *State) error {
			state.WAL.HeadSeq++
			return nil
		})
		if err != nil {
			t.Fatalf("failed to update state: %v", err)
		}

		if !loaded.State.CreatedAt.Equal(originalCreatedAt) {
			t.Errorf("created_at changed: original=%v, new=%v", originalCreatedAt, loaded.State.CreatedAt)
		}
	})
}

func TestSchemaTypeImmutability(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	loaded, err := mgr.Create(ctx, "test-schema-immut")
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	// Set initial schema
	loaded, err = mgr.Update(ctx, "test-schema-immut", loaded.ETag, func(state *State) error {
		state.Schema = &Schema{
			Attributes: map[string]AttributeSchema{
				"name":  {Type: "string"},
				"age":   {Type: "int"},
				"score": {Type: "float"},
			},
		}
		state.WAL.HeadSeq++
		return nil
	})
	if err != nil {
		t.Fatalf("failed to set initial schema: %v", err)
	}

	t.Run("reject type change string->int", func(t *testing.T) {
		_, err := mgr.Update(ctx, "test-schema-immut", loaded.ETag, func(state *State) error {
			state.Schema.Attributes["name"] = AttributeSchema{Type: "int"}
			state.WAL.HeadSeq++
			return nil
		})
		if err == nil {
			t.Error("expected error when changing attribute type")
		}
		if err != nil && !containsError(err, ErrSchemaTypeChange) {
			t.Errorf("expected ErrSchemaTypeChange, got %v", err)
		}
	})

	t.Run("allow adding new attributes", func(t *testing.T) {
		_, err := mgr.Update(ctx, "test-schema-immut", loaded.ETag, func(state *State) error {
			state.Schema.Attributes["newfield"] = AttributeSchema{Type: "string"}
			state.WAL.HeadSeq++
			return nil
		})
		if err != nil {
			t.Errorf("should allow adding new attributes: %v", err)
		}
	})

	t.Run("allow updating filterable option", func(t *testing.T) {
		loaded, _ = mgr.Load(ctx, "test-schema-immut")
		filterable := false
		_, err := mgr.Update(ctx, "test-schema-immut", loaded.ETag, func(state *State) error {
			attr := state.Schema.Attributes["name"]
			attr.Filterable = &filterable
			state.Schema.Attributes["name"] = attr
			state.WAL.HeadSeq++
			return nil
		})
		if err != nil {
			t.Errorf("should allow updating filterable: %v", err)
		}
	})
}

func TestPendingRebuilds(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	loaded, err := mgr.Create(ctx, "test-rebuilds")
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	t.Run("add pending rebuild", func(t *testing.T) {
		loaded, err = mgr.AddPendingRebuild(ctx, "test-rebuilds", loaded.ETag, "filter", "category")
		if err != nil {
			t.Fatalf("failed to add pending rebuild: %v", err)
		}

		if len(loaded.State.Index.PendingRebuilds) != 1 {
			t.Errorf("expected 1 pending rebuild, got %d", len(loaded.State.Index.PendingRebuilds))
		}

		pr := loaded.State.Index.PendingRebuilds[0]
		if pr.Kind != "filter" || pr.Attribute != "category" {
			t.Errorf("wrong pending rebuild: %+v", pr)
		}
		if pr.Ready {
			t.Error("pending rebuild should not be ready initially")
		}
	})

	t.Run("HasPendingRebuild returns true", func(t *testing.T) {
		if !HasPendingRebuild(loaded.State, "filter", "category") {
			t.Error("HasPendingRebuild should return true")
		}
	})

	t.Run("mark rebuild ready", func(t *testing.T) {
		loaded, err = mgr.MarkRebuildReady(ctx, "test-rebuilds", loaded.ETag, "filter", "category", 1)
		if err != nil {
			t.Fatalf("failed to mark rebuild ready: %v", err)
		}

		if !loaded.State.Index.PendingRebuilds[0].Ready {
			t.Error("pending rebuild should be ready")
		}
		if loaded.State.Index.PendingRebuilds[0].Version != 1 {
			t.Errorf("expected version 1, got %d", loaded.State.Index.PendingRebuilds[0].Version)
		}
	})

	t.Run("HasPendingRebuild returns false after ready", func(t *testing.T) {
		if HasPendingRebuild(loaded.State, "filter", "category") {
			t.Error("HasPendingRebuild should return false for ready rebuilds")
		}
	})

	t.Run("remove pending rebuild", func(t *testing.T) {
		loaded, err = mgr.RemovePendingRebuild(ctx, "test-rebuilds", loaded.ETag, "filter", "category")
		if err != nil {
			t.Fatalf("failed to remove pending rebuild: %v", err)
		}

		if len(loaded.State.Index.PendingRebuilds) != 0 {
			t.Errorf("expected 0 pending rebuilds, got %d", len(loaded.State.Index.PendingRebuilds))
		}
	})

	t.Run("multiple pending rebuilds", func(t *testing.T) {
		loaded, _ = mgr.AddPendingRebuild(ctx, "test-rebuilds", loaded.ETag, "filter", "attr1")
		loaded, _ = mgr.AddPendingRebuild(ctx, "test-rebuilds", loaded.ETag, "fts", "content")
		loaded, _ = mgr.AddPendingRebuild(ctx, "test-rebuilds", loaded.ETag, "filter", "attr2")

		if len(loaded.State.Index.PendingRebuilds) != 3 {
			t.Errorf("expected 3 pending rebuilds, got %d", len(loaded.State.Index.PendingRebuilds))
		}
	})
}

func TestAdvanceWAL(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	loaded, err := mgr.Create(ctx, "test-advance-wal")
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	t.Run("advance WAL with schema delta", func(t *testing.T) {
		schemaDelta := &Schema{
			Attributes: map[string]AttributeSchema{
				"title": {Type: "string"},
			},
		}

		loaded, err = mgr.AdvanceWAL(ctx, "test-advance-wal", loaded.ETag,
			"wal/00000000000000000001.wal.zst", 1024, schemaDelta)
		if err != nil {
			t.Fatalf("failed to advance WAL: %v", err)
		}

		if loaded.State.WAL.HeadSeq != 1 {
			t.Errorf("expected head_seq 1, got %d", loaded.State.WAL.HeadSeq)
		}
		if loaded.State.WAL.BytesUnindexedEst != 1024 {
			t.Errorf("expected bytes_unindexed_est 1024, got %d", loaded.State.WAL.BytesUnindexedEst)
		}
		if loaded.State.WAL.Status != "updating" {
			t.Errorf("expected status 'updating', got %q", loaded.State.WAL.Status)
		}
		if loaded.State.Schema == nil || loaded.State.Schema.Attributes["title"].Type != "string" {
			t.Error("schema delta not applied")
		}
	})

	t.Run("duplicate wal key returns existing state", func(t *testing.T) {
		ns := "test-advance-wal-duplicate"
		loaded, err := mgr.Create(ctx, ns)
		if err != nil {
			t.Fatalf("failed to create state: %v", err)
		}
		staleETag := loaded.ETag
		walKey := "wal/00000000000000000001.wal.zst"

		loaded, err = mgr.AdvanceWAL(ctx, ns, staleETag, walKey, 10, nil)
		if err != nil {
			t.Fatalf("failed to advance WAL: %v", err)
		}

		loaded, err = mgr.AdvanceWAL(ctx, ns, staleETag, walKey, 10, nil)
		if err != nil {
			t.Fatalf("expected duplicate WAL advance to succeed: %v", err)
		}
		if loaded.State.WAL.HeadSeq != 1 {
			t.Errorf("expected head_seq 1, got %d", loaded.State.WAL.HeadSeq)
		}
		if loaded.State.WAL.HeadKey != walKey {
			t.Errorf("expected head_key %q, got %q", walKey, loaded.State.WAL.HeadKey)
		}
		if loaded.State.WAL.BytesUnindexedEst != 10 {
			t.Errorf("expected bytes_unindexed_est 10, got %d", loaded.State.WAL.BytesUnindexedEst)
		}
	})
}

func TestAdvanceIndex(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	loaded, err := mgr.Create(ctx, "test-advance-index")
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	// Advance WAL first
	for i := 0; i < 5; i++ {
		loaded, _ = mgr.Update(ctx, "test-advance-index", loaded.ETag, func(state *State) error {
			state.WAL.HeadSeq++
			state.WAL.BytesUnindexedEst += 1000
			return nil
		})
	}

	t.Run("advance index state", func(t *testing.T) {
		loaded, err = mgr.AdvanceIndex(ctx, "test-advance-index", loaded.ETag,
			"index/manifests/00000001.idx.json", 1, 5, 5000)
		if err != nil {
			t.Fatalf("failed to advance index: %v", err)
		}

		if loaded.State.Index.ManifestSeq != 1 {
			t.Errorf("expected manifest_seq 1, got %d", loaded.State.Index.ManifestSeq)
		}
		if loaded.State.Index.IndexedWALSeq != 5 {
			t.Errorf("expected indexed_wal_seq 5, got %d", loaded.State.Index.IndexedWALSeq)
		}
		if loaded.State.Index.Status != "up-to-date" {
			t.Errorf("expected status 'up-to-date', got %q", loaded.State.Index.Status)
		}
		if loaded.State.WAL.BytesUnindexedEst != 0 {
			t.Errorf("expected bytes_unindexed_est 0, got %d", loaded.State.WAL.BytesUnindexedEst)
		}
	})
}

func TestTombstone(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	loaded, err := mgr.Create(ctx, "test-tombstone")
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}

	t.Run("set tombstoned", func(t *testing.T) {
		loaded, err = mgr.SetTombstoned(ctx, "test-tombstone", loaded.ETag)
		if err != nil {
			t.Fatalf("failed to set tombstone: %v", err)
		}

		if !loaded.State.Deletion.Tombstoned {
			t.Error("expected tombstoned=true")
		}
		if loaded.State.Deletion.TombstonedAt == nil {
			t.Error("expected tombstoned_at to be set")
		}
	})

	t.Run("load tombstoned namespace returns error", func(t *testing.T) {
		_, err := mgr.Load(ctx, "test-tombstone")
		if err == nil {
			t.Error("expected error loading tombstoned namespace")
		}
		if err != ErrNamespaceTombstoned {
			t.Errorf("expected ErrNamespaceTombstoned, got %v", err)
		}
	})
}

func TestWriteTombstone(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	t.Run("write tombstone creates file", func(t *testing.T) {
		tombstone, err := mgr.WriteTombstone(ctx, "test-write-tombstone")
		if err != nil {
			t.Fatalf("failed to write tombstone: %v", err)
		}

		if tombstone.DeletedAt.IsZero() {
			t.Error("expected deleted_at to be set")
		}

		// Verify file exists
		key := TombstoneKey("test-write-tombstone")
		reader, _, err := store.Get(ctx, key, nil)
		if err != nil {
			t.Fatalf("expected tombstone file to exist: %v", err)
		}
		reader.Close()
	})

	t.Run("write tombstone is idempotent", func(t *testing.T) {
		// First write
		_, err := mgr.WriteTombstone(ctx, "test-idempotent-tombstone")
		if err != nil {
			t.Fatalf("failed to write tombstone: %v", err)
		}

		// Second write should succeed (tombstone already exists)
		tombstone, err := mgr.WriteTombstone(ctx, "test-idempotent-tombstone")
		if err != nil {
			t.Fatalf("expected idempotent write to succeed: %v", err)
		}

		if tombstone.DeletedAt.IsZero() {
			t.Error("expected deleted_at to be set")
		}
	})
}

func TestTombstoneKey(t *testing.T) {
	key := TombstoneKey("my-namespace")
	expected := "vex/namespaces/my-namespace/meta/tombstone.json"
	if key != expected {
		t.Errorf("expected %q, got %q", expected, key)
	}
}

func TestDeleteNamespace(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	t.Run("delete existing namespace", func(t *testing.T) {
		// Create namespace first
		_, err := mgr.Create(ctx, "test-delete-ns")
		if err != nil {
			t.Fatalf("failed to create namespace: %v", err)
		}

		// Delete namespace
		err = mgr.DeleteNamespace(ctx, "test-delete-ns")
		if err != nil {
			t.Fatalf("failed to delete namespace: %v", err)
		}

		// Verify tombstone.json exists
		tombstoneKey := TombstoneKey("test-delete-ns")
		reader, _, err := store.Get(ctx, tombstoneKey, nil)
		if err != nil {
			t.Fatalf("expected tombstone.json to exist: %v", err)
		}
		reader.Close()

		// Verify state.json has tombstoned=true (read directly from store)
		stateKey := StateKey("test-delete-ns")
		reader, _, err = store.Get(ctx, stateKey, nil)
		if err != nil {
			t.Fatalf("expected state.json to exist: %v", err)
		}
		defer reader.Close()

		data, _ := io.ReadAll(reader)
		var state State
		if err := json.Unmarshal(data, &state); err != nil {
			t.Fatalf("failed to parse state: %v", err)
		}

		if !state.Deletion.Tombstoned {
			t.Error("expected state.deletion.tombstoned to be true")
		}
	})

	t.Run("delete non-existent namespace returns error", func(t *testing.T) {
		err := mgr.DeleteNamespace(ctx, "nonexistent-ns")
		if err == nil {
			t.Error("expected error deleting non-existent namespace")
		}
		if !errors.Is(err, ErrStateNotFound) {
			t.Errorf("expected ErrStateNotFound, got %v", err)
		}
	})

	t.Run("delete already deleted namespace returns error", func(t *testing.T) {
		// Create and delete namespace
		_, err := mgr.Create(ctx, "test-double-delete")
		if err != nil {
			t.Fatalf("failed to create namespace: %v", err)
		}

		err = mgr.DeleteNamespace(ctx, "test-double-delete")
		if err != nil {
			t.Fatalf("failed to delete namespace: %v", err)
		}

		// Try to delete again
		err = mgr.DeleteNamespace(ctx, "test-double-delete")
		if err == nil {
			t.Error("expected error deleting already deleted namespace")
		}
		if !errors.Is(err, ErrNamespaceTombstoned) {
			t.Errorf("expected ErrNamespaceTombstoned, got %v", err)
		}
	})
}

func TestLoadOrCreate(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := NewStateManager(store)

	t.Run("create new namespace", func(t *testing.T) {
		loaded, err := mgr.LoadOrCreate(ctx, "test-loc-1")
		if err != nil {
			t.Fatalf("failed to load or create: %v", err)
		}
		if loaded.State.Namespace != "test-loc-1" {
			t.Errorf("expected namespace 'test-loc-1', got %q", loaded.State.Namespace)
		}
	})

	t.Run("load existing namespace", func(t *testing.T) {
		// Modify the state
		loaded, _ := mgr.Load(ctx, "test-loc-1")
		mgr.Update(ctx, "test-loc-1", loaded.ETag, func(state *State) error {
			state.WAL.HeadSeq++
			return nil
		})

		// LoadOrCreate should return the existing modified state
		loaded2, err := mgr.LoadOrCreate(ctx, "test-loc-1")
		if err != nil {
			t.Fatalf("failed to load or create: %v", err)
		}
		if loaded2.State.WAL.HeadSeq != 1 {
			t.Errorf("expected head_seq 1, got %d", loaded2.State.WAL.HeadSeq)
		}
	})
}

func TestStateClone(t *testing.T) {
	state := NewState("test-clone")
	state.Schema = &Schema{
		Attributes: map[string]AttributeSchema{
			"name": {Type: "string"},
		},
	}

	clone := state.Clone()

	t.Run("clone is independent", func(t *testing.T) {
		clone.Schema.Attributes["age"] = AttributeSchema{Type: "int"}

		if _, exists := state.Schema.Attributes["age"]; exists {
			t.Error("modifying clone should not affect original")
		}
	})

	t.Run("clone has same values", func(t *testing.T) {
		if clone.Namespace != state.Namespace {
			t.Errorf("namespace mismatch: %q vs %q", clone.Namespace, state.Namespace)
		}
		if clone.Schema.Attributes["name"].Type != "string" {
			t.Error("schema not cloned correctly")
		}
	})
}

// containsError checks if err contains target error
func containsError(err, target error) bool {
	if err == nil {
		return false
	}
	return err.Error() == target.Error() ||
		(len(err.Error()) > len(target.Error()) &&
			err.Error()[:len(target.Error())] == target.Error())
}
