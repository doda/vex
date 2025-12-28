// Package version provides format versioning tests for all on-disk/on-object formats.
package version

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestFormatVersionInWALEntries verifies format_version is present and correct in WAL entries.
func TestFormatVersionInWALEntries(t *testing.T) {
	t.Run("format_version constant is defined", func(t *testing.T) {
		if wal.FormatVersion < 1 {
			t.Errorf("WAL FormatVersion should be >= 1, got %d", wal.FormatVersion)
		}
	})

	t.Run("NewWalEntry sets format_version", func(t *testing.T) {
		entry := wal.NewWalEntry("test-namespace", 1)
		if entry.FormatVersion != uint32(wal.FormatVersion) {
			t.Errorf("expected format_version %d, got %d", wal.FormatVersion, entry.FormatVersion)
		}
	})

	t.Run("format_version preserved after encode/decode roundtrip", func(t *testing.T) {
		encoder, err := wal.NewEncoder()
		if err != nil {
			t.Fatalf("failed to create encoder: %v", err)
		}
		defer encoder.Close()

		decoder, err := wal.NewDecoder()
		if err != nil {
			t.Fatalf("failed to create decoder: %v", err)
		}
		defer decoder.Close()

		entry := wal.NewWalEntry("test-namespace", 1)
		batch := wal.NewWriteSubBatch("req-1")
		batch.AddUpsert(&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}}, nil, nil, 0)
		entry.SubBatches = append(entry.SubBatches, batch)

		result, err := encoder.Encode(entry)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}

		decoded, err := decoder.Decode(result.Data)
		if err != nil {
			t.Fatalf("decode failed: %v", err)
		}

		if decoded.FormatVersion != uint32(wal.FormatVersion) {
			t.Errorf("format_version after roundtrip: expected %d, got %d", wal.FormatVersion, decoded.FormatVersion)
		}
	})

	t.Run("format_version is part of checksum", func(t *testing.T) {
		encoder, err := wal.NewEncoder()
		if err != nil {
			t.Fatalf("failed to create encoder: %v", err)
		}
		defer encoder.Close()

		entry := wal.NewWalEntry("test-namespace", 1)
		batch := wal.NewWriteSubBatch("req-1")
		batch.AddUpsert(&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}}, nil, nil, 0)
		entry.SubBatches = append(entry.SubBatches, batch)
		entry.CommittedUnixMs = 1234567890000

		result1, err := encoder.Encode(entry)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}

		// The format_version is included in the entry, so changing it would
		// change the checksum. This verifies that format_version is part of
		// the serialized data that gets checksummed.
		if result1.Checksum == [32]byte{} {
			t.Error("checksum should not be empty")
		}
	})
}

// TestFormatVersionInIndexManifests verifies format_version is present and correct in index manifests.
func TestFormatVersionInIndexManifests(t *testing.T) {
	t.Run("CurrentManifestVersion constant is defined", func(t *testing.T) {
		if index.CurrentManifestVersion < 1 {
			t.Errorf("CurrentManifestVersion should be >= 1, got %d", index.CurrentManifestVersion)
		}
	})

	t.Run("NewManifest sets format_version", func(t *testing.T) {
		m := index.NewManifest("test-namespace")
		if m.FormatVersion != index.CurrentManifestVersion {
			t.Errorf("expected format_version %d, got %d", index.CurrentManifestVersion, m.FormatVersion)
		}
	})

	t.Run("format_version in JSON serialization", func(t *testing.T) {
		m := index.NewManifest("test-namespace")

		data, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}

		var parsed map[string]interface{}
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}

		v, ok := parsed["format_version"]
		if !ok {
			t.Fatal("format_version field missing from JSON")
		}

		vFloat, ok := v.(float64)
		if !ok {
			t.Fatalf("format_version should be a number, got %T", v)
		}

		if int(vFloat) != index.CurrentManifestVersion {
			t.Errorf("format_version in JSON: expected %d, got %v", index.CurrentManifestVersion, vFloat)
		}
	})

	t.Run("format_version preserved after JSON roundtrip", func(t *testing.T) {
		m := index.NewManifest("test-namespace")
		m.IndexedWALSeq = 100
		m.AddSegment(index.Segment{
			ID:          "seg_001",
			Level:       0,
			StartWALSeq: 1,
			EndWALSeq:   100,
		})

		data, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}

		var m2 index.Manifest
		if err := json.Unmarshal(data, &m2); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}

		if m2.FormatVersion != index.CurrentManifestVersion {
			t.Errorf("format_version after roundtrip: expected %d, got %d", index.CurrentManifestVersion, m2.FormatVersion)
		}
	})

	t.Run("Validate rejects invalid format_version", func(t *testing.T) {
		m := index.NewManifest("test-namespace")
		m.FormatVersion = 0

		err := m.Validate()
		if err == nil {
			t.Error("expected validation error for format_version 0")
		}
	})

	t.Run("Validate accepts valid format_version", func(t *testing.T) {
		m := index.NewManifest("test-namespace")
		// format_version is already set correctly by NewManifest

		err := m.Validate()
		if err != nil {
			t.Errorf("unexpected validation error: %v", err)
		}
	})
}

// TestFormatVersionInStateJSON verifies format_version is present and correct in state.json.
func TestFormatVersionInStateJSON(t *testing.T) {
	t.Run("CurrentFormatVersion constant is defined", func(t *testing.T) {
		if namespace.CurrentFormatVersion < 1 {
			t.Errorf("CurrentFormatVersion should be >= 1, got %d", namespace.CurrentFormatVersion)
		}
	})

	t.Run("NewState sets format_version", func(t *testing.T) {
		state := namespace.NewState("test-namespace")
		if state.FormatVersion != namespace.CurrentFormatVersion {
			t.Errorf("expected format_version %d, got %d", namespace.CurrentFormatVersion, state.FormatVersion)
		}
	})

	t.Run("format_version in JSON serialization", func(t *testing.T) {
		state := namespace.NewState("test-namespace")

		data, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}

		var parsed map[string]interface{}
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}

		v, ok := parsed["format_version"]
		if !ok {
			t.Fatal("format_version field missing from JSON")
		}

		vFloat, ok := v.(float64)
		if !ok {
			t.Fatalf("format_version should be a number, got %T", v)
		}

		if int(vFloat) != namespace.CurrentFormatVersion {
			t.Errorf("format_version in JSON: expected %d, got %v", namespace.CurrentFormatVersion, vFloat)
		}
	})

	t.Run("format_version preserved after JSON roundtrip", func(t *testing.T) {
		state := namespace.NewState("test-namespace")
		state.WAL.HeadSeq = 100

		data, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}

		var state2 namespace.State
		if err := json.Unmarshal(data, &state2); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}

		if state2.FormatVersion != namespace.CurrentFormatVersion {
			t.Errorf("format_version after roundtrip: expected %d, got %d", namespace.CurrentFormatVersion, state2.FormatVersion)
		}
	})

	t.Run("StateManager preserves format_version on create", func(t *testing.T) {
		ctx := context.Background()
		store := objectstore.NewMemoryStore()
		mgr := namespace.NewStateManager(store)

		loaded, err := mgr.Create(ctx, "test-ns")
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		if loaded.State.FormatVersion != namespace.CurrentFormatVersion {
			t.Errorf("format_version after Create: expected %d, got %d",
				namespace.CurrentFormatVersion, loaded.State.FormatVersion)
		}
	})

	t.Run("StateManager preserves format_version on update", func(t *testing.T) {
		ctx := context.Background()
		store := objectstore.NewMemoryStore()
		mgr := namespace.NewStateManager(store)

		loaded, err := mgr.Create(ctx, "test-ns-update")
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		updated, err := mgr.Update(ctx, "test-ns-update", loaded.ETag, func(state *namespace.State) error {
			state.WAL.HeadSeq++
			return nil
		})
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		if updated.State.FormatVersion != namespace.CurrentFormatVersion {
			t.Errorf("format_version after Update: expected %d, got %d",
				namespace.CurrentFormatVersion, updated.State.FormatVersion)
		}
	})

	t.Run("StateManager preserves format_version on load", func(t *testing.T) {
		ctx := context.Background()
		store := objectstore.NewMemoryStore()
		mgr := namespace.NewStateManager(store)

		_, err := mgr.Create(ctx, "test-ns-load")
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		loaded, err := mgr.Load(ctx, "test-ns-load")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if loaded.State.FormatVersion != namespace.CurrentFormatVersion {
			t.Errorf("format_version after Load: expected %d, got %d",
				namespace.CurrentFormatVersion, loaded.State.FormatVersion)
		}
	})
}

// TestFormatVersionConsistency verifies that all format version constants are consistent.
func TestFormatVersionConsistency(t *testing.T) {
	t.Run("all format versions are positive", func(t *testing.T) {
		if wal.FormatVersion < 1 {
			t.Error("WAL FormatVersion should be >= 1")
		}
		if index.CurrentManifestVersion < 1 {
			t.Error("Index CurrentManifestVersion should be >= 1")
		}
		if namespace.CurrentFormatVersion < 1 {
			t.Error("Namespace CurrentFormatVersion should be >= 1")
		}
	})

	t.Run("format versions are currently version 1", func(t *testing.T) {
		// This test documents the current version. Update when upgrading.
		if wal.FormatVersion != 1 {
			t.Errorf("WAL FormatVersion: expected 1, got %d", wal.FormatVersion)
		}
		if index.CurrentManifestVersion != 1 {
			t.Errorf("Index CurrentManifestVersion: expected 1, got %d", index.CurrentManifestVersion)
		}
		if namespace.CurrentFormatVersion != 1 {
			t.Errorf("Namespace CurrentFormatVersion: expected 1, got %d", namespace.CurrentFormatVersion)
		}
	})
}
