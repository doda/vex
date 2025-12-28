// Package version provides format versioning and upgrade strategy tests.
package version

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/gc"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/indexer"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestQueryNodeCanReadN1FormatVersions verifies that query nodes can read N-1 format versions.
// This is the first step of the upgrade strategy.
func TestQueryNodeCanReadN1FormatVersions(t *testing.T) {
	t.Run("WAL decoder reads current version", func(t *testing.T) {
		err := wal.CheckWALFormatVersion(wal.FormatVersion)
		if err != nil {
			t.Errorf("current WAL version should be readable: %v", err)
		}
	})

	t.Run("WAL decoder reads N-1 version when current is 2+", func(t *testing.T) {
		// When FormatVersion is 2+, version 1 should still be readable
		// Currently FormatVersion is 1, so this tests the logic
		minVersion := wal.FormatVersion
		if minVersion > 1 {
			minVersion = wal.FormatVersion - 1
		}
		err := wal.CheckWALFormatVersion(minVersion)
		if err != nil {
			t.Errorf("N-1 WAL version should be readable: %v", err)
		}
	})

	t.Run("WAL decoder rejects version too old", func(t *testing.T) {
		// Version 0 should always be rejected
		err := wal.CheckWALFormatVersion(0)
		if err == nil {
			t.Error("version 0 should be rejected")
		}
	})

	t.Run("WAL decoder rejects version too new", func(t *testing.T) {
		// Version current+1 should be rejected
		err := wal.CheckWALFormatVersion(wal.FormatVersion + 1)
		if err == nil {
			t.Error("version current+1 should be rejected")
		}
	})

	t.Run("manifest validation reads current version", func(t *testing.T) {
		err := index.CheckManifestFormatVersion(index.CurrentManifestVersion)
		if err != nil {
			t.Errorf("current manifest version should be readable: %v", err)
		}
	})

	t.Run("manifest validation reads N-1 version when current is 2+", func(t *testing.T) {
		minVersion := index.MinSupportedManifestVersion
		err := index.CheckManifestFormatVersion(minVersion)
		if err != nil {
			t.Errorf("minimum manifest version should be readable: %v", err)
		}
	})

	t.Run("manifest validation rejects version too old", func(t *testing.T) {
		err := index.CheckManifestFormatVersion(0)
		if err == nil {
			t.Error("version 0 should be rejected")
		}
	})

	t.Run("manifest validation rejects version too new", func(t *testing.T) {
		err := index.CheckManifestFormatVersion(index.CurrentManifestVersion + 1)
		if err == nil {
			t.Error("version current+1 should be rejected")
		}
	})

	t.Run("manifest CanReadVersion helper", func(t *testing.T) {
		if !index.CanReadVersion(index.CurrentManifestVersion) {
			t.Error("should be able to read current version")
		}
		if index.CanReadVersion(0) {
			t.Error("should not be able to read version 0")
		}
		if index.CanReadVersion(index.CurrentManifestVersion + 1) {
			t.Error("should not be able to read future version")
		}
	})
}

// TestIndexerCanBeConfiguredToWriteOldFormat verifies that the indexer can be
// configured to write new or old format. This is the second step of the upgrade strategy.
func TestIndexerCanBeConfiguredToWriteOldFormat(t *testing.T) {
	t.Run("default config uses current WAL version", func(t *testing.T) {
		cfg := indexer.DefaultConfig()
		if cfg.GetWriteWALVersion() != wal.FormatVersion {
			t.Errorf("expected default to use current version %d, got %d",
				wal.FormatVersion, cfg.GetWriteWALVersion())
		}
	})

	t.Run("config can specify old WAL version", func(t *testing.T) {
		cfg := &indexer.IndexerConfig{
			PollInterval:    time.Second,
			WriteWALVersion: 1,
		}
		if cfg.GetWriteWALVersion() != 1 {
			t.Errorf("expected version 1, got %d", cfg.GetWriteWALVersion())
		}
	})

	t.Run("config validates version is supported", func(t *testing.T) {
		cfg := &indexer.IndexerConfig{
			WriteWALVersion: 1,
		}
		err := cfg.ValidateVersionConfig()
		if err != nil {
			t.Errorf("version 1 should be valid: %v", err)
		}
	})

	t.Run("config rejects unsupported version", func(t *testing.T) {
		cfg := &indexer.IndexerConfig{
			WriteWALVersion: 999,
		}
		err := cfg.ValidateVersionConfig()
		if err == nil {
			t.Error("version 999 should be rejected")
		}
	})

	t.Run("config defaults zero version to current", func(t *testing.T) {
		cfg := &indexer.IndexerConfig{
			WriteWALVersion:      0,
			WriteManifestVersion: 0,
		}
		if cfg.GetWriteWALVersion() != wal.FormatVersion {
			t.Errorf("zero should default to current: got %d", cfg.GetWriteWALVersion())
		}
		// Manifest version defaults to 1 when 0
		if cfg.GetWriteManifestVersion() != 1 {
			t.Errorf("zero should default to current: got %d", cfg.GetWriteManifestVersion())
		}
	})
}

// TestGCOfOldFormatSegmentsAfterRetentionWindow verifies that old format segments
// are garbage collected after the retention window. This is step 3 of the upgrade strategy.
func TestGCOfOldFormatSegmentsAfterRetentionWindow(t *testing.T) {
	t.Run("retention duration calculation", func(t *testing.T) {
		duration := gc.GetOldFormatRetentionDuration(7)
		expected := 7 * 24 * time.Hour
		if duration != expected {
			t.Errorf("expected %v, got %v", expected, duration)
		}
	})

	t.Run("retention duration defaults to 7 days for zero", func(t *testing.T) {
		duration := gc.GetOldFormatRetentionDuration(0)
		expected := 7 * 24 * time.Hour
		if duration != expected {
			t.Errorf("expected %v, got %v", expected, duration)
		}
	})

	t.Run("segment not eligible within retention window", func(t *testing.T) {
		createdAt := time.Now().Add(-24 * time.Hour) // 1 day ago
		eligible := gc.IsOldFormatSegmentEligibleForGC(createdAt, 7)
		if eligible {
			t.Error("segment created 1 day ago should not be eligible for 7-day retention")
		}
	})

	t.Run("segment eligible after retention window", func(t *testing.T) {
		createdAt := time.Now().Add(-8 * 24 * time.Hour) // 8 days ago
		eligible := gc.IsOldFormatSegmentEligibleForGC(createdAt, 7)
		if !eligible {
			t.Error("segment created 8 days ago should be eligible for 7-day retention")
		}
	})

	t.Run("policy CanGCManifest with current version", func(t *testing.T) {
		policy := gc.DefaultFormatRetentionPolicy()
		createdAt := time.Now().Add(-30 * 24 * time.Hour) // 30 days ago
		canGC := policy.CanGCManifest(index.CurrentManifestVersion, createdAt)
		if canGC {
			t.Error("current version manifests should never be GCed by format policy")
		}
	})

	t.Run("policy CanGCManifest with old version within retention", func(t *testing.T) {
		policy := gc.DefaultFormatRetentionPolicy()
		policy.CurrentManifestVersion = 2 // Pretend we're at version 2
		createdAt := time.Now().Add(-1 * 24 * time.Hour)
		canGC := policy.CanGCManifest(1, createdAt)
		if canGC {
			t.Error("old version manifest within retention should not be GCed")
		}
	})

	t.Run("policy CanGCManifest with old version after retention", func(t *testing.T) {
		policy := gc.FormatRetentionPolicy{
			RetentionDays:          7,
			CurrentManifestVersion: 2, // Pretend we're at version 2
		}
		createdAt := time.Now().Add(-8 * 24 * time.Hour) // 8 days ago
		canGC := policy.CanGCManifest(1, createdAt)
		if !canGC {
			t.Error("old version manifest after retention should be GCed")
		}
	})

	t.Run("GC config includes retention days", func(t *testing.T) {
		config := gc.DefaultConfig()
		if config.OldFormatRetentionDays != 7 {
			t.Errorf("expected default retention of 7 days, got %d", config.OldFormatRetentionDays)
		}
	})
}

// TestCollectOldFormatSegments tests the actual segment collection logic.
func TestCollectOldFormatSegments(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()

	t.Run("no manifests returns empty result", func(t *testing.T) {
		config := gc.DefaultConfig()
		collector := gc.NewCollector(store, config)

		result, err := collector.CollectOldFormatSegments(ctx, "empty-ns")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.OldFormatManifests != 0 {
			t.Errorf("expected 0 old format manifests, got %d", result.OldFormatManifests)
		}
	})

	t.Run("current version manifest is not collected", func(t *testing.T) {
		// Create a namespace with a current version manifest
		ns := "current-version-ns"
		manifest := index.NewManifest(ns)
		manifest.AddSegment(index.Segment{
			ID:          "seg_current",
			Level:       0,
			StartWALSeq: 1,
			EndWALSeq:   10,
		})

		manifestKey := index.ManifestKey(ns, 1)
		manifestData, _ := json.Marshal(manifest)
		store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)

		config := gc.DefaultConfig()
		collector := gc.NewCollector(store, config)

		result, err := collector.CollectOldFormatSegments(ctx, ns)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.OldFormatManifests != 0 {
			t.Errorf("current version manifest should not be counted as old format")
		}
		if len(result.ManifestsDeleted) != 0 {
			t.Errorf("current version manifest should not be deleted")
		}
	})

	t.Run("old version manifest within retention is not collected", func(t *testing.T) {
		ns := "old-version-recent-ns"

		// Create an old version manifest with recent creation time
		manifest := &index.Manifest{
			FormatVersion: 0, // Old version (pretend version 0 existed)
			Namespace:     ns,
			GeneratedAt:   time.Now().Add(-1 * 24 * time.Hour), // 1 day ago
			Segments: []index.Segment{
				{
					ID:          "seg_old_recent",
					Level:       0,
					StartWALSeq: 1,
					EndWALSeq:   10,
				},
			},
		}

		manifestKey := index.ManifestKey(ns, 1)
		manifestData, _ := json.Marshal(manifest)
		store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)

		config := gc.DefaultConfig()
		collector := gc.NewCollector(store, config)

		result, err := collector.CollectOldFormatSegments(ctx, ns)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.OldFormatManifests != 1 {
			t.Errorf("expected 1 old format manifest, got %d", result.OldFormatManifests)
		}
		if result.SegmentsRetained != 1 {
			t.Errorf("expected 1 segment retained, got %d", result.SegmentsRetained)
		}
		if len(result.ManifestsDeleted) != 0 {
			t.Error("recent manifest should not be deleted")
		}
	})

	t.Run("old version manifest after retention is collected", func(t *testing.T) {
		ns := "old-version-expired-ns"

		// Create an old version manifest with old creation time
		manifest := &index.Manifest{
			FormatVersion: 0, // Old version
			Namespace:     ns,
			GeneratedAt:   time.Now().Add(-30 * 24 * time.Hour), // 30 days ago
			Segments: []index.Segment{
				{
					ID:          "seg_old_expired",
					Level:       0,
					StartWALSeq: 1,
					EndWALSeq:   10,
					DocsKey:     "vex/namespaces/" + ns + "/index/segments/seg_old_expired/docs.bin",
				},
			},
		}

		manifestKey := index.ManifestKey(ns, 1)
		manifestData, _ := json.Marshal(manifest)
		store.Put(ctx, manifestKey, bytes.NewReader(manifestData), int64(len(manifestData)), nil)

		// Also create the segment file
		segKey := "vex/namespaces/" + ns + "/index/segments/seg_old_expired/docs.bin"
		segData := []byte("segment data")
		store.Put(ctx, segKey, bytes.NewReader(segData), int64(len(segData)), nil)

		config := gc.DefaultConfig()
		collector := gc.NewCollector(store, config)

		result, err := collector.CollectOldFormatSegments(ctx, ns)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.OldFormatManifests != 1 {
			t.Errorf("expected 1 old format manifest, got %d", result.OldFormatManifests)
		}
		if len(result.ManifestsDeleted) != 1 {
			t.Errorf("expected 1 manifest deleted, got %d", len(result.ManifestsDeleted))
		}
		if len(result.SegmentsDeleted) != 1 {
			t.Errorf("expected 1 segment deleted, got %d", len(result.SegmentsDeleted))
		}

		// Verify manifest is actually deleted
		_, _, err = store.Get(ctx, manifestKey, nil)
		if !objectstore.IsNotFoundError(err) {
			t.Error("manifest should be deleted from store")
		}

		// Verify segment is actually deleted
		_, _, err = store.Get(ctx, segKey, nil)
		if !objectstore.IsNotFoundError(err) {
			t.Error("segment should be deleted from store")
		}
	})
}

// TestVersionInfrastructureIntegration tests the version infrastructure.
func TestVersionInfrastructureIntegration(t *testing.T) {
	t.Run("SupportedVersions CanRead", func(t *testing.T) {
		sv := SupportedVersions{
			CurrentVersion: 2,
			MinVersion:     1,
		}
		if !sv.CanRead(1) {
			t.Error("should read version 1")
		}
		if !sv.CanRead(2) {
			t.Error("should read version 2")
		}
		if sv.CanRead(0) {
			t.Error("should not read version 0")
		}
		if sv.CanRead(3) {
			t.Error("should not read version 3")
		}
	})

	t.Run("WriteConfig defaults", func(t *testing.T) {
		wc := DefaultWriteConfig()
		if wc.GetWALVersion() != WALFormatVersionCurrent {
			t.Errorf("expected %d, got %d", WALFormatVersionCurrent, wc.GetWALVersion())
		}
		if wc.GetManifestVersion() != ManifestFormatVersionCurrent {
			t.Errorf("expected %d, got %d", ManifestFormatVersionCurrent, wc.GetManifestVersion())
		}
	})

	t.Run("WriteConfig validation accepts valid", func(t *testing.T) {
		wc := WriteConfig{
			WALVersion:      1,
			ManifestVersion: 1,
		}
		if err := wc.Validate(); err != nil {
			t.Errorf("valid config should pass: %v", err)
		}
	})

	t.Run("WriteConfig validation rejects invalid", func(t *testing.T) {
		wc := WriteConfig{
			WALVersion: 999,
		}
		if err := wc.Validate(); err == nil {
			t.Error("invalid WAL version should be rejected")
		}
	})

	t.Run("RetentionConfig CanGC", func(t *testing.T) {
		rc := DefaultRetentionConfig()

		// Current version should not be GCed
		info := SegmentVersionInfo{
			ManifestVersion: ManifestFormatVersionCurrent,
			CreatedAtUnix:   time.Now().Add(-30 * 24 * time.Hour).Unix(),
		}
		if rc.CanGC(info, time.Now().Unix()) {
			t.Error("current version should never be GCed")
		}

		// Old version within retention should not be GCed
		infoRecent := SegmentVersionInfo{
			ManifestVersion: 0, // Pretend old version
			CreatedAtUnix:   time.Now().Add(-1 * 24 * time.Hour).Unix(),
		}
		if rc.CanGC(infoRecent, time.Now().Unix()) {
			t.Error("recent old version should not be GCed")
		}

		// Old version after retention should be GCed
		infoOld := SegmentVersionInfo{
			ManifestVersion: 0,
			CreatedAtUnix:   time.Now().Add(-30 * 24 * time.Hour).Unix(),
		}
		if !rc.CanGC(infoOld, time.Now().Unix()) {
			t.Error("old version after retention should be GCed")
		}
	})

	t.Run("Error types", func(t *testing.T) {
		tooOld := &ErrVersionTooOld{Format: "WAL", Version: 0, MinVersion: 1}
		if tooOld.Error() == "" {
			t.Error("error message should not be empty")
		}

		tooNew := &ErrVersionTooNew{Format: "manifest", Version: 10, CurrentVersion: 1}
		if tooNew.Error() == "" {
			t.Error("error message should not be empty")
		}
	})
}

// TestStateFormatVersionHandling tests namespace state format version handling.
func TestStateFormatVersionHandling(t *testing.T) {
	ctx := context.Background()
	store := objectstore.NewMemoryStore()
	mgr := namespace.NewStateManager(store)

	t.Run("new state uses current format version", func(t *testing.T) {
		loaded, err := mgr.Create(ctx, "test-ns-version")
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		if loaded.State.FormatVersion != namespace.CurrentFormatVersion {
			t.Errorf("expected format version %d, got %d",
				namespace.CurrentFormatVersion, loaded.State.FormatVersion)
		}
	})

	t.Run("state format version survives roundtrip", func(t *testing.T) {
		ns := "test-ns-roundtrip"
		loaded, err := mgr.Create(ctx, ns)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		// Reload and check
		reloaded, err := mgr.Load(ctx, ns)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if reloaded.State.FormatVersion != loaded.State.FormatVersion {
			t.Errorf("format version changed after reload: %d -> %d",
				loaded.State.FormatVersion, reloaded.State.FormatVersion)
		}
	})
}

// TestWALVersionedEncoding tests that WAL entries include version information.
func TestWALVersionedEncoding(t *testing.T) {
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

	t.Run("entry includes format version", func(t *testing.T) {
		entry := wal.NewWalEntry("test-ns", 1)
		if entry.FormatVersion != uint32(wal.FormatVersion) {
			t.Errorf("entry should have format version %d, got %d",
				wal.FormatVersion, entry.FormatVersion)
		}
	})

	t.Run("version survives encode/decode", func(t *testing.T) {
		entry := wal.NewWalEntry("test-ns", 1)
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

		if decoded.FormatVersion != entry.FormatVersion {
			t.Errorf("version changed: %d -> %d", entry.FormatVersion, decoded.FormatVersion)
		}
	})

	t.Run("DecodeWithVersionCheck respects flag", func(t *testing.T) {
		entry := wal.NewWalEntry("test-ns", 1)
		batch := wal.NewWriteSubBatch("req-1")
		batch.AddUpsert(&wal.DocumentID{Id: &wal.DocumentID_U64{U64: 1}}, nil, nil, 0)
		entry.SubBatches = append(entry.SubBatches, batch)

		result, err := encoder.Encode(entry)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}

		// With version check
		_, err = decoder.DecodeWithVersionCheck(result.Data, true)
		if err != nil {
			t.Errorf("decode with version check failed: %v", err)
		}

		// Without version check
		_, err = decoder.DecodeWithVersionCheck(result.Data, false)
		if err != nil {
			t.Errorf("decode without version check failed: %v", err)
		}
	})
}
