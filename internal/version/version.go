// Package version provides format versioning and upgrade strategy support.
// It implements N-1 version compatibility for safe rolling upgrades.
//
// Upgrade Strategy (from SPEC.md Section 20):
// 1. Deploy new query nodes supporting both old and new formats
// 2. Deploy new indexers that start writing new format
// 3. After retention window, GC old format segments
package version

import (
	"fmt"
)

// Format versions for various on-disk/on-object formats.
const (
	// WALFormatVersionCurrent is the current WAL format version.
	WALFormatVersionCurrent = 1
	// WALFormatVersionMin is the minimum WAL version this node can read.
	WALFormatVersionMin = 1

	// ManifestFormatVersionCurrent is the current index manifest format version.
	ManifestFormatVersionCurrent = 1
	// ManifestFormatVersionMin is the minimum manifest version this node can read.
	ManifestFormatVersionMin = 1

	// StateFormatVersionCurrent is the current state.json format version.
	StateFormatVersionCurrent = 1
	// StateFormatVersionMin is the minimum state version this node can read.
	StateFormatVersionMin = 1

	// FilterIndexFormatVersionCurrent is the current filter index format version.
	FilterIndexFormatVersionCurrent = 1
	// FilterIndexFormatVersionMin is the minimum filter index version this node can read.
	FilterIndexFormatVersionMin = 1
)

// SupportedVersions tracks which versions are readable by query nodes.
// For N-1 compatibility, min = current - 1.
type SupportedVersions struct {
	CurrentVersion int
	MinVersion     int
}

// WALVersions returns the supported WAL format versions.
func WALVersions() SupportedVersions {
	return SupportedVersions{
		CurrentVersion: WALFormatVersionCurrent,
		MinVersion:     WALFormatVersionMin,
	}
}

// ManifestVersions returns the supported manifest format versions.
func ManifestVersions() SupportedVersions {
	return SupportedVersions{
		CurrentVersion: ManifestFormatVersionCurrent,
		MinVersion:     ManifestFormatVersionMin,
	}
}

// StateVersions returns the supported state format versions.
func StateVersions() SupportedVersions {
	return SupportedVersions{
		CurrentVersion: StateFormatVersionCurrent,
		MinVersion:     StateFormatVersionMin,
	}
}

// FilterIndexVersions returns the supported filter index format versions.
func FilterIndexVersions() SupportedVersions {
	return SupportedVersions{
		CurrentVersion: FilterIndexFormatVersionCurrent,
		MinVersion:     FilterIndexFormatVersionMin,
	}
}

// CanRead returns true if the given version is readable by this node.
func (sv SupportedVersions) CanRead(version int) bool {
	return version >= sv.MinVersion && version <= sv.CurrentVersion
}

// ErrVersionTooOld indicates a format version is older than the minimum supported.
type ErrVersionTooOld struct {
	Format     string
	Version    int
	MinVersion int
}

func (e *ErrVersionTooOld) Error() string {
	return fmt.Sprintf("%s format version %d is too old (minimum: %d)", e.Format, e.Version, e.MinVersion)
}

// ErrVersionTooNew indicates a format version is newer than this node can read.
type ErrVersionTooNew struct {
	Format         string
	Version        int
	CurrentVersion int
}

func (e *ErrVersionTooNew) Error() string {
	return fmt.Sprintf("%s format version %d is too new for this node (current: %d)", e.Format, e.Version, e.CurrentVersion)
}

// CheckWALVersion validates a WAL format version is readable.
func CheckWALVersion(version int) error {
	sv := WALVersions()
	if version < sv.MinVersion {
		return &ErrVersionTooOld{Format: "WAL", Version: version, MinVersion: sv.MinVersion}
	}
	if version > sv.CurrentVersion {
		return &ErrVersionTooNew{Format: "WAL", Version: version, CurrentVersion: sv.CurrentVersion}
	}
	return nil
}

// CheckManifestVersion validates a manifest format version is readable.
func CheckManifestVersion(version int) error {
	sv := ManifestVersions()
	if version < sv.MinVersion {
		return &ErrVersionTooOld{Format: "manifest", Version: version, MinVersion: sv.MinVersion}
	}
	if version > sv.CurrentVersion {
		return &ErrVersionTooNew{Format: "manifest", Version: version, CurrentVersion: sv.CurrentVersion}
	}
	return nil
}

// CheckStateVersion validates a state format version is readable.
func CheckStateVersion(version int) error {
	sv := StateVersions()
	if version < sv.MinVersion {
		return &ErrVersionTooOld{Format: "state", Version: version, MinVersion: sv.MinVersion}
	}
	if version > sv.CurrentVersion {
		return &ErrVersionTooNew{Format: "state", Version: version, CurrentVersion: sv.CurrentVersion}
	}
	return nil
}

// CheckFilterIndexVersion validates a filter index format version is readable.
func CheckFilterIndexVersion(version int) error {
	sv := FilterIndexVersions()
	if version < sv.MinVersion {
		return &ErrVersionTooOld{Format: "filter index", Version: version, MinVersion: sv.MinVersion}
	}
	if version > sv.CurrentVersion {
		return &ErrVersionTooNew{Format: "filter index", Version: version, CurrentVersion: sv.CurrentVersion}
	}
	return nil
}

// WriteConfig controls which format version an indexer should write.
type WriteConfig struct {
	// WALVersion is the WAL format version to write.
	// If 0, uses the current version.
	WALVersion int

	// ManifestVersion is the manifest format version to write.
	// If 0, uses the current version.
	ManifestVersion int

	// StateVersion is the state format version to write.
	// If 0, uses the current version.
	StateVersion int

	// FilterIndexVersion is the filter index format version to write.
	// If 0, uses the current version.
	FilterIndexVersion int
}

// DefaultWriteConfig returns a WriteConfig that writes the current format versions.
func DefaultWriteConfig() WriteConfig {
	return WriteConfig{
		WALVersion:         WALFormatVersionCurrent,
		ManifestVersion:    ManifestFormatVersionCurrent,
		StateVersion:       StateFormatVersionCurrent,
		FilterIndexVersion: FilterIndexFormatVersionCurrent,
	}
}

// GetWALVersion returns the WAL version to write, defaulting to current.
func (wc WriteConfig) GetWALVersion() int {
	if wc.WALVersion == 0 {
		return WALFormatVersionCurrent
	}
	return wc.WALVersion
}

// GetManifestVersion returns the manifest version to write, defaulting to current.
func (wc WriteConfig) GetManifestVersion() int {
	if wc.ManifestVersion == 0 {
		return ManifestFormatVersionCurrent
	}
	return wc.ManifestVersion
}

// GetStateVersion returns the state version to write, defaulting to current.
func (wc WriteConfig) GetStateVersion() int {
	if wc.StateVersion == 0 {
		return StateFormatVersionCurrent
	}
	return wc.StateVersion
}

// GetFilterIndexVersion returns the filter index version to write, defaulting to current.
func (wc WriteConfig) GetFilterIndexVersion() int {
	if wc.FilterIndexVersion == 0 {
		return FilterIndexFormatVersionCurrent
	}
	return wc.FilterIndexVersion
}

// Validate checks that the WriteConfig only specifies writable versions.
func (wc WriteConfig) Validate() error {
	walVersions := WALVersions()
	if wc.WALVersion != 0 && !walVersions.CanRead(wc.WALVersion) {
		return fmt.Errorf("WAL version %d is not supported (range: %d-%d)",
			wc.WALVersion, walVersions.MinVersion, walVersions.CurrentVersion)
	}

	manifestVersions := ManifestVersions()
	if wc.ManifestVersion != 0 && !manifestVersions.CanRead(wc.ManifestVersion) {
		return fmt.Errorf("manifest version %d is not supported (range: %d-%d)",
			wc.ManifestVersion, manifestVersions.MinVersion, manifestVersions.CurrentVersion)
	}

	stateVersions := StateVersions()
	if wc.StateVersion != 0 && !stateVersions.CanRead(wc.StateVersion) {
		return fmt.Errorf("state version %d is not supported (range: %d-%d)",
			wc.StateVersion, stateVersions.MinVersion, stateVersions.CurrentVersion)
	}

	filterVersions := FilterIndexVersions()
	if wc.FilterIndexVersion != 0 && !filterVersions.CanRead(wc.FilterIndexVersion) {
		return fmt.Errorf("filter index version %d is not supported (range: %d-%d)",
			wc.FilterIndexVersion, filterVersions.MinVersion, filterVersions.CurrentVersion)
	}

	return nil
}

// RetentionConfig controls GC behavior for old format versions.
type RetentionConfig struct {
	// MinFormatRetention is the minimum time to retain segments with old format versions.
	// This allows time for rolling upgrades to complete.
	MinFormatRetention int64 // seconds
}

// DefaultRetentionConfig returns the default GC retention configuration.
// Old format segments are retained for at least 7 days to allow safe upgrades.
func DefaultRetentionConfig() RetentionConfig {
	return RetentionConfig{
		MinFormatRetention: 7 * 24 * 60 * 60, // 7 days in seconds
	}
}

// SegmentVersionInfo contains version metadata for a segment.
type SegmentVersionInfo struct {
	ManifestVersion int
	CreatedAtUnix   int64
}

// CanGC returns true if a segment with old format can be garbage collected.
func (rc RetentionConfig) CanGC(info SegmentVersionInfo, nowUnix int64) bool {
	currentVersion := ManifestFormatVersionCurrent

	// Current version segments are not eligible for format-based GC.
	if info.ManifestVersion >= currentVersion {
		return false
	}

	// Check if the segment has been retained long enough.
	age := nowUnix - info.CreatedAtUnix
	return age >= rc.MinFormatRetention
}
