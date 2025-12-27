package namespace

import (
	"encoding/json"
	"time"
)

const (
	CurrentFormatVersion = 1
)

// State represents the namespace state stored in meta/state.json
// It is the single authoritative state snapshot used by query nodes and indexers.
type State struct {
	FormatVersion int       `json:"format_version"`
	Namespace     string    `json:"namespace"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`

	Schema        *Schema        `json:"schema,omitempty"`
	Vector        *VectorConfig  `json:"vector,omitempty"`
	WAL           WALState       `json:"wal"`
	Index         IndexState     `json:"index"`
	NamespaceFlags NamespaceFlags `json:"namespace_flags"`
	Deletion      DeletionState  `json:"deletion"`
}

// Schema represents the namespace schema with per-attribute configuration.
type Schema struct {
	Attributes map[string]AttributeSchema `json:"attributes,omitempty"`
}

// AttributeSchema represents the schema for a single attribute.
type AttributeSchema struct {
	Type           string          `json:"type"`
	Filterable     *bool           `json:"filterable,omitempty"`
	Regex          bool            `json:"regex,omitempty"`
	FullTextSearch json.RawMessage `json:"full_text_search,omitempty"`
	Vector         *VectorField    `json:"vector,omitempty"`
}

// VectorField represents vector configuration for a vector attribute.
type VectorField struct {
	Type string `json:"type"`
	ANN  bool   `json:"ann"`
}

// VectorConfig represents the global vector configuration for the namespace.
type VectorConfig struct {
	Dims           int    `json:"dims"`
	DType          string `json:"dtype"`
	DistanceMetric string `json:"distance_metric"`
	ANN            bool   `json:"ann"`
}

// WALState represents the WAL state within namespace state.
type WALState struct {
	HeadSeq           uint64 `json:"head_seq"`
	HeadKey           string `json:"head_key"`
	BytesUnindexedEst int64  `json:"bytes_unindexed_est"`
	Status            string `json:"status"`
}

// IndexState represents the index state within namespace state.
type IndexState struct {
	ManifestSeq      uint64           `json:"manifest_seq"`
	ManifestKey      string           `json:"manifest_key,omitempty"`
	IndexedWALSeq    uint64           `json:"indexed_wal_seq"`
	Status           string           `json:"status"`
	PendingRebuilds  []PendingRebuild `json:"pending_rebuilds,omitempty"`
}

// PendingRebuild represents a pending index rebuild request.
type PendingRebuild struct {
	Kind        string    `json:"kind"`
	Attribute   string    `json:"attribute"`
	RequestedAt time.Time `json:"requested_at"`
	Ready       bool      `json:"ready"`
	Version     int       `json:"version,omitempty"`
}

// NamespaceFlags represents namespace-level flags.
type NamespaceFlags struct {
	DisableBackpressure bool `json:"disable_backpressure"`
}

// DeletionState represents the deletion status of a namespace.
type DeletionState struct {
	Tombstoned   bool       `json:"tombstoned"`
	TombstonedAt *time.Time `json:"tombstoned_at,omitempty"`
}

// NewState creates a new state for a namespace with default values.
func NewState(namespace string) *State {
	now := time.Now().UTC()
	return &State{
		FormatVersion: CurrentFormatVersion,
		Namespace:     namespace,
		CreatedAt:     now,
		UpdatedAt:     now,
		WAL: WALState{
			HeadSeq:           0,
			HeadKey:           "",
			BytesUnindexedEst: 0,
			Status:            "up-to-date",
		},
		Index: IndexState{
			ManifestSeq:   0,
			ManifestKey:   "",
			IndexedWALSeq: 0,
			Status:        "up-to-date",
		},
		NamespaceFlags: NamespaceFlags{
			DisableBackpressure: false,
		},
		Deletion: DeletionState{
			Tombstoned:   false,
			TombstonedAt: nil,
		},
	}
}

// MarshalJSON serializes state to JSON.
func (s *State) MarshalJSON() ([]byte, error) {
	type stateAlias State
	return json.Marshal((*stateAlias)(s))
}

// UnmarshalJSON deserializes state from JSON.
func (s *State) UnmarshalJSON(data []byte) error {
	type stateAlias State
	return json.Unmarshal(data, (*stateAlias)(s))
}

// Clone creates a deep copy of the state.
func (s *State) Clone() *State {
	data, err := json.Marshal(s)
	if err != nil {
		return nil
	}
	var clone State
	if err := json.Unmarshal(data, &clone); err != nil {
		return nil
	}
	return &clone
}
