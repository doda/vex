package namespace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/vexsearch/vex/internal/metrics"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	ErrStateNotFound       = errors.New("namespace state not found")
	ErrNamespaceTombstoned = errors.New("namespace has been deleted")
	ErrCASRetryExhausted   = errors.New("CAS update failed after max retries")
	ErrWALSeqNotMonotonic  = errors.New("wal.head_seq must increase by exactly 1")
	ErrIndexSeqExceedsWAL  = errors.New("index.indexed_wal_seq cannot exceed wal.head_seq")
	ErrSchemaTypeChange    = errors.New("attribute type cannot be changed once set")
)

const (
	maxCASRetries = 10
	stateKeyFmt   = "vex/namespaces/%s/meta/state.json"
)

// StateManager manages namespace state in object storage with ETag-based CAS.
type StateManager struct {
	store objectstore.Store
}

// NewStateManager creates a new StateManager.
func NewStateManager(store objectstore.Store) *StateManager {
	return &StateManager{store: store}
}

// StateKey returns the object storage key for a namespace's state.
func StateKey(namespace string) string {
	return fmt.Sprintf(stateKeyFmt, namespace)
}

// LoadedState represents a state loaded from storage, including its ETag.
type LoadedState struct {
	State *State
	ETag  string
}

// Load loads the namespace state from object storage.
// Returns ErrStateNotFound if the state doesn't exist.
func (m *StateManager) Load(ctx context.Context, namespace string) (*LoadedState, error) {
	key := StateKey(namespace)
	reader, info, err := m.store.Get(ctx, key, nil)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return nil, ErrStateNotFound
		}
		return nil, fmt.Errorf("failed to load state: %w", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read state: %w", err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state: %w", err)
	}

	if state.Deletion.Tombstoned {
		return nil, ErrNamespaceTombstoned
	}

	return &LoadedState{
		State: &state,
		ETag:  info.ETag,
	}, nil
}

// Create creates a new namespace state if it doesn't exist.
// Uses PutIfAbsent to ensure atomic creation.
func (m *StateManager) Create(ctx context.Context, namespace string) (*LoadedState, error) {
	state := NewState(namespace)
	key := StateKey(namespace)

	data, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state: %w", err)
	}

	info, err := m.store.PutIfAbsent(ctx, key, bytes.NewReader(data), int64(len(data)), &objectstore.PutOptions{
		ContentType: "application/json",
	})
	if err != nil {
		if objectstore.IsConflictError(err) {
			// State already exists, load and return it
			return m.Load(ctx, namespace)
		}
		return nil, fmt.Errorf("failed to create state: %w", err)
	}

	return &LoadedState{
		State: state,
		ETag:  info.ETag,
	}, nil
}

// LoadOrCreate loads existing state or creates new state for a namespace.
func (m *StateManager) LoadOrCreate(ctx context.Context, namespace string) (*LoadedState, error) {
	loaded, err := m.Load(ctx, namespace)
	if err == nil {
		return loaded, nil
	}
	if errors.Is(err, ErrStateNotFound) {
		return m.Create(ctx, namespace)
	}
	return nil, err
}

// UpdateFunc is a function that updates state in place.
// It receives the current state and returns an error if the update should be aborted.
type UpdateFunc func(state *State) error

// Update applies an update function to the namespace state using CAS.
// It retries on ETag mismatch up to maxCASRetries times.
func (m *StateManager) Update(ctx context.Context, namespace string, etag string, update UpdateFunc) (*LoadedState, error) {
	var lastErr error
	currentETag := etag

	for i := 0; i < maxCASRetries; i++ {
		// Load current state if we don't have it or ETag changed
		var loaded *LoadedState
		var err error
		if i > 0 || currentETag == "" {
			loaded, err = m.Load(ctx, namespace)
			if err != nil {
				return nil, err
			}
			currentETag = loaded.ETag
		} else {
			// First iteration with provided ETag - load state
			loaded, err = m.Load(ctx, namespace)
			if err != nil {
				return nil, err
			}
			if loaded.ETag != currentETag {
				currentETag = loaded.ETag
			}
		}

		// Clone state for modification
		newState := loaded.State.Clone()
		if newState == nil {
			return nil, errors.New("failed to clone state")
		}

		// Apply the update
		if err := update(newState); err != nil {
			return nil, err
		}

		// Validate invariants
		if err := m.validateStateUpdate(loaded.State, newState); err != nil {
			return nil, err
		}

		// Update timestamp
		newState.UpdatedAt = time.Now().UTC()

		// Serialize and attempt CAS write
		data, err := json.Marshal(newState)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal state: %w", err)
		}

		key := StateKey(namespace)
		info, err := m.store.PutIfMatch(ctx, key, bytes.NewReader(data), int64(len(data)), currentETag, &objectstore.PutOptions{
			ContentType: "application/json",
		})
		if err != nil {
			if objectstore.IsPreconditionError(err) {
				lastErr = err
				continue // Retry with new ETag
			}
			return nil, fmt.Errorf("failed to update state: %w", err)
		}

		return &LoadedState{
			State: newState,
			ETag:  info.ETag,
		}, nil
	}

	return nil, fmt.Errorf("%w: %v", ErrCASRetryExhausted, lastErr)
}

// validateStateUpdate checks that state invariants are maintained.
func (m *StateManager) validateStateUpdate(old, new *State) error {
	// WAL seq must increase by exactly 1 (if it changes)
	if new.WAL.HeadSeq != old.WAL.HeadSeq {
		if new.WAL.HeadSeq != old.WAL.HeadSeq+1 {
			return fmt.Errorf("%w: old=%d, new=%d", ErrWALSeqNotMonotonic, old.WAL.HeadSeq, new.WAL.HeadSeq)
		}
	}

	// indexed_wal_seq cannot exceed head_seq
	if new.Index.IndexedWALSeq > new.WAL.HeadSeq {
		return fmt.Errorf("%w: indexed_wal_seq=%d, head_seq=%d", ErrIndexSeqExceedsWAL, new.Index.IndexedWALSeq, new.WAL.HeadSeq)
	}

	// Schema type immutability
	if err := validateSchemaTypeImmutability(old.Schema, new.Schema); err != nil {
		return err
	}

	return nil
}

// validateSchemaTypeImmutability ensures attribute types don't change.
func validateSchemaTypeImmutability(old, new *Schema) error {
	if old == nil || old.Attributes == nil {
		return nil
	}
	if new == nil || new.Attributes == nil {
		return nil
	}

	for name, oldAttr := range old.Attributes {
		if newAttr, exists := new.Attributes[name]; exists {
			if oldAttr.Type != newAttr.Type {
				return fmt.Errorf("%w: attribute %q: old type %q, new type %q",
					ErrSchemaTypeChange, name, oldAttr.Type, newAttr.Type)
			}
		}
	}
	return nil
}

// AdvanceWALOptions contains optional parameters for AdvanceWAL.
type AdvanceWALOptions struct {
	SchemaDelta        *Schema
	DisableBackpressure bool
}

// AdvanceWAL advances the WAL head sequence by 1 and updates related fields.
// This is a helper that enforces the strict +1 increment invariant.
func (m *StateManager) AdvanceWAL(ctx context.Context, namespace string, etag string, walKey string, bytesWritten int64, schemaDelta *Schema) (*LoadedState, error) {
	return m.AdvanceWALWithOptions(ctx, namespace, etag, walKey, bytesWritten, AdvanceWALOptions{SchemaDelta: schemaDelta})
}

// AdvanceWALWithOptions advances the WAL head sequence by 1 and updates related fields.
// Supports additional options like disabling backpressure tracking.
func (m *StateManager) AdvanceWALWithOptions(ctx context.Context, namespace string, etag string, walKey string, bytesWritten int64, opts AdvanceWALOptions) (*LoadedState, error) {
	loaded, err := m.Update(ctx, namespace, etag, func(state *State) error {
		state.WAL.HeadSeq++
		state.WAL.HeadKey = walKey
		state.WAL.BytesUnindexedEst += bytesWritten
		if state.WAL.BytesUnindexedEst > 0 {
			state.WAL.Status = "updating"
		}

		// Track if disable_backpressure was used while above threshold
		if opts.DisableBackpressure && state.WAL.BytesUnindexedEst > 2*1024*1024*1024 {
			state.NamespaceFlags.DisableBackpressure = true
		}

		// Apply schema delta if provided
		if opts.SchemaDelta != nil {
			if state.Schema == nil {
				state.Schema = &Schema{Attributes: make(map[string]AttributeSchema)}
			}
			if state.Schema.Attributes == nil {
				state.Schema.Attributes = make(map[string]AttributeSchema)
			}
			for name, attr := range opts.SchemaDelta.Attributes {
				state.Schema.Attributes[name] = attr
			}
		}

		return nil
	})
	if err == nil && loaded != nil {
		metrics.SetTailBytes(namespace, loaded.State.WAL.BytesUnindexedEst)
		metrics.SetIndexLag(namespace, loaded.State.WAL.HeadSeq-loaded.State.Index.IndexedWALSeq)
	}
	return loaded, err
}

// AdvanceIndex advances the index state after a successful index publish.
func (m *StateManager) AdvanceIndex(ctx context.Context, namespace string, etag string, manifestKey string, manifestSeq uint64, indexedWALSeq uint64, bytesIndexed int64) (*LoadedState, error) {
	loaded, err := m.Update(ctx, namespace, etag, func(state *State) error {
		state.Index.ManifestSeq = manifestSeq
		state.Index.ManifestKey = manifestKey
		state.Index.IndexedWALSeq = indexedWALSeq

		// Decrease unindexed bytes estimate
		state.WAL.BytesUnindexedEst -= bytesIndexed
		if state.WAL.BytesUnindexedEst < 0 {
			state.WAL.BytesUnindexedEst = 0
		}

		// Update status
		if state.Index.IndexedWALSeq >= state.WAL.HeadSeq {
			state.Index.Status = "up-to-date"
			state.WAL.Status = "up-to-date"
		} else {
			state.Index.Status = "updating"
		}

		return nil
	})
	if err == nil && loaded != nil {
		metrics.SetTailBytes(namespace, loaded.State.WAL.BytesUnindexedEst)
		metrics.SetIndexLag(namespace, loaded.State.WAL.HeadSeq-loaded.State.Index.IndexedWALSeq)
	}
	return loaded, err
}

// AddPendingRebuild adds a pending index rebuild to track.
func (m *StateManager) AddPendingRebuild(ctx context.Context, namespace string, etag string, kind, attribute string) (*LoadedState, error) {
	return m.Update(ctx, namespace, etag, func(state *State) error {
		// Check if already pending
		for _, pr := range state.Index.PendingRebuilds {
			if pr.Kind == kind && pr.Attribute == attribute && !pr.Ready {
				return nil // Already pending
			}
		}

		state.Index.PendingRebuilds = append(state.Index.PendingRebuilds, PendingRebuild{
			Kind:        kind,
			Attribute:   attribute,
			RequestedAt: time.Now().UTC(),
			Ready:       false,
		})
		return nil
	})
}

// MarkRebuildReady marks a pending rebuild as ready.
func (m *StateManager) MarkRebuildReady(ctx context.Context, namespace string, etag string, kind, attribute string, version int) (*LoadedState, error) {
	return m.Update(ctx, namespace, etag, func(state *State) error {
		for i := range state.Index.PendingRebuilds {
			pr := &state.Index.PendingRebuilds[i]
			if pr.Kind == kind && pr.Attribute == attribute && !pr.Ready {
				pr.Ready = true
				pr.Version = version
				return nil
			}
		}
		return nil // Not found, but not an error
	})
}

// RemovePendingRebuild removes a completed pending rebuild.
func (m *StateManager) RemovePendingRebuild(ctx context.Context, namespace string, etag string, kind, attribute string) (*LoadedState, error) {
	return m.Update(ctx, namespace, etag, func(state *State) error {
		rebuilds := make([]PendingRebuild, 0, len(state.Index.PendingRebuilds))
		for _, pr := range state.Index.PendingRebuilds {
			if pr.Kind != kind || pr.Attribute != attribute {
				rebuilds = append(rebuilds, pr)
			}
		}
		state.Index.PendingRebuilds = rebuilds
		return nil
	})
}

// HasPendingRebuild checks if there's a pending (not ready) rebuild for an attribute.
func HasPendingRebuild(state *State, kind, attribute string) bool {
	for _, pr := range state.Index.PendingRebuilds {
		if pr.Kind == kind && pr.Attribute == attribute && !pr.Ready {
			return true
		}
	}
	return false
}

// SetTombstoned marks the namespace as deleted.
func (m *StateManager) SetTombstoned(ctx context.Context, namespace string, etag string) (*LoadedState, error) {
	return m.Update(ctx, namespace, etag, func(state *State) error {
		now := time.Now().UTC()
		state.Deletion.Tombstoned = true
		state.Deletion.TombstonedAt = &now
		return nil
	})
}

// TombstoneKey returns the object storage key for a namespace's tombstone.
func TombstoneKey(namespace string) string {
	return fmt.Sprintf("vex/namespaces/%s/meta/tombstone.json", namespace)
}

// Tombstone represents the deletion marker for a namespace.
type Tombstone struct {
	DeletedAt time.Time `json:"deleted_at"`
}

// WriteTombstone writes the tombstone.json file for a namespace.
// This is used as a fast rejection mechanism for deleted namespaces.
func (m *StateManager) WriteTombstone(ctx context.Context, namespace string) (*Tombstone, error) {
	key := TombstoneKey(namespace)
	tombstone := &Tombstone{
		DeletedAt: time.Now().UTC(),
	}

	data, err := json.Marshal(tombstone)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tombstone: %w", err)
	}

	// Write tombstone using PutIfAbsent to avoid overwriting an existing tombstone
	_, err = m.store.PutIfAbsent(ctx, key, bytes.NewReader(data), int64(len(data)), &objectstore.PutOptions{
		ContentType: "application/json",
	})
	if err != nil {
		if objectstore.IsConflictError(err) {
			// Tombstone already exists, deletion already in progress
			return tombstone, nil
		}
		return nil, fmt.Errorf("failed to write tombstone: %w", err)
	}

	return tombstone, nil
}

// DeleteNamespace marks a namespace as deleted by writing tombstone.json and updating state.json.
// Returns an error if the namespace doesn't exist or is already deleted.
func (m *StateManager) DeleteNamespace(ctx context.Context, namespace string) error {
	// First, load the state to get the ETag and check if it exists
	loaded, err := m.Load(ctx, namespace)
	if err != nil {
		return err
	}

	// Write tombstone.json first (for fast rejection)
	_, err = m.WriteTombstone(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to write tombstone: %w", err)
	}

	// Update state.json with tombstoned=true
	_, err = m.SetTombstoned(ctx, namespace, loaded.ETag)
	if err != nil {
		return fmt.Errorf("failed to update state: %w", err)
	}

	return nil
}
