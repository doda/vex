package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	ErrStateUpdateFailed = errors.New("state update failed after maximum retries")
	ErrWALSeqConflict    = errors.New("WAL sequence already exists but state does not reference it")
	ErrWALUploadFailed   = errors.New("WAL upload failed")
)

const (
	maxCommitRetries = 10
)

// Committer handles WAL commit protocol with object storage durability.
// It ensures that WAL entries are committed idempotently and state is updated
// atomically using CAS (Compare-And-Swap).
type Committer struct {
	store        objectstore.Store
	stateManager *namespace.StateManager
	encoder      *Encoder
}

// NewCommitter creates a new WAL committer.
func NewCommitter(store objectstore.Store, stateManager *namespace.StateManager) (*Committer, error) {
	enc, err := NewEncoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create encoder: %w", err)
	}
	return &Committer{
		store:        store,
		stateManager: stateManager,
		encoder:      enc,
	}, nil
}

// Close releases resources held by the committer.
func (c *Committer) Close() error {
	return c.encoder.Close()
}

// CommitResult contains the result of a successful WAL commit.
type CommitResult struct {
	Seq         uint64
	WALKey      string
	ETag        string
	State       *namespace.State
	BytesWritten int64
}

// Commit commits a WAL entry to object storage and updates namespace state.
// It implements the full commit protocol:
// 1. Load state.json (capture ETag)
// 2. Choose seq = state.wal.head_seq + 1
// 3. Serialize WAL entry, compute checksum, compress
// 4. PUT wal/<seq>.wal.zst with If-None-Match: "*" (idempotent)
// 5. Update state.json with If-Match: <old-etag> (CAS)
// 6. On CAS failure, retry state update (don't re-upload WAL)
func (c *Committer) Commit(ctx context.Context, ns string, entry *WalEntry, schemaDelta *namespace.Schema) (*CommitResult, error) {
	// Step 1: Load or create namespace state
	loaded, err := c.stateManager.LoadOrCreate(ctx, ns)
	if err != nil {
		return nil, fmt.Errorf("failed to load namespace state: %w", err)
	}

	// Step 2: Calculate next sequence number
	seq := loaded.State.WAL.HeadSeq + 1

	// Update entry with correct seq and namespace
	entry.Namespace = ns
	entry.Seq = seq

	// Step 3: Encode the WAL entry (serialize + checksum + compress)
	encResult, err := c.encoder.Encode(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to encode WAL entry: %w", err)
	}

	// Step 4: Build the WAL key
	walKey := walKeyForNamespace(ns, seq)

	// Step 5: Upload WAL with If-None-Match: * for idempotency
	var walETag string
	walInfo, err := c.uploadWAL(ctx, walKey, encResult.Data)
	if err != nil {
		if objectstore.IsConflictError(err) {
			// WAL already exists - ensure it's the same entry before proceeding.
			if confirmErr := c.confirmExistingWAL(ctx, walKey, encResult.Data); confirmErr != nil {
				return nil, fmt.Errorf("%w: %v", ErrWALSeqConflict, confirmErr)
			}
		} else {
			return nil, fmt.Errorf("%w: %v", ErrWALUploadFailed, err)
		}
	} else {
		walETag = walInfo.ETag
	}

	// Step 6-7: Update state.json with CAS retry loop
	bytesWritten := int64(len(encResult.Data))
	walKeyRelative := KeyForSeq(seq)

	result, err := c.updateStateWithRetry(ctx, ns, loaded.ETag, walKeyRelative, bytesWritten, schemaDelta)
	if err != nil {
		return nil, err
	}

	return &CommitResult{
		Seq:          seq,
		WALKey:       walKey,
		ETag:         walETag,
		State:        result.State,
		BytesWritten: bytesWritten,
	}, nil
}

// uploadWAL uploads the WAL entry to object storage with If-None-Match: * for idempotency.
func (c *Committer) uploadWAL(ctx context.Context, key string, data []byte) (*objectstore.ObjectInfo, error) {
	info, err := c.store.PutIfAbsent(ctx, key, bytes.NewReader(data), int64(len(data)), &objectstore.PutOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return nil, err
	}
	return info, nil
}

// updateStateWithRetry updates state.json with CAS semantics and retries on ETag mismatch.
// Critical: WAL is not re-uploaded on state retry - it's already idempotent.
func (c *Committer) updateStateWithRetry(ctx context.Context, ns string, etag string, walKey string, bytesWritten int64, schemaDelta *namespace.Schema) (*namespace.LoadedState, error) {
	currentETag := etag
	var lastErr error

	for i := 0; i < maxCommitRetries; i++ {
		result, err := c.stateManager.AdvanceWAL(ctx, ns, currentETag, walKey, bytesWritten, schemaDelta)
		if err == nil {
			return result, nil
		}

		if !objectstore.IsPreconditionError(err) && !errors.Is(err, namespace.ErrCASRetryExhausted) {
			return nil, err
		}

		// ETag mismatch - reload state and retry
		loaded, loadErr := c.stateManager.Load(ctx, ns)
		if loadErr != nil {
			return nil, fmt.Errorf("failed to reload state during CAS retry: %w", loadErr)
		}

		// Check if state was already updated (e.g., by another writer or previous retry)
		if loaded.State.WAL.HeadKey == walKey {
			// State already points to our WAL entry, success!
			return loaded, nil
		}

		// Calculate what seq should be based on current state
		expectedSeq := loaded.State.WAL.HeadSeq + 1
		currentSeq := parseSeqFromKey(walKey)

		if currentSeq <= loaded.State.WAL.HeadSeq {
			// Our WAL entry is for an old seq that's already been superseded
			// This shouldn't happen in normal operation but can occur with concurrent writers
			return nil, fmt.Errorf("%w: WAL seq %d already committed (head_seq=%d)",
				ErrWALSeqConflict, currentSeq, loaded.State.WAL.HeadSeq)
		}

		if currentSeq != expectedSeq {
			// The sequence has moved beyond what we expected
			// Another writer has committed in between
			return nil, fmt.Errorf("%w: expected seq %d but got %d",
				ErrWALSeqConflict, expectedSeq, currentSeq)
		}

		currentETag = loaded.ETag
		lastErr = err
	}

	return nil, fmt.Errorf("%w: last error: %v", ErrStateUpdateFailed, lastErr)
}

// walKeyForNamespace constructs the full object storage key for a WAL entry.
func walKeyForNamespace(ns string, seq uint64) string {
	return fmt.Sprintf("vex/namespaces/%s/%s", ns, KeyForSeq(seq))
}

// parseSeqFromKey extracts the sequence number from a WAL key.
func parseSeqFromKey(key string) uint64 {
	var seq uint64
	// Key format: wal/<seq>.wal.zst
	_, err := fmt.Sscanf(key, "wal/%d"+FileExtension, &seq)
	if err != nil {
		return 0
	}
	return seq
}

// VerifyWALExists checks if a WAL entry exists for the given sequence.
func (c *Committer) VerifyWALExists(ctx context.Context, ns string, seq uint64) (bool, error) {
	key := walKeyForNamespace(ns, seq)
	_, err := c.store.Head(ctx, key)
	if err != nil {
		if objectstore.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ReadWAL reads and decodes a WAL entry from object storage.
func (c *Committer) ReadWAL(ctx context.Context, ns string, seq uint64) (*WalEntry, error) {
	key := walKeyForNamespace(ns, seq)
	reader, _, err := c.store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL data: %w", err)
	}

	decoder, err := NewDecoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create decoder: %w", err)
	}
	defer decoder.Close()

	entry, err := decoder.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode WAL entry: %w", err)
	}

	return entry, nil
}

func (c *Committer) confirmExistingWAL(ctx context.Context, key string, expected []byte) error {
	reader, _, err := c.store.Get(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("failed to read existing WAL: %w", err)
	}
	defer reader.Close()

	existing, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read existing WAL data: %w", err)
	}

	if !bytes.Equal(existing, expected) {
		return errors.New("existing WAL content does not match")
	}

	return nil
}
