// Package write implements the write path for Vex, handling document upserts,
// patches, and deletes with WAL commit to object storage.
package write

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/metrics"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultBatchWindow is the maximum time to wait before committing a batch.
	DefaultBatchWindow = 1 * time.Second

	// DefaultMaxBatchSize is the maximum uncompressed size of a batch before forcing commit.
	// 100 MB is a reasonable default to limit memory usage while allowing good batching.
	DefaultMaxBatchSize = 100 * 1024 * 1024
)

var (
	ErrBatcherClosed = errors.New("batcher is closed")
)

// BatcherConfig configures the write batcher.
type BatcherConfig struct {
	// BatchWindow is the maximum time to wait before committing a batch.
	// Default: 1 second.
	BatchWindow time.Duration

	// MaxBatchSize is the maximum accumulated size of pending writes before
	// forcing a commit. Default: 100 MB.
	MaxBatchSize int64
}

// DefaultBatcherConfig returns the default batcher configuration.
func DefaultBatcherConfig() BatcherConfig {
	return BatcherConfig{
		BatchWindow:  DefaultBatchWindow,
		MaxBatchSize: DefaultMaxBatchSize,
	}
}

// pendingWrite represents a write request waiting to be batched.
type pendingWrite struct {
	ctx       context.Context
	namespace string
	req       *WriteRequest
	result    chan batchResult
	reserved  bool // true if this request has an idempotency reservation
}

// batchResult contains the result of a batched write.
type batchResult struct {
	resp *WriteResponse
	err  error
}

// namespaceBatch collects writes for a single namespace.
type namespaceBatch struct {
	namespace string
	writes    []*pendingWrite
	timer     *time.Timer
	startTime time.Time
	size      int64 // estimated uncompressed size
	flushed   bool  // set to true once flush starts, prevents double-flush
}

type namespaceCommitState struct {
	mu         sync.Mutex
	lastCommit time.Time
}

// Batcher batches write requests per namespace, committing at most 1 WAL entry per second.
type Batcher struct {
	cfg         BatcherConfig
	store       objectstore.Store
	stateMan    *namespace.StateManager
	canon       *wal.Canonicalizer
	tailStore   tail.Store
	indexReader *index.Reader
	idempotency *IdempotencyStore
	compatMode  string // Compatibility mode: "turbopuffer" or "vex"

	mu           sync.Mutex
	batches      map[string]*namespaceBatch // namespace -> pending batch
	commitStates map[string]*namespaceCommitState
	closed       bool
	closeWg      sync.WaitGroup
}

// NewBatcher creates a new write batcher.
func NewBatcher(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store) (*Batcher, error) {
	return NewBatcherWithConfig(store, stateMan, tailStore, DefaultBatcherConfig())
}

// NewBatcherWithCompatMode creates a new write batcher with a specific compatibility mode.
func NewBatcherWithCompatMode(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store, compatMode string) (*Batcher, error) {
	return NewBatcherWithConfigAndCompatMode(store, stateMan, tailStore, DefaultBatcherConfig(), compatMode)
}

// NewBatcherWithConfig creates a new write batcher with custom configuration.
func NewBatcherWithConfig(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store, cfg BatcherConfig) (*Batcher, error) {
	return NewBatcherWithConfigAndCompatMode(store, stateMan, tailStore, cfg, "turbopuffer")
}

// NewBatcherWithConfigAndCompatMode creates a new write batcher with custom configuration and compat mode.
func NewBatcherWithConfigAndCompatMode(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store, cfg BatcherConfig, compatMode string) (*Batcher, error) {
	if cfg.BatchWindow <= 0 {
		cfg.BatchWindow = DefaultBatchWindow
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = DefaultMaxBatchSize
	}

	return &Batcher{
		cfg:          cfg,
		store:        store,
		stateMan:     stateMan,
		canon:        wal.NewCanonicalizer(),
		tailStore:    tailStore,
		indexReader:  index.NewReader(store, nil, nil),
		idempotency:  NewIdempotencyStore(),
		batches:      make(map[string]*namespaceBatch),
		commitStates: make(map[string]*namespaceCommitState),
		compatMode:   compatMode,
	}, nil
}

// CompatMode returns the compatibility mode of the batcher.
func (b *Batcher) CompatMode() string {
	return b.compatMode
}

// Close shuts down the batcher, flushing any pending batches.
func (b *Batcher) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true

	// Stop all timers and flush remaining batches
	for ns, batch := range b.batches {
		if batch.timer != nil {
			batch.timer.Stop()
		}
		delete(b.batches, ns)
		// Flush in a goroutine to avoid holding the lock
		b.closeWg.Add(1)
		go func(batch *namespaceBatch) {
			defer b.closeWg.Done()
			b.flushBatch(batch)
		}(batch)
	}
	b.mu.Unlock()

	// Wait for all pending flushes to complete
	b.closeWg.Wait()

	// Close idempotency store
	if b.idempotency != nil {
		b.idempotency.Close()
	}

	return nil
}

// Submit submits a write request for batching.
// It blocks until the write is committed or an error occurs.
// If a duplicate request_id is detected (cached or in-flight), returns the original response.
func (b *Batcher) Submit(ctx context.Context, ns string, req *WriteRequest) (*WriteResponse, error) {
	// Check for duplicate request_id and reserve if not
	reserved := false
	if req.RequestID != "" {
		result, cached, inflight := b.idempotency.Reserve(ns, req.RequestID)
		switch result {
		case ReserveCached:
			return cached, nil
		case ReserveInFlight:
			// Wait for the in-flight request to complete (respects ctx cancellation)
			resp, err := b.idempotency.WaitForInFlight(ctx, inflight)
			if resp != nil {
				return resp, nil
			}
			if err != nil {
				return nil, err
			}
			// Original request failed with no error, retry by reserving again
			return b.Submit(ctx, ns, req)
		case ReserveOK:
			// We have the reservation, proceed with batching
			reserved = true
		}
	}

	resultCh := make(chan batchResult, 1)

	pw := &pendingWrite{
		ctx:       ctx,
		namespace: ns,
		req:       req,
		result:    resultCh,
		reserved:  reserved,
	}

	// Estimate the size of this write for batching decisions
	size := b.estimateWriteSize(req)

	var batchToFlush *namespaceBatch

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		if reserved {
			b.idempotency.Release(ns, req.RequestID, ErrBatcherClosed)
		}
		return nil, ErrBatcherClosed
	}

	batch, exists := b.batches[ns]
	if !exists {
		// Start a new batch for this namespace
		batch = &namespaceBatch{
			namespace: ns,
			writes:    make([]*pendingWrite, 0, 16),
			startTime: time.Now(),
		}
		b.batches[ns] = batch

		// Start the timer for this batch
		timer := time.AfterFunc(b.cfg.BatchWindow, func() {
			b.timerFlush(ns)
		})
		batch.timer = timer
	}

	batch.writes = append(batch.writes, pw)
	batch.size += size

	// Check if we should force flush due to size
	if batch.size >= b.cfg.MaxBatchSize && !batch.flushed {
		// Mark as flushed BEFORE releasing the lock to prevent timer from also flushing
		batch.flushed = true
		// Stop the timer - even if it already fired, the flushed flag prevents double-flush
		if batch.timer != nil {
			batch.timer.Stop()
		}
		delete(b.batches, ns)
		batchToFlush = batch
	}
	b.mu.Unlock()

	// If size threshold reached, flush immediately (outside the lock)
	if batchToFlush != nil {
		b.flushBatch(batchToFlush)
	}

	// Wait for result
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		return result.resp, result.err
	}
}

// timerFlush is called when the batch window timer fires.
func (b *Batcher) timerFlush(ns string) {
	b.mu.Lock()
	batch, exists := b.batches[ns]
	if !exists {
		// Batch was already removed (by size-threshold flush or close)
		b.mu.Unlock()
		return
	}
	// Check if already flushed by size-threshold (race between timer firing and Stop())
	if batch.flushed {
		b.mu.Unlock()
		return
	}
	batch.flushed = true
	delete(b.batches, ns)
	b.mu.Unlock()

	b.flushBatch(batch)
}

func (b *Batcher) commitStateFor(ns string) *namespaceCommitState {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.commitStates[ns]
	if state == nil {
		state = &namespaceCommitState{}
		b.commitStates[ns] = state
	}
	return state
}

// flushBatch commits all pending writes in a batch to a single WAL entry.
func (b *Batcher) flushBatch(batch *namespaceBatch) {
	if len(batch.writes) == 0 {
		return
	}

	commitState := b.commitStateFor(batch.namespace)
	commitState.mu.Lock()
	defer commitState.mu.Unlock()
	if !commitState.lastCommit.IsZero() {
		nextCommit := commitState.lastCommit.Add(b.cfg.BatchWindow)
		if delay := time.Until(nextCommit); delay > 0 {
			time.Sleep(delay)
		}
	}

	// Use background context for batch-level operations (state loading, WAL commit)
	// These operations must succeed regardless of individual request contexts
	batchCtx := context.Background()

	loaded, err := b.stateMan.LoadOrCreate(batchCtx, batch.namespace)
	if err != nil {
		// Fail all writes in the batch and release their reservations
		batchErr := fmt.Errorf("failed to load namespace state: %w", err)
		for _, pw := range batch.writes {
			if pw.reserved {
				b.idempotency.Release(batch.namespace, pw.req.RequestID, batchErr)
			}
			pw.result <- batchResult{err: batchErr}
		}
		return
	}

	// Create WAL entry for the next sequence number
	nextSeq := loaded.State.WAL.HeadSeq + 1
	walEntry := wal.NewWalEntry(batch.namespace, nextSeq)

	// Track results, aggregate schema delta, and track disable_backpressure usage
	results := make([]*WriteResponse, len(batch.writes))
	var schemaDelta *namespace.Schema
	var anyDisableBackpressure bool
	rebuildChanges := make(map[string]PendingRebuildChange)
	baseSize, err := wal.LogicalSize(walEntry)
	if err != nil {
		batchErr := fmt.Errorf("failed to size WAL entry: %w", err)
		for _, pw := range batch.writes {
			if pw.reserved {
				b.idempotency.Release(batch.namespace, pw.req.RequestID, batchErr)
			}
			pw.result <- batchResult{err: batchErr}
		}
		return
	}
	bytesEstimate := loaded.State.WAL.BytesUnindexedEst + baseSize

	// Process each write request into its own sub-batch
	// Use each write's own context for its processing
	for i, pw := range batch.writes {
		// Check if this write's context is already done before processing
		writeCtx := pw.ctx
		if writeCtx == nil {
			writeCtx = batchCtx
		}

		// If write's context is cancelled, fail just this write
		select {
		case <-writeCtx.Done():
			results[i] = nil
			if pw.reserved {
				b.idempotency.Release(batch.namespace, pw.req.RequestID, writeCtx.Err())
			}
			pw.result <- batchResult{err: writeCtx.Err()}
			continue
		default:
		}

		resp, subBatch, writeSchemaDelta, incomingBytes, err := b.processWrite(writeCtx, batch.namespace, loaded.State, pw.req, bytesEstimate)
		if err != nil {
			// Record error for this specific write and release reservation
			results[i] = nil
			if pw.reserved {
				b.idempotency.Release(batch.namespace, pw.req.RequestID, err)
			}
			pw.result <- batchResult{err: err}
			continue
		}

		results[i] = resp
		walEntry.SubBatches = append(walEntry.SubBatches, subBatch)
		bytesEstimate += incomingBytes

		// Track if any write in batch used disable_backpressure
		if pw.req.DisableBackpressure {
			anyDisableBackpressure = true
		}

		// Merge schema deltas
		if writeSchemaDelta != nil {
			if schemaDelta == nil {
				schemaDelta = writeSchemaDelta
			} else {
				for k, v := range writeSchemaDelta.Attributes {
					schemaDelta.Attributes[k] = v
				}
			}
		}

		// Track pending rebuilds for schema updates in this request.
		if pw.req.Schema != nil {
			changes := DetectSchemaRebuildChanges(pw.req.Schema, loaded.State.Schema)
			for _, change := range changes {
				key := change.Kind + ":" + change.Attribute
				rebuildChanges[key] = change
			}
		}
	}

	// If all writes failed validation, nothing to commit
	if len(walEntry.SubBatches) == 0 {
		return
	}

	// Create encoder for this batch (avoid shared state race)
	encoder, err := wal.NewEncoder()
	if err != nil {
		batchErr := fmt.Errorf("failed to create encoder: %w", err)
		for i, pw := range batch.writes {
			if results[i] != nil {
				if pw.reserved {
					b.idempotency.Release(batch.namespace, pw.req.RequestID, batchErr)
				}
				pw.result <- batchResult{err: batchErr}
			}
		}
		return
	}
	defer encoder.Close()

	var updatedState *namespace.LoadedState
	var commitErr error
	for attempt := 0; attempt <= maxWALConflictRetries; attempt++ {
		attemptLoaded := loaded
		if attempt > 0 {
			attemptLoaded, err = b.stateMan.LoadOrCreate(batchCtx, batch.namespace)
			if err != nil {
				commitErr = fmt.Errorf("failed to load namespace state: %w", err)
				break
			}
		}

		nextSeq := attemptLoaded.State.WAL.HeadSeq + 1
		walEntry.Seq = nextSeq
		walEntry.CommittedUnixMs = time.Now().UnixMilli()

		result, err := encoder.Encode(walEntry)
		if err != nil {
			commitErr = fmt.Errorf("failed to encode WAL entry: %w", err)
			break
		}

		// Write WAL entry to object storage (use batchCtx for durability)
		walKeyRelative := wal.KeyForSeq(nextSeq)
		walKey := "vex/namespaces/" + batch.namespace + "/" + walKeyRelative
		_, err = b.store.PutIfAbsent(batchCtx, walKey, bytes.NewReader(result.Data), int64(len(result.Data)), &objectstore.PutOptions{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			if objectstore.IsConflictError(err) {
				if confirmErr := confirmExistingWAL(batchCtx, b.store, walKey, result.Data); confirmErr != nil {
					commitErr = wrapWALConflict(batchCtx, b.store, b.stateMan, batch.namespace, confirmErr)
					if isRetryableWALConflict(commitErr) && attempt < maxWALConflictRetries {
						continue
					}
					break
				}
			} else {
				commitErr = fmt.Errorf("failed to write WAL entry: %w", err)
				break
			}
		}

		// Update namespace state (use batchCtx for durability)
		updatedState, err = b.stateMan.AdvanceWALWithOptions(batchCtx, batch.namespace, attemptLoaded.ETag, walKeyRelative, result.LogicalBytes, namespace.AdvanceWALOptions{
			SchemaDelta:         schemaDelta,
			DisableBackpressure: anyDisableBackpressure,
		})
		if err != nil {
			commitErr = fmt.Errorf("failed to update namespace state: %w", err)
			if isRetryableWALConflict(commitErr) && attempt < maxWALConflictRetries {
				continue
			}
			break
		}

		commitErr = nil
		break
	}

	if commitErr != nil {
		for i, pw := range batch.writes {
			if results[i] != nil {
				if pw.reserved {
					b.idempotency.Release(batch.namespace, pw.req.RequestID, commitErr)
				}
				pw.result <- batchResult{err: commitErr}
			}
		}
		return
	}

	if len(rebuildChanges) > 0 && updatedState != nil {
		currentETag := updatedState.ETag
		for _, change := range rebuildChanges {
			updated, rebuildErr := b.stateMan.AddPendingRebuild(batchCtx, batch.namespace, currentETag, change.Kind, change.Attribute)
			if rebuildErr != nil {
				break
			}
			currentETag = updated.ETag
		}
	}

	var totalUpserts int64
	for _, subBatch := range walEntry.SubBatches {
		if subBatch != nil && subBatch.Stats != nil {
			totalUpserts += subBatch.Stats.RowsUpserted
		}
	}
	if totalUpserts > 0 {
		metrics.AddDocumentsIndexed(batch.namespace, totalUpserts)
	}

	commitState.lastCommit = time.Now()

	// Send success results and complete idempotency reservations
	for i, pw := range batch.writes {
		if results[i] != nil {
			// Complete the idempotency reservation with successful response
			if pw.reserved {
				b.idempotency.Complete(batch.namespace, pw.req.RequestID, results[i])
			}
			pw.result <- batchResult{resp: results[i]}
		}
	}
}

// processWrite processes a single write request and returns its sub-batch.
// This is similar to Handler.Handle but returns the sub-batch instead of committing.
func (b *Batcher) processWrite(ctx context.Context, ns string, state *namespace.State, req *WriteRequest, bytesEstimate int64) (*WriteResponse, *wal.WriteSubBatch, *namespace.Schema, int64, error) {
	// Validate distance_metric against compat mode if specified
	if req.DistanceMetric != "" {
		if err := ValidateDistanceMetric(req.DistanceMetric, b.compatMode); err != nil {
			return nil, nil, nil, 0, err
		}
	}

	// Validate and convert schema update if provided
	var schemaDelta *namespace.Schema
	var err error
	if req.Schema != nil {
		schemaDelta, err = ValidateAndConvertSchemaUpdate(req.Schema, state.Schema)
		if err != nil {
			return nil, nil, nil, 0, err
		}
	}
	schemaCollector := newSchemaCollector(state.Schema, schemaDelta)

	// Create sub-batch for this request
	subBatch := wal.NewWriteSubBatch(req.RequestID)

	// Track if we hit the filter-based operation limit
	var rowsRemaining bool

	// Create a handler-like processor for this write
	handler := &writeProcessor{
		store:       b.store,
		stateMan:    b.stateMan,
		canon:       b.canon,
		tailStore:   b.tailStore,
		indexReader: b.indexReader,
	}

	// PHASE 1: delete_by_filter runs BEFORE all other operations
	if req.DeleteByFilter != nil {
		remaining, err := handler.processDeleteByFilter(ctx, ns, state.WAL.HeadSeq, state, req.DeleteByFilter, subBatch)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		rowsRemaining = remaining
	}

	// PHASE 2: patch_by_filter runs AFTER delete_by_filter, BEFORE other operations
	if req.PatchByFilter != nil {
		remaining, err := handler.processPatchByFilter(ctx, ns, state.WAL.HeadSeq, state, req.PatchByFilter, subBatch, schemaCollector)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		if remaining {
			rowsRemaining = true
		}
	}

	// PHASE 3: copy_from_namespace runs AFTER patch_by_filter, BEFORE explicit upserts/patches/deletes
	if req.CopyFromNamespace != "" {
		if err := handler.processCopyFromNamespace(ctx, req.CopyFromNamespace, subBatch, schemaCollector); err != nil {
			return nil, nil, nil, 0, err
		}
	}

	// PHASE 4: Process upsert_rows with last-write-wins deduplication
	dedupedUpserts, err := handler.deduplicateUpsertRows(req.UpsertRows)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	if req.UpsertCondition != nil {
		if err := handler.processConditionalUpserts(ctx, ns, state.WAL.HeadSeq, state, dedupedUpserts, req.UpsertCondition, subBatch, schemaCollector); err != nil {
			return nil, nil, nil, 0, err
		}
	} else {
		for _, row := range dedupedUpserts {
			if err := handler.processUpsertRow(row, subBatch, schemaCollector); err != nil {
				return nil, nil, nil, 0, err
			}
		}
	}

	// PHASE 5: Process patch_rows with last-write-wins deduplication
	dedupedPatches, err := handler.deduplicatePatchRows(req.PatchRows)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	if req.PatchCondition != nil {
		if err := handler.processConditionalPatches(ctx, ns, state.WAL.HeadSeq, state, dedupedPatches, req.PatchCondition, subBatch, schemaCollector); err != nil {
			return nil, nil, nil, 0, err
		}
	} else {
		patchSnapshotAvailable := false
		if len(dedupedPatches) > 0 {
			needsSnapshot := false
			for _, row := range dedupedPatches {
				rawID, ok := row["id"]
				if !ok {
					return nil, nil, nil, 0, fmt.Errorf("%w: missing 'id' field", ErrInvalidRequest)
				}
				docID, err := document.ParseID(rawID)
				if err != nil {
					return nil, nil, nil, 0, fmt.Errorf("%w: %v", ErrInvalidID, err)
				}
				if subBatch.HasPendingUpsert(docID) || subBatch.HasPendingDelete(docID) {
					continue
				}
				needsSnapshot = true
				break
			}
			if needsSnapshot {
				if handler.tailStore == nil {
					return nil, nil, nil, 0, ErrPatchRequiresTail
				}
				if err := handler.tailStore.Refresh(ctx, ns, 0, state.WAL.HeadSeq); err != nil {
					return nil, nil, nil, 0, err
				}
				patchSnapshotAvailable = true
			}
		}

		for _, row := range dedupedPatches {
			docID, attrs, canonAttrs, err := handler.preparePatchRow(row)
			if err != nil {
				return nil, nil, nil, 0, err
			}
			if patchSnapshotAvailable {
				if !subBatch.HasPendingUpsert(docID) {
					if subBatch.HasPendingDelete(docID) {
						continue
					}
					existingDoc, err := handler.tailStore.GetDocument(ctx, ns, docID)
					if err != nil {
						return nil, nil, nil, 0, fmt.Errorf("failed to get existing document: %w", err)
					}
					if existingDoc == nil || existingDoc.Deleted {
						continue
					}
				}
			}
			if schemaCollector != nil {
				if err := schemaCollector.ObserveAttributes(attrs); err != nil {
					return nil, nil, nil, 0, err
				}
			}
			subBatch.AddPatch(wal.DocumentIDFromID(docID), canonAttrs)
		}
	}

	// PHASE 6: Process deletes
	if req.DeleteCondition != nil {
		if err := handler.processConditionalDeletes(ctx, ns, state.WAL.HeadSeq, state, req.Deletes, req.DeleteCondition, subBatch); err != nil {
			return nil, nil, nil, 0, err
		}
	} else {
		for _, id := range req.Deletes {
			if err := handler.processDelete(id, subBatch); err != nil {
				return nil, nil, nil, 0, err
			}
		}
	}

	schemaDelta = mergeSchemaDelta(schemaDelta, schemaCollector.Delta())
	if schemaDelta != nil {
		walDeltas, err := schemaDeltaToWAL(schemaDelta)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		subBatch.SchemaDeltas = append(subBatch.SchemaDeltas, walDeltas...)
	}

	resp := &WriteResponse{
		RowsAffected:  subBatch.Stats.RowsAffected,
		RowsUpserted:  subBatch.Stats.RowsUpserted,
		RowsPatched:   subBatch.Stats.RowsPatched,
		RowsDeleted:   subBatch.Stats.RowsDeleted,
		RowsRemaining: rowsRemaining,
	}

	incomingBytes := subBatchLogicalBytes(subBatch)
	if !req.DisableBackpressure && bytesEstimate+incomingBytes > MaxUnindexedBytes {
		return nil, nil, nil, 0, ErrBackpressure
	}

	return resp, subBatch, schemaDelta, incomingBytes, nil
}

// estimateWriteSize estimates the uncompressed size of a write request.
func (b *Batcher) estimateWriteSize(req *WriteRequest) int64 {
	var size int64

	// Estimate upsert rows size
	for _, row := range req.UpsertRows {
		size += 100 // base overhead per row
		for k, v := range row {
			size += int64(len(k))
			size += estimateValueSize(v)
		}
	}

	// Estimate patch rows size
	for _, row := range req.PatchRows {
		size += 50 // base overhead per row
		for k, v := range row {
			size += int64(len(k))
			size += estimateValueSize(v)
		}
	}

	// Estimate deletes
	size += int64(len(req.Deletes)) * 20

	return size
}

func subBatchLogicalBytes(subBatch *wal.WriteSubBatch) int64 {
	if subBatch == nil {
		return 0
	}
	msgSize := proto.Size(subBatch)
	return int64(1 + varintLen(uint64(msgSize)) + msgSize)
}

func varintLen(v uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], v)
}

// estimateValueSize estimates the size of a value in bytes.
func estimateValueSize(v any) int64 {
	switch val := v.(type) {
	case string:
		return int64(len(val))
	case []any:
		var size int64 = 8
		for _, elem := range val {
			size += estimateValueSize(elem)
		}
		return size
	case []float64, []float32:
		return 8 * 128 // assume 128 dimensions for vectors
	default:
		return 8
	}
}

// writeProcessor wraps the processing logic for individual writes.
// It reuses the logic from Handler but without committing.
type writeProcessor struct {
	store       objectstore.Store
	stateMan    *namespace.StateManager
	canon       *wal.Canonicalizer
	tailStore   tail.Store
	indexReader *index.Reader
}

// The following methods delegate to the Handler implementation.
// We create a temporary Handler instance to reuse the existing logic.

func (p *writeProcessor) processDeleteByFilter(ctx context.Context, ns string, snapshotSeq uint64, state *namespace.State, req *DeleteByFilterRequest, batch *wal.WriteSubBatch) (bool, error) {
	h := &Handler{
		store:       p.store,
		stateMan:    p.stateMan,
		canon:       p.canon,
		tailStore:   p.tailStore,
		indexReader: p.indexReader,
	}
	return h.processDeleteByFilter(ctx, ns, snapshotSeq, state, req, batch)
}

func (p *writeProcessor) processPatchByFilter(ctx context.Context, ns string, snapshotSeq uint64, state *namespace.State, req *PatchByFilterRequest, batch *wal.WriteSubBatch, schemaCollector *schemaCollector) (bool, error) {
	h := &Handler{
		store:       p.store,
		stateMan:    p.stateMan,
		canon:       p.canon,
		tailStore:   p.tailStore,
		indexReader: p.indexReader,
	}
	return h.processPatchByFilter(ctx, ns, snapshotSeq, state, req, batch, schemaCollector)
}

func (p *writeProcessor) processCopyFromNamespace(ctx context.Context, sourceNs string, batch *wal.WriteSubBatch, schemaCollector *schemaCollector) error {
	h := &Handler{
		store:       p.store,
		stateMan:    p.stateMan,
		canon:       p.canon,
		tailStore:   p.tailStore,
		indexReader: p.indexReader,
	}
	return h.processCopyFromNamespace(ctx, sourceNs, batch, schemaCollector)
}

func (p *writeProcessor) processConditionalUpserts(ctx context.Context, ns string, snapshotSeq uint64, state *namespace.State, rows []map[string]any, condition any, batch *wal.WriteSubBatch, schemaCollector *schemaCollector) error {
	h := &Handler{
		store:       p.store,
		stateMan:    p.stateMan,
		canon:       p.canon,
		tailStore:   p.tailStore,
		indexReader: p.indexReader,
	}
	return h.processConditionalUpserts(ctx, ns, snapshotSeq, state, rows, condition, batch, schemaCollector)
}

func (p *writeProcessor) processConditionalPatches(ctx context.Context, ns string, snapshotSeq uint64, state *namespace.State, rows []map[string]any, condition any, batch *wal.WriteSubBatch, schemaCollector *schemaCollector) error {
	h := &Handler{
		store:       p.store,
		stateMan:    p.stateMan,
		canon:       p.canon,
		tailStore:   p.tailStore,
		indexReader: p.indexReader,
	}
	return h.processConditionalPatches(ctx, ns, snapshotSeq, state, rows, condition, batch, schemaCollector)
}

func (p *writeProcessor) processConditionalDeletes(ctx context.Context, ns string, snapshotSeq uint64, state *namespace.State, ids []any, condition any, batch *wal.WriteSubBatch) error {
	h := &Handler{
		store:       p.store,
		stateMan:    p.stateMan,
		canon:       p.canon,
		tailStore:   p.tailStore,
		indexReader: p.indexReader,
	}
	return h.processConditionalDeletes(ctx, ns, snapshotSeq, state, ids, condition, batch)
}

func (p *writeProcessor) deduplicateUpsertRows(rows []map[string]any) ([]map[string]any, error) {
	h := &Handler{canon: p.canon}
	return h.deduplicateUpsertRows(rows)
}

func (p *writeProcessor) deduplicatePatchRows(rows []map[string]any) ([]map[string]any, error) {
	h := &Handler{canon: p.canon}
	return h.deduplicatePatchRows(rows)
}

func (p *writeProcessor) processUpsertRow(row map[string]any, batch *wal.WriteSubBatch, schemaCollector *schemaCollector) error {
	h := &Handler{canon: p.canon}
	return h.processUpsertRow(row, batch, schemaCollector)
}

func (p *writeProcessor) preparePatchRow(row map[string]any) (document.ID, map[string]any, map[string]*wal.AttributeValue, error) {
	h := &Handler{canon: p.canon}
	return h.preparePatchRow(row)
}

func (p *writeProcessor) processDelete(rawID any, batch *wal.WriteSubBatch) error {
	h := &Handler{canon: p.canon}
	return h.processDelete(rawID, batch)
}
