// Package write implements the write path for Vex, handling document upserts,
// patches, and deletes with WAL commit to object storage.
package write

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
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
	namespace   string
	writes      []*pendingWrite
	timer       *time.Timer
	startTime   time.Time
	size        int64 // estimated uncompressed size
	schemaDelta *namespace.Schema
	flushed     bool  // set to true once flush starts, prevents double-flush
}

// Batcher batches write requests per namespace, committing at most 1 WAL entry per second.
type Batcher struct {
	cfg           BatcherConfig
	store         objectstore.Store
	stateMan      *namespace.StateManager
	canon         *wal.Canonicalizer
	tailStore     tail.Store
	idempotency   *IdempotencyStore

	mu       sync.Mutex
	batches  map[string]*namespaceBatch // namespace -> pending batch
	closed   bool
	closeWg  sync.WaitGroup
}

// NewBatcher creates a new write batcher.
func NewBatcher(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store) (*Batcher, error) {
	return NewBatcherWithConfig(store, stateMan, tailStore, DefaultBatcherConfig())
}

// NewBatcherWithConfig creates a new write batcher with custom configuration.
func NewBatcherWithConfig(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store, cfg BatcherConfig) (*Batcher, error) {
	if cfg.BatchWindow <= 0 {
		cfg.BatchWindow = DefaultBatchWindow
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = DefaultMaxBatchSize
	}

	return &Batcher{
		cfg:         cfg,
		store:       store,
		stateMan:    stateMan,
		canon:       wal.NewCanonicalizer(),
		tailStore:   tailStore,
		idempotency: NewIdempotencyStore(),
		batches:     make(map[string]*namespaceBatch),
	}, nil
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

// flushBatch commits all pending writes in a batch to a single WAL entry.
func (b *Batcher) flushBatch(batch *namespaceBatch) {
	if len(batch.writes) == 0 {
		return
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

		resp, subBatch, writeSchemaDelta, err := b.processWrite(writeCtx, batch.namespace, loaded.State, pw.req)
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

	// Encode the WAL entry
	result, err := encoder.Encode(walEntry)
	if err != nil {
		// Fail all pending writes and release reservations
		batchErr := fmt.Errorf("failed to encode WAL entry: %w", err)
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

	// Write WAL entry to object storage (use batchCtx for durability)
	walKeyRelative := wal.KeyForSeq(nextSeq)
	walKey := "vex/namespaces/" + batch.namespace + "/" + walKeyRelative
	_, err = b.store.PutIfAbsent(batchCtx, walKey, bytes.NewReader(result.Data), int64(len(result.Data)), &objectstore.PutOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil && !objectstore.IsConflictError(err) {
		// Fail all pending writes and release reservations
		batchErr := fmt.Errorf("failed to write WAL entry: %w", err)
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

	// Update namespace state (use batchCtx for durability)
	_, err = b.stateMan.AdvanceWALWithOptions(batchCtx, batch.namespace, loaded.ETag, walKeyRelative, int64(len(result.Data)), namespace.AdvanceWALOptions{
		SchemaDelta:        schemaDelta,
		DisableBackpressure: anyDisableBackpressure,
	})
	if err != nil {
		// Fail all pending writes and release reservations
		batchErr := fmt.Errorf("failed to update namespace state: %w", err)
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
func (b *Batcher) processWrite(ctx context.Context, ns string, state *namespace.State, req *WriteRequest) (*WriteResponse, *wal.WriteSubBatch, *namespace.Schema, error) {
	// Check backpressure: reject writes when unindexed data > 2GB unless disable_backpressure is set
	if !req.DisableBackpressure && state.WAL.BytesUnindexedEst > MaxUnindexedBytes {
		return nil, nil, nil, ErrBackpressure
	}

	// Validate and convert schema update if provided
	var schemaDelta *namespace.Schema
	var err error
	if req.Schema != nil {
		schemaDelta, err = ValidateAndConvertSchemaUpdate(req.Schema, state.Schema)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// Create sub-batch for this request
	subBatch := wal.NewWriteSubBatch(req.RequestID)

	// Track if we hit the filter-based operation limit
	var rowsRemaining bool

	// Create a handler-like processor for this write
	handler := &writeProcessor{
		store:     b.store,
		stateMan:  b.stateMan,
		canon:     b.canon,
		tailStore: b.tailStore,
	}

	// PHASE 1: delete_by_filter runs BEFORE all other operations
	if req.DeleteByFilter != nil {
		remaining, err := handler.processDeleteByFilter(ctx, ns, state.WAL.HeadSeq, req.DeleteByFilter, subBatch, state.Schema)
		if err != nil {
			return nil, nil, nil, err
		}
		rowsRemaining = remaining
	}

	// PHASE 2: patch_by_filter runs AFTER delete_by_filter, BEFORE other operations
	if req.PatchByFilter != nil {
		remaining, err := handler.processPatchByFilter(ctx, ns, state.WAL.HeadSeq, req.PatchByFilter, subBatch, state.Schema)
		if err != nil {
			return nil, nil, nil, err
		}
		if remaining {
			rowsRemaining = true
		}
	}

	// PHASE 3: copy_from_namespace runs AFTER patch_by_filter, BEFORE explicit upserts/patches/deletes
	if req.CopyFromNamespace != "" {
		if err := handler.processCopyFromNamespace(ctx, req.CopyFromNamespace, subBatch); err != nil {
			return nil, nil, nil, err
		}
	}

	// PHASE 4: Process upsert_rows with last-write-wins deduplication
	dedupedUpserts, err := handler.deduplicateUpsertRows(req.UpsertRows)
	if err != nil {
		return nil, nil, nil, err
	}

	if req.UpsertCondition != nil {
		if err := handler.processConditionalUpserts(ctx, ns, state.WAL.HeadSeq, dedupedUpserts, req.UpsertCondition, subBatch); err != nil {
			return nil, nil, nil, err
		}
	} else {
		for _, row := range dedupedUpserts {
			if err := handler.processUpsertRow(row, subBatch); err != nil {
				return nil, nil, nil, err
			}
		}
	}

	// PHASE 5: Process patch_rows with last-write-wins deduplication
	dedupedPatches, err := handler.deduplicatePatchRows(req.PatchRows)
	if err != nil {
		return nil, nil, nil, err
	}

	if req.PatchCondition != nil {
		if err := handler.processConditionalPatches(ctx, ns, state.WAL.HeadSeq, dedupedPatches, req.PatchCondition, subBatch); err != nil {
			return nil, nil, nil, err
		}
	} else {
		for _, row := range dedupedPatches {
			if err := handler.processPatchRow(row, subBatch); err != nil {
				return nil, nil, nil, err
			}
		}
	}

	// PHASE 6: Process deletes
	if req.DeleteCondition != nil {
		if err := handler.processConditionalDeletes(ctx, ns, state.WAL.HeadSeq, req.Deletes, req.DeleteCondition, subBatch); err != nil {
			return nil, nil, nil, err
		}
	} else {
		for _, id := range req.Deletes {
			if err := handler.processDelete(id, subBatch); err != nil {
				return nil, nil, nil, err
			}
		}
	}

	return &WriteResponse{
		RowsAffected:  subBatch.Stats.RowsAffected,
		RowsUpserted:  subBatch.Stats.RowsUpserted,
		RowsPatched:   subBatch.Stats.RowsPatched,
		RowsDeleted:   subBatch.Stats.RowsDeleted,
		RowsRemaining: rowsRemaining,
	}, subBatch, schemaDelta, nil
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
	store     objectstore.Store
	stateMan  *namespace.StateManager
	canon     *wal.Canonicalizer
	tailStore tail.Store
}

// The following methods delegate to the Handler implementation.
// We create a temporary Handler instance to reuse the existing logic.

func (p *writeProcessor) processDeleteByFilter(ctx context.Context, ns string, snapshotSeq uint64, req *DeleteByFilterRequest, batch *wal.WriteSubBatch, nsSchema *namespace.Schema) (bool, error) {
	h := &Handler{
		store:     p.store,
		stateMan:  p.stateMan,
		canon:     p.canon,
		tailStore: p.tailStore,
	}
	return h.processDeleteByFilter(ctx, ns, snapshotSeq, req, batch, nsSchema)
}

func (p *writeProcessor) processPatchByFilter(ctx context.Context, ns string, snapshotSeq uint64, req *PatchByFilterRequest, batch *wal.WriteSubBatch, nsSchema *namespace.Schema) (bool, error) {
	h := &Handler{
		store:     p.store,
		stateMan:  p.stateMan,
		canon:     p.canon,
		tailStore: p.tailStore,
	}
	return h.processPatchByFilter(ctx, ns, snapshotSeq, req, batch, nsSchema)
}

func (p *writeProcessor) processCopyFromNamespace(ctx context.Context, sourceNs string, batch *wal.WriteSubBatch) error {
	h := &Handler{
		store:     p.store,
		stateMan:  p.stateMan,
		canon:     p.canon,
		tailStore: p.tailStore,
	}
	return h.processCopyFromNamespace(ctx, sourceNs, batch)
}

func (p *writeProcessor) processConditionalUpserts(ctx context.Context, ns string, snapshotSeq uint64, rows []map[string]any, condition any, batch *wal.WriteSubBatch) error {
	h := &Handler{
		store:     p.store,
		stateMan:  p.stateMan,
		canon:     p.canon,
		tailStore: p.tailStore,
	}
	return h.processConditionalUpserts(ctx, ns, snapshotSeq, rows, condition, batch)
}

func (p *writeProcessor) processConditionalPatches(ctx context.Context, ns string, snapshotSeq uint64, rows []map[string]any, condition any, batch *wal.WriteSubBatch) error {
	h := &Handler{
		store:     p.store,
		stateMan:  p.stateMan,
		canon:     p.canon,
		tailStore: p.tailStore,
	}
	return h.processConditionalPatches(ctx, ns, snapshotSeq, rows, condition, batch)
}

func (p *writeProcessor) processConditionalDeletes(ctx context.Context, ns string, snapshotSeq uint64, ids []any, condition any, batch *wal.WriteSubBatch) error {
	h := &Handler{
		store:     p.store,
		stateMan:  p.stateMan,
		canon:     p.canon,
		tailStore: p.tailStore,
	}
	return h.processConditionalDeletes(ctx, ns, snapshotSeq, ids, condition, batch)
}

func (p *writeProcessor) deduplicateUpsertRows(rows []map[string]any) ([]map[string]any, error) {
	h := &Handler{canon: p.canon}
	return h.deduplicateUpsertRows(rows)
}

func (p *writeProcessor) deduplicatePatchRows(rows []map[string]any) ([]map[string]any, error) {
	h := &Handler{canon: p.canon}
	return h.deduplicatePatchRows(rows)
}

func (p *writeProcessor) processUpsertRow(row map[string]any, batch *wal.WriteSubBatch) error {
	h := &Handler{canon: p.canon}
	return h.processUpsertRow(row, batch)
}

func (p *writeProcessor) processPatchRow(row map[string]any, batch *wal.WriteSubBatch) error {
	h := &Handler{canon: p.canon}
	return h.processPatchRow(row, batch)
}

func (p *writeProcessor) processDelete(rawID any, batch *wal.WriteSubBatch) error {
	h := &Handler{canon: p.canon}
	return h.processDelete(rawID, batch)
}
