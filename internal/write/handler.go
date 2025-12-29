// Package write implements the write path for Vex, handling document upserts,
// patches, and deletes with WAL commit to object storage.
package write

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/schema"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

const (
	// MaxDeleteByFilterRows is the maximum number of rows that can be deleted by filter
	MaxDeleteByFilterRows = 5_000_000
	// MaxPatchByFilterRows is the maximum number of rows that can be patched by filter
	MaxPatchByFilterRows = 500_000
)

const (
	// MaxUnindexedBytes is the threshold for backpressure (2GB).
	MaxUnindexedBytes = 2 * 1024 * 1024 * 1024
)

var (
	ErrInvalidRequest            = errors.New("invalid write request")
	ErrInvalidID                 = errors.New("invalid document ID")
	ErrInvalidAttribute          = errors.New("invalid attribute")
	ErrDuplicateIDColumn         = errors.New("duplicate IDs in columnar format")
	ErrVectorPatchForbidden      = errors.New("vector attribute cannot be patched")
	ErrDeleteByFilterTooMany     = errors.New("delete_by_filter exceeds maximum rows limit")
	ErrPatchByFilterTooMany      = errors.New("patch_by_filter exceeds maximum rows limit")
	ErrInvalidFilter             = errors.New("invalid filter expression")
	ErrSourceNamespaceNotFound   = errors.New("source namespace not found")
	ErrFilterOpRequiresTail      = errors.New("filter operation requires tail store")
	ErrSchemaTypeChange          = errors.New("changing attribute type is not allowed")
	ErrInvalidSchema             = errors.New("invalid schema")
	ErrBackpressure              = errors.New("write rejected: unindexed data exceeds 2GB threshold")
	ErrInvalidDistanceMetric     = errors.New("invalid distance_metric")
	ErrDotProductNotAllowed      = errors.New("dot_product distance metric is not supported in turbopuffer compatibility mode")
)

// DeleteByFilterRequest represents a delete_by_filter operation.
type DeleteByFilterRequest struct {
	Filter       any  // The filter expression (JSON-compatible)
	AllowPartial bool // If true, allow partial deletion when limit is exceeded
}

// PatchByFilterRequest represents a patch_by_filter operation.
type PatchByFilterRequest struct {
	Filter       any            // The filter expression (JSON-compatible)
	Updates      map[string]any // The attributes to update on matching documents
	AllowPartial bool           // If true, allow partial patch when limit is exceeded
}

// SchemaUpdate represents an explicit schema change in a write request.
type SchemaUpdate struct {
	Attributes map[string]AttributeSchemaUpdate
}

// AttributeSchemaUpdate represents an update to a single attribute's schema.
type AttributeSchemaUpdate struct {
	Type           string // Required: the attribute type (string, int, uint, float, uuid, datetime, bool, or array variants)
	Filterable     *bool  // Optional: whether the attribute is filterable
	Regex          *bool  // Optional: whether regex is enabled
	FullTextSearch any    // Optional: true, false, or config object
}

// WriteRequest represents a write request from the API.
type WriteRequest struct {
	RequestID           string
	UpsertRows          []map[string]any
	PatchRows           []map[string]any
	Deletes             []any
	DeleteByFilter      *DeleteByFilterRequest // Optional delete_by_filter operation
	PatchByFilter       *PatchByFilterRequest  // Optional patch_by_filter operation
	CopyFromNamespace   string                 // Optional: source namespace for server-side bulk copy
	Schema              *SchemaUpdate          // Optional: explicit schema changes
	UpsertCondition     any                    // Optional: condition evaluated against current doc for upserts
	PatchCondition      any                    // Optional: condition evaluated against current doc for patches
	DeleteCondition     any                    // Optional: condition evaluated against current doc for deletes
	DisableBackpressure bool                   // If true, bypass the 2GB unindexed threshold check
	DistanceMetric      string                 // Optional: distance metric for vectors (cosine_distance, euclidean_squared, dot_product)
}

// ColumnarData represents columnar format data with ids and attribute arrays.
type ColumnarData struct {
	IDs        []any
	Attributes map[string][]any
}

// WriteResponse contains the result of a write operation.
type WriteResponse struct {
	RowsAffected  int64 `json:"rows_affected"`
	RowsUpserted  int64 `json:"rows_upserted"`
	RowsPatched   int64 `json:"rows_patched"`
	RowsDeleted   int64 `json:"rows_deleted"`
	RowsRemaining bool  `json:"rows_remaining,omitempty"` // True if filter-based op hit cap
}

// Handler handles write operations for a namespace.
type Handler struct {
	store       objectstore.Store
	stateMan    *namespace.StateManager
	encoder     *wal.Encoder
	canon       *wal.Canonicalizer
	tailStore   tail.Store         // Optional tail store for filter evaluation
	idempotency *IdempotencyStore  // Optional idempotency store for de-duplication
	compatMode  string             // Compatibility mode: "turbopuffer" or "vex"
}

// NewHandler creates a new write handler.
func NewHandler(store objectstore.Store, stateMan *namespace.StateManager) (*Handler, error) {
	return NewHandlerWithTail(store, stateMan, nil)
}

// NewHandlerWithTail creates a new write handler with an optional tail store for filter operations.
func NewHandlerWithTail(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store) (*Handler, error) {
	return NewHandlerWithOptions(store, stateMan, tailStore, "turbopuffer")
}

// NewHandlerWithOptions creates a new write handler with all options including compat mode.
func NewHandlerWithOptions(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store, compatMode string) (*Handler, error) {
	encoder, err := wal.NewEncoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL encoder: %w", err)
	}

	return &Handler{
		store:       store,
		stateMan:    stateMan,
		encoder:     encoder,
		canon:       wal.NewCanonicalizer(),
		tailStore:   tailStore,
		idempotency: NewIdempotencyStore(),
		compatMode:  compatMode,
	}, nil
}

// CompatMode returns the compatibility mode of the handler.
func (h *Handler) CompatMode() string {
	return h.compatMode
}

// Close releases resources held by the handler.
func (h *Handler) Close() error {
	if h.idempotency != nil {
		h.idempotency.Close()
	}
	return h.encoder.Close()
}

// Handle processes a write request for the given namespace.
// It returns only after the WAL entry is committed to object storage.
// If a duplicate request_id is detected (cached or in-flight), returns the original response.
//
// Write operation ordering (per spec):
//  1. delete_by_filter - before all other operations
//  2. patch_by_filter - after delete_by_filter, before any other operation
//  3. copy_from_namespace - before explicit upserts/patches/deletes
//  4. upserts (upsert_rows/upsert_columns)
//  5. patches (patch_rows/patch_columns)
//  6. deletes
func (h *Handler) Handle(ctx context.Context, ns string, req *WriteRequest) (*WriteResponse, error) {
	// Check for duplicate request_id and reserve if not
	reserved := false
	if h.idempotency != nil && req.RequestID != "" {
		result, cached, inflight := h.idempotency.Reserve(ns, req.RequestID)
		switch result {
		case ReserveCached:
			return cached, nil
		case ReserveInFlight:
			// Wait for the in-flight request to complete (respects ctx cancellation)
			resp, err := h.idempotency.WaitForInFlight(ctx, inflight)
			if resp != nil {
				return resp, nil
			}
			if err != nil {
				return nil, err
			}
			// Original request failed with no error, retry by reserving again
			return h.Handle(ctx, ns, req)
		case ReserveOK:
			// We have the reservation, proceed with processing
			reserved = true
		}
	}

	// Helper to release reservation on error
	releaseOnError := func(err error) {
		if reserved && h.idempotency != nil {
			h.idempotency.Release(ns, req.RequestID, err)
		}
	}

	// Load or create namespace state
	loaded, err := h.stateMan.LoadOrCreate(ctx, ns)
	if err != nil {
		releaseOnError(err)
		if errors.Is(err, namespace.ErrNamespaceTombstoned) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to load namespace state: %w", err)
	}

	// Check backpressure: reject writes when unindexed data > 2GB unless disable_backpressure is set
	if !req.DisableBackpressure && loaded.State.WAL.BytesUnindexedEst > MaxUnindexedBytes {
		releaseOnError(ErrBackpressure)
		return nil, ErrBackpressure
	}

	// Validate distance_metric against compat mode if specified
	if req.DistanceMetric != "" {
		if err := ValidateDistanceMetric(req.DistanceMetric, h.compatMode); err != nil {
			releaseOnError(err)
			return nil, err
		}
	}

	// Validate and convert schema update if provided
	var schemaDelta *namespace.Schema
	if req.Schema != nil {
		schemaDelta, err = ValidateAndConvertSchemaUpdate(req.Schema, loaded.State.Schema)
		if err != nil {
			releaseOnError(err)
			return nil, err
		}
	}

	// Create WAL entry for the next sequence number
	nextSeq := loaded.State.WAL.HeadSeq + 1
	walEntry := wal.NewWalEntry(ns, nextSeq)

	// Create sub-batch for this request
	subBatch := wal.NewWriteSubBatch(req.RequestID)

	// Track if we hit the filter-based operation limit
	var rowsRemaining bool

	// PHASE 1: delete_by_filter runs BEFORE all other operations
	if req.DeleteByFilter != nil {
		remaining, err := h.processDeleteByFilter(ctx, ns, loaded.State.WAL.HeadSeq, req.DeleteByFilter, subBatch, loaded.State.Schema)
		if err != nil {
			releaseOnError(err)
			return nil, err
		}
		rowsRemaining = remaining
	}

	// PHASE 2: patch_by_filter runs AFTER delete_by_filter, BEFORE other operations
	if req.PatchByFilter != nil {
		remaining, err := h.processPatchByFilter(ctx, ns, loaded.State.WAL.HeadSeq, req.PatchByFilter, subBatch, loaded.State.Schema)
		if err != nil {
			releaseOnError(err)
			return nil, err
		}
		// Only update rowsRemaining if we haven't already set it
		if remaining {
			rowsRemaining = true
		}
	}

	// PHASE 3: copy_from_namespace runs AFTER patch_by_filter, BEFORE explicit upserts/patches/deletes
	if req.CopyFromNamespace != "" {
		if err := h.processCopyFromNamespace(ctx, req.CopyFromNamespace, subBatch); err != nil {
			releaseOnError(err)
			return nil, err
		}
	}

	// PHASE 4: Process upsert_rows with last-write-wins deduplication
	// Later occurrences of the same ID override earlier ones
	dedupedUpserts, err := h.deduplicateUpsertRows(req.UpsertRows)
	if err != nil {
		releaseOnError(err)
		return nil, err
	}

	// If upsert_condition is specified, process conditionally
	if req.UpsertCondition != nil {
		if err := h.processConditionalUpserts(ctx, ns, loaded.State.WAL.HeadSeq, dedupedUpserts, req.UpsertCondition, subBatch); err != nil {
			releaseOnError(err)
			return nil, err
		}
	} else {
		for _, row := range dedupedUpserts {
			if err := h.processUpsertRow(row, subBatch); err != nil {
				releaseOnError(err)
				return nil, err
			}
		}
	}

	// PHASE 5: Process patch_rows with last-write-wins deduplication
	// Patches run after upserts per write ordering spec
	dedupedPatches, err := h.deduplicatePatchRows(req.PatchRows)
	if err != nil {
		releaseOnError(err)
		return nil, err
	}

	// If patch_condition is specified, process conditionally
	if req.PatchCondition != nil {
		if err := h.processConditionalPatches(ctx, ns, loaded.State.WAL.HeadSeq, dedupedPatches, req.PatchCondition, subBatch); err != nil {
			releaseOnError(err)
			return nil, err
		}
	} else {
		for _, row := range dedupedPatches {
			if err := h.processPatchRow(row, subBatch); err != nil {
				releaseOnError(err)
				return nil, err
			}
		}
	}

	// PHASE 6: Process deletes (runs after patches per write ordering spec)
	// If delete_condition is specified, process conditionally
	if req.DeleteCondition != nil {
		if err := h.processConditionalDeletes(ctx, ns, loaded.State.WAL.HeadSeq, req.Deletes, req.DeleteCondition, subBatch); err != nil {
			releaseOnError(err)
			return nil, err
		}
	} else {
		for _, id := range req.Deletes {
			if err := h.processDelete(id, subBatch); err != nil {
				releaseOnError(err)
				return nil, err
			}
		}
	}

	walEntry.SubBatches = append(walEntry.SubBatches, subBatch)

	// Encode the WAL entry
	result, err := h.encoder.Encode(walEntry)
	if err != nil {
		err = fmt.Errorf("failed to encode WAL entry: %w", err)
		releaseOnError(err)
		return nil, err
	}

	// Write WAL entry to object storage with If-None-Match for idempotency
	walKeyRelative := wal.KeyForSeq(nextSeq)
	walKey := "vex/namespaces/" + ns + "/" + walKeyRelative
	_, err = h.store.PutIfAbsent(ctx, walKey, bytes.NewReader(result.Data), int64(len(result.Data)), &objectstore.PutOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil && !objectstore.IsConflictError(err) {
		err = fmt.Errorf("failed to write WAL entry: %w", err)
		releaseOnError(err)
		return nil, err
	}

	// Detect schema changes that require index rebuilds before updating state
	rebuildChanges := DetectSchemaRebuildChanges(req.Schema, loaded.State.Schema)

	// Update namespace state to advance WAL head with schema delta and disable_backpressure flag
	updatedState, err := h.stateMan.AdvanceWALWithOptions(ctx, ns, loaded.ETag, walKeyRelative, int64(len(result.Data)), namespace.AdvanceWALOptions{
		SchemaDelta:        schemaDelta,
		DisableBackpressure: req.DisableBackpressure,
	})
	if err != nil {
		err = fmt.Errorf("failed to update namespace state: %w", err)
		releaseOnError(err)
		return nil, err
	}

	// Add pending rebuilds for detected schema changes
	if len(rebuildChanges) > 0 {
		currentETag := updatedState.ETag
		for _, change := range rebuildChanges {
			updated, rebuildErr := h.stateMan.AddPendingRebuild(ctx, ns, currentETag, change.Kind, change.Attribute)
			if rebuildErr != nil {
				// Log but don't fail the write - the rebuild tracking is best-effort
				// The indexer will still reindex based on schema changes
				break
			}
			currentETag = updated.ETag
		}
	}

	resp := &WriteResponse{
		RowsAffected:  subBatch.Stats.RowsAffected,
		RowsUpserted:  subBatch.Stats.RowsUpserted,
		RowsPatched:   subBatch.Stats.RowsPatched,
		RowsDeleted:   subBatch.Stats.RowsDeleted,
		RowsRemaining: rowsRemaining,
	}

	// Complete the idempotency reservation with the successful response
	if reserved && h.idempotency != nil {
		h.idempotency.Complete(ns, req.RequestID, resp)
	}

	return resp, nil
}

// processDeleteByFilter implements the two-phase delete_by_filter operation.
// Phase 1: Evaluate filter at snapshot, select matching IDs (bounded by limit)
// Phase 2: Re-evaluate filter and delete IDs that still match
// Returns true if rows_remaining (more rows matched than limit)
func (h *Handler) processDeleteByFilter(ctx context.Context, ns string, snapshotSeq uint64, req *DeleteByFilterRequest, batch *wal.WriteSubBatch, nsSchema *namespace.Schema) (bool, error) {
	// Parse the filter
	f, err := filter.Parse(req.Filter)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}

	// If filter is nil, no documents match
	if f == nil {
		return false, nil
	}

	// Validate regex filters against schema
	if f.UsesRegexOperators() {
		schemaChecker := buildSchemaChecker(nsSchema)
		if err := f.ValidateWithSchema(schemaChecker); err != nil {
			return false, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
		}
	}

	// Serialize filter expression to JSON for WAL persistence
	filterJSON, err := serializeFilterToJSON(req.Filter)
	if err != nil {
		return false, fmt.Errorf("failed to serialize filter: %w", err)
	}

	// Phase 1: Get candidate IDs at the current snapshot
	// We need a tail store to evaluate the filter
	if h.tailStore == nil {
		return false, fmt.Errorf("%w: delete_by_filter requires tail store", ErrFilterOpRequiresTail)
	}

	// Refresh tail to ensure we have current data
	if err := h.tailStore.Refresh(ctx, ns, 0, snapshotSeq); err != nil {
		return false, fmt.Errorf("failed to refresh tail: %w", err)
	}

	// Scan for matching documents
	docs, err := h.tailStore.Scan(ctx, ns, f)
	if err != nil {
		return false, fmt.Errorf("failed to scan for matching documents: %w", err)
	}

	// Check if we exceed the limit
	rowsRemaining := len(docs) > MaxDeleteByFilterRows
	if rowsRemaining && !req.AllowPartial {
		return false, fmt.Errorf("%w: %d rows match filter, max is %d", ErrDeleteByFilterTooMany, len(docs), MaxDeleteByFilterRows)
	}

	// Limit the candidates if we're in partial mode
	candidateIDs := make([]document.ID, 0, len(docs))
	for i, doc := range docs {
		if i >= MaxDeleteByFilterRows {
			break
		}
		candidateIDs = append(candidateIDs, doc.ID)
	}

	// Record filter operation metadata in WAL for deterministic replay.
	// Use AddFilterOp to support multiple filter operations in the same request.
	batch.AddFilterOp(&wal.FilterOperation{
		Type:              wal.FilterOperationType_FILTER_OPERATION_TYPE_DELETE,
		Phase1SnapshotSeq: snapshotSeq,
		CandidateIds:      documentIDsToProto(candidateIDs),
		FilterJson:        filterJSON,
		AllowPartial:      req.AllowPartial,
	})

	// Phase 2: Re-evaluate filter for each candidate and delete those that still match
	// This implements "Read Committed" semantics - docs that newly qualify between
	// phases can be missed, and docs that no longer qualify are not deleted
	for _, id := range candidateIDs {
		// Get the document again to re-evaluate
		doc, err := h.tailStore.GetDocument(ctx, ns, id)
		if err != nil {
			continue // Skip on error
		}
		if doc == nil || doc.Deleted {
			continue // Document was deleted between phases
		}

		// Re-evaluate the filter
		filterDoc := make(filter.Document)
		for k, v := range doc.Attributes {
			filterDoc[k] = v
		}
		if !f.Eval(filterDoc) {
			continue // Document no longer matches filter
		}

		// Add delete mutation
		protoID := wal.DocumentIDFromID(id)
		batch.AddDelete(protoID)
	}

	return rowsRemaining, nil
}

// processPatchByFilter implements the two-phase patch_by_filter operation.
// Phase 1: Evaluate filter at snapshot, select matching IDs (bounded by limit)
// Phase 2: Re-evaluate filter and patch IDs that still match
// Returns true if rows_remaining (more rows matched than limit)
func (h *Handler) processPatchByFilter(ctx context.Context, ns string, snapshotSeq uint64, req *PatchByFilterRequest, batch *wal.WriteSubBatch, nsSchema *namespace.Schema) (bool, error) {
	// Parse the filter
	f, err := filter.Parse(req.Filter)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}

	// If filter is nil, no documents match
	if f == nil {
		return false, nil
	}

	// Validate regex filters against schema
	if f.UsesRegexOperators() {
		schemaChecker := buildSchemaChecker(nsSchema)
		if err := f.ValidateWithSchema(schemaChecker); err != nil {
			return false, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
		}
	}

	// Serialize filter and patch to JSON for WAL persistence
	filterJSON, err := serializeFilterToJSON(req.Filter)
	if err != nil {
		return false, fmt.Errorf("failed to serialize filter: %w", err)
	}
	patchJSON, err := serializeUpdatesToJSON(req.Updates)
	if err != nil {
		return false, fmt.Errorf("failed to serialize updates: %w", err)
	}

	// Phase 1: Get candidate IDs at the current snapshot
	// We need a tail store to evaluate the filter
	if h.tailStore == nil {
		return false, fmt.Errorf("%w: patch_by_filter requires tail store", ErrFilterOpRequiresTail)
	}

	// Refresh tail to ensure we have current data
	if err := h.tailStore.Refresh(ctx, ns, 0, snapshotSeq); err != nil {
		return false, fmt.Errorf("failed to refresh tail: %w", err)
	}

	// Scan for matching documents
	docs, err := h.tailStore.Scan(ctx, ns, f)
	if err != nil {
		return false, fmt.Errorf("failed to scan for matching documents: %w", err)
	}

	// Check if we exceed the limit
	rowsRemaining := len(docs) > MaxPatchByFilterRows
	if rowsRemaining && !req.AllowPartial {
		return false, fmt.Errorf("%w: %d rows match filter, max is %d", ErrPatchByFilterTooMany, len(docs), MaxPatchByFilterRows)
	}

	// Limit the candidates if we're in partial mode
	candidateIDs := make([]document.ID, 0, len(docs))
	for i, doc := range docs {
		if i >= MaxPatchByFilterRows {
			break
		}
		candidateIDs = append(candidateIDs, doc.ID)
	}

	// Record filter operation metadata in WAL for deterministic replay.
	// Use AddFilterOp to support multiple filter operations in the same request.
	batch.AddFilterOp(&wal.FilterOperation{
		Type:              wal.FilterOperationType_FILTER_OPERATION_TYPE_PATCH,
		Phase1SnapshotSeq: snapshotSeq,
		CandidateIds:      documentIDsToProto(candidateIDs),
		FilterJson:        filterJSON,
		PatchJson:         patchJSON,
		AllowPartial:      req.AllowPartial,
	})

	// Canonicalize the updates once before the loop
	attrs := make(map[string]any, len(req.Updates))
	for k, v := range req.Updates {
		if k == "id" {
			continue
		}
		if k == "vector" {
			return false, ErrVectorPatchForbidden
		}
		if err := schema.ValidateAttributeName(k); err != nil {
			return false, fmt.Errorf("%w: %s: %v", ErrInvalidAttribute, k, err)
		}
		attrs[k] = v
	}
	canonAttrs, err := h.canon.CanonicalizeAttributes(attrs)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidAttribute, err)
	}

	// Phase 2: Re-evaluate filter for each candidate and patch those that still match
	// This implements "Read Committed" semantics - docs that newly qualify between
	// phases can be missed, and docs that no longer qualify are not patched
	for _, id := range candidateIDs {
		// Skip if this document was already deleted by delete_by_filter in this request
		// per spec: patch_by_filter runs AFTER delete_by_filter
		if batch.HasPendingDelete(id) {
			continue
		}

		// Get the document again to re-evaluate
		doc, err := h.tailStore.GetDocument(ctx, ns, id)
		if err != nil {
			continue // Skip on error
		}
		if doc == nil || doc.Deleted {
			continue // Document was deleted between phases
		}

		// Re-evaluate the filter
		filterDoc := make(filter.Document)
		for k, v := range doc.Attributes {
			filterDoc[k] = v
		}
		if !f.Eval(filterDoc) {
			continue // Document no longer matches filter
		}

		// Add patch mutation
		protoID := wal.DocumentIDFromID(id)
		batch.AddPatch(protoID, canonAttrs)
	}

	return rowsRemaining, nil
}

// processCopyFromNamespace performs a server-side bulk copy from a source namespace.
// It reads all documents from the source namespace at a consistent snapshot
// and adds them as upserts to the current batch.
func (h *Handler) processCopyFromNamespace(ctx context.Context, sourceNs string, batch *wal.WriteSubBatch) error {
	// We need a tail store to read source documents
	if h.tailStore == nil {
		return fmt.Errorf("%w: tail store required for copy_from_namespace", ErrInvalidRequest)
	}

	// Load source namespace state to get a consistent snapshot
	sourceLoaded, err := h.stateMan.Load(ctx, sourceNs)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrSourceNamespaceNotFound, sourceNs, err)
	}

	// Refresh tail for source namespace at consistent snapshot
	if err := h.tailStore.Refresh(ctx, sourceNs, 0, sourceLoaded.State.WAL.HeadSeq); err != nil {
		return fmt.Errorf("failed to refresh source namespace tail: %w", err)
	}

	// Scan all documents from source namespace (no filter = all docs)
	docs, err := h.tailStore.Scan(ctx, sourceNs, nil)
	if err != nil {
		return fmt.Errorf("failed to scan source namespace: %w", err)
	}

	// Add each document as an upsert
	for _, doc := range docs {
		if doc.Deleted {
			continue
		}

		protoID := wal.DocumentIDFromID(doc.ID)

		// Canonicalize attributes
		canonAttrs, err := h.canon.CanonicalizeAttributes(doc.Attributes)
		if err != nil {
			return fmt.Errorf("failed to canonicalize attributes from source doc %v: %w", doc.ID, err)
		}

		// Convert vector to byte slice if present
		var vectorData []byte
		var vectorDims uint32
		if len(doc.Vector) > 0 {
			vectorDims = uint32(len(doc.Vector))
			vectorData = vectorToBytes(doc.Vector)
		}

		batch.AddUpsert(protoID, canonAttrs, vectorData, vectorDims)
	}

	return nil
}

// processConditionalUpserts processes upserts with a condition.
// Conditional upsert semantics:
//   - If doc missing → apply unconditionally
//   - If doc exists and condition met → apply upsert
//   - If doc exists and condition not met → skip
//
// The condition can contain $ref_new references that are resolved
// to values from the new document being upserted.
func (h *Handler) processConditionalUpserts(ctx context.Context, ns string, snapshotSeq uint64, rows []map[string]any, condition any, batch *wal.WriteSubBatch) error {
	// We need a tail store to read existing documents
	// If no tail store, we cannot evaluate conditions against existing docs
	// In this case, treat all upserts as unconditional (applying them all)
	if h.tailStore == nil {
		for _, row := range rows {
			if err := h.processUpsertRow(row, batch); err != nil {
				return err
			}
		}
		return nil
	}

	// Refresh tail to ensure we have current data
	if err := h.tailStore.Refresh(ctx, ns, 0, snapshotSeq); err != nil {
		return fmt.Errorf("failed to refresh tail for conditional upsert: %w", err)
	}

	for _, row := range rows {
		// Extract and parse ID
		rawID, ok := row["id"]
		if !ok {
			return fmt.Errorf("%w: missing 'id' field", ErrInvalidRequest)
		}
		docID, err := document.ParseID(rawID)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidID, err)
		}

		// Get existing document
		existingDoc, err := h.tailStore.GetDocument(ctx, ns, docID)
		if err != nil {
			return fmt.Errorf("failed to get existing document: %w", err)
		}

		// Build new doc attributes for $ref_new resolution (exclude id and vector)
		newDocAttrs := make(map[string]any)
		for k, v := range row {
			if k == "id" || k == "vector" {
				continue
			}
			newDocAttrs[k] = v
		}

		// Apply upsert based on conditional semantics
		shouldApply, err := h.shouldApplyConditionalUpsert(existingDoc, condition, newDocAttrs)
		if err != nil {
			return err
		}
		if shouldApply {
			if err := h.processUpsertRow(row, batch); err != nil {
				return err
			}
		}
	}

	return nil
}

// shouldApplyConditionalUpsert determines whether an upsert should be applied.
// Conditional upsert semantics:
//   - doc missing → apply unconditionally (return true)
//   - doc exists + condition met → apply (return true)
//   - doc exists + condition not met → skip (return false)
func (h *Handler) shouldApplyConditionalUpsert(existingDoc *tail.Document, condition any, newDocAttrs map[string]any) (bool, error) {
	// If document doesn't exist, apply unconditionally
	if existingDoc == nil {
		return true, nil
	}

	// Document exists - evaluate condition
	// Resolve $ref_new references in condition using new doc attributes
	resolvedCondition := filter.ResolveRefNew(condition, newDocAttrs)

	// Parse the resolved condition
	f, err := filter.Parse(resolvedCondition)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}

	// If no filter (nil condition), apply unconditionally
	if f == nil {
		return true, nil
	}

	// Build filter document from existing doc attributes
	filterDoc := make(filter.Document)
	for k, v := range existingDoc.Attributes {
		filterDoc[k] = v
	}

	// Evaluate condition against existing document
	return f.Eval(filterDoc), nil
}

// processConditionalPatches processes patches with a condition.
// Conditional patch semantics (different from upsert):
//   - If doc missing → skip (patch is not applied, no doc created)
//   - If doc exists and condition met → apply patch
//   - If doc exists and condition not met → skip
//
// The condition can contain $ref_new references that are resolved
// to values from the patch data being applied.
func (h *Handler) processConditionalPatches(ctx context.Context, ns string, snapshotSeq uint64, rows []map[string]any, condition any, batch *wal.WriteSubBatch) error {
	// We need a tail store to read existing documents
	// If no tail store, we cannot evaluate conditions against existing docs
	// In this case, skip all patches since we can't verify doc existence
	if h.tailStore == nil {
		return nil
	}

	// Refresh tail to ensure we have current data
	if err := h.tailStore.Refresh(ctx, ns, 0, snapshotSeq); err != nil {
		return fmt.Errorf("failed to refresh tail for conditional patch: %w", err)
	}

	for _, row := range rows {
		// Extract and parse ID
		rawID, ok := row["id"]
		if !ok {
			return fmt.Errorf("%w: missing 'id' field", ErrInvalidRequest)
		}
		docID, err := document.ParseID(rawID)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidID, err)
		}

		// Get existing document
		existingDoc, err := h.tailStore.GetDocument(ctx, ns, docID)
		if err != nil {
			return fmt.Errorf("failed to get existing document: %w", err)
		}

		// Build patch attributes for $ref_new resolution (exclude id and vector)
		patchAttrs := make(map[string]any)
		for k, v := range row {
			if k == "id" || k == "vector" {
				continue
			}
			patchAttrs[k] = v
		}

		// Apply patch based on conditional semantics
		shouldApply, err := h.shouldApplyConditionalPatch(existingDoc, condition, patchAttrs)
		if err != nil {
			return err
		}
		if shouldApply {
			if err := h.processPatchRow(row, batch); err != nil {
				return err
			}
		}
	}

	return nil
}

// shouldApplyConditionalPatch determines whether a patch should be applied.
// Conditional patch semantics:
//   - doc missing → skip (return false) - unlike upsert!
//   - doc exists + condition met → apply (return true)
//   - doc exists + condition not met → skip (return false)
func (h *Handler) shouldApplyConditionalPatch(existingDoc *tail.Document, condition any, patchAttrs map[string]any) (bool, error) {
	// If document doesn't exist, skip (patches to missing docs are skipped)
	if existingDoc == nil || existingDoc.Deleted {
		return false, nil
	}

	// Document exists - evaluate condition
	// Resolve $ref_new references in condition using patch attributes
	resolvedCondition := filter.ResolveRefNew(condition, patchAttrs)

	// Parse the resolved condition
	f, err := filter.Parse(resolvedCondition)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}

	// If no filter (nil condition), apply unconditionally
	if f == nil {
		return true, nil
	}

	// Build filter document from existing doc attributes
	filterDoc := make(filter.Document)
	for k, v := range existingDoc.Attributes {
		filterDoc[k] = v
	}

	// Evaluate condition against existing document
	return f.Eval(filterDoc), nil
}

// processConditionalDeletes processes deletes with a condition.
// Conditional delete semantics:
//   - If doc missing → skip (can't delete what doesn't exist)
//   - If doc exists and condition met → delete
//   - If doc exists and condition not met → skip
//
// For $ref_new references in conditions, all attributes are supplied as null
// since deletion has no "new" values.
func (h *Handler) processConditionalDeletes(ctx context.Context, ns string, snapshotSeq uint64, ids []any, condition any, batch *wal.WriteSubBatch) error {
	// We need a tail store to read existing documents
	// If no tail store, we cannot evaluate conditions against existing docs
	// In this case, skip all deletes since we can't verify doc existence/conditions
	if h.tailStore == nil {
		return nil
	}

	// Refresh tail to ensure we have current data
	if err := h.tailStore.Refresh(ctx, ns, 0, snapshotSeq); err != nil {
		return fmt.Errorf("failed to refresh tail for conditional delete: %w", err)
	}

	for _, rawID := range ids {
		docID, err := document.ParseID(rawID)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidID, err)
		}

		// Get existing document
		existingDoc, err := h.tailStore.GetDocument(ctx, ns, docID)
		if err != nil {
			return fmt.Errorf("failed to get existing document: %w", err)
		}

		// Apply delete based on conditional semantics
		shouldApply, err := h.shouldApplyConditionalDelete(existingDoc, condition)
		if err != nil {
			return err
		}
		if shouldApply {
			if err := h.processDelete(rawID, batch); err != nil {
				return err
			}
		}
	}

	return nil
}

// shouldApplyConditionalDelete determines whether a delete should be applied.
// Conditional delete semantics:
//   - doc missing → skip (return false) - can't delete what doesn't exist
//   - doc exists + condition met → apply (return true)
//   - doc exists + condition not met → skip (return false)
//
// For $ref_new references, all attributes are resolved to null since
// deletion has no "new" values.
func (h *Handler) shouldApplyConditionalDelete(existingDoc *tail.Document, condition any) (bool, error) {
	// If document doesn't exist, skip (can't delete what doesn't exist)
	if existingDoc == nil || existingDoc.Deleted {
		return false, nil
	}

	// Document exists - evaluate condition
	// For delete_condition, $ref_new attributes are supplied as null
	// since deletion has no "new" value. We pass an empty map which will
	// cause all $ref_new.X references to resolve to null.
	nullAttrs := make(map[string]any) // Empty map means all $ref_new.X → null
	resolvedCondition := filter.ResolveRefNew(condition, nullAttrs)

	// Parse the resolved condition
	f, err := filter.Parse(resolvedCondition)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}

	// If no filter (nil condition), apply unconditionally
	if f == nil {
		return true, nil
	}

	// Build filter document from existing doc attributes
	filterDoc := make(filter.Document)
	for k, v := range existingDoc.Attributes {
		filterDoc[k] = v
	}

	// Evaluate condition against existing document
	return f.Eval(filterDoc), nil
}

// vectorToBytes converts a float32 slice to bytes (little-endian).
func vectorToBytes(v []float32) []byte {
	data := make([]byte, len(v)*4)
	for i, f := range v {
		bits := math.Float32bits(f)
		data[i*4] = byte(bits)
		data[i*4+1] = byte(bits >> 8)
		data[i*4+2] = byte(bits >> 16)
		data[i*4+3] = byte(bits >> 24)
	}
	return data
}

// processUpsertRow processes a single row for upsert.
func (h *Handler) processUpsertRow(row map[string]any, batch *wal.WriteSubBatch) error {
	// Extract and validate ID
	rawID, ok := row["id"]
	if !ok {
		return fmt.Errorf("%w: missing 'id' field", ErrInvalidRequest)
	}

	docID, err := h.canon.CanonicalizeID(rawID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidID, err)
	}

	// Extract vector if present
	var vectorData []byte
	var vectorDims uint32
	if v, ok := row["vector"]; ok {
		vectorData, vectorDims, err = h.canon.CanonicalizeVector(v)
		if err != nil {
			return fmt.Errorf("%w: vector: %v", ErrInvalidAttribute, err)
		}
	}

	// Process other attributes (excluding id and vector)
	attrs := make(map[string]any)
	for k, v := range row {
		if k == "id" || k == "vector" {
			continue
		}
		// Validate attribute name
		if err := schema.ValidateAttributeName(k); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrInvalidAttribute, k, err)
		}
		attrs[k] = v
	}

	canonAttrs, err := h.canon.CanonicalizeAttributes(attrs)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAttribute, err)
	}

	batch.AddUpsert(docID, canonAttrs, vectorData, vectorDims)
	return nil
}

// processDelete processes a single delete by ID.
func (h *Handler) processDelete(rawID any, batch *wal.WriteSubBatch) error {
	docID, err := h.canon.CanonicalizeID(rawID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidID, err)
	}

	batch.AddDelete(docID)
	return nil
}

// processPatchRow processes a single row for patching.
// Patches update only specified attributes - they do not include vectors.
// If the document does not exist, the patch is silently ignored (recorded but no-op at apply time).
func (h *Handler) processPatchRow(row map[string]any, batch *wal.WriteSubBatch) error {
	// Extract and validate ID
	rawID, ok := row["id"]
	if !ok {
		return fmt.Errorf("%w: missing 'id' field", ErrInvalidRequest)
	}

	docID, err := h.canon.CanonicalizeID(rawID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidID, err)
	}

	// Vector attribute cannot be patched - return 400
	if _, hasVector := row["vector"]; hasVector {
		return ErrVectorPatchForbidden
	}

	// Process other attributes (excluding id)
	attrs := make(map[string]any)
	for k, v := range row {
		if k == "id" {
			continue
		}
		// Validate attribute name
		if err := schema.ValidateAttributeName(k); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrInvalidAttribute, k, err)
		}
		attrs[k] = v
	}

	canonAttrs, err := h.canon.CanonicalizeAttributes(attrs)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAttribute, err)
	}

	batch.AddPatch(docID, canonAttrs)
	return nil
}

// deduplicatePatchRows removes duplicate IDs from the patch_rows array,
// keeping only the last occurrence (last-write-wins semantics).
func (h *Handler) deduplicatePatchRows(rows []map[string]any) ([]map[string]any, error) {
	if len(rows) == 0 {
		return rows, nil
	}

	// Track last occurrence index for each normalized ID
	idLastIdx := make(map[string]int, len(rows))
	for i, row := range rows {
		rawID, ok := row["id"]
		if !ok {
			return nil, fmt.Errorf("%w: missing 'id' field", ErrInvalidRequest)
		}
		id, err := document.ParseID(rawID)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidID, err)
		}
		idLastIdx[id.String()] = i
	}

	// If no duplicates, return original
	if len(idLastIdx) == len(rows) {
		return rows, nil
	}

	// Build deduplicated list preserving order of last occurrences
	result := make([]map[string]any, 0, len(idLastIdx))
	seen := make(map[string]bool, len(idLastIdx))
	for i, row := range rows {
		rawID := row["id"]
		id, _ := document.ParseID(rawID)
		key := id.String()
		if idLastIdx[key] == i && !seen[key] {
			result = append(result, row)
			seen[key] = true
		}
	}
	return result, nil
}

// deduplicateUpsertRows removes duplicate IDs from the upsert_rows array,
// keeping only the last occurrence (last-write-wins semantics).
func (h *Handler) deduplicateUpsertRows(rows []map[string]any) ([]map[string]any, error) {
	if len(rows) == 0 {
		return rows, nil
	}

	// Track last occurrence index for each normalized ID
	idLastIdx := make(map[string]int, len(rows))
	for i, row := range rows {
		rawID, ok := row["id"]
		if !ok {
			return nil, fmt.Errorf("%w: missing 'id' field", ErrInvalidRequest)
		}
		id, err := document.ParseID(rawID)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidID, err)
		}
		idLastIdx[id.String()] = i
	}

	// If no duplicates, return original
	if len(idLastIdx) == len(rows) {
		return rows, nil
	}

	// Build deduplicated list preserving order of last occurrences
	result := make([]map[string]any, 0, len(idLastIdx))
	seen := make(map[string]bool, len(idLastIdx))
	for i, row := range rows {
		rawID := row["id"]
		id, _ := document.ParseID(rawID)
		key := id.String()
		if idLastIdx[key] == i && !seen[key] {
			result = append(result, row)
			seen[key] = true
		}
	}
	return result, nil
}

// ParseWriteRequest parses a write request from a JSON body map.
// If request_id is provided in the body, it takes precedence over the parameter.
func ParseWriteRequest(requestID string, body map[string]any) (*WriteRequest, error) {
	req := &WriteRequest{
		RequestID: requestID,
	}

	// Parse request_id from body if provided (takes precedence)
	if rid, ok := body["request_id"]; ok {
		if ridStr, ok := rid.(string); ok && ridStr != "" {
			req.RequestID = ridStr
		}
	}

	// Parse upsert_rows
	if rows, ok := body["upsert_rows"]; ok {
		rowsSlice, ok := rows.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: upsert_rows must be an array", ErrInvalidRequest)
		}
		req.UpsertRows = make([]map[string]any, 0, len(rowsSlice))
		for i, r := range rowsSlice {
			rowMap, ok := r.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: upsert_rows[%d] must be an object", ErrInvalidRequest, i)
			}
			req.UpsertRows = append(req.UpsertRows, rowMap)
		}
	}

	// Parse upsert_columns (columnar format)
	if cols, ok := body["upsert_columns"]; ok {
		colsMap, ok := cols.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: upsert_columns must be an object", ErrInvalidRequest)
		}
		columnarRows, err := ParseColumnarToRows(colsMap)
		if err != nil {
			return nil, err
		}
		// Append columnar rows after upsert_rows (same phase in ordering)
		req.UpsertRows = append(req.UpsertRows, columnarRows...)
	}

	// Parse patch_rows
	if rows, ok := body["patch_rows"]; ok {
		rowsSlice, ok := rows.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: patch_rows must be an array", ErrInvalidRequest)
		}
		req.PatchRows = make([]map[string]any, 0, len(rowsSlice))
		for i, r := range rowsSlice {
			rowMap, ok := r.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%w: patch_rows[%d] must be an object", ErrInvalidRequest, i)
			}
			req.PatchRows = append(req.PatchRows, rowMap)
		}
	}

	// Parse patch_columns (columnar format for patches)
	if cols, ok := body["patch_columns"]; ok {
		colsMap, ok := cols.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: patch_columns must be an object", ErrInvalidRequest)
		}
		columnarRows, err := ParseColumnarToPatchRows(colsMap)
		if err != nil {
			return nil, err
		}
		// Append columnar rows after patch_rows (same phase in ordering)
		req.PatchRows = append(req.PatchRows, columnarRows...)
	}

	// Parse deletes
	if deletes, ok := body["deletes"]; ok {
		deletesSlice, ok := deletes.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: deletes must be an array", ErrInvalidRequest)
		}
		req.Deletes = deletesSlice
	}

	// Parse delete_by_filter
	if dbf, ok := body["delete_by_filter"]; ok {
		req.DeleteByFilter = &DeleteByFilterRequest{
			Filter: dbf,
		}
		// Check for delete_by_filter_allow_partial flag
		if allowPartial, ok := body["delete_by_filter_allow_partial"]; ok {
			if b, ok := allowPartial.(bool); ok {
				req.DeleteByFilter.AllowPartial = b
			}
		}
	}

	// Parse patch_by_filter
	if pbf, ok := body["patch_by_filter"]; ok {
		pbfMap, ok := pbf.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: patch_by_filter must be an object", ErrInvalidRequest)
		}

		filterExpr, hasFilter := pbfMap["filter"]
		if !hasFilter {
			return nil, fmt.Errorf("%w: patch_by_filter missing 'filter' field", ErrInvalidRequest)
		}

		updates, hasUpdates := pbfMap["updates"]
		if !hasUpdates {
			return nil, fmt.Errorf("%w: patch_by_filter missing 'updates' field", ErrInvalidRequest)
		}
		updatesMap, ok := updates.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: patch_by_filter 'updates' must be an object", ErrInvalidRequest)
		}

		req.PatchByFilter = &PatchByFilterRequest{
			Filter:  filterExpr,
			Updates: updatesMap,
		}

		// Check for patch_by_filter_allow_partial flag
		if allowPartial, ok := body["patch_by_filter_allow_partial"]; ok {
			if b, ok := allowPartial.(bool); ok {
				req.PatchByFilter.AllowPartial = b
			}
		}
	}

	// Parse copy_from_namespace
	if cfn, ok := body["copy_from_namespace"]; ok {
		cfnStr, ok := cfn.(string)
		if !ok {
			return nil, fmt.Errorf("%w: copy_from_namespace must be a string", ErrInvalidRequest)
		}
		req.CopyFromNamespace = cfnStr
	}

	// Parse schema
	if schemaVal, ok := body["schema"]; ok {
		schemaUpdate, err := ParseSchemaUpdate(schemaVal)
		if err != nil {
			return nil, err
		}
		req.Schema = schemaUpdate
	}

	// Parse upsert_condition
	if cond, ok := body["upsert_condition"]; ok {
		req.UpsertCondition = cond
	}

	// Parse patch_condition
	if cond, ok := body["patch_condition"]; ok {
		req.PatchCondition = cond
	}

	// Parse delete_condition
	if cond, ok := body["delete_condition"]; ok {
		req.DeleteCondition = cond
	}

	// Parse disable_backpressure
	if dbp, ok := body["disable_backpressure"]; ok {
		if b, ok := dbp.(bool); ok {
			req.DisableBackpressure = b
		}
	}

	// Parse distance_metric
	if dm, ok := body["distance_metric"]; ok {
		dmStr, ok := dm.(string)
		if !ok {
			return nil, fmt.Errorf("%w: distance_metric must be a string", ErrInvalidRequest)
		}
		if dmStr == "" {
			return nil, fmt.Errorf("%w: distance_metric must be non-empty", ErrInvalidDistanceMetric)
		}
		req.DistanceMetric = dmStr
	}

	return req, nil
}

// ParseSchemaUpdate parses a schema object from a write request.
// The schema format is: {"attr_name": {"type": "string", "filterable": true, ...}, ...}
func ParseSchemaUpdate(v any) (*SchemaUpdate, error) {
	if v == nil {
		return nil, nil
	}

	schemaMap, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: schema must be an object", ErrInvalidSchema)
	}

	update := &SchemaUpdate{
		Attributes: make(map[string]AttributeSchemaUpdate),
	}

	for attrName, attrVal := range schemaMap {
		// Validate attribute name
		if err := schema.ValidateAttributeName(attrName); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidSchema, err)
		}

		attrMap, ok := attrVal.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: schema attribute %q must be an object", ErrInvalidSchema, attrName)
		}

		attrUpdate := AttributeSchemaUpdate{}

		// Parse type (required)
		if typeVal, ok := attrMap["type"]; ok {
			typeStr, ok := typeVal.(string)
			if !ok {
				return nil, fmt.Errorf("%w: schema attribute %q type must be a string", ErrInvalidSchema, attrName)
			}
			attrUpdate.Type = typeStr
		}

		// Parse filterable (optional)
		if filterableVal, ok := attrMap["filterable"]; ok {
			filterableBool, ok := filterableVal.(bool)
			if !ok {
				return nil, fmt.Errorf("%w: schema attribute %q filterable must be a boolean", ErrInvalidSchema, attrName)
			}
			attrUpdate.Filterable = &filterableBool
		}

		// Parse regex (optional)
		if regexVal, ok := attrMap["regex"]; ok {
			regexBool, ok := regexVal.(bool)
			if !ok {
				return nil, fmt.Errorf("%w: schema attribute %q regex must be a boolean", ErrInvalidSchema, attrName)
			}
			attrUpdate.Regex = &regexBool
		}

		// Parse full_text_search (optional) - can be bool or object
		if ftsVal, ok := attrMap["full_text_search"]; ok {
			attrUpdate.FullTextSearch = ftsVal
		}

		update.Attributes[attrName] = attrUpdate
	}

	return update, nil
}

// ValidateAndConvertSchemaUpdate validates the schema update against the existing schema
// and converts it to a namespace.Schema for the state manager.
// Returns ErrSchemaTypeChange if any attribute type changes are attempted.
func ValidateAndConvertSchemaUpdate(update *SchemaUpdate, existingSchema *namespace.Schema) (*namespace.Schema, error) {
	if update == nil || len(update.Attributes) == 0 {
		return nil, nil
	}

	result := &namespace.Schema{
		Attributes: make(map[string]namespace.AttributeSchema),
	}

	for name, attrUpdate := range update.Attributes {
		// Validate type is valid if provided
		if attrUpdate.Type != "" {
			attrType := schema.AttrType(attrUpdate.Type)
			if !attrType.IsValid() {
				return nil, fmt.Errorf("%w: attribute %q has invalid type %q", ErrInvalidSchema, name, attrUpdate.Type)
			}
		}

		// Check if attribute exists in current schema
		var existingAttr *namespace.AttributeSchema
		if existingSchema != nil && existingSchema.Attributes != nil {
			if ea, exists := existingSchema.Attributes[name]; exists {
				existingAttr = &ea
			}
		}

		if existingAttr != nil {
			// Attribute exists - validate type doesn't change
			if attrUpdate.Type != "" && attrUpdate.Type != existingAttr.Type {
				return nil, fmt.Errorf("%w: attribute %q has type %q, cannot change to %q",
					ErrSchemaTypeChange, name, existingAttr.Type, attrUpdate.Type)
			}

			// Build the updated attribute from existing values.
			newAttr := *existingAttr

			// Apply updates
			if attrUpdate.Filterable != nil {
				newAttr.Filterable = attrUpdate.Filterable
			}
			if attrUpdate.Regex != nil {
				newAttr.Regex = *attrUpdate.Regex
			}
			if attrUpdate.FullTextSearch != nil {
				ftsBytes, err := encodeFullTextSearch(attrUpdate.FullTextSearch)
				if err != nil {
					return nil, fmt.Errorf("%w: attribute %q: %v", ErrInvalidSchema, name, err)
				}
				newAttr.FullTextSearch = ftsBytes
			}

			result.Attributes[name] = newAttr
		} else {
			// New attribute - type is required
			if attrUpdate.Type == "" {
				return nil, fmt.Errorf("%w: new attribute %q requires type", ErrInvalidSchema, name)
			}

			newAttr := namespace.AttributeSchema{
				Type: attrUpdate.Type,
			}

			if attrUpdate.Filterable != nil {
				newAttr.Filterable = attrUpdate.Filterable
			}
			if attrUpdate.Regex != nil {
				newAttr.Regex = *attrUpdate.Regex
			}
			if attrUpdate.FullTextSearch != nil {
				ftsBytes, err := encodeFullTextSearch(attrUpdate.FullTextSearch)
				if err != nil {
					return nil, fmt.Errorf("%w: attribute %q: %v", ErrInvalidSchema, name, err)
				}
				newAttr.FullTextSearch = ftsBytes
			}

			result.Attributes[name] = newAttr
		}
	}

	return result, nil
}

// encodeFullTextSearch encodes the full_text_search value as JSON bytes.
func encodeFullTextSearch(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	switch val := v.(type) {
	case bool:
		if val {
			return json.RawMessage(`true`), nil
		}
		return nil, nil // false means disabled, don't set
	case map[string]any:
		data, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("failed to encode full_text_search: %w", err)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("full_text_search must be a boolean or object, got %T", v)
	}
}

// ValidateColumnarIDs checks for duplicate IDs in columnar format.
func ValidateColumnarIDs(ids []any) error {
	seen := make(map[string]bool, len(ids))
	for _, rawID := range ids {
		// Parse to normalize the ID
		id, err := document.ParseID(rawID)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidID, err)
		}
		key := id.String()
		if seen[key] {
			return ErrDuplicateIDColumn
		}
		seen[key] = true
	}
	return nil
}

// ParseColumnarToRows converts columnar format to row format.
// The columnar format is: {"ids": [id1, id2, ...], "attr1": [val1, val2, ...], ...}
// All attribute arrays must have the same length as the ids array.
func ParseColumnarToRows(cols map[string]any) ([]map[string]any, error) {
	// Extract ids array
	rawIDs, ok := cols["ids"]
	if !ok {
		return nil, fmt.Errorf("%w: upsert_columns missing 'ids' field", ErrInvalidRequest)
	}
	ids, ok := rawIDs.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: upsert_columns 'ids' must be an array", ErrInvalidRequest)
	}

	// Validate for duplicate IDs
	if err := ValidateColumnarIDs(ids); err != nil {
		return nil, err
	}

	// Build rows from columnar data
	rows := make([]map[string]any, len(ids))
	for i := range rows {
		rows[i] = map[string]any{"id": ids[i]}
	}

	// Process each attribute column
	for key, rawValues := range cols {
		if key == "ids" {
			continue
		}
		values, ok := rawValues.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: upsert_columns attribute '%s' must be an array", ErrInvalidRequest, key)
		}
		if len(values) != len(ids) {
			return nil, fmt.Errorf("%w: upsert_columns attribute '%s' has %d elements, expected %d (same as ids)", ErrInvalidRequest, key, len(values), len(ids))
		}
		for i, v := range values {
			rows[i][key] = v
		}
	}

	return rows, nil
}

// ParseColumnarToPatchRows converts columnar format to row format for patches.
// Like ParseColumnarToRows but rejects vector attributes since they cannot be patched.
// The columnar format is: {"ids": [id1, id2, ...], "attr1": [val1, val2, ...], ...}
// All attribute arrays must have the same length as the ids array.
func ParseColumnarToPatchRows(cols map[string]any) ([]map[string]any, error) {
	// Extract ids array
	rawIDs, ok := cols["ids"]
	if !ok {
		return nil, fmt.Errorf("%w: patch_columns missing 'ids' field", ErrInvalidRequest)
	}
	ids, ok := rawIDs.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: patch_columns 'ids' must be an array", ErrInvalidRequest)
	}

	// Validate for duplicate IDs (columnar patches reject duplicates with 400)
	if err := ValidateColumnarIDs(ids); err != nil {
		return nil, err
	}

	// Check for vector attribute - patches cannot modify vectors
	if _, hasVector := cols["vector"]; hasVector {
		return nil, ErrVectorPatchForbidden
	}

	// Build rows from columnar data
	rows := make([]map[string]any, len(ids))
	for i := range rows {
		rows[i] = map[string]any{"id": ids[i]}
	}

	// Process each attribute column
	for key, rawValues := range cols {
		if key == "ids" {
			continue
		}
		values, ok := rawValues.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: patch_columns attribute '%s' must be an array", ErrInvalidRequest, key)
		}
		if len(values) != len(ids) {
			return nil, fmt.Errorf("%w: patch_columns attribute '%s' has %d elements, expected %d (same as ids)", ErrInvalidRequest, key, len(values), len(ids))
		}
		for i, v := range values {
			rows[i][key] = v
		}
	}

	return rows, nil
}

// buildSchemaChecker creates a filter.SchemaChecker from namespace schema.
func buildSchemaChecker(schema *namespace.Schema) filter.SchemaChecker {
	regexAttrs := make(map[string]bool)
	if schema != nil {
		for name, attr := range schema.Attributes {
			if attr.Regex {
				regexAttrs[name] = true
			}
		}
	}
	return filter.NewNamespaceSchemaAdapter(regexAttrs)
}

// ValidateDistanceMetric validates a distance_metric value against the compatibility mode.
// Returns an error if the metric is not allowed in the given compat mode.
func ValidateDistanceMetric(metric string, compatMode string) error {
	// Valid metrics: cosine_distance, euclidean_squared, dot_product (Vex-only)
	switch metric {
	case "cosine_distance", "euclidean_squared":
		return nil
	case "dot_product":
		// dot_product is only allowed in "vex" compat mode
		if compatMode == "turbopuffer" || compatMode == "" {
			return ErrDotProductNotAllowed
		}
		return nil
	default:
		return fmt.Errorf("%w: %q is not a valid distance metric", ErrInvalidDistanceMetric, metric)
	}
}

// serializeFilterToJSON converts a filter expression to JSON for WAL persistence.
func serializeFilterToJSON(filter any) (string, error) {
	if filter == nil {
		return "", nil
	}
	data, err := json.Marshal(filter)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// serializeUpdatesToJSON converts update attributes to JSON for WAL persistence.
func serializeUpdatesToJSON(updates map[string]any) (string, error) {
	if updates == nil || len(updates) == 0 {
		return "", nil
	}
	data, err := json.Marshal(updates)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// documentIDsToProto converts a slice of document.ID to WAL proto format.
func documentIDsToProto(ids []document.ID) []*wal.DocumentID {
	result := make([]*wal.DocumentID, len(ids))
	for i, id := range ids {
		result[i] = wal.DocumentIDFromID(id)
	}
	return result
}
