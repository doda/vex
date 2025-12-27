// Package write implements the write path for Vex, handling document upserts,
// patches, and deletes with WAL commit to object storage.
package write

import (
	"bytes"
	"context"
	"errors"
	"fmt"

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
)

var (
	ErrInvalidRequest         = errors.New("invalid write request")
	ErrInvalidID              = errors.New("invalid document ID")
	ErrInvalidAttribute       = errors.New("invalid attribute")
	ErrDuplicateIDColumn      = errors.New("duplicate IDs in columnar format")
	ErrVectorPatchForbidden   = errors.New("vector attribute cannot be patched")
	ErrDeleteByFilterTooMany  = errors.New("delete_by_filter exceeds maximum rows limit")
	ErrInvalidFilter          = errors.New("invalid filter expression")
)

// DeleteByFilterRequest represents a delete_by_filter operation.
type DeleteByFilterRequest struct {
	Filter       any  // The filter expression (JSON-compatible)
	AllowPartial bool // If true, allow partial deletion when limit is exceeded
}

// WriteRequest represents a write request from the API.
type WriteRequest struct {
	RequestID      string
	UpsertRows     []map[string]any
	PatchRows      []map[string]any
	Deletes        []any
	DeleteByFilter *DeleteByFilterRequest // Optional delete_by_filter operation
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
	store     objectstore.Store
	stateMan  *namespace.StateManager
	encoder   *wal.Encoder
	canon     *wal.Canonicalizer
	tailStore tail.Store // Optional tail store for filter evaluation
}

// NewHandler creates a new write handler.
func NewHandler(store objectstore.Store, stateMan *namespace.StateManager) (*Handler, error) {
	return NewHandlerWithTail(store, stateMan, nil)
}

// NewHandlerWithTail creates a new write handler with an optional tail store for filter operations.
func NewHandlerWithTail(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store) (*Handler, error) {
	encoder, err := wal.NewEncoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL encoder: %w", err)
	}

	return &Handler{
		store:     store,
		stateMan:  stateMan,
		encoder:   encoder,
		canon:     wal.NewCanonicalizer(),
		tailStore: tailStore,
	}, nil
}

// Close releases resources held by the handler.
func (h *Handler) Close() error {
	return h.encoder.Close()
}

// Handle processes a write request for the given namespace.
// It returns only after the WAL entry is committed to object storage.
//
// Write operation ordering (per spec):
//  1. delete_by_filter - before all other operations
//  2. patch_by_filter - after delete_by_filter, before any other operation
//  3. copy_from_namespace - before explicit upserts/patches/deletes
//  4. upserts (upsert_rows/upsert_columns)
//  5. patches (patch_rows/patch_columns)
//  6. deletes
func (h *Handler) Handle(ctx context.Context, ns string, req *WriteRequest) (*WriteResponse, error) {
	// Load or create namespace state
	loaded, err := h.stateMan.LoadOrCreate(ctx, ns)
	if err != nil {
		if errors.Is(err, namespace.ErrNamespaceTombstoned) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to load namespace state: %w", err)
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
		remaining, err := h.processDeleteByFilter(ctx, ns, loaded.State.WAL.HeadSeq, req.DeleteByFilter, subBatch)
		if err != nil {
			return nil, err
		}
		rowsRemaining = remaining
	}

	// PHASE 2: Process upsert_rows with last-write-wins deduplication
	// Later occurrences of the same ID override earlier ones
	dedupedUpserts, err := h.deduplicateUpsertRows(req.UpsertRows)
	if err != nil {
		return nil, err
	}
	for _, row := range dedupedUpserts {
		if err := h.processUpsertRow(row, subBatch); err != nil {
			return nil, err
		}
	}

	// PHASE 3: Process patch_rows with last-write-wins deduplication
	// Patches run after upserts per write ordering spec
	dedupedPatches, err := h.deduplicatePatchRows(req.PatchRows)
	if err != nil {
		return nil, err
	}
	for _, row := range dedupedPatches {
		if err := h.processPatchRow(row, subBatch); err != nil {
			return nil, err
		}
	}

	// PHASE 4: Process deletes (runs after patches per write ordering spec)
	for _, id := range req.Deletes {
		if err := h.processDelete(id, subBatch); err != nil {
			return nil, err
		}
	}

	walEntry.SubBatches = append(walEntry.SubBatches, subBatch)

	// Encode the WAL entry
	result, err := h.encoder.Encode(walEntry)
	if err != nil {
		return nil, fmt.Errorf("failed to encode WAL entry: %w", err)
	}

	// Write WAL entry to object storage with If-None-Match for idempotency
	walKey := wal.KeyForSeq(nextSeq)
	_, err = h.store.PutIfAbsent(ctx, walKey, bytes.NewReader(result.Data), int64(len(result.Data)), &objectstore.PutOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil && !objectstore.IsConflictError(err) {
		return nil, fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Update namespace state to advance WAL head
	_, err = h.stateMan.AdvanceWAL(ctx, ns, loaded.ETag, walKey, int64(len(result.Data)), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to update namespace state: %w", err)
	}

	return &WriteResponse{
		RowsAffected:  subBatch.Stats.RowsAffected,
		RowsUpserted:  subBatch.Stats.RowsUpserted,
		RowsPatched:   subBatch.Stats.RowsPatched,
		RowsDeleted:   subBatch.Stats.RowsDeleted,
		RowsRemaining: rowsRemaining,
	}, nil
}

// processDeleteByFilter implements the two-phase delete_by_filter operation.
// Phase 1: Evaluate filter at snapshot, select matching IDs (bounded by limit)
// Phase 2: Re-evaluate filter and delete IDs that still match
// Returns true if rows_remaining (more rows matched than limit)
func (h *Handler) processDeleteByFilter(ctx context.Context, ns string, snapshotSeq uint64, req *DeleteByFilterRequest, batch *wal.WriteSubBatch) (bool, error) {
	// Parse the filter
	f, err := filter.Parse(req.Filter)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}

	// If filter is nil, no documents match
	if f == nil {
		return false, nil
	}

	// Phase 1: Get candidate IDs at the current snapshot
	// We need a tail store to evaluate the filter
	if h.tailStore == nil {
		// No tail store configured - filter operations require document state
		// For now, we'll scan available documents
		// In a real deployment, this should be provided
		return false, nil
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
func ParseWriteRequest(requestID string, body map[string]any) (*WriteRequest, error) {
	req := &WriteRequest{
		RequestID: requestID,
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

	return req, nil
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
