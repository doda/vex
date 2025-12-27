// Package write implements the write path for Vex, handling document upserts,
// patches, and deletes with WAL commit to object storage.
package write

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/schema"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/pkg/objectstore"
)

var (
	ErrInvalidRequest      = errors.New("invalid write request")
	ErrInvalidID           = errors.New("invalid document ID")
	ErrInvalidAttribute    = errors.New("invalid attribute")
	ErrDuplicateIDColumn   = errors.New("duplicate IDs in columnar format")
	ErrVectorPatchForbidden = errors.New("vector attribute cannot be patched")
)

// WriteRequest represents a write request from the API.
type WriteRequest struct {
	RequestID   string
	UpsertRows  []map[string]any
	PatchRows   []map[string]any
	Deletes     []any
}

// ColumnarData represents columnar format data with ids and attribute arrays.
type ColumnarData struct {
	IDs        []any
	Attributes map[string][]any
}

// WriteResponse contains the result of a write operation.
type WriteResponse struct {
	RowsAffected int64 `json:"rows_affected"`
	RowsUpserted int64 `json:"rows_upserted"`
	RowsPatched  int64 `json:"rows_patched"`
	RowsDeleted  int64 `json:"rows_deleted"`
}

// Handler handles write operations for a namespace.
type Handler struct {
	store    objectstore.Store
	stateMan *namespace.StateManager
	encoder  *wal.Encoder
	canon    *wal.Canonicalizer
}

// NewHandler creates a new write handler.
func NewHandler(store objectstore.Store, stateMan *namespace.StateManager) (*Handler, error) {
	encoder, err := wal.NewEncoder()
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL encoder: %w", err)
	}

	return &Handler{
		store:    store,
		stateMan: stateMan,
		encoder:  encoder,
		canon:    wal.NewCanonicalizer(),
	}, nil
}

// Close releases resources held by the handler.
func (h *Handler) Close() error {
	return h.encoder.Close()
}

// Handle processes a write request for the given namespace.
// It returns only after the WAL entry is committed to object storage.
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

	// Process upsert_rows with last-write-wins deduplication
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

	// Process patch_rows with last-write-wins deduplication
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

	// Process deletes (runs after patches per write ordering spec)
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
		RowsAffected: subBatch.Stats.RowsAffected,
		RowsUpserted: subBatch.Stats.RowsUpserted,
		RowsPatched:  subBatch.Stats.RowsPatched,
		RowsDeleted:  subBatch.Stats.RowsDeleted,
	}, nil
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
