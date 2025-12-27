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
	ErrInvalidRequest    = errors.New("invalid write request")
	ErrInvalidID         = errors.New("invalid document ID")
	ErrInvalidAttribute  = errors.New("invalid attribute")
	ErrDuplicateIDColumn = errors.New("duplicate IDs in columnar format")
)

// WriteRequest represents a write request from the API.
type WriteRequest struct {
	RequestID   string
	UpsertRows  []map[string]any
	Deletes     []any
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

	// Process upsert_rows
	for _, row := range req.UpsertRows {
		if err := h.processUpsertRow(row, subBatch); err != nil {
			return nil, err
		}
	}

	// Process deletes
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
