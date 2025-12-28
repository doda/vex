// Package query implements the query execution engine for Vex.
package query

import (
	"context"
	"errors"
	"sort"

	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/pkg/objectstore"
)

const (
	// MaxTopK is the maximum allowed limit/top_k value.
	MaxTopK = 10_000

	// DefaultLimit is the default result limit if not specified.
	DefaultLimit = 10
)

var (
	ErrRankByRequired       = errors.New("rank_by is required unless aggregate_by is specified")
	ErrInvalidRankBy        = errors.New("invalid rank_by expression")
	ErrInvalidLimit         = errors.New("limit/top_k must be between 1 and 10,000")
	ErrInvalidFilter        = errors.New("invalid filter expression")
	ErrInvalidRequest       = errors.New("invalid query request")
	ErrNamespaceNotFound    = errors.New("namespace not found")
	ErrAttributeConflict    = errors.New("cannot specify both include_attributes and exclude_attributes")
	ErrInvalidVectorEncoding = errors.New("vector_encoding must be 'float' or 'base64'")
)

// QueryRequest represents a query request from the API.
type QueryRequest struct {
	RankBy            any      // Required unless AggregateBy is set
	Filters           any      // Optional filter expression
	IncludeAttributes []string // Optional: attributes to include in response
	ExcludeAttributes []string // Optional: attributes to exclude from response
	Limit             int      // Alias: top_k - max results to return (default 10, max 10,000)
	AggregateBy       any      // Optional: aggregation specification
	GroupBy           any      // Optional: group-by specification
	VectorEncoding    string   // Optional: "float" (default) or "base64"
	Consistency       string   // Optional: "strong" (default) or "eventual"
}

// QueryResponse represents the response from a query.
type QueryResponse struct {
	Rows        []Row                  `json:"rows"`
	Billing     BillingInfo            `json:"billing"`
	Performance PerformanceInfo        `json:"performance"`
}

// Row represents a single result row.
type Row struct {
	ID         any            `json:"id"`
	Dist       *float64       `json:"$dist,omitempty"` // Distance/score, omitted for order-by queries
	Attributes map[string]any `json:"-"`               // Additional attributes are merged at serialization
}

// BillingInfo contains billing-related metrics.
type BillingInfo struct {
	BillableLogicalBytesQueried  int64 `json:"billable_logical_bytes_queried"`
	BillableLogicalBytesReturned int64 `json:"billable_logical_bytes_returned"`
}

// PerformanceInfo contains performance-related metrics.
type PerformanceInfo struct {
	CacheTemperature string  `json:"cache_temperature"`
	ServerTotalMs    float64 `json:"server_total_ms"`
}

// RankByType indicates the type of ranking.
type RankByType int

const (
	RankByVector RankByType = iota // ["vector", "ANN", <vector>]
	RankByBM25                     // ["field", "BM25", "query"]
	RankByAttr                     // ["attr", "asc|desc"]
)

// ParsedRankBy holds the parsed rank_by expression.
type ParsedRankBy struct {
	Type        RankByType
	Field       string     // For attribute or BM25 ranking
	Direction   string     // "asc" or "desc" for attribute ranking
	QueryVector []float32  // For vector ANN
}

// Handler handles query operations.
type Handler struct {
	store     objectstore.Store
	stateMan  *namespace.StateManager
	tailStore tail.Store
}

// NewHandler creates a new query handler.
func NewHandler(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store) *Handler {
	return &Handler{
		store:     store,
		stateMan:  stateMan,
		tailStore: tailStore,
	}
}

// Handle executes a query against the given namespace.
func (h *Handler) Handle(ctx context.Context, ns string, req *QueryRequest) (*QueryResponse, error) {
	// Validate the request
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	// Load namespace state
	loaded, err := h.stateMan.Load(ctx, ns)
	if err != nil {
		if errors.Is(err, namespace.ErrStateNotFound) {
			return nil, ErrNamespaceNotFound
		}
		if errors.Is(err, namespace.ErrNamespaceTombstoned) {
			return nil, namespace.ErrNamespaceTombstoned
		}
		return nil, err
	}

	// Refresh tail to WAL head for strong consistency
	if req.Consistency != "eventual" {
		if h.tailStore != nil {
			headSeq := loaded.State.WAL.HeadSeq
			indexedSeq := loaded.State.Index.IndexedWALSeq
			if headSeq > indexedSeq {
				if err := h.tailStore.Refresh(ctx, ns, indexedSeq, headSeq); err != nil {
					return nil, err
				}
			}
		}
	}

	// Parse the rank_by expression
	parsed, err := parseRankBy(req.RankBy)
	if err != nil {
		return nil, err
	}

	// Parse filters if provided
	var f *filter.Filter
	if req.Filters != nil {
		f, err = filter.Parse(req.Filters)
		if err != nil {
			return nil, ErrInvalidFilter
		}
	}

	// Execute the query based on rank_by type
	var rows []Row
	if parsed != nil {
		switch parsed.Type {
		case RankByVector:
			rows, err = h.executeVectorQuery(ctx, ns, parsed, f, req)
		case RankByAttr:
			rows, err = h.executeAttrQuery(ctx, ns, parsed, f, req)
		case RankByBM25:
			rows, err = h.executeBM25Query(ctx, ns, parsed, f, req)
		}
		if err != nil {
			return nil, err
		}
	}
	// If parsed is nil, it means aggregate_by is set without rank_by
	// Aggregation is not yet implemented, so return empty rows

	// Apply attribute filtering
	rows = filterAttributes(rows, req.IncludeAttributes, req.ExcludeAttributes)

	return &QueryResponse{
		Rows: rows,
		Billing: BillingInfo{
			BillableLogicalBytesQueried:  0,
			BillableLogicalBytesReturned: 0,
		},
		Performance: PerformanceInfo{
			CacheTemperature: "cold",
			ServerTotalMs:    0,
		},
	}, nil
}

// validateRequest validates the query request.
func (h *Handler) validateRequest(req *QueryRequest) error {
	// rank_by is required unless aggregate_by is specified
	if req.RankBy == nil && req.AggregateBy == nil {
		return ErrRankByRequired
	}

	// Validate limit
	if req.Limit < 0 || req.Limit > MaxTopK {
		return ErrInvalidLimit
	}
	if req.Limit == 0 {
		req.Limit = DefaultLimit
	}

	// Cannot specify both include and exclude
	if len(req.IncludeAttributes) > 0 && len(req.ExcludeAttributes) > 0 {
		return ErrAttributeConflict
	}

	// Validate vector_encoding if specified
	if req.VectorEncoding != "" && req.VectorEncoding != "float" && req.VectorEncoding != "base64" {
		return ErrInvalidVectorEncoding
	}

	return nil
}

// parseRankBy parses the rank_by expression.
func parseRankBy(rankBy any) (*ParsedRankBy, error) {
	if rankBy == nil {
		return nil, nil
	}

	arr, ok := rankBy.([]any)
	if !ok {
		return nil, ErrInvalidRankBy
	}

	if len(arr) < 2 {
		return nil, ErrInvalidRankBy
	}

	field, ok := arr[0].(string)
	if !ok {
		return nil, ErrInvalidRankBy
	}

	// Check for vector ANN: ["vector", "ANN", <vector>]
	if field == "vector" {
		if len(arr) < 3 {
			return nil, ErrInvalidRankBy
		}
		op, ok := arr[1].(string)
		if !ok || op != "ANN" {
			return nil, ErrInvalidRankBy
		}
		vec, err := parseVector(arr[2])
		if err != nil {
			return nil, err
		}
		return &ParsedRankBy{
			Type:        RankByVector,
			QueryVector: vec,
		}, nil
	}

	// Check for BM25: ["field", "BM25", "query"]
	if len(arr) >= 3 {
		op, ok := arr[1].(string)
		if ok && op == "BM25" {
			return &ParsedRankBy{
				Type:  RankByBM25,
				Field: field,
			}, nil
		}
	}

	// Attribute ordering: ["attr", "asc|desc"]
	dir, ok := arr[1].(string)
	if !ok || (dir != "asc" && dir != "desc") {
		return nil, ErrInvalidRankBy
	}
	return &ParsedRankBy{
		Type:      RankByAttr,
		Field:     field,
		Direction: dir,
	}, nil
}

// parseVector parses a vector from the query (float array or base64).
func parseVector(v any) ([]float32, error) {
	switch vec := v.(type) {
	case []any:
		result := make([]float32, len(vec))
		for i, val := range vec {
			switch n := val.(type) {
			case float64:
				result[i] = float32(n)
			case float32:
				result[i] = n
			case int:
				result[i] = float32(n)
			case int64:
				result[i] = float32(n)
			default:
				return nil, ErrInvalidRankBy
			}
		}
		return result, nil
	case []float64:
		result := make([]float32, len(vec))
		for i, val := range vec {
			result[i] = float32(val)
		}
		return result, nil
	case []float32:
		return vec, nil
	default:
		return nil, ErrInvalidRankBy
	}
}

// executeVectorQuery executes a vector ANN query.
func (h *Handler) executeVectorQuery(ctx context.Context, ns string, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest) ([]Row, error) {
	if h.tailStore == nil {
		return nil, nil
	}

	// Perform exhaustive vector scan on tail (no ANN index yet)
	results, err := h.tailStore.VectorScan(ctx, ns, parsed.QueryVector, req.Limit, tail.MetricCosineDistance, f)
	if err != nil {
		return nil, err
	}

	rows := make([]Row, 0, len(results))
	for _, res := range results {
		dist := res.Distance
		rows = append(rows, Row{
			ID:         docIDToAny(res.Doc.ID),
			Dist:       &dist,
			Attributes: res.Doc.Attributes,
		})
	}

	return rows, nil
}

// executeAttrQuery executes an order-by attribute query.
func (h *Handler) executeAttrQuery(ctx context.Context, ns string, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest) ([]Row, error) {
	if h.tailStore == nil {
		return nil, nil
	}

	// Scan all documents
	docs, err := h.tailStore.Scan(ctx, ns, f)
	if err != nil {
		return nil, err
	}

	// Sort by the specified attribute
	sort.Slice(docs, func(i, j int) bool {
		vi := getAttrValue(docs[i].Attributes, parsed.Field)
		vj := getAttrValue(docs[j].Attributes, parsed.Field)
		cmp := compareValues(vi, vj)
		if parsed.Direction == "desc" {
			return cmp > 0
		}
		return cmp < 0
	})

	// Apply limit
	if len(docs) > req.Limit {
		docs = docs[:req.Limit]
	}

	rows := make([]Row, 0, len(docs))
	for _, doc := range docs {
		rows = append(rows, Row{
			ID:         docIDToAny(doc.ID),
			Dist:       nil, // No $dist for order-by queries
			Attributes: doc.Attributes,
		})
	}

	return rows, nil
}

// executeBM25Query executes a BM25 full-text search query.
func (h *Handler) executeBM25Query(ctx context.Context, ns string, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest) ([]Row, error) {
	// BM25 indexing not yet implemented - return empty results
	return nil, nil
}

// docIDToAny converts a document.ID to an interface{} for JSON serialization.
func docIDToAny(id document.ID) any {
	switch id.Type() {
	case document.IDTypeU64:
		return id.U64()
	case document.IDTypeUUID:
		return id.UUID().String()
	case document.IDTypeString:
		return id.String()
	default:
		return id.String()
	}
}

// getAttrValue gets an attribute value, returning nil if not found.
func getAttrValue(attrs map[string]any, key string) any {
	if attrs == nil {
		return nil
	}
	return attrs[key]
}

// compareValues compares two values for sorting.
func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch va := a.(type) {
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			}
			if va > vb {
				return 1
			}
			return 0
		}
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			}
			if va > vb {
				return 1
			}
			return 0
		}
	case uint64:
		if vb, ok := b.(uint64); ok {
			if va < vb {
				return -1
			}
			if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			}
			if va > vb {
				return 1
			}
			return 0
		}
	case bool:
		if vb, ok := b.(bool); ok {
			if !va && vb {
				return -1
			}
			if va && !vb {
				return 1
			}
			return 0
		}
	}

	return 0
}

// filterAttributes applies include/exclude filtering to rows.
func filterAttributes(rows []Row, include, exclude []string) []Row {
	if len(include) == 0 && len(exclude) == 0 {
		return rows
	}

	includeSet := make(map[string]bool)
	for _, attr := range include {
		includeSet[attr] = true
	}

	excludeSet := make(map[string]bool)
	for _, attr := range exclude {
		excludeSet[attr] = true
	}

	for i := range rows {
		if rows[i].Attributes == nil {
			continue
		}

		if len(include) > 0 {
			// Only keep included attributes
			filtered := make(map[string]any)
			for k, v := range rows[i].Attributes {
				if includeSet[k] {
					filtered[k] = v
				}
			}
			rows[i].Attributes = filtered
		} else if len(exclude) > 0 {
			// Remove excluded attributes without mutating shared maps.
			filtered := make(map[string]any, len(rows[i].Attributes))
			for k, v := range rows[i].Attributes {
				filtered[k] = v
			}
			for k := range excludeSet {
				delete(filtered, k)
			}
			rows[i].Attributes = filtered
		}
	}

	return rows
}
