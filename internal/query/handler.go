// Package query implements the query execution engine for Vex.
package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/fts"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/vector"
	"github.com/vexsearch/vex/pkg/objectstore"
)

const (
	// MaxTopK is the maximum allowed limit/top_k value.
	MaxTopK = 10_000

	// DefaultLimit is the default result limit if not specified.
	DefaultLimit = 10

	// EventualTailCapBytes is the max tail bytes to search in eventual consistency mode (128 MiB).
	EventualTailCapBytes = 128 * 1024 * 1024

	// MaxMultiQuery is the maximum number of subqueries in a multi-query request.
	MaxMultiQuery = 16
)

var (
	ErrRankByRequired        = errors.New("rank_by is required unless aggregate_by is specified")
	ErrInvalidRankBy         = errors.New("invalid rank_by expression")
	ErrInvalidLimit          = errors.New("limit/top_k must be between 1 and 10,000")
	ErrInvalidFilter         = errors.New("invalid filter expression")
	ErrInvalidRequest        = errors.New("invalid query request")
	ErrNamespaceNotFound     = errors.New("namespace not found")
	ErrAttributeConflict     = errors.New("cannot specify both include_attributes and exclude_attributes")
	ErrInvalidVectorEncoding = errors.New("vector_encoding must be 'float' or 'base64'")
	ErrInvalidConsistency    = errors.New("consistency must be 'strong' or 'eventual'")
	ErrSnapshotRefreshFailed = errors.New("failed to refresh snapshot for strong consistency")
	ErrInvalidAggregation    = errors.New("invalid aggregation expression")
	ErrInvalidGroupBy        = errors.New("invalid group_by expression")
	ErrGroupByWithoutAgg     = errors.New("group_by requires aggregate_by to be specified")
	ErrTooManySubqueries     = errors.New("multi-query exceeds maximum of 16 subqueries")
	ErrInvalidMultiQuery     = errors.New("invalid multi-query format")
	ErrConcurrencyLimitWait  = errors.New("query concurrency limit reached, request queued")
)

// AggregationType indicates the type of aggregation function.
type AggregationType int

const (
	AggCount AggregationType = iota // ["Count"]
	AggSum                          // ["Sum", "attribute_name"]
)

// ParsedAggregation holds a parsed aggregation function.
type ParsedAggregation struct {
	Type      AggregationType
	Attribute string // For Sum, the attribute to sum
}

// QueryRequest represents a query request from the API.
type QueryRequest struct {
	RankBy            any      // Required unless AggregateBy is set
	Filters           any      // Optional filter expression
	IncludeAttributes []string // Optional: attributes to include in response
	ExcludeAttributes []string // Optional: attributes to exclude from response
	Limit             int      // Alias: top_k - max results to return (default 10, max 10,000)
	Per               []string // Optional: diversification attribute(s) for order-by queries
	AggregateBy       any      // Optional: aggregation specification
	GroupBy           any      // Optional: group-by specification
	VectorEncoding    string   // Optional: "float" (default) or "base64"
	Consistency       string   // Optional: "strong" (default) or "eventual"
}

// QueryResponse represents the response from a query.
type QueryResponse struct {
	Rows              []Row            `json:"rows,omitempty"`
	Aggregations      map[string]any   `json:"aggregations,omitempty"`
	AggregationGroups []map[string]any `json:"aggregation_groups,omitempty"`
	Billing           BillingInfo      `json:"billing"`
	Performance       PerformanceInfo  `json:"performance"`
}

// MultiQueryResponse represents the response from a multi-query request.
type MultiQueryResponse struct {
	Results     []SubQueryResult `json:"results"`
	Billing     BillingInfo      `json:"billing"`
	Performance PerformanceInfo  `json:"performance"`
}

// SubQueryResult represents a single subquery result within a multi-query response.
type SubQueryResult struct {
	Rows              []Row            `json:"rows,omitempty"`
	Aggregations      map[string]any   `json:"aggregations,omitempty"`
	AggregationGroups []map[string]any `json:"aggregation_groups,omitempty"`
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
	QueryText   string     // For BM25 text search
}

// DefaultNProbe is the default number of centroids to probe in ANN search.
const DefaultNProbe = 16

// Handler handles query operations.
type Handler struct {
	store       objectstore.Store
	stateMan    *namespace.StateManager
	tailStore   tail.Store
	indexReader *index.Reader
	limiter     *ConcurrencyLimiter
}

// NewHandler creates a new query handler.
func NewHandler(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store) *Handler {
	return &Handler{
		store:       store,
		stateMan:    stateMan,
		tailStore:   tailStore,
		indexReader: index.NewReader(store, nil, nil),
		limiter:     NewConcurrencyLimiter(DefaultConcurrencyLimit),
	}
}

// NewHandlerWithLimit creates a new query handler with a custom concurrency limit.
func NewHandlerWithLimit(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store, limit int) *Handler {
	return &Handler{
		store:       store,
		stateMan:    stateMan,
		tailStore:   tailStore,
		indexReader: index.NewReader(store, nil, nil),
		limiter:     NewConcurrencyLimiter(limit),
	}
}

// NewHandlerWithCache creates a new query handler with caching support.
func NewHandlerWithCache(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store, diskCache *cache.DiskCache, ramCache *cache.MemoryCache) *Handler {
	return &Handler{
		store:       store,
		stateMan:    stateMan,
		tailStore:   tailStore,
		indexReader: index.NewReader(store, diskCache, ramCache),
		limiter:     NewConcurrencyLimiter(DefaultConcurrencyLimit),
	}
}

// Limiter returns the concurrency limiter for testing/metrics.
func (h *Handler) Limiter() *ConcurrencyLimiter {
	return h.limiter
}

// Handle executes a query against the given namespace.
func (h *Handler) Handle(ctx context.Context, ns string, req *QueryRequest) (*QueryResponse, error) {
	// Validate the request
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	// Acquire concurrency slot - blocks until available or context cancelled
	release, err := h.limiter.Acquire(ctx, ns)
	if err != nil {
		return nil, err
	}
	defer release()

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

	// Strong consistency is the default: refresh tail to WAL head to include all committed writes
	isStrongConsistency := req.Consistency != "eventual"
	if isStrongConsistency {
		if h.tailStore != nil {
			headSeq := loaded.State.WAL.HeadSeq
			indexedSeq := loaded.State.Index.IndexedWALSeq
			if headSeq > indexedSeq {
				if err := h.tailStore.Refresh(ctx, ns, indexedSeq, headSeq); err != nil {
					return nil, fmt.Errorf("%w: %v", ErrSnapshotRefreshFailed, err)
				}
			}
		}
	}

	// Parse the rank_by expression
	parsed, err := parseRankBy(req.RankBy, req.VectorEncoding)
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

		// Validate regex filters against schema
		if f.UsesRegexOperators() {
			schemaChecker := buildSchemaChecker(loaded.State.Schema)
			if err := f.ValidateWithSchema(schemaChecker); err != nil {
				return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
			}
		}
	}

	// Determine byte limit for eventual consistency
	var tailByteCap int64
	if req.Consistency == "eventual" {
		tailByteCap = EventualTailCapBytes
	}

	// Check if this is an aggregation query
	if req.AggregateBy != nil {
		// Parse aggregations
		aggregations, err := parseAggregateBy(req.AggregateBy)
		if err != nil {
			return nil, err
		}

		// Check if this is a grouped aggregation
		if req.GroupBy != nil {
			// Parse group_by
			groupByAttrs, err := parseGroupBy(req.GroupBy)
			if err != nil {
				return nil, err
			}

			// Execute grouped aggregation query
			aggGroups, err := h.executeGroupedAggregationQuery(ctx, ns, f, aggregations, groupByAttrs, req.Limit, tailByteCap)
			if err != nil {
				return nil, err
			}

			return &QueryResponse{
				AggregationGroups: aggGroups,
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

		// Execute non-grouped aggregation query
		aggResults, err := h.executeAggregationQuery(ctx, ns, f, aggregations, tailByteCap)
		if err != nil {
			return nil, err
		}

		return &QueryResponse{
			Aggregations: aggResults,
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

	// Execute the query based on rank_by type
	var rows []Row
	if parsed != nil {
		switch parsed.Type {
		case RankByVector:
			rows, err = h.executeVectorQuery(ctx, ns, loaded, parsed, f, req, tailByteCap)
		case RankByAttr:
			rows, err = h.executeAttrQuery(ctx, ns, parsed, f, req, tailByteCap)
		case RankByBM25:
			rows, err = h.executeBM25Query(ctx, ns, loaded, parsed, f, req)
		}
		if err != nil {
			return nil, err
		}
	}

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

	// Validate consistency if specified
	if req.Consistency != "" && req.Consistency != "strong" && req.Consistency != "eventual" {
		return ErrInvalidConsistency
	}

	// group_by requires aggregate_by to be specified
	if req.GroupBy != nil && req.AggregateBy == nil {
		return ErrGroupByWithoutAgg
	}

	return nil
}

// parseRankBy parses the rank_by expression.
func parseRankBy(rankBy any, vectorEncoding string) (*ParsedRankBy, error) {
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
		vec, err := parseVector(arr[2], vectorEncoding)
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
			queryText, ok := arr[2].(string)
			if !ok {
				return nil, ErrInvalidRankBy
			}
			return &ParsedRankBy{
				Type:      RankByBM25,
				Field:     field,
				QueryText: queryText,
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
func parseVector(v any, vectorEncoding string) ([]float32, error) {
	if vectorEncoding == "" {
		vectorEncoding = string(vector.DefaultEncoding)
	}

	switch vectorEncoding {
	case "base64":
		decoded, err := vector.DecodeVectorWithEncoding(v, 0, vector.EncodingBase64)
		if err != nil {
			return nil, ErrInvalidRankBy
		}
		return decoded, nil
	case "float":
		if _, ok := v.(string); ok {
			return nil, ErrInvalidRankBy
		}
		decoded, err := vector.DecodeVectorWithEncoding(v, 0, vector.EncodingFloat)
		if err != nil {
			return nil, ErrInvalidRankBy
		}
		return decoded, nil
	default:
		decoded, err := vector.DecodeVector(v, 0)
		if err != nil {
			return nil, ErrInvalidRankBy
		}
		return decoded, nil
	}
}

// executeVectorQuery executes a vector ANN query.
// tailByteCap of 0 means no limit (strong consistency); >0 means eventual consistency byte cap.
func (h *Handler) executeVectorQuery(ctx context.Context, ns string, loaded *namespace.LoadedState, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest, tailByteCap int64) ([]Row, error) {
	// Determine distance metric from namespace config or use default
	metric := tail.MetricCosineDistance
	vectorMetric := vector.MetricCosineDistance
	if loaded.State.Vector != nil && loaded.State.Vector.DistanceMetric != "" {
		switch loaded.State.Vector.DistanceMetric {
		case string(vector.MetricCosineDistance):
			metric = tail.MetricCosineDistance
			vectorMetric = vector.MetricCosineDistance
		case string(vector.MetricEuclideanSquared):
			metric = tail.MetricEuclideanSquared
			vectorMetric = vector.MetricEuclideanSquared
		case string(vector.MetricDotProduct):
			metric = tail.MetricDotProduct
			vectorMetric = vector.MetricDotProduct
		}
	}

	// Check if there's an IVF index available
	var indexResults []vector.IVFSearchResult
	indexedWALSeq := loaded.State.Index.IndexedWALSeq

	// Determine if we should use the index
	// IMPORTANT: When filters are present, we CANNOT use the IVF index because:
	// 1. The IVF index only stores docID + vector, not document attributes
	// 2. We cannot verify if indexed docs match the filter
	// 3. Using the index with filters would return unfiltered results (incorrect)
	// 4. Documents in the index but not in tail cannot be post-filtered
	//
	// The only safe approach is to use exhaustive search when filters are present.
	// This ensures all documents are checked against the filter.
	//
	// Recall-aware filtering helps optimize the exhaustive search by:
	// - Estimating selectivity to predict result set size
	// - Providing metrics for monitoring and optimization
	useIndex := loaded.State.Index.ManifestKey != "" && h.indexReader != nil && f == nil

	if useIndex {
		ivfReader, clusterDataKey, err := h.indexReader.GetIVFReader(ctx, ns, loaded.State.Index.ManifestKey, loaded.State.Index.ManifestSeq)
		if err != nil {
			// Log error but fall back to exhaustive search
			_ = err
		} else if ivfReader != nil {
			// Use standard ANN parameters (no filter present)
			nProbe := DefaultNProbe
			candidates := req.Limit

			indexResults, err = h.indexReader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, parsed.QueryVector, candidates, nProbe)
			if err != nil {
				// Fall back to exhaustive search on error
				indexResults = nil
			}
		}
	}

	// Get results from tail (unindexed data)
	var tailResults []tail.VectorScanResult
	if h.tailStore != nil {
		var err error
		// Only search tail for WAL entries after the indexed sequence
		if tailByteCap > 0 {
			tailResults, err = h.tailStore.VectorScanWithByteLimit(ctx, ns, parsed.QueryVector, req.Limit, metric, f, tailByteCap)
		} else {
			tailResults, err = h.tailStore.VectorScan(ctx, ns, parsed.QueryVector, req.Limit, metric, f)
		}
		if err != nil {
			return nil, err
		}
	}

	// If we have no index results, just return tail results
	if len(indexResults) == 0 {
		rows := make([]Row, 0, len(tailResults))
		for _, res := range tailResults {
			dist := res.Distance
			rows = append(rows, Row{
				ID:         docIDToAny(res.Doc.ID),
				Dist:       &dist,
				Attributes: res.Doc.Attributes,
			})
		}
		return rows, nil
	}

	// Merge index results with tail results
	// Tail results are authoritative for deduplication (newer data)
	// Note: This path is only reached when no filter is present (f == nil)
	rows := h.mergeVectorResults(ctx, ns, indexResults, tailResults, req.Limit, vectorMetric, indexedWALSeq)
	return rows, nil
}

// mergeVectorResults merges IVF index results with tail results.
// Tail results take precedence for deduplication since they contain newer data.
// Note: This function is only called when no filter is present (filters skip the index path).
// Deduplication ensures last-write-wins semantics across segments and tail.
//
// The deduplication strategy:
// 1. Tail documents have explicit WAL seq and always take precedence for the same doc ID
// 2. Index results have WAL seq <= indexedWALSeq (when the index was built)
// 3. For duplicate doc IDs in index results, we keep the first occurrence (they should be unique in a well-formed index)
// 4. Tombstones in tail exclude documents from results
func (h *Handler) mergeVectorResults(ctx context.Context, ns string, indexResults []vector.IVFSearchResult, tailResults []tail.VectorScanResult, limit int, metric vector.DistanceMetric, indexedWALSeq uint64) []Row {
	// Use deduplicator to track tail documents for proper deduplication.
	// Tail always has the authoritative version (higher WAL seq than index).
	dedup := NewDeduplicator()

	// Add tail results first (they have WAL seq info and are always authoritative over index)
	for _, res := range tailResults {
		dedup.AddTailDoc(res.Doc)
	}

	// Track distances separately since Deduplicator doesn't store distances
	type scoredDoc struct {
		docID      any
		dist       float64
		attributes map[string]any
	}

	// Map to track best distance per unique doc ID (after deduplication)
	docDistances := make(map[string]scoredDoc)

	// Add tail results with their distances (skip deleted/tombstoned docs)
	for _, res := range tailResults {
		if res.Doc.Deleted {
			continue
		}
		idStr := res.Doc.ID.String()
		docDistances[idStr] = scoredDoc{
			docID:      docIDToAny(res.Doc.ID),
			dist:       res.Distance,
			attributes: res.Doc.Attributes,
		}
	}

	// Process index results - deduplicate across segments and against tail
	// Use a set to deduplicate doc IDs that might appear multiple times in index results
	seenIndexDocs := make(map[uint64]bool)
	for _, res := range indexResults {
		// Skip if already seen in index results (handles duplicates across segments)
		if seenIndexDocs[res.DocID] {
			continue
		}
		seenIndexDocs[res.DocID] = true

		idStr := document.NewU64ID(res.DocID).String()

		// Check if this doc exists in tail - tail is always authoritative
		// Tail contains all docs with WAL seq > indexedWALSeq, so if a doc
		// is in tail, that version supersedes the index version.
		if existingDoc := dedup.GetDoc(idStr); existingDoc != nil {
			// Tail has this doc - tail is authoritative since it has newer data
			// The doc is either updated (keep tail version) or deleted (skip entirely)
			// Deleted docs are already excluded from docDistances above
			continue
		}

		// Add to results - doc not in tail so index version is current
		docDistances[idStr] = scoredDoc{
			docID:      res.DocID,
			dist:       float64(res.Distance),
			attributes: nil, // No attributes from index
		}
	}

	// Convert to slice and sort by distance
	candidates := make([]scoredDoc, 0, len(docDistances))
	for _, sc := range docDistances {
		candidates = append(candidates, sc)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].dist < candidates[j].dist
	})

	// Build rows with limit
	rows := make([]Row, 0, limit)
	for i := 0; i < len(candidates) && len(rows) < limit; i++ {
		dist := candidates[i].dist
		rows = append(rows, Row{
			ID:         candidates[i].docID,
			Dist:       &dist,
			Attributes: candidates[i].attributes,
		})
	}

	return rows
}

// executeAttrQuery executes an order-by attribute query.
// tailByteCap of 0 means no limit (strong consistency); >0 means eventual consistency byte cap.
func (h *Handler) executeAttrQuery(ctx context.Context, ns string, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest, tailByteCap int64) ([]Row, error) {
	if h.tailStore == nil {
		return nil, nil
	}

	// Scan documents, using byte-limited scan for eventual consistency
	var docs []*tail.Document
	var err error
	if tailByteCap > 0 {
		docs, err = h.tailStore.ScanWithByteLimit(ctx, ns, f, tailByteCap)
	} else {
		docs, err = h.tailStore.Scan(ctx, ns, f)
	}
	if err != nil {
		return nil, err
	}

	// Sort by the specified attribute
	sort.Slice(docs, func(i, j int) bool {
		vi := getDocAttrValue(docs[i], parsed.Field)
		vj := getDocAttrValue(docs[j], parsed.Field)
		cmp := compareValues(vi, vj)
		if parsed.Direction == "desc" {
			return cmp > 0
		}
		return cmp < 0
	})

	// Apply limit with optional per diversification
	var selected []*tail.Document
	if len(req.Per) > 0 {
		// Diversification: limit N results per distinct value of the per attribute
		selected = applyPerDiversification(docs, req.Per, req.Limit)
	} else {
		// Simple limit
		if len(docs) > req.Limit {
			selected = docs[:req.Limit]
		} else {
			selected = docs
		}
	}

	rows := make([]Row, 0, len(selected))
	for _, doc := range selected {
		rows = append(rows, Row{
			ID:         docIDToAny(doc.ID),
			Dist:       nil, // No $dist for order-by queries
			Attributes: doc.Attributes,
		})
	}

	return rows, nil
}

// executeBM25Query executes a BM25 full-text search query.
// It searches the tail for documents matching the query and ranks them by BM25 score.
// Documents with zero score are excluded from results.
func (h *Handler) executeBM25Query(ctx context.Context, ns string, loaded *namespace.LoadedState, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest) ([]Row, error) {
	if h.tailStore == nil {
		return nil, nil
	}

	// Get FTS config for the field from schema
	ftsCfg := h.getFTSConfig(loaded, parsed.Field)
	if ftsCfg == nil {
		// Use default config if not configured
		ftsCfg = fts.DefaultConfig()
	}

	// Tokenize the query
	tokenizer := fts.NewTokenizer(ftsCfg)
	queryTokens := tokenizer.Tokenize(parsed.QueryText)
	if ftsCfg.RemoveStopwords {
		queryTokens = fts.RemoveStopwords(queryTokens)
	}

	// If query produces no tokens, return empty results
	if len(queryTokens) == 0 {
		return []Row{}, nil
	}

	// Scan documents from tail
	docs, err := h.tailStore.Scan(ctx, ns, f)
	if err != nil {
		return nil, err
	}

	if len(docs) == 0 {
		return []Row{}, nil
	}

	// Build a temporary FTS index from tail documents for BM25 scoring
	// This computes term frequencies and document statistics needed for BM25
	index := fts.NewIndex(parsed.Field, ftsCfg)
	docIDMap := make(map[uint32]*tail.Document) // Map internal doc IDs to tail docs

	for i, doc := range docs {
		if doc.Deleted {
			continue
		}

		// Get the text value for the field
		text, ok := h.getDocTextValue(doc, parsed.Field)
		if !ok || text == "" {
			continue
		}

		internalID := uint32(i)
		index.AddDocument(internalID, text)
		docIDMap[internalID] = doc
	}

	// If no documents have the field, return empty results
	if index.TotalDocs == 0 {
		return []Row{}, nil
	}

	// Score documents using BM25
	type scoredDoc struct {
		doc   *tail.Document
		score float64
	}
	var scored []scoredDoc

	for docID, doc := range docIDMap {
		score := computeBM25Score(index, docID, queryTokens)
		if score > 0 {
			scored = append(scored, scoredDoc{doc: doc, score: score})
		}
	}

	// Sort by score descending
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	// Apply limit
	if len(scored) > req.Limit {
		scored = scored[:req.Limit]
	}

	// Build result rows
	rows := make([]Row, 0, len(scored))
	for _, sc := range scored {
		dist := sc.score
		rows = append(rows, Row{
			ID:         docIDToAny(sc.doc.ID),
			Dist:       &dist,
			Attributes: sc.doc.Attributes,
		})
	}

	return rows, nil
}

// getFTSConfig retrieves the FTS configuration for a field from the schema.
func (h *Handler) getFTSConfig(loaded *namespace.LoadedState, field string) *fts.Config {
	if loaded.State.Schema == nil {
		return nil
	}

	attrSchema, ok := loaded.State.Schema.Attributes[field]
	if !ok {
		return nil
	}

	if attrSchema.FullTextSearch == nil {
		return nil
	}

	// Parse the FTS config from the raw JSON
	var ftsVal any
	if err := json.Unmarshal(attrSchema.FullTextSearch, &ftsVal); err != nil {
		return nil
	}

	cfg, err := fts.Parse(ftsVal)
	if err != nil {
		return nil
	}

	return cfg
}

// getDocTextValue gets the string value of a field from a document.
func (h *Handler) getDocTextValue(doc *tail.Document, field string) (string, bool) {
	if doc.Attributes == nil {
		return "", false
	}
	val, ok := doc.Attributes[field]
	if !ok {
		return "", false
	}
	text, ok := val.(string)
	return text, ok
}

// computeBM25Score computes the BM25 score for a document given query tokens.
func computeBM25Score(idx *fts.Index, docID uint32, queryTokens []string) float64 {
	k1 := idx.Config.K1
	b := idx.Config.B
	avgDL := idx.AvgDocLength
	docLen := float64(idx.GetDocLength(docID))
	n := float64(idx.TotalDocs)

	var score float64
	for _, term := range queryTokens {
		tf := float64(idx.GetTermFrequency(term, docID))
		if tf == 0 {
			continue
		}

		df := float64(idx.GetDocumentFrequency(term))
		if df == 0 {
			continue
		}

		// IDF: log((N - df + 0.5) / (df + 0.5) + 1)
		idf := math.Log((n-df+0.5)/(df+0.5) + 1)

		// BM25 term score: IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (docLen / avgDL)))
		denominator := tf + k1*(1-b+b*(docLen/avgDL))
		if denominator > 0 {
			score += idf * (tf * (k1 + 1)) / denominator
		}
	}

	return score
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

// getDocAttrValue gets an attribute value from a document, handling "id" specially.
func getDocAttrValue(doc *tail.Document, key string) any {
	if key == "id" {
		return docIDToSortable(doc.ID)
	}
	if doc.Attributes == nil {
		return nil
	}
	return doc.Attributes[key]
}

// docIDToSortable converts a document ID to a sortable value.
func docIDToSortable(id document.ID) any {
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

// applyPerDiversification applies the per diversification option to the sorted documents.
// It returns at most limit documents per distinct value of the per attribute(s).
func applyPerDiversification(docs []*tail.Document, perAttrs []string, limit int) []*tail.Document {
	if len(perAttrs) == 0 || limit <= 0 {
		return docs
	}

	// Track count per distinct value(s)
	counts := make(map[string]int)
	var result []*tail.Document

	for _, doc := range docs {
		// Build key from per attribute values
		key := buildPerKey(doc, perAttrs)
		if counts[key] < limit {
			result = append(result, doc)
			counts[key]++
		}
	}

	return result
}

// buildPerKey builds a string key from the document's per attribute values.
func buildPerKey(doc *tail.Document, perAttrs []string) string {
	if len(perAttrs) == 1 {
		v := getDocAttrValue(doc, perAttrs[0])
		return formatKeyValue(v)
	}

	// Multiple attributes - concatenate their values
	var key string
	for i, attr := range perAttrs {
		v := getDocAttrValue(doc, attr)
		if i > 0 {
			key += "|"
		}
		key += formatKeyValue(v)
	}
	return key
}

// formatKeyValue formats a value for use in a per-diversification key.
func formatKeyValue(v any) string {
	if v == nil {
		return "<nil>"
	}
	switch val := v.(type) {
	case string:
		return val
	case uint64:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%f", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
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

// parseAggregateBy parses the aggregate_by field into a map of label -> ParsedAggregation.
// The aggregate_by field is an object mapping labels to aggregation functions:
// {"my_count": ["Count"], "my_sum": ["Sum", "price"]}
func parseAggregateBy(aggregateBy any) (map[string]*ParsedAggregation, error) {
	aggMap, ok := aggregateBy.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: aggregate_by must be an object", ErrInvalidAggregation)
	}

	result := make(map[string]*ParsedAggregation)
	for label, aggExpr := range aggMap {
		parsed, err := parseAggregationExpr(aggExpr)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrInvalidAggregation, label, err)
		}
		result[label] = parsed
	}

	return result, nil
}

// parseAggregationExpr parses a single aggregation expression like ["Count"] or ["Sum", "price"].
func parseAggregationExpr(expr any) (*ParsedAggregation, error) {
	arr, ok := expr.([]any)
	if !ok {
		return nil, errors.New("aggregation must be an array")
	}

	if len(arr) < 1 {
		return nil, errors.New("aggregation array must have at least one element")
	}

	funcName, ok := arr[0].(string)
	if !ok {
		return nil, errors.New("aggregation function name must be a string")
	}

	switch funcName {
	case "Count":
		if len(arr) != 1 {
			return nil, errors.New("Count takes no arguments")
		}
		return &ParsedAggregation{Type: AggCount}, nil
	case "Sum":
		if len(arr) != 2 {
			return nil, errors.New("Sum requires exactly one attribute argument")
		}
		attr, ok := arr[1].(string)
		if !ok {
			return nil, errors.New("Sum attribute must be a string")
		}
		return &ParsedAggregation{Type: AggSum, Attribute: attr}, nil
	default:
		return nil, fmt.Errorf("unknown aggregation function: %s", funcName)
	}
}

// executeAggregationQuery executes an aggregation query over the result set.
func (h *Handler) executeAggregationQuery(ctx context.Context, ns string, f *filter.Filter, aggregations map[string]*ParsedAggregation, tailByteCap int64) (map[string]any, error) {
	if h.tailStore == nil {
		// Return zeros when no tail store available
		result := make(map[string]any)
		for label, agg := range aggregations {
			switch agg.Type {
			case AggCount:
				result[label] = int64(0)
			case AggSum:
				result[label] = float64(0)
			}
		}
		return result, nil
	}

	// Scan all documents matching the filter
	var docs []*tail.Document
	var err error
	if tailByteCap > 0 {
		docs, err = h.tailStore.ScanWithByteLimit(ctx, ns, f, tailByteCap)
	} else {
		docs, err = h.tailStore.Scan(ctx, ns, f)
	}
	if err != nil {
		return nil, err
	}

	// Compute aggregations over the documents
	result := make(map[string]any)
	for label, agg := range aggregations {
		switch agg.Type {
		case AggCount:
			result[label] = int64(len(docs))
		case AggSum:
			sum := computeSum(docs, agg.Attribute)
			result[label] = sum
		}
	}

	return result, nil
}

// computeSum computes the sum of a numeric attribute across all documents.
func computeSum(docs []*tail.Document, attr string) float64 {
	var sum float64
	for _, doc := range docs {
		if doc.Attributes == nil {
			continue
		}
		val := doc.Attributes[attr]
		if val == nil {
			continue
		}
		switch v := val.(type) {
		case int:
			sum += float64(v)
		case int64:
			sum += float64(v)
		case uint64:
			sum += float64(v)
		case float64:
			sum += v
		case float32:
			sum += float64(v)
		}
	}
	return sum
}

// parseGroupBy parses the group_by field into an array of attribute names.
func parseGroupBy(groupBy any) ([]string, error) {
	switch v := groupBy.(type) {
	case []string:
		if len(v) == 0 {
			return nil, fmt.Errorf("%w: group_by must have at least one attribute", ErrInvalidGroupBy)
		}
		return v, nil
	case []any:
		if len(v) == 0 {
			return nil, fmt.Errorf("%w: group_by must have at least one attribute", ErrInvalidGroupBy)
		}
		result := make([]string, 0, len(v))
		for i, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%w: group_by element %d is not a string", ErrInvalidGroupBy, i)
			}
			result = append(result, s)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("%w: group_by must be an array of attribute names", ErrInvalidGroupBy)
	}
}

// executeGroupedAggregationQuery executes an aggregation query with grouping.
func (h *Handler) executeGroupedAggregationQuery(ctx context.Context, ns string, f *filter.Filter, aggregations map[string]*ParsedAggregation, groupByAttrs []string, limit int, tailByteCap int64) ([]map[string]any, error) {
	if h.tailStore == nil {
		return []map[string]any{}, nil
	}

	// Scan all documents matching the filter
	var docs []*tail.Document
	var err error
	if tailByteCap > 0 {
		docs, err = h.tailStore.ScanWithByteLimit(ctx, ns, f, tailByteCap)
	} else {
		docs, err = h.tailStore.Scan(ctx, ns, f)
	}
	if err != nil {
		return nil, err
	}

	// Group documents by the group key
	groups := make(map[string][]*tail.Document)
	groupKeys := make(map[string]map[string]any) // group key string -> attribute values

	for _, doc := range docs {
		keyStr, keyVals := buildGroupKey(doc, groupByAttrs)
		groups[keyStr] = append(groups[keyStr], doc)
		if _, exists := groupKeys[keyStr]; !exists {
			groupKeys[keyStr] = keyVals
		}
	}

	// Compute aggregations for each group
	result := make([]map[string]any, 0, len(groups))
	for keyStr, groupDocs := range groups {
		row := make(map[string]any)

		// Add group key attributes
		keyVals := groupKeys[keyStr]
		for attr, val := range keyVals {
			row[attr] = val
		}

		// Compute aggregations for this group
		for label, agg := range aggregations {
			switch agg.Type {
			case AggCount:
				row[label] = int64(len(groupDocs))
			case AggSum:
				row[label] = computeSum(groupDocs, agg.Attribute)
			}
		}

		result = append(result, row)
	}

	// Sort result by group key attributes to ensure deterministic order
	sortAggregationGroups(result, groupByAttrs)

	// Apply limit if set
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}

	return result, nil
}

// buildGroupKey builds a string key and attribute map from a document's group attributes.
func buildGroupKey(doc *tail.Document, groupByAttrs []string) (string, map[string]any) {
	keyVals := make(map[string]any, len(groupByAttrs))
	var keyStr string

	for i, attr := range groupByAttrs {
		val := getDocAttrValue(doc, attr)
		keyVals[attr] = val
		if i > 0 {
			keyStr += "|"
		}
		keyStr += formatKeyValue(val)
	}

	return keyStr, keyVals
}

// sortAggregationGroups sorts aggregation groups by group key attributes.
func sortAggregationGroups(groups []map[string]any, groupByAttrs []string) {
	sort.Slice(groups, func(i, j int) bool {
		for _, attr := range groupByAttrs {
			vi := groups[i][attr]
			vj := groups[j][attr]
			cmp := compareValues(vi, vj)
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
}

// HandleMultiQuery executes multiple subqueries against the same snapshot.
// All subqueries share the same snapshot for consistency (snapshot isolation).
func (h *Handler) HandleMultiQuery(ctx context.Context, ns string, queries []map[string]any, consistency string) (*MultiQueryResponse, error) {
	// Validate subquery count
	if len(queries) == 0 {
		return nil, ErrInvalidMultiQuery
	}
	if len(queries) > MaxMultiQuery {
		return nil, ErrTooManySubqueries
	}
	if consistency != "" && consistency != "strong" && consistency != "eventual" {
		return nil, ErrInvalidConsistency
	}

	// Acquire concurrency slot - blocks until available or context cancelled
	release, err := h.limiter.Acquire(ctx, ns)
	if err != nil {
		return nil, err
	}
	defer release()

	// Load namespace state once for snapshot isolation
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

	// Determine consistency mode - strong is default
	isStrongConsistency := consistency != "eventual"

	// Refresh tail to WAL head once for all subqueries (snapshot isolation).
	// This ensures all subqueries execute against the same consistent snapshot.
	if isStrongConsistency && h.tailStore != nil {
		headSeq := loaded.State.WAL.HeadSeq
		indexedSeq := loaded.State.Index.IndexedWALSeq
		if headSeq > indexedSeq {
			if err := h.tailStore.Refresh(ctx, ns, indexedSeq, headSeq); err != nil {
				return nil, fmt.Errorf("%w: %v", ErrSnapshotRefreshFailed, err)
			}
		}
	}

	// Determine byte limit for eventual consistency
	var tailByteCap int64
	if !isStrongConsistency {
		tailByteCap = EventualTailCapBytes
	}

	// Execute each subquery in order
	results := make([]SubQueryResult, 0, len(queries))
	for _, subQueryBody := range queries {
		result, err := h.executeSubQuery(ctx, ns, loaded, subQueryBody, tailByteCap)
		if err != nil {
			return nil, err
		}
		results = append(results, *result)
	}

	return &MultiQueryResponse{
		Results: results,
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

// executeSubQuery executes a single subquery within a multi-query context.
// It uses the pre-loaded namespace state and does not refresh the snapshot.
func (h *Handler) executeSubQuery(ctx context.Context, ns string, loaded *namespace.LoadedState, body map[string]any, tailByteCap int64) (*SubQueryResult, error) {
	// Parse the subquery request
	req, err := ParseQueryRequest(body)
	if err != nil {
		return nil, err
	}

	// Validate the request
	if err := h.validateRequest(req); err != nil {
		return nil, err
	}

	// Parse the rank_by expression
	parsed, err := parseRankBy(req.RankBy, req.VectorEncoding)
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

		// Validate regex filters against schema
		if f.UsesRegexOperators() {
			schemaChecker := buildSchemaChecker(loaded.State.Schema)
			if err := f.ValidateWithSchema(schemaChecker); err != nil {
				return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
			}
		}
	}

	// Check if this is an aggregation query
	if req.AggregateBy != nil {
		// Parse aggregations
		aggregations, err := parseAggregateBy(req.AggregateBy)
		if err != nil {
			return nil, err
		}

		// Check if this is a grouped aggregation
		if req.GroupBy != nil {
			// Parse group_by
			groupByAttrs, err := parseGroupBy(req.GroupBy)
			if err != nil {
				return nil, err
			}

			// Execute grouped aggregation query
			aggGroups, err := h.executeGroupedAggregationQuery(ctx, ns, f, aggregations, groupByAttrs, req.Limit, tailByteCap)
			if err != nil {
				return nil, err
			}

			return &SubQueryResult{
				AggregationGroups: aggGroups,
			}, nil
		}

		// Execute non-grouped aggregation query
		aggResults, err := h.executeAggregationQuery(ctx, ns, f, aggregations, tailByteCap)
		if err != nil {
			return nil, err
		}

		return &SubQueryResult{
			Aggregations: aggResults,
		}, nil
	}

	// Execute the query based on rank_by type
	var rows []Row
	if parsed != nil {
		switch parsed.Type {
		case RankByVector:
			rows, err = h.executeVectorQuery(ctx, ns, loaded, parsed, f, req, tailByteCap)
		case RankByAttr:
			rows, err = h.executeAttrQuery(ctx, ns, parsed, f, req, tailByteCap)
		case RankByBM25:
			rows, err = h.executeBM25Query(ctx, ns, loaded, parsed, f, req)
		}
		if err != nil {
			return nil, err
		}
	}

	// Apply attribute filtering
	rows = filterAttributes(rows, req.IncludeAttributes, req.ExcludeAttributes)

	return &SubQueryResult{
		Rows: rows,
	}, nil
}
