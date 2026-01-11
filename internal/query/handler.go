// Package query implements the query execution engine for Vex.
package query

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/document"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/fts"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/metrics"
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

	// EventualSnapshotTTL is the maximum staleness allowed for eventual snapshots.
	EventualSnapshotTTL = 60 * time.Second

	// MaxMultiQuery is the maximum number of subqueries in a multi-query request.
	MaxMultiQuery = 16

	// MaxUnindexedBytes is the threshold for backpressure (2GB).
	MaxUnindexedBytes = 2 * 1024 * 1024 * 1024
)

var (
	ErrRankByRequired          = errors.New("rank_by is required unless aggregate_by is specified")
	ErrInvalidRankBy           = errors.New("invalid rank_by expression")
	ErrInvalidLimit            = errors.New("limit/top_k must be between 1 and 10,000")
	ErrInvalidFilter           = errors.New("invalid filter expression")
	ErrInvalidRequest          = errors.New("invalid query request")
	ErrNamespaceNotFound       = errors.New("namespace not found")
	ErrAttributeConflict       = errors.New("cannot specify both include_attributes and exclude_attributes")
	ErrInvalidVectorEncoding   = errors.New("vector_encoding must be 'float' or 'base64'")
	ErrInvalidVectorDims       = errors.New("query vector dimensions do not match schema")
	ErrInvalidConsistency      = errors.New("consistency must be 'strong' or 'eventual'")
	ErrSnapshotRefreshFailed   = errors.New("failed to refresh snapshot for strong consistency")
	ErrStrongQueryBackpressure = errors.New("strong query unavailable: unindexed data exceeds 2GB with disable_backpressure=true")
	ErrInvalidAggregation      = errors.New("invalid aggregation expression")
	ErrInvalidGroupBy          = errors.New("invalid group_by expression")
	ErrGroupByWithoutAgg       = errors.New("group_by requires aggregate_by to be specified")
	ErrTooManySubqueries       = errors.New("multi-query exceeds maximum of 16 subqueries")
	ErrInvalidMultiQuery       = errors.New("invalid multi-query format")
	ErrConcurrencyLimitWait    = errors.New("query concurrency limit reached, request queued")
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
	RankByVector    RankByType = iota // ["vector", "ANN", <vector>]
	RankByBM25                        // ["field", "BM25", "query"]
	RankByAttr                        // ["attr", "asc|desc"]
	RankByComposite                   // ["Sum", ...], ["Max", ...], ["Product", ...], filter boost
)

// ParsedRankBy holds the parsed rank_by expression.
type ParsedRankBy struct {
	Type         RankByType
	Field        string      // For attribute or BM25 ranking
	Direction    string      // "asc" or "desc" for attribute ranking
	QueryVector  []float32   // For vector ANN
	QueryText    string      // For BM25 text search
	LastAsPrefix bool        // For BM25: treat last token as prefix for typeahead
	Clause       *RankClause // For composite rank_by expressions (Sum, Max, Product, Filter)
}

// DefaultNProbe is the default number of centroids to probe in ANN search.
// Lower values = faster queries, higher values = better recall.
const DefaultNProbe = 4

// Handler handles query operations.
type Handler struct {
	store       objectstore.Store
	stateMan    *namespace.StateManager
	tailStore   tail.Store
	indexReader *index.Reader
	limiter     *ConcurrencyLimiter
	refreshMu   sync.RWMutex
	lastRefresh map[string]time.Time
	now         func() time.Time
	eventualTTL time.Duration
}

// NewHandler creates a new query handler.
func NewHandler(store objectstore.Store, stateMan *namespace.StateManager, tailStore tail.Store) *Handler {
	return &Handler{
		store:       store,
		stateMan:    stateMan,
		tailStore:   tailStore,
		indexReader: index.NewReader(store, nil, nil),
		limiter:     NewConcurrencyLimiter(DefaultConcurrencyLimit),
		lastRefresh: make(map[string]time.Time),
		now:         time.Now,
		eventualTTL: EventualSnapshotTTL,
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
		lastRefresh: make(map[string]time.Time),
		now:         time.Now,
		eventualTTL: EventualSnapshotTTL,
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
		lastRefresh: make(map[string]time.Time),
		now:         time.Now,
		eventualTTL: EventualSnapshotTTL,
	}
}

// Limiter returns the concurrency limiter for testing/metrics.
func (h *Handler) Limiter() *ConcurrencyLimiter {
	return h.limiter
}

// SetNowFunc overrides the handler clock (useful for tests).
func (h *Handler) SetNowFunc(now func() time.Time) {
	h.refreshMu.Lock()
	defer h.refreshMu.Unlock()
	if now == nil {
		h.now = time.Now
		return
	}
	h.now = now
}

// SetEventualSnapshotTTL overrides the eventual consistency staleness window.
func (h *Handler) SetEventualSnapshotTTL(ttl time.Duration) {
	h.refreshMu.Lock()
	h.eventualTTL = ttl
	h.refreshMu.Unlock()
}

// MarkSnapshotRefreshed records a refresh time for eventual consistency tracking.
func (h *Handler) MarkSnapshotRefreshed(namespace string, at time.Time) {
	h.refreshMu.Lock()
	if h.lastRefresh == nil {
		h.lastRefresh = make(map[string]time.Time)
	}
	h.lastRefresh[namespace] = at
	h.refreshMu.Unlock()
}

func (h *Handler) nowTime() time.Time {
	h.refreshMu.RLock()
	now := h.now
	h.refreshMu.RUnlock()
	if now == nil {
		return time.Now()
	}
	return now()
}

func (h *Handler) shouldRefreshEventual(namespace string, now time.Time) bool {
	h.refreshMu.RLock()
	last, ok := h.lastRefresh[namespace]
	ttl := h.eventualTTL
	h.refreshMu.RUnlock()
	if ttl == 0 {
		ttl = EventualSnapshotTTL
	}
	if !ok {
		return true
	}
	return now.Sub(last) > ttl
}

func queryTypeForRequest(parsed *ParsedRankBy, aggregate bool) string {
	if aggregate {
		return "aggregate"
	}
	if parsed == nil {
		return "attribute"
	}
	switch parsed.Type {
	case RankByVector:
		return "vector"
	case RankByAttr:
		return "attribute"
	case RankByBM25:
		return "bm25"
	case RankByComposite:
		if len(collectBM25Fields(parsed)) > 0 {
			return "bm25"
		}
		return "attribute"
	default:
		return "attribute"
	}
}

func (h *Handler) finalizeQueryResponse(start time.Time, ns string, queryType string, resp *QueryResponse) *QueryResponse {
	if resp.Performance.CacheTemperature == "" {
		resp.Performance.CacheTemperature = "cold"
	}
	resp.Performance.ServerTotalMs = float64(time.Since(start).Microseconds()) / 1000.0
	metrics.ObserveQuery(ns, queryType, time.Since(start).Seconds(), nil)
	return resp
}

func (h *Handler) refreshTailIfNeeded(ctx context.Context, ns string, loaded *namespace.LoadedState, strong bool) error {
	if h.tailStore == nil {
		return nil
	}

	headSeq := loaded.State.WAL.HeadSeq
	indexedSeq := loaded.State.Index.IndexedWALSeq
	if headSeq <= indexedSeq {
		return nil
	}

	if !strong {
		now := h.nowTime()
		if !h.shouldRefreshEventual(ns, now) {
			return nil
		}
		refreshStart := time.Now()
		if err := h.tailStore.Refresh(ctx, ns, indexedSeq, headSeq); err != nil {
			// For eventual consistency, gracefully continue with stale data
			// rather than failing the query when refresh fails
			debugLogf("[query] refresh failed for eventual query ns=%s (continuing with stale data): %v", ns, err)
			return nil
		}
		debugLogf("[query] refresh ns=%s strong=false head_seq=%d indexed_seq=%d dur=%s", ns, headSeq, indexedSeq, time.Since(refreshStart))
		h.MarkSnapshotRefreshed(ns, now)
		return nil
	}

	refreshStart := time.Now()
	if err := h.tailStore.Refresh(ctx, ns, indexedSeq, headSeq); err != nil {
		return fmt.Errorf("%w: %v", ErrSnapshotRefreshFailed, err)
	}
	debugLogf("[query] refresh ns=%s strong=true head_seq=%d indexed_seq=%d dur=%s", ns, headSeq, indexedSeq, time.Since(refreshStart))
	h.MarkSnapshotRefreshed(ns, h.nowTime())
	return nil
}

// Handle executes a query against the given namespace.
func (h *Handler) Handle(ctx context.Context, ns string, req *QueryRequest) (*QueryResponse, error) {
	start := time.Now()
	debug := queryDebugEnabled()
	var (
		validateDur    time.Duration
		acquireDur     time.Duration
		loadDur        time.Duration
		refreshDur     time.Duration
		parseRankDur   time.Duration
		parseFilterDur time.Duration
		pendingDur     time.Duration
		execDur        time.Duration
		filterAttrDur  time.Duration
	)
	logDone := func(queryType string, err error) {
		if !debug {
			return
		}
		debugLogf(
			"[query] done ns=%s type=%s validate=%s acquire=%s load=%s refresh=%s parse_rank=%s parse_filter=%s pending=%s exec=%s filter_attrs=%s total=%s err=%v",
			ns,
			queryType,
			validateDur,
			acquireDur,
			loadDur,
			refreshDur,
			parseRankDur,
			parseFilterDur,
			pendingDur,
			execDur,
			filterAttrDur,
			time.Since(start),
			err,
		)
	}

	if debug {
		debugLogf("[query] start ns=%s consistency=%s limit=%d", ns, req.Consistency, req.Limit)
	}

	// Validate the request
	stepStart := time.Now()
	if err := h.validateRequest(req); err != nil {
		logDone("error", err)
		return nil, err
	}
	validateDur = time.Since(stepStart)
	if debug {
		debugLogf("[query] stage=validate ns=%s dur=%s", ns, validateDur)
	}

	// Acquire concurrency slot - blocks until available or context cancelled
	stepStart = time.Now()
	release, err := h.limiter.Acquire(ctx, ns)
	if err != nil {
		acquireDur = time.Since(stepStart)
		logDone("error", err)
		return nil, err
	}
	acquireDur = time.Since(stepStart)
	if debug {
		debugLogf("[query] stage=acquire ns=%s dur=%s", ns, acquireDur)
	}
	defer release()

	// Load namespace state
	stepStart = time.Now()
	loaded, err := h.stateMan.Load(ctx, ns)
	if err != nil {
		loadDur = time.Since(stepStart)
		logDone("error", err)
		if errors.Is(err, namespace.ErrStateNotFound) {
			return nil, ErrNamespaceNotFound
		}
		if errors.Is(err, namespace.ErrNamespaceTombstoned) {
			return nil, namespace.ErrNamespaceTombstoned
		}
		return nil, err
	}
	loadDur = time.Since(stepStart)
	if debug {
		debugLogf("[query] stage=load_state ns=%s dur=%s", ns, loadDur)
	}

	// Strong consistency is the default: refresh tail to WAL head to include all committed writes
	isStrongConsistency := req.Consistency != "eventual"
	if isStrongConsistency {
		if loaded.State.NamespaceFlags.DisableBackpressure && loaded.State.WAL.BytesUnindexedEst > MaxUnindexedBytes {
			logDone("error", ErrStrongQueryBackpressure)
			return nil, ErrStrongQueryBackpressure
		}
	}
	stepStart = time.Now()
	if err := h.refreshTailIfNeeded(ctx, ns, loaded, isStrongConsistency); err != nil {
		refreshDur = time.Since(stepStart)
		logDone("error", err)
		return nil, err
	}
	refreshDur = time.Since(stepStart)
	if debug {
		debugLogf("[query] stage=refresh ns=%s dur=%s", ns, refreshDur)
	}

	// Parse the rank_by expression
	stepStart = time.Now()
	parsed, err := parseRankBy(req.RankBy, req.VectorEncoding)
	if err != nil {
		parseRankDur = time.Since(stepStart)
		logDone("error", err)
		return nil, err
	}
	parseRankDur = time.Since(stepStart)
	if debug {
		debugLogf("[query] stage=parse_rank ns=%s dur=%s", ns, parseRankDur)
	}
	if err := validateVectorDims(loaded.State, parsed); err != nil {
		logDone("error", err)
		return nil, err
	}

	// Parse filters if provided
	var f *filter.Filter
	if req.Filters != nil {
		stepStart = time.Now()
		f, err = filter.Parse(req.Filters)
		if err != nil {
			parseFilterDur = time.Since(stepStart)
			logDone("error", ErrInvalidFilter)
			return nil, ErrInvalidFilter
		}
		parseFilterDur = time.Since(stepStart)
		if debug {
			debugLogf("[query] stage=parse_filter ns=%s dur=%s", ns, parseFilterDur)
		}

		// Validate regex filters against schema
		if f.UsesRegexOperators() {
			schemaChecker := buildSchemaChecker(loaded.State.Schema)
			if err := f.ValidateWithSchema(schemaChecker); err != nil {
				logDone("error", err)
				return nil, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
			}
		}
	}

	// Check for pending index rebuilds that affect this query
	stepStart = time.Now()
	if err := CheckPendingRebuilds(loaded.State, f, parsed); err != nil {
		pendingDur = time.Since(stepStart)
		logDone("error", err)
		return nil, err
	}
	pendingDur = time.Since(stepStart)
	if debug {
		debugLogf("[query] stage=pending ns=%s dur=%s", ns, pendingDur)
	}

	// Determine byte limit for eventual consistency
	var tailByteCap int64
	if req.Consistency == "eventual" {
		tailByteCap = EventualTailCapBytes
	}

	// Check if this is an aggregation query
	if req.AggregateBy != nil {
		queryType := queryTypeForRequest(nil, true)
		// Parse aggregations
		stepStart = time.Now()
		aggregations, err := parseAggregateBy(req.AggregateBy)
		if err != nil {
			execDur = time.Since(stepStart)
			logDone(queryType, err)
			return nil, err
		}
		execDur = time.Since(stepStart)

		// Check if this is a grouped aggregation
		if req.GroupBy != nil {
			// Parse group_by
			stepStart = time.Now()
			groupByAttrs, err := parseGroupBy(req.GroupBy)
			if err != nil {
				execDur = time.Since(stepStart)
				logDone(queryType, err)
				return nil, err
			}
			execDur = time.Since(stepStart)

			// Execute grouped aggregation query
			stepStart = time.Now()
			aggGroups, err := h.executeGroupedAggregationQuery(ctx, ns, f, aggregations, groupByAttrs, req.Limit, tailByteCap)
			if err != nil {
				execDur = time.Since(stepStart)
				logDone(queryType, err)
				return nil, err
			}
			execDur = time.Since(stepStart)

			resp := &QueryResponse{
				AggregationGroups: aggGroups,
				Billing: BillingInfo{
					BillableLogicalBytesQueried:  0,
					BillableLogicalBytesReturned: 0,
				},
			}
			logDone(queryType, nil)
			return h.finalizeQueryResponse(start, ns, queryType, resp), nil
		}

		// Execute non-grouped aggregation query
		stepStart = time.Now()
		aggResults, err := h.executeAggregationQuery(ctx, ns, f, aggregations, tailByteCap)
		if err != nil {
			execDur = time.Since(stepStart)
			logDone(queryType, err)
			return nil, err
		}
		execDur = time.Since(stepStart)

		resp := &QueryResponse{
			Aggregations: aggResults,
			Billing: BillingInfo{
				BillableLogicalBytesQueried:  0,
				BillableLogicalBytesReturned: 0,
			},
		}
		logDone(queryType, nil)
		return h.finalizeQueryResponse(start, ns, queryType, resp), nil
	}

	// Execute the query based on rank_by type
	var rows []Row
	if parsed != nil {
		stepStart = time.Now()
		switch parsed.Type {
		case RankByVector:
			rows, err = h.executeVectorQuery(ctx, ns, loaded, parsed, f, req, tailByteCap)
		case RankByAttr:
			rows, err = h.executeAttrQuery(ctx, ns, loaded, parsed, f, req, tailByteCap)
		case RankByBM25:
			rows, err = h.executeBM25Query(ctx, ns, loaded, parsed, f, req, tailByteCap)
		case RankByComposite:
			rows, err = h.executeCompositeQuery(ctx, ns, loaded, parsed, f, req, tailByteCap)
		}
		execDur = time.Since(stepStart)
		if debug {
			debugLogf("[query] stage=execute ns=%s type=%s dur=%s", ns, queryTypeForRequest(parsed, false), execDur)
		}
		if err != nil {
			logDone(queryTypeForRequest(parsed, false), err)
			return nil, err
		}
	}

	// Apply attribute filtering
	stepStart = time.Now()
	rows = filterAttributes(rows, req.IncludeAttributes, req.ExcludeAttributes)
	filterAttrDur = time.Since(stepStart)
	if debug {
		debugLogf("[query] stage=filter_attrs ns=%s dur=%s", ns, filterAttrDur)
	}

	queryType := queryTypeForRequest(parsed, false)
	resp := &QueryResponse{
		Rows: rows,
		Billing: BillingInfo{
			BillableLogicalBytesQueried:  0,
			BillableLogicalBytesReturned: 0,
		},
	}
	logDone(queryType, nil)
	return h.finalizeQueryResponse(start, ns, queryType, resp), nil
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
		// Not an array - could be a filter map for filter-based boost
		clause, err := ParseRankClause(rankBy)
		if err != nil {
			return nil, ErrInvalidRankBy
		}
		return &ParsedRankBy{
			Type:   RankByComposite,
			Clause: clause,
		}, nil
	}

	if len(arr) < 1 {
		return nil, ErrInvalidRankBy
	}

	// Check if first element is a string (operator or field name)
	field, ok := arr[0].(string)
	if !ok {
		return nil, ErrInvalidRankBy
	}

	// Check for composite operators: Sum, Max, Product
	switch field {
	case "Sum", "Max", "Product":
		clause, err := ParseRankClause(rankBy)
		if err != nil {
			return nil, err
		}
		return &ParsedRankBy{
			Type:   RankByComposite,
			Clause: clause,
		}, nil
	}

	if len(arr) < 2 {
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

	// Check for BM25: ["field", "BM25", "query"] or ["field", "BM25", {"query": "...", "last_as_prefix": true}]
	if len(arr) >= 3 {
		op, ok := arr[1].(string)
		if ok && op == "BM25" {
			queryText, lastAsPrefix, err := parseBM25QueryArg(arr[2])
			if err != nil {
				return nil, err
			}
			return &ParsedRankBy{
				Type:         RankByBM25,
				Field:        field,
				QueryText:    queryText,
				LastAsPrefix: lastAsPrefix,
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

// parseBM25QueryArg parses the BM25 query argument which can be:
// 1. A string: "hello world"
// 2. An object: {"query": "hello wor", "last_as_prefix": true}
func parseBM25QueryArg(v any) (queryText string, lastAsPrefix bool, err error) {
	switch val := v.(type) {
	case string:
		return val, false, nil
	case map[string]any:
		q, ok := val["query"].(string)
		if !ok {
			return "", false, ErrInvalidRankBy
		}
		if lap, ok := val["last_as_prefix"].(bool); ok {
			lastAsPrefix = lap
		}
		return q, lastAsPrefix, nil
	default:
		return "", false, ErrInvalidRankBy
	}
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

func validateVectorDims(state *namespace.State, parsed *ParsedRankBy) error {
	if parsed == nil || parsed.Type != RankByVector {
		return nil
	}

	expected, ok, err := expectedVectorDims(state)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidVectorDims, err)
	}
	if !ok {
		return nil
	}

	if len(parsed.QueryVector) != expected {
		return fmt.Errorf("%w: expected %d dimensions, got %d", ErrInvalidVectorDims, expected, len(parsed.QueryVector))
	}
	return nil
}

func expectedVectorDims(state *namespace.State) (int, bool, error) {
	if state == nil {
		return 0, false, nil
	}
	if state.Vector != nil && state.Vector.Dims > 0 {
		return state.Vector.Dims, true, nil
	}
	if state.Schema == nil || state.Schema.Attributes == nil {
		return 0, false, nil
	}
	attr, ok := state.Schema.Attributes["vector"]
	if !ok || attr.Vector == nil || attr.Vector.Type == "" {
		return 0, false, nil
	}
	dims, _, err := vector.ParseVectorType(attr.Vector.Type)
	if err != nil {
		return 0, false, err
	}
	if dims <= 0 {
		return 0, false, nil
	}
	return dims, true, nil
}

// executeVectorQuery executes a vector ANN query.
// tailByteCap of 0 means no limit (strong consistency); >0 means eventual consistency byte cap.
func (h *Handler) executeVectorQuery(ctx context.Context, ns string, loaded *namespace.LoadedState, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest, tailByteCap int64) ([]Row, error) {
	debug := queryDebugEnabled()
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

	// Determine if we can use the index
	// We can use the index:
	// 1. When no filter is present (unfiltered ANN search)
	// 2. When a filter is present AND we have filter bitmap indexes that cover the filter
	useIndex := loaded.State.Index.ManifestKey != "" && h.indexReader != nil

	if useIndex {
		t0 := time.Now()
		ivfReader, clusterDataKey, err := h.indexReader.GetIVFReader(ctx, ns, loaded.State.Index.ManifestKey, loaded.State.Index.ManifestSeq)
		getIVFReaderMs := time.Since(t0).Milliseconds()
		if err != nil {
			// Log error but fall back to exhaustive search
			if debug {
				debugLogf("[query] get_ivf_reader namespace=%s err=%v dur_ms=%d", ns, err, getIVFReaderMs)
			}
		} else if ivfReader != nil {
			if debug {
				debugLogf("[query] get_ivf_reader namespace=%s clusters=%d vectors=%d dur_ms=%d", ns, ivfReader.NClusters, len(ivfReader.ClusterOffsets), getIVFReaderMs)
			}
			nProbe := DefaultNProbe
			candidates := req.Limit

			if f == nil {
				// No filter - use standard ANN search
				t1 := time.Now()
				indexResults, err = h.indexReader.SearchWithMultiRange(ctx, ivfReader, clusterDataKey, parsed.QueryVector, candidates, nProbe)
				searchMs := time.Since(t1).Milliseconds()
				if err != nil {
					if debug {
						debugLogf("[query] search_with_multi_range namespace=%s err=%v dur_ms=%d", ns, err, searchMs)
					}
					indexResults = nil
				} else {
					if debug {
						debugLogf("[query] search_with_multi_range namespace=%s dur_ms=%d nProbe=%d candidates=%d results=%d", ns, searchMs, nProbe, candidates, len(indexResults))
					}
				}
			} else {
				// Filter present - try to use filter bitmap indexes
				t1 := time.Now()
				indexResults = h.searchIndexWithFilter(ctx, ns, loaded, ivfReader, clusterDataKey, parsed.QueryVector, candidates, nProbe, f)
				if debug {
					debugLogf("[query] search_index_with_filter namespace=%s dur_ms=%d results=%d", ns, time.Since(t1).Milliseconds(), len(indexResults))
				}
			}
		} else {
			if debug {
				debugLogf("[query] get_ivf_reader namespace=%s dur_ms=%d reader=nil", ns, getIVFReaderMs)
			}
		}
	} else {
		if debug {
			debugLogf("[query] ann_use_index=false namespace=%s manifestKey=%q", ns, loaded.State.Index.ManifestKey)
		}
	}

	// Get results from tail (unindexed data)
	var tailResults []tail.VectorScanResult
	if h.tailStore != nil {
		t2 := time.Now()
		var err error
		// Only search tail for WAL entries after the indexed sequence
		if tailByteCap > 0 {
			tailResults, err = h.tailStore.VectorScanWithByteLimit(ctx, ns, parsed.QueryVector, req.Limit, metric, f, tailByteCap)
		} else {
			tailResults, err = h.tailStore.VectorScan(ctx, ns, parsed.QueryVector, req.Limit, metric, f)
		}
		if debug {
			debugLogf("[query] tail_search namespace=%s dur_ms=%d results=%d byte_cap=%d", ns, time.Since(t2).Milliseconds(), len(tailResults), tailByteCap)
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
	// Only load the specific documents we need, not all documents
	var indexDocIDs map[uint64]document.ID
	var indexDocAttrs map[uint64]map[string]any
	var segmentStates map[string]segmentDocState

	// Optimization: skip expensive doc loading when safe
	// We can skip if:
	// 1. No tail results to dedupe against
	// 2. Only 1 segment (no cross-segment tombstones)
	// 3. All data is indexed (bytes_unindexed_est == 0)
	// This is critical for large indices where docs.col.zst can be gigabytes
	// Note: We disable this optimization and always load docs because:
	// - LoadDocsForIDs only loads specific result docs, not all 191k docs
	// - Users almost always need attributes returned
	var skipDocLoad bool
	if false && len(tailResults) == 0 && h.indexReader != nil {
		// Check if there's unindexed data
		bytesUnindexed := loaded.State.WAL.BytesUnindexedEst
		if bytesUnindexed == 0 {
			segments, err := h.indexReader.GetManifestSegments(ctx, loaded.State.Index.ManifestKey)
			if err == nil && len(segments) == 1 {
				// Single segment with no unindexed data - safe to skip doc loading
				// Results will use numeric IDs instead of string document IDs
				skipDocLoad = true
			}
		}
	}

	if len(indexResults) > 0 && h.indexReader != nil && !skipDocLoad {
		// Extract the numeric IDs from index results
		resultIDs := make([]uint64, 0, len(indexResults))
		for _, res := range indexResults {
			resultIDs = append(resultIDs, res.DocID)
		}

		// Load only the documents we need (not all docs in the segment)
		// Always use LoadDocsForIDs which searches ALL segments to find tombstones
		// that may exist in later segments. This is critical for correctness when
		// documents are deleted in a segment after being indexed in an earlier segment.
		t3 := time.Now()
		var err error
		var indexedDocs []index.IndexedDocument
		indexedDocs, err = h.indexReader.LoadDocsForIDs(ctx, loaded.State.Index.ManifestKey, resultIDs)
		if debug {
			debugLogf("[query] load_docs_for_ids namespace=%s dur_ms=%d requested=%d loaded=%d err=%v", ns, time.Since(t3).Milliseconds(), len(resultIDs), len(indexedDocs), err)
		}
		if err == nil && len(indexedDocs) > 0 {
			indexDocIDs = make(map[uint64]document.ID, len(indexedDocs))
			indexDocAttrs = make(map[uint64]map[string]any, len(indexedDocs))
			segmentStates = make(map[string]segmentDocState, len(indexedDocs))

			for _, idoc := range indexedDocs {
				// Build numericID -> document.ID mapping
				if idoc.NumericID != 0 {
					if docID, ok := indexedDocumentID(idoc); ok {
						indexDocIDs[idoc.NumericID] = docID
					}
					// Build numericID -> attributes mapping
					if idoc.Attributes != nil {
						indexDocAttrs[idoc.NumericID] = idoc.Attributes
					}
				}

				// Build docID -> segmentDocState mapping
				if docID, ok := indexedDocumentID(idoc); ok {
					idStr := docID.String()
					if existing, ok := segmentStates[idStr]; !ok || idoc.WALSeq > existing.walSeq {
						segmentStates[idStr] = segmentDocState{
							walSeq:  idoc.WALSeq,
							deleted: idoc.Deleted,
						}
					}
				}
			}
		}
	}
	rows := h.mergeVectorResults(ctx, ns, indexResults, tailResults, req.Limit, vectorMetric, indexedWALSeq, indexDocIDs, indexDocAttrs, segmentStates)
	return rows, nil
}

// searchIndexWithFilter attempts to search the IVF index with a filter.
// It loads filter bitmap indexes, evaluates the filter to get a bitmap of matching docIDs,
// and then searches the IVF index with that filter constraint.
// Returns nil if filter indexes are not available for the required fields.
func (h *Handler) searchIndexWithFilter(ctx context.Context, ns string, loaded *namespace.LoadedState, ivfReader *vector.IVFReader, clusterDataKey string, queryVector []float32, candidates, nProbe int, f *filter.Filter) []vector.IVFSearchResult {
	if h.indexReader == nil || f == nil {
		return nil
	}

	// Get segments from manifest to find filter keys
	segments, err := h.indexReader.GetManifestSegments(ctx, loaded.State.Index.ManifestKey)
	if err != nil || len(segments) == 0 {
		return nil
	}

	var filterBitmap *roaring.Bitmap
	for _, seg := range segments {
		hasIVF := (seg.IVFKeys != nil && seg.IVFKeys.HasIVF()) || seg.VectorsKey != ""
		if !hasIVF {
			continue
		}
		if len(seg.FilterKeys) == 0 {
			return nil
		}

		filterIndexes, err := h.indexReader.LoadFilterIndexes(ctx, seg.FilterKeys)
		if err != nil || len(filterIndexes) == 0 {
			return nil
		}

		var totalDocs uint32
		if seg.Stats.RowCount > 0 {
			totalDocs = uint32(seg.Stats.RowCount)
		} else if seg.IVFKeys != nil && seg.IVFKeys.VectorCount > 0 {
			totalDocs = uint32(seg.IVFKeys.VectorCount)
		} else {
			for _, idx := range filterIndexes {
				if docCount := idx.GetDocCount(); docCount > totalDocs {
					totalDocs = docCount
				}
			}
		}

		segBitmap := h.indexReader.EvaluateFilterOnIndex(f, filterIndexes, totalDocs)
		if segBitmap == nil {
			return nil
		}
		if segBitmap.IsEmpty() {
			continue
		}
		if filterBitmap == nil {
			filterBitmap = segBitmap
		} else {
			filterBitmap.Or(segBitmap)
		}
	}

	if filterBitmap == nil {
		return nil
	}

	if filterBitmap.IsEmpty() {
		return []vector.IVFSearchResult{}
	}

	// Search the IVF index with the filter constraint
	results, err := h.indexReader.SearchWithFilter(ctx, ivfReader, clusterDataKey, queryVector, candidates, nProbe, filterBitmap)
	if err != nil {
		return nil
	}

	return results
}

// mergeVectorResults merges IVF index results with tail results.
// Tail results take precedence for deduplication since they contain newer data.
// Deduplication ensures last-write-wins semantics across segments and tail.
//
// The deduplication strategy:
// 1. Tail documents have explicit WAL seq and always take precedence for the same doc ID
// 2. Index results have WAL seq <= indexedWALSeq (when the index was built)
// 3. For duplicate doc IDs in index results, we keep the first occurrence (they should be unique in a well-formed index)
// 4. Tombstones in tail exclude documents from results
type segmentDocState struct {
	walSeq  uint64
	deleted bool
}

func (h *Handler) mergeVectorResults(ctx context.Context, ns string, indexResults []vector.IVFSearchResult, tailResults []tail.VectorScanResult, limit int, metric vector.DistanceMetric, indexedWALSeq uint64, indexDocIDs map[uint64]document.ID, indexDocAttrs map[uint64]map[string]any, segmentStates map[string]segmentDocState) []Row {
	type tailCandidate struct {
		doc  *tail.Document
		dist float64
	}

	tailByID := make(map[string]tailCandidate, len(tailResults))
	for _, res := range tailResults {
		if res.Doc == nil {
			continue
		}
		idStr := res.Doc.ID.String()
		existing, ok := tailByID[idStr]
		if !ok {
			tailByID[idStr] = tailCandidate{doc: res.Doc, dist: res.Distance}
			continue
		}
		newVersion := DocVersion{WalSeq: res.Doc.WalSeq, SubBatchID: res.Doc.SubBatchID}
		oldVersion := DocVersion{WalSeq: existing.doc.WalSeq, SubBatchID: existing.doc.SubBatchID}
		if newVersion.IsNewerThan(oldVersion) {
			tailByID[idStr] = tailCandidate{doc: res.Doc, dist: res.Distance}
		}
	}

	// Track distances for tail/index candidates keyed by document ID.
	type scoredDoc struct {
		docID      any
		dist       float64
		attributes map[string]any
	}

	// Map to track best distance per unique doc ID (after deduplication)
	docDistances := make(map[string]scoredDoc)

	// Add tail results with their distances (skip deleted/tombstoned docs)
	for idStr, entry := range tailByID {
		if entry.doc.Deleted {
			continue
		}
		docDistances[idStr] = scoredDoc{
			docID:      docIDToAny(entry.doc.ID),
			dist:       entry.dist,
			attributes: entry.doc.Attributes,
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

		docID := document.NewU64ID(res.DocID)
		if indexDocIDs != nil {
			if mapped, ok := indexDocIDs[res.DocID]; ok {
				docID = mapped
			}
		}
		idStr := docID.String()

		indexWalSeq := indexedWALSeq
		if segmentStates != nil {
			if state, ok := segmentStates[idStr]; ok {
				indexWalSeq = state.walSeq
			}
		}

		if tailEntry, ok := tailByID[idStr]; ok {
			tailVersion := DocVersion{WalSeq: tailEntry.doc.WalSeq, SubBatchID: tailEntry.doc.SubBatchID}
			indexVersion := DocVersion{WalSeq: indexWalSeq}
			if tailVersion.IsNewerThan(indexVersion) {
				continue
			}
		}

		if segmentStates != nil {
			if state, ok := segmentStates[idStr]; ok && state.deleted {
				continue
			}
		}

		// Add to results - doc not in tail so index version is current
		var attrs map[string]any
		if indexDocAttrs != nil {
			attrs = indexDocAttrs[res.DocID]
		}
		docDistances[idStr] = scoredDoc{
			docID:      docIDToAny(docID),
			dist:       float64(res.Distance),
			attributes: attrs,
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
// This function queries both indexed segments and tail data, merging results with
// tail taking precedence for deduplication (newer data).
func (h *Handler) executeAttrQuery(ctx context.Context, ns string, loaded *namespace.LoadedState, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest, tailByteCap int64) ([]Row, error) {
	debug := queryDebugEnabled()
	start := time.Now()
	var (
		indexedCount int
		tailCount    int
		dedupedCount int
		loadIndexDur time.Duration
		tailScanDur  time.Duration
		dedupeDur    time.Duration
		sortDur      time.Duration
	)

	// Collect all documents from both index and tail
	var allDocs []*tail.Document

	// Step 1: Load documents from indexed segments if available
	indexedWALSeq := loaded.State.Index.IndexedWALSeq
	if loaded.State.Index.ManifestKey != "" && h.indexReader != nil {
		loadStart := time.Now()
		indexedDocs, err := h.indexReader.LoadSegmentDocs(ctx, loaded.State.Index.ManifestKey)
		loadIndexDur = time.Since(loadStart)
		if err == nil && len(indexedDocs) > 0 {
			// Convert indexed documents to tail.Document format
			for _, idoc := range indexedDocs {
				// Skip deleted documents from index
				if idoc.Deleted {
					continue
				}

				docID, ok := indexedDocumentID(idoc)
				if !ok {
					continue // Skip documents with no valid ID
				}

				doc := &tail.Document{
					ID:         docID,
					WalSeq:     idoc.WALSeq,
					Attributes: idoc.Attributes,
					Deleted:    false,
				}

				// Apply filter if present
				if f != nil {
					filterDoc := make(map[string]any)
					filterDoc["id"] = docIDToAny(doc.ID)
					for k, v := range doc.Attributes {
						filterDoc[k] = v
					}
					if !f.Eval(filterDoc) {
						continue
					}
				}

				allDocs = append(allDocs, doc)
			}
			indexedCount = len(allDocs)
		}
	}

	// Step 2: Get documents from tail (unindexed data)
	if h.tailStore != nil {
		var tailDocs []*tail.Document
		var err error
		tailStart := time.Now()
		if tailByteCap > 0 {
			tailDocs, err = h.tailStore.ScanWithByteLimit(ctx, ns, f, tailByteCap)
		} else {
			tailDocs, err = h.tailStore.Scan(ctx, ns, f)
		}
		tailScanDur = time.Since(tailStart)
		if err != nil {
			return nil, err
		}
		tailCount = len(tailDocs)
		allDocs = append(allDocs, tailDocs...)
	}

	// Step 3: Deduplicate documents - tail (newer WAL seq) takes precedence
	dedupeStart := time.Now()
	allDocs = h.deduplicateAttrQueryDocs(allDocs, indexedWALSeq)
	dedupeDur = time.Since(dedupeStart)
	dedupedCount = len(allDocs)

	// Step 4: Sort by the specified attribute
	sortStart := time.Now()
	sort.Slice(allDocs, func(i, j int) bool {
		vi := getDocAttrValue(allDocs[i], parsed.Field)
		vj := getDocAttrValue(allDocs[j], parsed.Field)
		cmp := compareValues(vi, vj)
		if parsed.Direction == "desc" {
			return cmp > 0
		}
		return cmp < 0
	})
	sortDur = time.Since(sortStart)

	// Step 5: Apply limit with optional per diversification
	var selected []*tail.Document
	if len(req.Per) > 0 {
		// Diversification: limit N results per distinct value of the per attribute
		selected = applyPerDiversification(allDocs, req.Per, req.Limit)
	} else {
		// Simple limit
		if len(allDocs) > req.Limit {
			selected = allDocs[:req.Limit]
		} else {
			selected = allDocs
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

	if debug {
		debugLogf(
			"[attr] ns=%s indexed=%d tail=%d deduped=%d load_index=%s tail_scan=%s dedupe=%s sort=%s total=%s",
			ns,
			indexedCount,
			tailCount,
			dedupedCount,
			loadIndexDur,
			tailScanDur,
			dedupeDur,
			sortDur,
			time.Since(start),
		)
	}
	return rows, nil
}

// deduplicateAttrQueryDocs removes duplicate documents, keeping the one with highest WAL seq.
// For documents with the same ID, the one from tail (higher WAL seq) takes precedence.
// Deleted (tombstoned) documents are excluded from results.
func (h *Handler) deduplicateAttrQueryDocs(docs []*tail.Document, indexedWALSeq uint64) []*tail.Document {
	if len(docs) == 0 {
		return docs
	}

	// Track best version of each document by ID
	docMap := make(map[string]*tail.Document)

	for _, doc := range docs {
		idStr := doc.ID.String()
		existing, found := docMap[idStr]
		if !found {
			// First occurrence of this doc
			if !doc.Deleted {
				docMap[idStr] = doc
			}
		} else {
			// Compare WAL sequences - higher wins
			if doc.WalSeq > existing.WalSeq {
				if doc.Deleted {
					// Newer version is deleted, remove from results
					delete(docMap, idStr)
				} else {
					docMap[idStr] = doc
				}
			}
		}
	}

	// Convert map back to slice
	result := make([]*tail.Document, 0, len(docMap))
	for _, doc := range docMap {
		result = append(result, doc)
	}

	return result
}

// executeBM25Query executes a BM25 full-text search query.
// It searches both indexed segments and tail for documents matching the query
// and ranks them by BM25 score.
// Documents with zero score are excluded from results.
// When LastAsPrefix is true, the last token is matched as a prefix for typeahead support,
// and prefix matches score 1.0 (as per turbopuffer spec).
func (h *Handler) executeBM25Query(ctx context.Context, ns string, loaded *namespace.LoadedState, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest, tailByteCap int64) ([]Row, error) {
	debug := queryDebugEnabled()
	start := time.Now()

	// Get FTS config for the field from schema
	ftsCfg := h.getFTSConfig(loaded, parsed.Field)
	if ftsCfg == nil {
		// Use default config if not configured
		ftsCfg = fts.DefaultConfig()
	}

	// Tokenize the query
	tokenizeStart := time.Now()
	tokenizer := fts.NewTokenizer(ftsCfg)
	queryTokens := tokenizer.Tokenize(parsed.QueryText)
	if debug {
		debugLogf("[bm25] ns=%s tokens=%d tokenize=%s", ns, len(queryTokens), time.Since(tokenizeStart))
	}

	// If query produces no tokens, return empty results
	if len(queryTokens) == 0 {
		if debug {
			debugLogf("[bm25] ns=%s empty_tokens total=%s", ns, time.Since(start))
		}
		return []Row{}, nil
	}

	// Prefer prebuilt FTS indexes when available.
	if loaded.State.Index.ManifestKey != "" && h.indexReader != nil {
		indexStart := time.Now()
		rows, ok, err := h.executeBM25QueryWithFTSIndexes(ctx, ns, loaded, parsed, f, req, tailByteCap, ftsCfg, queryTokens)
		if debug {
			debugLogf("[bm25] ns=%s index_ok=%t rows=%d dur=%s err=%v", ns, ok, len(rows), time.Since(indexStart), err)
		}
		if err != nil {
			return nil, err
		}
		if ok {
			return rows, nil
		}
	}

	if debug {
		debugLogf("[bm25] ns=%s fallback_full_scan start total=%s", ns, time.Since(start))
	}
	return h.executeBM25QueryFullScan(ctx, ns, loaded, parsed, f, req, tailByteCap, ftsCfg, queryTokens)
}

func (h *Handler) executeBM25QueryWithFTSIndexes(ctx context.Context, ns string, loaded *namespace.LoadedState, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest, tailByteCap int64, ftsCfg *fts.Config, queryTokens []string) ([]Row, bool, error) {
	debug := queryDebugEnabled()
	loadStart := time.Now()
	maxSegments := bm25MaxSegments()
	var ftsIndexes []index.FTSSegmentIndex
	var err error
	manifest, err := h.indexReader.LoadManifest(ctx, loaded.State.Index.ManifestKey)
	if err != nil {
		if debug {
			debugLogf("[bm25] ns=%s fts_manifest err=%v dur=%s", ns, err, time.Since(loadStart))
		}
		return nil, false, err
	}
	if manifest == nil || len(manifest.Segments) == 0 {
		return nil, false, nil
	}

	segments := manifest.Segments
	segmentsToLoad := segments
	filteredOutAll := false

	termsStart := time.Now()
	segmentTerms, termErr := h.indexReader.LoadFTSTermsForField(ctx, loaded.State.Index.ManifestKey, parsed.Field)
	if termErr == nil && len(segmentTerms) > 0 {
		termsByID := make(map[string][]string, len(segmentTerms))
		for _, entry := range segmentTerms {
			termsByID[entry.Segment.ID] = entry.Terms
		}
		filtered := make([]index.Segment, 0, len(segments))
		for _, seg := range segments {
			terms, ok := termsByID[seg.ID]
			if ok && !ftsTermsMatch(terms, queryTokens, parsed.LastAsPrefix) {
				continue
			}
			filtered = append(filtered, seg)
		}
		segmentsToLoad = filtered
		if len(segmentsToLoad) == 0 {
			filteredOutAll = true
		}
	}
	if debug {
		debugLogf("[bm25] ns=%s fts_terms=%d segments=%d filtered=%d term_dur=%s err=%v", ns, len(segmentTerms), len(segments), len(segmentsToLoad), time.Since(termsStart), termErr)
	}

	if maxSegments > 0 && len(segmentsToLoad) > maxSegments {
		ordered := make([]index.Segment, len(segmentsToLoad))
		copy(ordered, segmentsToLoad)
		sort.Slice(ordered, func(i, j int) bool {
			if ordered[i].EndWALSeq == ordered[j].EndWALSeq {
				return ordered[i].ID > ordered[j].ID
			}
			return ordered[i].EndWALSeq > ordered[j].EndWALSeq
		})
		segmentsToLoad = ordered[:maxSegments]
	}

	if len(segmentsToLoad) > 0 {
		ftsIndexes, err = h.indexReader.LoadFTSIndexesForFieldInSegments(ctx, loaded.State.Index.ManifestKey, parsed.Field, segmentsToLoad)
		if err != nil {
			if debug {
				debugLogf("[bm25] ns=%s fts_load err=%v dur=%s", ns, err, time.Since(loadStart))
			}
			return nil, false, err
		}
	}
	if debug {
		debugLogf("[bm25] ns=%s fts_indexes=%d max_segments=%d load_dur=%s", ns, len(ftsIndexes), maxSegments, time.Since(loadStart))
	}
	if len(ftsIndexes) == 0 && !filteredOutAll {
		return nil, false, nil
	}

	tailStart := time.Now()
	tailLatest, tailScores, err := h.loadTailBM25Scores(ctx, ns, parsed, f, tailByteCap, ftsCfg, queryTokens)
	if err != nil {
		if debug {
			debugLogf("[bm25] ns=%s tail_scores err=%v dur=%s", ns, err, time.Since(tailStart))
		}
		return nil, false, err
	}
	if debug {
		debugLogf("[bm25] ns=%s tail_docs=%d tail_scores=%d dur=%s", ns, len(tailLatest), len(tailScores), time.Since(tailStart))
	}

	segmentLimit := req.Limit + len(tailLatest)
	if segmentLimit < req.Limit {
		segmentLimit = req.Limit
	}
	if segmentLimit <= 0 {
		segmentLimit = req.Limit
	}

	var fallbackRows []Row
	for {
		indexed := make(map[string]bm25Candidate)

		type bm25SegmentRows struct {
			seg  index.Segment
			rows []bm25RowScore
		}

		segments := make([]bm25SegmentRows, 0, len(ftsIndexes))
		totalCandidates := 0
		truncated := false
		for _, seg := range ftsIndexes {
			topStart := time.Now()
			topRows := bm25TopRowScores(seg.Index, queryTokens, parsed.LastAsPrefix, ftsCfg, segmentLimit)
			if debug {
				debugLogf("[bm25] ns=%s segment=%s top_rows=%d top_dur=%s", ns, seg.Segment.ID, len(topRows), time.Since(topStart))
			}
			if len(topRows) == 0 {
				continue
			}
			if len(topRows) == segmentLimit {
				truncated = true
			}
			segments = append(segments, bm25SegmentRows{seg: seg.Segment, rows: topRows})
			totalCandidates += len(topRows)
		}

		if len(segments) == 0 {
			scored := mergeBM25Candidates(indexed, tailLatest, tailScores)
			sort.Slice(scored, func(i, j int) bool {
				return scored[i].score > scored[j].score
			})
			if len(scored) > req.Limit {
				scored = scored[:req.Limit]
			}
			rows := make([]Row, 0, len(scored))
			for _, sc := range scored {
				dist := sc.score
				rows = append(rows, Row{
					ID:         docIDToAny(sc.doc.ID),
					Dist:       &dist,
					Attributes: sc.doc.Attributes,
				})
			}
			if debug {
				debugLogf("[bm25] ns=%s index_rows=%d indexed_candidates=%d", ns, len(rows), len(scored))
			}
			return rows, true, nil
		}

		merge := make(bm25MergeHeap, 0, len(segments))
		for i, seg := range segments {
			merge = append(merge, bm25MergeItem{
				segIdx: i,
				rowIdx: 0,
				score:  seg.rows[0].score,
			})
		}
		heap.Init(&merge)

		candidates := make([]bm25RowCandidate, 0, totalCandidates)
		processed := 0
		budget := req.Limit + len(tailLatest)
		if budget < req.Limit {
			budget = req.Limit
		}
		if f != nil {
			budget += req.Limit
			if budget < req.Limit {
				budget = req.Limit
			}
		}
		if budget > totalCandidates {
			budget = totalCandidates
		}

		for {
			for len(candidates) < budget && merge.Len() > 0 {
				item := heap.Pop(&merge).(bm25MergeItem)
				segRows := segments[item.segIdx].rows
				row := segRows[item.rowIdx]
				candidates = append(candidates, bm25RowCandidate{
					segIdx: item.segIdx,
					rowID:  row.rowID,
					score:  row.score,
				})
				nextIdx := item.rowIdx + 1
				if nextIdx < len(segRows) {
					heap.Push(&merge, bm25MergeItem{
						segIdx: item.segIdx,
						rowIdx: nextIdx,
						score:  segRows[nextIdx].score,
					})
				}
			}

			if processed < len(candidates) {
				batches := make(map[int][]bm25RowCandidate)
				for _, cand := range candidates[processed:] {
					batches[cand.segIdx] = append(batches[cand.segIdx], cand)
				}

				type bm25BatchResult struct {
					segIdx     int
					candidates []bm25Candidate
					docCount   int
					dur        time.Duration
					err        error
				}

				loadConcurrency := bm25LoadConcurrency()
				if loadConcurrency < 1 {
					loadConcurrency = 1
				}
				sem := make(chan struct{}, loadConcurrency)
				results := make(chan bm25BatchResult, len(batches))
				var wg sync.WaitGroup

				for segIdx, batch := range batches {
					segIdx := segIdx
					batch := batch
					wg.Add(1)
					sem <- struct{}{}
					go func() {
						defer wg.Done()
						defer func() { <-sem }()

						rowIDs := make([]uint32, 0, len(batch))
						for _, cand := range batch {
							rowIDs = append(rowIDs, cand.rowID)
						}

						loadStart := time.Now()
						docsByRow, err := h.indexReader.LoadDocsForRowIDs(ctx, segments[segIdx].seg, rowIDs)
						result := bm25BatchResult{
							segIdx:   segIdx,
							docCount: len(docsByRow),
							dur:      time.Since(loadStart),
							err:      err,
						}
						if err == nil && len(docsByRow) > 0 {
							cands := make([]bm25Candidate, 0, len(batch))
							for _, cand := range batch {
								idoc, ok := docsByRow[cand.rowID]
								if !ok {
									continue
								}
								if cand.score <= 0 {
									continue
								}

								docID, ok := indexedDocumentID(idoc)
								if !ok {
									continue
								}

								doc := &tail.Document{
									ID:         docID,
									WalSeq:     idoc.WALSeq,
									Attributes: idoc.Attributes,
									Deleted:    idoc.Deleted,
								}

								cands = append(cands, bm25Candidate{
									doc:         doc,
									score:       cand.score,
									filterMatch: filterMatches(f, doc),
								})
							}
							result.candidates = cands
						}
						results <- result
					}()
				}

				wg.Wait()
				close(results)

				for result := range results {
					if result.err != nil {
						return nil, false, result.err
					}
					if debug {
						debugLogf("[bm25] ns=%s segment=%s docs=%d load_dur=%s", ns, segments[result.segIdx].seg.ID, result.docCount, result.dur)
					}
					for _, cand := range result.candidates {
						key := cand.doc.ID.String()
						existing, ok := indexed[key]
						if ok && cand.doc.WalSeq < existing.doc.WalSeq {
							continue
						}
						indexed[key] = cand
					}
				}
				processed = len(candidates)
			}

			scored := mergeBM25Candidates(indexed, tailLatest, tailScores)
			if len(scored) >= req.Limit || merge.Len() == 0 || budget >= totalCandidates {
				sort.Slice(scored, func(i, j int) bool {
					return scored[i].score > scored[j].score
				})
				if len(scored) > req.Limit {
					scored = scored[:req.Limit]
				}

				rows := make([]Row, 0, len(scored))
				for _, sc := range scored {
					dist := sc.score
					rows = append(rows, Row{
						ID:         docIDToAny(sc.doc.ID),
						Dist:       &dist,
						Attributes: sc.doc.Attributes,
					})
				}

				if debug {
					debugLogf("[bm25] ns=%s index_rows=%d indexed_candidates=%d", ns, len(rows), len(scored))
				}

				if len(scored) >= req.Limit || !truncated {
					return rows, true, nil
				}
				fallbackRows = rows
				break
			}

			newBudget := budget * 2
			if newBudget <= budget {
				newBudget = budget + 1
			}
			if newBudget > totalCandidates {
				newBudget = totalCandidates
			}
			budget = newBudget
		}

		nextLimit := segmentLimit * 2
		if nextLimit <= segmentLimit {
			if fallbackRows == nil {
				return []Row{}, true, nil
			}
			return fallbackRows, true, nil
		}
		segmentLimit = nextLimit
	}
}

func (h *Handler) loadTailBM25Scores(ctx context.Context, ns string, parsed *ParsedRankBy, f *filter.Filter, tailByteCap int64, ftsCfg *fts.Config, queryTokens []string) (map[string]*tail.Document, map[string]float64, error) {
	tailLatest := make(map[string]*tail.Document)
	if h.tailStore == nil {
		return tailLatest, map[string]float64{}, nil
	}

	tailDocs, err := h.scanTailForBM25(ctx, ns, f, tailByteCap)
	if err != nil {
		return nil, nil, err
	}

	for _, doc := range tailDocs {
		key := doc.ID.String()
		if existing, ok := tailLatest[key]; !ok || doc.WalSeq > existing.WalSeq {
			tailLatest[key] = doc
		}
	}

	if len(tailLatest) == 0 {
		return tailLatest, map[string]float64{}, nil
	}

	index := fts.NewIndex(parsed.Field, ftsCfg)
	docIDMap := make(map[uint32]*tail.Document)
	i := uint32(0)
	for _, doc := range tailLatest {
		if doc.Deleted {
			continue
		}
		text, ok := h.getDocTextValue(doc, parsed.Field)
		if !ok || text == "" {
			continue
		}
		index.AddDocument(i, text)
		docIDMap[i] = doc
		i++
	}

	scores := make(map[string]float64)
	if index.TotalDocs == 0 {
		return tailLatest, scores, nil
	}

	for docID, doc := range docIDMap {
		score := computeBM25ScoreWithPrefixAndMatch(index, docID, queryTokens, parsed.LastAsPrefix, ftsCfg, false)
		if score > 0 {
			scores[doc.ID.String()] = score
		}
	}

	return tailLatest, scores, nil
}

func (h *Handler) executeBM25QueryFullScan(ctx context.Context, ns string, loaded *namespace.LoadedState, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest, tailByteCap int64, ftsCfg *fts.Config, queryTokens []string) ([]Row, error) {
	debug := queryDebugEnabled()
	start := time.Now()
	var (
		indexedCount  int
		tailCount     int
		dedupedCount  int
		loadIndexDur  time.Duration
		tailScanDur   time.Duration
		buildIndexDur time.Duration
		scoreDur      time.Duration
		sortDur       time.Duration
	)

	// Collect all documents from both indexed segments and tail
	var allDocs []*tail.Document
	// Step 1: Load documents from indexed segments if available
	if loaded.State.Index.ManifestKey != "" && h.indexReader != nil {
		loadStart := time.Now()
		indexedDocs, err := h.indexReader.LoadSegmentDocs(ctx, loaded.State.Index.ManifestKey)
		loadIndexDur = time.Since(loadStart)
		if err == nil && len(indexedDocs) > 0 {
			// Convert indexed documents to tail.Document format
			for _, idoc := range indexedDocs {
				// Skip deleted documents from index
				if idoc.Deleted {
					continue
				}

				docID, ok := indexedDocumentID(idoc)
				if !ok {
					continue // Skip documents with no valid ID
				}

				doc := &tail.Document{
					ID:         docID,
					WalSeq:     idoc.WALSeq,
					Attributes: idoc.Attributes,
					Deleted:    false,
				}

				// Apply filter if present
				if f != nil && !filterMatches(f, doc) {
					continue
				}

				allDocs = append(allDocs, doc)
			}
			indexedCount = len(allDocs)
		}
	}

	// Step 2: Get documents from tail (unindexed data), including deletes for deduplication.
	if h.tailStore != nil {
		tailStart := time.Now()
		tailDocs, err := h.scanTailForBM25(ctx, ns, f, tailByteCap)
		tailScanDur = time.Since(tailStart)
		if err != nil {
			return nil, err
		}
		tailCount = len(tailDocs)
		allDocs = append(allDocs, tailDocs...)
	}

	// Step 3: Deduplicate documents - tail (newer WAL seq) takes precedence
	allDocs = h.deduplicateBM25Docs(allDocs)
	dedupedCount = len(allDocs)

	if len(allDocs) == 0 {
		if debug {
			debugLogf("[bm25] ns=%s full_scan empty indexed=%d tail=%d total=%s", ns, indexedCount, tailCount, time.Since(start))
		}
		return []Row{}, nil
	}

	// Step 4: Build a temporary FTS index from all documents for BM25 scoring
	// This computes term frequencies and document statistics needed for BM25
	buildStart := time.Now()
	index := fts.NewIndex(parsed.Field, ftsCfg)
	docIDMap := make(map[uint32]*tail.Document) // Map internal doc IDs to docs

	for i, doc := range allDocs {
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
	buildIndexDur = time.Since(buildStart)

	// If no documents have the field, return empty results
	if index.TotalDocs == 0 {
		if debug {
			debugLogf("[bm25] ns=%s full_scan no_docs indexed=%d tail=%d deduped=%d build=%s total=%s", ns, indexedCount, tailCount, dedupedCount, buildIndexDur, time.Since(start))
		}
		return []Row{}, nil
	}

	// Step 5: Score documents using BM25 (with optional prefix matching for last token)
	type scoredDoc struct {
		doc   *tail.Document
		score float64
	}
	scoreStart := time.Now()
	var scored []scoredDoc

	for docID, doc := range docIDMap {
		score := computeBM25ScoreWithPrefix(index, docID, queryTokens, parsed.LastAsPrefix, ftsCfg)
		if score > 0 {
			scored = append(scored, scoredDoc{doc: doc, score: score})
		}
	}
	scoreDur = time.Since(scoreStart)

	// Sort by score descending
	sortStart := time.Now()
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})
	sortDur = time.Since(sortStart)

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

	if debug {
		debugLogf(
			"[bm25] ns=%s full_scan indexed=%d tail=%d deduped=%d scored=%d load_index=%s tail_scan=%s build=%s score=%s sort=%s total=%s",
			ns,
			indexedCount,
			tailCount,
			dedupedCount,
			len(scored),
			loadIndexDur,
			tailScanDur,
			buildIndexDur,
			scoreDur,
			sortDur,
			time.Since(start),
		)
	}
	return rows, nil
}

// deduplicateBM25Docs removes duplicate documents, keeping the one with higher WAL seq.
// If a document appears in both indexed (lower WAL seq) and tail (higher WAL seq),
// the tail version takes precedence. Deleted documents are also handled.
func (h *Handler) deduplicateBM25Docs(docs []*tail.Document) []*tail.Document {
	// Map from document ID key to the latest version
	seen := make(map[string]*tail.Document)

	for _, doc := range docs {
		key := doc.ID.String()
		existing, ok := seen[key]
		if !ok {
			seen[key] = doc
			continue
		}

		// Keep the document with the higher WAL sequence number
		if doc.WalSeq > existing.WalSeq {
			seen[key] = doc
		}
	}

	// Collect non-deleted documents
	result := make([]*tail.Document, 0, len(seen))
	for _, doc := range seen {
		if !doc.Deleted {
			result = append(result, doc)
		}
	}

	return result
}

// scanTailForBM25 includes deleted docs so they can shadow indexed versions.
func (h *Handler) scanTailForBM25(ctx context.Context, ns string, f *filter.Filter, tailByteCap int64) ([]*tail.Document, error) {
	type scanIncludingDeleted interface {
		ScanIncludingDeleted(ctx context.Context, namespace string, f *filter.Filter) ([]*tail.Document, error)
	}
	type scanIncludingDeletedWithByteLimit interface {
		ScanIncludingDeletedWithByteLimit(ctx context.Context, namespace string, f *filter.Filter, byteLimitBytes int64) ([]*tail.Document, error)
	}

	if tailByteCap > 0 {
		if scanner, ok := h.tailStore.(scanIncludingDeletedWithByteLimit); ok {
			return scanner.ScanIncludingDeletedWithByteLimit(ctx, ns, f, tailByteCap)
		}
	}

	if scanner, ok := h.tailStore.(scanIncludingDeleted); ok {
		return scanner.ScanIncludingDeleted(ctx, ns, f)
	}
	return h.tailStore.Scan(ctx, ns, f)
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

// computeBM25ScoreWithPrefix computes the BM25 score with optional prefix matching for the last token.
// When lastAsPrefix is true:
// - The last query token is matched as a prefix and scores 1.0 if it matches
// - ALL non-prefix tokens must be present in the document (typeahead requires all tokens)
// - ALL matches are required for a non-zero score (consistent with ContainsAllTokens filter)
// This supports typeahead/autocomplete scenarios where users type partial words.
func computeBM25ScoreWithPrefix(idx *fts.Index, docID uint32, queryTokens []string, lastAsPrefix bool, ftsCfg *fts.Config) float64 {
	return computeBM25ScoreWithPrefixAndMatch(idx, docID, queryTokens, lastAsPrefix, ftsCfg, false)
}

func computeBM25ScoreWithPrefixAndMatch(idx *fts.Index, docID uint32, queryTokens []string, lastAsPrefix bool, ftsCfg *fts.Config, prefixMatched bool) float64 {
	if len(queryTokens) == 0 {
		return 0
	}

	// For non-prefix mode, use standard BM25
	if !lastAsPrefix {
		return computeBM25Score(idx, docID, queryTokens)
	}

	// Prefix mode: require ALL tokens to match for typeahead behavior
	k1 := idx.Config.K1
	b := idx.Config.B
	avgDL := idx.AvgDocLength
	docLen := float64(idx.GetDocLength(docID))
	n := float64(idx.TotalDocs)

	var score float64

	// Determine how many tokens to process with normal BM25
	normalTokenCount := len(queryTokens) - 1

	// Score non-prefix tokens using standard BM25, but require ALL to be present
	for i := 0; i < normalTokenCount; i++ {
		term := queryTokens[i]
		tf := float64(idx.GetTermFrequency(term, docID))
		if tf == 0 {
			// Required token not found - no match for typeahead
			return 0
		}

		df := float64(idx.GetDocumentFrequency(term))
		if df == 0 {
			return 0
		}

		// IDF: log((N - df + 0.5) / (df + 0.5) + 1)
		idf := math.Log((n-df+0.5)/(df+0.5) + 1)

		// BM25 term score: IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (docLen / avgDL)))
		denominator := tf + k1*(1-b+b*(docLen/avgDL))
		if denominator > 0 {
			score += idf * (tf * (k1 + 1)) / denominator
		}
	}

	// Handle the last token with prefix matching
	lastToken := queryTokens[len(queryTokens)-1]
	if !prefixMatched {
		prefixMatched = docHasPrefixMatch(idx, docID, lastToken)
	}
	if !prefixMatched {
		// Prefix token didn't match - no match for typeahead
		return 0
	}
	// Prefix matches score 1.0 as per spec
	score += 1.0

	return score
}

// docHasPrefixMatch checks if a document contains any term that starts with the given prefix.
func docHasPrefixMatch(idx *fts.Index, docID uint32, prefix string) bool {
	// First check for exact match (which is also a valid prefix match)
	if idx.GetTermFrequency(prefix, docID) > 0 {
		return true
	}

	// Check all terms in the index for prefix matches
	// This could be optimized with a trie or sorted term list, but for now iterate
	for term := range idx.TermPostings {
		if strings.HasPrefix(term, prefix) {
			if idx.GetTermFrequency(term, docID) > 0 {
				return true
			}
		}
	}
	return false
}

type bm25Candidate struct {
	doc         *tail.Document
	score       float64
	filterMatch bool
}

type bm25RowScore struct {
	rowID uint32
	score float64
}

type bm25RowCandidate struct {
	segIdx int
	rowID  uint32
	score  float64
}

type bm25TermStat struct {
	idf   float64
	freqs map[uint32]uint32
}

type bm25RowScoreHeap []bm25RowScore

func (h bm25RowScoreHeap) Len() int           { return len(h) }
func (h bm25RowScoreHeap) Less(i, j int) bool { return h[i].score < h[j].score }
func (h bm25RowScoreHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *bm25RowScoreHeap) Push(x any)        { *h = append(*h, x.(bm25RowScore)) }
func (h *bm25RowScoreHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type bm25MergeItem struct {
	segIdx int
	rowIdx int
	score  float64
}

type bm25MergeHeap []bm25MergeItem

func (h bm25MergeHeap) Len() int           { return len(h) }
func (h bm25MergeHeap) Less(i, j int) bool { return h[i].score > h[j].score }
func (h bm25MergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *bm25MergeHeap) Push(x any)        { *h = append(*h, x.(bm25MergeItem)) }
func (h *bm25MergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func buildBM25Stats(idx *fts.Index, queryTokens []string) []bm25TermStat {
	if idx == nil || len(queryTokens) == 0 {
		return nil
	}
	n := float64(idx.TotalDocs)
	stats := make([]bm25TermStat, 0, len(queryTokens))
	for _, term := range queryTokens {
		freqs := idx.TermFreqs[term]
		if len(freqs) == 0 {
			continue
		}
		df := float64(len(freqs))
		idf := math.Log((n-df+0.5)/(df+0.5) + 1)
		stats = append(stats, bm25TermStat{
			idf:   idf,
			freqs: freqs,
		})
	}
	return stats
}

func computeBM25ScoreWithStats(idx *fts.Index, docID uint32, stats []bm25TermStat) float64 {
	if idx == nil || len(stats) == 0 {
		return 0
	}
	k1 := idx.Config.K1
	b := idx.Config.B
	avgDL := idx.AvgDocLength
	docLen := float64(idx.DocLengths[docID])
	norm := k1 * (1 - b + b*(docLen/avgDL))
	var score float64
	for _, stat := range stats {
		tf := float64(stat.freqs[docID])
		if tf == 0 {
			continue
		}
		denominator := tf + norm
		if denominator > 0 {
			score += stat.idf * (tf * (k1 + 1)) / denominator
		}
	}
	return score
}

func bm25TopRowScores(idx *fts.Index, queryTokens []string, lastAsPrefix bool, ftsCfg *fts.Config, limit int) []bm25RowScore {
	if limit <= 0 {
		return nil
	}
	candidates := bm25CandidateBitmap(idx, queryTokens, lastAsPrefix)
	if candidates == nil || candidates.IsEmpty() {
		return nil
	}

	top := make(bm25RowScoreHeap, 0, limit)
	stats := []bm25TermStat(nil)
	if !lastAsPrefix {
		stats = buildBM25Stats(idx, queryTokens)
		if len(stats) == 0 {
			return nil
		}
	}
	iter := candidates.Iterator()
	for iter.HasNext() {
		docID := iter.Next()
		var score float64
		if lastAsPrefix {
			score = computeBM25ScoreWithPrefixAndMatch(idx, docID, queryTokens, lastAsPrefix, ftsCfg, lastAsPrefix)
		} else {
			score = computeBM25ScoreWithStats(idx, docID, stats)
		}
		if score <= 0 {
			continue
		}
		if len(top) < limit {
			heap.Push(&top, bm25RowScore{rowID: docID, score: score})
			continue
		}
		if score > top[0].score {
			top[0] = bm25RowScore{rowID: docID, score: score}
			heap.Fix(&top, 0)
		}
	}
	if len(top) == 0 {
		return nil
	}

	rows := make([]bm25RowScore, len(top))
	copy(rows, top)
	sort.Slice(rows, func(i, j int) bool { return rows[i].score > rows[j].score })
	return rows
}

func bm25CandidateBitmap(idx *fts.Index, queryTokens []string, lastAsPrefix bool) *roaring.Bitmap {
	if idx == nil || len(queryTokens) == 0 {
		return nil
	}

	if !lastAsPrefix {
		union := roaring.New()
		for _, term := range queryTokens {
			posting := idx.TermPostings[term]
			if posting == nil {
				continue
			}
			union.Or(posting)
		}
		if union.IsEmpty() {
			return nil
		}
		return union
	}

	normalTokens := queryTokens[:len(queryTokens)-1]
	var required *roaring.Bitmap
	for _, term := range normalTokens {
		posting := idx.TermPostings[term]
		if posting == nil {
			return nil
		}
		if required == nil {
			required = posting.Clone()
		} else {
			required.And(posting)
		}
	}

	prefixToken := queryTokens[len(queryTokens)-1]
	prefixMatches := roaring.New()
	for term, posting := range idx.TermPostings {
		if strings.HasPrefix(term, prefixToken) {
			prefixMatches.Or(posting)
		}
	}
	if prefixMatches.IsEmpty() {
		return nil
	}

	if required == nil {
		return prefixMatches
	}
	required.And(prefixMatches)
	if required.IsEmpty() {
		return nil
	}
	return required
}

func bm25ScoresForIndex(idx *fts.Index, queryTokens []string, lastAsPrefix bool, ftsCfg *fts.Config) map[uint32]float64 {
	candidates := bm25CandidateBitmap(idx, queryTokens, lastAsPrefix)
	if candidates == nil || candidates.IsEmpty() {
		return nil
	}

	scores := make(map[uint32]float64, int(candidates.GetCardinality()))
	stats := []bm25TermStat(nil)
	if !lastAsPrefix {
		stats = buildBM25Stats(idx, queryTokens)
		if len(stats) == 0 {
			return nil
		}
	}
	iter := candidates.Iterator()
	for iter.HasNext() {
		docID := iter.Next()
		var score float64
		if lastAsPrefix {
			score = computeBM25ScoreWithPrefixAndMatch(idx, docID, queryTokens, lastAsPrefix, ftsCfg, lastAsPrefix)
		} else {
			score = computeBM25ScoreWithStats(idx, docID, stats)
		}
		if score > 0 {
			scores[docID] = score
		}
	}
	if len(scores) == 0 {
		return nil
	}
	return scores
}

func ftsTermsMatch(terms []string, queryTokens []string, lastAsPrefix bool) bool {
	if len(terms) == 0 || len(queryTokens) == 0 {
		return false
	}
	if !lastAsPrefix {
		for _, token := range queryTokens {
			if ftsTermsContain(terms, token) {
				return true
			}
		}
		return false
	}
	if len(queryTokens) == 1 {
		return ftsTermsContainPrefix(terms, queryTokens[0])
	}
	for _, token := range queryTokens[:len(queryTokens)-1] {
		if !ftsTermsContain(terms, token) {
			return false
		}
	}
	return ftsTermsContainPrefix(terms, queryTokens[len(queryTokens)-1])
}

func ftsTermsContain(terms []string, token string) bool {
	if token == "" {
		return false
	}
	idx := sort.SearchStrings(terms, token)
	return idx < len(terms) && terms[idx] == token
}

func ftsTermsContainPrefix(terms []string, prefix string) bool {
	if prefix == "" {
		return false
	}
	idx := sort.SearchStrings(terms, prefix)
	if idx >= len(terms) {
		return false
	}
	return strings.HasPrefix(terms[idx], prefix)
}

func mergeBM25Candidates(indexed map[string]bm25Candidate, tailLatest map[string]*tail.Document, tailScores map[string]float64) []bm25Candidate {
	merged := make(map[string]bm25Candidate, len(indexed)+len(tailLatest))
	for id, cand := range indexed {
		merged[id] = cand
	}

	for id, doc := range tailLatest {
		if doc.Deleted {
			delete(merged, id)
			continue
		}
		score := tailScores[id]
		if score <= 0 {
			delete(merged, id)
			continue
		}
		merged[id] = bm25Candidate{
			doc:         doc,
			score:       score,
			filterMatch: true,
		}
	}

	scored := make([]bm25Candidate, 0, len(merged))
	for _, cand := range merged {
		if cand.doc == nil || cand.doc.Deleted || !cand.filterMatch || cand.score <= 0 {
			continue
		}
		scored = append(scored, cand)
	}
	return scored
}

func filterMatches(f *filter.Filter, doc *tail.Document) bool {
	if f == nil {
		return true
	}
	filterDoc := make(map[string]any, len(doc.Attributes)+1)
	filterDoc["id"] = docIDToAny(doc.ID)
	for k, v := range doc.Attributes {
		filterDoc[k] = v
	}
	return f.Eval(filterDoc)
}

// executeCompositeQuery executes a composite rank_by query.
// Supports Sum, Max, Product operators and filter-based boosting.
// Documents with zero score are excluded from results.
func (h *Handler) executeCompositeQuery(ctx context.Context, ns string, loaded *namespace.LoadedState, parsed *ParsedRankBy, f *filter.Filter, req *QueryRequest, tailByteCap int64) ([]Row, error) {
	if h.tailStore == nil {
		return nil, nil
	}

	if parsed.Clause == nil {
		return nil, ErrInvalidRankBy
	}

	// Scan documents from tail (filtered if needed)
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

	if len(docs) == 0 {
		return []Row{}, nil
	}

	// Build FTS configs map from schema
	ftsConfigs := h.buildFTSConfigsFromSchema(loaded)

	// Create scorer and build indexes for BM25 fields
	scorer := NewRankScorer(parsed.Clause, ftsConfigs)
	scorer.BuildIndexes(docs)

	// Score all documents
	type scoredDoc struct {
		doc   *tail.Document
		score float64
	}
	var scored []scoredDoc

	for i, doc := range docs {
		if doc.Deleted {
			continue
		}
		score := scorer.Score(doc, uint32(i))
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

// buildFTSConfigsFromSchema extracts FTS configurations for all attributes from the schema.
func (h *Handler) buildFTSConfigsFromSchema(loaded *namespace.LoadedState) map[string]*fts.Config {
	configs := make(map[string]*fts.Config)
	if loaded.State.Schema == nil {
		return configs
	}

	for name := range loaded.State.Schema.Attributes {
		cfg := h.getFTSConfig(loaded, name)
		if cfg != nil {
			configs[name] = cfg
		}
	}
	return configs
}

func indexedDocumentID(idoc index.IndexedDocument) (document.ID, bool) {
	if idoc.ID != "" {
		docID, err := document.ParseIDKey(idoc.ID)
		if err == nil {
			return docID, true
		}
		docID, err = document.ParseID(idoc.ID)
		if err == nil {
			return docID, true
		}
		docID, err = document.NewStringID(idoc.ID)
		if err == nil {
			return docID, true
		}
		return document.ID{}, false
	}
	if idoc.NumericID != 0 {
		return document.NewU64ID(idoc.NumericID), true
	}
	return document.ID{}, false
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
	if isStrongConsistency && loaded.State.NamespaceFlags.DisableBackpressure && loaded.State.WAL.BytesUnindexedEst > MaxUnindexedBytes {
		return nil, ErrStrongQueryBackpressure
	}

	// Refresh tail to WAL head once for all subqueries (snapshot isolation).
	// This ensures all subqueries execute against the same consistent snapshot.
	if err := h.refreshTailIfNeeded(ctx, ns, loaded, isStrongConsistency); err != nil {
		return nil, err
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
	if err := validateVectorDims(loaded.State, parsed); err != nil {
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

	// Check for pending index rebuilds that affect this subquery
	if err := CheckPendingRebuilds(loaded.State, f, parsed); err != nil {
		return nil, err
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
			rows, err = h.executeAttrQuery(ctx, ns, loaded, parsed, f, req, tailByteCap)
		case RankByBM25:
			rows, err = h.executeBM25Query(ctx, ns, loaded, parsed, f, req, tailByteCap)
		case RankByComposite:
			rows, err = h.executeCompositeQuery(ctx, ns, loaded, parsed, f, req, tailByteCap)
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
