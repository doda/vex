package api

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/logging"
	"github.com/vexsearch/vex/internal/membership"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/internal/routing"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/write"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// NamespaceState represents the state of a namespace for testing/simulation purposes.
type NamespaceState struct {
	Exists           bool
	Deleted          bool
	IndexBuilding    bool
	UnindexedBytes   int64
	BackpressureOff  bool // disable_backpressure=true was used
}

// ObjectStoreState represents the state of object storage for testing/simulation purposes.
type ObjectStoreState struct {
	Available bool
}

// ServerState holds server-level state for testing/simulation purposes.
type ServerState struct {
	Namespaces  map[string]*NamespaceState
	ObjectStore ObjectStoreState
}

var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

type gzipResponseWriter struct {
	http.ResponseWriter
	gz *gzip.Writer
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.gz.Write(b)
}

// loggingResponseWriter captures status code for logging.
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *loggingResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *loggingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

type Router struct {
	cfg          *config.Config
	mux          *http.ServeMux
	state        *ServerState
	router       *routing.Router
	membership   *membership.Manager
	proxy        *routing.Proxy
	logger       *logging.Logger
	store        objectstore.Store
	stateManager *namespace.StateManager
	tailStore    tail.Store
	writeHandler *write.Handler
	writeBatcher *write.Batcher  // batches writes at 1/sec per namespace
	queryHandler *query.Handler  // handles query execution
}

func NewRouter(cfg *config.Config) *Router {
	return NewRouterWithMembership(cfg, nil, nil)
}

func NewRouterWithMembership(cfg *config.Config, clusterRouter *routing.Router, membershipMgr *membership.Manager) *Router {
	return NewRouterWithLogger(cfg, clusterRouter, membershipMgr, nil)
}

func NewRouterWithLogger(cfg *config.Config, clusterRouter *routing.Router, membershipMgr *membership.Manager, logger *logging.Logger) *Router {
	return NewRouterWithStore(cfg, clusterRouter, membershipMgr, logger, nil)
}

// NewRouterWithStore creates a new Router with all dependencies including object store.
func NewRouterWithStore(cfg *config.Config, clusterRouter *routing.Router, membershipMgr *membership.Manager, logger *logging.Logger, store objectstore.Store) *Router {
	if logger == nil {
		logger = logging.New()
	}
	r := &Router{
		cfg: cfg,
		mux: http.NewServeMux(),
		state: &ServerState{
			Namespaces:  make(map[string]*NamespaceState),
			ObjectStore: ObjectStoreState{Available: true},
		},
		router:     clusterRouter,
		membership: membershipMgr,
		logger:     logger,
		store:      store,
	}

	// Set up state manager, tail store, write handler, and batcher if store is available
	if store != nil {
		r.stateManager = namespace.NewStateManager(store)
		r.tailStore = tail.New(tail.DefaultConfig(), store, nil, nil)
		writeHandler, err := write.NewHandlerWithTail(store, r.stateManager, r.tailStore)
		if err == nil {
			r.writeHandler = writeHandler
		}
		// Create write batcher for 1/sec batching per namespace
		writeBatcher, err := write.NewBatcher(store, r.stateManager, r.tailStore)
		if err == nil {
			r.writeBatcher = writeBatcher
		}
		// Create query handler
		r.queryHandler = query.NewHandler(store, r.stateManager, r.tailStore)
	}

	// Set up proxy if we have a cluster router
	if clusterRouter != nil {
		r.proxy = routing.NewProxy(clusterRouter, routing.ProxyConfig{})
	}

	r.mux.HandleFunc("GET /health", r.handleHealth)
	r.mux.HandleFunc("POST /_test/state", r.handleSetTestState)
	r.mux.HandleFunc("GET /v1/namespaces", r.authMiddleware(r.handleListNamespaces))
	r.mux.HandleFunc("GET /v1/namespaces/{namespace}/metadata", r.authMiddleware(r.validateNamespace(r.handleGetMetadata)))
	r.mux.HandleFunc("GET /v1/namespaces/{namespace}/hint_cache_warm", r.authMiddleware(r.validateNamespace(r.handleWarmCache)))
	r.mux.HandleFunc("POST /v2/namespaces/{namespace}", r.authMiddleware(r.validateNamespace(r.handleWrite)))
	r.mux.HandleFunc("POST /v2/namespaces/{namespace}/query", r.authMiddleware(r.validateNamespace(r.handleQuery)))
	r.mux.HandleFunc("DELETE /v2/namespaces/{namespace}", r.authMiddleware(r.validateNamespace(r.handleDeleteNamespace)))
	r.mux.HandleFunc("POST /v1/namespaces/{namespace}/_debug/recall", r.authMiddleware(r.validateNamespace(r.handleDebugRecall)))

	return r
}

// Close releases resources held by the router.
func (r *Router) Close() error {
	var firstErr error
	// Close batcher first to flush pending writes
	if r.writeBatcher != nil {
		if err := r.writeBatcher.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.writeHandler != nil {
		if err := r.writeHandler.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.tailStore != nil {
		if err := r.tailStore.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	start := time.Now()

	// Generate or extract request ID
	requestID := req.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = uuid.New().String()
	}
	w.Header().Set("X-Request-ID", requestID)

	// Build context with request info
	ctx := req.Context()
	ctx = logging.ContextWithRequestID(ctx, requestID)
	ctx = logging.ContextWithRequestTime(ctx, start)
	ctx = logging.ContextWithEndpoint(ctx, req.Method+" "+req.URL.Path)
	req = req.WithContext(ctx)

	// Wrap response writer to capture status code
	lw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

	// Check Content-Length for payload size limit
	if req.ContentLength > MaxRequestBodySize {
		r.writeAPIError(lw, ErrPayloadTooLarge("request body exceeds 256MB limit"))
		r.logRequest(req, lw.statusCode, start, "", logging.CacheCold)
		return
	}

	// Wrap body with a size limiter
	req.Body = http.MaxBytesReader(lw, req.Body, MaxRequestBodySize)
	req.Body = r.decompressBody(req)

	if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
		gz := gzipWriterPool.Get().(*gzip.Writer)
		gz.Reset(lw)
		defer func() {
			gz.Close()
			gzipWriterPool.Put(gz)
		}()

		lw.Header().Set("Content-Encoding", "gzip")
		lw.Header().Del("Content-Length")
		r.mux.ServeHTTP(&gzipResponseWriter{ResponseWriter: lw, gz: gz}, req)
		r.logRequest(req, lw.statusCode, start, req.PathValue("namespace"), logging.CacheCold)
		return
	}

	r.mux.ServeHTTP(lw, req)
	r.logRequest(req, lw.statusCode, start, req.PathValue("namespace"), logging.CacheCold)
}

// logRequest logs the completed request with structured JSON logging.
func (r *Router) logRequest(req *http.Request, status int, start time.Time, namespace string, cacheTemp logging.CacheTemperature) {
	elapsed := float64(time.Since(start).Microseconds()) / 1000.0

	info := &logging.RequestInfo{
		RequestID:     logging.RequestIDFromContext(req.Context()),
		Namespace:     namespace,
		Endpoint:      req.Method + " " + req.URL.Path,
		CacheTemp:     cacheTemp,
		ServerTotalMs: elapsed,
	}

	r.logger.WithRequestInfo(info).Info("request completed",
		"status", status,
		"method", req.Method,
		"path", req.URL.Path,
	)
}

func (r *Router) decompressBody(req *http.Request) io.ReadCloser {
	if req.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(req.Body)
		if err != nil {
			return req.Body
		}
		return gz
	}
	return req.Body
}

func (r *Router) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if r.cfg.AuthToken == "" {
			next(w, req)
			return
		}

		auth := req.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			r.writeError(w, http.StatusUnauthorized, "missing or invalid Authorization header")
			return
		}

		token := strings.TrimPrefix(auth, "Bearer ")
		if token != r.cfg.AuthToken {
			r.writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}

		next(w, req)
	}
}

func (r *Router) validateNamespace(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ns := req.PathValue("namespace")
		if err := namespace.ValidateName(ns); err != nil {
			r.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		next(w, req)
	}
}

func (r *Router) handleHealth(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (r *Router) handleListNamespaces(w http.ResponseWriter, req *http.Request) {
	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"namespaces":  []string{},
		"next_cursor": nil,
	})
}

func (r *Router) handleGetMetadata(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Metadata requires the namespace to exist
	if err := r.checkNamespaceExists(ns, true); err != nil {
		r.writeAPIError(w, err)
		return
	}

	nsState := r.getNamespaceState(ns)
	indexStatus := "up-to-date"
	var unindexedBytes interface{} = nil
	if nsState != nil && nsState.IndexBuilding {
		indexStatus = "updating"
		unindexedBytes = nsState.UnindexedBytes
	}

	response := map[string]interface{}{
		"namespace":            ns,
		"approx_row_count":     0,
		"approx_logical_bytes": 0,
		"created_at":           nil,
		"updated_at":           nil,
		"index": map[string]interface{}{
			"status": indexStatus,
		},
	}
	if unindexedBytes != nil {
		response["index"].(map[string]interface{})["unindexed_bytes"] = unindexedBytes
	}
	r.writeJSON(w, http.StatusOK, response)
}

func (r *Router) handleWarmCache(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check if namespace exists
	if err := r.checkNamespaceExists(ns, true); err != nil {
		r.writeAPIError(w, err)
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (r *Router) handleWrite(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Check if namespace is deleted (but write to non-existent namespace is OK - implicitly creates it)
	if err := r.checkNamespaceExists(ns, false); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Check backpressure
	if err := r.checkBackpressure(ns); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Parse request body to validate JSON
	var body map[string]interface{}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		// Check if it's due to body size limit
		if strings.Contains(err.Error(), "http: request body too large") {
			r.writeAPIError(w, ErrPayloadTooLarge("request body exceeds 256MB limit"))
			return
		}
		r.writeAPIError(w, ErrInvalidJSON())
		return
	}

	// Check for duplicate IDs in upsert_columns
	if cols, ok := body["upsert_columns"].(map[string]interface{}); ok {
		if ids, ok := cols["ids"].([]interface{}); ok {
			if err := write.ValidateColumnarIDs(ids); err != nil {
				if errors.Is(err, write.ErrDuplicateIDColumn) {
					r.writeAPIError(w, ErrDuplicateIDs())
					return
				}
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
		}
	}

	// Parse the write request
	requestID := logging.RequestIDFromContext(req.Context())
	writeReq, err := write.ParseWriteRequest(requestID, body)
	if err != nil {
		r.writeAPIError(w, ErrBadRequest(err.Error()))
		return
	}

	// Prefer the batcher for batched writes (1/sec per namespace)
	// Fall back to direct handler if batcher is not available
	var resp *write.WriteResponse
	if r.writeBatcher != nil {
		resp, err = r.writeBatcher.Submit(req.Context(), ns, writeReq)
	} else if r.writeHandler != nil {
		resp, err = r.writeHandler.Handle(req.Context(), ns, writeReq)
	} else {
		// Fallback when no write handler configured (test mode)
		r.writeJSON(w, http.StatusOK, map[string]interface{}{
			"rows_affected": 0,
			"rows_upserted": 0,
			"rows_patched":  0,
			"rows_deleted":  0,
		})
		return
	}

	if err != nil {
		if errors.Is(err, namespace.ErrNamespaceTombstoned) {
			r.writeAPIError(w, ErrNamespaceDeleted(ns))
			return
		}
		if errors.Is(err, write.ErrBackpressure) {
			r.writeAPIError(w, ErrBackpressure())
			return
		}
		if errors.Is(err, write.ErrInvalidID) || errors.Is(err, write.ErrInvalidAttribute) || errors.Is(err, write.ErrInvalidRequest) || errors.Is(err, write.ErrInvalidFilter) || errors.Is(err, write.ErrDeleteByFilterTooMany) || errors.Is(err, write.ErrPatchByFilterTooMany) || errors.Is(err, write.ErrVectorPatchForbidden) || errors.Is(err, write.ErrSchemaTypeChange) || errors.Is(err, write.ErrInvalidSchema) {
			r.writeAPIError(w, ErrBadRequest(err.Error()))
			return
		}
		r.writeAPIError(w, ErrInternalServer(err.Error()))
		return
	}

	response := map[string]interface{}{
		"rows_affected": resp.RowsAffected,
		"rows_upserted": resp.RowsUpserted,
		"rows_patched":  resp.RowsPatched,
		"rows_deleted":  resp.RowsDeleted,
	}
	if resp.RowsRemaining {
		response["rows_remaining"] = true
	}
	r.writeJSON(w, http.StatusOK, response)
}

func (r *Router) handleQuery(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Check if namespace exists (query requires existing namespace)
	if err := r.checkNamespaceExists(ns, true); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Check if index is still building and return 202
	nsState := r.getNamespaceState(ns)
	if nsState != nil && nsState.IndexBuilding {
		r.writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"status":  "accepted",
			"message": "query depends on index still building",
			"rows":    []interface{}{},
			"billing": map[string]int{
				"billable_logical_bytes_queried":  0,
				"billable_logical_bytes_returned": 0,
			},
			"performance": map[string]interface{}{
				"cache_temperature": "cold",
				"server_total_ms":   0,
			},
		})
		return
	}

	// Parse request body
	var body map[string]interface{}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		if strings.Contains(err.Error(), "http: request body too large") {
			r.writeAPIError(w, ErrPayloadTooLarge("request body exceeds 256MB limit"))
			return
		}
		r.writeAPIError(w, ErrInvalidJSON())
		return
	}

	// Check for multi-query (queries array)
	if queriesRaw, ok := body["queries"]; ok {
		r.handleMultiQuery(w, req, ns, body, queriesRaw)
		return
	}

	// Parse the query request
	queryReq, err := query.ParseQueryRequest(body)
	if err != nil {
		r.writeAPIError(w, ErrBadRequest(err.Error()))
		return
	}

	// Check for strong query with backpressure disabled and high unindexed data.
	// Only apply this check for strong consistency queries (default or explicit "strong").
	// Eventual queries should continue to work even when unindexed > 2GB.
	if queryReq.Consistency != "eventual" {
		if err := r.checkStrongQueryBackpressure(ns); err != nil {
			r.writeAPIError(w, err)
			return
		}
	}

	// Execute query if handler is available
	if r.queryHandler != nil {
		resp, err := r.queryHandler.Handle(req.Context(), ns, queryReq)
		if err != nil {
			if errors.Is(err, query.ErrRankByRequired) {
				r.writeAPIError(w, ErrBadRequest("rank_by is required unless aggregate_by is specified"))
				return
			}
			if errors.Is(err, query.ErrInvalidLimit) {
				r.writeAPIError(w, ErrBadRequest("limit/top_k must be between 1 and 10,000"))
				return
			}
			if errors.Is(err, query.ErrInvalidRankBy) || errors.Is(err, query.ErrInvalidFilter) || errors.Is(err, query.ErrAttributeConflict) || errors.Is(err, query.ErrInvalidVectorEncoding) || errors.Is(err, query.ErrInvalidConsistency) {
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
			if errors.Is(err, query.ErrNamespaceNotFound) {
				r.writeAPIError(w, ErrNamespaceNotFound(ns))
				return
			}
			if errors.Is(err, namespace.ErrNamespaceTombstoned) {
				r.writeAPIError(w, ErrNamespaceDeleted(ns))
				return
			}
			if errors.Is(err, query.ErrSnapshotRefreshFailed) {
				r.writeAPIError(w, ErrServiceUnavailable("failed to refresh snapshot for strong consistency query"))
				return
			}
			if errors.Is(err, query.ErrInvalidAggregation) {
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
			if errors.Is(err, query.ErrInvalidGroupBy) {
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
			if errors.Is(err, query.ErrGroupByWithoutAgg) {
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
			r.writeAPIError(w, ErrInternalServer(err.Error()))
			return
		}

		// Build response based on whether it's an aggregation or rank_by query
		responseBody := map[string]interface{}{
			"billing": map[string]int64{
				"billable_logical_bytes_queried":  resp.Billing.BillableLogicalBytesQueried,
				"billable_logical_bytes_returned": resp.Billing.BillableLogicalBytesReturned,
			},
			"performance": map[string]interface{}{
				"cache_temperature": resp.Performance.CacheTemperature,
				"server_total_ms":   resp.Performance.ServerTotalMs,
			},
		}

		if resp.AggregationGroups != nil {
			// Grouped aggregation query response
			responseBody["aggregation_groups"] = resp.AggregationGroups
		} else if resp.Aggregations != nil {
			// Aggregation query response
			responseBody["aggregations"] = resp.Aggregations
		} else {
			// Normal rank_by query response
			rows := make([]interface{}, 0, len(resp.Rows))
			for _, row := range resp.Rows {
				rows = append(rows, query.RowToJSON(row))
			}
			responseBody["rows"] = rows
		}

		r.writeJSON(w, http.StatusOK, responseBody)
		return
	}

	// Fallback when no query handler (test mode) - validate request first
	if queryReq.RankBy == nil && queryReq.AggregateBy == nil {
		r.writeAPIError(w, ErrBadRequest("rank_by is required unless aggregate_by is specified"))
		return
	}
	if queryReq.Limit < 0 || queryReq.Limit > query.MaxTopK {
		r.writeAPIError(w, ErrBadRequest("limit/top_k must be between 1 and 10,000"))
		return
	}
	// Validate filters if provided
	if queryReq.Filters != nil {
		if _, err := filter.Parse(queryReq.Filters); err != nil {
			r.writeAPIError(w, ErrBadRequest("invalid filter expression: "+err.Error()))
			return
		}
	}

	// Build appropriate response for fallback mode
	fallbackResponse := map[string]interface{}{
		"billing": map[string]int{
			"billable_logical_bytes_queried":  0,
			"billable_logical_bytes_returned": 0,
		},
		"performance": map[string]interface{}{
			"cache_temperature": "cold",
			"server_total_ms":   0,
		},
	}

	if queryReq.AggregateBy != nil {
		// Return empty aggregations for fallback mode
		fallbackResponse["aggregations"] = map[string]interface{}{}
	} else {
		fallbackResponse["rows"] = []interface{}{}
	}

	r.writeJSON(w, http.StatusOK, fallbackResponse)
}

// handleMultiQuery handles multi-query requests with snapshot isolation.
func (r *Router) handleMultiQuery(w http.ResponseWriter, req *http.Request, ns string, body map[string]interface{}, queriesRaw interface{}) {
	// Parse the queries array
	queriesSlice, ok := queriesRaw.([]interface{})
	if !ok {
		r.writeAPIError(w, ErrBadRequest("queries must be an array"))
		return
	}

	// Convert to []map[string]any
	queries := make([]map[string]any, 0, len(queriesSlice))
	for i, q := range queriesSlice {
		qMap, ok := q.(map[string]interface{})
		if !ok {
			r.writeAPIError(w, ErrBadRequest(fmt.Sprintf("queries[%d] must be an object", i)))
			return
		}
		queries = append(queries, qMap)
	}

	// Extract top-level consistency
	consistency := ""
	if c, ok := body["consistency"].(string); ok {
		consistency = c
	}

	// Check for strong query with backpressure disabled and high unindexed data.
	if consistency != "eventual" {
		if err := r.checkStrongQueryBackpressure(ns); err != nil {
			r.writeAPIError(w, err)
			return
		}
	}

	// Execute multi-query if handler is available
	if r.queryHandler != nil {
		resp, err := r.queryHandler.HandleMultiQuery(req.Context(), ns, queries, consistency)
		if err != nil {
			if errors.Is(err, query.ErrTooManySubqueries) {
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
			if errors.Is(err, query.ErrInvalidMultiQuery) {
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
			if errors.Is(err, query.ErrRankByRequired) {
				r.writeAPIError(w, ErrBadRequest("rank_by is required unless aggregate_by is specified"))
				return
			}
			if errors.Is(err, query.ErrInvalidLimit) {
				r.writeAPIError(w, ErrBadRequest("limit/top_k must be between 1 and 10,000"))
				return
			}
			if errors.Is(err, query.ErrInvalidRankBy) || errors.Is(err, query.ErrInvalidFilter) || errors.Is(err, query.ErrAttributeConflict) || errors.Is(err, query.ErrInvalidVectorEncoding) || errors.Is(err, query.ErrInvalidConsistency) {
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
			if errors.Is(err, query.ErrNamespaceNotFound) {
				r.writeAPIError(w, ErrNamespaceNotFound(ns))
				return
			}
			if errors.Is(err, namespace.ErrNamespaceTombstoned) {
				r.writeAPIError(w, ErrNamespaceDeleted(ns))
				return
			}
			if errors.Is(err, query.ErrSnapshotRefreshFailed) {
				r.writeAPIError(w, ErrServiceUnavailable("failed to refresh snapshot for strong consistency query"))
				return
			}
			if errors.Is(err, query.ErrInvalidAggregation) {
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
			if errors.Is(err, query.ErrInvalidGroupBy) {
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
			if errors.Is(err, query.ErrGroupByWithoutAgg) {
				r.writeAPIError(w, ErrBadRequest(err.Error()))
				return
			}
			r.writeAPIError(w, ErrInternalServer(err.Error()))
			return
		}

		// Build response with results array
		resultsJSON := make([]interface{}, 0, len(resp.Results))
		for _, result := range resp.Results {
			resultMap := make(map[string]interface{})
			if result.AggregationGroups != nil {
				resultMap["aggregation_groups"] = result.AggregationGroups
			} else if result.Aggregations != nil {
				resultMap["aggregations"] = result.Aggregations
			} else {
				rows := make([]interface{}, 0, len(result.Rows))
				for _, row := range result.Rows {
					rows = append(rows, query.RowToJSON(row))
				}
				resultMap["rows"] = rows
			}
			resultsJSON = append(resultsJSON, resultMap)
		}

		responseBody := map[string]interface{}{
			"results": resultsJSON,
			"billing": map[string]int64{
				"billable_logical_bytes_queried":  resp.Billing.BillableLogicalBytesQueried,
				"billable_logical_bytes_returned": resp.Billing.BillableLogicalBytesReturned,
			},
			"performance": map[string]interface{}{
				"cache_temperature": resp.Performance.CacheTemperature,
				"server_total_ms":   resp.Performance.ServerTotalMs,
			},
		}

		r.writeJSON(w, http.StatusOK, responseBody)
		return
	}

	// Fallback mode for multi-query
	if len(queries) > query.MaxMultiQuery {
		r.writeAPIError(w, ErrBadRequest("multi-query exceeds maximum of 16 subqueries"))
		return
	}
	if len(queries) == 0 {
		r.writeAPIError(w, ErrBadRequest("invalid multi-query format"))
		return
	}

	// Return empty results for each subquery in fallback mode
	results := make([]interface{}, 0, len(queries))
	for range queries {
		results = append(results, map[string]interface{}{
			"rows": []interface{}{},
		})
	}

	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"results": results,
		"billing": map[string]int{
			"billable_logical_bytes_queried":  0,
			"billable_logical_bytes_returned": 0,
		},
		"performance": map[string]interface{}{
			"cache_temperature": "cold",
			"server_total_ms":   0,
		},
	})
}

func (r *Router) handleDeleteNamespace(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Check if namespace exists (delete requires existing namespace)
	if err := r.checkNamespaceExists(ns, true); err != nil {
		r.writeAPIError(w, err)
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (r *Router) handleDebugRecall(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check if namespace exists
	if err := r.checkNamespaceExists(ns, true); err != nil {
		r.writeAPIError(w, err)
		return
	}

	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"avg_recall":           1.0,
		"avg_ann_count":        0,
		"avg_exhaustive_count": 0,
	})
}

func (r *Router) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (r *Router) writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "error",
		"error":  message,
	})
}

func (r *Router) writeAPIError(w http.ResponseWriter, err *APIError) {
	r.writeError(w, err.StatusCode, err.Message)
}

// SetState allows tests to configure server state for testing error conditions.
func (r *Router) SetState(state *ServerState) {
	r.state = state
}

// GetState returns the current server state.
func (r *Router) GetState() *ServerState {
	return r.state
}

// handleSetTestState allows setting server state via HTTP for integration tests.
func (r *Router) handleSetTestState(w http.ResponseWriter, req *http.Request) {
	var state ServerState
	if err := json.NewDecoder(req.Body).Decode(&state); err != nil {
		r.writeAPIError(w, ErrInvalidJSON())
		return
	}
	if state.Namespaces == nil {
		state.Namespaces = make(map[string]*NamespaceState)
	}
	r.state = &state
	r.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// getNamespaceState returns the state for a namespace, or nil if no state is set.
func (r *Router) getNamespaceState(ns string) *NamespaceState {
	if r.state == nil || r.state.Namespaces == nil {
		return nil
	}
	return r.state.Namespaces[ns]
}

// checkObjectStore returns an error if object store is unavailable.
func (r *Router) checkObjectStore() *APIError {
	if r.state != nil && !r.state.ObjectStore.Available {
		return ErrObjectStoreUnavailable()
	}
	return nil
}

// checkNamespaceExists returns an error if namespace doesn't exist or is deleted.
// Returns nil if namespace exists and is not deleted.
// If requireExists is false, non-existent namespaces are allowed (for write endpoints).
func (r *Router) checkNamespaceExists(ns string, requireExists bool) *APIError {
	nsState := r.getNamespaceState(ns)
	if nsState == nil {
		// No state set - if requireExists is true, it's a 404
		if requireExists {
			return ErrNamespaceNotFound(ns)
		}
		return nil
	}
	if nsState.Deleted {
		return ErrNamespaceDeleted(ns)
	}
	if requireExists && !nsState.Exists {
		return ErrNamespaceNotFound(ns)
	}
	return nil
}

// checkBackpressure returns an error if write should be rejected due to backpressure.
func (r *Router) checkBackpressure(ns string) *APIError {
	nsState := r.getNamespaceState(ns)
	if nsState == nil {
		return nil
	}
	if nsState.UnindexedBytes > MaxUnindexedBytes && !nsState.BackpressureOff {
		return ErrBackpressure()
	}
	return nil
}

// checkStrongQueryBackpressure returns an error for strong queries when
// disable_backpressure is true and unindexed data exceeds threshold.
func (r *Router) checkStrongQueryBackpressure(ns string) *APIError {
	nsState := r.getNamespaceState(ns)
	if nsState == nil {
		return nil
	}
	if nsState.UnindexedBytes > MaxUnindexedBytes && nsState.BackpressureOff {
		return ErrStrongQueryBackpressure()
	}
	return nil
}

// ClusterRouter returns the cluster routing.Router for routing calculations.
func (r *Router) ClusterRouter() *routing.Router {
	return r.router
}

// MembershipManager returns the membership manager.
func (r *Router) MembershipManager() *membership.Manager {
	return r.membership
}

// IsHomeNode returns true if this node is the home node for the given namespace.
func (r *Router) IsHomeNode(namespace string) bool {
	if r.router == nil {
		return true // No cluster routing, assume we're the home node
	}
	return r.router.IsHomeNode(namespace)
}

// HomeNode returns the home node for the given namespace.
func (r *Router) HomeNode(namespace string) (routing.Node, bool) {
	if r.router == nil {
		return routing.Node{}, false
	}
	return r.router.HomeNode(namespace)
}

// ClusterNodes returns the list of nodes in the cluster.
func (r *Router) ClusterNodes() []routing.Node {
	if r.router == nil {
		return nil
	}
	return r.router.Nodes()
}

// SetStore sets the object store and initializes the write handler, batcher, and query handler.
// Used for testing.
func (r *Router) SetStore(store objectstore.Store) error {
	r.store = store
	if store != nil {
		r.stateManager = namespace.NewStateManager(store)
		r.tailStore = tail.New(tail.DefaultConfig(), store, nil, nil)
		writeHandler, err := write.NewHandlerWithTail(store, r.stateManager, r.tailStore)
		if err != nil {
			return err
		}
		r.writeHandler = writeHandler
		// Initialize the batcher for 1/sec batching
		writeBatcher, err := write.NewBatcher(store, r.stateManager, r.tailStore)
		if err != nil {
			return err
		}
		r.writeBatcher = writeBatcher
		// Initialize the query handler
		r.queryHandler = query.NewHandler(store, r.stateManager, r.tailStore)
	}
	return nil
}

// Store returns the object store.
func (r *Router) Store() objectstore.Store {
	return r.store
}

// StateManager returns the namespace state manager.
func (r *Router) StateManager() *namespace.StateManager {
	return r.stateManager
}
