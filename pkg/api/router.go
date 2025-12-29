package api

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/filter"
	"github.com/vexsearch/vex/internal/logging"
	"github.com/vexsearch/vex/internal/membership"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/query"
	"github.com/vexsearch/vex/internal/routing"
	"github.com/vexsearch/vex/internal/tail"
	"github.com/vexsearch/vex/internal/wal"
	"github.com/vexsearch/vex/internal/warmer"
	"github.com/vexsearch/vex/internal/write"
	"github.com/vexsearch/vex/pkg/objectstore"
	"google.golang.org/protobuf/proto"
)

// NamespaceState represents the state of a namespace for testing/simulation purposes.
type NamespaceState struct {
	Exists          bool
	Deleted         bool
	IndexBuilding   bool
	UnindexedBytes  int64
	BackpressureOff bool // disable_backpressure=true was used
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
	writeBatcher *write.Batcher     // batches writes at 1/sec per namespace
	queryHandler *query.Handler     // handles query execution
	cacheWarmer  *warmer.Warmer     // background cache warmer
	diskCache    *cache.DiskCache   // NVMe SSD cache
	ramCache     *cache.MemoryCache // RAM cache for hot index structures
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
		// Get compat mode from config, defaulting to turbopuffer
		compatMode := string(cfg.GetCompatMode())
		writeHandler, err := write.NewHandlerWithOptions(store, r.stateManager, r.tailStore, compatMode)
		if err == nil {
			r.writeHandler = writeHandler
		}
		// Create write batcher for 1/sec batching per namespace
		writeBatcher, err := write.NewBatcherWithCompatMode(store, r.stateManager, r.tailStore, compatMode)
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
	r.mux.HandleFunc("GET /metrics", r.handleMetrics)
	r.mux.HandleFunc("POST /_test/state", r.handleSetTestState)
	r.mux.HandleFunc("GET /v1/namespaces", r.authMiddleware(r.handleListNamespaces))
	r.mux.HandleFunc("GET /v1/namespaces/{namespace}/metadata", r.authMiddleware(r.validateNamespace(r.handleGetMetadata)))
	r.mux.HandleFunc("GET /v1/namespaces/{namespace}/hint_cache_warm", r.authMiddleware(r.validateNamespace(r.handleWarmCache)))
	r.mux.HandleFunc("POST /v2/namespaces/{namespace}", r.authMiddleware(r.validateNamespace(r.writeTimeoutMiddleware(r.handleWrite))))
	r.mux.HandleFunc("POST /v2/namespaces/{namespace}/query", r.authMiddleware(r.validateNamespace(r.queryTimeoutMiddleware(r.handleQuery))))
	r.mux.HandleFunc("DELETE /v2/namespaces/{namespace}", r.authMiddleware(r.validateNamespace(r.handleDeleteNamespace)))
	r.mux.HandleFunc("POST /v1/namespaces/{namespace}/_debug/recall", r.authMiddleware(r.validateNamespace(r.queryTimeoutMiddleware(r.handleDebugRecall))))
	r.mux.HandleFunc("GET /_debug/state/{namespace}", r.adminAuthMiddleware(r.validateNamespace(r.handleDebugState)))
	r.mux.HandleFunc("GET /_debug/cache/{namespace}", r.adminAuthMiddleware(r.validateNamespace(r.handleDebugCache)))
	r.mux.HandleFunc("GET /_debug/wal/{namespace}", r.adminAuthMiddleware(r.validateNamespace(r.handleDebugWal)))

	return r
}

// Close releases resources held by the router.
func (r *Router) Close() error {
	var firstErr error
	// Close cache warmer first to stop background tasks
	if r.cacheWarmer != nil {
		if err := r.cacheWarmer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// Close batcher next to flush pending writes
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

func (r *Router) adminAuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if r.cfg.AdminToken == "" {
			r.writeError(w, http.StatusForbidden, "admin endpoints require admin token configuration")
			return
		}

		auth := req.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			r.writeError(w, http.StatusUnauthorized, "missing or invalid Authorization header")
			return
		}

		token := strings.TrimPrefix(auth, "Bearer ")
		if token != r.cfg.AdminToken {
			r.writeError(w, http.StatusForbidden, "admin access denied")
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

// timeoutMiddleware wraps a handler with a context deadline for CPU budget enforcement.
func (r *Router) timeoutMiddleware(timeoutMs int, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		timeout := time.Duration(timeoutMs) * time.Millisecond
		ctx, cancel := context.WithTimeout(req.Context(), timeout)
		defer cancel()
		req = req.WithContext(ctx)
		next(w, req)
	}
}

// queryTimeoutMiddleware applies the query timeout to a handler.
func (r *Router) queryTimeoutMiddleware(next http.HandlerFunc) http.HandlerFunc {
	timeoutMs := r.cfg.Timeout.GetQueryTimeout()
	return r.timeoutMiddleware(timeoutMs, next)
}

// writeTimeoutMiddleware applies the write timeout to a handler.
func (r *Router) writeTimeoutMiddleware(next http.HandlerFunc) http.HandlerFunc {
	timeoutMs := r.cfg.Timeout.GetWriteTimeout()
	return r.timeoutMiddleware(timeoutMs, next)
}

func (r *Router) handleHealth(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (r *Router) handleMetrics(w http.ResponseWriter, req *http.Request) {
	promhttp.Handler().ServeHTTP(w, req)
}

func (r *Router) handleListNamespaces(w http.ResponseWriter, req *http.Request) {
	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Parse query parameters
	q := req.URL.Query()
	cursor := q.Get("cursor")
	prefix := q.Get("prefix")
	pageSizeStr := q.Get("page_size")

	// Default page_size is 100, max is 1000
	pageSize := 100
	if pageSizeStr != "" {
		ps, err := parsePageSize(pageSizeStr)
		if err != nil {
			r.writeAPIError(w, ErrBadRequest(err.Error()))
			return
		}
		pageSize = ps
	}

	// List namespaces from object store
	if r.store != nil {
		namespaces, nextCursor, err := listNamespacesFromStore(req.Context(), r.store, cursor, prefix, pageSize)
		if err != nil {
			r.writeAPIError(w, ErrInternalServer(err.Error()))
			return
		}

		response := map[string]interface{}{
			"namespaces": namespaces,
		}
		if nextCursor != "" {
			response["next_cursor"] = nextCursor
		}
		r.writeJSON(w, http.StatusOK, response)
		return
	}

	// Fallback for test mode - return empty list
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"namespaces": []interface{}{},
	})
}

func (r *Router) handleGetMetadata(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Try to load state from the real state manager first
	if r.stateManager != nil {
		loaded, err := r.stateManager.Load(req.Context(), ns)
		if err != nil {
			if errors.Is(err, namespace.ErrStateNotFound) {
				r.writeAPIError(w, ErrNamespaceNotFound(ns))
				return
			}
			if errors.Is(err, namespace.ErrNamespaceTombstoned) {
				r.writeAPIError(w, ErrNamespaceDeleted(ns))
				return
			}
			r.writeAPIError(w, ErrInternalServer(err.Error()))
			return
		}

		state := loaded.State

		// Build response from real namespace state
		indexStatus := "up-to-date"
		if state.Index.Status != "" {
			indexStatus = state.Index.Status
		}
		if state.WAL.HeadSeq > state.Index.IndexedWALSeq {
			indexStatus = "updating"
		}

		indexObj := map[string]interface{}{
			"status": indexStatus,
		}
		if indexStatus == "updating" && state.WAL.BytesUnindexedEst > 0 {
			indexObj["unindexed_bytes"] = state.WAL.BytesUnindexedEst
		}

		response := map[string]interface{}{
			"namespace":            ns,
			"approx_row_count":     int64(0),
			"approx_logical_bytes": int64(0),
			"created_at":           state.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
			"updated_at":           state.UpdatedAt.Format("2006-01-02T15:04:05.000Z"),
			"encryption": map[string]interface{}{
				"sse": true,
			},
			"index": indexObj,
		}

		// Include schema if present
		if state.Schema != nil && len(state.Schema.Attributes) > 0 {
			response["schema"] = state.Schema
		}

		r.writeJSON(w, http.StatusOK, response)
		return
	}

	// Fallback to test-mode state for backward compatibility
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

	indexObj := map[string]interface{}{
		"status": indexStatus,
	}
	if unindexedBytes != nil {
		indexObj["unindexed_bytes"] = unindexedBytes
	}

	response := map[string]interface{}{
		"namespace":            ns,
		"approx_row_count":     int64(0),
		"approx_logical_bytes": int64(0),
		"created_at":           nil,
		"updated_at":           nil,
		"encryption": map[string]interface{}{
			"sse": true,
		},
		"index": indexObj,
	}
	r.writeJSON(w, http.StatusOK, response)
}

func (r *Router) handleWarmCache(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Try to proxy to home node if we're not the home node
	// The warm cache hint should go to the home node for best cache locality
	if r.proxy != nil && r.proxy.ShouldProxy(ns, req) {
		resp, err := r.proxy.ProxyRequest(req.Context(), ns, req)
		if err == nil {
			// Proxy succeeded, copy response
			defer resp.Body.Close()
			for key, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			return
		}
		// Proxy failed, fall back to local handling
	}

	// Check if namespace exists using real state manager first
	if r.stateManager != nil {
		_, err := r.stateManager.Load(req.Context(), ns)
		if err != nil {
			if errors.Is(err, namespace.ErrStateNotFound) {
				r.writeAPIError(w, ErrNamespaceNotFound(ns))
				return
			}
			if errors.Is(err, namespace.ErrNamespaceTombstoned) {
				r.writeAPIError(w, ErrNamespaceDeleted(ns))
				return
			}
			r.writeAPIError(w, ErrInternalServer(err.Error()))
			return
		}
	} else {
		// Fallback to test state
		if err := r.checkNamespaceExists(ns, true); err != nil {
			r.writeAPIError(w, err)
			return
		}
	}

	// Enqueue cache warming task (non-blocking)
	if r.cacheWarmer != nil {
		r.cacheWarmer.Enqueue(ns)
	}

	// Return 200 immediately - cache warming happens in background
	r.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (r *Router) handleWrite(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Try to proxy to home node if we're not the home node
	// This ensures writes go to the node with the warmest cache for this namespace
	if r.proxy != nil && r.proxy.ShouldProxy(ns, req) {
		resp, err := r.proxy.ProxyRequest(req.Context(), ns, req)
		if err == nil {
			// Proxy succeeded, copy response
			defer resp.Body.Close()
			for key, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			return
		}
		// Proxy failed, fall back to local handling
	}

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
		// Check for context timeout first
		if errors.Is(err, context.DeadlineExceeded) {
			r.writeAPIError(w, ErrWriteTimeout())
			return
		}
		if errors.Is(err, context.Canceled) {
			r.writeAPIError(w, ErrWriteTimeout())
			return
		}
		if errors.Is(err, namespace.ErrNamespaceTombstoned) {
			r.writeAPIError(w, ErrNamespaceDeleted(ns))
			return
		}
		if errors.Is(err, write.ErrBackpressure) {
			r.writeAPIError(w, ErrBackpressure())
			return
		}
		if errors.Is(err, write.ErrDuplicateIDColumn) {
			r.writeAPIError(w, ErrDuplicateIDs())
			return
		}
		if errors.Is(err, write.ErrFilterOpRequiresTail) {
			r.writeAPIError(w, ErrServiceUnavailable(err.Error()))
			return
		}
		if errors.Is(err, write.ErrConditionalRequiresTail) {
			r.writeAPIError(w, ErrServiceUnavailable(err.Error()))
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

	// Try to proxy to home node if we're not the home node
	// This ensures queries go to the node with the warmest cache for this namespace
	if r.proxy != nil && r.proxy.ShouldProxy(ns, req) {
		resp, err := r.proxy.ProxyRequest(req.Context(), ns, req)
		if err == nil {
			// Proxy succeeded, copy response
			defer resp.Body.Close()
			for key, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			return
		}
		// Proxy failed, fall back to local handling
	}

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
			// Check for context timeout first
			if errors.Is(err, context.DeadlineExceeded) {
				r.writeAPIError(w, ErrQueryTimeout())
				return
			}
			if errors.Is(err, context.Canceled) {
				r.writeAPIError(w, ErrQueryTimeout())
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
			if errors.Is(err, query.ErrInvalidRankBy) || errors.Is(err, query.ErrInvalidFilter) || errors.Is(err, query.ErrAttributeConflict) || errors.Is(err, query.ErrInvalidVectorEncoding) || errors.Is(err, query.ErrInvalidVectorDims) || errors.Is(err, query.ErrInvalidConsistency) {
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
			if errors.Is(err, query.ErrIndexRebuilding) {
				// Return 202 Accepted when query depends on index still being rebuilt
				r.writeJSON(w, http.StatusAccepted, map[string]interface{}{
					"status":  "accepted",
					"message": "query depends on index still being rebuilt",
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
			// Check for context timeout first
			if errors.Is(err, context.DeadlineExceeded) {
				r.writeAPIError(w, ErrQueryTimeout())
				return
			}
			if errors.Is(err, context.Canceled) {
				r.writeAPIError(w, ErrQueryTimeout())
				return
			}
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
			if errors.Is(err, query.ErrInvalidRankBy) || errors.Is(err, query.ErrInvalidFilter) || errors.Is(err, query.ErrAttributeConflict) || errors.Is(err, query.ErrInvalidVectorEncoding) || errors.Is(err, query.ErrInvalidVectorDims) || errors.Is(err, query.ErrInvalidConsistency) {
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
			if errors.Is(err, query.ErrIndexRebuilding) {
				// Return 202 Accepted when query depends on index still being rebuilt
				r.writeJSON(w, http.StatusAccepted, map[string]interface{}{
					"status":  "accepted",
					"message": "query depends on index still being rebuilt",
					"results": []interface{}{},
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

	// Use state manager to delete the namespace if available
	if r.stateManager != nil {
		err := r.stateManager.DeleteNamespace(req.Context(), ns)
		if err != nil {
			if errors.Is(err, namespace.ErrStateNotFound) {
				r.writeAPIError(w, ErrNamespaceNotFound(ns))
				return
			}
			if errors.Is(err, namespace.ErrNamespaceTombstoned) {
				r.writeAPIError(w, ErrNamespaceDeleted(ns))
				return
			}
			r.writeAPIError(w, ErrInternalServer(err.Error()))
			return
		}
		r.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	// Fallback to test-mode state
	if err := r.checkNamespaceExists(ns, true); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Mark namespace as deleted in test state
	nsState := r.getNamespaceState(ns)
	if nsState != nil {
		nsState.Deleted = true
	}

	r.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (r *Router) handleDebugRecall(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Parse request body
	var body map[string]interface{}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		if errors.Is(err, io.EOF) {
			// Empty body is OK, use defaults
			body = make(map[string]interface{})
		} else if strings.Contains(err.Error(), "http: request body too large") {
			r.writeAPIError(w, ErrPayloadTooLarge("request body exceeds 256MB limit"))
			return
		} else {
			r.writeAPIError(w, ErrInvalidJSON())
			return
		}
	}

	// Parse request parameters
	recallReq := &query.RecallRequest{
		Num:  query.DefaultRecallNum,
		TopK: query.DefaultRecallTopK,
	}
	if num, ok := body["num"].(float64); ok && num > 0 {
		recallReq.Num = int(num)
	}
	if topK, ok := body["top_k"].(float64); ok && topK > 0 {
		recallReq.TopK = int(topK)
	}

	// Use query handler if available (this will also check namespace existence)
	if r.queryHandler != nil {
		resp, err := r.queryHandler.HandleRecall(req.Context(), ns, recallReq)
		if err != nil {
			if errors.Is(err, query.ErrNamespaceNotFound) {
				r.writeAPIError(w, ErrNamespaceNotFound(ns))
				return
			}
			if errors.Is(err, namespace.ErrNamespaceTombstoned) {
				r.writeAPIError(w, ErrNamespaceDeleted(ns))
				return
			}
			r.writeAPIError(w, ErrInternalServer(err.Error()))
			return
		}

		r.writeJSON(w, http.StatusOK, map[string]interface{}{
			"avg_recall":           resp.AvgRecall,
			"avg_ann_count":        resp.AvgANNCount,
			"avg_exhaustive_count": resp.AvgExhaustiveCount,
		})
		return
	}

	// Fallback when no query handler (test mode)
	// Check if namespace exists using test state
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

func (r *Router) handleDebugState(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Try to load state from the real state manager first
	if r.stateManager != nil {
		loaded, err := r.stateManager.Load(req.Context(), ns)
		if err != nil {
			if errors.Is(err, namespace.ErrStateNotFound) {
				r.writeAPIError(w, ErrNamespaceNotFound(ns))
				return
			}
			if errors.Is(err, namespace.ErrNamespaceTombstoned) {
				r.writeAPIError(w, ErrNamespaceDeleted(ns))
				return
			}
			r.writeAPIError(w, ErrInternalServer(err.Error()))
			return
		}

		// Return the full state as JSON
		r.writeJSON(w, http.StatusOK, loaded.State)
		return
	}

	// Fallback to test-mode state for backward compatibility
	if err := r.checkNamespaceExists(ns, true); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Return a minimal state representation for test mode
	nsState := r.getNamespaceState(ns)
	response := map[string]interface{}{
		"namespace": ns,
		"exists":    nsState != nil && nsState.Exists,
		"deleted":   nsState != nil && nsState.Deleted,
		"test_mode": true,
	}
	r.writeJSON(w, http.StatusOK, response)
}

func (r *Router) handleDebugCache(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Check namespace exists
	if r.stateManager != nil {
		_, err := r.stateManager.Load(req.Context(), ns)
		if err != nil {
			if errors.Is(err, namespace.ErrStateNotFound) {
				r.writeAPIError(w, ErrNamespaceNotFound(ns))
				return
			}
			if errors.Is(err, namespace.ErrNamespaceTombstoned) {
				r.writeAPIError(w, ErrNamespaceDeleted(ns))
				return
			}
			r.writeAPIError(w, ErrInternalServer(err.Error()))
			return
		}
	} else {
		// Fallback to test-mode state
		if err := r.checkNamespaceExists(ns, true); err != nil {
			r.writeAPIError(w, err)
			return
		}
	}

	// Build cache status response
	response := map[string]interface{}{
		"namespace": ns,
	}

	// Add disk cache stats
	if r.diskCache != nil {
		diskStats := r.diskCache.Stats()
		response["disk_cache"] = map[string]interface{}{
			"used_bytes":   diskStats.UsedBytes,
			"max_bytes":    diskStats.MaxBytes,
			"entry_count":  diskStats.EntryCount,
			"pinned_count": diskStats.PinnedCount,
			"budget_pct":   diskStats.BudgetPct,
			"is_pinned":    r.diskCache.IsPinned("vex/namespaces/" + ns + "/"),
		}
	}

	// Add RAM cache stats for this namespace
	if r.ramCache != nil {
		memStats := r.ramCache.Stats()
		nsUsage := r.ramCache.GetNamespaceUsage(ns)
		response["ram_cache"] = map[string]interface{}{
			"total_used_bytes": memStats.UsedBytes,
			"total_max_bytes":  memStats.MaxBytes,
			"namespace_bytes":  nsUsage,
			"entry_count":      memStats.EntryCount,
			"shard_count":      memStats.ShardCount,
			"hits":             memStats.Hits,
			"misses":           memStats.Misses,
			"hit_ratio":        memStats.HitRatio,
		}
	}

	// Indicate test mode if no caches are configured
	if r.diskCache == nil && r.ramCache == nil {
		response["test_mode"] = true
	}

	r.writeJSON(w, http.StatusOK, response)
}

func (r *Router) handleDebugWal(w http.ResponseWriter, req *http.Request) {
	ns := req.PathValue("namespace")

	// Check object store availability
	if err := r.checkObjectStore(); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Check namespace exists
	if r.stateManager != nil {
		loaded, err := r.stateManager.Load(req.Context(), ns)
		if err != nil {
			if errors.Is(err, namespace.ErrStateNotFound) {
				r.writeAPIError(w, ErrNamespaceNotFound(ns))
				return
			}
			if errors.Is(err, namespace.ErrNamespaceTombstoned) {
				r.writeAPIError(w, ErrNamespaceDeleted(ns))
				return
			}
			r.writeAPIError(w, ErrInternalServer(err.Error()))
			return
		}

		// Parse from_seq query parameter
		fromSeq := uint64(1)
		if fromSeqStr := req.URL.Query().Get("from_seq"); fromSeqStr != "" {
			parsed, err := parseUint64(fromSeqStr)
			if err != nil {
				r.writeAPIError(w, ErrBadRequest("from_seq must be a positive integer"))
				return
			}
			if parsed < 1 {
				r.writeAPIError(w, ErrBadRequest("from_seq must be at least 1"))
				return
			}
			fromSeq = parsed
		}

		// Parse optional limit (default 100, max 1000)
		limit := 100
		if limitStr := req.URL.Query().Get("limit"); limitStr != "" {
			parsed, err := parseUint64(limitStr)
			if err != nil || parsed < 1 || parsed > 1000 {
				r.writeAPIError(w, ErrBadRequest("limit must be between 1 and 1000"))
				return
			}
			limit = int(parsed)
		}

		// Get WAL head sequence from state
		headSeq := loaded.State.WAL.HeadSeq
		if fromSeq > headSeq {
			// No entries to return
			r.writeJSON(w, http.StatusOK, map[string]interface{}{
				"namespace": ns,
				"entries":   []interface{}{},
				"head_seq":  headSeq,
			})
			return
		}

		// Read WAL entries from from_seq to min(headSeq, from_seq+limit-1)
		entries := make([]interface{}, 0, limit)
		for seq := fromSeq; seq <= headSeq && len(entries) < limit; seq++ {
			entry, err := readWalEntry(req.Context(), r.store, ns, seq)
			if err != nil {
				// Skip entries that can't be read (e.g., not found, corrupted)
				continue
			}
			entries = append(entries, entry)
		}

		response := map[string]interface{}{
			"namespace": ns,
			"entries":   entries,
			"head_seq":  headSeq,
		}
		if fromSeq+uint64(limit)-1 < headSeq && len(entries) == limit {
			response["next_seq"] = fromSeq + uint64(limit)
		}

		r.writeJSON(w, http.StatusOK, response)
		return
	}

	// Fallback to test-mode state
	if err := r.checkNamespaceExists(ns, true); err != nil {
		r.writeAPIError(w, err)
		return
	}

	// Test mode returns empty entries
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"namespace": ns,
		"entries":   []interface{}{},
		"head_seq":  uint64(0),
		"test_mode": true,
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
	// First check test state for deleted flag (for test compatibility)
	nsState := r.getNamespaceState(ns)
	if nsState != nil && nsState.Deleted {
		return ErrNamespaceDeleted(ns)
	}

	// When we have a real state manager, use it to check namespace existence
	if r.stateManager != nil {
		_, err := r.stateManager.Load(context.Background(), ns)
		if err != nil {
			if errors.Is(err, namespace.ErrStateNotFound) {
				// Namespace doesn't exist in store - check test state
				if nsState != nil && nsState.Exists {
					return nil // Test says it exists
				}
				if requireExists {
					return ErrNamespaceNotFound(ns)
				}
				return nil
			}
			if errors.Is(err, namespace.ErrNamespaceTombstoned) {
				return ErrNamespaceDeleted(ns)
			}
			// For other errors (e.g. network issues), return nil and let the handler deal with it
			return nil
		}
		return nil
	}

	// Fallback to test state when no real state manager
	if nsState == nil {
		// No state set - if requireExists is true, it's a 404
		if requireExists {
			return ErrNamespaceNotFound(ns)
		}
		return nil
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
// Checks both real namespace state (from state manager) and test state.
func (r *Router) checkStrongQueryBackpressure(ns string) *APIError {
	// First check real namespace state from state manager
	if r.stateManager != nil {
		loaded, err := r.stateManager.Load(context.Background(), ns)
		if err == nil {
			state := loaded.State
			if state.WAL.BytesUnindexedEst > MaxUnindexedBytes && state.NamespaceFlags.DisableBackpressure {
				return ErrStrongQueryBackpressure()
			}
			return nil
		}
		// If state not found or other error, fall through to test state check
	}

	// Fall back to test state for backward compatibility
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
		// Get compat mode from config, defaulting to turbopuffer
		compatMode := string(r.cfg.GetCompatMode())
		writeHandler, err := write.NewHandlerWithOptions(store, r.stateManager, r.tailStore, compatMode)
		if err != nil {
			return err
		}
		r.writeHandler = writeHandler
		// Initialize the batcher for 1/sec batching with compat mode
		writeBatcher, err := write.NewBatcherWithCompatMode(store, r.stateManager, r.tailStore, compatMode)
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

// SetCacheWarmer sets the cache warmer for the router.
func (r *Router) SetCacheWarmer(w *warmer.Warmer) {
	r.cacheWarmer = w
}

// CacheWarmer returns the cache warmer.
func (r *Router) CacheWarmer() *warmer.Warmer {
	return r.cacheWarmer
}

// SetDiskCache sets the disk cache for the router.
func (r *Router) SetDiskCache(dc *cache.DiskCache) {
	r.diskCache = dc
}

// DiskCache returns the disk cache.
func (r *Router) DiskCache() *cache.DiskCache {
	return r.diskCache
}

// SetRAMCache sets the RAM cache for the router.
func (r *Router) SetRAMCache(mc *cache.MemoryCache) {
	r.ramCache = mc
}

// RAMCache returns the RAM cache.
func (r *Router) RAMCache() *cache.MemoryCache {
	return r.ramCache
}

// GetWriteHandler returns the write handler for direct access (testing).
func (r *Router) GetWriteHandler() *write.Handler {
	return r.writeHandler
}

// GetQueryHandler returns the query handler for direct access (testing).
func (r *Router) GetQueryHandler() *query.Handler {
	return r.queryHandler
}

// parsePageSize parses and validates the page_size parameter.
// Returns default 100 if empty, validates range [1, 1000].
func parsePageSize(s string) (int, error) {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errors.New("page_size must be a positive integer")
		}
		n = n*10 + int(c-'0')
		if n > 1000 {
			return 0, errors.New("page_size cannot exceed 1000")
		}
	}
	if n < 1 {
		return 0, errors.New("page_size must be at least 1")
	}
	return n, nil
}

// listNamespacesFromStore lists namespaces from catalog objects with pagination.
// Returns namespace entries, next_cursor (if more results), and any error.
// Catalog objects are stored under: catalog/namespaces/<namespace>
// This is more efficient than scanning state.json files.
func listNamespacesFromStore(ctx context.Context, store objectstore.Store, cursor, prefix string, pageSize int) ([]map[string]interface{}, string, error) {
	// Catalog entries are stored under: catalog/namespaces/<namespace>
	// We list with prefix "catalog/namespaces/" and extract namespace names.
	listPrefix := "catalog/namespaces/"
	if prefix != "" {
		listPrefix = "catalog/namespaces/" + prefix
	}

	// Request exactly pageSize + 1 to detect if there are more results
	result, err := store.List(ctx, &objectstore.ListOptions{
		Prefix:    listPrefix,
		Marker:    cursor,
		MaxKeys:   pageSize + 1,
		Delimiter: "",
	})
	if err != nil {
		return nil, "", err
	}

	// Extract namespace names from catalog keys
	namespaces := make([]map[string]interface{}, 0, pageSize)
	var lastKey string

	for _, obj := range result.Objects {
		// Key format: catalog/namespaces/<namespace>
		parts := strings.Split(obj.Key, "/")
		if len(parts) != 3 {
			continue
		}
		nsName := parts[2] // "catalog", "namespaces", "<namespace>"

		// Apply prefix filter if specified (already filtered by list prefix, but double-check)
		if prefix != "" && !strings.HasPrefix(nsName, prefix) {
			continue
		}

		// Stop if we have enough
		if len(namespaces) >= pageSize {
			break
		}

		namespaces = append(namespaces, map[string]interface{}{
			"id": nsName,
		})
		lastKey = obj.Key
	}

	// Determine next_cursor
	var nextCursor string
	// If we got more than pageSize results, or the listing was truncated, there's more
	if len(result.Objects) > pageSize || result.IsTruncated {
		// Use the last key we actually included as the cursor for pagination
		nextCursor = lastKey
	}

	return namespaces, nextCursor, nil
}

// parseUint64 parses a string as a uint64.
func parseUint64(s string) (uint64, error) {
	var n uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid uint64")
		}
		n = n*10 + uint64(c-'0')
	}
	return n, nil
}

// readWalEntry reads and decodes a WAL entry from object storage.
// WAL entries are stored under vex/namespaces/<namespace>/wal/<zero-padded seq>.wal.zst.
func readWalEntry(ctx context.Context, store objectstore.Store, ns string, seq uint64) (map[string]interface{}, error) {
	key := fmt.Sprintf("vex/namespaces/%s/%s", ns, wal.KeyForSeq(seq))
	reader, _, err := store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// Decompress and decode WAL entry
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	decompressed, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, err
	}

	// Parse the protobuf WAL entry and convert to JSON-friendly structure
	var entry wal.WalEntry
	if err := proto.Unmarshal(decompressed, &entry); err != nil {
		return nil, err
	}

	// Convert to JSON-friendly map
	result := map[string]interface{}{
		"seq":             entry.Seq,
		"namespace":       entry.Namespace,
		"format_version":  entry.FormatVersion,
		"committed_at_ms": entry.CommittedUnixMs,
	}

	// Convert sub-batches
	subBatches := make([]interface{}, 0, len(entry.SubBatches))
	for _, batch := range entry.SubBatches {
		batchMap := map[string]interface{}{
			"request_id":     batch.RequestId,
			"received_at_ms": batch.ReceivedAtMs,
		}

		// Convert mutations
		mutations := make([]interface{}, 0, len(batch.Mutations))
		for _, m := range batch.Mutations {
			mutMap := map[string]interface{}{
				"type": m.Type.String(),
			}
			if m.Id != nil {
				mutMap["id"] = walDocIDToInterface(m.Id)
			}
			if len(m.Attributes) > 0 {
				attrs := make(map[string]interface{}, len(m.Attributes))
				for k, v := range m.Attributes {
					attrs[k] = walAttrValueToInterface(v)
				}
				mutMap["attributes"] = attrs
			}
			if m.VectorDims > 0 {
				mutMap["vector_dims"] = m.VectorDims
			}
			mutations = append(mutations, mutMap)
		}
		if len(mutations) > 0 {
			batchMap["mutations"] = mutations
		}

		// Convert stats
		if batch.Stats != nil {
			batchMap["stats"] = map[string]interface{}{
				"rows_affected": batch.Stats.RowsAffected,
				"rows_upserted": batch.Stats.RowsUpserted,
				"rows_patched":  batch.Stats.RowsPatched,
				"rows_deleted":  batch.Stats.RowsDeleted,
			}
		}

		subBatches = append(subBatches, batchMap)
	}
	if len(subBatches) > 0 {
		result["sub_batches"] = subBatches
	}

	return result, nil
}

// walDocIDToInterface converts a WAL DocumentID to an interface{} value.
func walDocIDToInterface(id *wal.DocumentID) interface{} {
	if id == nil {
		return nil
	}
	switch v := id.GetId().(type) {
	case *wal.DocumentID_U64:
		return v.U64
	case *wal.DocumentID_Uuid:
		if len(v.Uuid) == 16 {
			return fmt.Sprintf("%x-%x-%x-%x-%x", v.Uuid[0:4], v.Uuid[4:6], v.Uuid[6:8], v.Uuid[8:10], v.Uuid[10:16])
		}
		return nil
	case *wal.DocumentID_Str:
		return v.Str
	default:
		return nil
	}
}

// walAttrValueToInterface converts a WAL AttributeValue to an interface{} value.
func walAttrValueToInterface(v *wal.AttributeValue) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.GetValue().(type) {
	case *wal.AttributeValue_StringVal:
		return val.StringVal
	case *wal.AttributeValue_IntVal:
		return val.IntVal
	case *wal.AttributeValue_UintVal:
		return val.UintVal
	case *wal.AttributeValue_FloatVal:
		return val.FloatVal
	case *wal.AttributeValue_BoolVal:
		return val.BoolVal
	case *wal.AttributeValue_DatetimeVal:
		return val.DatetimeVal
	case *wal.AttributeValue_NullVal:
		return nil
	case *wal.AttributeValue_UuidVal:
		if len(val.UuidVal) == 16 {
			return fmt.Sprintf("%x-%x-%x-%x-%x", val.UuidVal[0:4], val.UuidVal[4:6], val.UuidVal[6:8], val.UuidVal[8:10], val.UuidVal[10:16])
		}
		return nil
	case *wal.AttributeValue_StringArray:
		if val.StringArray != nil {
			return val.StringArray.Values
		}
		return nil
	case *wal.AttributeValue_IntArray:
		if val.IntArray != nil {
			return val.IntArray.Values
		}
		return nil
	case *wal.AttributeValue_UintArray:
		if val.UintArray != nil {
			return val.UintArray.Values
		}
		return nil
	case *wal.AttributeValue_FloatArray:
		if val.FloatArray != nil {
			return val.FloatArray.Values
		}
		return nil
	case *wal.AttributeValue_DatetimeArray:
		if val.DatetimeArray != nil {
			return val.DatetimeArray.Values
		}
		return nil
	case *wal.AttributeValue_BoolArray:
		if val.BoolArray != nil {
			return val.BoolArray.Values
		}
		return nil
	default:
		return nil
	}
}
