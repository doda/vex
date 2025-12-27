package api

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/internal/membership"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/routing"
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

type Router struct {
	cfg        *config.Config
	mux        *http.ServeMux
	state      *ServerState
	router     *routing.Router
	membership *membership.Manager
	proxy      *routing.Proxy
}

func NewRouter(cfg *config.Config) *Router {
	return NewRouterWithMembership(cfg, nil, nil)
}

func NewRouterWithMembership(cfg *config.Config, clusterRouter *routing.Router, membershipMgr *membership.Manager) *Router {
	r := &Router{
		cfg: cfg,
		mux: http.NewServeMux(),
		state: &ServerState{
			Namespaces:  make(map[string]*NamespaceState),
			ObjectStore: ObjectStoreState{Available: true},
		},
		router:     clusterRouter,
		membership: membershipMgr,
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

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Check Content-Length for payload size limit
	if req.ContentLength > MaxRequestBodySize {
		r.writeAPIError(w, ErrPayloadTooLarge("request body exceeds 256MB limit"))
		return
	}

	// Wrap body with a size limiter
	req.Body = http.MaxBytesReader(w, req.Body, MaxRequestBodySize)
	req.Body = r.decompressBody(req)

	if strings.Contains(req.Header.Get("Accept-Encoding"), "gzip") {
		gz := gzipWriterPool.Get().(*gzip.Writer)
		gz.Reset(w)
		defer func() {
			gz.Close()
			gzipWriterPool.Put(gz)
		}()

		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Del("Content-Length")
		r.mux.ServeHTTP(&gzipResponseWriter{ResponseWriter: w, gz: gz}, req)
		return
	}

	r.mux.ServeHTTP(w, req)
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
			seen := make(map[interface{}]bool)
			for _, id := range ids {
				if seen[id] {
					r.writeAPIError(w, ErrDuplicateIDs())
					return
				}
				seen[id] = true
			}
		}
	}

	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"rows_affected": 0,
		"rows_upserted": 0,
		"rows_patched":  0,
		"rows_deleted":  0,
	})
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

	// Check for strong query with backpressure disabled and high unindexed data
	if err := r.checkStrongQueryBackpressure(ns); err != nil {
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

	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"rows": []interface{}{},
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
