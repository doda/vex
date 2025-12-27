package api

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/vexsearch/vex/internal/config"
)

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
	cfg *config.Config
	mux *http.ServeMux
}

func NewRouter(cfg *config.Config) *Router {
	r := &Router{
		cfg: cfg,
		mux: http.NewServeMux(),
	}

	r.mux.HandleFunc("GET /health", r.handleHealth)
	r.mux.HandleFunc("GET /v1/namespaces", r.authMiddleware(r.handleListNamespaces))
	r.mux.HandleFunc("GET /v1/namespaces/{namespace}/metadata", r.authMiddleware(r.handleGetMetadata))
	r.mux.HandleFunc("GET /v1/namespaces/{namespace}/hint_cache_warm", r.authMiddleware(r.handleWarmCache))
	r.mux.HandleFunc("POST /v2/namespaces/{namespace}", r.authMiddleware(r.handleWrite))
	r.mux.HandleFunc("POST /v2/namespaces/{namespace}/query", r.authMiddleware(r.handleQuery))
	r.mux.HandleFunc("DELETE /v2/namespaces/{namespace}", r.authMiddleware(r.handleDeleteNamespace))
	r.mux.HandleFunc("POST /v1/namespaces/{namespace}/_debug/recall", r.authMiddleware(r.handleDebugRecall))

	return r
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
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

func (r *Router) handleHealth(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (r *Router) handleListNamespaces(w http.ResponseWriter, req *http.Request) {
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"namespaces":  []string{},
		"next_cursor": nil,
	})
}

func (r *Router) handleGetMetadata(w http.ResponseWriter, req *http.Request) {
	namespace := req.PathValue("namespace")
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"namespace":          namespace,
		"approx_row_count":   0,
		"approx_logical_bytes": 0,
		"created_at":         nil,
		"updated_at":         nil,
		"index": map[string]string{
			"status": "up-to-date",
		},
	})
}

func (r *Router) handleWarmCache(w http.ResponseWriter, req *http.Request) {
	r.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (r *Router) handleWrite(w http.ResponseWriter, req *http.Request) {
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"rows_affected":  0,
		"rows_upserted":  0,
		"rows_patched":   0,
		"rows_deleted":   0,
	})
}

func (r *Router) handleQuery(w http.ResponseWriter, req *http.Request) {
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"rows": []interface{}{},
		"billing": map[string]int{
			"billable_logical_bytes_queried": 0,
			"billable_logical_bytes_returned": 0,
		},
		"performance": map[string]interface{}{
			"cache_temperature": "cold",
			"server_total_ms": 0,
		},
	})
}

func (r *Router) handleDeleteNamespace(w http.ResponseWriter, req *http.Request) {
	r.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (r *Router) handleDebugRecall(w http.ResponseWriter, req *http.Request) {
	r.writeJSON(w, http.StatusOK, map[string]interface{}{
		"avg_recall": 1.0,
		"avg_ann_count": 0,
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
