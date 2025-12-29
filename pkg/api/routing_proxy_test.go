package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/routing"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// findNamespaceForNode finds a namespace that hashes to the target node.
func findNamespaceForNode(router *routing.Router, targetNodeID string) string {
	for i := 0; i < 1000; i++ {
		ns := "ns-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i/26))
		home, ok := router.HomeNode(ns)
		if ok && home.ID == targetNodeID {
			return ns
		}
	}
	return ""
}

// TestWriteProxiesToHomeNode tests that write requests are proxied to the home node.
func TestWriteProxiesToHomeNode(t *testing.T) {
	var receivedBody []byte
	var receivedMethod string
	var receivedPath string
	var receivedProxiedHeader string

	// Create a mock home node server
	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedPath = r.URL.Path
		receivedProxiedHeader = r.Header.Get("X-Vex-Proxied")
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Served-By", "home-node")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"rows_affected": 5,
			"rows_upserted": 5,
			"rows_patched":  0,
			"rows_deleted":  0,
		})
	}))
	defer homeServer.Close()

	// Set up cluster with home node and this node
	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	clusterRouter := routing.New("other-node:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	// Find a namespace that routes to the home node
	ns := findNamespaceForNode(clusterRouter, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	router := NewRouterWithStore(cfg, clusterRouter, nil, nil, store)
	defer router.Close()

	// Create a write request
	writeBody := `{"upsert_rows": [{"id": 1, "name": "test"}]}`
	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, strings.NewReader(writeBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	// Verify the request was proxied to the home node
	if receivedMethod != "POST" {
		t.Errorf("Expected POST method, got %s", receivedMethod)
	}
	if receivedPath != "/v2/namespaces/"+ns {
		t.Errorf("Expected path /v2/namespaces/%s, got %s", ns, receivedPath)
	}
	if receivedProxiedHeader != "true" {
		t.Errorf("Expected X-Vex-Proxied header to be 'true', got '%s'", receivedProxiedHeader)
	}
	if string(receivedBody) != writeBody {
		t.Errorf("Body not forwarded correctly. Expected %q, got %q", writeBody, string(receivedBody))
	}

	// Verify the response from home node was forwarded
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Served-By") != "home-node" {
		t.Errorf("Expected X-Served-By: home-node, got %s", resp.Header.Get("X-Served-By"))
	}

	// Verify the response body
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["rows_upserted"].(float64) != 5 {
		t.Errorf("Expected rows_upserted 5, got %v", result["rows_upserted"])
	}
}

// TestWriteFallbackOnProxyFailure tests that writes fall back to local handling when proxy fails.
func TestWriteFallbackOnProxyFailure(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	// Create a cluster router with an unreachable home node
	clusterRouter := routing.New("local-node:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "unreachable-node", Addr: "unreachable:9999"},
		{ID: "local-node", Addr: "local-node:8080"},
	})

	// Find a namespace that routes to the unreachable node
	ns := findNamespaceForNode(clusterRouter, "unreachable-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to unreachable-node")
	}

	router := NewRouterWithStore(cfg, clusterRouter, nil, nil, store)
	defer router.Close()

	// Create a write request
	writeBody := map[string]interface{}{
		"upsert_rows": []interface{}{
			map[string]interface{}{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(writeBody)
	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	start := time.Now()
	router.ServeAuthed(w, req)
	elapsed := time.Since(start)

	// Verify the request was handled locally (with proxy timeout)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d (body: %s)", resp.StatusCode, string(body))
	}

	// Verify the response contains expected fields (from local handling)
	var result map[string]interface{}
	resp.Body.Close()
	resp = w.Result()
	json.NewDecoder(resp.Body).Decode(&result)

	// Check that rows_affected is present (local write succeeded)
	if _, ok := result["rows_affected"]; !ok {
		t.Errorf("Expected rows_affected in response, got %v", result)
	}

	// Verify the fallback happened within reasonable time (including proxy timeout)
	if elapsed > 10*time.Second {
		t.Errorf("Fallback took too long: %v", elapsed)
	}
}

// TestQueryProxiesToHomeNode tests that query requests are proxied to the home node.
func TestQueryProxiesToHomeNode(t *testing.T) {
	var receivedBody []byte
	var receivedMethod string
	var receivedPath string
	var receivedProxiedHeader string

	// Create a mock home node server
	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedPath = r.URL.Path
		receivedProxiedHeader = r.Header.Get("X-Vex-Proxied")
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Served-By", "home-node")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"rows": []interface{}{
				map[string]interface{}{"id": 1, "name": "test", "$dist": 0.5},
			},
			"billing": map[string]int64{
				"billable_logical_bytes_queried":  1000,
				"billable_logical_bytes_returned": 500,
			},
			"performance": map[string]interface{}{
				"cache_temperature": "hot",
				"server_total_ms":   1.5,
			},
		})
	}))
	defer homeServer.Close()

	// Set up cluster with home node and this node
	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	clusterRouter := routing.New("other-node:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	// Find a namespace that routes to the home node
	ns := findNamespaceForNode(clusterRouter, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	router := NewRouterWithStore(cfg, clusterRouter, nil, nil, store)
	defer router.Close()

	// Create a query request
	queryBody := `{"rank_by": ["id", "asc"], "top_k": 10}`
	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns+"/query", strings.NewReader(queryBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	// Verify the request was proxied to the home node
	if receivedMethod != "POST" {
		t.Errorf("Expected POST method, got %s", receivedMethod)
	}
	if receivedPath != "/v2/namespaces/"+ns+"/query" {
		t.Errorf("Expected path /v2/namespaces/%s/query, got %s", ns, receivedPath)
	}
	if receivedProxiedHeader != "true" {
		t.Errorf("Expected X-Vex-Proxied header to be 'true', got '%s'", receivedProxiedHeader)
	}
	if string(receivedBody) != queryBody {
		t.Errorf("Body not forwarded correctly. Expected %q, got %q", queryBody, string(receivedBody))
	}

	// Verify the response from home node was forwarded
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Served-By") != "home-node" {
		t.Errorf("Expected X-Served-By: home-node, got %s", resp.Header.Get("X-Served-By"))
	}

	// Verify the response body
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	rows := result["rows"].([]interface{})
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

// TestQueryFallbackOnProxyFailure tests that queries fall back to local handling when proxy fails.
func TestQueryFallbackOnProxyFailure(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	// Create a cluster router with an unreachable home node
	clusterRouter := routing.New("local-node:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "unreachable-node", Addr: "unreachable:9999"},
		{ID: "local-node", Addr: "local-node:8080"},
	})

	// Find a namespace that routes to the unreachable node
	ns := findNamespaceForNode(clusterRouter, "unreachable-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to unreachable-node")
	}

	router := NewRouterWithStore(cfg, clusterRouter, nil, nil, store)
	defer router.Close()

	// First create the namespace
	writeBody := map[string]interface{}{
		"upsert_rows": []interface{}{
			map[string]interface{}{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(writeBody)
	writeReq := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(bodyBytes))
	writeReq.Header.Set("Content-Type", "application/json")
	writeReq.Header.Set("X-Vex-Proxied", "true") // Skip proxy for write
	ww := httptest.NewRecorder()
	router.ServeAuthed(ww, writeReq)
	if ww.Result().StatusCode != http.StatusOK {
		body, _ := io.ReadAll(ww.Result().Body)
		t.Fatalf("Failed to create namespace: %d: %s", ww.Result().StatusCode, string(body))
	}

	// Now create a query request
	queryBody := map[string]interface{}{
		"rank_by": []interface{}{"id", "asc"},
		"top_k":   10,
	}
	queryBytes, _ := json.Marshal(queryBody)
	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns+"/query", bytes.NewReader(queryBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	start := time.Now()
	router.ServeAuthed(w, req)
	elapsed := time.Since(start)

	// Verify the request was handled locally (with proxy timeout)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d (body: %s)", resp.StatusCode, string(body))
	}

	// Verify the fallback happened within reasonable time (including proxy timeout)
	if elapsed > 10*time.Second {
		t.Errorf("Fallback took too long: %v", elapsed)
	}
}

// TestWriteNoProxyWhenHomeNode tests that writes are handled locally when this is the home node.
func TestWriteNoProxyWhenHomeNode(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	// Create a cluster router where this node is the only node
	clusterRouter := routing.New("node1:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "node1", Addr: "node1:8080"},
	})

	router := NewRouterWithStore(cfg, clusterRouter, nil, nil, store)
	defer router.Close()

	// Any namespace will route to node1 since it's the only node
	writeBody := map[string]interface{}{
		"upsert_rows": []interface{}{
			map[string]interface{}{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(writeBody)
	req := httptest.NewRequest("POST", "/v2/namespaces/local-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	// Verify the request was handled locally
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d (body: %s)", resp.StatusCode, string(body))
	}

	// Verify the response contains expected fields (from local handling)
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if _, ok := result["rows_affected"]; !ok {
		t.Errorf("Expected rows_affected in response, got %v", result)
	}
}

// TestQueryNoProxyWhenHomeNode tests that queries are handled locally when this is the home node.
func TestQueryNoProxyWhenHomeNode(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	// Create a cluster router where this node is the only node
	clusterRouter := routing.New("node1:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "node1", Addr: "node1:8080"},
	})

	router := NewRouterWithStore(cfg, clusterRouter, nil, nil, store)
	defer router.Close()

	// First create the namespace
	writeBody := map[string]interface{}{
		"upsert_rows": []interface{}{
			map[string]interface{}{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(writeBody)
	writeReq := httptest.NewRequest("POST", "/v2/namespaces/local-ns", bytes.NewReader(bodyBytes))
	writeReq.Header.Set("Content-Type", "application/json")
	ww := httptest.NewRecorder()
	router.ServeAuthed(ww, writeReq)

	if ww.Result().StatusCode != http.StatusOK {
		body, _ := io.ReadAll(ww.Result().Body)
		t.Fatalf("Failed to create namespace: %d: %s", ww.Result().StatusCode, string(body))
	}

	// Any namespace will route to node1 since it's the only node
	queryBody := map[string]interface{}{
		"rank_by": []interface{}{"id", "asc"},
		"top_k":   10,
	}
	queryBytes, _ := json.Marshal(queryBody)
	req := httptest.NewRequest("POST", "/v2/namespaces/local-ns/query", bytes.NewReader(queryBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	// Verify the request was handled locally
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d (body: %s)", resp.StatusCode, string(body))
	}

	// Verify the response contains expected fields (from local handling)
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if _, ok := result["rows"]; !ok {
		t.Errorf("Expected rows in response, got %v", result)
	}
}

// TestWriteProxiedHeaderPreventsProxy tests that already-proxied writes are not proxied again.
func TestWriteProxiedHeaderPreventsProxy(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	// Create a cluster router with two nodes
	clusterRouter := routing.New("local-node:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "other-node", Addr: "other-node:8080"},
		{ID: "local-node", Addr: "local-node:8080"},
	})

	router := NewRouterWithStore(cfg, clusterRouter, nil, nil, store)
	defer router.Close()

	// Find a namespace that routes to the other node
	ns := findNamespaceForNode(clusterRouter, "other-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to other-node")
	}

	// Create a write request with X-Vex-Proxied header
	writeBody := map[string]interface{}{
		"upsert_rows": []interface{}{
			map[string]interface{}{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(writeBody)
	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Vex-Proxied", "true") // Simulate already-proxied request
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	// Verify the request was handled locally (not proxied again)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d (body: %s)", resp.StatusCode, string(body))
	}

	// Verify the response contains expected fields (from local handling)
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if _, ok := result["rows_affected"]; !ok {
		t.Errorf("Expected rows_affected in response, got %v", result)
	}
}

// TestQueryProxiedHeaderPreventsProxy tests that already-proxied queries are not proxied again.
func TestQueryProxiedHeaderPreventsProxy(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	// Create a cluster router with two nodes
	clusterRouter := routing.New("local-node:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "other-node", Addr: "other-node:8080"},
		{ID: "local-node", Addr: "local-node:8080"},
	})

	router := NewRouterWithStore(cfg, clusterRouter, nil, nil, store)
	defer router.Close()

	// Find a namespace that routes to the other node
	ns := findNamespaceForNode(clusterRouter, "other-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to other-node")
	}

	// First create the namespace with X-Vex-Proxied header
	writeBody := map[string]interface{}{
		"upsert_rows": []interface{}{
			map[string]interface{}{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(writeBody)
	writeReq := httptest.NewRequest("POST", "/v2/namespaces/"+ns, bytes.NewReader(bodyBytes))
	writeReq.Header.Set("Content-Type", "application/json")
	writeReq.Header.Set("X-Vex-Proxied", "true")
	ww := httptest.NewRecorder()
	router.ServeAuthed(ww, writeReq)

	if ww.Result().StatusCode != http.StatusOK {
		body, _ := io.ReadAll(ww.Result().Body)
		t.Fatalf("Failed to create namespace: %d: %s", ww.Result().StatusCode, string(body))
	}

	// Create a query request with X-Vex-Proxied header
	queryBody := map[string]interface{}{
		"rank_by": []interface{}{"id", "asc"},
		"top_k":   10,
	}
	queryBytes, _ := json.Marshal(queryBody)
	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns+"/query", bytes.NewReader(queryBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Vex-Proxied", "true") // Simulate already-proxied request
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	// Verify the request was handled locally (not proxied again)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d (body: %s)", resp.StatusCode, string(body))
	}

	// Verify the response contains expected fields (from local handling)
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if _, ok := result["rows"]; !ok {
		t.Errorf("Expected rows in response, got %v", result)
	}
}

// TestWriteNoProxyWithoutClusterRouter tests writes work without cluster routing.
func TestWriteNoProxyWithoutClusterRouter(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	// Create router without cluster router
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	writeBody := map[string]interface{}{
		"upsert_rows": []interface{}{
			map[string]interface{}{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(writeBody)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	// Verify the request was handled locally
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// TestQueryNoProxyWithoutClusterRouter tests queries work without cluster routing.
func TestQueryNoProxyWithoutClusterRouter(t *testing.T) {
	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	// Create router without cluster router
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// First create the namespace
	writeBody := map[string]interface{}{
		"upsert_rows": []interface{}{
			map[string]interface{}{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(writeBody)
	writeReq := httptest.NewRequest("POST", "/v2/namespaces/test-ns", bytes.NewReader(bodyBytes))
	writeReq.Header.Set("Content-Type", "application/json")
	ww := httptest.NewRecorder()
	router.ServeAuthed(ww, writeReq)

	if ww.Result().StatusCode != http.StatusOK {
		body, _ := io.ReadAll(ww.Result().Body)
		t.Fatalf("Failed to create namespace: %d: %s", ww.Result().StatusCode, string(body))
	}

	queryBody := map[string]interface{}{
		"rank_by": []interface{}{"id", "asc"},
		"top_k":   10,
	}
	queryBytes, _ := json.Marshal(queryBody)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(queryBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	// Verify the request was handled locally
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Expected status 200, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// TestProxyPreservesErrorResponses tests that error responses from proxied requests are forwarded.
func TestProxyPreservesErrorResponses(t *testing.T) {
	// Create a mock home node server that returns an error
	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "error",
			"error":  "test error from home node",
		})
	}))
	defer homeServer.Close()

	cfg := testConfig()
	store := objectstore.NewMemoryStore()

	clusterRouter := routing.New("other-node:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	// Find a namespace that routes to the home node
	ns := findNamespaceForNode(clusterRouter, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	router := NewRouterWithStore(cfg, clusterRouter, nil, nil, store)
	defer router.Close()

	// Create a write request
	writeBody := `{"upsert_rows": [{"id": 1, "name": "test"}]}`
	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, strings.NewReader(writeBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeAuthed(w, req)

	// Verify the error response was forwarded
	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["error"].(string) != "test error from home node" {
		t.Errorf("Expected error message from home node, got %v", result["error"])
	}
}
