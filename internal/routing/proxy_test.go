package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// findNamespaceForNode finds a namespace that hashes to the target node.
func findNamespaceForNode(router *Router, targetNodeID string) string {
	for i := 0; i < 1000; i++ {
		ns := "ns-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i/26))
		home, ok := router.HomeNode(ns)
		if ok && home.ID == targetNodeID {
			return ns
		}
	}
	return ""
}

func TestProxyToHomeNode(t *testing.T) {
	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Served-By", "home-node")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "served_by": "home"})
	}))
	defer homeServer.Close()

	router := New("other-node:8080")
	router.SetNodes([]Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	ns := findNamespaceForNode(router, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	proxy := NewProxy(router, ProxyConfig{Timeout: 5 * time.Second})

	req := httptest.NewRequest("GET", "/v1/namespaces/"+ns+"/metadata", nil)

	resp, err := proxy.ProxyRequest(context.Background(), ns, req)
	if err != nil {
		t.Fatalf("ProxyRequest failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	if resp.Header.Get("X-Served-By") != "home-node" {
		t.Errorf("Expected X-Served-By: home-node, got %s", resp.Header.Get("X-Served-By"))
	}
}

func TestProxyBoundedTimeout(t *testing.T) {
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer slowServer.Close()

	router := New("other-node:8080")
	router.SetNodes([]Node{
		{ID: "slow-node", Addr: strings.TrimPrefix(slowServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	ns := findNamespaceForNode(router, "slow-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to slow-node")
	}

	proxy := NewProxy(router, ProxyConfig{Timeout: 100 * time.Millisecond})

	req := httptest.NewRequest("GET", "/v1/namespaces/"+ns+"/metadata", nil)

	start := time.Now()
	_, err := proxy.ProxyRequest(context.Background(), ns, req)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	if elapsed > 500*time.Millisecond {
		t.Errorf("Timeout took too long: %v (expected ~100ms)", elapsed)
	}
}

func TestFallbackToLocalOnProxyFailure(t *testing.T) {
	router := New("local-node:8080")
	router.SetNodes([]Node{
		{ID: "unreachable-node", Addr: "unreachable:9999"},
		{ID: "local-node", Addr: "local-node:8080"},
	})

	ns := findNamespaceForNode(router, "unreachable-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to unreachable-node")
	}

	proxy := NewProxy(router, ProxyConfig{Timeout: 100 * time.Millisecond})

	localHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Served-By", "local")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"served_by": "local"})
	})

	handler := proxy.ProxyHandler(ns, localHandler)

	req := httptest.NewRequest("GET", "/v1/namespaces/"+ns+"/metadata", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	if rec.Header().Get("X-Served-By") != "local" {
		t.Errorf("Expected local fallback, got served_by: %s", rec.Header().Get("X-Served-By"))
	}
}

func TestWritesProxyToHomeNode(t *testing.T) {
	var receivedBody []byte
	var receivedMethod string

	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("X-Served-By", "home-node")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"rows_affected": 5,
			"rows_upserted": 5,
		})
	}))
	defer homeServer.Close()

	router := New("other-node:8080")
	router.SetNodes([]Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	ns := findNamespaceForNode(router, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	proxy := NewProxy(router, ProxyConfig{Timeout: 5 * time.Second})

	writeBody := `{"upsert_rows": [{"id": 1, "name": "test"}]}`
	req := httptest.NewRequest("POST", "/v2/namespaces/"+ns, strings.NewReader(writeBody))
	req.Header.Set("Content-Type", "application/json")

	resp, err := proxy.ProxyRequest(context.Background(), ns, req)
	if err != nil {
		t.Fatalf("ProxyRequest failed: %v", err)
	}
	defer resp.Body.Close()

	if receivedMethod != "POST" {
		t.Errorf("Expected POST method, got %s", receivedMethod)
	}

	if string(receivedBody) != writeBody {
		t.Errorf("Body not forwarded correctly. Expected %q, got %q", writeBody, string(receivedBody))
	}

	if resp.Header.Get("X-Served-By") != "home-node" {
		t.Errorf("Expected X-Served-By: home-node, got %s", resp.Header.Get("X-Served-By"))
	}
}

func TestShouldProxyWhenNotHomeNode(t *testing.T) {
	router := New("node2:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	})

	proxy := NewProxy(router, ProxyConfig{})

	namespaces := []string{
		"test1", "test2", "test3", "namespace-a", "namespace-b",
	}

	for _, ns := range namespaces {
		req := httptest.NewRequest("GET", "/test", nil)
		shouldProxy := proxy.ShouldProxy(ns, req)
		isHome := router.IsHomeNode(ns)

		if shouldProxy == isHome {
			t.Errorf("For namespace %q: ShouldProxy=%v but IsHomeNode=%v (should be opposite)",
				ns, shouldProxy, isHome)
		}
	}
}

func TestShouldNotProxyAlreadyProxiedRequests(t *testing.T) {
	router := New("node2:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	})

	proxy := NewProxy(router, ProxyConfig{})

	for _, ns := range []string{"test1", "test2", "test3"} {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set(ProxiedHeader, "true")

		if proxy.ShouldProxy(ns, req) {
			t.Errorf("ShouldProxy returned true for already-proxied request (namespace: %s)", ns)
		}
	}
}

func TestProxyPreservesHeaders(t *testing.T) {
	var receivedHeaders http.Header

	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer homeServer.Close()

	router := New("other-node:8080")
	router.SetNodes([]Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	ns := findNamespaceForNode(router, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	proxy := NewProxy(router, ProxyConfig{Timeout: 5 * time.Second})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("X-Custom-Header", "custom-value")
	req.Header.Set("Content-Type", "application/json")

	resp, err := proxy.ProxyRequest(context.Background(), ns, req)
	if err != nil {
		t.Fatalf("ProxyRequest failed: %v", err)
	}
	resp.Body.Close()

	if receivedHeaders.Get("Authorization") != "Bearer test-token" {
		t.Errorf("Authorization header not preserved: %s", receivedHeaders.Get("Authorization"))
	}

	if receivedHeaders.Get("X-Custom-Header") != "custom-value" {
		t.Errorf("X-Custom-Header not preserved: %s", receivedHeaders.Get("X-Custom-Header"))
	}

	if receivedHeaders.Get(ProxiedHeader) != "true" {
		t.Errorf("ProxiedHeader not set: %s", receivedHeaders.Get(ProxiedHeader))
	}
}

func TestProxyMiddleware(t *testing.T) {
	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Served-By", "home-node")
		json.NewEncoder(w).Encode(map[string]string{"source": "proxied"})
	}))
	defer homeServer.Close()

	router := New("other-node:8080")
	router.SetNodes([]Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	ns := findNamespaceForNode(router, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	proxy := NewProxy(router, ProxyConfig{Timeout: 5 * time.Second})

	localHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Served-By", "local")
		json.NewEncoder(w).Encode(map[string]string{"source": "local"})
	})

	extractNS := func(r *http.Request) string {
		return ns
	}

	handler := proxy.ProxyMiddleware(extractNS, localHandler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Header().Get("X-Served-By") != "home-node" {
		t.Errorf("Expected proxied response from home-node, got %s", rec.Header().Get("X-Served-By"))
	}
}

func TestProxyMiddlewareFallbackOnError(t *testing.T) {
	router := New("local-node:8080")
	router.SetNodes([]Node{
		{ID: "unreachable", Addr: "unreachable:9999"},
		{ID: "local-node", Addr: "local-node:8080"},
	})

	ns := findNamespaceForNode(router, "unreachable")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to unreachable")
	}

	proxy := NewProxy(router, ProxyConfig{Timeout: 100 * time.Millisecond})

	localHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Served-By", "local")
		json.NewEncoder(w).Encode(map[string]string{"source": "local"})
	})

	extractNS := func(r *http.Request) string {
		return ns
	}

	handler := proxy.ProxyMiddleware(extractNS, localHandler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Header().Get("X-Served-By") != "local" {
		t.Errorf("Expected local fallback, got %s", rec.Header().Get("X-Served-By"))
	}
}

func TestProxyNoHomeNode(t *testing.T) {
	router := New("node1:8080")

	proxy := NewProxy(router, ProxyConfig{})

	req := httptest.NewRequest("GET", "/test", nil)
	_, err := proxy.ProxyRequest(context.Background(), "any-namespace", req)

	if err != ErrNoHomeNode {
		t.Errorf("Expected ErrNoHomeNode, got %v", err)
	}
}

func TestProxyRequestWithBody(t *testing.T) {
	var receivedBody []byte

	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer homeServer.Close()

	router := New("other-node:8080")
	router.SetNodes([]Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	ns := findNamespaceForNode(router, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	proxy := NewProxy(router, ProxyConfig{Timeout: 5 * time.Second})

	body := `{"key": "value", "nested": {"a": 1}}`
	req := httptest.NewRequest("POST", "/test", bytes.NewReader([]byte(body)))

	resp, err := proxy.ProxyRequest(context.Background(), ns, req)
	if err != nil {
		t.Fatalf("ProxyRequest failed: %v", err)
	}
	resp.Body.Close()

	if string(receivedBody) != body {
		t.Errorf("Body mismatch. Expected %q, got %q", body, string(receivedBody))
	}
}

func TestDefaultProxyTimeout(t *testing.T) {
	proxy := NewProxy(New("node:8080"), ProxyConfig{})

	if proxy.config.Timeout != 0 {
		t.Errorf("Expected 0 config timeout (uses default), got %v", proxy.config.Timeout)
	}

	if proxy.httpClient.Timeout != DefaultProxyTimeout {
		t.Errorf("Expected client timeout %v, got %v", DefaultProxyTimeout, proxy.httpClient.Timeout)
	}
}

func TestProxyHandlerSkipsWhenHomeNode(t *testing.T) {
	localCalled := false

	router := New("node1:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
	})

	proxy := NewProxy(router, ProxyConfig{})

	localHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		localCalled = true
		w.WriteHeader(http.StatusOK)
	})

	handler := proxy.ProxyHandler("any-namespace", localHandler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if !localCalled {
		t.Error("Local handler should have been called when this is the home node")
	}
}

func TestProxyQueryPath(t *testing.T) {
	var receivedPath string
	var receivedQuery string

	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		receivedQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	}))
	defer homeServer.Close()

	router := New("other-node:8080")
	router.SetNodes([]Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	ns := findNamespaceForNode(router, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	proxy := NewProxy(router, ProxyConfig{Timeout: 5 * time.Second})

	req := httptest.NewRequest("GET", "/v1/namespaces?prefix=test&limit=100", nil)

	resp, err := proxy.ProxyRequest(context.Background(), ns, req)
	if err != nil {
		t.Fatalf("ProxyRequest failed: %v", err)
	}
	resp.Body.Close()

	if receivedPath != "/v1/namespaces" {
		t.Errorf("Path not preserved. Expected /v1/namespaces, got %s", receivedPath)
	}

	if receivedQuery != "prefix=test&limit=100" {
		t.Errorf("Query not preserved. Expected prefix=test&limit=100, got %s", receivedQuery)
	}
}

func TestProxyLoopPrevention(t *testing.T) {
	router := New("node1:8080")
	router.SetNodes([]Node{
		{ID: "node1", Addr: "node1:8080"},
		{ID: "node2", Addr: "node2:8080"},
	})

	proxy := NewProxy(router, ProxyConfig{})

	// Find a namespace that would be proxied (not home for node1)
	ns := findNamespaceForNode(router, "node2")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to node2")
	}

	// Normal request should be proxied
	req := httptest.NewRequest("GET", "/test", nil)
	if !proxy.ShouldProxy(ns, req) {
		t.Error("Normal request should be proxied to node2")
	}

	// Request with proxied header should NOT be proxied again
	req2 := httptest.NewRequest("GET", "/test", nil)
	req2.Header.Set(ProxiedHeader, "true")
	if proxy.ShouldProxy(ns, req2) {
		t.Error("Already-proxied request should not be proxied again")
	}
}

func TestProxyResponseCopied(t *testing.T) {
	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom-Response", "custom-value")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"result": "created"})
	}))
	defer homeServer.Close()

	router := New("other-node:8080")
	router.SetNodes([]Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "other-node", Addr: "other-node:8080"},
	})

	ns := findNamespaceForNode(router, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	proxy := NewProxy(router, ProxyConfig{Timeout: 5 * time.Second})

	localHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Local handler should not be called")
	})

	handler := proxy.ProxyHandler(ns, localHandler)

	req := httptest.NewRequest("POST", "/test", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", rec.Code)
	}

	if rec.Header().Get("X-Custom-Response") != "custom-value" {
		t.Errorf("Response header not copied: %s", rec.Header().Get("X-Custom-Response"))
	}

	var result map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if result["result"] != "created" {
		t.Errorf("Response body not copied correctly: %v", result)
	}
}
