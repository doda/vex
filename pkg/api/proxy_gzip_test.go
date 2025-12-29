package api

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vexsearch/vex/internal/routing"
)

func TestProxyGzipResponsePassthrough(t *testing.T) {
	payload := []byte(`{"status":"ok"}`)
	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			gz := gzip.NewWriter(w)
			defer gz.Close()
			if _, err := gz.Write(payload); err != nil {
				t.Fatalf("gzip write failed: %v", err)
			}
			return
		}
		if _, err := w.Write(payload); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}))
	t.Cleanup(homeServer.Close)

	clusterRouter := routing.New("proxy-node:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "proxy-node", Addr: "proxy-node:8080"},
	})

	ns := findNamespaceForNode(clusterRouter, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	proxyRouter := NewRouterWithMembership(testConfig(), clusterRouter, nil)

	req := httptest.NewRequest("GET", "/v1/namespaces/"+ns+"/hint_cache_warm", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	addAuth(req)

	rec := httptest.NewRecorder()
	proxyRouter.ServeHTTP(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	encodings := resp.Header.Values("Content-Encoding")
	if len(encodings) != 1 || encodings[0] != "gzip" {
		t.Fatalf("expected single gzip Content-Encoding, got %v", encodings)
	}

	gz, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("gzip reader failed: %v", err)
	}
	decoded, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("gzip read failed: %v", err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatalf("unexpected gzip payload: %s", string(decoded))
	}
}

func TestProxyNonGzipResponsePassthrough(t *testing.T) {
	payload := []byte(`{"status":"ok"}`)
	homeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(payload); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}))
	t.Cleanup(homeServer.Close)

	clusterRouter := routing.New("proxy-node:8080")
	clusterRouter.SetNodes([]routing.Node{
		{ID: "home-node", Addr: strings.TrimPrefix(homeServer.URL, "http://")},
		{ID: "proxy-node", Addr: "proxy-node:8080"},
	})

	ns := findNamespaceForNode(clusterRouter, "home-node")
	if ns == "" {
		t.Fatal("Could not find namespace that hashes to home-node")
	}

	proxyRouter := NewRouterWithMembership(testConfig(), clusterRouter, nil)

	req := httptest.NewRequest("GET", "/v1/namespaces/"+ns+"/hint_cache_warm", nil)
	addAuth(req)

	rec := httptest.NewRecorder()
	proxyRouter.ServeHTTP(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" {
		t.Fatalf("expected no Content-Encoding, got %q", encoding)
	}

	if !bytes.Equal(rec.Body.Bytes(), payload) {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}
