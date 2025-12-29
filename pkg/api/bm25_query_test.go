package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/config"
)

func TestBM25QueryAPI(t *testing.T) {
	cfg := &config.Config{AuthToken: testAuthToken}
	router := NewRouter(cfg)

	// Set up a namespace that exists
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	t.Run("BM25 query syntax is accepted", func(t *testing.T) {
		body := map[string]any{
			"rank_by": []any{"content", "BM25", "fox"},
			"limit":   10,
		}
		bodyBytes, _ := json.Marshal(body)

		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		router.ServeAuthed(w, req)

		// Should return 200 (no query handler set, so fallback returns empty)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Response should have rows array (may be empty)
		if _, ok := resp["rows"]; !ok {
			t.Error("expected rows in response")
		}
	})

	t.Run("BM25 query response includes billing and performance", func(t *testing.T) {
		body := map[string]any{
			"rank_by": []any{"content", "BM25", "hello"},
			"limit":   10,
		}
		bodyBytes, _ := json.Marshal(body)

		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		router.ServeAuthed(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if _, ok := resp["billing"]; !ok {
			t.Error("expected billing object in response")
		}
		if _, ok := resp["performance"]; !ok {
			t.Error("expected performance object in response")
		}
	})

	t.Run("BM25 query with empty query string is accepted", func(t *testing.T) {
		body := map[string]any{
			"rank_by": []any{"content", "BM25", ""},
			"limit":   10,
		}
		bodyBytes, _ := json.Marshal(body)

		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		router.ServeAuthed(w, req)

		// Empty query is syntactically valid, should return 200 with empty results
		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
}
