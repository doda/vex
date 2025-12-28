package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/config"
)

func TestQueryAPI(t *testing.T) {
	cfg := &config.Config{}
	router := NewRouter(cfg)

	// Set up a namespace that exists
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	tests := []struct {
		name           string
		namespace      string
		body           map[string]any
		wantStatus     int
		wantErrContain string
	}{
		{
			name:      "valid query with rank_by",
			namespace: "test-ns",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:      "valid query with rank_by and limit",
			namespace: "test-ns",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"limit":   100,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:      "valid query with top_k alias",
			namespace: "test-ns",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"top_k":   50,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:      "missing rank_by returns 400",
			namespace: "test-ns",
			body:      map[string]any{},
			wantStatus:     http.StatusBadRequest,
			wantErrContain: "rank_by is required",
		},
		{
			name:      "aggregate_by allows no rank_by",
			namespace: "test-ns",
			body: map[string]any{
				"aggregate_by": map[string]any{"field": "category"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:      "limit too high returns 400",
			namespace: "test-ns",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"limit":   10001,
			},
			wantStatus:     http.StatusBadRequest,
			wantErrContain: "limit/top_k must be between 1 and 10,000",
		},
		{
			name:      "valid limit at max",
			namespace: "test-ns",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"limit":   10000,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:      "include_attributes",
			namespace: "test-ns",
			body: map[string]any{
				"rank_by":            []any{"id", "asc"},
				"include_attributes": []any{"name", "age"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:      "exclude_attributes",
			namespace: "test-ns",
			body: map[string]any{
				"rank_by":            []any{"id", "asc"},
				"exclude_attributes": []any{"secret"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:      "vector ANN query",
			namespace: "test-ns",
			body: map[string]any{
				"rank_by": []any{"vector", "ANN", []any{1.0, 2.0, 3.0}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:      "namespace not found",
			namespace: "nonexistent",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
			},
			wantStatus:     http.StatusNotFound,
			wantErrContain: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodyBytes, _ := json.Marshal(tt.body)
			req := httptest.NewRequest("POST", "/v2/namespaces/"+tt.namespace+"/query", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, rec.Code)
				body, _ := io.ReadAll(rec.Body)
				t.Logf("response body: %s", body)
			}

			if tt.wantErrContain != "" {
				body, _ := io.ReadAll(rec.Body)
				if !bytes.Contains(body, []byte(tt.wantErrContain)) {
					t.Errorf("expected error to contain %q, got %s", tt.wantErrContain, body)
				}
			}
		})
	}
}

func TestQueryAPIResponse(t *testing.T) {
	cfg := &config.Config{}
	router := NewRouter(cfg)

	// Set up a namespace that exists
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	body := map[string]any{
		"rank_by": []any{"id", "asc"},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify rows array is present
	if _, ok := resp["rows"]; !ok {
		t.Error("response should include 'rows' array")
	}

	// Verify billing is present
	billing, ok := resp["billing"].(map[string]any)
	if !ok {
		t.Error("response should include 'billing' object")
	} else {
		if _, ok := billing["billable_logical_bytes_queried"]; !ok {
			t.Error("billing should include billable_logical_bytes_queried")
		}
		if _, ok := billing["billable_logical_bytes_returned"]; !ok {
			t.Error("billing should include billable_logical_bytes_returned")
		}
	}

	// Verify performance is present
	perf, ok := resp["performance"].(map[string]any)
	if !ok {
		t.Error("response should include 'performance' object")
	} else {
		if _, ok := perf["cache_temperature"]; !ok {
			t.Error("performance should include cache_temperature")
		}
		if _, ok := perf["server_total_ms"]; !ok {
			t.Error("performance should include server_total_ms")
		}
	}
}

func TestQueryAPIWithAuth(t *testing.T) {
	cfg := &config.Config{
		AuthToken: "test-token",
	}
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	body := map[string]any{
		"rank_by": []any{"id", "asc"},
	}
	bodyBytes, _ := json.Marshal(body)

	t.Run("no auth returns 401", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected 401, got %d", rec.Code)
		}
	})

	t.Run("valid auth returns 200", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer test-token")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", rec.Code)
		}
	})
}

func TestQueryAPIIndexBuilding(t *testing.T) {
	cfg := &config.Config{}
	router := NewRouter(cfg)

	// Set up a namespace that is still building
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"building-ns": {Exists: true, IndexBuilding: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	body := map[string]any{
		"rank_by": []any{"id", "asc"},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/building-ns/query", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected 202 Accepted for building index, got %d", rec.Code)
	}
}

func TestQueryAPIDeletedNamespace(t *testing.T) {
	cfg := &config.Config{}
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"deleted-ns": {Exists: true, Deleted: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	body := map[string]any{
		"rank_by": []any{"id", "asc"},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/deleted-ns/query", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 for deleted namespace, got %d", rec.Code)
	}
}

func TestQueryAPIObjectStoreUnavailable(t *testing.T) {
	cfg := &config.Config{}
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: false},
	})

	body := map[string]any{
		"rank_by": []any{"id", "asc"},
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for unavailable object store, got %d", rec.Code)
	}
}
