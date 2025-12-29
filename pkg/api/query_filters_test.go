package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/config"
)

func TestQueryAPIWithFilters(t *testing.T) {
	cfg := &config.Config{AuthToken: testAuthToken}
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	tests := []struct {
		name           string
		body           map[string]any
		wantStatus     int
		wantErrContain string
	}{
		{
			name: "query with simple Eq filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"category", "Eq", "electronics"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with NotEq filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"status", "NotEq", "deleted"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with In filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"type", "In", []any{"A", "B", "C"}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with NotIn filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"type", "NotIn", []any{"X", "Y"}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with Lt filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"price", "Lt", 100},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with Gte filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"score", "Gte", 50},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with And compound filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"And", []any{
					[]any{"status", "Eq", "active"},
					[]any{"price", "Lt", 200},
				}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with Or compound filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"Or", []any{
					[]any{"category", "Eq", "sale"},
					[]any{"featured", "Eq", true},
				}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with nested compound filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"And", []any{
					[]any{"status", "Eq", "active"},
					[]any{"Or", []any{
						[]any{"category", "Eq", "premium"},
						[]any{"price", "Gte", 100},
					}},
				}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with Not filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"Not", []any{"deleted", "Eq", true}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with Eq null filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"optional_field", "Eq", nil},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with NotEq null filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"required_field", "NotEq", nil},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid filter - not an array",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": "invalid",
			},
			wantStatus:     http.StatusBadRequest,
			wantErrContain: "invalid filter",
		},
		{
			name: "invalid filter - unknown operator",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"field", "Unknown", "value"},
			},
			wantStatus:     http.StatusBadRequest,
			wantErrContain: "invalid filter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodyBytes, _ := json.Marshal(tt.body)
			req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			router.ServeAuthed(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, rec.Code)
				t.Logf("response body: %s", rec.Body.String())
			}

			if tt.wantErrContain != "" {
				if !bytes.Contains(rec.Body.Bytes(), []byte(tt.wantErrContain)) {
					t.Errorf("expected error to contain %q, got %s", tt.wantErrContain, rec.Body.String())
				}
			}
		})
	}
}

func TestVectorQueryWithFiltersAPI(t *testing.T) {
	cfg := &config.Config{AuthToken: testAuthToken}
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	tests := []struct {
		name       string
		body       map[string]any
		wantStatus int
	}{
		{
			name: "vector ANN with simple filter",
			body: map[string]any{
				"rank_by": []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
				"filters": []any{"type", "Eq", "product"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "vector ANN with compound filter",
			body: map[string]any{
				"rank_by": []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
				"filters": []any{"And", []any{
					[]any{"type", "Eq", "product"},
					[]any{"active", "Eq", true},
				}},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "vector ANN with filter and limit",
			body: map[string]any{
				"rank_by": []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
				"filters": []any{"category", "In", []any{"electronics", "books"}},
				"limit":   100,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "vector ANN with filter and include_attributes",
			body: map[string]any{
				"rank_by":            []any{"vector", "ANN", []any{1.0, 0.0, 0.0}},
				"filters":            []any{"status", "Eq", "published"},
				"include_attributes": []any{"title", "author"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodyBytes, _ := json.Marshal(tt.body)
			req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			router.ServeAuthed(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, rec.Code)
				t.Logf("response body: %s", rec.Body.String())
			}

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}
				if _, ok := resp["rows"]; !ok {
					t.Error("expected rows in response")
				}
			}
		})
	}
}

func TestQueryFiltersWithArrayOperators(t *testing.T) {
	cfg := &config.Config{AuthToken: testAuthToken}
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	tests := []struct {
		name       string
		body       map[string]any
		wantStatus int
	}{
		{
			name: "Contains filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"tags", "Contains", "featured"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "NotContains filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"tags", "NotContains", "hidden"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "ContainsAny filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"categories", "ContainsAny", []any{"sale", "new"}},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodyBytes, _ := json.Marshal(tt.body)
			req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			router.ServeAuthed(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, rec.Code)
				t.Logf("response body: %s", rec.Body.String())
			}
		})
	}
}

func TestQueryFiltersWithGlobOperators(t *testing.T) {
	cfg := &config.Config{AuthToken: testAuthToken}
	router := NewRouter(cfg)

	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	tests := []struct {
		name       string
		body       map[string]any
		wantStatus int
	}{
		{
			name: "Glob filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"name", "Glob", "test*"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "IGlob filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"name", "IGlob", "TEST*"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "NotGlob filter",
			body: map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": []any{"path", "NotGlob", "/tmp/*"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodyBytes, _ := json.Marshal(tt.body)
			req := httptest.NewRequest("POST", "/v2/namespaces/test-ns/query", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			router.ServeAuthed(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, rec.Code)
				t.Logf("response body: %s", rec.Body.String())
			}
		})
	}
}
