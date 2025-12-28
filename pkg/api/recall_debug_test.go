package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// TestRecallDebugEndpoint_ReturnsRecallMetrics tests that the recall debug endpoint returns recall metrics.
func TestRecallDebugEndpoint_ReturnsRecallMetrics(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// First create a namespace with vectors by writing to it
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "vector": []float64{1.0, 0.0, 0.0}},
			map[string]any{"id": 2, "vector": []float64{0.0, 1.0, 0.0}},
			map[string]any{"id": 3, "vector": []float64{0.0, 0.0, 1.0}},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-recall-ns", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(w.Result().Body)
		t.Fatalf("failed to create namespace: %d, body: %s", w.Result().StatusCode, string(respBody))
	}

	// Now call the recall debug endpoint
	recallReq := map[string]any{
		"num":   2,
		"top_k": 2,
	}
	recallReqBytes, _ := json.Marshal(recallReq)
	req = httptest.NewRequest("POST", "/v1/namespaces/test-recall-ns/_debug/recall", bytes.NewReader(recallReqBytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 200, got %d, body: %s", resp.StatusCode, string(respBody))
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Verify all expected fields are present
	if _, ok := result["avg_recall"]; !ok {
		t.Errorf("expected avg_recall field")
	}
	if _, ok := result["avg_ann_count"]; !ok {
		t.Errorf("expected avg_ann_count field")
	}
	if _, ok := result["avg_exhaustive_count"]; !ok {
		t.Errorf("expected avg_exhaustive_count field")
	}

	// Without an IVF index, recall should be perfect (1.0)
	avgRecall, ok := result["avg_recall"].(float64)
	if !ok {
		t.Fatalf("avg_recall is not a float64: %T", result["avg_recall"])
	}
	if avgRecall != 1.0 {
		t.Errorf("expected avg_recall=1.0 (no index), got %v", avgRecall)
	}
}

// TestRecallDebugEndpoint_DefaultParams tests that the endpoint uses default parameters when none provided.
func TestRecallDebugEndpoint_DefaultParams(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace with vectors
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "vector": []float64{1.0, 0.0, 0.0}},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-recall-defaults", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Call recall debug with empty body (use defaults)
	req = httptest.NewRequest("POST", "/v1/namespaces/test-recall-defaults/_debug/recall", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 200, got %d, body: %s", resp.StatusCode, string(respBody))
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Verify response fields exist
	if _, ok := result["avg_recall"]; !ok {
		t.Errorf("expected avg_recall field")
	}
}

// TestRecallDebugEndpoint_NamespaceNotFound tests that the endpoint returns 404 for nonexistent namespace.
func TestRecallDebugEndpoint_NamespaceNotFound(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	req := httptest.NewRequest("POST", "/v1/namespaces/nonexistent-ns/_debug/recall", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

// TestRecallDebugEndpoint_EmptyNamespace tests recall on namespace with no vectors.
func TestRecallDebugEndpoint_EmptyNamespace(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace with no vectors (just attributes)
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "test"},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-recall-empty", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Call recall debug
	req = httptest.NewRequest("POST", "/v1/namespaces/test-recall-empty/_debug/recall", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 200, got %d, body: %s", resp.StatusCode, string(respBody))
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// With no vectors, recall should be perfect (trivially)
	avgRecall, ok := result["avg_recall"].(float64)
	if !ok {
		t.Fatalf("avg_recall is not a float64: %T", result["avg_recall"])
	}
	if avgRecall != 1.0 {
		t.Errorf("expected avg_recall=1.0 (no vectors), got %v", avgRecall)
	}

	// Counts should be 0
	avgANNCount, _ := result["avg_ann_count"].(float64)
	avgExhaustiveCount, _ := result["avg_exhaustive_count"].(float64)
	if avgANNCount != 0 || avgExhaustiveCount != 0 {
		t.Errorf("expected zero counts, got ann=%v, exhaustive=%v", avgANNCount, avgExhaustiveCount)
	}
}

// TestRecallDebugEndpoint_SamplesVectors tests that the endpoint samples the requested number of vectors.
func TestRecallDebugEndpoint_SamplesVectors(t *testing.T) {
	cfg := config.Default()
	store := objectstore.NewMemoryStore()
	router := NewRouterWithStore(cfg, nil, nil, nil, store)
	defer router.Close()

	// Create namespace with 5 vectors
	body := map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "vector": []float64{1.0, 0.0, 0.0}},
			map[string]any{"id": 2, "vector": []float64{0.0, 1.0, 0.0}},
			map[string]any{"id": 3, "vector": []float64{0.0, 0.0, 1.0}},
			map[string]any{"id": 4, "vector": []float64{0.5, 0.5, 0.0}},
			map[string]any{"id": 5, "vector": []float64{0.0, 0.5, 0.5}},
		},
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/v2/namespaces/test-recall-sample", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("failed to create namespace: %d", w.Result().StatusCode)
	}

	// Request recall with num=3, top_k=2
	recallReq := map[string]any{
		"num":   3,
		"top_k": 2,
	}
	recallReqBytes, _ := json.Marshal(recallReq)
	req = httptest.NewRequest("POST", "/v1/namespaces/test-recall-sample/_debug/recall", bytes.NewReader(recallReqBytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 200, got %d, body: %s", resp.StatusCode, string(respBody))
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Verify counts are at most top_k
	avgANNCount, _ := result["avg_ann_count"].(float64)
	avgExhaustiveCount, _ := result["avg_exhaustive_count"].(float64)

	if avgANNCount > 2 || avgExhaustiveCount > 2 {
		t.Errorf("counts should be <= top_k=2, got ann=%v, exhaustive=%v", avgANNCount, avgExhaustiveCount)
	}
}

// TestRecallDebugEndpoint_TestMode tests fallback mode when no query handler is configured.
func TestRecallDebugEndpoint_TestMode(t *testing.T) {
	cfg := config.Default()
	router := NewRouter(cfg)
	defer router.Close()

	// Set up test state with an existing namespace
	router.SetState(&ServerState{
		Namespaces: map[string]*NamespaceState{
			"test-ns": {Exists: true},
		},
		ObjectStore: ObjectStoreState{Available: true},
	})

	req := httptest.NewRequest("POST", "/v1/namespaces/test-ns/_debug/recall", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// In test mode, should return default values
	avgRecall, _ := result["avg_recall"].(float64)
	avgANNCount, _ := result["avg_ann_count"].(float64)
	avgExhaustiveCount, _ := result["avg_exhaustive_count"].(float64)

	if avgRecall != 1.0 {
		t.Errorf("expected avg_recall=1.0, got %v", avgRecall)
	}
	if avgANNCount != 0 {
		t.Errorf("expected avg_ann_count=0, got %v", avgANNCount)
	}
	if avgExhaustiveCount != 0 {
		t.Errorf("expected avg_exhaustive_count=0, got %v", avgExhaustiveCount)
	}
}
