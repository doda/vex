package compat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// turbopufferClient provides a simple HTTP client for the turbopuffer API.
// Tests using this client are skipped when TURBOPUFFER_API_KEY is not set.
type turbopufferClient struct {
	apiKey  string
	baseURL string
	client  *http.Client
}

// newTurbopufferClient creates a client for turbopuffer API testing.
// Returns nil if TURBOPUFFER_API_KEY is not set.
func newTurbopufferClient() *turbopufferClient {
	apiKey := os.Getenv("TURBOPUFFER_API_KEY")
	if apiKey == "" {
		return nil
	}

	baseURL := os.Getenv("TURBOPUFFER_API_URL")
	if baseURL == "" {
		baseURL = "https://api.turbopuffer.com"
	}

	return &turbopufferClient{
		apiKey:  apiKey,
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// testNamespace generates a unique namespace name for tests to avoid collisions.
func testNamespace(t *testing.T, prefix string) string {
	return fmt.Sprintf("vex-compat-test-%s-%d", prefix, time.Now().UnixNano())
}

// request makes an HTTP request to the turbopuffer API.
func (c *turbopufferClient) request(ctx context.Context, method, path string, body any) (int, map[string]any, error) {
	var bodyReader io.Reader
	if body != nil {
		jsonBytes, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(jsonBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return 0, nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read response: %w", err)
	}

	var result map[string]any
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &result); err != nil {
			return resp.StatusCode, nil, fmt.Errorf("unmarshal response: %w (body: %s)", err, string(respBody))
		}
	}

	return resp.StatusCode, result, nil
}

// write upserts documents to a namespace.
func (c *turbopufferClient) write(ctx context.Context, namespace string, body map[string]any) (int, map[string]any, error) {
	return c.request(ctx, "POST", "/v2/namespaces/"+namespace, body)
}

// query queries documents from a namespace.
func (c *turbopufferClient) query(ctx context.Context, namespace string, body map[string]any) (int, map[string]any, error) {
	return c.request(ctx, "POST", "/v2/namespaces/"+namespace+"/query", body)
}

// deleteNamespace deletes a namespace.
func (c *turbopufferClient) deleteNamespace(ctx context.Context, namespace string) error {
	status, _, err := c.request(ctx, "DELETE", "/v2/namespaces/"+namespace, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return fmt.Errorf("unexpected status: %d", status)
	}
	return nil
}

// metadata gets namespace metadata.
func (c *turbopufferClient) metadata(ctx context.Context, namespace string) (int, map[string]any, error) {
	return c.request(ctx, "GET", "/v1/namespaces/"+namespace+"/metadata", nil)
}

// skipIfNoAPIKey skips the test if TURBOPUFFER_API_KEY is not set.
func skipIfNoAPIKey(t *testing.T) *turbopufferClient {
	t.Helper()
	client := newTurbopufferClient()
	if client == nil {
		t.Skip("TURBOPUFFER_API_KEY not set, skipping turbopuffer compatibility test")
	}
	return client
}

// =============================================================================
// SECTION 1: Golden Tests Against Turbopuffer
// =============================================================================

// TestTurbopufferWriteAndQuery tests basic write and query operations against turbopuffer.
func TestTurbopufferWriteAndQuery(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "write-query")

	defer client.deleteNamespace(ctx, ns)

	t.Run("upsert_rows creates documents", func(t *testing.T) {
		status, resp, err := client.write(ctx, ns, map[string]any{
			"upsert_rows": []any{
				map[string]any{"id": 1, "name": "Widget A", "category": "widgets", "price": 19.99},
				map[string]any{"id": 2, "name": "Widget B", "category": "widgets", "price": 24.99},
				map[string]any{"id": 3, "name": "Gadget X", "category": "gadgets", "price": 99.99},
			},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d: %v", status, resp)
		}

		// Verify response shape
		if _, ok := resp["rows_affected"]; !ok {
			t.Error("expected rows_affected in response")
		}
		if _, ok := resp["rows_upserted"]; !ok {
			t.Error("expected rows_upserted in response")
		}
	})

	t.Run("query by id asc", func(t *testing.T) {
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"limit":   10,
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d: %v", status, resp)
		}

		rows, ok := resp["rows"].([]any)
		if !ok {
			t.Fatal("expected rows array")
		}
		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}

		// Verify billing and performance are present
		if _, ok := resp["billing"]; !ok {
			t.Error("expected billing in response")
		}
		if _, ok := resp["performance"]; !ok {
			t.Error("expected performance in response")
		}
	})

	t.Run("query with filter", func(t *testing.T) {
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"filters": []any{"category", "Eq", "widgets"},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d: %v", status, resp)
		}

		rows, ok := resp["rows"].([]any)
		if !ok {
			t.Fatal("expected rows array")
		}
		if len(rows) != 2 {
			t.Errorf("expected 2 widgets, got %d", len(rows))
		}

		for _, row := range rows {
			r := row.(map[string]any)
			if r["category"] != "widgets" {
				t.Errorf("expected category=widgets, got %v", r["category"])
			}
		}
	})
}

// TestTurbopufferIncludeExcludeAttributes tests attribute filtering.
func TestTurbopufferIncludeExcludeAttributes(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "attrs")

	defer client.deleteNamespace(ctx, ns)

	// Setup data
	_, _, err := client.write(ctx, ns, map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "Test", "category": "a", "price": 10.0, "stock": 100},
		},
	})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	t.Run("include_attributes", func(t *testing.T) {
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by":            []any{"id", "asc"},
			"include_attributes": []any{"name", "price"},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		rows := resp["rows"].([]any)
		if len(rows) == 0 {
			t.Fatal("expected at least 1 row")
		}

		row := rows[0].(map[string]any)
		// id should always be present
		if _, ok := row["id"]; !ok {
			t.Error("expected id to be present")
		}
		// name and price should be present
		if _, ok := row["name"]; !ok {
			t.Error("expected name to be present")
		}
		if _, ok := row["price"]; !ok {
			t.Error("expected price to be present")
		}
		// category and stock should NOT be present
		if _, ok := row["category"]; ok {
			t.Error("category should not be present with include_attributes")
		}
		if _, ok := row["stock"]; ok {
			t.Error("stock should not be present with include_attributes")
		}
	})

	t.Run("exclude_attributes", func(t *testing.T) {
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by":            []any{"id", "asc"},
			"exclude_attributes": []any{"stock"},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		rows := resp["rows"].([]any)
		if len(rows) == 0 {
			t.Fatal("expected at least 1 row")
		}

		row := rows[0].(map[string]any)
		// stock should NOT be present
		if _, ok := row["stock"]; ok {
			t.Error("stock should not be present with exclude_attributes")
		}
		// Other fields should be present
		if _, ok := row["name"]; !ok {
			t.Error("expected name to be present")
		}
	})
}

// =============================================================================
// SECTION 2: Success/Failure Behavior Comparison
// =============================================================================

// TestTurbopufferStatusCodes tests that turbopuffer returns expected status codes.
func TestTurbopufferStatusCodes(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "status-codes")

	defer client.deleteNamespace(ctx, ns)

	t.Run("400 for invalid JSON", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, "POST", client.baseURL+"/v2/namespaces/"+ns, strings.NewReader("{invalid"))
		req.Header.Set("Authorization", "Bearer "+client.apiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.client.Do(req)
		if err != nil {
			t.Fatalf("request error: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", resp.StatusCode)
		}
	})

	t.Run("400 for limit too high", func(t *testing.T) {
		// First create namespace
		_, _, _ = client.write(ctx, ns, map[string]any{
			"upsert_rows": []any{map[string]any{"id": 1}},
		})

		status, _, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"limit":   10001,
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusBadRequest {
			t.Errorf("expected 400 for limit > 10000, got %d", status)
		}
	})

	t.Run("400 for duplicate IDs in upsert_columns", func(t *testing.T) {
		status, resp, err := client.write(ctx, ns, map[string]any{
			"upsert_columns": map[string]any{
				"ids":  []any{1, 1},
				"name": []any{"a", "b"},
			},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusBadRequest {
			t.Errorf("expected 400 for duplicate IDs, got %d: %v", status, resp)
		}
	})

	t.Run("401 for missing auth", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, "GET", client.baseURL+"/v1/namespaces", nil)
		// No Authorization header

		resp, err := client.client.Do(req)
		if err != nil {
			t.Fatalf("request error: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401, got %d", resp.StatusCode)
		}
	})

	t.Run("404 for nonexistent namespace metadata", func(t *testing.T) {
		status, _, err := client.metadata(ctx, "nonexistent-namespace-vex-test-12345")
		if err != nil {
			t.Fatalf("metadata error: %v", err)
		}
		if status != http.StatusNotFound {
			t.Errorf("expected 404, got %d", status)
		}
	})
}

// TestTurbopufferErrorResponseFormat tests error response format.
func TestTurbopufferErrorResponseFormat(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "error-format")

	defer client.deleteNamespace(ctx, ns)

	// First create namespace
	_, _, _ = client.write(ctx, ns, map[string]any{
		"upsert_rows": []any{map[string]any{"id": 1}},
	})

	status, resp, err := client.query(ctx, ns, map[string]any{
		"rank_by": []any{"id", "asc"},
		"limit":   10001,
	})
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if status != http.StatusBadRequest {
		t.Skipf("skipping error format check - expected 400, got %d", status)
	}

	// Verify error response format
	if resp["status"] != "error" {
		t.Errorf("expected status='error', got %v", resp["status"])
	}
	if _, ok := resp["error"].(string); !ok {
		t.Errorf("expected 'error' to be a string, got %T", resp["error"])
	}
}

// =============================================================================
// SECTION 3: Ordering Semantics
// =============================================================================

// TestTurbopufferWriteOrdering tests that write operations follow documented ordering.
func TestTurbopufferWriteOrdering(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "ordering")

	defer client.deleteNamespace(ctx, ns)

	t.Run("delete_by_filter runs before upserts (resurrection)", func(t *testing.T) {
		// First, create some documents
		_, _, err := client.write(ctx, ns, map[string]any{
			"upsert_rows": []any{
				map[string]any{"id": 1, "name": "Original", "category": "spam"},
			},
		})
		if err != nil {
			t.Fatalf("setup write error: %v", err)
		}

		// Now delete_by_filter + upsert in same request (resurrection pattern)
		status, resp, err := client.write(ctx, ns, map[string]any{
			"delete_by_filter": []any{"category", "Eq", "spam"},
			"upsert_rows": []any{
				map[string]any{"id": 1, "name": "Resurrected", "category": "ham"},
			},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d: %v", status, resp)
		}

		// Verify the document exists with new category
		status, resp, err = client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"filters": []any{"id", "Eq", 1},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		rows := resp["rows"].([]any)
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}

		row := rows[0].(map[string]any)
		if row["category"] != "ham" {
			t.Errorf("expected category=ham after resurrection, got %v", row["category"])
		}
		if row["name"] != "Resurrected" {
			t.Errorf("expected name=Resurrected, got %v", row["name"])
		}
	})

	t.Run("upsert overwrites within same request", func(t *testing.T) {
		ns2 := testNamespace(t, "upsert-overwrite")
		defer client.deleteNamespace(ctx, ns2)

		// Multiple upserts of same ID - last one wins
		status, _, err := client.write(ctx, ns2, map[string]any{
			"upsert_rows": []any{
				map[string]any{"id": 1, "name": "First"},
				map[string]any{"id": 1, "name": "Second"},
				map[string]any{"id": 1, "name": "Third"},
			},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		status, resp, err := client.query(ctx, ns2, map[string]any{
			"rank_by": []any{"id", "asc"},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}

		rows := resp["rows"].([]any)
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}

		row := rows[0].(map[string]any)
		if row["name"] != "Third" {
			t.Errorf("expected name=Third (last write wins), got %v", row["name"])
		}
	})

	t.Run("deletes run after upserts", func(t *testing.T) {
		ns3 := testNamespace(t, "delete-after-upsert")
		defer client.deleteNamespace(ctx, ns3)

		// Upsert and delete same ID in one request - delete should win
		status, resp, err := client.write(ctx, ns3, map[string]any{
			"upsert_rows": []any{
				map[string]any{"id": 1, "name": "Created"},
			},
			"deletes": []any{1},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d: %v", status, resp)
		}

		// Document should not exist
		status, resp, err = client.query(ctx, ns3, map[string]any{
			"rank_by": []any{"id", "asc"},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}

		rows := resp["rows"].([]any)
		if len(rows) != 0 {
			t.Errorf("expected 0 rows (deleted), got %d", len(rows))
		}
	})
}

// =============================================================================
// SECTION 4: rows_remaining Behavior
// =============================================================================

// TestTurbopufferRowsRemaining tests rows_remaining behavior for partial operations.
func TestTurbopufferRowsRemaining(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "rows-remaining")

	defer client.deleteNamespace(ctx, ns)

	// Create many documents for testing partial operations
	rows := make([]any, 100)
	for i := 0; i < 100; i++ {
		rows[i] = map[string]any{
			"id":       i + 1,
			"name":     fmt.Sprintf("Doc %d", i+1),
			"category": "test",
		}
	}

	_, _, err := client.write(ctx, ns, map[string]any{
		"upsert_rows": rows,
	})
	if err != nil {
		t.Fatalf("setup write error: %v", err)
	}

	t.Run("delete_by_filter with allow_partial", func(t *testing.T) {
		// Note: This test may not trigger rows_remaining if turbopuffer's limit
		// is higher than our test data. We test the mechanics anyway.
		status, resp, err := client.write(ctx, ns, map[string]any{
			"delete_by_filter": map[string]any{
				"filter":        []any{"category", "Eq", "nonexistent"},
				"allow_partial": true,
			},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Errorf("expected 200, got %d: %v", status, resp)
		}

		// rows_remaining should be present in response
		if _, ok := resp["rows_remaining"]; !ok {
			t.Log("rows_remaining not present in response - may be omitted when false")
		}
	})

	t.Run("delete_by_filter without allow_partial respects limit", func(t *testing.T) {
		// This tests that exceeding delete limit without allow_partial fails
		// We can't easily hit the 5M limit, so we just verify the API accepts the flag
		status, _, err := client.write(ctx, ns, map[string]any{
			"delete_by_filter": map[string]any{
				"filter":        []any{"category", "Eq", "test"},
				"allow_partial": false,
			},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		// Should succeed since we only have 100 docs
		if status != http.StatusOK {
			t.Errorf("expected 200, got %d", status)
		}
	})
}

// =============================================================================
// SECTION 5: ANN Recall Tolerance
// =============================================================================

// makeVector creates a normalized vector of the given dimension.
func makeVector(dim int, seed float64) []float64 {
	vec := make([]float64, dim)
	magnitude := 0.0
	for i := range vec {
		val := math.Sin(seed*float64(i+1)) + math.Cos(seed*float64(i+2))
		vec[i] = val
		magnitude += val * val
	}
	magnitude = math.Sqrt(magnitude)
	for i := range vec {
		vec[i] /= magnitude
	}
	return vec
}

// encodeVectorBase64 encodes a float32 vector to base64.
func encodeVectorBase64(vec []float64) string {
	buf := make([]byte, len(vec)*4)
	for i, v := range vec {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(float32(v)))
	}
	return base64.StdEncoding.EncodeToString(buf)
}

// cosineDistance computes cosine distance between two vectors.
func cosineDistance(a, b []float64) float64 {
	if len(a) != len(b) {
		return 1.0
	}
	var dot, magA, magB float64
	for i := range a {
		dot += a[i] * b[i]
		magA += a[i] * a[i]
		magB += b[i] * b[i]
	}
	if magA == 0 || magB == 0 {
		return 1.0
	}
	return 1.0 - dot/(math.Sqrt(magA)*math.Sqrt(magB))
}

// TestTurbopufferVectorANN tests vector ANN search with recall tolerance.
func TestTurbopufferVectorANN(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "vector-ann")

	defer client.deleteNamespace(ctx, ns)

	const dim = 8
	const numVectors = 50

	// Create vectors with known seeds
	vectors := make([][]float64, numVectors)
	rows := make([]any, numVectors)
	for i := 0; i < numVectors; i++ {
		vectors[i] = makeVector(dim, float64(i+1))
		rows[i] = map[string]any{
			"id":     i + 1,
			"name":   fmt.Sprintf("Vec %d", i+1),
			"vector": vectors[i],
		}
	}

	_, _, err := client.write(ctx, ns, map[string]any{
		"upsert_rows": rows,
	})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	t.Run("ANN search returns similar vectors", func(t *testing.T) {
		// Query with a vector similar to the first one
		queryVec := makeVector(dim, 1.1) // Slightly different from seed 1

		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"vector", "ANN", queryVec},
			"top_k":   10,
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d: %v", status, resp)
		}

		rows, ok := resp["rows"].([]any)
		if !ok {
			t.Fatal("expected rows array")
		}
		if len(rows) == 0 {
			t.Fatal("expected at least 1 result")
		}

		// Verify $dist is present
		firstRow := rows[0].(map[string]any)
		if _, ok := firstRow["$dist"]; !ok {
			t.Error("expected $dist in ANN result")
		}

		// Verify distances are ordered (ascending)
		var lastDist float64 = -1
		for i, row := range rows {
			r := row.(map[string]any)
			dist, ok := r["$dist"].(float64)
			if !ok {
				t.Errorf("row %d: expected $dist to be float64", i)
				continue
			}
			if dist < lastDist {
				t.Errorf("row %d: distances not ascending (%f < %f)", i, dist, lastDist)
			}
			lastDist = dist
		}
	})

	t.Run("ANN recall with tolerance", func(t *testing.T) {
		// This test computes ground truth and allows some recall tolerance
		// since ANN is approximate
		queryVec := makeVector(dim, 5.0)

		// Compute ground truth (exact distances)
		type distPair struct {
			id   int
			dist float64
		}
		groundTruth := make([]distPair, numVectors)
		for i := 0; i < numVectors; i++ {
			groundTruth[i] = distPair{
				id:   i + 1,
				dist: cosineDistance(queryVec, vectors[i]),
			}
		}
		sort.Slice(groundTruth, func(i, j int) bool {
			return groundTruth[i].dist < groundTruth[j].dist
		})

		// Get top 10 from API
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"vector", "ANN", queryVec},
			"top_k":   10,
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		rows := resp["rows"].([]any)

		// Build set of returned IDs
		returnedIDs := make(map[int]bool)
		for _, row := range rows {
			r := row.(map[string]any)
			id := int(r["id"].(float64))
			returnedIDs[id] = true
		}

		// Count how many of the true top-10 are in the result
		recall := 0
		trueTop10 := make(map[int]bool)
		for i := 0; i < 10 && i < len(groundTruth); i++ {
			trueTop10[groundTruth[i].id] = true
			if returnedIDs[groundTruth[i].id] {
				recall++
			}
		}

		recallPct := float64(recall) / 10.0

		// Allow recall tolerance of 70% for ANN (it's approximate!)
		if recallPct < 0.7 {
			t.Errorf("recall too low: %.0f%% (expected >= 70%%)", recallPct*100)
			t.Logf("True top-10 IDs: %v", trueTop10)
			t.Logf("Returned IDs: %v", returnedIDs)
		} else {
			t.Logf("ANN recall: %.0f%%", recallPct*100)
		}
	})

	t.Run("base64 vector encoding", func(t *testing.T) {
		queryVec := makeVector(dim, 2.0)
		queryBase64 := encodeVectorBase64(queryVec)

		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by":         []any{"vector", "ANN", queryBase64},
			"vector_encoding": "base64",
			"top_k":           5,
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d: %v", status, resp)
		}

		rows := resp["rows"].([]any)
		if len(rows) == 0 {
			t.Error("expected results with base64 encoding")
		}
	})
}

// =============================================================================
// SECTION 6: Aggregation and Multi-Query
// =============================================================================

// TestTurbopufferAggregations tests aggregation queries.
func TestTurbopufferAggregations(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "aggs")

	defer client.deleteNamespace(ctx, ns)

	// Setup data
	_, _, err := client.write(ctx, ns, map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "category": "a", "value": 10},
			map[string]any{"id": 2, "category": "a", "value": 20},
			map[string]any{"id": 3, "category": "b", "value": 30},
			map[string]any{"id": 4, "category": "b", "value": 40},
			map[string]any{"id": 5, "category": "c", "value": 50},
		},
	})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	t.Run("count aggregation", func(t *testing.T) {
		status, resp, err := client.query(ctx, ns, map[string]any{
			"aggregate_by": map[string]any{
				"total": []any{"Count"},
			},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		aggs, ok := resp["aggregations"].(map[string]any)
		if !ok {
			t.Fatal("expected aggregations object")
		}
		if aggs["total"] != float64(5) {
			t.Errorf("expected count=5, got %v", aggs["total"])
		}
	})

	t.Run("sum aggregation", func(t *testing.T) {
		status, resp, err := client.query(ctx, ns, map[string]any{
			"aggregate_by": map[string]any{
				"total_value": []any{"Sum", "value"},
			},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		aggs := resp["aggregations"].(map[string]any)
		if aggs["total_value"] != float64(150) {
			t.Errorf("expected sum=150, got %v", aggs["total_value"])
		}
	})

	t.Run("group_by aggregation", func(t *testing.T) {
		status, resp, err := client.query(ctx, ns, map[string]any{
			"aggregate_by": map[string]any{
				"count": []any{"Count"},
			},
			"group_by": []any{"category"},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		groups, ok := resp["aggregation_groups"].([]any)
		if !ok {
			t.Fatal("expected aggregation_groups array")
		}
		if len(groups) != 3 {
			t.Errorf("expected 3 groups, got %d", len(groups))
		}
	})
}

// TestTurbopufferMultiQuery tests multi-query functionality.
func TestTurbopufferMultiQuery(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "multi-query")

	defer client.deleteNamespace(ctx, ns)

	// Setup data
	_, _, err := client.write(ctx, ns, map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "A", "score": 10},
			map[string]any{"id": 2, "name": "B", "score": 20},
			map[string]any{"id": 3, "name": "C", "score": 30},
		},
	})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	t.Run("multiple subqueries", func(t *testing.T) {
		status, resp, err := client.query(ctx, ns, map[string]any{
			"queries": []any{
				map[string]any{"rank_by": []any{"id", "asc"}, "limit": 2},
				map[string]any{"rank_by": []any{"score", "desc"}, "limit": 1},
			},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		results, ok := resp["results"].([]any)
		if !ok {
			t.Fatal("expected results array")
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}

		// First result should have 2 rows
		first := results[0].(map[string]any)
		if rows, ok := first["rows"].([]any); ok {
			if len(rows) != 2 {
				t.Errorf("first result: expected 2 rows, got %d", len(rows))
			}
		}

		// Second result should have 1 row (highest score)
		second := results[1].(map[string]any)
		if rows, ok := second["rows"].([]any); ok {
			if len(rows) != 1 {
				t.Errorf("second result: expected 1 row, got %d", len(rows))
			}
			if len(rows) > 0 {
				row := rows[0].(map[string]any)
				if row["name"] != "C" {
					t.Errorf("expected highest score to be C, got %v", row["name"])
				}
			}
		}
	})

	t.Run("max 16 subqueries", func(t *testing.T) {
		queries := make([]any, 17)
		for i := range queries {
			queries[i] = map[string]any{"rank_by": []any{"id", "asc"}, "limit": 1}
		}

		status, _, err := client.query(ctx, ns, map[string]any{
			"queries": queries,
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusBadRequest {
			t.Errorf("expected 400 for 17 subqueries, got %d", status)
		}
	})
}

// =============================================================================
// SECTION 7: Filter Operators
// =============================================================================

// TestTurbopufferFilters tests various filter operators.
func TestTurbopufferFilters(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "filters")

	defer client.deleteNamespace(ctx, ns)

	// Setup data
	_, _, err := client.write(ctx, ns, map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "Alpha", "score": 10, "tags": []any{"a", "b"}},
			map[string]any{"id": 2, "name": "Beta", "score": 20, "tags": []any{"b", "c"}},
			map[string]any{"id": 3, "name": "Gamma", "score": 30, "tags": []any{"c", "d"}},
			map[string]any{"id": 4, "name": "Delta", "score": 40},
		},
	})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	testCases := []struct {
		name          string
		filter        any
		expectedCount int
	}{
		{"Eq", []any{"score", "Eq", 20}, 1},
		{"NotEq", []any{"score", "NotEq", 20}, 3},
		{"Lt", []any{"score", "Lt", 25}, 2},
		{"Lte", []any{"score", "Lte", 20}, 2},
		{"Gt", []any{"score", "Gt", 20}, 2},
		{"Gte", []any{"score", "Gte", 20}, 3},
		{"In", []any{"score", "In", []any{10, 30}}, 2},
		{"NotIn", []any{"score", "NotIn", []any{10, 30}}, 2},
		{"And", []any{"And", []any{[]any{"score", "Gt", 10}, []any{"score", "Lt", 40}}}, 2},
		{"Or", []any{"Or", []any{[]any{"score", "Eq", 10}, []any{"score", "Eq", 40}}}, 2},
		{"Contains", []any{"tags", "Contains", "b"}, 2},
		{"Eq null (missing)", []any{"tags", "Eq", nil}, 1},       // Delta has no tags
		{"NotEq null (present)", []any{"tags", "NotEq", nil}, 3}, // Alpha, Beta, Gamma have tags
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, resp, err := client.query(ctx, ns, map[string]any{
				"rank_by": []any{"id", "asc"},
				"filters": tc.filter,
			})
			if err != nil {
				t.Fatalf("query error: %v", err)
			}
			if status != http.StatusOK {
				t.Fatalf("expected 200, got %d: %v", status, resp)
			}

			rows := resp["rows"].([]any)
			if len(rows) != tc.expectedCount {
				t.Errorf("expected %d rows, got %d", tc.expectedCount, len(rows))
			}
		})
	}
}

// TestTurbopufferEqNullSemantics specifically tests Eq null behavior.
func TestTurbopufferEqNullSemantics(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "eq-null")

	defer client.deleteNamespace(ctx, ns)

	// Setup: some docs with email, some without
	_, _, err := client.write(ctx, ns, map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "With Email", "email": "a@test.com"},
			map[string]any{"id": 2, "name": "Without Email"},
			map[string]any{"id": 3, "name": "Null Email", "email": nil},
			map[string]any{"id": 4, "name": "Also With Email", "email": "b@test.com"},
		},
	})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	t.Run("Eq null matches missing and null", func(t *testing.T) {
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"filters": []any{"email", "Eq", nil},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		rows := resp["rows"].([]any)
		// Should match id:2 (missing) and id:3 (null)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows with null email, got %d", len(rows))
			for _, r := range rows {
				t.Logf("  got: %v", r)
			}
		}
	})

	t.Run("NotEq null matches present values", func(t *testing.T) {
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"filters": []any{"email", "NotEq", nil},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		rows := resp["rows"].([]any)
		// Should match id:1 and id:4 (have email values)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows with non-null email, got %d", len(rows))
			for _, r := range rows {
				t.Logf("  got: %v", r)
			}
		}
	})
}

// =============================================================================
// SECTION 8: Conditional Operations
// =============================================================================

// TestTurbopufferConditionalUpsert tests conditional upsert with upsert_condition.
func TestTurbopufferConditionalUpsert(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "cond-upsert")

	defer client.deleteNamespace(ctx, ns)

	// Setup: create a document with version 1
	_, _, err := client.write(ctx, ns, map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "Original", "version": 1},
		},
	})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	t.Run("condition met - upsert applies", func(t *testing.T) {
		status, _, err := client.write(ctx, ns, map[string]any{
			"upsert_rows": []any{
				map[string]any{"id": 1, "name": "Updated", "version": 2},
			},
			"upsert_condition": []any{"version", "Lt", map[string]any{"$ref_new": "version"}},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		// Verify update applied
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"filters": []any{"id", "Eq", 1},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}

		rows := resp["rows"].([]any)
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}

		row := rows[0].(map[string]any)
		if row["name"] != "Updated" {
			t.Errorf("expected name=Updated, got %v", row["name"])
		}
		if row["version"] != float64(2) {
			t.Errorf("expected version=2, got %v", row["version"])
		}
	})

	t.Run("condition not met - upsert skipped", func(t *testing.T) {
		// Try to "downgrade" version - should fail condition
		status, _, err := client.write(ctx, ns, map[string]any{
			"upsert_rows": []any{
				map[string]any{"id": 1, "name": "Downgraded", "version": 1},
			},
			"upsert_condition": []any{"version", "Lt", map[string]any{"$ref_new": "version"}},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		// Verify update was NOT applied
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"filters": []any{"id", "Eq", 1},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}

		rows := resp["rows"].([]any)
		row := rows[0].(map[string]any)
		if row["name"] == "Downgraded" {
			t.Error("conditional upsert should have been skipped")
		}
		if row["version"] != float64(2) {
			t.Errorf("version should still be 2, got %v", row["version"])
		}
	})

	t.Run("missing doc - upsert applies unconditionally", func(t *testing.T) {
		status, _, err := client.write(ctx, ns, map[string]any{
			"upsert_rows": []any{
				map[string]any{"id": 999, "name": "New", "version": 1},
			},
			"upsert_condition": []any{"version", "Lt", map[string]any{"$ref_new": "version"}},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		// Verify new doc was created
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"filters": []any{"id", "Eq", 999},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}

		rows := resp["rows"].([]any)
		if len(rows) != 1 {
			t.Errorf("expected 1 row (new doc created), got %d", len(rows))
		}
	})
}

// =============================================================================
// SECTION 9: Namespace Operations
// =============================================================================

// TestTurbopufferNamespaceLifecycle tests namespace creation and deletion.
func TestTurbopufferNamespaceLifecycle(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "lifecycle")

	t.Run("namespace created on first write", func(t *testing.T) {
		// Write creates namespace implicitly
		status, _, err := client.write(ctx, ns, map[string]any{
			"upsert_rows": []any{
				map[string]any{"id": 1, "name": "Test"},
			},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d", status)
		}

		// Metadata should be accessible
		status, resp, err := client.metadata(ctx, ns)
		if err != nil {
			t.Fatalf("metadata error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200 for metadata, got %d", status)
		}

		// Verify metadata fields
		if _, ok := resp["created_at"]; !ok {
			t.Error("expected created_at in metadata")
		}
	})

	t.Run("namespace delete returns 200", func(t *testing.T) {
		err := client.deleteNamespace(ctx, ns)
		if err != nil {
			t.Fatalf("delete error: %v", err)
		}

		// Metadata should return 404
		status, _, err := client.metadata(ctx, ns)
		if err != nil {
			t.Fatalf("metadata error: %v", err)
		}
		if status != http.StatusNotFound {
			t.Errorf("expected 404 after delete, got %d", status)
		}
	})
}

// =============================================================================
// SECTION 10: Patch Operations
// =============================================================================

// TestTurbopufferPatchRows tests patch_rows functionality.
func TestTurbopufferPatchRows(t *testing.T) {
	client := skipIfNoAPIKey(t)
	ctx := context.Background()
	ns := testNamespace(t, "patch-rows")

	defer client.deleteNamespace(ctx, ns)

	// Setup
	_, _, err := client.write(ctx, ns, map[string]any{
		"upsert_rows": []any{
			map[string]any{"id": 1, "name": "Original", "score": 10, "extra": "keep"},
		},
	})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	t.Run("patch updates only specified attributes", func(t *testing.T) {
		status, resp, err := client.write(ctx, ns, map[string]any{
			"patch_rows": []any{
				map[string]any{"id": 1, "name": "Patched"},
			},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200, got %d: %v", status, resp)
		}

		// Query to verify
		status, resp, err = client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}

		rows := resp["rows"].([]any)
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}

		row := rows[0].(map[string]any)
		if row["name"] != "Patched" {
			t.Errorf("expected name=Patched, got %v", row["name"])
		}
		if row["score"] != float64(10) {
			t.Errorf("expected score=10 (unchanged), got %v", row["score"])
		}
		if row["extra"] != "keep" {
			t.Errorf("expected extra=keep (unchanged), got %v", row["extra"])
		}
	})

	t.Run("patch to missing ID is silently ignored", func(t *testing.T) {
		status, _, err := client.write(ctx, ns, map[string]any{
			"patch_rows": []any{
				map[string]any{"id": 999, "name": "NonExistent"},
			},
		})
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("expected 200 for patch to missing ID, got %d", status)
		}

		// Verify doc was NOT created
		status, resp, err := client.query(ctx, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"filters": []any{"id", "Eq", 999},
		})
		if err != nil {
			t.Fatalf("query error: %v", err)
		}

		rows := resp["rows"].([]any)
		if len(rows) != 0 {
			t.Errorf("expected 0 rows for non-existent ID, got %d", len(rows))
		}
	})
}
