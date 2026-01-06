package golden

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vexsearch/vex/internal/write"
	"github.com/vexsearch/vex/pkg/api"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// goldenTestFixture contains the shared test setup for all golden tests.
type goldenTestFixture struct {
	router *api.Router
	store  objectstore.Store
	ctx    context.Context
}

// newGoldenFixture creates a new test fixture with a fixed dataset.
// This creates a namespace "products" with 10 product documents.
func newGoldenFixture(t *testing.T) *goldenTestFixture {
	t.Helper()

	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	store := objectstore.NewMemoryStore()
	if err := router.SetStore(store); err != nil {
		t.Fatalf("failed to set store: %v", err)
	}

	ctx := httptest.NewRequest("GET", "/", nil).Context()

	// Create the test namespace using the router's state manager
	stateMan := router.StateManager()
	if stateMan == nil {
		t.Fatal("state manager not available")
	}

	_, err := stateMan.Create(ctx, "products")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Insert the fixed dataset
	fixture := &goldenTestFixture{
		router: router,
		store:  store,
		ctx:    ctx,
	}

	fixture.insertFixedDataset(t)

	// Set router state AFTER inserting data
	router.SetState(&api.ServerState{
		Namespaces: map[string]*api.NamespaceState{
			"products": {Exists: true},
		},
		ObjectStore: api.ObjectStoreState{Available: true},
	})

	return fixture
}

// insertFixedDataset inserts a fixed set of product documents for golden tests.
func (f *goldenTestFixture) insertFixedDataset(t *testing.T) {
	t.Helper()

	// Fixed product dataset with varied attributes for testing
	products := []map[string]any{
		{"id": uint64(1), "name": "Widget A", "category": "widgets", "price": float64(19.99), "stock": int64(100), "active": true},
		{"id": uint64(2), "name": "Widget B", "category": "widgets", "price": float64(24.99), "stock": int64(50), "active": true},
		{"id": uint64(3), "name": "Gadget X", "category": "gadgets", "price": float64(99.99), "stock": int64(25), "active": true},
		{"id": uint64(4), "name": "Gadget Y", "category": "gadgets", "price": float64(149.99), "stock": int64(10), "active": false},
		{"id": uint64(5), "name": "Tool Alpha", "category": "tools", "price": float64(49.99), "stock": int64(75), "active": true},
		{"id": uint64(6), "name": "Tool Beta", "category": "tools", "price": float64(59.99), "stock": int64(30), "active": true},
		{"id": uint64(7), "name": "Part 100", "category": "parts", "price": float64(5.99), "stock": int64(500), "active": true},
		{"id": uint64(8), "name": "Part 200", "category": "parts", "price": float64(8.99), "stock": int64(300), "active": true},
		{"id": uint64(9), "name": "Part 300", "category": "parts", "price": float64(12.99), "stock": int64(200), "active": false},
		{"id": uint64(10), "name": "Special Item", "category": "special", "price": float64(999.99), "stock": int64(5), "active": true},
	}

	writeReq := &write.WriteRequest{
		UpsertRows: products,
	}

	// Use the write handler directly
	if f.router.GetWriteHandler() == nil {
		t.Fatal("write handler not available")
	}

	_, err := f.router.GetWriteHandler().Handle(f.ctx, "products", writeReq)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}
}

// close cleans up resources
func (f *goldenTestFixture) close() {
	if f.router != nil {
		f.router.Close()
	}
}

// executeQuery executes a query and returns the response as a map.
func (f *goldenTestFixture) executeQuery(t *testing.T, namespace string, body map[string]any) (int, map[string]any) {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("failed to marshal request body: %v", err)
	}

	req := httptest.NewRequest("POST", "/v2/namespaces/"+namespace+"/query", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req)
	rec := httptest.NewRecorder()

	f.router.ServeHTTP(rec, req)

	var resp map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
	}

	return rec.Code, resp
}

// TestGoldenFixedDataset tests that the fixed dataset is correctly created.
func TestGoldenFixedDataset(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	// Query all products ordered by id
	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"rank_by": []any{"id", "asc"},
		"limit":   100,
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	rows, ok := resp["rows"].([]any)
	if !ok {
		t.Fatal("expected rows array in response")
	}

	// Verify we have 10 products
	if len(rows) != 10 {
		t.Errorf("expected 10 products, got %d", len(rows))
	}

	// Verify first product
	if len(rows) > 0 {
		first := rows[0].(map[string]any)
		if first["name"] != "Widget A" {
			t.Errorf("expected first product to be 'Widget A', got %v", first["name"])
		}
	}
}

// TestGoldenIncludeAttributes tests include_attributes filtering.
func TestGoldenIncludeAttributes(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"rank_by":            []any{"id", "asc"},
		"limit":              3,
		"include_attributes": []any{"name", "price"},
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	rows, ok := resp["rows"].([]any)
	if !ok {
		t.Fatal("expected rows array")
	}

	// Verify only included attributes are present
	for i, row := range rows {
		r := row.(map[string]any)
		if _, ok := r["id"]; !ok {
			t.Errorf("row %d: expected 'id' to be present", i)
		}
		if _, ok := r["name"]; !ok {
			t.Errorf("row %d: expected 'name' to be present", i)
		}
		if _, ok := r["price"]; !ok {
			t.Errorf("row %d: expected 'price' to be present", i)
		}
		// These should NOT be present
		if _, ok := r["category"]; ok {
			t.Errorf("row %d: 'category' should not be present with include_attributes", i)
		}
		if _, ok := r["stock"]; ok {
			t.Errorf("row %d: 'stock' should not be present with include_attributes", i)
		}
		if _, ok := r["active"]; ok {
			t.Errorf("row %d: 'active' should not be present with include_attributes", i)
		}
	}
}

// TestGoldenExcludeAttributes tests exclude_attributes filtering.
func TestGoldenExcludeAttributes(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"rank_by":            []any{"id", "asc"},
		"limit":              3,
		"exclude_attributes": []any{"stock", "active"},
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	rows, ok := resp["rows"].([]any)
	if !ok {
		t.Fatal("expected rows array")
	}

	// Verify excluded attributes are NOT present
	for i, row := range rows {
		r := row.(map[string]any)
		// These SHOULD be present
		if _, ok := r["id"]; !ok {
			t.Errorf("row %d: expected 'id' to be present", i)
		}
		if _, ok := r["name"]; !ok {
			t.Errorf("row %d: expected 'name' to be present", i)
		}
		if _, ok := r["price"]; !ok {
			t.Errorf("row %d: expected 'price' to be present", i)
		}
		if _, ok := r["category"]; !ok {
			t.Errorf("row %d: expected 'category' to be present", i)
		}
		// These should NOT be present
		if _, ok := r["stock"]; ok {
			t.Errorf("row %d: 'stock' should not be present with exclude_attributes", i)
		}
		if _, ok := r["active"]; ok {
			t.Errorf("row %d: 'active' should not be present with exclude_attributes", i)
		}
	}
}

// TestGoldenLimit tests limit parameter behavior.
func TestGoldenLimit(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	testCases := []struct {
		name  string
		limit int
		want  int
	}{
		{"limit_1", 1, 1},
		{"limit_3", 3, 3},
		{"limit_5", 5, 5},
		{"limit_10", 10, 10},
		{"limit_100", 100, 10}, // Only 10 docs exist
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, resp := fixture.executeQuery(t, "products", map[string]any{
				"rank_by": []any{"id", "asc"},
				"limit":   tc.limit,
			})

			if status != http.StatusOK {
				t.Fatalf("expected 200, got %d", status)
			}

			rows := resp["rows"].([]any)
			if len(rows) != tc.want {
				t.Errorf("expected %d rows, got %d", tc.want, len(rows))
			}
		})
	}
}

// TestGoldenAggregationCount tests Count aggregation.
func TestGoldenAggregationCount(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"aggregate_by": map[string]any{
			"total_count": []any{"Count"},
		},
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	// Should have aggregations, not rows
	if _, ok := resp["rows"]; ok {
		t.Error("aggregation response should not have 'rows'")
	}

	aggs, ok := resp["aggregations"].(map[string]any)
	if !ok {
		t.Fatal("expected 'aggregations' object")
	}

	// Verify count is 10
	if aggs["total_count"] != float64(10) {
		t.Errorf("expected total_count=10, got %v", aggs["total_count"])
	}
}

// TestGoldenAggregationSum tests Sum aggregation.
func TestGoldenAggregationSum(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"aggregate_by": map[string]any{
			"total_stock": []any{"Sum", "stock"},
		},
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	aggs, ok := resp["aggregations"].(map[string]any)
	if !ok {
		t.Fatal("expected 'aggregations' object")
	}

	// Sum of stock: 100+50+25+10+75+30+500+300+200+5 = 1295
	expectedSum := float64(1295)
	if aggs["total_stock"] != expectedSum {
		t.Errorf("expected total_stock=%v, got %v", expectedSum, aggs["total_stock"])
	}
}

// TestGoldenAggregationWithFilter tests aggregation with filter.
func TestGoldenAggregationWithFilter(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"aggregate_by": map[string]any{
			"count":       []any{"Count"},
			"total_stock": []any{"Sum", "stock"},
		},
		"filters": []any{"category", "Eq", "widgets"},
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	aggs, ok := resp["aggregations"].(map[string]any)
	if !ok {
		t.Fatal("expected 'aggregations' object")
	}

	// Widgets: 2 items (Widget A stock=100, Widget B stock=50)
	if aggs["count"] != float64(2) {
		t.Errorf("expected count=2 for widgets, got %v", aggs["count"])
	}
	if aggs["total_stock"] != float64(150) {
		t.Errorf("expected total_stock=150 for widgets, got %v", aggs["total_stock"])
	}
}

// TestGoldenGroupBy tests grouped aggregation.
func TestGoldenGroupBy(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"aggregate_by": map[string]any{
			"count": []any{"Count"},
		},
		"group_by": []any{"category"},
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	// Should have aggregation_groups, not rows or aggregations
	if _, ok := resp["rows"]; ok {
		t.Error("grouped response should not have 'rows'")
	}
	if _, ok := resp["aggregations"]; ok {
		t.Error("grouped response should not have 'aggregations'")
	}

	groups, ok := resp["aggregation_groups"].([]any)
	if !ok {
		t.Fatal("expected 'aggregation_groups' array")
	}

	// Should have 5 categories: widgets(2), gadgets(2), tools(2), parts(3), special(1)
	if len(groups) != 5 {
		t.Errorf("expected 5 groups, got %d", len(groups))
	}

	// Verify group structure and count by category
	categoryCount := make(map[string]float64)
	for _, g := range groups {
		group := g.(map[string]any)
		cat := group["category"].(string)
		count := group["count"].(float64)
		categoryCount[cat] = count
	}

	expectedCounts := map[string]float64{
		"widgets": 2,
		"gadgets": 2,
		"tools":   2,
		"parts":   3,
		"special": 1,
	}

	for cat, expected := range expectedCounts {
		if categoryCount[cat] != expected {
			t.Errorf("category %s: expected count=%v, got %v", cat, expected, categoryCount[cat])
		}
	}
}

// TestGoldenMultiQueryBasic tests basic multi-query functionality.
func TestGoldenMultiQueryBasic(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"queries": []any{
			map[string]any{"rank_by": []any{"id", "asc"}, "limit": 3},
			map[string]any{"rank_by": []any{"price", "desc"}, "limit": 2},
		},
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	// Should have results array
	results, ok := resp["results"].([]any)
	if !ok {
		t.Fatal("expected 'results' array for multi-query")
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// First result: 3 products ordered by id asc
	first := results[0].(map[string]any)
	firstRows := first["rows"].([]any)
	if len(firstRows) != 3 {
		t.Errorf("first query: expected 3 rows, got %d", len(firstRows))
	}

	// Second result: 2 products ordered by price desc (most expensive first)
	second := results[1].(map[string]any)
	secondRows := second["rows"].([]any)
	if len(secondRows) != 2 {
		t.Errorf("second query: expected 2 rows, got %d", len(secondRows))
	}

	// Verify order: most expensive should be "Special Item" at 999.99
	if len(secondRows) > 0 {
		firstByPrice := secondRows[0].(map[string]any)
		if firstByPrice["name"] != "Special Item" {
			t.Errorf("expected most expensive to be 'Special Item', got %v", firstByPrice["name"])
		}
	}
}

// TestGoldenMultiQueryMixed tests multi-query with mixed query types.
func TestGoldenMultiQueryMixed(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"queries": []any{
			map[string]any{"rank_by": []any{"id", "asc"}, "limit": 5},                                // Regular query
			map[string]any{"aggregate_by": map[string]any{"total": []any{"Count"}}},                 // Aggregation
			map[string]any{"aggregate_by": map[string]any{"count": []any{"Count"}}, "group_by": []any{"category"}}, // Grouped
		},
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	results := resp["results"].([]any)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// First result: should have rows
	first := results[0].(map[string]any)
	if _, ok := first["rows"]; !ok {
		t.Error("first result should have 'rows'")
	}
	if _, ok := first["aggregations"]; ok {
		t.Error("first result should not have 'aggregations'")
	}

	// Second result: should have aggregations
	second := results[1].(map[string]any)
	if _, ok := second["aggregations"]; !ok {
		t.Error("second result should have 'aggregations'")
	}
	if _, ok := second["rows"]; ok {
		t.Error("second result should not have 'rows'")
	}

	// Third result: should have aggregation_groups
	third := results[2].(map[string]any)
	if _, ok := third["aggregation_groups"]; !ok {
		t.Error("third result should have 'aggregation_groups'")
	}
	if _, ok := third["rows"]; ok {
		t.Error("third result should not have 'rows'")
	}
	if _, ok := third["aggregations"]; ok {
		t.Error("third result should not have 'aggregations'")
	}
}

// TestGoldenMultiQueryMax16 tests that max 16 subqueries are allowed.
func TestGoldenMultiQueryMax16(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	// 16 should be allowed
	queries16 := make([]any, 16)
	for i := range queries16 {
		queries16[i] = map[string]any{"rank_by": []any{"id", "asc"}, "limit": 1}
	}

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"queries": queries16,
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200 for 16 queries, got %d", status)
	}

	results := resp["results"].([]any)
	if len(results) != 16 {
		t.Errorf("expected 16 results, got %d", len(results))
	}

	// 17 should fail
	queries17 := make([]any, 17)
	for i := range queries17 {
		queries17[i] = map[string]any{"rank_by": []any{"id", "asc"}, "limit": 1}
	}

	status, _ = fixture.executeQuery(t, "products", map[string]any{
		"queries": queries17,
	})

	if status != http.StatusBadRequest {
		t.Errorf("expected 400 for 17 queries, got %d", status)
	}
}

// TestGoldenResponseStructure tests the complete response structure with billing and performance.
func TestGoldenResponseStructure(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"rank_by": []any{"id", "asc"},
		"limit":   5,
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	// Verify rows
	rows, ok := resp["rows"].([]any)
	if !ok {
		t.Fatal("expected 'rows' array")
	}
	if len(rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows))
	}

	// Verify billing
	billing, ok := resp["billing"].(map[string]any)
	if !ok {
		t.Fatal("expected 'billing' object")
	}
	if _, ok := billing["billable_logical_bytes_queried"]; !ok {
		t.Error("billing should have 'billable_logical_bytes_queried'")
	}
	if _, ok := billing["billable_logical_bytes_returned"]; !ok {
		t.Error("billing should have 'billable_logical_bytes_returned'")
	}

	// Verify performance
	perf, ok := resp["performance"].(map[string]any)
	if !ok {
		t.Fatal("expected 'performance' object")
	}
	if _, ok := perf["cache_temperature"]; !ok {
		t.Error("performance should have 'cache_temperature'")
	}
	if _, ok := perf["server_total_ms"]; !ok {
		t.Error("performance should have 'server_total_ms'")
	}
}

// TestGoldenMultiQueryResponseStructure tests multi-query response structure.
func TestGoldenMultiQueryResponseStructure(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"queries": []any{
			map[string]any{"rank_by": []any{"id", "asc"}, "limit": 2},
			map[string]any{"rank_by": []any{"id", "desc"}, "limit": 2},
		},
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	// Multi-query should have results array, not rows
	if _, ok := resp["rows"]; ok {
		t.Error("multi-query should not have top-level 'rows'")
	}

	results, ok := resp["results"].([]any)
	if !ok {
		t.Fatal("expected 'results' array")
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	// Multi-query should still have top-level billing and performance
	if _, ok := resp["billing"]; !ok {
		t.Error("multi-query should have 'billing'")
	}
	if _, ok := resp["performance"]; !ok {
		t.Error("multi-query should have 'performance'")
	}

	// Each result should have its own rows (for rank_by queries)
	for i, res := range results {
		result := res.(map[string]any)
		if _, ok := result["rows"]; !ok {
			t.Errorf("result %d should have 'rows'", i)
		}
	}
}

// TestGoldenOrderByDesc tests descending order by attribute.
func TestGoldenOrderByDesc(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"rank_by": []any{"price", "desc"},
		"limit":   3,
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	rows := resp["rows"].([]any)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Should be ordered by price descending
	// Special Item (999.99), Gadget Y (149.99), Gadget X (99.99)
	expectedNames := []string{"Special Item", "Gadget Y", "Gadget X"}
	for i, row := range rows {
		r := row.(map[string]any)
		if r["name"] != expectedNames[i] {
			t.Errorf("row %d: expected name=%q, got %q", i, expectedNames[i], r["name"])
		}
	}
}

// TestGoldenFilterAndLimit tests filter combined with limit.
func TestGoldenFilterAndLimit(t *testing.T) {
	fixture := newGoldenFixture(t)
	defer fixture.close()

	status, resp := fixture.executeQuery(t, "products", map[string]any{
		"rank_by": []any{"id", "asc"},
		"limit":   2,
		"filters": []any{"active", "Eq", true},
	})

	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	rows := resp["rows"].([]any)

	// Should return exactly 2 active products (limit=2)
	if len(rows) != 2 {
		t.Errorf("expected 2 rows with limit=2, got %d", len(rows))
	}

	// All should be active
	for i, row := range rows {
		r := row.(map[string]any)
		if r["active"] != true {
			t.Errorf("row %d: expected active=true", i)
		}
	}
}
