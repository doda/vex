package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/config"
	"github.com/vexsearch/vex/pkg/api"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// minioTestFixture contains the shared test setup for MinIO integration tests.
type minioTestFixture struct {
	router   *api.Router
	store    objectstore.Store
	server   *httptest.Server
	endpoint string
}

// skipIfNoMinio skips the test if MinIO is not available.
func skipIfNoMinio(t *testing.T) {
	t.Helper()

	endpoint := os.Getenv("VEX_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint+"/minio/health/live", nil)
	if err != nil {
		t.Skipf("Skipping MinIO test: cannot create request: %v", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Skipf("Skipping MinIO test: MinIO not available at %s: %v", endpoint, err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Skipf("Skipping MinIO test: MinIO health check failed with status %d", resp.StatusCode)
	}
}

// newMinioFixture creates a new test fixture with MinIO as the object store.
func newMinioFixture(t *testing.T, namespace string) *minioTestFixture {
	t.Helper()
	skipIfNoMinio(t)

	endpoint := os.Getenv("VEX_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	accessKey := os.Getenv("VEX_TEST_MINIO_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	secretKey := os.Getenv("VEX_TEST_MINIO_SECRET_KEY")
	if secretKey == "" {
		secretKey = "minioadmin"
	}
	bucket := os.Getenv("VEX_TEST_MINIO_BUCKET")
	if bucket == "" {
		bucket = "vex"
	}

	store, err := objectstore.New(objectstore.Config{
		Type:      "minio",
		Endpoint:  endpoint,
		Bucket:    bucket,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    "us-east-1",
		UseSSL:    false,
	})
	if err != nil {
		t.Fatalf("failed to create MinIO store: %v", err)
	}

	cfg := &config.Config{}
	router := api.NewRouter(cfg)
	if err := router.SetStore(store); err != nil {
		t.Fatalf("failed to set store: %v", err)
	}

	// Create the test server
	server := httptest.NewServer(router)

	return &minioTestFixture{
		router:   router,
		store:    store,
		server:   server,
		endpoint: server.URL,
	}
}

func (f *minioTestFixture) close(t *testing.T) {
	t.Helper()
	if f.router != nil {
		if err := f.router.Close(); err != nil {
			t.Logf("warning: failed to close router: %v", err)
		}
	}
	if f.server != nil {
		f.server.Close()
	}
}

func (f *minioTestFixture) write(t *testing.T, ns string, data map[string]any) map[string]any {
	t.Helper()

	body, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("failed to marshal write request: %v", err)
	}

	resp, err := http.Post(f.endpoint+"/v2/namespaces/"+ns, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("write request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write request returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	return result
}

func (f *minioTestFixture) query(t *testing.T, ns string, data map[string]any) map[string]any {
	t.Helper()

	body, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("failed to marshal query request: %v", err)
	}

	resp, err := http.Post(f.endpoint+"/v2/namespaces/"+ns+"/query", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("query request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query request returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	return result
}

func (f *minioTestFixture) getMetadata(t *testing.T, ns string) map[string]any {
	t.Helper()

	resp, err := http.Get(f.endpoint + "/v1/namespaces/" + ns + "/metadata")
	if err != nil {
		t.Fatalf("metadata request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metadata request returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	return result
}

// TestMinioBasicCRUD tests basic CRUD operations with MinIO.
func TestMinioBasicCRUD(t *testing.T) {
	ns := fmt.Sprintf("minio-test-%d", time.Now().UnixNano())
	fixture := newMinioFixture(t, ns)
	defer fixture.close(t)

	// 1. Write documents
	t.Run("write documents", func(t *testing.T) {
		result := fixture.write(t, ns, map[string]any{
			"upsert_rows": []map[string]any{
				{"id": "doc1", "name": "Alice", "age": 30},
				{"id": "doc2", "name": "Bob", "age": 25},
				{"id": "doc3", "name": "Charlie", "age": 35},
			},
		})

		if result["rows_upserted"].(float64) != 3 {
			t.Errorf("expected 3 rows upserted, got %v", result["rows_upserted"])
		}
	})

	// Wait for write to be committed
	time.Sleep(2 * time.Second)

	// 2. Query all documents
	t.Run("query all documents", func(t *testing.T) {
		result := fixture.query(t, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"limit":   10,
		})

		rows, ok := result["rows"].([]any)
		if !ok {
			t.Fatalf("expected rows to be an array, got %T", result["rows"])
		}

		if len(rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(rows))
		}
	})

	// 3. Query with filter
	t.Run("query with filter", func(t *testing.T) {
		result := fixture.query(t, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"limit":   10,
			"filters": []any{"age", "Gt", 28},
		})

		rows, ok := result["rows"].([]any)
		if !ok {
			t.Fatalf("expected rows to be an array, got %T", result["rows"])
		}

		if len(rows) != 2 {
			t.Errorf("expected 2 rows (age > 28), got %d", len(rows))
		}
	})

	// 4. Update document
	t.Run("update document", func(t *testing.T) {
		result := fixture.write(t, ns, map[string]any{
			"patch_rows": []map[string]any{
				{"id": "doc1", "age": 31},
			},
		})

		if result["rows_patched"].(float64) != 1 {
			t.Errorf("expected 1 row patched, got %v", result["rows_patched"])
		}
	})

	// Wait for update
	time.Sleep(2 * time.Second)

	// 5. Verify update
	t.Run("verify update", func(t *testing.T) {
		result := fixture.query(t, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"limit":   10,
			"filters": []any{"id", "Eq", "doc1"},
		})

		rows := result["rows"].([]any)
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}

		doc := rows[0].(map[string]any)
		if doc["age"].(float64) != 31 {
			t.Errorf("expected age to be 31, got %v", doc["age"])
		}
	})

	// 6. Delete document
	t.Run("delete document", func(t *testing.T) {
		result := fixture.write(t, ns, map[string]any{
			"deletes": []string{"doc3"},
		})

		if result["rows_deleted"].(float64) != 1 {
			t.Errorf("expected 1 row deleted, got %v", result["rows_deleted"])
		}
	})

	// Wait for delete
	time.Sleep(2 * time.Second)

	// 7. Verify delete
	t.Run("verify delete", func(t *testing.T) {
		result := fixture.query(t, ns, map[string]any{
			"rank_by": []any{"id", "asc"},
			"limit":   10,
		})

		rows := result["rows"].([]any)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows after delete, got %d", len(rows))
		}
	})

	// 8. Get metadata
	t.Run("get metadata", func(t *testing.T) {
		metadata := fixture.getMetadata(t, ns)

		if metadata["namespace"] != ns {
			t.Errorf("expected namespace %s, got %v", ns, metadata["namespace"])
		}

		if metadata["created_at"] == nil {
			t.Error("expected created_at to be set")
		}
	})
}

// TestMinioPersistence tests that data persists across router restarts.
func TestMinioPersistence(t *testing.T) {
	ns := fmt.Sprintf("minio-persist-%d", time.Now().UnixNano())

	// Create fixture and write data
	fixture1 := newMinioFixture(t, ns)

	// Write data
	fixture1.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "persist1", "value": "hello"},
			{"id": "persist2", "value": "world"},
		},
	})
	time.Sleep(2 * time.Second)

	// Verify data exists
	result := fixture1.query(t, ns, map[string]any{
		"rank_by": []any{"id", "asc"},
		"limit":   10,
	})
	rows := result["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows before close, got %d", len(rows))
	}

	// Close the first fixture
	fixture1.close(t)

	// Create a new fixture with the same MinIO backend
	fixture2 := newMinioFixture(t, ns)
	defer fixture2.close(t)

	// Query should return the same data
	result = fixture2.query(t, ns, map[string]any{
		"rank_by": []any{"id", "asc"},
		"limit":   10,
	})
	rows = result["rows"].([]any)
	if len(rows) != 2 {
		t.Errorf("expected 2 rows after restart, got %d", len(rows))
	}
}
