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
	"strings"
	"testing"
	"time"

	"github.com/vexsearch/vex/pkg/api"
	"github.com/vexsearch/vex/pkg/objectstore"
)

// s3TestFixture contains the shared test setup for S3-compatible integration tests.
type s3TestFixture struct {
	router   *api.Router
	store    objectstore.Store
	server   *httptest.Server
	endpoint string
}

const garageCredsPath = "/tmp/vex-garage/creds.env"

func getenvAny(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}

func parseBoolEnv(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

func loadGarageEnvFromFile(t *testing.T) {
	t.Helper()

	if os.Getenv("VEX_TEST_S3_ACCESS_KEY") != "" || os.Getenv("VEX_TEST_MINIO_ACCESS_KEY") != "" {
		return
	}

	data, err := os.ReadFile(garageCredsPath)
	if err != nil {
		return
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		value := strings.TrimSpace(line[idx+1:])
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') ||
				(value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		_ = os.Setenv(key, value)
	}
}

// newS3Fixture creates a new test fixture with an S3-compatible object store.
func newS3Fixture(t *testing.T, namespace string) *s3TestFixture {
	t.Helper()
	loadGarageEnvFromFile(t)

	endpoint := getenvAny("VEX_TEST_S3_ENDPOINT", "VEX_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:3900"
	}
	accessKey := getenvAny("VEX_TEST_S3_ACCESS_KEY", "VEX_TEST_MINIO_ACCESS_KEY")
	secretKey := getenvAny("VEX_TEST_S3_SECRET_KEY", "VEX_TEST_MINIO_SECRET_KEY")
	if accessKey == "" || secretKey == "" {
		t.Fatalf("missing object store credentials; run ./scripts/setup-garage.sh and source %s", garageCredsPath)
	}
	bucket := getenvAny("VEX_TEST_S3_BUCKET", "VEX_TEST_MINIO_BUCKET")
	if bucket == "" {
		bucket = "vex"
	}
	region := getenvAny("VEX_TEST_S3_REGION", "VEX_TEST_MINIO_REGION")
	if region == "" {
		region = "garage"
	}
	useSSL := parseBoolEnv(getenvAny("VEX_TEST_S3_USE_SSL", "VEX_TEST_MINIO_USE_SSL"))

	s3Store, err := objectstore.NewS3Store(objectstore.S3Config{
		Endpoint:  endpoint,
		Bucket:    bucket,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    region,
		UseSSL:    useSSL,
	})
	if err != nil {
		t.Fatalf("failed to create S3 store: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s3Store.EnsureBucket(ctx); err != nil {
		t.Fatalf("failed to ensure bucket %q: %v", bucket, err)
	}

	store := objectstore.NewInstrumentedStore(s3Store)

	cfg := newTestConfig()
	router := api.NewRouter(cfg)
	if err := router.SetStore(store); err != nil {
		t.Fatalf("failed to set store: %v", err)
	}

	// Create the test server
	server := httptest.NewServer(router)

	return &s3TestFixture{
		router:   router,
		store:    store,
		server:   server,
		endpoint: server.URL,
	}
}

func (f *s3TestFixture) close(t *testing.T) {
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

func (f *s3TestFixture) write(t *testing.T, ns string, data map[string]any) map[string]any {
	t.Helper()

	body, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("failed to marshal write request: %v", err)
	}

	req, err := http.NewRequest("POST", f.endpoint+"/v2/namespaces/"+ns, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to build write request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req)
	resp, err := http.DefaultClient.Do(req)
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

func (f *s3TestFixture) query(t *testing.T, ns string, data map[string]any) map[string]any {
	t.Helper()

	body, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("failed to marshal query request: %v", err)
	}

	req, err := http.NewRequest("POST", f.endpoint+"/v2/namespaces/"+ns+"/query", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to build query request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req)
	resp, err := http.DefaultClient.Do(req)
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

func (f *s3TestFixture) getMetadata(t *testing.T, ns string) map[string]any {
	t.Helper()

	req, err := http.NewRequest("GET", f.endpoint+"/v1/namespaces/"+ns+"/metadata", nil)
	if err != nil {
		t.Fatalf("failed to build metadata request: %v", err)
	}
	addAuthHeader(req)
	resp, err := http.DefaultClient.Do(req)
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

// TestObjectStoreBasicCRUD tests basic CRUD operations with an S3-compatible store.
func TestObjectStoreBasicCRUD(t *testing.T) {
	ns := fmt.Sprintf("objectstore-test-%d", time.Now().UnixNano())
	fixture := newS3Fixture(t, ns)
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

// TestObjectStorePersistence tests that data persists across router restarts.
func TestObjectStorePersistence(t *testing.T) {
	ns := fmt.Sprintf("objectstore-persist-%d", time.Now().UnixNano())

	// Create fixture and write data
	fixture1 := newS3Fixture(t, ns)

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

	// Create a new fixture with the same backend
	fixture2 := newS3Fixture(t, ns)
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
