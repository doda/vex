package integration

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/wal"
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
func newS3Store(t *testing.T) objectstore.Store {
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

	return objectstore.NewInstrumentedStore(s3Store)
}

func newS3FixtureWithStore(t *testing.T, store objectstore.Store) *s3TestFixture {
	t.Helper()
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

// newS3Fixture creates a new test fixture with an S3-compatible object store.
func newS3Fixture(t *testing.T, namespace string) *s3TestFixture {
	t.Helper()
	store := newS3Store(t)
	return newS3FixtureWithStore(t, store)
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

func (f *s3TestFixture) writeRaw(t *testing.T, ns string, data map[string]any) (int, []byte) {
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

	return resp.StatusCode, respBody
}

func (f *s3TestFixture) write(t *testing.T, ns string, data map[string]any) map[string]any {
	t.Helper()

	status, respBody := f.writeRaw(t, ns, data)

	if status != http.StatusOK {
		t.Fatalf("write request returned status %d: %s", status, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	return result
}

type stateCASFailingStore struct {
	objectstore.Store
	stateKey       string
	mu             sync.Mutex
	failsRemaining int
	failCount      int
}

func (s *stateCASFailingStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	s.mu.Lock()
	if key == s.stateKey && s.failsRemaining > 0 {
		s.failsRemaining--
		s.failCount++
		s.mu.Unlock()
		return nil, objectstore.ErrPrecondition
	}
	s.mu.Unlock()
	return s.Store.PutIfMatch(ctx, key, body, size, etag, opts)
}

func (s *stateCASFailingStore) FailCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failCount
}

func readState(ctx context.Context, store objectstore.Store, ns string) (*namespace.State, error) {
	key := fmt.Sprintf("vex/namespaces/%s/meta/state.json", ns)
	reader, _, err := store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var state namespace.State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func waitForStateHeadSeq(t *testing.T, store objectstore.Store, ns string, want uint64) *namespace.State {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, err := readState(ctx, store, ns)
		cancel()
		if err == nil && state.WAL.HeadSeq >= want {
			return state
		}
		if err != nil && !objectstore.IsNotFoundError(err) {
			t.Fatalf("failed to read state.json: %v", err)
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatalf("timed out waiting for state.json: %v", err)
			}
			t.Fatalf("timed out waiting for wal head_seq %d (current %d)", want, state.WAL.HeadSeq)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func readWalEntry(ctx context.Context, store objectstore.Store, ns string, seq uint64) (*wal.WalEntry, error) {
	key := fmt.Sprintf("vex/namespaces/%s/%s", ns, wal.KeyForSeq(seq))
	reader, _, err := store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	decoder, err := wal.NewDecoder()
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	entry, err := decoder.Decode(data)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func waitForWalEntry(t *testing.T, store objectstore.Store, ns string, seq uint64) *wal.WalEntry {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		entry, err := readWalEntry(ctx, store, ns, seq)
		cancel()
		if err == nil {
			return entry
		}
		if err != nil && !objectstore.IsNotFoundError(err) {
			t.Fatalf("failed to read WAL entry %d: %v", seq, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for WAL entry %d: %v", seq, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
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

func (f *s3TestFixture) listNamespaces(t *testing.T, prefix string, pageSize int) map[string]any {
	t.Helper()

	values := url.Values{}
	if prefix != "" {
		values.Set("prefix", prefix)
	}
	if pageSize > 0 {
		values.Set("page_size", fmt.Sprintf("%d", pageSize))
	}
	endpoint := f.endpoint + "/v1/namespaces"
	if encoded := values.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		t.Fatalf("failed to build list namespaces request: %v", err)
	}
	addAuthHeader(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list namespaces request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list namespaces request returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	return result
}

func (f *s3TestFixture) deleteNamespace(t *testing.T, ns string) map[string]any {
	t.Helper()

	req, err := http.NewRequest("DELETE", f.endpoint+"/v2/namespaces/"+ns, nil)
	if err != nil {
		t.Fatalf("failed to build delete namespace request: %v", err)
	}
	addAuthHeader(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete namespace request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete namespace request returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	return result
}

func (f *s3TestFixture) warmCache(t *testing.T, ns string) []byte {
	t.Helper()

	req, err := http.NewRequest("GET", f.endpoint+"/v1/namespaces/"+ns+"/hint_cache_warm", nil)
	if err != nil {
		t.Fatalf("failed to build warm cache request: %v", err)
	}
	addAuthHeader(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("warm cache request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("warm cache request returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody
}

func (f *s3TestFixture) recall(t *testing.T, ns string, data map[string]any) map[string]any {
	t.Helper()

	payload := []byte("{}")
	if data != nil {
		body, err := json.Marshal(data)
		if err != nil {
			t.Fatalf("failed to marshal recall request: %v", err)
		}
		payload = body
	}

	req, err := http.NewRequest("POST", f.endpoint+"/v1/namespaces/"+ns+"/_debug/recall", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("failed to build recall request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("recall request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("recall request returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	return result
}

func gzipPayload(t *testing.T, payload []byte) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(payload); err != nil {
		t.Fatalf("failed to gzip payload: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("failed to close gzip writer: %v", err)
	}
	return &buf
}

func readMaybeGzip(t *testing.T, resp *http.Response) []byte {
	t.Helper()

	if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatalf("failed to create gzip reader: %v", err)
		}
		defer gz.Close()
		data, err := io.ReadAll(gz)
		if err != nil {
			t.Fatalf("failed to read gzip body: %v", err)
		}
		return data
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	return data
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

func TestObjectStoreGzip(t *testing.T) {
	ns := fmt.Sprintf("objectstore-gzip-%d", time.Now().UnixNano())
	fixture := newS3Fixture(t, ns)
	defer fixture.close(t)

	t.Run("gzip write", func(t *testing.T) {
		payload, err := json.Marshal(map[string]any{
			"upsert_rows": []map[string]any{
				{"id": "doc1", "name": "Zed", "age": 42},
			},
		})
		if err != nil {
			t.Fatalf("failed to marshal write payload: %v", err)
		}

		req, err := http.NewRequest("POST", fixture.endpoint+"/v2/namespaces/"+ns, gzipPayload(t, payload))
		if err != nil {
			t.Fatalf("failed to build gzip write request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Accept-Encoding", "gzip")
		addAuthHeader(req)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("gzip write request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body := readMaybeGzip(t, resp)
			t.Fatalf("gzip write returned status %d: %s", resp.StatusCode, string(body))
		}
		if resp.Header.Get("Content-Encoding") != "gzip" {
			t.Fatalf("expected gzip write response, got Content-Encoding=%q", resp.Header.Get("Content-Encoding"))
		}

		body := readMaybeGzip(t, resp)
		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("failed to unmarshal gzip write response: %v", err)
		}
		if result["rows_upserted"].(float64) != 1 {
			t.Fatalf("expected 1 row upserted, got %v", result["rows_upserted"])
		}
	})

	time.Sleep(2 * time.Second)

	t.Run("gzip query", func(t *testing.T) {
		payload, err := json.Marshal(map[string]any{
			"rank_by": []any{"id", "asc"},
			"limit":   10,
		})
		if err != nil {
			t.Fatalf("failed to marshal query payload: %v", err)
		}

		req, err := http.NewRequest("POST", fixture.endpoint+"/v2/namespaces/"+ns+"/query", gzipPayload(t, payload))
		if err != nil {
			t.Fatalf("failed to build gzip query request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Accept-Encoding", "gzip")
		addAuthHeader(req)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("gzip query request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body := readMaybeGzip(t, resp)
			t.Fatalf("gzip query returned status %d: %s", resp.StatusCode, string(body))
		}
		if resp.Header.Get("Content-Encoding") != "gzip" {
			t.Fatalf("expected gzip query response, got Content-Encoding=%q", resp.Header.Get("Content-Encoding"))
		}

		body := readMaybeGzip(t, resp)
		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("failed to unmarshal gzip query response: %v", err)
		}
		rows, ok := result["rows"].([]any)
		if !ok {
			t.Fatalf("expected rows to be an array, got %T", result["rows"])
		}
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
	})

	t.Run("gzip metadata", func(t *testing.T) {
		req, err := http.NewRequest("GET", fixture.endpoint+"/v1/namespaces/"+ns+"/metadata", nil)
		if err != nil {
			t.Fatalf("failed to build gzip metadata request: %v", err)
		}
		req.Header.Set("Accept-Encoding", "gzip")
		addAuthHeader(req)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("gzip metadata request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body := readMaybeGzip(t, resp)
			t.Fatalf("gzip metadata returned status %d: %s", resp.StatusCode, string(body))
		}
		if resp.Header.Get("Content-Encoding") != "gzip" {
			t.Fatalf("expected gzip metadata response, got Content-Encoding=%q", resp.Header.Get("Content-Encoding"))
		}

		body := readMaybeGzip(t, resp)
		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("failed to unmarshal gzip metadata response: %v", err)
		}
		if result["namespace"] != ns {
			t.Fatalf("expected namespace %s, got %v", ns, result["namespace"])
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

func TestObjectStoreEndpoints(t *testing.T) {
	prefix := fmt.Sprintf("objectstore-endpoints-%d", time.Now().UnixNano())
	nsPrimary := prefix + "-primary"
	nsSecondary := prefix + "-secondary"
	fixture := newS3Fixture(t, nsPrimary)
	defer fixture.close(t)

	fixture.write(t, nsPrimary, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc1", "name": "Alpha"},
		},
	})
	fixture.write(t, nsSecondary, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc1", "name": "Beta"},
		},
	})

	t.Run("list namespaces", func(t *testing.T) {
		deadline := time.Now().Add(10 * time.Second)
		for {
			result := fixture.listNamespaces(t, prefix, 100)
			rawNamespaces, ok := result["namespaces"].([]any)
			if !ok {
				t.Fatalf("expected namespaces to be an array, got %T", result["namespaces"])
			}

			found := map[string]bool{}
			for _, raw := range rawNamespaces {
				entry, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				if id, ok := entry["id"].(string); ok {
					found[id] = true
				}
			}

			if found[nsPrimary] && found[nsSecondary] {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("expected namespaces %q and %q to be listed, got %v", nsPrimary, nsSecondary, rawNamespaces)
			}
			time.Sleep(300 * time.Millisecond)
		}
	})

	t.Run("warm cache hint", func(t *testing.T) {
		body := fixture.warmCache(t, nsPrimary)
		if len(body) != 0 {
			t.Fatalf("expected empty warm cache response, got %q", string(body))
		}
	})

	t.Run("recall endpoint", func(t *testing.T) {
		result := fixture.recall(t, nsPrimary, map[string]any{
			"num":   2,
			"top_k": 2,
		})

		if _, ok := result["avg_recall"].(float64); !ok {
			t.Fatalf("expected avg_recall float64, got %T", result["avg_recall"])
		}
		if _, ok := result["avg_ann_count"].(float64); !ok {
			t.Fatalf("expected avg_ann_count float64, got %T", result["avg_ann_count"])
		}
		if _, ok := result["avg_exhaustive_count"].(float64); !ok {
			t.Fatalf("expected avg_exhaustive_count float64, got %T", result["avg_exhaustive_count"])
		}
	})

	t.Run("delete namespace", func(t *testing.T) {
		result := fixture.deleteNamespace(t, nsPrimary)
		if result["status"] != "ok" {
			t.Fatalf("expected status ok, got %v", result["status"])
		}
	})
}

func TestObjectStoreWalCASIdempotency(t *testing.T) {
	ns := fmt.Sprintf("objectstore-walcas-%d", time.Now().UnixNano())
	store := newS3Store(t)
	stateKey := fmt.Sprintf("vex/namespaces/%s/meta/state.json", ns)
	casStore := &stateCASFailingStore{
		Store:          store,
		stateKey:       stateKey,
		failsRemaining: 2,
	}
	fixture := newS3FixtureWithStore(t, casStore)
	defer fixture.close(t)

	firstReq := map[string]any{
		"request_id": "req-1",
		"upsert_rows": []map[string]any{
			{"id": "doc-1", "name": "alpha"},
		},
	}
	status, body := fixture.writeRaw(t, ns, firstReq)
	if status != http.StatusOK {
		t.Fatalf("first write returned status %d: %s", status, string(body))
	}
	var firstResp map[string]any
	if err := json.Unmarshal(body, &firstResp); err != nil {
		t.Fatalf("failed to unmarshal first response: %v", err)
	}

	dupReq := map[string]any{
		"request_id": "req-1",
		"upsert_rows": []map[string]any{
			{"id": "doc-2", "name": "beta"},
		},
	}
	status, body = fixture.writeRaw(t, ns, dupReq)
	if status != http.StatusOK {
		t.Fatalf("duplicate write returned status %d: %s", status, string(body))
	}
	var dupResp map[string]any
	if err := json.Unmarshal(body, &dupResp); err != nil {
		t.Fatalf("failed to unmarshal duplicate response: %v", err)
	}
	if !reflect.DeepEqual(firstResp, dupResp) {
		t.Fatalf("expected duplicate response to match original: %+v != %+v", firstResp, dupResp)
	}

	time.Sleep(2 * time.Second)

	result := fixture.query(t, ns, map[string]any{
		"rank_by": []any{"id", "asc"},
		"limit":   1,
		"filters": []any{"id", "Eq", "doc-2"},
	})
	rows, ok := result["rows"].([]any)
	if !ok {
		t.Fatalf("expected rows to be an array, got %T", result["rows"])
	}
	if len(rows) != 0 {
		t.Fatalf("expected duplicate request_id to be ignored, got %d rows for doc-2", len(rows))
	}

	time.Sleep(1200 * time.Millisecond)

	secondReq := map[string]any{
		"request_id": "req-2",
		"upsert_rows": []map[string]any{
			{"id": "doc-3", "name": "gamma"},
		},
	}
	status, body = fixture.writeRaw(t, ns, secondReq)
	if status != http.StatusOK {
		t.Fatalf("second write returned status %d: %s", status, string(body))
	}

	if casStore.FailCount() == 0 {
		t.Fatalf("expected CAS retry failures to be injected")
	}

	state := waitForStateHeadSeq(t, casStore, ns, 2)
	if state.WAL.HeadSeq != 2 {
		t.Fatalf("expected wal head_seq 2, got %d", state.WAL.HeadSeq)
	}
	if state.WAL.HeadKey != wal.KeyForSeq(2) {
		t.Fatalf("expected wal head_key %q, got %q", wal.KeyForSeq(2), state.WAL.HeadKey)
	}

	entry1 := waitForWalEntry(t, casStore, ns, 1)
	entry2 := waitForWalEntry(t, casStore, ns, 2)

	if entry1.Seq != 1 {
		t.Fatalf("expected wal seq 1, got %d", entry1.Seq)
	}
	if entry2.Seq != 2 {
		t.Fatalf("expected wal seq 2, got %d", entry2.Seq)
	}
	if len(entry1.SubBatches) != 1 {
		t.Fatalf("expected 1 sub-batch in wal seq 1, got %d", len(entry1.SubBatches))
	}
	if entry1.SubBatches[0].RequestId != "req-1" {
		t.Fatalf("expected wal seq 1 request_id req-1, got %q", entry1.SubBatches[0].RequestId)
	}
	if len(entry2.SubBatches) != 1 {
		t.Fatalf("expected 1 sub-batch in wal seq 2, got %d", len(entry2.SubBatches))
	}
	if entry2.SubBatches[0].RequestId != "req-2" {
		t.Fatalf("expected wal seq 2 request_id req-2, got %q", entry2.SubBatches[0].RequestId)
	}
	if entry1.CommittedUnixMs > entry2.CommittedUnixMs {
		t.Fatalf("expected wal commit order to increase, got %d then %d", entry1.CommittedUnixMs, entry2.CommittedUnixMs)
	}
}
