package integration

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vexsearch/vex/internal/cache"
	"github.com/vexsearch/vex/internal/index"
	"github.com/vexsearch/vex/internal/indexer"
	"github.com/vexsearch/vex/internal/namespace"
	"github.com/vexsearch/vex/internal/warmer"
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

const (
	garageCredsPath       = "/tmp/vex-garage/creds.env"
	eventualTailCapBytes  = 128 * 1024 * 1024
	payloadSeededBytes    = 12 * 1024 * 1024
	maxConsistencyEntries = 12
)

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

func (f *s3TestFixture) queryRaw(t *testing.T, ns string, data map[string]any) (int, []byte) {
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

type blockingGetStore struct {
	objectstore.Store
	mu          sync.Mutex
	blockKey    string
	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func newBlockingGetStore(store objectstore.Store) *blockingGetStore {
	return &blockingGetStore{
		Store:   store,
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (s *blockingGetStore) SetBlockKey(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockKey = key
}

func (s *blockingGetStore) WaitForBlock(t *testing.T) {
	t.Helper()
	select {
	case <-s.started:
		return
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for cache warm manifest fetch")
	}
}

func (s *blockingGetStore) Release() {
	s.releaseOnce.Do(func() {
		close(s.release)
	})
}

func (s *blockingGetStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	s.mu.Lock()
	blockKey := s.blockKey
	s.mu.Unlock()

	if blockKey != "" && key == blockKey {
		select {
		case s.started <- struct{}{}:
		default:
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	return s.Store.Get(ctx, key, opts)
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

func readManifest(ctx context.Context, store objectstore.Store, key string) (*index.Manifest, error) {
	reader, _, err := store.Get(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	var manifest index.Manifest
	if err := manifest.UnmarshalJSON(data); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func readStateWithInfo(ctx context.Context, store objectstore.Store, ns string) (*namespace.State, *objectstore.ObjectInfo, error) {
	key := fmt.Sprintf("vex/namespaces/%s/meta/state.json", ns)
	reader, info, err := store.Get(ctx, key, nil)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, err
	}

	var state namespace.State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, nil, err
	}

	if info == nil {
		info, err = store.Head(ctx, key)
		if err != nil {
			return &state, nil, err
		}
	}

	return &state, info, nil
}

func updateState(t *testing.T, store objectstore.Store, ns string, mutate func(*namespace.State)) *namespace.State {
	t.Helper()

	stateKey := fmt.Sprintf("vex/namespaces/%s/meta/state.json", ns)
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, info, err := readStateWithInfo(ctx, store, ns)
		cancel()
		if err != nil {
			if objectstore.IsNotFoundError(err) && time.Now().Before(deadline) {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			t.Fatalf("failed to read state.json: %v", err)
		}

		mutate(state)
		state.UpdatedAt = time.Now().UTC()

		data, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("failed to marshal state.json: %v", err)
		}

		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		_, err = store.PutIfMatch(ctx, stateKey, bytes.NewReader(data), int64(len(data)), info.ETag, &objectstore.PutOptions{
			ContentType: "application/json",
		})
		cancel()
		if err == nil {
			return state
		}
		if objectstore.IsPreconditionError(err) && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		t.Fatalf("failed to update state.json: %v", err)
	}
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

func waitForSchemaAttribute(t *testing.T, store objectstore.Store, ns, attr string) *namespace.State {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, err := readState(ctx, store, ns)
		cancel()
		if err != nil && !objectstore.IsNotFoundError(err) {
			t.Fatalf("failed to read state.json: %v", err)
		}
		if err == nil && state.Schema != nil && state.Schema.Attributes != nil {
			if _, ok := state.Schema.Attributes[attr]; ok {
				return state
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for schema attribute %q", attr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func waitForPendingRebuild(t *testing.T, store objectstore.Store, ns, kind, attribute string, wantReady bool) *namespace.State {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, err := readState(ctx, store, ns)
		cancel()
		if err != nil && !objectstore.IsNotFoundError(err) {
			t.Fatalf("failed to read state.json: %v", err)
		}
		if err == nil {
			for _, pr := range state.Index.PendingRebuilds {
				if pr.Kind == kind && pr.Attribute == attribute && pr.Ready == wantReady {
					return state
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for pending rebuild %s/%s ready=%v", kind, attribute, wantReady)
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

func waitForWalSize(t *testing.T, store objectstore.Store, ns string, seq uint64) int64 {
	t.Helper()
	key := fmt.Sprintf("vex/namespaces/%s/%s", ns, wal.KeyForSeq(seq))
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		info, err := store.Head(ctx, key)
		cancel()
		if err == nil {
			return info.Size
		}
		if err != nil && !objectstore.IsNotFoundError(err) {
			t.Fatalf("failed to head WAL entry %d: %v", seq, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for WAL entry %d size: %v", seq, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func indexNamespaceToSeq(t *testing.T, store objectstore.Store, ns string, endSeq uint64) {
	t.Helper()
	stateMan := namespace.NewStateManager(store)
	idx := indexer.New(store, stateMan, nil, nil)
	processor := indexer.NewL0SegmentProcessor(store, stateMan, nil, idx)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	loaded, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to load state for indexing: %v", err)
	}
	result, err := processor.ProcessWAL(ctx, ns, 0, endSeq, loaded.State, loaded.ETag)
	if err != nil {
		t.Fatalf("failed to index WAL range: %v", err)
	}
	if result == nil || !result.ManifestWritten {
		t.Fatalf("expected manifest to be written during indexing")
	}
	updated, err := stateMan.Load(ctx, ns)
	if err != nil {
		t.Fatalf("failed to reload state after indexing: %v", err)
	}
	if updated.State.Index.IndexedWALSeq < endSeq {
		t.Fatalf("expected indexed_wal_seq >= %d, got %d", endSeq, updated.State.Index.IndexedWALSeq)
	}
}

func seededPayload(t *testing.T, sizeBytes int) string {
	t.Helper()
	buf := make([]byte, sizeBytes)
	rng := rand.New(rand.NewSource(1))
	if _, err := rng.Read(buf); err != nil {
		t.Fatalf("failed to generate payload: %v", err)
	}
	return base64.StdEncoding.EncodeToString(buf)
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

func extractRowIDs(t *testing.T, result map[string]any) map[string]bool {
	t.Helper()
	rows, ok := result["rows"].([]any)
	if !ok {
		t.Fatalf("expected rows to be an array, got %T", result["rows"])
	}
	ids := make(map[string]bool, len(rows))
	for _, row := range rows {
		entry, ok := row.(map[string]any)
		if !ok {
			t.Fatalf("expected row to be object, got %T", row)
		}
		id, ok := entry["id"].(string)
		if !ok {
			t.Fatalf("expected id to be string, got %T", entry["id"])
		}
		ids[id] = true
	}
	return ids
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

func TestObjectStoreDeleteList(t *testing.T) {
	prefix := fmt.Sprintf("objectstore-delete-list-%d", time.Now().UnixNano())
	nsDelete := prefix + "-delete"
	nsKeep := prefix + "-keep"
	fixture := newS3Fixture(t, nsDelete)
	defer fixture.close(t)

	fixture.write(t, nsDelete, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc1", "name": "Alpha"},
		},
	})
	fixture.write(t, nsKeep, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc1", "name": "Beta"},
		},
	})

	collectIDs := func(result map[string]any) map[string]bool {
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
		return found
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		found := collectIDs(fixture.listNamespaces(t, prefix, 100))
		if found[nsDelete] && found[nsKeep] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected namespaces %q and %q to be listed, got %v", nsDelete, nsKeep, found)
		}
		time.Sleep(300 * time.Millisecond)
	}

	fixture.deleteNamespace(t, nsDelete)

	deadline = time.Now().Add(10 * time.Second)
	for {
		found := collectIDs(fixture.listNamespaces(t, prefix, 100))
		if found[nsKeep] && !found[nsDelete] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected namespace %q present and %q absent, got %v", nsKeep, nsDelete, found)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func TestObjectStoreWarmCacheHint(t *testing.T) {
	ns := fmt.Sprintf("objectstore-warmcache-%d", time.Now().UnixNano())
	store := newS3Store(t)
	blockingStore := newBlockingGetStore(store)

	cfg := newTestConfig()
	router := api.NewRouter(cfg)

	diskCache, err := cache.NewDiskCache(cache.DiskCacheConfig{
		RootPath: t.TempDir(),
		MaxBytes: 64 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create disk cache: %v", err)
	}
	ramCache := cache.NewMemoryCache(cache.MemoryCacheConfig{
		MaxBytes: 8 * 1024 * 1024,
	})

	router.SetDiskCache(diskCache)
	router.SetRAMCache(ramCache)
	if err := router.SetStore(blockingStore); err != nil {
		t.Fatalf("failed to set store: %v", err)
	}
	cacheWarmer := warmer.New(blockingStore, router.StateManager(), diskCache, ramCache, warmer.DefaultConfig())
	router.SetCacheWarmer(cacheWarmer)

	server := httptest.NewServer(router)
	fixture := &s3TestFixture{
		router:   router,
		store:    blockingStore,
		server:   server,
		endpoint: server.URL,
	}
	defer fixture.close(t)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc1", "vector": []float64{0.1, 0.2}, "category": "alpha"},
			{"id": "doc2", "vector": []float64{0.2, 0.1}, "category": "beta"},
		},
	})

	state := waitForStateHeadSeq(t, store, ns, 1)
	indexNamespaceToSeq(t, store, ns, state.WAL.HeadSeq)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	updated, err := readState(ctx, store, ns)
	cancel()
	if err != nil {
		t.Fatalf("failed to read state: %v", err)
	}

	manifestKey := updated.Index.ManifestKey
	if manifestKey == "" && updated.Index.ManifestSeq != 0 {
		manifestKey = index.ManifestKey(ns, updated.Index.ManifestSeq)
	}
	if manifestKey == "" {
		t.Fatalf("expected manifest key after indexing")
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	manifest, err := readManifest(ctx, store, manifestKey)
	cancel()
	if err != nil {
		t.Fatalf("failed to read manifest: %v", err)
	}
	if len(manifest.Segments) == 0 {
		t.Fatalf("expected at least one segment in manifest")
	}

	segment := manifest.Segments[0]
	if segment.IVFKeys == nil || segment.IVFKeys.CentroidsKey == "" || segment.IVFKeys.ClusterOffsetsKey == "" {
		t.Fatalf("expected IVF keys for warm cache prefetch")
	}
	if segment.DocsKey == "" {
		t.Fatalf("expected docs key for warm cache prefetch")
	}
	if len(segment.FilterKeys) == 0 {
		t.Fatalf("expected filter keys for warm cache prefetch")
	}

	expectedKeys := []string{segment.IVFKeys.CentroidsKey, segment.IVFKeys.ClusterOffsetsKey, segment.DocsKey}
	expectedKeys = append(expectedKeys, segment.FilterKeys...)

	cacheKeys := make(map[string]cache.CacheKey, len(expectedKeys))
	for _, key := range expectedKeys {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		info, err := store.Head(ctx, key)
		cancel()
		if err != nil {
			t.Fatalf("failed to head %s: %v", key, err)
		}
		cacheKeys[key] = cache.CacheKey{ObjectKey: key, ETag: info.ETag}
	}

	missingBefore := false
	for _, key := range expectedKeys {
		if _, err := diskCache.Get(cacheKeys[key]); err != nil {
			if errors.Is(err, cache.ErrCacheMiss) {
				missingBefore = true
				continue
			}
			t.Fatalf("unexpected disk cache get error for %s: %v", key, err)
		}
	}
	if !missingBefore {
		t.Fatalf("expected disk cache to be cold before warming")
	}

	blockingStore.SetBlockKey(manifestKey)
	t.Cleanup(blockingStore.Release)

	body := fixture.warmCache(t, ns)
	if len(body) != 0 {
		t.Fatalf("expected empty warm cache response, got %q", string(body))
	}

	blockingStore.WaitForBlock(t)

	prefix := "vex/namespaces/" + ns + "/"
	deadline := time.Now().Add(5 * time.Second)
	for !diskCache.IsPinned(prefix) {
		if time.Now().After(deadline) {
			t.Fatalf("expected disk cache to be pinned during warm")
		}
		time.Sleep(20 * time.Millisecond)
	}

	blockingStore.Release()

	waitForDisk := func(key string) {
		t.Helper()
		cacheKey := cacheKeys[key]
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := diskCache.Get(cacheKey); err == nil {
				return
			} else if !errors.Is(err, cache.ErrCacheMiss) {
				t.Fatalf("unexpected disk cache error for %s: %v", key, err)
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for disk cache entry %s", key)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	waitForRAM := func(key string) {
		t.Helper()
		memKey := cache.MemoryCacheKey{
			Namespace: ns,
			ShardID:   "warm",
			ItemID:    key,
			ItemType:  cache.TypeCentroid,
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := ramCache.Get(memKey); err == nil {
				return
			} else if !errors.Is(err, cache.ErrRAMCacheMiss) {
				t.Fatalf("unexpected RAM cache error for %s: %v", key, err)
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for RAM cache entry %s", key)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	for _, key := range expectedKeys {
		waitForDisk(key)
	}
	waitForRAM(segment.IVFKeys.CentroidsKey)
	waitForRAM(segment.IVFKeys.ClusterOffsetsKey)

	deadline = time.Now().Add(5 * time.Second)
	for diskCache.IsPinned(prefix) {
		if time.Now().After(deadline) {
			t.Fatalf("expected disk cache to unpin after warm")
		}
		time.Sleep(20 * time.Millisecond)
	}
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

func TestObjectStoreWriteBackpressure(t *testing.T) {
	ns := fmt.Sprintf("objectstore-backpressure-%d", time.Now().UnixNano())
	fixture := newS3Fixture(t, ns)
	defer fixture.close(t)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "seed", "name": "seed"},
		},
	})

	updateState(t, fixture.store, ns, func(state *namespace.State) {
		state.WAL.BytesUnindexedEst = int64(api.MaxUnindexedBytes) + 1
		state.WAL.Status = "updating"
		state.NamespaceFlags.DisableBackpressure = false
	})

	status, body := fixture.writeRaw(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "doc-1", "name": "alpha"},
		},
	})
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected 429 for backpressure, got %d: %s", status, string(body))
	}
	var errResp map[string]any
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("failed to unmarshal backpressure response: %v", err)
	}
	if msg, ok := errResp["error"].(string); !ok || !strings.Contains(msg, "backpressure") {
		t.Fatalf("expected backpressure error message, got %v", errResp["error"])
	}

	status, body = fixture.writeRaw(t, ns, map[string]any{
		"disable_backpressure": true,
		"upsert_rows": []map[string]any{
			{"id": "doc-2", "name": "beta"},
		},
	})
	if status != http.StatusOK {
		t.Fatalf("expected 200 with disable_backpressure, got %d: %s", status, string(body))
	}

	state := waitForStateHeadSeq(t, fixture.store, ns, 2)
	if !state.NamespaceFlags.DisableBackpressure {
		t.Fatalf("expected namespace disable_backpressure flag to be set")
	}
}

func TestObjectStoreBM25FTS(t *testing.T) {
	ns := fmt.Sprintf("objectstore-bm25-fts-%d", time.Now().UnixNano())
	fixture := newS3Fixture(t, ns)
	defer fixture.close(t)

	fixture.write(t, ns, map[string]any{
		"schema": map[string]any{
			"content": map[string]any{
				"type": "string",
			},
		},
		"upsert_rows": []map[string]any{
			{"id": "doc1", "content": "hello world"},
			{"id": "doc2", "content": "vex search engine"},
		},
	})

	state := waitForStateHeadSeq(t, fixture.store, ns, 1)
	if state.WAL.HeadSeq != 1 {
		t.Fatalf("expected wal head_seq 1, got %d", state.WAL.HeadSeq)
	}
	state = waitForSchemaAttribute(t, fixture.store, ns, "content")
	if len(state.Schema.Attributes["content"].FullTextSearch) != 0 {
		t.Fatalf("expected full_text_search to be disabled initially")
	}

	fixture.write(t, ns, map[string]any{
		"schema": map[string]any{
			"content": map[string]any{
				"type":             "string",
				"full_text_search": true,
			},
		},
		"upsert_rows": []map[string]any{
			{"id": "doc3", "content": "search results for vex"},
		},
	})

	waitForStateHeadSeq(t, fixture.store, ns, 2)
	state = waitForSchemaAttribute(t, fixture.store, ns, "content")
	attr := state.Schema.Attributes["content"]
	if len(attr.FullTextSearch) == 0 {
		t.Fatalf("expected full_text_search to be enabled in schema update")
	}
	waitForPendingRebuild(t, fixture.store, ns, "fts", "content", false)

	status, body := fixture.queryRaw(t, ns, map[string]any{
		"rank_by": []any{"content", "BM25", "search"},
		"limit":   10,
	})
	if status != http.StatusAccepted {
		t.Fatalf("expected 202 for pending FTS rebuild, got %d: %s", status, string(body))
	}
	var pendingResp map[string]any
	if err := json.Unmarshal(body, &pendingResp); err != nil {
		t.Fatalf("failed to unmarshal pending rebuild response: %v", err)
	}
	if pendingResp["status"] != "accepted" {
		t.Fatalf("expected status accepted, got %v", pendingResp["status"])
	}

	stateMan := namespace.NewStateManager(fixture.store)
	idx := indexer.New(fixture.store, stateMan, &indexer.IndexerConfig{
		PollInterval:          100 * time.Millisecond,
		NamespacePollInterval: 100 * time.Millisecond,
	}, nil)
	if err := idx.WatchNamespace(ns); err != nil {
		t.Fatalf("failed to watch namespace: %v", err)
	}
	t.Cleanup(func() {
		if err := idx.Stop(); err != nil {
			t.Logf("failed to stop indexer: %v", err)
		}
	})

	readyState := waitForPendingRebuild(t, fixture.store, ns, "fts", "content", true)
	if readyState.Index.IndexedWALSeq < readyState.WAL.HeadSeq {
		t.Fatalf("expected indexed_wal_seq >= head_seq after rebuild ready, got %d < %d", readyState.Index.IndexedWALSeq, readyState.WAL.HeadSeq)
	}

	result := fixture.query(t, ns, map[string]any{
		"rank_by": []any{"content", "BM25", "search"},
		"limit":   10,
	})
	ids := extractRowIDs(t, result)
	if !ids["doc2"] || !ids["doc3"] {
		t.Fatalf("expected BM25 results to include doc2 and doc3, got %v", ids)
	}
}

func TestObjectStoreConsistencyTail(t *testing.T) {
	ns := fmt.Sprintf("objectstore-consistency-tail-%d", time.Now().UnixNano())
	fixture := newS3Fixture(t, ns)
	defer fixture.close(t)

	fixture.write(t, ns, map[string]any{
		"upsert_rows": []map[string]any{
			{"id": "base-1", "category": "base"},
		},
	})
	state := waitForStateHeadSeq(t, fixture.store, ns, 1)
	indexNamespaceToSeq(t, fixture.store, ns, state.WAL.HeadSeq)

	payload := seededPayload(t, payloadSeededBytes)

	var (
		tailDocs       []string
		tailSizes      []int64
		totalTailBytes int64
		expectedSeq    = state.WAL.HeadSeq
	)

	for totalTailBytes <= eventualTailCapBytes {
		if len(tailDocs) >= maxConsistencyEntries {
			t.Fatalf("tail bytes did not exceed cap after %d entries (total=%d)", len(tailDocs), totalTailBytes)
		}
		id := fmt.Sprintf("tail-%02d", len(tailDocs)+1)
		fixture.write(t, ns, map[string]any{
			"upsert_rows": []map[string]any{
				{"id": id, "category": "tail", "payload": payload},
			},
		})
		expectedSeq++
		state = waitForStateHeadSeq(t, fixture.store, ns, expectedSeq)
		if state.WAL.HeadSeq != expectedSeq {
			t.Fatalf("expected wal head_seq %d, got %d", expectedSeq, state.WAL.HeadSeq)
		}
		size := waitForWalSize(t, fixture.store, ns, state.WAL.HeadSeq)
		tailDocs = append(tailDocs, id)
		tailSizes = append(tailSizes, size)
		totalTailBytes += size
	}

	strongResult := fixture.query(t, ns, map[string]any{
		"rank_by":            []any{"id", "asc"},
		"limit":              2000,
		"consistency":        "strong",
		"exclude_attributes": []any{"payload"},
	})
	strongIDs := extractRowIDs(t, strongResult)
	if !strongIDs["base-1"] {
		t.Fatalf("expected strong query to include base-1")
	}
	for _, id := range tailDocs {
		if !strongIDs[id] {
			t.Fatalf("expected strong query to include %s", id)
		}
	}

	remaining := int64(eventualTailCapBytes)
	included := make(map[string]bool, len(tailDocs))
	for i := len(tailDocs) - 1; i >= 0; i-- {
		size := tailSizes[i]
		if size > remaining {
			break
		}
		remaining -= size
		included[tailDocs[i]] = true
	}
	if len(included) == len(tailDocs) {
		t.Fatalf("expected some tail entries to be excluded by cap (total=%d)", totalTailBytes)
	}

	eventualResult := fixture.query(t, ns, map[string]any{
		"rank_by":            []any{"id", "asc"},
		"limit":              2000,
		"consistency":        "eventual",
		"exclude_attributes": []any{"payload"},
	})
	eventualIDs := extractRowIDs(t, eventualResult)
	if !eventualIDs["base-1"] {
		t.Fatalf("expected eventual query to include base-1")
	}
	for _, id := range tailDocs {
		if included[id] && !eventualIDs[id] {
			t.Fatalf("expected eventual query to include %s", id)
		}
		if !included[id] && eventualIDs[id] {
			t.Fatalf("expected eventual query to exclude %s", id)
		}
	}
}
