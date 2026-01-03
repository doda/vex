package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/vexsearch/vex/pkg/objectstore"
)

type failingStore struct {
	inner          objectstore.Store
	err            error
	failGet        bool
	failHead       bool
	failPut        bool
	failPutIfAbsent bool
	failPutIfMatch bool
	failDelete     bool
	failList       bool
}

func newFailingStore(inner objectstore.Store, err error) *failingStore {
	if inner == nil {
		inner = objectstore.NewMemoryStore()
	}
	if err == nil {
		err = errors.New("object store failure")
	}
	return &failingStore{
		inner: inner,
		err:   err,
	}
}

func (s *failingStore) Get(ctx context.Context, key string, opts *objectstore.GetOptions) (io.ReadCloser, *objectstore.ObjectInfo, error) {
	if s.failGet {
		return nil, nil, s.err
	}
	return s.inner.Get(ctx, key, opts)
}

func (s *failingStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	if s.failHead {
		return nil, s.err
	}
	return s.inner.Head(ctx, key)
}

func (s *failingStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if s.failPut {
		return nil, s.err
	}
	return s.inner.Put(ctx, key, body, size, opts)
}

func (s *failingStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if s.failPutIfAbsent {
		return nil, s.err
	}
	return s.inner.PutIfAbsent(ctx, key, body, size, opts)
}

func (s *failingStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *objectstore.PutOptions) (*objectstore.ObjectInfo, error) {
	if s.failPutIfMatch {
		return nil, s.err
	}
	return s.inner.PutIfMatch(ctx, key, body, size, etag, opts)
}

func (s *failingStore) Delete(ctx context.Context, key string) error {
	if s.failDelete {
		return s.err
	}
	return s.inner.Delete(ctx, key)
}

func (s *failingStore) List(ctx context.Context, opts *objectstore.ListOptions) (*objectstore.ListResult, error) {
	if s.failList {
		return nil, s.err
	}
	return s.inner.List(ctx, opts)
}

func parseErrorResponse(t *testing.T, resp *http.Response, expectedStatus int) string {
	t.Helper()

	if resp.StatusCode != expectedStatus {
		t.Fatalf("expected status %d, got %d", expectedStatus, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["status"] != "error" {
		t.Fatalf("expected status 'error', got %v", result["status"])
	}

	errMsg, ok := result["error"].(string)
	if !ok || errMsg == "" {
		t.Fatalf("expected error message, got %v", result["error"])
	}

	return errMsg
}
