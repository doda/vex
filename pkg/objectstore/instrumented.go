package objectstore

import (
	"context"
	"io"
	"time"

	"github.com/vexsearch/vex/internal/metrics"
)

// InstrumentedStore wraps a Store with Prometheus metrics.
type InstrumentedStore struct {
	inner Store
}

// NewInstrumentedStore creates a new instrumented store wrapper.
func NewInstrumentedStore(inner Store) *InstrumentedStore {
	return &InstrumentedStore{inner: inner}
}

// Get retrieves an object from the store.
func (s *InstrumentedStore) Get(ctx context.Context, key string, opts *GetOptions) (io.ReadCloser, *ObjectInfo, error) {
	start := time.Now()
	reader, info, err := s.inner.Get(ctx, key, opts)
	metrics.ObserveObjectStoreOp("get", time.Since(start).Seconds(), err)
	return reader, info, err
}

// Head retrieves metadata for an object.
func (s *InstrumentedStore) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	start := time.Now()
	info, err := s.inner.Head(ctx, key)
	metrics.ObserveObjectStoreOp("head", time.Since(start).Seconds(), err)
	return info, err
}

// Put stores an object.
func (s *InstrumentedStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error) {
	start := time.Now()
	info, err := s.inner.Put(ctx, key, body, size, opts)
	metrics.ObserveObjectStoreOp("put", time.Since(start).Seconds(), err)
	return info, err
}

// PutIfAbsent stores an object only if it doesn't already exist.
func (s *InstrumentedStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error) {
	start := time.Now()
	info, err := s.inner.PutIfAbsent(ctx, key, body, size, opts)
	metrics.ObserveObjectStoreOp("put_if_absent", time.Since(start).Seconds(), err)
	return info, err
}

// PutIfMatch stores an object only if the ETag matches.
func (s *InstrumentedStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *PutOptions) (*ObjectInfo, error) {
	start := time.Now()
	info, err := s.inner.PutIfMatch(ctx, key, body, size, etag, opts)
	metrics.ObserveObjectStoreOp("put_if_match", time.Since(start).Seconds(), err)
	return info, err
}

// Delete removes an object from the store.
func (s *InstrumentedStore) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := s.inner.Delete(ctx, key)
	metrics.ObserveObjectStoreOp("delete", time.Since(start).Seconds(), err)
	return err
}

// List lists objects in the store.
func (s *InstrumentedStore) List(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	start := time.Now()
	result, err := s.inner.List(ctx, opts)
	metrics.ObserveObjectStoreOp("list", time.Since(start).Seconds(), err)
	return result, err
}
