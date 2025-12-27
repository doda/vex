package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

type MemoryStore struct {
	mu      sync.RWMutex
	objects map[string]*memoryObject
}

type memoryObject struct {
	data         []byte
	size         int64
	etag         string
	lastModified time.Time
	contentType  string
	checksum     string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		objects: make(map[string]*memoryObject),
	}
}

func (s *MemoryStore) Get(ctx context.Context, key string, opts *GetOptions) (io.ReadCloser, *ObjectInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, ok := s.objects[key]
	if !ok {
		return nil, nil, ErrNotFound
	}

	if opts != nil {
		if opts.IfMatch != "" && obj.etag != opts.IfMatch {
			return nil, nil, ErrPrecondition
		}
		if opts.IfNoneMatch != "" && obj.etag == opts.IfNoneMatch {
			return nil, nil, ErrPrecondition
		}
	}

	info := &ObjectInfo{
		Key:          key,
		Size:         obj.size,
		ETag:         obj.etag,
		LastModified: obj.lastModified,
		ContentType:  obj.contentType,
	}

	data := obj.data
	if opts != nil && opts.Range != nil {
		size := int64(len(data))
		start := opts.Range.Start
		end := opts.Range.End
		if start < 0 {
			start = 0
		}
		if end < start {
			return io.NopCloser(bytes.NewReader(nil)), info, nil
		}
		if start >= size {
			return io.NopCloser(bytes.NewReader(nil)), info, nil
		}
		if end >= size {
			end = size - 1
		}
		data = data[start : end+1]
	}

	return io.NopCloser(bytes.NewReader(data)), info, nil
}

func (s *MemoryStore) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, ok := s.objects[key]
	if !ok {
		return nil, ErrNotFound
	}

	return &ObjectInfo{
		Key:          key,
		Size:         obj.size,
		ETag:         obj.etag,
		LastModified: obj.lastModified,
		ContentType:  obj.contentType,
	}, nil
}

func (s *MemoryStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.putInternal(key, body, size, opts)
}

func (s *MemoryStore) putInternal(key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error) {
	var buf bytes.Buffer
	hash := sha256.New()
	tee := io.TeeReader(body, hash)
	if _, err := io.Copy(&buf, tee); err != nil {
		return nil, err
	}

	checksum := base64.StdEncoding.EncodeToString(hash.Sum(nil))

	if opts != nil && opts.Checksum != "" {
		if checksum != opts.Checksum {
			return nil, fmt.Errorf("%w: checksum mismatch: expected %s, got %s", ErrChecksumFailed, opts.Checksum, checksum)
		}
	}

	etag := fmt.Sprintf("%x", hash.Sum(nil)[:16])

	obj := &memoryObject{
		data:         buf.Bytes(),
		size:         int64(buf.Len()),
		etag:         etag,
		lastModified: time.Now(),
		checksum:     checksum,
	}
	if opts != nil && opts.ContentType != "" {
		obj.contentType = opts.ContentType
	}

	s.objects[key] = obj

	return &ObjectInfo{
		Key:          key,
		Size:         obj.size,
		ETag:         obj.etag,
		LastModified: obj.lastModified,
		ContentType:  obj.contentType,
	}, nil
}

func (s *MemoryStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.objects[key]; ok {
		return nil, ErrAlreadyExists
	}

	return s.putInternal(key, body, size, opts)
}

func (s *MemoryStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *PutOptions) (*ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	obj, ok := s.objects[key]
	if !ok {
		return nil, ErrNotFound
	}

	if obj.etag != etag {
		return nil, ErrPrecondition
	}

	return s.putInternal(key, body, size, opts)
}

func (s *MemoryStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.objects, key)
	return nil
}

func (s *MemoryStore) List(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prefix := ""
	marker := ""
	maxKeys := 1000

	if opts != nil {
		prefix = opts.Prefix
		marker = opts.Marker
		if opts.MaxKeys > 0 {
			maxKeys = opts.MaxKeys
		}
	}

	var keys []string
	for k := range s.objects {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		if marker != "" && k <= marker {
			continue
		}
		keys = append(keys, k)
	}

	sort.Strings(keys)

	result := &ListResult{}
	for i, key := range keys {
		if i >= maxKeys {
			result.IsTruncated = true
			result.NextMarker = keys[i-1]
			break
		}

		obj := s.objects[key]
		result.Objects = append(result.Objects, ObjectInfo{
			Key:          key,
			Size:         obj.size,
			ETag:         obj.etag,
			LastModified: obj.lastModified,
			ContentType:  obj.contentType,
		})
	}

	return result, nil
}

func (s *MemoryStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects = make(map[string]*memoryObject)
}
