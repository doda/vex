package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type FSStore struct {
	root string
	mu   sync.RWMutex
}

type fsMeta struct {
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
	ContentType  string    `json:"content_type"`
	Checksum     string    `json:"checksum,omitempty"`
}

func NewFSStore(root string) (*FSStore, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}
	return &FSStore{root: root}, nil
}

func (s *FSStore) objectPath(key string) string {
	return filepath.Join(s.root, "objects", key)
}

func (s *FSStore) metaPath(key string) string {
	return filepath.Join(s.root, "meta", key+".json")
}

func (s *FSStore) Get(ctx context.Context, key string, opts *GetOptions) (io.ReadCloser, *ObjectInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, err := s.readMeta(key)
	if err != nil {
		return nil, nil, err
	}

	if opts != nil {
		if opts.IfMatch != "" && meta.ETag != opts.IfMatch {
			return nil, nil, ErrPrecondition
		}
		if opts.IfNoneMatch != "" && meta.ETag == opts.IfNoneMatch {
			return nil, nil, ErrPrecondition
		}
	}

	objPath := s.objectPath(key)
	file, err := os.Open(objPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}

	info := &ObjectInfo{
		Key:          key,
		Size:         meta.Size,
		ETag:         meta.ETag,
		LastModified: meta.LastModified,
		ContentType:  meta.ContentType,
	}

	if opts != nil && opts.Range != nil {
		if _, err := file.Seek(opts.Range.Start, io.SeekStart); err != nil {
			file.Close()
			return nil, nil, err
		}
		rangeSize := opts.Range.End - opts.Range.Start + 1
		return &limitedReadCloser{
			rc:    file,
			limit: rangeSize,
		}, info, nil
	}

	return file, info, nil
}

type limitedReadCloser struct {
	rc    io.ReadCloser
	limit int64
	read  int64
}

func (l *limitedReadCloser) Read(p []byte) (int, error) {
	if l.read >= l.limit {
		return 0, io.EOF
	}
	remaining := l.limit - l.read
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := l.rc.Read(p)
	l.read += int64(n)
	return n, err
}

func (l *limitedReadCloser) Close() error {
	return l.rc.Close()
}

func (s *FSStore) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, err := s.readMeta(key)
	if err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Key:          key,
		Size:         meta.Size,
		ETag:         meta.ETag,
		LastModified: meta.LastModified,
		ContentType:  meta.ContentType,
	}, nil
}

func (s *FSStore) Put(ctx context.Context, key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.putInternal(key, body, size, opts)
}

func (s *FSStore) putInternal(key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error) {
	objPath := s.objectPath(key)
	metaPath := s.metaPath(key)

	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
		return nil, err
	}

	// Read body and compute hash for ETag
	var buf bytes.Buffer
	hash := sha256.New()
	tee := io.TeeReader(body, hash)
	if _, err := io.Copy(&buf, tee); err != nil {
		return nil, err
	}

	checksum := base64.StdEncoding.EncodeToString(hash.Sum(nil))

	// Verify checksum if provided
	if opts != nil && opts.Checksum != "" {
		if checksum != opts.Checksum {
			return nil, fmt.Errorf("%w: checksum mismatch: expected %s, got %s", ErrChecksumFailed, opts.Checksum, checksum)
		}
	}

	// Generate ETag from SHA-256 (first 16 bytes as hex)
	etag := fmt.Sprintf("%x", hash.Sum(nil)[:16])

	// Write object file
	if err := os.WriteFile(objPath, buf.Bytes(), 0644); err != nil {
		return nil, err
	}

	// Write metadata
	meta := fsMeta{
		Size:         int64(buf.Len()),
		ETag:         etag,
		LastModified: time.Now(),
		Checksum:     checksum,
	}
	if opts != nil && opts.ContentType != "" {
		meta.ContentType = opts.ContentType
	}

	metaData, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(metaPath, metaData, 0644); err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Key:          key,
		Size:         meta.Size,
		ETag:         meta.ETag,
		LastModified: meta.LastModified,
		ContentType:  meta.ContentType,
	}, nil
}

func (s *FSStore) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if object exists
	if _, err := s.readMeta(key); err == nil {
		return nil, ErrAlreadyExists
	} else if err != ErrNotFound {
		return nil, err
	}

	return s.putInternal(key, body, size, opts)
}

func (s *FSStore) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *PutOptions) (*ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if object exists with matching ETag
	meta, err := s.readMeta(key)
	if err != nil {
		return nil, err
	}

	if meta.ETag != etag {
		return nil, ErrPrecondition
	}

	return s.putInternal(key, body, size, opts)
}

func (s *FSStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	objPath := s.objectPath(key)
	metaPath := s.metaPath(key)

	// Delete object file (ignore if not exists)
	if err := os.Remove(objPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Delete metadata file (ignore if not exists)
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (s *FSStore) List(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	metaDir := filepath.Join(s.root, "meta")
	if _, err := os.Stat(metaDir); os.IsNotExist(err) {
		return &ListResult{}, nil
	}

	var keys []string
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

	err := filepath.Walk(metaDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".json") {
			return nil
		}

		rel, err := filepath.Rel(metaDir, path)
		if err != nil {
			return err
		}
		key := strings.TrimSuffix(rel, ".json")

		// Apply prefix filter
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		// Apply marker filter
		if marker != "" && key <= marker {
			return nil
		}

		keys = append(keys, key)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Sort keys
	sort.Strings(keys)

	result := &ListResult{}
	for i, key := range keys {
		if i >= maxKeys {
			result.IsTruncated = true
			result.NextMarker = keys[i-1]
			break
		}

		meta, err := s.readMeta(key)
		if err != nil {
			continue
		}

		result.Objects = append(result.Objects, ObjectInfo{
			Key:          key,
			Size:         meta.Size,
			ETag:         meta.ETag,
			LastModified: meta.LastModified,
			ContentType:  meta.ContentType,
		})
	}

	return result, nil
}

func (s *FSStore) readMeta(key string) (*fsMeta, error) {
	metaPath := s.metaPath(key)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	var meta fsMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

func (s *FSStore) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.RemoveAll(s.root); err != nil {
		return err
	}
	return os.MkdirAll(s.root, 0755)
}
