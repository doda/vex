package objectstore

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrNotFound       = errors.New("object not found")
	ErrPrecondition   = errors.New("precondition failed")
	ErrAlreadyExists  = errors.New("object already exists")
)

type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
}

type ListResult struct {
	Objects     []ObjectInfo
	NextMarker  string
	IsTruncated bool
}

type GetOptions struct {
	IfMatch     string
	IfNoneMatch string
	Range       *ByteRange
}

type ByteRange struct {
	Start int64
	End   int64
}

type PutOptions struct {
	IfMatch     string
	IfNoneMatch string
	ContentType string
	Checksum    string
}

type ListOptions struct {
	Prefix    string
	Marker    string
	MaxKeys   int
	Delimiter string
}

type Store interface {
	Get(ctx context.Context, key string, opts *GetOptions) (io.ReadCloser, *ObjectInfo, error)
	Head(ctx context.Context, key string) (*ObjectInfo, error)
	Put(ctx context.Context, key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error)
	PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error)
	PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *PutOptions) (*ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, opts *ListOptions) (*ListResult, error)
}
