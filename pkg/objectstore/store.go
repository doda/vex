package objectstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"
)

var (
	ErrNotFound       = errors.New("object not found")
	ErrPrecondition   = errors.New("precondition failed")
	ErrAlreadyExists  = errors.New("object already exists")
	ErrChecksumFailed = errors.New("checksum verification failed")
)

func ErrorToHTTPStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if errors.Is(err, ErrNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, ErrPrecondition) {
		return http.StatusPreconditionFailed
	}
	if errors.Is(err, ErrAlreadyExists) {
		return http.StatusConflict
	}
	if errors.Is(err, ErrChecksumFailed) {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func IsConflictError(err error) bool {
	return errors.Is(err, ErrAlreadyExists)
}

func IsPreconditionError(err error) bool {
	return errors.Is(err, ErrPrecondition)
}

func IsNotFoundError(err error) bool {
	return errors.Is(err, ErrNotFound)
}

type Config struct {
	Type      string
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
	UseSSL    bool
	RootPath  string // For filesystem store
}

func New(cfg Config) (Store, error) {
	switch cfg.Type {
	case "s3", "minio":
		return NewS3Store(S3Config{
			Endpoint:  cfg.Endpoint,
			Bucket:    cfg.Bucket,
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
			Region:    cfg.Region,
			UseSSL:    cfg.UseSSL,
		})
	case "filesystem", "fs":
		if cfg.RootPath == "" {
			cfg.RootPath = "/tmp/vex-objectstore"
		}
		return NewFSStore(cfg.RootPath)
	case "memory", "mem":
		return NewMemoryStore(), nil
	default:
		return nil, errors.New("unsupported object store type: " + cfg.Type)
	}
}

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
