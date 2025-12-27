package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Config struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
	UseSSL    bool
}

type S3Store struct {
	client *minio.Client
	bucket string
}

func NewS3Store(cfg S3Config) (*S3Store, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	return &S3Store{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

func (s *S3Store) Get(ctx context.Context, key string, opts *GetOptions) (io.ReadCloser, *ObjectInfo, error) {
	getOpts := minio.GetObjectOptions{}

	if opts != nil {
		if opts.IfMatch != "" {
			getOpts.SetMatchETag(opts.IfMatch)
		}
		if opts.IfNoneMatch != "" {
			getOpts.SetMatchETagExcept(opts.IfNoneMatch)
		}
		if opts.Range != nil {
			if err := getOpts.SetRange(opts.Range.Start, opts.Range.End); err != nil {
				return nil, nil, fmt.Errorf("invalid range: %w", err)
			}
		}
	}

	obj, err := s.client.GetObject(ctx, s.bucket, key, getOpts)
	if err != nil {
		return nil, nil, s.mapError(err)
	}

	stat, err := obj.Stat()
	if err != nil {
		obj.Close()
		return nil, nil, s.mapError(err)
	}

	info := &ObjectInfo{
		Key:          key,
		Size:         stat.Size,
		ETag:         strings.Trim(stat.ETag, "\""),
		LastModified: stat.LastModified,
		ContentType:  stat.ContentType,
	}

	return obj, info, nil
}

func (s *S3Store) Head(ctx context.Context, key string) (*ObjectInfo, error) {
	stat, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, s.mapError(err)
	}

	return &ObjectInfo{
		Key:          key,
		Size:         stat.Size,
		ETag:         strings.Trim(stat.ETag, "\""),
		LastModified: stat.LastModified,
		ContentType:  stat.ContentType,
	}, nil
}

func (s *S3Store) Put(ctx context.Context, key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error) {
	putOpts := minio.PutObjectOptions{}

	if opts != nil {
		if opts.ContentType != "" {
			putOpts.ContentType = opts.ContentType
		}
		if opts.Checksum != "" {
			checksumBytes, err := base64.StdEncoding.DecodeString(opts.Checksum)
			if err == nil && len(checksumBytes) == 32 {
				putOpts.UserMetadata = map[string]string{
					"X-Amz-Checksum-SHA256": opts.Checksum,
				}
			}
		}
	}

	// Compute SHA-256 if checksum verification is needed
	var buf bytes.Buffer
	if opts != nil && opts.Checksum != "" {
		hash := sha256.New()
		tee := io.TeeReader(body, hash)
		if _, err := io.Copy(&buf, tee); err != nil {
			return nil, fmt.Errorf("failed to compute checksum: %w", err)
		}
		computed := base64.StdEncoding.EncodeToString(hash.Sum(nil))
		if computed != opts.Checksum {
			return nil, fmt.Errorf("%w: checksum mismatch: expected %s, got %s", ErrChecksumFailed, opts.Checksum, computed)
		}
		body = bytes.NewReader(buf.Bytes())
		size = int64(buf.Len())
	}

	info, err := s.client.PutObject(ctx, s.bucket, key, body, size, putOpts)
	if err != nil {
		return nil, s.mapError(err)
	}

	return &ObjectInfo{
		Key:          key,
		Size:         info.Size,
		ETag:         strings.Trim(info.ETag, "\""),
		LastModified: info.LastModified,
	}, nil
}

func (s *S3Store) PutIfAbsent(ctx context.Context, key string, body io.Reader, size int64, opts *PutOptions) (*ObjectInfo, error) {
	// Check if object exists
	_, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err == nil {
		// Object already exists
		return nil, ErrAlreadyExists
	}
	errResp := minio.ToErrorResponse(err)
	if errResp.Code != "NoSuchKey" {
		return nil, s.mapError(err)
	}

	// Object doesn't exist, try to put it
	// Note: MinIO doesn't support atomic If-None-Match, so there's a race window
	// For production use with S3, you'd use CopyObject with a conditional header
	return s.Put(ctx, key, body, size, opts)
}

func (s *S3Store) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *PutOptions) (*ObjectInfo, error) {
	// Check if object exists with matching ETag
	stat, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, s.mapError(err)
	}

	currentETag := strings.Trim(stat.ETag, "\"")
	if currentETag != etag {
		return nil, ErrPrecondition
	}

	// ETag matches, proceed with put
	return s.Put(ctx, key, body, size, opts)
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return s.mapError(err)
	}
	return nil
}

func (s *S3Store) List(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	listOpts := minio.ListObjectsOptions{}

	if opts != nil {
		listOpts.Prefix = opts.Prefix
		listOpts.StartAfter = opts.Marker
		if opts.MaxKeys > 0 {
			listOpts.MaxKeys = opts.MaxKeys
		}
	}

	result := &ListResult{}
	objCh := s.client.ListObjects(ctx, s.bucket, listOpts)

	maxKeys := 1000
	if opts != nil && opts.MaxKeys > 0 {
		maxKeys = opts.MaxKeys
	}

	for obj := range objCh {
		if obj.Err != nil {
			return nil, s.mapError(obj.Err)
		}

		result.Objects = append(result.Objects, ObjectInfo{
			Key:          obj.Key,
			Size:         obj.Size,
			ETag:         strings.Trim(obj.ETag, "\""),
			LastModified: obj.LastModified,
			ContentType:  obj.ContentType,
		})

		if len(result.Objects) >= maxKeys {
			result.IsTruncated = true
			result.NextMarker = obj.Key
			break
		}
	}

	return result, nil
}

func (s *S3Store) EnsureBucket(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket: %w", err)
	}
	if !exists {
		err = s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}
	return nil
}

func (s *S3Store) mapError(err error) error {
	if err == nil {
		return nil
	}

	errResp := minio.ToErrorResponse(err)
	switch errResp.Code {
	case "NoSuchKey":
		return ErrNotFound
	case "PreconditionFailed":
		return ErrPrecondition
	}

	// Check for HTTP status code from the error response
	if errResp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if errResp.StatusCode == http.StatusPreconditionFailed {
		return ErrPrecondition
	}
	if errResp.StatusCode == http.StatusConflict {
		return ErrAlreadyExists
	}

	return err
}
