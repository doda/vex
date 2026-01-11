package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
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
	// Strip http:// or https:// from endpoint if present
	// minio-go expects just the host:port, not a full URL
	endpoint := cfg.Endpoint
	secure := cfg.UseSSL
	if strings.HasPrefix(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
		secure = true
	} else if strings.HasPrefix(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
		secure = false
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: secure,
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

	if opts != nil && opts.Range != nil {
		return obj, &ObjectInfo{Key: key}, nil
	}

	stat, err := obj.Stat()
	if err != nil {
		mapped := s.mapError(err)
		// Some gateways can return 412 on Stat even though the object is readable.
		if errors.Is(mapped, ErrPrecondition) {
			if headInfo, headErr := s.Head(ctx, key); headErr == nil {
				return obj, headInfo, nil
			}
		}
		obj.Close()
		return nil, nil, mapped
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
	reader, size, putOpts, err := s.preparePut(body, size, opts)
	if err != nil {
		return nil, err
	}

	info, err := s.client.PutObject(ctx, s.bucket, key, reader, size, putOpts)
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
	reader, size, putOpts, err := s.preparePut(body, size, opts)
	if err != nil {
		return nil, err
	}
	putOpts.SetMatchETagExcept("*")

	info, err := s.client.PutObject(ctx, s.bucket, key, reader, size, putOpts)
	if err != nil {
		mapped := s.mapError(err)
		if errors.Is(mapped, ErrPrecondition) {
			return nil, ErrAlreadyExists
		}
		return nil, mapped
	}

	return &ObjectInfo{
		Key:          key,
		Size:         info.Size,
		ETag:         strings.Trim(info.ETag, "\""),
		LastModified: info.LastModified,
	}, nil
}

func (s *S3Store) PutIfMatch(ctx context.Context, key string, body io.Reader, size int64, etag string, opts *PutOptions) (*ObjectInfo, error) {
	reader, size, putOpts, err := s.preparePut(body, size, opts)
	if err != nil {
		return nil, err
	}
	putOpts.SetMatchETag(etag)

	info, err := s.client.PutObject(ctx, s.bucket, key, reader, size, putOpts)
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

func (s *S3Store) preparePut(body io.Reader, size int64, opts *PutOptions) (io.Reader, int64, minio.PutObjectOptions, error) {
	putOpts := minio.PutObjectOptions{}

	if opts != nil {
		if opts.ContentType != "" {
			putOpts.ContentType = opts.ContentType
		}
		if opts.IfMatch != "" {
			putOpts.SetMatchETag(opts.IfMatch)
		}
		if opts.IfNoneMatch != "" {
			putOpts.SetMatchETagExcept(opts.IfNoneMatch)
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

	if opts == nil || opts.Checksum == "" {
		return body, size, putOpts, nil
	}

	var buf bytes.Buffer
	hash := sha256.New()
	tee := io.TeeReader(body, hash)
	if _, err := io.Copy(&buf, tee); err != nil {
		return nil, 0, minio.PutObjectOptions{}, fmt.Errorf("failed to compute checksum: %w", err)
	}
	computed := base64.StdEncoding.EncodeToString(hash.Sum(nil))
	if computed != opts.Checksum {
		return nil, 0, minio.PutObjectOptions{}, fmt.Errorf("%w: checksum mismatch: expected %s, got %s", ErrChecksumFailed, opts.Checksum, computed)
	}

	return bytes.NewReader(buf.Bytes()), int64(buf.Len()), putOpts, nil
}
