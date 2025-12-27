package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"os"
	"testing"
)

func TestS3Store(t *testing.T) {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("MINIO_ENDPOINT not set, skipping S3 tests")
	}

	cfg := S3Config{
		Endpoint:  endpoint,
		Bucket:    os.Getenv("MINIO_BUCKET"),
		AccessKey: os.Getenv("MINIO_ACCESS_KEY"),
		SecretKey: os.Getenv("MINIO_SECRET_KEY"),
		Region:    "us-east-1",
		UseSSL:    false,
	}

	if cfg.Bucket == "" {
		cfg.Bucket = "vex-test"
	}
	if cfg.AccessKey == "" {
		cfg.AccessKey = "minioadmin"
	}
	if cfg.SecretKey == "" {
		cfg.SecretKey = "minioadmin"
	}

	store, err := NewS3Store(cfg)
	if err != nil {
		t.Fatalf("failed to create S3 store: %v", err)
	}

	ctx := context.Background()

	// Ensure bucket exists
	if err := store.EnsureBucket(ctx); err != nil {
		t.Fatalf("failed to ensure bucket: %v", err)
	}

	t.Run("basic CRUD", func(t *testing.T) {
		key := "test/s3/basic.txt"
		content := []byte("hello s3")

		// Put
		info, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if info.ETag == "" {
			t.Error("ETag should not be empty")
		}

		// Head
		headInfo, err := store.Head(ctx, key)
		if err != nil {
			t.Fatalf("Head failed: %v", err)
		}
		if headInfo.Size != int64(len(content)) {
			t.Errorf("size mismatch: got %d, want %d", headInfo.Size, len(content))
		}

		// Get
		rc, _, err := store.Get(ctx, key, nil)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		data, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(data, content) {
			t.Errorf("content mismatch: got %q, want %q", data, content)
		}

		// Delete
		if err := store.Delete(ctx, key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify deleted
		_, err = store.Head(ctx, key)
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("PutIfAbsent", func(t *testing.T) {
		key := "test/s3/if-absent.txt"
		content := []byte("first")

		// First put should succeed
		_, err := store.PutIfAbsent(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("first PutIfAbsent failed: %v", err)
		}

		// Second put should fail
		_, err = store.PutIfAbsent(ctx, key, bytes.NewReader([]byte("second")), 6, nil)
		if err != ErrAlreadyExists {
			t.Errorf("expected ErrAlreadyExists, got %v", err)
		}

		store.Delete(ctx, key)
	})

	t.Run("PutIfMatch", func(t *testing.T) {
		key := "test/s3/if-match.txt"
		content := []byte("version 1")

		info, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("initial Put failed: %v", err)
		}

		// Update with correct ETag
		newContent := []byte("version 2")
		_, err = store.PutIfMatch(ctx, key, bytes.NewReader(newContent), int64(len(newContent)), info.ETag, nil)
		if err != nil {
			t.Fatalf("PutIfMatch with correct ETag failed: %v", err)
		}

		// Update with wrong ETag should fail
		_, err = store.PutIfMatch(ctx, key, bytes.NewReader([]byte("v3")), 2, info.ETag, nil)
		if err != ErrPrecondition {
			t.Errorf("expected ErrPrecondition, got %v", err)
		}

		store.Delete(ctx, key)
	})

	t.Run("checksum verification", func(t *testing.T) {
		key := "test/s3/checksum.txt"
		content := []byte("checksum content")
		hash := sha256.Sum256(content)
		checksum := base64.StdEncoding.EncodeToString(hash[:])

		// Valid checksum
		_, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), &PutOptions{
			Checksum: checksum,
		})
		if err != nil {
			t.Fatalf("Put with valid checksum failed: %v", err)
		}

		store.Delete(ctx, key)

		// Invalid checksum
		invalidChecksum := base64.StdEncoding.EncodeToString(make([]byte, 32))
		_, err = store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), &PutOptions{
			Checksum: invalidChecksum,
		})
		if err == nil {
			t.Error("expected error for invalid checksum")
			store.Delete(ctx, key)
		}
	})

	t.Run("list operations", func(t *testing.T) {
		keys := []string{
			"test/s3/list/a.txt",
			"test/s3/list/b.txt",
			"test/s3/list/c.txt",
		}

		for _, key := range keys {
			_, err := store.Put(ctx, key, bytes.NewReader([]byte("test")), 4, nil)
			if err != nil {
				t.Fatalf("Put failed for %s: %v", key, err)
			}
		}

		result, err := store.List(ctx, &ListOptions{Prefix: "test/s3/list/"})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(result.Objects) != 3 {
			t.Errorf("expected 3 objects, got %d", len(result.Objects))
		}

		for _, key := range keys {
			store.Delete(ctx, key)
		}
	})

	t.Run("read-after-write", func(t *testing.T) {
		key := "test/s3/raw.txt"
		content := []byte("read after write")

		info, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Immediately read
		rc, getInfo, err := store.Get(ctx, key, nil)
		if err != nil {
			t.Fatalf("Get after Put failed: %v", err)
		}
		data, _ := io.ReadAll(rc)
		rc.Close()

		if !bytes.Equal(data, content) {
			t.Errorf("content mismatch")
		}
		if getInfo.ETag != info.ETag {
			t.Errorf("ETag mismatch in read-after-write")
		}

		store.Delete(ctx, key)
	})
}
