package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMemoryStore(t *testing.T) {
	store := NewMemoryStore()
	runStoreTests(t, store)
}

func TestFSStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vex-fs-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewFSStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create fs store: %v", err)
	}

	runStoreTests(t, store)
}

func runStoreTests(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("basic CRUD", func(t *testing.T) {
		testBasicCRUD(t, ctx, store)
	})

	t.Run("conditional writes", func(t *testing.T) {
		testConditionalWrites(t, ctx, store)
	})

	t.Run("list operations", func(t *testing.T) {
		testListOperations(t, ctx, store)
	})

	t.Run("checksum verification", func(t *testing.T) {
		testChecksumVerification(t, ctx, store)
	})

	t.Run("read-after-write", func(t *testing.T) {
		testReadAfterWrite(t, ctx, store)
	})

	t.Run("range reads", func(t *testing.T) {
		testRangeReads(t, ctx, store)
	})
}

func testBasicCRUD(t *testing.T, ctx context.Context, store Store) {
	key := "test/basic/object.txt"
	content := []byte("hello world")

	// Put
	info, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if info.Size != int64(len(content)) {
		t.Errorf("size mismatch: got %d, want %d", info.Size, len(content))
	}
	if info.ETag == "" {
		t.Error("ETag should not be empty")
	}

	// Head
	headInfo, err := store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if headInfo.Size != info.Size {
		t.Errorf("head size mismatch: got %d, want %d", headInfo.Size, info.Size)
	}
	if headInfo.ETag != info.ETag {
		t.Errorf("head etag mismatch: got %s, want %s", headInfo.ETag, info.ETag)
	}

	// Get
	rc, getInfo, err := store.Get(ctx, key, nil)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", data, content)
	}
	if getInfo.ETag != info.ETag {
		t.Errorf("get etag mismatch: got %s, want %s", getInfo.ETag, info.ETag)
	}

	// Delete
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	_, err = store.Head(ctx, key)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func testConditionalWrites(t *testing.T, ctx context.Context, store Store) {
	t.Run("PutIfAbsent", func(t *testing.T) {
		key := "test/conditional/if-absent.txt"
		content := []byte("first")

		// First put should succeed
		_, err := store.PutIfAbsent(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("first PutIfAbsent failed: %v", err)
		}

		// Second put should fail with ErrAlreadyExists
		_, err = store.PutIfAbsent(ctx, key, bytes.NewReader([]byte("second")), 6, nil)
		if err != ErrAlreadyExists {
			t.Errorf("expected ErrAlreadyExists, got %v", err)
		}

		// Verify HTTP status mapping for 409
		status := ErrorToHTTPStatus(err)
		if status != http.StatusConflict {
			t.Errorf("expected status 409, got %d", status)
		}

		// Verify original content unchanged
		rc, _, err := store.Get(ctx, key, nil)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		data, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(data, content) {
			t.Errorf("content changed unexpectedly: got %q, want %q", data, content)
		}

		store.Delete(ctx, key)
	})

	t.Run("PutIfMatch", func(t *testing.T) {
		key := "test/conditional/if-match.txt"
		content := []byte("version 1")

		// Create initial object
		info, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("initial Put failed: %v", err)
		}
		originalETag := info.ETag

		// Update with correct ETag should succeed
		newContent := []byte("version 2")
		info2, err := store.PutIfMatch(ctx, key, bytes.NewReader(newContent), int64(len(newContent)), originalETag, nil)
		if err != nil {
			t.Fatalf("PutIfMatch with correct ETag failed: %v", err)
		}
		if info2.ETag == originalETag {
			t.Error("ETag should change after update")
		}

		// Update with wrong ETag should fail with ErrPrecondition (412)
		_, err = store.PutIfMatch(ctx, key, bytes.NewReader([]byte("version 3")), 9, originalETag, nil)
		if err != ErrPrecondition {
			t.Errorf("expected ErrPrecondition, got %v", err)
		}

		// Verify HTTP status mapping for 412
		status := ErrorToHTTPStatus(err)
		if status != http.StatusPreconditionFailed {
			t.Errorf("expected status 412, got %d", status)
		}

		// PutIfMatch on non-existent key should fail with ErrNotFound
		_, err = store.PutIfMatch(ctx, key+"nonexistent", bytes.NewReader([]byte("test")), 4, "someetag", nil)
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound for nonexistent key, got %v", err)
		}

		store.Delete(ctx, key)
	})

	t.Run("GetWithIfMatch", func(t *testing.T) {
		key := "test/conditional/get-if-match.txt"
		content := []byte("test content")

		info, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Get with matching ETag should succeed
		rc, _, err := store.Get(ctx, key, &GetOptions{IfMatch: info.ETag})
		if err != nil {
			t.Fatalf("Get with matching IfMatch failed: %v", err)
		}
		rc.Close()

		// Get with non-matching ETag should fail
		_, _, err = store.Get(ctx, key, &GetOptions{IfMatch: "wrongetag"})
		if err != ErrPrecondition {
			t.Errorf("expected ErrPrecondition for wrong IfMatch, got %v", err)
		}

		// Get with IfNoneMatch matching should fail
		_, _, err = store.Get(ctx, key, &GetOptions{IfNoneMatch: info.ETag})
		if err != ErrPrecondition {
			t.Errorf("expected ErrPrecondition for matching IfNoneMatch, got %v", err)
		}

		// Get with IfNoneMatch non-matching should succeed
		rc, _, err = store.Get(ctx, key, &GetOptions{IfNoneMatch: "otheretag"})
		if err != nil {
			t.Fatalf("Get with non-matching IfNoneMatch failed: %v", err)
		}
		rc.Close()

		store.Delete(ctx, key)
	})
}

func testListOperations(t *testing.T, ctx context.Context, store Store) {
	// Create test objects
	keys := []string{
		"list/a/1.txt",
		"list/a/2.txt",
		"list/b/1.txt",
		"list/c/1.txt",
	}

	for _, key := range keys {
		content := []byte("content for " + key)
		_, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
		if err != nil {
			t.Fatalf("Put failed for %s: %v", key, err)
		}
	}

	t.Run("list all", func(t *testing.T) {
		result, err := store.List(ctx, &ListOptions{Prefix: "list/"})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(result.Objects) != 4 {
			t.Errorf("expected 4 objects, got %d", len(result.Objects))
		}
	})

	t.Run("list with prefix", func(t *testing.T) {
		result, err := store.List(ctx, &ListOptions{Prefix: "list/a/"})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(result.Objects) != 2 {
			t.Errorf("expected 2 objects, got %d", len(result.Objects))
		}
	})

	t.Run("list with pagination", func(t *testing.T) {
		result, err := store.List(ctx, &ListOptions{Prefix: "list/", MaxKeys: 2})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(result.Objects) != 2 {
			t.Errorf("expected 2 objects, got %d", len(result.Objects))
		}
		if !result.IsTruncated {
			t.Error("expected IsTruncated to be true")
		}

		// Get next page
		result2, err := store.List(ctx, &ListOptions{Prefix: "list/", MaxKeys: 2, Marker: result.NextMarker})
		if err != nil {
			t.Fatalf("List page 2 failed: %v", err)
		}
		if len(result2.Objects) != 2 {
			t.Errorf("expected 2 objects in page 2, got %d", len(result2.Objects))
		}
	})

	// Cleanup
	for _, key := range keys {
		store.Delete(ctx, key)
	}
}

func testChecksumVerification(t *testing.T, ctx context.Context, store Store) {
	key := "test/checksum/object.txt"
	content := []byte("test content for checksum verification")

	// Compute SHA-256
	hash := sha256.Sum256(content)
	validChecksum := base64.StdEncoding.EncodeToString(hash[:])

	t.Run("valid checksum", func(t *testing.T) {
		_, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), &PutOptions{
			Checksum: validChecksum,
		})
		if err != nil {
			t.Fatalf("Put with valid checksum failed: %v", err)
		}

		// Verify content
		rc, _, err := store.Get(ctx, key, nil)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		data, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(data, content) {
			t.Error("content mismatch after checksum-verified put")
		}

		store.Delete(ctx, key)
	})

	t.Run("invalid checksum", func(t *testing.T) {
		invalidChecksum := base64.StdEncoding.EncodeToString(make([]byte, 32)) // all zeros

		_, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), &PutOptions{
			Checksum: invalidChecksum,
		})
		if err == nil {
			t.Error("expected error for invalid checksum")
			store.Delete(ctx, key)
		} else if !strings.Contains(err.Error(), "checksum mismatch") {
			t.Errorf("expected checksum mismatch error, got: %v", err)
		}
	})
}

func testReadAfterWrite(t *testing.T, ctx context.Context, store Store) {
	key := "test/raw/object.txt"
	content := []byte("read after write test")

	// Write
	info, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Immediately read
	rc, getInfo, err := store.Get(ctx, key, nil)
	if err != nil {
		t.Fatalf("Get immediately after Put failed: %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	// Verify content matches
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", data, content)
	}

	// Verify ETag matches
	if getInfo.ETag != info.ETag {
		t.Errorf("ETag mismatch in read-after-write: put=%s, get=%s", info.ETag, getInfo.ETag)
	}

	store.Delete(ctx, key)
}

func testRangeReads(t *testing.T, ctx context.Context, store Store) {
	key := "test/range/object.txt"
	content := []byte("0123456789ABCDEF")

	_, err := store.Put(ctx, key, bytes.NewReader(content), int64(len(content)), nil)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	t.Run("partial range", func(t *testing.T) {
		rc, _, err := store.Get(ctx, key, &GetOptions{
			Range: &ByteRange{Start: 5, End: 9},
		})
		if err != nil {
			t.Fatalf("Get with range failed: %v", err)
		}
		data, _ := io.ReadAll(rc)
		rc.Close()

		expected := "56789"
		if string(data) != expected {
			t.Errorf("range read mismatch: got %q, want %q", data, expected)
		}
	})

	t.Run("range from start", func(t *testing.T) {
		rc, _, err := store.Get(ctx, key, &GetOptions{
			Range: &ByteRange{Start: 0, End: 4},
		})
		if err != nil {
			t.Fatalf("Get with range failed: %v", err)
		}
		data, _ := io.ReadAll(rc)
		rc.Close()

		expected := "01234"
		if string(data) != expected {
			t.Errorf("range read mismatch: got %q, want %q", data, expected)
		}
	})

	store.Delete(ctx, key)
}

func TestErrorHelpers(t *testing.T) {
	t.Run("IsConflictError", func(t *testing.T) {
		if !IsConflictError(ErrAlreadyExists) {
			t.Error("IsConflictError should return true for ErrAlreadyExists")
		}
		if IsConflictError(ErrNotFound) {
			t.Error("IsConflictError should return false for ErrNotFound")
		}
	})

	t.Run("IsPreconditionError", func(t *testing.T) {
		if !IsPreconditionError(ErrPrecondition) {
			t.Error("IsPreconditionError should return true for ErrPrecondition")
		}
		if IsPreconditionError(ErrNotFound) {
			t.Error("IsPreconditionError should return false for ErrNotFound")
		}
	})

	t.Run("IsNotFoundError", func(t *testing.T) {
		if !IsNotFoundError(ErrNotFound) {
			t.Error("IsNotFoundError should return true for ErrNotFound")
		}
		if IsNotFoundError(ErrPrecondition) {
			t.Error("IsNotFoundError should return false for ErrPrecondition")
		}
	})

	t.Run("ErrorToHTTPStatus", func(t *testing.T) {
		cases := []struct {
			err    error
			status int
		}{
			{nil, http.StatusOK},
			{ErrNotFound, http.StatusNotFound},
			{ErrPrecondition, http.StatusPreconditionFailed},
			{ErrAlreadyExists, http.StatusConflict},
		}
		for _, tc := range cases {
			got := ErrorToHTTPStatus(tc.err)
			if got != tc.status {
				t.Errorf("ErrorToHTTPStatus(%v) = %d, want %d", tc.err, got, tc.status)
			}
		}
	})
}

func TestFSStoreCreation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vex-fs-creation-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store in non-existent subdirectory
	subDir := filepath.Join(tmpDir, "subdir", "nested")
	store, err := NewFSStore(subDir)
	if err != nil {
		t.Fatalf("NewFSStore failed: %v", err)
	}

	// Should be able to write
	_, err = store.Put(context.Background(), "test.txt", bytes.NewReader([]byte("test")), 4, nil)
	if err != nil {
		t.Errorf("Put after creation failed: %v", err)
	}
}

func TestFSStoreClear(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vex-fs-clear-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewFSStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFSStore failed: %v", err)
	}

	ctx := context.Background()

	// Add some objects
	store.Put(ctx, "a.txt", bytes.NewReader([]byte("a")), 1, nil)
	store.Put(ctx, "b.txt", bytes.NewReader([]byte("b")), 1, nil)

	// Clear
	if err := store.Clear(); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	// Verify empty
	result, err := store.List(ctx, nil)
	if err != nil {
		t.Fatalf("List after clear failed: %v", err)
	}
	if len(result.Objects) != 0 {
		t.Errorf("expected 0 objects after clear, got %d", len(result.Objects))
	}
}

func TestMemoryStoreClear(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Add some objects
	store.Put(ctx, "a.txt", bytes.NewReader([]byte("a")), 1, nil)
	store.Put(ctx, "b.txt", bytes.NewReader([]byte("b")), 1, nil)

	// Clear
	store.Clear()

	// Verify empty
	result, err := store.List(ctx, nil)
	if err != nil {
		t.Fatalf("List after clear failed: %v", err)
	}
	if len(result.Objects) != 0 {
		t.Errorf("expected 0 objects after clear, got %d", len(result.Objects))
	}
}

func TestNewStoreFactory(t *testing.T) {
	t.Run("memory store", func(t *testing.T) {
		store, err := New(Config{Type: "memory"})
		if err != nil {
			t.Fatalf("New memory store failed: %v", err)
		}
		if store == nil {
			t.Error("expected non-nil store")
		}
		// Verify it works
		_, err = store.Put(context.Background(), "test.txt", bytes.NewReader([]byte("test")), 4, nil)
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}
	})

	t.Run("filesystem store", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "vex-factory-test")
		defer os.RemoveAll(tmpDir)

		store, err := New(Config{Type: "fs", RootPath: tmpDir})
		if err != nil {
			t.Fatalf("New fs store failed: %v", err)
		}
		if store == nil {
			t.Error("expected non-nil store")
		}
	})

	t.Run("unsupported type", func(t *testing.T) {
		_, err := New(Config{Type: "unknown"})
		if err == nil {
			t.Error("expected error for unsupported type")
		}
	})
}
