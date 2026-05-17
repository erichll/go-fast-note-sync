package hash

import (
	"os"
	"path/filepath"
	"testing"
)

func TestContent(t *testing.T) {
	h := Content([]byte("hello"))
	// SHA256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
	want := "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if h != want {
		t.Errorf("Content(hello): want %s, got %s", want, h)
	}
}

func TestContentEmpty(t *testing.T) {
	h := Content([]byte{})
	// SHA256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	want := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if h != want {
		t.Errorf("Content(empty): want %s, got %s", want, h)
	}
}

func TestPath(t *testing.T) {
	h1 := Path("notes/foo.md")
	h2 := Path("notes/foo.md")
	h3 := Path("notes/bar.md")
	if h1 != h2 {
		t.Error("same path should produce same hash")
	}
	if h1 == h3 {
		t.Error("different paths should produce different hashes")
	}
	if len(h1) != 64 {
		t.Errorf("expected 64 hex chars, got %d", len(h1))
	}
}

func TestFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	content := []byte("hello world")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	h, mtime, size, err := File(path)
	if err != nil {
		t.Fatalf("File: %v", err)
	}
	if h != Content(content) {
		t.Errorf("hash mismatch: want %s, got %s", Content(content), h)
	}
	if size != int64(len(content)) {
		t.Errorf("size: want %d, got %d", len(content), size)
	}
	if mtime == 0 {
		t.Error("mtime should be non-zero")
	}
}

func TestFileMissing(t *testing.T) {
	_, _, _, err := File("/nonexistent/file.txt")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestFileCachedHit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	content := []byte("cached content")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Get real values first
	h, mtime, size, err := File(path)
	if err != nil {
		t.Fatalf("File: %v", err)
	}

	entry := &CacheEntry{Hash: h, MTime: mtime, Size: size}
	got, fromCache, err := FileCached(path, entry)
	if err != nil {
		t.Fatalf("FileCached: %v", err)
	}
	if !fromCache {
		t.Error("expected cache hit")
	}
	if got != h {
		t.Errorf("hash mismatch: want %s, got %s", h, got)
	}
}

func TestFileCachedMiss(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(path, []byte("original"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Stale cache entry
	stale := &CacheEntry{Hash: "oldhash", MTime: 1, Size: 999}
	got, fromCache, err := FileCached(path, stale)
	if err != nil {
		t.Fatalf("FileCached: %v", err)
	}
	if fromCache {
		t.Error("expected cache miss with stale entry")
	}
	if got == "oldhash" {
		t.Error("should return fresh hash, not stale")
	}
}

func TestFileCachedNilEntry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, fromCache, err := FileCached(path, nil)
	if err != nil {
		t.Fatalf("FileCached: %v", err)
	}
	if fromCache {
		t.Error("nil entry should always be a miss")
	}
	if got != Content([]byte("data")) {
		t.Errorf("unexpected hash: %s", got)
	}
}
