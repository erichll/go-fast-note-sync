package hash

import (
	"os"
	"path/filepath"
	"testing"
)

func TestContent(t *testing.T) {
	h := Content([]byte("hello"))
	want := "99162322"
	if h != want {
		t.Errorf("Content(hello): want %s, got %s", want, h)
	}
}

func TestContentEmpty(t *testing.T) {
	h := Content([]byte{})
	want := "0"
	if h != want {
		t.Errorf("Content(empty): want %s, got %s", want, h)
	}
}

func TestTextUsesJavaScriptUTF16CodeUnits(t *testing.T) {
	if got, want := Text("A😀"), "1835364"; got != want {
		t.Errorf("Text(A😀): want %s, got %s", want, got)
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
	if h1 != Text("notes/foo.md") {
		t.Errorf("Path should use text hashing: got %s", h1)
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

func TestTextFileUsesUTF16Hash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "unicode.md")
	if err := os.WriteFile(path, []byte("A😀"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, _, _, err := TextFile(path)
	if err != nil {
		t.Fatalf("TextFile: %v", err)
	}
	if want := Text("A😀"); got != want {
		t.Errorf("TextFile: want %s, got %s", want, got)
	}
	if got == Content([]byte("A😀")) {
		t.Error("text and byte hashes should differ for non-ASCII content")
	}
}

func TestFileContentSamplesLargeData(t *testing.T) {
	data := make([]byte, fullFileHashLimit+1)
	for index := range data {
		data[index] = byte(index)
	}
	middle := len(data)/2 - fileHashSample/2
	sample := make([]byte, 0, fileHashSample*3)
	sample = append(sample, data[:fileHashSample]...)
	sample = append(sample, data[middle:middle+fileHashSample]...)
	sample = append(sample, data[len(data)-fileHashSample:]...)
	if got, want := FileContent(data), Content(sample); got != want {
		t.Errorf("FileContent(large): want %s, got %s", want, got)
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
