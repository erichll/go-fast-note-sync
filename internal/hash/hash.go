package hash

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

type CacheEntry struct {
	Hash  string
	MTime int64
	Size  int64
}

func Content(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func Path(p string) string {
	return Content([]byte(p))
}

func File(path string) (hash string, mtime int64, size int64, err error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", 0, 0, fmt.Errorf("stat %s: %w", path, err)
	}

	f, err := os.Open(path)
	if err != nil {
		return "", 0, 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", 0, 0, fmt.Errorf("hash %s: %w", path, err)
	}

	return hex.EncodeToString(h.Sum(nil)), info.ModTime().UnixMilli(), info.Size(), nil
}

// FileCached returns the hash from cache if mtime and size match; otherwise recomputes.
// Returns (hash, fromCache, error).
func FileCached(path string, entry *CacheEntry) (string, bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", false, fmt.Errorf("stat %s: %w", path, err)
	}

	mtime := info.ModTime().UnixMilli()
	size := info.Size()

	if entry != nil && entry.MTime == mtime && entry.Size == size && entry.Hash != "" {
		return entry.Hash, true, nil
	}

	h, _, _, err := File(path)
	if err != nil {
		return "", false, err
	}
	return h, false, nil
}
