package hash

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"unicode/utf16"
)

const (
	fullFileHashLimit = 10 * 1024 * 1024
	fileHashSample    = 5 * 1024 * 1024
)

type CacheEntry struct {
	Hash  string
	MTime int64
	Size  int64
}

func Content(data []byte) string {
	var sum int32
	for _, value := range data {
		sum = sum*31 + int32(value)
	}
	return strconv.FormatInt(int64(sum), 10)
}

func FileContent(data []byte) string {
	if len(data) <= fullFileHashLimit {
		return Content(data)
	}
	middle := len(data)/2 - fileHashSample/2
	sample := make([]byte, 0, fileHashSample*3)
	sample = append(sample, data[:fileHashSample]...)
	sample = append(sample, data[middle:middle+fileHashSample]...)
	sample = append(sample, data[len(data)-fileHashSample:]...)
	return Content(sample)
}

func Text(value string) string {
	var sum int32
	for _, codeUnit := range utf16.Encode([]rune(value)) {
		sum = sum*31 + int32(codeUnit)
	}
	return strconv.FormatInt(int64(sum), 10)
}

func Path(path string) string {
	return Text(path)
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

	var data []byte
	if info.Size() <= fullFileHashLimit {
		data, err = io.ReadAll(f)
	} else {
		data, err = readFileSamples(f, info.Size())
	}
	if err != nil {
		return "", 0, 0, fmt.Errorf("hash %s: %w", path, err)
	}

	return Content(data), info.ModTime().UnixMilli(), info.Size(), nil
}

func TextFile(path string) (hash string, mtime int64, size int64, err error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", 0, 0, fmt.Errorf("stat %s: %w", path, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", 0, 0, fmt.Errorf("read %s: %w", path, err)
	}
	return Text(string(data)), info.ModTime().UnixMilli(), info.Size(), nil
}

func readFileSamples(file *os.File, size int64) ([]byte, error) {
	sample := make([]byte, fileHashSample*3)
	middle := size/2 - fileHashSample/2
	offsets := []int64{0, middle, size - fileHashSample}
	for index, offset := range offsets {
		if _, err := file.ReadAt(sample[index*fileHashSample:(index+1)*fileHashSample], offset); err != nil && err != io.EOF {
			return nil, err
		}
	}
	return sample, nil
}

// FileCached returns the hash from cache if mtime and size match; otherwise recomputes.
// Returns (hash, fromCache, error).
func FileCached(path string, entry *CacheEntry) (string, bool, error) {
	return cached(path, entry, File)
}

func TextFileCached(path string, entry *CacheEntry) (string, bool, error) {
	return cached(path, entry, TextFile)
}

func cached(path string, entry *CacheEntry, calculate func(string) (string, int64, int64, error)) (string, bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", false, fmt.Errorf("stat %s: %w", path, err)
	}

	mtime := info.ModTime().UnixMilli()
	size := info.Size()

	if entry != nil && entry.MTime == mtime && entry.Size == size && entry.Hash != "" {
		return entry.Hash, true, nil
	}

	h, _, _, err := calculate(path)
	if err != nil {
		return "", false, err
	}
	return h, false, nil
}
