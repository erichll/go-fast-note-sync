package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	s := New()
	if s.FileHashMap == nil {
		t.Error("FileHashMap should be initialized")
	}
	if s.ConfigHashMap == nil {
		t.Error("ConfigHashMap should be initialized")
	}
	if s.FolderSnapshot == nil {
		t.Error("FolderSnapshot should be initialized")
	}
	if s.PendingNoteModifies == nil {
		t.Error("PendingNoteModifies should be initialized")
	}
	if s.UploadCheckpoints == nil {
		t.Error("UploadCheckpoints should be initialized")
	}
}

func TestDefaultPath(t *testing.T) {
	p := DefaultPath()
	if p == "" {
		t.Error("DefaultPath should not be empty")
	}
	if filepath.Base(p) != "state.json" {
		t.Errorf("expected state.json, got %q", filepath.Base(p))
	}
}

func TestLoadMissing(t *testing.T) {
	s, err := Load("/nonexistent/state.json")
	if err != nil {
		t.Fatalf("expected empty state for missing file, got error: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil state")
	}
	if s.WsCount != 0 {
		t.Error("expected WsCount=0 for fresh state")
	}
}

func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	s := New()
	s.WsCount = 3
	s.IsInitSync = true
	s.FileHashMap["notes/foo.md"] = FileHashEntry{Hash: "abc", MTime: 1000, Size: 42}
	s.PendingNoteModifies["notes/bar.md"] = "def"
	s.UploadCheckpoints["path-hash"] = UploadCheckpoint{
		SessionID:      "session",
		PathHash:       "path-hash",
		ContentHash:    "content-hash",
		LastChunkIndex: 3,
		Timestamp:      123456,
	}

	if err := Save(path, s); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.WsCount != 3 {
		t.Errorf("WsCount: want 3, got %d", loaded.WsCount)
	}
	if !loaded.IsInitSync {
		t.Error("IsInitSync should be true")
	}
	entry := loaded.FileHashMap["notes/foo.md"]
	if entry.Hash != "abc" || entry.MTime != 1000 || entry.Size != 42 {
		t.Errorf("FileHashMap entry mismatch: %+v", entry)
	}
	if loaded.PendingNoteModifies["notes/bar.md"] != "def" {
		t.Error("PendingNoteModifies mismatch")
	}
	cp := loaded.UploadCheckpoints["path-hash"]
	if cp.SessionID != "session" || cp.LastChunkIndex != 3 || cp.Timestamp != 123456 {
		t.Errorf("UploadCheckpoint mismatch: %+v", cp)
	}
}

func TestLoadCorruptJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("{not valid json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := Load(path)
	if err == nil {
		t.Error("expected error for corrupt JSON")
	}
}

func TestLoadNilMapsNormalized(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	// JSON with explicit null maps
	data, _ := json.Marshal(map[string]interface{}{
		"file_hash_map":   nil,
		"config_hash_map": nil,
		"folder_snapshot": nil,
	})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	s, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s.FileHashMap == nil {
		t.Error("FileHashMap should be non-nil after normalization")
	}
	if s.FolderSnapshot == nil {
		t.Error("FolderSnapshot should be non-nil after normalization")
	}
	if s.UploadCheckpoints == nil {
		t.Error("UploadCheckpoints should be non-nil after normalization")
	}
}

func TestSaveCreatesDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deep", "state.json")

	if err := Save(path, New()); err != nil {
		t.Fatalf("Save with nested dirs: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("state file not found: %v", err)
	}
}

func TestSaveEmptyPathError(t *testing.T) {
	if err := Save("", New()); err == nil {
		t.Fatal("expected error for empty state path")
	}
}

func TestSaveCreateDirError(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := Save(filepath.Join(blocker, "state.json"), New())
	if err == nil {
		t.Fatal("expected error when parent path is a file")
	}
}

func TestSaveConcurrentUsesIndependentTempFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := New()
			s.WsCount = i
			s.FileHashMap["note.md"] = FileHashEntry{Hash: "hash", MTime: int64(i), Size: int64(i)}
			errs <- Save(path, s)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Save returned error: %v", err)
		}
	}
	if _, err := Load(path); err != nil {
		t.Fatalf("Load after concurrent saves: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "state.json.*.tmp"))
	if err != nil {
		t.Fatalf("glob tmp files: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files left behind: %v", matches)
	}
}
