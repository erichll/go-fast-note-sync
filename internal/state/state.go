package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type FileHashEntry struct {
	Hash  string `json:"hash"`
	MTime int64  `json:"mtime"`
	Size  int64  `json:"size"`
}

type UploadCheckpoint struct {
	SessionID      string `json:"sessionId"`
	PathHash       string `json:"pathHash"`
	ContentHash    string `json:"contentHash"`
	LastChunkIndex int    `json:"lastChunkIndex"`
	Timestamp      int64  `json:"timestamp"`
}

type State struct {
	FileHashMap    map[string]FileHashEntry `json:"file_hash_map"`
	ConfigHashMap  map[string]FileHashEntry `json:"config_hash_map"`
	FolderSnapshot map[string]int64         `json:"folder_snapshot"`

	PendingNoteModifies   map[string]string `json:"pending_note_modifies"`
	PendingUploadHashes   map[string]string `json:"pending_upload_hashes"`
	PendingConfigModifies map[string]string `json:"pending_config_modifies"`

	NoteSyncTime   int64 `json:"note_sync_time"`
	FileSyncTime   int64 `json:"file_sync_time"`
	ConfigSyncTime int64 `json:"config_sync_time"`
	FolderSyncTime int64 `json:"folder_sync_time"`
	WsCount        int   `json:"ws_count"`
	IsInitSync     bool  `json:"is_init_sync"`

	UploadCheckpoints map[string]UploadCheckpoint `json:"upload_checkpoints"`
}

func New() *State {
	return &State{
		FileHashMap:           make(map[string]FileHashEntry),
		ConfigHashMap:         make(map[string]FileHashEntry),
		FolderSnapshot:        make(map[string]int64),
		PendingNoteModifies:   make(map[string]string),
		PendingUploadHashes:   make(map[string]string),
		PendingConfigModifies: make(map[string]string),
		UploadCheckpoints:     make(map[string]UploadCheckpoint),
	}
}

func DefaultPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "state.json"
	}
	return filepath.Join(home, ".local", "share", "go-fast-note-sync", "state.json")
}

func Load(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return New(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state %s: %w", path, err)
	}

	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse state %s: %w", path, err)
	}

	// ensure maps are non-nil after unmarshal
	if s.FileHashMap == nil {
		s.FileHashMap = make(map[string]FileHashEntry)
	}
	if s.ConfigHashMap == nil {
		s.ConfigHashMap = make(map[string]FileHashEntry)
	}
	if s.FolderSnapshot == nil {
		s.FolderSnapshot = make(map[string]int64)
	}
	if s.PendingNoteModifies == nil {
		s.PendingNoteModifies = make(map[string]string)
	}
	if s.PendingUploadHashes == nil {
		s.PendingUploadHashes = make(map[string]string)
	}
	if s.PendingConfigModifies == nil {
		s.PendingConfigModifies = make(map[string]string)
	}
	if s.UploadCheckpoints == nil {
		s.UploadCheckpoints = make(map[string]UploadCheckpoint)
	}
	return &s, nil
}

func Save(path string, s *State) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	return WriteFileAtomic(path, data)
}

// WriteFileAtomic writes data to path through a unique temporary file in the
// same directory, then renames it into place.
func WriteFileAtomic(path string, data []byte) error {
	if path == "" {
		return fmt.Errorf("state path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create state tmp: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write state tmp: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("chmod state tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close state tmp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename state: %w", err)
	}
	return nil
}
