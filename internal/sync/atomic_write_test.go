package sync

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/erichll/go-fast-note-sync/internal/config"
	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/local"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

func TestStageFileInDestDirIsolation(t *testing.T) {
	vault := t.TempDir()
	source := filepath.Join(t.TempDir(), "merged")
	content := []byte("validated attachment")
	if err := os.WriteFile(source, content, 0o600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(vault, "image.png")
	if err := os.WriteFile(dest, []byte("old"), 0o640); err != nil {
		t.Fatal(err)
	}
	staged, err := stageFileInDestDir(source, dest)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(staged)
	if filepath.Dir(staged) != vault || !strings.HasPrefix(filepath.Base(staged), ".image.png.tmp-") {
		t.Fatalf("unexpected staging path %s", staged)
	}
	bytes, err := os.ReadFile(staged)
	if err != nil || string(bytes) != string(content) {
		t.Fatalf("staging bytes = %q, %v", bytes, err)
	}
	info, err := os.Stat(staged)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o640 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
	for _, whitelist := range [][]string{nil, {"image.png", filepath.Base(staged)}} {
		s := newTestService(&config.Config{VaultPath: vault, SyncExcludeWhitelist: whitelist}, nil, "")
		result, scanErr := s.scanVault(false)
		if scanErr != nil {
			t.Fatal(scanErr)
		}
		if len(result.files) != 1 || result.files[0].Path != "image.png" {
			t.Fatalf("real staging leaked: %+v", result.files)
		}
		if s.localCategory(filepath.Base(staged), false) != localCategorySkip {
			t.Fatal("real staging local event allowed")
		}
	}
	old, err := os.ReadFile(dest)
	if err != nil || string(old) != "old" {
		t.Fatalf("staging changed destination: %q, %v", old, err)
	}
}

func TestStageFileInDestDirFailuresCleanUp(t *testing.T) {
	for _, failure := range []string{"source missing", "source directory", "parent missing", "destination stat"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			source := filepath.Join(t.TempDir(), "merged")
			dest := filepath.Join(dir, "image.png")
			if err := os.WriteFile(source, []byte("data"), 0o600); err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "source missing":
				source += "-missing"
			case "source directory":
				source = t.TempDir()
			case "parent missing":
				dest = filepath.Join(dir, "missing", "image.png")
			case "destination stat":
				if err := os.WriteFile(dest, []byte("not a directory"), 0o600); err != nil {
					t.Fatal(err)
				}
				dest = filepath.Join(dest, "child")
			}
			staged, err := stageFileInDestDir(source, dest)
			if err == nil || staged != "" {
				t.Fatalf("got %q, %v; want empty path and error", staged, err)
			}
			matches, err := filepath.Glob(filepath.Join(dir, ".*.tmp-*"))
			if err != nil || len(matches) != 0 {
				t.Fatalf("staging cleanup: %v, %v", matches, err)
			}
		})
	}
}

func TestM24DownloadCommitLifecycle(t *testing.T) {
	for _, scenario := range []string{"new", "overwrite", "empty", "replace failure", "parent failure", "cancelled", "superseded", "size mismatch", "hash mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			vault := t.TempDir()
			stateDir := t.TempDir()
			cfg := config.Default()
			cfg.VaultPath = vault
			cfg.ConcurrencyControlEnabled = true
			cfg.SyncExcludeWhitelist = []string{"attachments"}
			s := newTestService(cfg, state.New(), filepath.Join(stateDir, "state.json"))
			s.conn = &fakeWSConn{}
			s.isOpen, s.isAuth = true, true
			dest := filepath.Join(vault, "attachments", "image.png")
			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				t.Fatal(err)
			}
			existing := scenario != "new" && scenario != "empty"
			if existing {
				if err := os.WriteFile(dest, []byte("old"), 0o640); err != nil {
					t.Fatal(err)
				}
				s.st.FileHashMap["attachments/image.png"] = state.FileHashEntry{Hash: "old", MTime: 1, Size: 3}
			}
			content := []byte("new attachment")
			chunks := 1
			if scenario == "empty" {
				content = nil
				chunks = 0
			}
			session := &FileDownloadSession{Path: "attachments/image.png", SessionID: "test", TempDir: filepath.Join(stateDir, "chunks"), TotalChunks: chunks, ContentHash: h.FileContent(content), Size: int64(len(content)), MTime: 1700000000000, LastTime: 42, SlotHeld: true}
			if err := os.MkdirAll(session.TempDir, 0o755); err != nil {
				t.Fatal(err)
			}
			if chunks > 0 {
				if err := os.WriteFile(filepath.Join(session.TempDir, "0"), content, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			s.fileDownloadSessions[session.SessionID] = session
			s.concurrency.WaitForSlot("download_"+session.Path, false, 0)
			switch scenario {
			case "cancelled", "superseded":
				// Invalidation before commit must also clean the destination candidate.
				session.Cancelled = scenario == "cancelled"
				delete(s.fileDownloadSessions, session.SessionID)
				session.SlotHeld = false
				s.concurrency.ReleaseSlot("download_" + session.Path)
			case "size mismatch":
				session.Size++
			case "hash mismatch":
				session.ContentHash = "invalid"
			case "parent failure":
				// ENOTDIR is portable even when tests run as root.
				session.Path = "attachments/image.png/child.png"
			}
			originalReplace := replaceFile
			t.Cleanup(func() { replaceFile = originalReplace })
			replaced := false
			replaceFile = func(source, target string) error {
				replaced = true
				if filepath.Dir(source) != filepath.Dir(target) || !isFilesystemJunkPath(filepath.Base(source)) {
					t.Errorf("unsafe commit candidate: %s -> %s", source, target)
				}
				if s.st.FileSyncTime != 0 {
					t.Error("state advanced before replacement")
				}
				if scenario == "replace failure" {
					return errors.New("injected replacement failure")
				}
				return originalReplace(source, target)
			}
			s.mergeDownloadSession(session)
			success := scenario == "new" || scenario == "overwrite" || scenario == "empty"
			if success {
				got, err := os.ReadFile(dest)
				if err != nil || string(got) != string(content) {
					t.Fatalf("committed bytes = %q, %v", got, err)
				}
				info, err := os.Stat(dest)
				if err != nil {
					t.Fatal(err)
				}
				wantMode := os.FileMode(0o644)
				if existing {
					wantMode = 0o640
				}
				if runtime.GOOS != "windows" && info.Mode().Perm() != wantMode {
					t.Errorf("mode = %o, want %o", info.Mode().Perm(), wantMode)
				}
				if info.ModTime().UnixMilli() != session.MTime || s.st.FileSyncTime != 42 || s.st.FileHashMap[session.Path].Hash != session.ContentHash {
					t.Error("successful commit metadata not applied")
				}
				if result := s.HandleLocalModify(local.PathEvent{Path: session.Path}); result.Attempted || result.Err != nil {
					t.Errorf("final-file echo was not suppressed: %+v", result)
				}
				if !replaced {
					t.Error("replacement primitive not called")
				}
			} else {
				old, err := os.ReadFile(dest)
				if err != nil || string(old) != "old" {
					t.Fatalf("failure changed destination: %q, %v", old, err)
				}
				if s.st.FileSyncTime != 0 || s.st.FileHashMap["attachments/image.png"].Hash != "old" {
					t.Error("failed commit changed persisted metadata")
				}
				if replaced && scenario != "replace failure" {
					t.Error("invalid session reached replacement")
				}
			}
			if len(s.fileDownloadSessions) != 0 || len(s.concurrency.slots) != 0 || session.SlotHeld {
				t.Error("session or slot leaked")
			}
			if _, err := os.Stat(session.TempDir); !os.IsNotExist(err) {
				t.Errorf("chunk directory leaked: %v", err)
			}
			candidates, err := filepath.Glob(filepath.Join(vault, "attachments", ".*.tmp-*"))
			if err != nil || len(candidates) != 0 {
				t.Errorf("destination staging leaked: %v, %v", candidates, err)
			}
		})
	}
}
