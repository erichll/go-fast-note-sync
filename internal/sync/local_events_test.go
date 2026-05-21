package sync

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/local"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

func newLocalEventService(t *testing.T) (*SyncService, *fakeWSConn, string) {
	t.Helper()
	vault := t.TempDir()
	cfg := config.Default()
	cfg.API = "http://test.example.com"
	cfg.Vault = "vault"
	cfg.VaultPath = vault
	cfg.SyncExcludeFolders = []string{"excluded"}
	cfg.SyncExcludeExtensions = []string{".tmp"}
	cfg.ConfigSyncOtherDirs = []string{"config-extra"}
	cfg.ConcurrencyControlEnabled = false
	st := state.New()
	statePath := filepath.Join(t.TempDir(), "state.json")
	svc := newTestService(cfg, st, statePath)
	conn := &fakeWSConn{}
	svc.conn = conn
	svc.isOpen = true
	svc.isAuth = true
	return svc, conn, vault
}

func writeVaultFile(t *testing.T, vault, rel, content string) {
	t.Helper()
	abs := filepath.Join(vault, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func mkdirVault(t *testing.T, vault, rel string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(vault, filepath.FromSlash(rel)), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
}

func firstAction(t *testing.T, conn *fakeWSConn) string {
	t.Helper()
	if len(conn.written) == 0 {
		t.Fatal("expected a websocket write")
	}
	before, _, ok := strings.Cut(conn.written[0], "|")
	if !ok {
		t.Fatalf("message missing action separator: %q", conn.written[0])
	}
	return before
}

func TestHandleLocalModifyRoutesAllCategories(t *testing.T) {
	cases := []struct {
		name string
		path string
		dir  bool
		want string
	}{
		{name: "note", path: "notes/a.md", want: "NoteModify"},
		{name: "file", path: "assets/a.png", want: "FileUploadCheck"},
		{name: "setting", path: ".obsidian/app.json", want: "SettingModify"},
		{name: "folder", path: "folders/new", dir: true, want: "FolderModify"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, conn, vault := newLocalEventService(t)
			if tc.dir {
				mkdirVault(t, vault, tc.path)
			} else {
				writeVaultFile(t, vault, tc.path, "content")
			}
			got := svc.HandleLocalModify(local.PathEvent{Path: tc.path, IsDir: tc.dir})
			if !got.Attempted || got.Err != nil {
				t.Fatalf("result = %+v, want attempted success", got)
			}
			if action := firstAction(t, conn); action != tc.want {
				t.Fatalf("action = %s, want %s", action, tc.want)
			}
		})
	}
}

func TestHandleLocalDeleteRoutesAllCategories(t *testing.T) {
	cases := []struct {
		name string
		path string
		dir  bool
		want string
	}{
		{name: "note", path: "notes/a.md", want: "NoteDelete"},
		{name: "file", path: "assets/a.png", want: "FileDelete"},
		{name: "setting", path: ".obsidian/app.json", want: "SettingDelete"},
		{name: "folder", path: "folders/new", dir: true, want: "FolderDelete"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, conn, _ := newLocalEventService(t)
			got := svc.HandleLocalDelete(local.PathEvent{Path: tc.path, IsDir: tc.dir})
			if !got.Attempted || got.Err != nil {
				t.Fatalf("result = %+v, want attempted success", got)
			}
			if action := firstAction(t, conn); action != tc.want {
				t.Fatalf("action = %s, want %s", action, tc.want)
			}
		})
	}
}

func TestHandleLocalRenameSameAndCrossCategory(t *testing.T) {
	t.Run("same category note", func(t *testing.T) {
		svc, conn, vault := newLocalEventService(t)
		writeVaultFile(t, vault, "new.md", "new")
		got := svc.HandleLocalRename(local.RenameEvent{OldPath: "old.md", NewPath: "new.md"})
		if !got.Attempted || got.Err != nil {
			t.Fatalf("result = %+v", got)
		}
		if action := firstAction(t, conn); action != "NoteRename" {
			t.Fatalf("action = %s, want NoteRename", action)
		}
	})
	t.Run("cross category delete then modify", func(t *testing.T) {
		svc, conn, vault := newLocalEventService(t)
		writeVaultFile(t, vault, "new.bin", "new")
		got := svc.HandleLocalRename(local.RenameEvent{OldPath: "old.md", NewPath: "new.bin"})
		if !got.Attempted || got.Err != nil {
			t.Fatalf("result = %+v", got)
		}
		if len(conn.written) != 2 {
			t.Fatalf("writes = %d, want 2: %#v", len(conn.written), conn.written)
		}
		if !strings.HasPrefix(conn.written[0], "NoteDelete|") || !strings.HasPrefix(conn.written[1], "FileUploadCheck|") {
			t.Fatalf("writes = %#v, want NoteDelete then FileUploadCheck", conn.written)
		}
	})
}

func TestHandleLocalEventsSkipPolicyScopeAndEcho(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*SyncService)
		event local.PathEvent
	}{
		{name: "manual", setup: func(s *SyncService) { s.cfg.ManualSyncEnabled = true }, event: local.PathEvent{Path: "a.md"}},
		{name: "readonly", setup: func(s *SyncService) { s.cfg.ReadOnlySyncEnabled = true }, event: local.PathEvent{Path: "a.md"}},
		{name: "sync disabled", setup: func(s *SyncService) { s.cfg.SyncEnabled = false }, event: local.PathEvent{Path: "a.md"}},
		{name: "socket closed", setup: func(s *SyncService) { s.isOpen = false }, event: local.PathEvent{Path: "a.md"}},
		{name: "socket open unauthenticated", setup: func(s *SyncService) { s.isAuth = false }, event: local.PathEvent{Path: "a.md"}},
		{name: "config disabled", setup: func(s *SyncService) { s.cfg.ConfigSyncEnabled = false }, event: local.PathEvent{Path: ".obsidian/app.json"}},
		{name: "config denied", event: local.PathEvent{Path: ".obsidian/workspace.json"}},
		{name: "fast note sync plugin data", event: local.PathEvent{Path: ".obsidian/plugins/fast-note-sync/data.json"}},
		{name: "excluded file", event: local.PathEvent{Path: "excluded/a.md"}},
		{name: "unsafe path", event: local.PathEvent{Path: "../a.md"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, conn, vault := newLocalEventService(t)
			if !strings.HasPrefix(tc.event.Path, "..") {
				writeVaultFile(t, vault, tc.event.Path, "content")
			}
			if tc.setup != nil {
				tc.setup(svc)
			}
			got := svc.HandleLocalModify(tc.event)
			if got.Attempted || got.Err != nil {
				t.Fatalf("result = %+v, want clean skip", got)
			}
			if len(conn.written) != 0 {
				t.Fatalf("unexpected writes: %#v", conn.written)
			}
		})
	}
}

func TestSensitivePluginConfigLocalEventsDoNotEmitSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*SyncService) local.Result
	}{
		{
			name: "modify",
			run: func(s *SyncService) local.Result {
				return s.HandleLocalModify(local.PathEvent{Path: ".obsidian/plugins/fast-note-sync/data.json"})
			},
		},
		{
			name: "delete",
			run: func(s *SyncService) local.Result {
				return s.HandleLocalDelete(local.PathEvent{Path: ".obsidian/plugins/fast-note-sync/data.json"})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, conn, vault := newLocalEventService(t)
			writeVaultFile(t, vault, ".obsidian/plugins/fast-note-sync/data.json", `{"token":"secret"}`)
			got := tc.run(svc)
			if got.Attempted || got.Err != nil {
				t.Fatalf("result = %+v, want skip", got)
			}
			for _, write := range conn.written {
				if strings.HasPrefix(write, "SettingModify|") || strings.HasPrefix(write, "SettingDelete|") {
					t.Fatalf("sensitive plugin data emitted setting sync message: %#v", conn.written)
				}
			}
		})
	}
}

func TestHandleLocalEventsEchoSuppression(t *testing.T) {
	t.Run("modify", func(t *testing.T) {
		svc, conn, vault := newLocalEventService(t)
		writeVaultFile(t, vault, "a.md", "content")
		svc.lastSyncMtime["a.md"] = 1
		got := svc.HandleLocalModify(local.PathEvent{Path: "a.md"})
		if got.Attempted || len(conn.written) != 0 {
			t.Fatalf("got result=%+v writes=%#v, want skip", got, conn.written)
		}
	})
	t.Run("delete", func(t *testing.T) {
		svc, conn, _ := newLocalEventService(t)
		svc.lastSyncPathDeleted["a.md"] = struct{}{}
		got := svc.HandleLocalDelete(local.PathEvent{Path: "a.md"})
		if got.Attempted || len(conn.written) != 0 {
			t.Fatalf("got result=%+v writes=%#v, want skip", got, conn.written)
		}
	})
	t.Run("rename", func(t *testing.T) {
		svc, conn, vault := newLocalEventService(t)
		writeVaultFile(t, vault, "b.md", "content")
		svc.lastSyncPathRenamed["a.md"] = struct{}{}
		got := svc.HandleLocalRename(local.RenameEvent{OldPath: "a.md", NewPath: "b.md"})
		if got.Attempted || len(conn.written) != 0 {
			t.Fatalf("got result=%+v writes=%#v, want skip", got, conn.written)
		}
	})
}

func TestHandleLocalModifySendFailureDoesNotCommitPending(t *testing.T) {
	svc, _, vault := newLocalEventService(t)
	writeVaultFile(t, vault, "a.md", "content")
	svc.conn = &fakeWSConn{writeErr: errors.New("boom")}
	got := svc.HandleLocalModify(local.PathEvent{Path: "a.md"})
	if !got.Attempted || got.Err == nil {
		t.Fatalf("result = %+v, want attempted error", got)
	}
	if _, ok := svc.pendingNoteModifies["a.md"]; ok {
		t.Fatal("pending note modify was committed after send failure")
	}
	if _, ok := svc.st.PendingNoteModifies["a.md"]; ok {
		t.Fatal("persistent pending note modify was committed after send failure")
	}
}

func TestShouldWatchDirKeepsObsidianAndSkipsExcluded(t *testing.T) {
	svc, _, _ := newLocalEventService(t)
	if !svc.ShouldWatchDir(".") || !svc.ShouldWatchDir(".obsidian") || !svc.ShouldWatchDir(".obsidian/plugins") {
		t.Fatal("root and .obsidian directories should be watchable")
	}
	if svc.ShouldWatchDir("excluded") || svc.ShouldWatchDir("excluded/child") {
		t.Fatal("excluded directory should not be watchable")
	}
	if svc.ShouldWatchDir("../escape") {
		t.Fatal("escaping path should not be watchable")
	}
}
