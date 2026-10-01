package sync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/local"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

func TestM24ExclusionRuleMatrix(t *testing.T) {
	s := newTestService(&config.Config{
		SyncExcludeFolders:    []string{"blocked", "root/private", `windows\excluded\`},
		SyncExcludeExtensions: []string{"LOG"},
		SyncExcludeWhitelist:  []string{".hidden/keep.md", "blocked/allowed", `windows\excluded\keep`},
	}, nil, "")
	for _, tt := range []struct {
		path     string
		excluded bool
	}{
		{".hidden/other.md", true}, {".hidden/keep.md", false},
		{"nested/.hidden/file.png", true}, {"nested/blocked/note.md", true},
		{"nested/blockedness/note.md", false}, {"nested/Blocked/note.md", false},
		{"nested/blocked", false}, {"blocked", true}, {"root/private", true},
		{"root/private/note.md", true}, {"nested/root/private/note.md", false},
		{"blocked/allowed/note.LOG", false}, {"./blocked/allowed/note.LOG", false}, {"blocked/other.md", true},
		{"../outside.md", true}, {"", true}, {"/outside.md", true},
		{`windows\excluded\keep\note.md`, false}, {`windows\excluded\other.md`, true},
		{"normal/file.LOG", true}, {".obsidian/app.json", false},
		{"normal/name.tmp", true}, {"normal/name.tmp.backup", true},
		{"normal/.image.png.tmp-123", true}, {"blocked/allowed/.image.png.tmp-123", true},
		{"blocked/allowed/name.tmp", true}, {"normal/name.tmpish", false},
		{"normal/image.png.tmp-123", false}, {"blocked/allowed/._note.md", true},
		{"blocked/allowed/.DS_Store", true},
	} {
		t.Run(tt.path, func(t *testing.T) {
			if got := s.isVaultFileExcluded(tt.path); got != tt.excluded {
				t.Errorf("isVaultFileExcluded(%q) = %v, want %v", tt.path, got, tt.excluded)
			}
		})
	}
}

func TestM24WhitelistedDescendantsRemainReachable(t *testing.T) {
	for _, offline := range []bool{false, true} {
		for _, incremental := range []bool{false, true} {
			t.Run(testBoolName(offline)+"/"+testBoolName(incremental), func(t *testing.T) {
				vault := t.TempDir()
				kept := []string{".hidden/keep.md", ".hidden/sub/note.md", ".hidden/sub/image.png", "nested/blocked/allowed/note.md"}
				ignored := []string{".hidden/other.md", ".hidden/sibling/image.png", "nested/blocked/other.md"}
				st := state.New()
				for _, rel := range append(kept, ignored...) {
					abs := filepath.Join(vault, filepath.FromSlash(rel))
					if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(abs, []byte("present"), 0o644); err != nil {
						t.Fatal(err)
					}
					st.FileHashMap[rel] = state.FileHashEntry{Hash: "old"}
				}
				st.FolderSnapshot[".hidden"] = 1
				st.FolderSnapshot[".hidden/sub"] = 1
				s := newTestService(&config.Config{VaultPath: vault, OfflineDeleteSyncEnabled: offline,
					SyncExcludeFolders:   []string{"blocked"},
					SyncExcludeWhitelist: []string{".hidden/keep.md", ".hidden/sub", "nested/blocked/allowed"},
				}, st, "")
				for _, ancestor := range []string{".hidden", "nested/blocked"} {
					if !s.ShouldWatchDir(ancestor) {
						t.Errorf("whitelist ancestor %q must be watched", ancestor)
					}
					if !s.isFolderPathExcluded(ancestor) {
						t.Errorf("ancestor %q must not itself sync", ancestor)
					}
				}
				if s.ShouldWatchDir(".hidden/sibling") {
					t.Error("excluded sibling must not be watched")
				}
				result, err := s.scanVault(incremental)
				if err != nil {
					t.Fatal(err)
				}
				found := map[string]bool{}
				for _, file := range append(result.notes, result.files...) {
					found[file.Path] = true
				}
				for _, rel := range kept {
					if !found[rel] {
						t.Errorf("whitelisted file missing from scan: %s", rel)
					}
					if s.localCategory(rel, false) == localCategorySkip {
						t.Errorf("whitelisted local event skipped: %s", rel)
					}
				}
				for _, rel := range ignored {
					if found[rel] {
						t.Errorf("excluded sibling scanned: %s", rel)
					}
				}
				for _, folder := range result.folders {
					if folder.Path == ".hidden" || folder.Path == "nested/blocked" {
						t.Errorf("excluded ancestor synced: %s", folder.Path)
					}
				}
				if len(result.delNotes)+len(result.delFiles)+len(result.delFolders)+len(result.missingNotes)+len(result.missingFiles)+len(result.missingFolders) != 0 {
					t.Fatalf("live files/folders inferred missing or deleted: %+v", result)
				}
			})
		}
	}
}

func testBoolName(value bool) string {
	if value {
		return "enabled"
	}
	return "disabled"
}

func TestM24ExcludedLocalEventsNeverSend(t *testing.T) {
	for _, rel := range []string{"attachments/.image.png.tmp-123", "attachments/name.tmp", "attachments/name.tmp.backup", ".hidden/other.md", "nested/blocked/other.md", ".obsidian/plugins/fast-note-sync/data.json"} {
		t.Run(rel, func(t *testing.T) {
			s, conn, vault := newLocalEventService(t)
			s.cfg.SyncExcludeFolders = []string{"blocked"}
			s.cfg.SyncExcludeWhitelist = []string{"attachments", ".obsidian"}
			writeVaultFile(t, vault, rel, "must not sync")
			for _, result := range []local.Result{
				s.HandleLocalModify(local.PathEvent{Path: rel}),
				s.HandleLocalDelete(local.PathEvent{Path: rel}),
				s.HandleLocalRename(local.RenameEvent{OldPath: rel, NewPath: rel + ".tmp"}),
			} {
				if result.Attempted || result.Err != nil {
					t.Errorf("excluded event = %+v", result)
				}
			}
			if len(conn.written) != 0 || len(s.activeUploads) != 0 || len(s.pendingNoteModifies)+len(s.pendingUploadHashes)+len(s.pendingConfigModifies) != 0 {
				t.Fatal("excluded event sent or queued work")
			}
		})
	}
}

func TestM24IncomingTemporaryPathsAreIgnored(t *testing.T) {
	for name, handler := range map[string]func(json.RawMessage, *SyncService){
		"note modify": handleNoteSyncModify, "note delete": handleNoteSyncDelete,
		"note mtime": handleNoteSyncMtime, "note rename": handleNoteSyncRename,
		"note upload": handleNoteSyncNeedPush, "file update": handleFileSyncUpdate,
		"file upload": handleFileUpload, "file delete": handleFileSyncDelete,
		"file mtime": handleFileSyncMtime, "file rename": handleFileSyncRename,
		"setting modify": handleSettingSyncModify, "setting upload": handleSettingSyncNeedUpload,
		"setting delete": handleSettingSyncDelete, "setting mtime": handleSettingSyncMtime,
		"folder modify": handleFolderSyncModify, "folder delete": handleFolderSyncDelete,
		"folder rename": handleFolderSyncRename,
	} {
		t.Run(name, func(t *testing.T) {
			s, conn, vault := newLocalEventService(t)
			s.cfg.SyncExcludeWhitelist = []string{"attachments"}
			s.cfg.ConfigSyncOtherDirs = []string{"attachments"}
			rel := "attachments/.image.tmp-123/file.md"
			writeVaultFile(t, vault, rel, "unchanged")
			data, err := json.Marshal(map[string]interface{}{"path": rel, "oldPath": rel, "content": "remote", "contentHash": "new", "mtime": 1, "lastTime": 1, "size": 6})
			if err != nil {
				t.Fatal(err)
			}
			handler(data, s)
			content, err := os.ReadFile(filepath.Join(vault, filepath.FromSlash(rel)))
			if err != nil || string(content) != "unchanged" {
				t.Fatalf("excluded remote mutation: %q, %v", content, err)
			}
			if len(conn.written)+len(s.activeUploads)+len(s.fileDownloadSessions)+len(s.st.FileHashMap)+len(s.st.ConfigHashMap) != 0 {
				t.Fatal("excluded remote path sent, queued, or persisted work")
			}
		})
	}
}

func TestM24ConfigTraversalPreservesDedicatedScope(t *testing.T) {
	s := newTestService(&config.Config{ConfigSyncEnabled: true, ConfigSyncOtherDirs: []string{"blocked/.extra"}, SyncExcludeFolders: []string{"blocked"}}, nil, "")
	for _, dir := range []string{"blocked", "blocked/.extra", "blocked/.extra/nested", ".obsidian", ".obsidian/plugins"} {
		if !s.ShouldWatchDir(dir) {
			t.Errorf("configured setting scope must remain reachable: %s", dir)
		}
	}
	for _, dir := range []string{"blocked/sibling", "blocked/.extra/.config.tmp-123", ".obsidian/.tmp"} {
		if s.ShouldWatchDir(dir) {
			t.Errorf("unrelated or hard-excluded dir watched: %s", dir)
		}
	}
}

func TestM24StagingHardExcludedAcrossScopes(t *testing.T) {
	vault := t.TempDir()
	paths := []string{"attachments/.image.png.tmp-123", "attachments/image.tmp", "attachments/image.tmp.backup", ".obsidian/.app.json.tmp-123", "extra/.config.tmp-123/settings.json"}
	st := state.New()
	for _, rel := range paths {
		abs := filepath.Join(vault, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(abs, []byte("temporary"), 0o644); err != nil {
			t.Fatal(err)
		}
		st.FileHashMap[rel] = state.FileHashEntry{Hash: "old"}
		st.ConfigHashMap[rel] = state.FileHashEntry{Hash: "old"}
	}
	s := newTestService(&config.Config{VaultPath: vault, ConfigSyncEnabled: true, OfflineDeleteSyncEnabled: true,
		SyncExcludeWhitelist: []string{"attachments", ".obsidian", "extra"}, ConfigSyncOtherDirs: []string{"extra"},
	}, st, "")
	for _, rel := range paths {
		if s.localCategory(rel, false) != localCategorySkip {
			t.Errorf("temporary local event allowed: %s", rel)
		}
		if s.isConfigSyncPathAllowed(rel) {
			t.Errorf("temporary setting allowed: %s", rel)
		}
	}
	result, err := s.scanVault(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.notes)+len(result.files)+len(result.configs)+len(result.delNotes)+len(result.delFiles)+len(result.delConfigs) != 0 {
		t.Fatalf("temporary artifact scanned or inferred deleted: %+v", result)
	}
}
