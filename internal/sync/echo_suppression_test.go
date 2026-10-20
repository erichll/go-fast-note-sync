package sync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/local"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

func TestEchoUnknownMtimeBaselineDoesNotSuppressLocalChanges(t *testing.T) {
	for _, tc := range []struct {
		path    string
		handler func(json.RawMessage, *SyncService)
		action  string
	}{
		{"note.md", handleNoteSyncMtime, "NoteModify"},
		{"asset.bin", handleFileSyncMtime, "FileUploadCheck"},
		{".obsidian/app.json", handleSettingSyncMtime, "SettingModify"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			s, c, v := newLocalEventService(t)
			s.cfg.ConfigSyncEnabled = true
			writeVaultFile(t, v, tc.path, "not yet synchronized")
			raw, _ := json.Marshal(receiveMtimeMessage{Path: tc.path, MTime: time.Now().UnixMilli()})
			tc.handler(raw, s)
			r := s.HandleLocalModify(local.PathEvent{Path: tc.path})
			if !r.Attempted || r.Err != nil || firstAction(t, c) != tc.action {
				t.Fatalf("unknown baseline swallowed local content: result=%+v writes=%v", r, c.Written())
			}
		})
	}
}

func TestEchoKnownMtimeBaselineDistinguishesLocalContent(t *testing.T) {
	for _, setting := range []bool{false, true} {
		for _, edited := range []bool{false, true} {
			name := "note"
			if setting {
				name = "setting"
			}
			if edited {
				name += "_edited"
			}
			t.Run(name, func(t *testing.T) {
				s, c, v := newLocalEventService(t)
				s.cfg.ConfigSyncEnabled = true
				path := "note.md"
				handler := handleNoteSyncMtime
				hash := h.Text("baseline")
				if setting {
					path = ".obsidian/app.json"
					handler = handleSettingSyncMtime
					hash = h.FileContent([]byte("baseline"))
				}
				entry := state.FileHashEntry{Hash: hash, Size: int64(len("baseline"))}
				if setting {
					s.st.ConfigHashMap[path] = entry
				} else {
					s.st.FileHashMap[path] = entry
				}
				content := "baseline"
				if edited {
					content = "new bytes"
				}
				writeVaultFile(t, v, path, content)
				raw, _ := json.Marshal(receiveMtimeMessage{Path: path, MTime: time.Now().UnixMilli()})
				handler(raw, s)
				r := s.HandleLocalModify(local.PathEvent{Path: path})
				if r.Err != nil || r.Attempted != edited {
					t.Fatalf("edited=%t result=%+v writes=%v", edited, r, c.Written())
				}
			})
		}
	}
}

func waitForLocalRechecks(t *testing.T, s *SyncService) {
	t.Helper()
	waitForLocalEventCondition(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.localRechecks == 0
	})
}

func TestEchoAmbiguousCallbacksSurviveFinalization(t *testing.T) {
	for _, outcome := range []string{"finish", "abort", "supersede"} {
		t.Run(outcome, func(t *testing.T) {
			s, c, v := newLocalEventService(t)
			writeVaultFile(t, v, "note.md", "old remote")
			oldHash := h.Text("old remote")
			old := s.beginRemoteWrite("note.md", oldHash, 10)
			s.mu.Lock()
			s.finishRemoteWriteLocked("note.md", old, oldHash, 1, 10)
			s.mu.Unlock()
			unlock := s.lockPath("note.md")
			token := s.beginRemoteWrite("note.md", h.Text("new remote"), 10)
			// This could be delayed old-version echo or the user's deliberate undo.
			r := s.HandleLocalModify(local.PathEvent{Path: "note.md"})
			s.mu.Lock()
			queued := s.echoWriteMeta["note.md"].recheck
			s.mu.Unlock()
			if r.Attempted || !queued {
				unlock()
				t.Fatalf("ambiguous callback not deferred: result=%+v queued=%t", r, queued)
			}
			switch outcome {
			case "finish":
				s.mu.Lock()
				s.finishRemoteWriteLocked("note.md", token, h.Text("new remote"), 2, 10)
				s.mu.Unlock()
			case "abort":
				s.abortRemoteWrite("note.md", token)
			case "supersede":
				newer := s.beginRemoteWrite("note.md", h.Text("third version"), 13)
				s.abortRemoteWrite("note.md", token)
				s.mu.Lock()
				queued = s.echoWriteMeta["note.md"].recheck
				s.finishRemoteWriteLocked("note.md", newer, h.Text("third version"), 3, 13)
				s.mu.Unlock()
				if !queued {
					unlock()
					t.Fatal("new write lost earlier deferred callback")
				}
			}
			unlock()
			waitForLocalRechecks(t, s)
			waitForLocalEventCondition(t, func() bool { return len(c.Written()) > 0 })
			if firstAction(t, c) != "NoteModify" {
				t.Fatalf("deferred callback not propagated: %v", c.Written())
			}
		})
	}
}

func TestEchoReadFailureDoesNotSuppressLocalEdit(t *testing.T) {
	s, _, v := newLocalEventService(t)
	// ReadFile on a directory fails even when Stat and the supplied snapshot's
	// size/mtime agree. Falling back to mtime would incorrectly declare an echo.
	mkdirVault(t, v, "note.md")
	rp, err := s.resolveVaultPath("note.md")
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(v, "note.md"))
	if err != nil {
		t.Fatal(err)
	}
	token := s.beginRemoteWrite("note.md", "known-hash", info.Size())
	s.mu.Lock()
	s.finishRemoteWriteLocked("note.md", token, "known-hash", info.ModTime().UnixMilli(), info.Size())
	s.mu.Unlock()
	if s.isWriteEchoed(rp, localCategoryNote) {
		t.Fatal("failed content read was treated as confirmed echo")
	}
}

func TestEchoDestinationWriteDoesNotSuppressLocalMove(t *testing.T) {
	s, c, v := newLocalEventService(t)
	raw := json.RawMessage(`{"path":"destination.md","content":"same bytes"}`)
	handleNoteSyncModify(raw, s)
	writeVaultFile(t, v, "source.md", "same bytes")
	if err := os.Rename(filepath.Join(v, "source.md"), filepath.Join(v, "destination.md")); err != nil {
		t.Fatal(err)
	}
	r := s.HandleLocalRename(local.RenameEvent{OldPath: "source.md", NewPath: "destination.md"})
	if !r.Attempted || r.Err != nil || firstAction(t, c) != "NoteRename" {
		t.Fatalf("real source move swallowed by destination write marker: result=%+v writes=%v", r, c.Written())
	}
}
