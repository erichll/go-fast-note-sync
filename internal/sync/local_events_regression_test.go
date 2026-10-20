// Local event safety regressions, including issue #15 review findings.
package sync

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/local"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

func waitForLocalEventCondition(t *testing.T, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !pred() {
		if time.Now().After(deadline) {
			t.Fatal("setup barrier timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLocalEventsDeleteDoesNotBlockAck(t *testing.T) {
	s, c, v := newLocalEventService(t)
	s.cfg.ConcurrencyControlEnabled = true
	s.cfg.MaxConcurrentUploads = 1
	s.concurrency = NewConcurrencyManager(s.cfg)
	writeVaultFile(t, v, "blocker.md", "pending upload")
	if r := s.HandleLocalModify(local.PathEvent{Path: "blocker.md"}); r.Err != nil || !r.Attempted {
		t.Fatalf("setup send: %+v", r)
	}
	s.mu.Lock()
	s.lastSyncMtime["race.md"] = 1
	s.mu.Unlock()
	deleted := make(chan local.Result, 1)
	go func() { deleted <- s.HandleLocalDelete(local.PathEvent{Path: "race.md"}) }()
	waitForLocalEventCondition(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); _, marked := s.lastSyncMtime["race.md"]; return !marked })
	readDone := make(chan struct{})
	go func() {
		// Same sequential dispatch as readLoop: a path-locking remote mtime
		// handler precedes the ACK that frees the occupied slot. Using the
		// metadata handler avoids confusing slow file fsync with a lock cycle.
		s.dispatchText(`NoteSyncMtime|{"code":200,"data":{"path":"race.md","mtime":1}}`)
		s.dispatchText(`NoteModifyAck|{"code":200,"data":{"path":"blocker.md"}}`)
		close(readDone)
	}()
	blocked := false
	select {
	case <-readDone:
	case <-time.After(800 * time.Millisecond):
		blocked = true
	}
	// Explicit rescue; without this no ACK can be read to free the slot.
	if blocked {
		s.concurrency.ReleaseSlot("manual rescue")
	}
	select {
	case <-deleted:
	case <-time.After(2 * time.Second):
		t.Fatal("delete could not be rescued")
	}
	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("read dispatch could not be rescued")
	}
	if blocked {
		t.Fatalf("readLoop blocked before ACK: delete held path lock while waiting for occupied slot; manual rescue required; writes=%v", c.Written())
	}
}

func TestLocalEventsDeleteAfterOverlappingRemoteRenames(t *testing.T) {
	s, c, v := newLocalEventService(t)
	s.cfg.SyncUpdateDelay = 10
	s.markRenamed("a.md", "b.md")
	s.markRenamed("b.md", "c.md")
	time.Sleep(s.echoSuppressWindow() + 150*time.Millisecond)
	s.mu.Lock()
	_, stale := s.lastSyncPathRenamed["a.md"]
	s.mu.Unlock()
	// Reusing the now-vacant old path should not prevent a later real deletion.
	writeVaultFile(t, v, "a.md", "new local file")
	if r := s.HandleLocalModify(local.PathEvent{Path: "a.md"}); r.Err != nil {
		t.Fatal(r.Err)
	}
	if err := os.Remove(filepath.Join(v, "a.md")); err != nil {
		t.Fatal(err)
	}
	r := s.HandleLocalDelete(local.PathEvent{Path: "a.md"})
	if stale || !r.Attempted || r.Err != nil {
		t.Fatalf("overlapping remote renames leave permanent old-path marker: stale=%t real delete=%+v writes=%v", stale, r, c.Written())
	}
}

func TestLocalEventsMtimePreservesUnsentEdit(t *testing.T) {
	s, c, v := newLocalEventService(t)
	writeVaultFile(t, v, "note.md", "synced content")
	s.st.FileHashMap["note.md"] = state.FileHashEntry{Hash: h.Text("synced content")}
	// Edit after the baseline was established, before its debounce callback.
	writeVaultFile(t, v, "note.md", "UNSENT LOCAL EDIT")
	data, _ := json.Marshal(receiveMtimeMessage{Path: "note.md", MTime: time.Now().UnixMilli()})
	handleNoteSyncMtime(data, s)
	r := s.HandleLocalModify(local.PathEvent{Path: "note.md"})
	if !r.Attempted || r.Err != nil || len(c.Written()) != 1 {
		t.Fatalf("mtime-only message adopted unsent content as remote snapshot and swallowed edit: result=%+v writes=%v", r, c.Written())
	}
}

func TestLocalEventsEditDuringRemoteWrite(t *testing.T) {
	s, c, v := newLocalEventService(t)
	oldReplace := replaceFile
	committed := make(chan struct{})
	resume := make(chan struct{})
	// Barrier after replacement: equivalent to descheduling the remote handler
	// after the filesystem operation but before it finalizes the echo snapshot.
	replaceFile = func(src, dst string) error {
		if err := oldReplace(src, dst); err != nil {
			return err
		}
		close(committed)
		<-resume
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleNoteSyncModify(json.RawMessage(`{"path":"note.md","content":"remote content"}`), s)
	}()
	select {
	case <-committed:
	case <-time.After(2 * time.Second):
		close(resume)
		<-done
		replaceFile = oldReplace
		t.Fatal("replace barrier timed out")
	}
	writeVaultFile(t, v, "note.md", "UNSENT LOCAL EDIT")
	r := s.HandleLocalModify(local.PathEvent{Path: "note.md"})
	close(resume)
	<-done
	replaceFile = oldReplace
	b, err := os.ReadFile(filepath.Join(v, "note.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !r.Attempted || r.Err != nil || len(c.Written()) == 0 {
		t.Fatalf("local callback while remote handler was inFlight was dropped without retry: result=%+v disk=%q writes=%v", r, b, c.Written())
	}
	_, payload, _ := strings.Cut(c.Written()[0], "|")
	var sent map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &sent); err != nil {
		t.Fatal(err)
	}
	if sent["baseHash"] != h.Text("remote content") {
		t.Fatalf("in-flight edit lost known incoming revision: %v", sent)
	}
	s.mu.Lock()
	pending := s.pendingNoteModifies["note.md"]
	s.mu.Unlock()
	if pending != h.Text("UNSENT LOCAL EDIT") {
		t.Fatalf("remote finalization cleared pending local edit: %q", pending)
	}
}

func TestLocalEventsMtimeWithoutBaseline(t *testing.T) {
	s, c, v := newLocalEventService(t)
	// An mtime response can arrive before startup's scanned cache has committed.
	// The pending callback must not lose bytes edited since the offered scan.
	writeVaultFile(t, v, "note.md", "UNSENT LOCAL EDIT")
	data, _ := json.Marshal(receiveMtimeMessage{Path: "note.md", MTime: time.Now().UnixMilli()})
	handleNoteSyncMtime(data, s)
	r := s.HandleLocalModify(local.PathEvent{Path: "note.md"})
	if !r.Attempted || r.Err != nil || len(c.Written()) != 1 {
		t.Fatalf("unknown content baseline treated as proof of remote echo: result=%+v writes=%v", r, c.Written())
	}
}

func TestLocalEventsRevertDuringConsecutiveRemoteWrites(t *testing.T) {
	s, c, v := newLocalEventService(t)
	// First remote version leaves a valid live echo snapshot.
	handleNoteSyncModify(json.RawMessage(`{"path":"note.md","content":"remote v1"}`), s)
	oldReplace := replaceFile
	committed := make(chan struct{})
	resume := make(chan struct{})
	replaceFile = func(src, dst string) error {
		if err := oldReplace(src, dst); err != nil {
			return err
		}
		close(committed)
		<-resume
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleNoteSyncModify(json.RawMessage(`{"path":"note.md","content":"remote v2"}`), s)
	}()
	select {
	case <-committed:
	case <-time.After(2 * time.Second):
		close(resume)
		<-done
		replaceFile = oldReplace
		t.Fatal("replace barrier timed out")
	}
	// A user's undo intentionally restores the preceding remote version, AFTER
	// the newer remote version has replaced the file. This is a real edit.
	writeVaultFile(t, v, "note.md", "remote v1")
	r := s.HandleLocalModify(local.PathEvent{Path: "note.md"})
	close(resume)
	<-done
	replaceFile = oldReplace
	waitForLocalRechecks(t, s)
	disk, err := os.ReadFile(filepath.Join(v, "note.md"))
	if err != nil {
		t.Fatal(err)
	}
	// Accept either immediate propagation or a deferred recheck after the
	// in-flight handler completes; do not force one repair strategy.
	deadline := time.Now().Add(2 * time.Second)
	for len(c.Written()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r.Err != nil || len(c.Written()) == 0 {
		t.Fatalf("prevHash swallowed genuine user undo after replacement with no replay: result=%+v disk=%q writes=%v", r, disk, c.Written())
	}
	_, payload, _ := strings.Cut(c.Written()[0], "|")
	var sent map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &sent); err != nil {
		t.Fatal(err)
	}
	if sent["baseHash"] != h.Text("remote v2") {
		t.Fatalf("undo retry used an obsolete remote base: %v", sent)
	}
}

func TestLocalEventsUndoRedoRemoteRename(t *testing.T) {
	s, c, v := newLocalEventService(t)
	writeVaultFile(t, v, "a.md", "content")
	raw, _ := json.Marshal(receiveRenameMessage{OldPath: "a.md", Path: "b.md", ContentHash: h.Text("content")})
	handleNoteSyncRename(raw, s)
	// Immediately undo and redo that rename locally while echo markers are live.
	if err := os.Rename(filepath.Join(v, "b.md"), filepath.Join(v, "a.md")); err != nil {
		t.Fatal(err)
	}
	r1 := s.HandleLocalRename(local.RenameEvent{OldPath: "b.md", NewPath: "a.md"})
	if !r1.Attempted || r1.Err != nil {
		t.Fatalf("first local rename: %+v", r1)
	}
	if err := os.Rename(filepath.Join(v, "a.md"), filepath.Join(v, "b.md")); err != nil {
		t.Fatal(err)
	}
	r2 := s.HandleLocalRename(local.RenameEvent{OldPath: "a.md", NewPath: "b.md"})
	if !r2.Attempted || r2.Err != nil || len(c.Written()) != 2 {
		t.Fatalf("obsolete remote rename/write markers swallowed local redo: second=%+v writes=%v", r2, c.Written())
	}
}

func TestLocalEventsDeleteSkipsExistingRemoteNote(t *testing.T) {
	svc, fake, vault := newLocalEventService(t)
	writeVaultFile(t, vault, "note.md", "initial")
	b, _ := json.Marshal(receiveContentMessage{Path: "note.md", Content: "remote content"})
	handleNoteSyncModify(b, svc)
	r := svc.HandleLocalDelete(local.PathEvent{Path: "note.md"})
	if r.Attempted || r.Err != nil || len(fake.written) != 0 {
		t.Fatalf("existing remotely written note should not emit NoteDelete: result=%+v writes=%v", r, fake.written)
	}
	if !readObservedFile(filepath.Join(vault, "note.md")).Exists {
		t.Fatal("note.md should still exist on disk")
	}
}

func TestLocalEventsDeleteGuards(t *testing.T) {
	t.Run("existing_paths_never_emit_delete_across_categories", func(t *testing.T) {
		svc, conn, vault := newLocalEventService(t)
		svc.cfg.ConfigSyncEnabled = true
		svc.cfg.ConcurrencyControlEnabled = true
		svc.cfg.MaxConcurrentUploads = 1
		svc.concurrency = NewConcurrencyManager(svc.cfg)

		writeVaultFile(t, vault, "note.md", "note")
		writeVaultFile(t, vault, "asset.png", "bin")
		writeVaultFile(t, vault, ".obsidian/app.json", "{}")
		mkdirVault(t, vault, "folder")

		for _, ev := range []local.PathEvent{
			{Path: "note.md"},
			{Path: "asset.png"},
			{Path: ".obsidian/app.json"},
			{Path: "folder", IsDir: true},
		} {
			res := svc.HandleLocalDelete(ev)
			if res.Attempted || res.Err != nil {
				t.Fatalf("HandleLocalDelete(%+v) = %+v, want clean skip", ev, res)
			}
		}

		// Cross-category and setting renames where oldPath still exists must degrade to modify on newPath only.
		writeVaultFile(t, vault, "new.bin", "bin2")
		if res := svc.HandleLocalRename(local.RenameEvent{OldPath: "note.md", NewPath: "new.bin"}); !res.Attempted || res.Err != nil {
			t.Fatalf("cross-category rename with existing oldPath failed: %+v", res)
		}
		svc.concurrency.ReleaseSlot("new.bin")
		writeVaultFile(t, vault, ".obsidian/hotkeys.json", "{}")
		if res := svc.HandleLocalRename(local.RenameEvent{OldPath: ".obsidian/app.json", NewPath: ".obsidian/hotkeys.json"}); !res.Attempted || res.Err != nil {
			t.Fatalf("setting rename with existing oldPath failed: %+v", res)
		}
		svc.concurrency.ReleaseSlot(".obsidian/hotkeys.json")

		for _, msg := range conn.written {
			if strings.Contains(msg, "Delete|") || strings.Contains(msg, "Rename|") {
				t.Fatalf("unexpected delete or rename emitted when oldPath exists: %v", conn.written)
			}
		}
		svc.mu.Lock()
		ackCount := len(svc.pendingNoteDeleteAcks) + len(svc.pendingFileDeleteAcks) + len(svc.pendingConfigDeleteAcks) + len(svc.pendingDeleteFolderPaths)
		svc.mu.Unlock()
		if ackCount != 0 {
			t.Fatalf("pending delete acks recorded on skipped deletes: %d", ackCount)
		}
	})

	t.Run("stat_permission_error_not_treated_as_missing", func(t *testing.T) {
		if runtime.GOOS == "windows" || os.Geteuid() == 0 {
			t.Skip("chmod permission test requires non-root POSIX user")
		}
		svc, conn, vault := newLocalEventService(t)
		writeVaultFile(t, vault, "locked/note.md", "secret")
		lockedDir := filepath.Join(vault, "locked")
		if err := os.Chmod(lockedDir, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(lockedDir, 0o755) })

		res := svc.HandleLocalDelete(local.PathEvent{Path: "locked/note.md"})
		if res.Attempted || len(conn.written) != 0 {
			t.Fatalf("permission error treated as missing file: res=%+v writes=%v", res, conn.written)
		}
	})

	t.Run("send_failure_releases_slot_and_skips_pending_ack", func(t *testing.T) {
		svc, _, _ := newLocalEventService(t)
		svc.cfg.ConfigSyncEnabled = true
		svc.cfg.ConcurrencyControlEnabled = true
		svc.cfg.MaxConcurrentUploads = 1
		svc.concurrency = NewConcurrencyManager(svc.cfg)
		svc.conn = &fakeWSConn{writeErr: errors.New("broken pipe")}

		for _, ev := range []local.PathEvent{
			{Path: "missing.md"},
			{Path: "missing.png"},
			{Path: ".obsidian/app.json"},
			{Path: "missing-dir", IsDir: true},
		} {
			res := svc.HandleLocalDelete(ev)
			if !res.Attempted || res.Err == nil {
				t.Fatalf("HandleLocalDelete(%+v) = %+v, want attempted error", ev, res)
			}
		}
		svc.mu.Lock()
		ackCount := len(svc.pendingNoteDeleteAcks) + len(svc.pendingFileDeleteAcks) + len(svc.pendingConfigDeleteAcks) + len(svc.pendingDeleteFolderPaths)
		svc.mu.Unlock()
		if ackCount != 0 {
			t.Fatalf("pending delete acks recorded after send failure: %d", ackCount)
		}
		// Verify concurrency slot was not leaked by acquiring and releasing it with a short timeout.
		acquired := make(chan struct{})
		go func() {
			svc.concurrency.WaitForSlot("probe.md", false, 0)
			svc.concurrency.ReleaseSlot("probe.md")
			close(acquired)
		}()
		select {
		case <-acquired:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("concurrency slot leaked after send failure")
		}
	})

	t.Run("recreated_while_waiting_for_slot_aborts_send", func(t *testing.T) {
		svc, conn, vault := newLocalEventService(t)
		svc.cfg.ConcurrencyControlEnabled = true
		svc.cfg.MaxConcurrentUploads = 1
		svc.concurrency = NewConcurrencyManager(svc.cfg)

		// Seed lastSyncMtime["race.md"]; sendLocalDeleteForCategory clears it only after passing
		// the initial pathConfirmedMissing check and immediately before blocking on WaitForSlot.
		svc.mu.Lock()
		svc.lastSyncMtime["race.md"] = 1
		svc.mu.Unlock()

		// Occupy the single concurrency slot so HandleLocalDelete blocks at WaitForSlot.
		svc.concurrency.WaitForSlot("blocker.md", false, 0)

		done := make(chan local.Result, 1)
		go func() {
			done <- svc.HandleLocalDelete(local.PathEvent{Path: "race.md"})
		}()

		deadline := time.Now().Add(2 * time.Second)
		for {
			svc.mu.Lock()
			_, stillSet := svc.lastSyncMtime["race.md"]
			svc.mu.Unlock()
			if !stillSet {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for HandleLocalDelete to reach WaitForSlot")
			}
			time.Sleep(time.Millisecond)
		}

		// Recreate race.md while HandleLocalDelete is waiting for the slot, then release the slot.
		writeVaultFile(t, vault, "race.md", "recreated before slot acquired")
		svc.concurrency.ReleaseSlot("blocker.md")

		res := <-done
		if res.Attempted || res.Err != nil || len(conn.written) != 0 {
			t.Fatalf("recreated file still emitted NoteDelete after WaitForSlot: res=%+v writes=%v", res, conn.written)
		}
	})

	t.Run("failed_remote_write_does_not_clear_newer_marker", func(t *testing.T) {
		svc, _, vault := newLocalEventService(t)
		writeVaultFile(t, vault, "note.md", "v2")
		tokenOld := svc.beginRemoteWrite("note.md", "hash-v1", 2)
		tokenNew := svc.beginRemoteWrite("note.md", "hash-v2", 2)
		svc.mu.Lock()
		svc.finishRemoteWriteLocked("note.md", tokenNew, "hash-v2", 1000, 2)
		svc.mu.Unlock()
		// Stale failure cleanup for tokenOld must not clear tokenNew.
		svc.abortRemoteWrite("note.md", tokenOld)
		svc.mu.Lock()
		_, hasMtime := svc.lastSyncMtime["note.md"]
		meta, hasMeta := svc.echoWriteMeta["note.md"]
		svc.mu.Unlock()
		if !hasMtime || !hasMeta || meta.token != tokenNew {
			t.Fatalf("abortRemoteWrite with old token cleared newer marker: hasMtime=%t hasMeta=%t meta=%+v", hasMtime, hasMeta, meta)
		}
	})
}
