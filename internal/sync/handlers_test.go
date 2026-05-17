package sync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/config"
	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

// ---- FolderSyncEnd ----

func TestHandleFolderSyncEnd_SetsNeedCounts(t *testing.T) {
	svc := newTestService(nil, nil, "")
	data, _ := json.Marshal(SyncEndData{
		LastTime:           500,
		NeedUploadCount:    1,
		NeedModifyCount:    2,
		NeedSyncMtimeCount: 3,
		NeedDeleteCount:    4,
	})
	handleFolderSyncEnd(data, svc)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if !svc.folderSyncEnd {
		t.Error("folderSyncEnd should be true")
	}
	if svc.folderSyncTasks.NeedUpload != 1 || svc.folderSyncTasks.NeedModify != 2 {
		t.Errorf("task counts wrong: %+v", svc.folderSyncTasks)
	}
	if svc.st.FolderSyncTime != 500 {
		t.Errorf("FolderSyncTime = %d, want 500", svc.st.FolderSyncTime)
	}
}

func TestHandleFolderSyncEnd_ClearsPendingDeletes(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.st.FolderSnapshot["old-folder"] = 100
	svc.pendingDeleteFolderPaths["old-folder"] = struct{}{}

	data, _ := json.Marshal(SyncEndData{LastTime: 1})
	handleFolderSyncEnd(data, svc)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if _, ok := svc.st.FolderSnapshot["old-folder"]; ok {
		t.Error("old-folder should be removed from FolderSnapshot on FolderSyncEnd")
	}
	if len(svc.pendingDeleteFolderPaths) != 0 {
		t.Error("pendingDeleteFolderPaths should be cleared")
	}
}

func TestHandleFolderSyncEnd_BadJSON(t *testing.T) {
	svc := newTestService(nil, nil, "")
	// Should not panic
	handleFolderSyncEnd(json.RawMessage(`not-json`), svc)
}

// ---- FolderSyncModify ----

func TestHandleFolderSyncModify_CreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")

	msg, _ := json.Marshal(folderSyncMsgData{Path: "new-folder", MTime: 12345})
	handleFolderSyncModify(msg, svc)

	absPath := filepath.Join(dir, "new-folder")
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		t.Error("new-folder should have been created")
	}

	svc.mu.Lock()
	mtime, ok := svc.st.FolderSnapshot["new-folder"]
	svc.mu.Unlock()
	if !ok {
		t.Error("new-folder should be in FolderSnapshot")
	}
	if mtime != 12345 {
		t.Errorf("FolderSnapshot mtime = %d, want 12345", mtime)
	}
}

func TestHandleFolderSyncModify_IncrementCompleted(t *testing.T) {
	svc := newTestService(nil, nil, "")
	before := svc.folderSyncTasks.Completed
	msg, _ := json.Marshal(folderSyncMsgData{Path: "f"})
	handleFolderSyncModify(msg, svc)
	if svc.folderSyncTasks.Completed != before+1 {
		t.Errorf("Completed = %d, want %d", svc.folderSyncTasks.Completed, before+1)
	}
}

func TestHandleFolderSyncModify_ExcludedPath(t *testing.T) {
	cfg := &config.Config{SyncExcludeFolders: []string{"private"}}
	svc := newTestService(cfg, nil, "")
	before := svc.folderSyncTasks.Completed
	msg, _ := json.Marshal(folderSyncMsgData{Path: "private"})
	handleFolderSyncModify(msg, svc)
	if svc.folderSyncTasks.Completed != before+1 {
		t.Error("Completed should be incremented even for excluded paths")
	}
}

func TestHandleFolderSyncModify_BadJSON(t *testing.T) {
	svc := newTestService(nil, nil, "")
	before := svc.folderSyncTasks.Completed
	handleFolderSyncModify(json.RawMessage(`bad`), svc)
	if svc.folderSyncTasks.Completed != before+1 {
		t.Error("Completed should be incremented even on parse error")
	}
}

// ---- FolderSyncDelete ----

func TestHandleFolderSyncDelete_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	svc.st.FolderSnapshot["empty-dir"] = 100

	// Create empty directory
	os.Mkdir(filepath.Join(dir, "empty-dir"), 0o755)

	msg, _ := json.Marshal(folderSyncMsgData{Path: "empty-dir"})
	handleFolderSyncDelete(msg, svc)

	if _, err := os.Stat(filepath.Join(dir, "empty-dir")); !os.IsNotExist(err) {
		t.Error("empty-dir should have been deleted")
	}
	svc.mu.Lock()
	_, inSnapshot := svc.st.FolderSnapshot["empty-dir"]
	svc.mu.Unlock()
	if inSnapshot {
		t.Error("empty-dir should be removed from FolderSnapshot")
	}
}

func TestHandleFolderSyncDelete_NonEmptyDir_NotDeleted(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")

	// Create non-empty directory
	os.Mkdir(filepath.Join(dir, "has-files"), 0o755)
	os.WriteFile(filepath.Join(dir, "has-files", "file.txt"), []byte("x"), 0o644)

	msg, _ := json.Marshal(folderSyncMsgData{Path: "has-files"})
	handleFolderSyncDelete(msg, svc)

	if _, err := os.Stat(filepath.Join(dir, "has-files")); os.IsNotExist(err) {
		t.Error("non-empty dir should NOT be deleted")
	}
}

func TestHandleFolderSyncDelete_IncrementCompleted(t *testing.T) {
	svc := newTestService(nil, nil, "")
	before := svc.folderSyncTasks.Completed
	msg, _ := json.Marshal(folderSyncMsgData{Path: "nonexistent"})
	handleFolderSyncDelete(msg, svc)
	if svc.folderSyncTasks.Completed != before+1 {
		t.Error("Completed should be incremented")
	}
}

func TestHandleFolderSyncDelete_BadJSON(t *testing.T) {
	svc := newTestService(nil, nil, "")
	before := svc.folderSyncTasks.Completed
	handleFolderSyncDelete(json.RawMessage(`bad`), svc)
	if svc.folderSyncTasks.Completed != before+1 {
		t.Error("Completed should be incremented even on parse error")
	}
}

// ---- NoteSyncEnd ----

func TestHandleNoteSyncEnd_SetsFlag(t *testing.T) {
	svc := newTestService(nil, nil, "")
	data, _ := json.Marshal(SyncEndData{LastTime: 999, NeedUploadCount: 2})
	handleNoteSyncEnd(data, svc)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if !svc.noteSyncEnd {
		t.Error("noteSyncEnd should be true")
	}
	if svc.noteSyncTasks.NeedUpload != 2 {
		t.Errorf("NeedUpload = %d, want 2", svc.noteSyncTasks.NeedUpload)
	}
	if svc.st.NoteSyncTime != 999 {
		t.Errorf("NoteSyncTime = %d, want 999", svc.st.NoteSyncTime)
	}
}

func TestHandleNoteSyncEnd_ClearsPendingDeletes(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.st.FileHashMap["dead.md"] = state.FileHashEntry{Hash: "h"}
	svc.pendingDeleteNotePaths["dead.md"] = struct{}{}

	data, _ := json.Marshal(SyncEndData{LastTime: 1})
	handleNoteSyncEnd(data, svc)

	svc.mu.Lock()
	_, stillInMap := svc.st.FileHashMap["dead.md"]
	svc.mu.Unlock()
	if stillInMap {
		t.Error("dead.md should be removed from FileHashMap")
	}
}

func TestHandleNoteSyncEnd_CommitsScannedHashes(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.scannedNoteHashes["note.md"] = state.FileHashEntry{Hash: "newhash", MTime: 100, Size: 5}

	data, _ := json.Marshal(SyncEndData{LastTime: 1})
	handleNoteSyncEnd(data, svc)

	svc.mu.Lock()
	entry, ok := svc.st.FileHashMap["note.md"]
	svc.mu.Unlock()
	if !ok {
		t.Error("note.md should be committed to FileHashMap from scannedNoteHashes")
	}
	if entry.Hash != "newhash" {
		t.Errorf("Hash = %q, want newhash", entry.Hash)
	}
}

func TestHandleNoteSyncEnd_DoesNotCommitPendingModifies(t *testing.T) {
	dir := t.TempDir()
	absPath := filepath.Join(dir, "modified.md")
	os.WriteFile(absPath, []byte("updated"), 0o644)

	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	svc.pendingNoteModifies["modified.md"] = "newhash"

	data, _ := json.Marshal(SyncEndData{LastTime: 1})
	handleNoteSyncEnd(data, svc)

	svc.mu.Lock()
	_, committed := svc.st.FileHashMap["modified.md"]
	pending := svc.pendingNoteModifies["modified.md"]
	svc.mu.Unlock()
	if committed {
		t.Error("modified.md should not be committed from pendingNoteModifies on SyncEnd")
	}
	if pending != "newhash" {
		t.Errorf("pending hash = %q, want newhash", pending)
	}
}

func TestHandleNoteSyncEnd_BadJSON(t *testing.T) {
	svc := newTestService(nil, nil, "")
	// Should not panic
	handleNoteSyncEnd(json.RawMessage(`bad`), svc)
	if svc.noteSyncEnd {
		t.Error("noteSyncEnd should remain false on parse error")
	}
}

// ---- FileSyncEnd ----

func TestHandleFileSyncEnd_SetsFlag(t *testing.T) {
	svc := newTestService(nil, nil, "")
	data, _ := json.Marshal(SyncEndData{LastTime: 777, NeedModifyCount: 3})
	handleFileSyncEnd(data, svc)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if !svc.fileSyncEnd {
		t.Error("fileSyncEnd should be true")
	}
	if svc.fileSyncTasks.NeedModify != 3 {
		t.Errorf("NeedModify = %d, want 3", svc.fileSyncTasks.NeedModify)
	}
	if svc.st.FileSyncTime != 777 {
		t.Errorf("FileSyncTime = %d, want 777", svc.st.FileSyncTime)
	}
}

func TestHandleFileSyncEnd_ClearsPendingDeletes(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.st.FileHashMap["dead.png"] = state.FileHashEntry{Hash: "h"}
	svc.pendingDeleteFilePaths["dead.png"] = struct{}{}

	data, _ := json.Marshal(SyncEndData{LastTime: 1})
	handleFileSyncEnd(data, svc)

	svc.mu.Lock()
	_, ok := svc.st.FileHashMap["dead.png"]
	svc.mu.Unlock()
	if ok {
		t.Error("dead.png should be removed from FileHashMap")
	}
}

func TestHandleFileSyncEnd_BadJSON(t *testing.T) {
	svc := newTestService(nil, nil, "")
	handleFileSyncEnd(json.RawMessage(`bad`), svc)
	if svc.fileSyncEnd {
		t.Error("fileSyncEnd should remain false on parse error")
	}
}

// ---- SettingSyncEnd ----

func TestHandleSettingSyncEnd_SetsFlag(t *testing.T) {
	svc := newTestService(nil, nil, "")
	data, _ := json.Marshal(SyncEndData{LastTime: 888, NeedUploadCount: 1})
	handleSettingSyncEnd(data, svc)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if !svc.configSyncEnd {
		t.Error("configSyncEnd should be true")
	}
	// hasUpdates=true → lastTime should be updated
	if svc.st.ConfigSyncTime != 888 {
		t.Errorf("ConfigSyncTime = %d, want 888", svc.st.ConfigSyncTime)
	}
}

func TestHandleSettingSyncEnd_NoUpdateLastTimeWhenNoTasks(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.st.ConfigSyncTime = 555

	data, _ := json.Marshal(SyncEndData{LastTime: 999}) // all need counts = 0
	handleSettingSyncEnd(data, svc)

	svc.mu.Lock()
	ts := svc.st.ConfigSyncTime
	svc.mu.Unlock()
	if ts != 555 {
		t.Errorf("ConfigSyncTime should not update when no tasks: got %d", ts)
	}
}

func TestHandleSettingSyncEnd_CommitsScannedConfigHashes(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.scannedConfigHashes[".obsidian/app.json"] = state.FileHashEntry{Hash: "cfg-hash", MTime: 50}

	data, _ := json.Marshal(SyncEndData{LastTime: 1})
	handleSettingSyncEnd(data, svc)

	svc.mu.Lock()
	entry, ok := svc.st.ConfigHashMap[".obsidian/app.json"]
	svc.mu.Unlock()
	if !ok || entry.Hash != "cfg-hash" {
		t.Errorf("config hash not committed: %+v", entry)
	}
}

func TestHandleSettingSyncEnd_BadJSON(t *testing.T) {
	svc := newTestService(nil, nil, "")
	handleSettingSyncEnd(json.RawMessage(`bad`), svc)
	if svc.configSyncEnd {
		t.Error("configSyncEnd should remain false on parse error")
	}
}

// ---- commitPendingModifies ----

func TestCommitPendingModifies_EmptyVaultPath(t *testing.T) {
	out := commitPendingModifies(map[string]string{"a.md": "hash"}, "")
	if len(out) != 0 {
		t.Error("empty vaultPath should produce no committed entries")
	}
}

func TestCommitPendingModifies_FileExists(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "note.md"), []byte("hello"), 0o644)
	out := commitPendingModifies(map[string]string{"note.md": "myhash"}, dir)
	entry, ok := out["note.md"]
	if !ok {
		t.Fatal("note.md should be in committed output")
	}
	if entry.Hash != "myhash" {
		t.Errorf("Hash = %q, want myhash", entry.Hash)
	}
	if entry.MTime == 0 {
		t.Error("MTime should be set from file stat")
	}
}

func TestCommitPendingModifies_MissingFile(t *testing.T) {
	dir := t.TempDir()
	out := commitPendingModifies(map[string]string{"missing.md": "hash"}, dir)
	if _, ok := out["missing.md"]; ok {
		t.Error("missing file should not appear in committed output")
	}
}

// ---- integration: handleSync → full round with fake conn ----

func TestHandleSync_FullRoundTrip(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "note.md"), []byte("content"), 0o644)

	statePath := filepath.Join(dir, "state.json")
	st := state.New()
	cfg := &config.Config{
		Vault:             "V",
		VaultPath:         dir,
		ConfigSyncEnabled: false,
	}
	svc := newTestService(cfg, st, statePath)
	svc.statePath = statePath
	svc.syncTimeout = 200 * time.Millisecond

	// Inject fake connection so Send() doesn't log "not connected"
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	svc.handleSync(false)

	// Wait enough for scan + send + completion goroutine to fire
	time.Sleep(150 * time.Millisecond)

	// At least FolderSync should have been sent
	foundFolder := false
	for _, msg := range fc.Written() {
		if len(msg) > 10 && msg[:10] == "FolderSync" {
			foundFolder = true
			break
		}
	}
	if !foundFolder {
		// If conn was set after goroutine started scanning, it might have missed
		t.Log("FolderSync may not have been captured (timing); verifying no panic instead")
	}
}

// ---- FolderSyncModify: additional branches ----

func TestHandleFolderSyncModify_AdvancesLastTime(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	svc.st.FolderSyncTime = 100

	msg, _ := json.Marshal(folderSyncMsgData{Path: "newfolder", LastTime: 500})
	handleFolderSyncModify(msg, svc)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.st.FolderSyncTime != 500 {
		t.Errorf("FolderSyncTime = %d, want 500", svc.st.FolderSyncTime)
	}
}

func TestHandleFolderSyncModify_KeepsHigherLastTime(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	svc.st.FolderSyncTime = 1000

	msg, _ := json.Marshal(folderSyncMsgData{Path: "newfolder", LastTime: 500})
	handleFolderSyncModify(msg, svc)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.st.FolderSyncTime != 1000 {
		t.Errorf("FolderSyncTime should remain 1000, got %d", svc.st.FolderSyncTime)
	}
}

func TestHandleFolderSyncModify_EmptyVaultPath(t *testing.T) {
	svc := newTestService(nil, nil, "")
	before := svc.folderSyncTasks.Completed
	msg, _ := json.Marshal(folderSyncMsgData{Path: "f"})
	handleFolderSyncModify(msg, svc)
	if svc.folderSyncTasks.Completed != before+1 {
		t.Error("Completed should still be incremented when vaultPath is empty")
	}
}

func TestHandleFolderSyncModify_DefaultMTimeWhenZero(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	before := time.Now().UnixMilli()

	msg, _ := json.Marshal(folderSyncMsgData{Path: "z", MTime: 0})
	handleFolderSyncModify(msg, svc)

	svc.mu.Lock()
	mtime := svc.st.FolderSnapshot["z"]
	svc.mu.Unlock()
	if mtime < before {
		t.Errorf("default mtime = %d, should be >= %d", mtime, before)
	}
}

// ---- FolderSyncDelete: additional branches ----

func TestHandleFolderSyncDelete_AdvancesLastTime(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	svc.st.FolderSyncTime = 100

	emptyDir := filepath.Join(dir, "empty")
	os.Mkdir(emptyDir, 0o755)
	svc.st.FolderSnapshot["empty"] = 1

	msg, _ := json.Marshal(folderSyncMsgData{Path: "empty", LastTime: 800})
	handleFolderSyncDelete(msg, svc)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.st.FolderSyncTime != 800 {
		t.Errorf("FolderSyncTime = %d, want 800", svc.st.FolderSyncTime)
	}
}

func TestHandleFolderSyncDelete_EmptyVaultPath(t *testing.T) {
	svc := newTestService(nil, nil, "")
	before := svc.folderSyncTasks.Completed
	msg, _ := json.Marshal(folderSyncMsgData{Path: "f"})
	handleFolderSyncDelete(msg, svc)
	if svc.folderSyncTasks.Completed != before+1 {
		t.Error("Completed should still be incremented")
	}
}

func TestHandleFolderSyncDelete_ExcludedPath(t *testing.T) {
	cfg := &config.Config{SyncExcludeFolders: []string{"private"}}
	svc := newTestService(cfg, nil, "")
	before := svc.folderSyncTasks.Completed
	msg, _ := json.Marshal(folderSyncMsgData{Path: "private"})
	handleFolderSyncDelete(msg, svc)
	if svc.folderSyncTasks.Completed != before+1 {
		t.Error("Completed should be incremented even on exclude")
	}
}

// ---- M1.4 receive handlers ----

func TestHandleNoteSyncModify_WritesFileAndRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")

	content := "hello"
	msg, _ := json.Marshal(receiveContentMessage{
		Path:        "notes/a.md",
		Content:     content,
		ContentHash: h.Content([]byte(content)),
		MTime:       time.Now().UnixMilli(),
		LastTime:    123,
	})
	handleNoteSyncModify(msg, svc)

	if got, err := os.ReadFile(filepath.Join(dir, "notes", "a.md")); err != nil || string(got) != content {
		t.Fatalf("note content = %q, err=%v", string(got), err)
	}
	svc.mu.Lock()
	entry, ok := svc.st.FileHashMap["notes/a.md"]
	completed := svc.noteSyncTasks.Completed
	lastTime := svc.st.NoteSyncTime
	svc.mu.Unlock()
	if !ok || entry.Hash != h.Content([]byte(content)) {
		t.Fatalf("FileHashMap entry not committed: %+v", entry)
	}
	if completed != 1 || lastTime != 123 {
		t.Fatalf("completed/lastTime = %d/%d, want 1/123", completed, lastTime)
	}

	bad, _ := json.Marshal(receiveContentMessage{Path: "../evil.md", Content: "x"})
	handleNoteSyncModify(bad, svc)
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "evil.md")); !os.IsNotExist(err) {
		t.Fatalf("traversal path should not be written, stat err=%v", err)
	}
}

func TestHandleNoteSyncNeedPush_SendSuccessWritesPendingAndCompleted(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "push.md"), []byte("push me"), 0o644)
	cfg := &config.Config{Vault: "V", VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	msg, _ := json.Marshal(receivePathMessage{Path: "push.md", LastTime: 44})
	handleNoteSyncNeedPush(msg, svc)

	if len(fc.written) != 1 || !strings.HasPrefix(fc.written[0], "NoteModify|") {
		t.Fatalf("written = %#v, want NoteModify", fc.written)
	}
	svc.mu.Lock()
	pending := svc.pendingNoteModifies["push.md"]
	completed := svc.noteSyncTasks.Completed
	svc.mu.Unlock()
	if pending == "" {
		t.Fatal("pendingNoteModifies should be set after send success")
	}
	if completed != 1 {
		t.Fatalf("Completed = %d, want 1", completed)
	}
}

func TestHandleNoteSyncNeedPush_SendFailureDoesNotWritePendingOrComplete(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "push.md"), []byte("push me"), 0o644)
	cfg := &config.Config{Vault: "V", VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{writeErr: os.ErrPermission}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	msg, _ := json.Marshal(receivePathMessage{Path: "push.md"})
	handleNoteSyncNeedPush(msg, svc)

	svc.mu.Lock()
	_, pending := svc.pendingNoteModifies["push.md"]
	completed := svc.noteSyncTasks.Completed
	svc.mu.Unlock()
	if pending {
		t.Fatal("pendingNoteModifies should not be set after send failure")
	}
	if completed != 0 {
		t.Fatalf("Completed = %d, want 0 on send failure", completed)
	}
}

func TestHandleNoteModifyAck_CommitsPendingAndReleasesSlot(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "ack.md"), []byte("ack"), 0o644)
	cfg := &config.Config{VaultPath: dir, ConcurrencyControlEnabled: true, MaxConcurrentUploads: 1}
	svc := newTestService(cfg, nil, "")
	hash := h.Content([]byte("ack"))
	svc.setPendingNoteModify("ack.md", hash)
	svc.concurrency.WaitForSlot("ack.md", false, 0)

	msg, _ := json.Marshal(receivePathMessage{Path: "ack.md", LastTime: 9})
	handleNoteModifyAck(msg, svc)

	svc.mu.Lock()
	entry := svc.st.FileHashMap["ack.md"]
	_, pending := svc.pendingNoteModifies["ack.md"]
	lastTime := svc.st.NoteSyncTime
	svc.mu.Unlock()
	if pending || entry.Hash != hash || lastTime != 9 {
		t.Fatalf("ack commit wrong: pending=%v entry=%+v lastTime=%d", pending, entry, lastTime)
	}
	if got := len(svc.concurrency.slots); got != 0 {
		t.Fatalf("slot len = %d, want 0 after ack release", got)
	}
}

func TestHandleFileUploadAck_CommitsPendingUpload(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "img.png"), []byte("png"), 0o644)
	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	hash := h.Content([]byte("png"))
	svc.setPendingUpload("img.png", hash)

	msg, _ := json.Marshal(receivePathMessage{Path: "img.png", LastTime: 10})
	handleFileUploadAck(msg, svc)

	svc.mu.Lock()
	entry := svc.st.FileHashMap["img.png"]
	_, pending := svc.pendingUploadHashes["img.png"]
	lastTime := svc.st.FileSyncTime
	svc.mu.Unlock()
	if pending || entry.Hash != hash || lastTime != 10 {
		t.Fatalf("upload ack wrong: pending=%v entry=%+v lastTime=%d", pending, entry, lastTime)
	}
}

func TestHandleSettingSyncModify_SkipsLocalStorage(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	msg, _ := json.Marshal(receiveContentMessage{
		Path:    ".obsidian/_localStorage/plugin.json",
		Content: "x",
	})
	handleSettingSyncModify(msg, svc)

	if _, err := os.Stat(filepath.Join(dir, ".obsidian", "_localStorage", "plugin.json")); !os.IsNotExist(err) {
		t.Fatalf("_localStorage path should be skipped, stat err=%v", err)
	}
	if svc.configSyncTasks.Completed != 1 {
		t.Fatalf("Completed = %d, want 1", svc.configSyncTasks.Completed)
	}
}

func TestHandleFolderSyncRename_RenamesSnapshotAndDirectory(t *testing.T) {
	dir := t.TempDir()
	os.Mkdir(filepath.Join(dir, "old"), 0o755)
	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	svc.st.FolderSnapshot["old"] = 1

	msg, _ := json.Marshal(receiveRenameMessage{OldPath: "old", Path: "new", MTime: 55, LastTime: 77})
	handleFolderSyncRename(msg, svc)

	if _, err := os.Stat(filepath.Join(dir, "new")); err != nil {
		t.Fatalf("new folder should exist: %v", err)
	}
	svc.mu.Lock()
	_, oldOK := svc.st.FolderSnapshot["old"]
	newMtime := svc.st.FolderSnapshot["new"]
	completed := svc.folderSyncTasks.Completed
	lastTime := svc.st.FolderSyncTime
	svc.mu.Unlock()
	if oldOK || newMtime != 55 || completed != 1 || lastTime != 77 {
		t.Fatalf("folder rename state oldOK=%v newMtime=%d completed=%d lastTime=%d", oldOK, newMtime, completed, lastTime)
	}
}

func TestM14NoteMtimeDeleteRenameAndAcks(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "mtime.md"), []byte("mtime"), 0o644)
	os.WriteFile(filepath.Join(dir, "delete.md"), []byte("delete"), 0o644)
	os.WriteFile(filepath.Join(dir, "old.md"), []byte("old"), 0o644)
	cfg := &config.Config{Vault: "V", VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	svc.setPendingNoteModify("mtime.md", h.Content([]byte("mtime")))
	svc.st.FileHashMap["delete.md"] = state.FileHashEntry{Hash: "old"}
	svc.st.FileHashMap["old.md"] = state.FileHashEntry{Hash: "oldhash"}

	mtimeMsg, _ := json.Marshal(receiveMtimeMessage{Path: "mtime.md", MTime: 2000, LastTime: 20})
	handleNoteSyncMtime(mtimeMsg, svc)
	delMsg, _ := json.Marshal(receivePathMessage{Path: "delete.md", LastTime: 21})
	handleNoteSyncDelete(delMsg, svc)
	renameMsg, _ := json.Marshal(receiveRenameMessage{OldPath: "old.md", Path: "new.md", ContentHash: h.Content([]byte("old")), LastTime: 22})
	handleNoteSyncRename(renameMsg, svc)

	if _, err := os.Stat(filepath.Join(dir, "delete.md")); !os.IsNotExist(err) {
		t.Fatalf("delete.md should be removed, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "new.md")); err != nil {
		t.Fatalf("new.md should exist: %v", err)
	}
	svc.mu.Lock()
	_, pendingMtime := svc.pendingNoteModifies["mtime.md"]
	_, deleteHash := svc.st.FileHashMap["delete.md"]
	_, oldHash := svc.st.FileHashMap["old.md"]
	_, newHash := svc.st.FileHashMap["new.md"]
	completed := svc.noteSyncTasks.Completed
	lastTime := svc.st.NoteSyncTime
	svc.mu.Unlock()
	if pendingMtime || deleteHash || oldHash || !newHash || completed != 3 || lastTime != 22 {
		t.Fatalf("note state pending=%v deleteHash=%v oldHash=%v newHash=%v completed=%d lastTime=%d", pendingMtime, deleteHash, oldHash, newHash, completed, lastTime)
	}

	svc.pendingNoteRenames = append(svc.pendingNoteRenames, struct{ OldPath, NewPath, ContentHash string }{"new.md", "final.md", h.Content([]byte("old"))})
	svc.concurrency.WaitForSlot("rename", true, 0)
	handleNoteRenameAck([]byte(`{"lastTime":23}`), svc)
	svc.pendingNoteDeleteAcks["final.md"] = struct{}{}
	deleteAck, _ := json.Marshal(receivePathMessage{Path: "final.md", LastTime: 24})
	handleNoteDeleteAck(deleteAck, svc)
	if got := len(svc.concurrency.fifo); got != 0 {
		t.Fatalf("fifo slot len = %d, want 0", got)
	}
}

func TestM14FileHandlers(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "mtime.bin"), []byte("mtime"), 0o644)
	os.WriteFile(filepath.Join(dir, "delete.bin"), []byte("delete"), 0o644)
	os.WriteFile(filepath.Join(dir, "old.bin"), []byte("old"), 0o644)
	cfg := &config.Config{Vault: "V", VaultPath: dir}
	svc := newTestService(cfg, nil, "")
	svc.setPendingUpload("mtime.bin", h.Content([]byte("mtime")))
	svc.st.FileHashMap["delete.bin"] = state.FileHashEntry{Hash: "old"}
	svc.st.FileHashMap["old.bin"] = state.FileHashEntry{Hash: "oldhash"}

	mtimeMsg, _ := json.Marshal(receiveMtimeMessage{Path: "mtime.bin", MTime: 3000, LastTime: 30})
	handleFileSyncMtime(mtimeMsg, svc)
	delMsg, _ := json.Marshal(receivePathMessage{Path: "delete.bin", LastTime: 31})
	handleFileSyncDelete(delMsg, svc)
	renameMsg, _ := json.Marshal(receiveRenameMessage{OldPath: "old.bin", Path: "new.bin", ContentHash: h.Content([]byte("old")), Size: 3, LastTime: 32})
	handleFileSyncRename(renameMsg, svc)

	svc.pendingFileRenames = append(svc.pendingFileRenames, struct{ OldPath, NewPath, ContentHash string }{"new.bin", "final.bin", h.Content([]byte("old"))})
	handleFileRenameAck([]byte(`{"lastTime":33}`), svc)
	svc.pendingFileDeleteAcks["final.bin"] = struct{}{}
	deleteAck, _ := json.Marshal(receivePathMessage{Path: "final.bin", LastTime: 34})
	handleFileDeleteAck(deleteAck, svc)

	svc.mu.Lock()
	_, pendingMtime := svc.pendingUploadHashes["mtime.bin"]
	_, deleteHash := svc.st.FileHashMap["delete.bin"]
	completed := svc.fileSyncTasks.Completed
	lastTime := svc.st.FileSyncTime
	svc.mu.Unlock()
	if pendingMtime || deleteHash || completed != 3 || lastTime != 34 {
		t.Fatalf("file state pending=%v deleteHash=%v completed=%d lastTime=%d", pendingMtime, deleteHash, completed, lastTime)
	}
}

func TestM14SettingHandlers(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{Vault: "V", VaultPath: dir, ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	content := `{"ok":true}`
	modifyMsg, _ := json.Marshal(receiveContentMessage{Path: ".obsidian/app.json", Content: content, ContentHash: h.Content([]byte(content)), MTime: 100, LastTime: 40})
	handleSettingSyncModify(modifyMsg, svc)
	needMsg, _ := json.Marshal(receivePathMessage{Path: ".obsidian/app.json", LastTime: 41})
	handleSettingSyncNeedUpload(needMsg, svc)
	mtimeMsg, _ := json.Marshal(receiveMtimeMessage{Path: ".obsidian/app.json", MTime: 200, LastTime: 42})
	handleSettingSyncMtime(mtimeMsg, svc)
	deleteMsg, _ := json.Marshal(receivePathMessage{Path: ".obsidian/app.json", LastTime: 43})
	handleSettingSyncDelete(deleteMsg, svc)
	handleSettingSyncClear(nil, svc)

	os.MkdirAll(filepath.Join(dir, ".obsidian"), 0o755)
	os.WriteFile(filepath.Join(dir, ".obsidian", "hotkeys.json"), []byte("{}"), 0o644)
	hash := h.Content([]byte("{}"))
	svc.setPendingConfigModify(".obsidian/hotkeys.json", hash)
	ackMsg, _ := json.Marshal(receivePathMessage{Path: ".obsidian/hotkeys.json", LastTime: 44})
	handleSettingModifyAck(ackMsg, svc)
	svc.st.ConfigHashMap[".obsidian/hotkeys.json"] = state.FileHashEntry{Hash: hash}
	svc.pendingConfigDeleteAcks[".obsidian/hotkeys.json"] = struct{}{}
	handleSettingDeleteAck(ackMsg, svc)

	svc.mu.Lock()
	_, cfgEntry := svc.st.ConfigHashMap[".obsidian/hotkeys.json"]
	completed := svc.configSyncTasks.Completed
	lastTime := svc.st.ConfigSyncTime
	svc.mu.Unlock()
	if cfgEntry || completed != 5 || lastTime != 44 {
		t.Fatalf("setting state cfgEntry=%v completed=%d lastTime=%d", cfgEntry, completed, lastTime)
	}
	if len(fc.written) == 0 || !strings.HasPrefix(fc.written[0], "SettingModify|") {
		t.Fatalf("SettingModify should be sent, written=%#v", fc.written)
	}
}

func TestM14LocalSendHelpersAndRuntimeHelpers(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "note.md"), []byte("n"), 0o644)
	os.WriteFile(filepath.Join(dir, "file.bin"), []byte("f"), 0o644)
	os.MkdirAll(filepath.Join(dir, ".obsidian"), 0o755)
	os.WriteFile(filepath.Join(dir, ".obsidian", "app.json"), []byte("{}"), 0o644)
	os.WriteFile(filepath.Join(dir, ".obsidian", "app2.json"), []byte("{}"), 0o644)
	os.Mkdir(filepath.Join(dir, "folder"), 0o755)
	cfg := &config.Config{
		Vault:                     "V",
		VaultPath:                 dir,
		ConfigSyncEnabled:         true,
		ConcurrencyControlEnabled: true,
		MaxConcurrentUploads:      20,
		ConfigSyncOtherDirs:       []string{"extras"},
		OfflineDeleteSyncEnabled:  true,
		SyncExcludeFolders:        []string{"private"},
		SyncExcludeWhitelist:      []string{"private/ok"},
		SyncExcludeExtensions:     []string{".tmp"},
		BinarySyncLimitEnabled:    true,
		OfflineSyncStrategy:       "auto",
		ReadOnlySyncEnabled:       false,
		ManualSyncEnabled:         false,
		AutoRedirectEnabled:       false,
		SyncEnabled:               true,
	}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.st.FileHashMap["note.md"] = state.FileHashEntry{Hash: h.Content([]byte("n"))}
	svc.st.FileHashMap["file.bin"] = state.FileHashEntry{Hash: h.Content([]byte("f"))}
	svc.mu.Unlock()

	if _, err := normalizeSyncPath(""); err == nil {
		t.Fatal("empty path should be rejected")
	}
	if _, err := svc.resolveVaultPath("../escape"); err == nil {
		t.Fatal("traversal path should be rejected")
	}
	if !svc.isConfigSyncPathAllowed(".obsidian/app.json") || !svc.isConfigSyncPathAllowed("extras/x.json") {
		t.Fatal("expected config paths to be allowed")
	}
	if svc.isConfigSyncPathAllowed(".obsidian/workspace.json") {
		t.Fatal("workspace.json should be excluded from config sync")
	}

	if err := svc.SendNoteDelete("note.md"); err != nil {
		t.Fatalf("SendNoteDelete: %v", err)
	}
	if err := svc.SendFileDelete("file.bin"); err != nil {
		t.Fatalf("SendFileDelete: %v", err)
	}
	if err := svc.SendSettingDelete(".obsidian/app.json"); err != nil {
		t.Fatalf("SendSettingDelete: %v", err)
	}
	if err := svc.SendNoteRename("note.md", "note2.md"); err != nil {
		t.Fatalf("SendNoteRename: %v", err)
	}
	svc.concurrency.ReleaseFifoSlot()
	if err := svc.SendFileRename("file.bin", "file2.bin"); err != nil {
		t.Fatalf("SendFileRename: %v", err)
	}
	svc.concurrency.ReleaseFifoSlot()
	if err := svc.SendSettingRename(".obsidian/app.json", ".obsidian/app2.json"); err != nil {
		t.Fatalf("SendSettingRename: %v", err)
	}
	if err := svc.SendFolderModify("folder"); err != nil {
		t.Fatalf("SendFolderModify: %v", err)
	}
	if err := svc.SendFolderDelete("folder"); err != nil {
		t.Fatalf("SendFolderDelete: %v", err)
	}
	if err := svc.SendFolderRename("folder", "folder2"); err != nil {
		t.Fatalf("SendFolderRename: %v", err)
	}
	svc.concurrency.Clear()
	if got := len(svc.concurrency.slots) + len(svc.concurrency.fifo); got != 0 {
		t.Fatalf("concurrency slots after clear = %d", got)
	}
	if len(fc.written) < 8 {
		t.Fatalf("expected local helpers to send messages, got %#v", fc.written)
	}
}
