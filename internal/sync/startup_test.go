package sync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

// ---- newUUID ----

func TestNewUUID_Format(t *testing.T) {
	id := newUUID()
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Errorf("UUID should have 5 parts, got %d: %q", len(parts), id)
	}
	if len(parts[0]) != 8 {
		t.Errorf("part[0] length = %d, want 8", len(parts[0]))
	}
}

func TestNewUUID_Unique(t *testing.T) {
	a, b := newUUID(), newUUID()
	if a == b {
		t.Error("consecutive UUIDs should not be equal")
	}
}

// ---- payload builders ----

func makeMinimalResult() *scanResult {
	r := newScanResult()
	r.notes = []SnapFile{{Path: "a.md", PathHash: "ph1", ContentHash: "ch1"}}
	r.files = []SnapFile{{Path: "img.png", PathHash: "ph2", ContentHash: "ch2"}}
	r.folders = []SnapFolder{{Path: "notes", PathHash: "fph"}}
	r.configs = []SnapFile{{Path: ".obsidian/app.json", PathHash: "cph", ContentHash: "cch"}}
	return r
}

func TestBuildFolderSyncPayload_Fields(t *testing.T) {
	r := makeMinimalResult()
	p := buildFolderSyncPayload("MyVault", 100, "ctx-1", r, false)

	if p["vault"] != "MyVault" {
		t.Errorf("vault = %v", p["vault"])
	}
	if p["lastTime"] != int64(100) {
		t.Errorf("lastTime = %v", p["lastTime"])
	}
	if p["context"] != "ctx-1" {
		t.Errorf("context = %v", p["context"])
	}
	if _, ok := p["delFolders"]; ok {
		t.Error("delFolders should not be present when offlineDel=false")
	}
}

func TestBuildFolderSyncPayload_WithOfflineDel(t *testing.T) {
	r := makeMinimalResult()
	r.delFolders = []PathHashFile{{Path: "old", PathHash: "oph"}}
	p := buildFolderSyncPayload("V", 0, "ctx", r, true)
	if _, ok := p["delFolders"]; !ok {
		t.Error("delFolders should be present when offlineDel=true")
	}
}

func TestBuildFolderSyncPayload_MissingFolders(t *testing.T) {
	r := makeMinimalResult()
	r.missingFolders = []PathHashFile{{Path: "gone", PathHash: "gph"}}
	p := buildFolderSyncPayload("V", 0, "ctx", r, false)
	if _, ok := p["missingFolders"]; !ok {
		t.Error("missingFolders should be present when non-empty")
	}
}

func TestBuildNoteSyncPayload_Fields(t *testing.T) {
	r := makeMinimalResult()
	p := buildNoteSyncPayload("V", 50, "ctx", r, false)
	if p["notes"] == nil {
		t.Error("notes should be present")
	}
	if _, ok := p["delNotes"]; ok {
		t.Error("delNotes should not be present when offlineDel=false")
	}
}

func TestBuildFileSyncPayload_Fields(t *testing.T) {
	r := makeMinimalResult()
	p := buildFileSyncPayload("V", 0, "ctx", r, false)
	if p["files"] == nil {
		t.Error("files should be present")
	}
}

func TestBuildSettingSyncPayload_Cover(t *testing.T) {
	r := makeMinimalResult()
	// lastTime=0 → cover=true
	p := buildSettingSyncPayload("V", 0, "ctx", r, false)
	if cover, ok := p["cover"].(bool); !ok || !cover {
		t.Error("cover should be true when lastTime=0")
	}
	// lastTime>0 → cover=false
	p2 := buildSettingSyncPayload("V", 100, "ctx", r, false)
	if cover, ok := p2["cover"].(bool); !ok || cover {
		t.Error("cover should be false when lastTime>0")
	}
}

func TestBuildSettingSyncPayload_FieldName(t *testing.T) {
	r := makeMinimalResult()
	p := buildSettingSyncPayload("V", 0, "ctx", r, false)
	if _, ok := p["settings"]; !ok {
		t.Error("field name must be 'settings', not 'configs'")
	}
}

// ---- isVaultFileExcluded ----

func newExclusionService(excludeFolders, excludeExts, whitelist []string) *SyncService {
	cfg := &config.Config{
		SyncExcludeFolders:    excludeFolders,
		SyncExcludeExtensions: excludeExts,
		SyncExcludeWhitelist:  whitelist,
	}
	return newTestService(cfg, nil, "")
}

func TestIsVaultFileExcluded_ExcludedFolder(t *testing.T) {
	svc := newExclusionService([]string{"private"}, nil, nil)
	if !svc.isVaultFileExcluded("private/note.md") {
		t.Error("file in excluded folder should be excluded")
	}
	if !svc.isVaultFileExcluded("private") {
		t.Error("excluded folder itself should be excluded")
	}
}

func TestIsVaultFileExcluded_ExcludedExtension(t *testing.T) {
	svc := newExclusionService(nil, []string{".tmp", "log"}, nil)
	if !svc.isVaultFileExcluded("data.tmp") {
		t.Error(".tmp should be excluded")
	}
	if !svc.isVaultFileExcluded("app.log") {
		t.Error(".log (without dot in config) should be excluded")
	}
}

func TestIsVaultFileExcluded_NotExcluded(t *testing.T) {
	svc := newExclusionService([]string{"private"}, []string{".tmp"}, nil)
	if svc.isVaultFileExcluded("notes/note.md") {
		t.Error("normal file should not be excluded")
	}
}

func TestIsVaultFileExcluded_WhitelistOverridesFolder(t *testing.T) {
	svc := newExclusionService([]string{"private"}, nil, []string{"private/allowed"})
	if svc.isVaultFileExcluded("private/allowed/note.md") {
		t.Error("whitelisted path should not be excluded even if folder is excluded")
	}
	if !svc.isVaultFileExcluded("private/other/note.md") {
		t.Error("non-whitelisted path under excluded folder should still be excluded")
	}
}

func TestIsFolderPathExcluded_Basic(t *testing.T) {
	svc := newExclusionService([]string{"archive"}, nil, nil)
	if !svc.isFolderPathExcluded("archive") {
		t.Error("archive folder should be excluded")
	}
	if !svc.isFolderPathExcluded("archive/2024") {
		t.Error("sub-folder of excluded folder should be excluded")
	}
	if svc.isFolderPathExcluded("notes") {
		t.Error("notes folder should not be excluded")
	}
}

// ---- folderSyncDone ----

func TestFolderSyncDone_NotDoneWhenFlagFalse(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.folderSyncEnd = false
	if svc.folderSyncDone() {
		t.Error("should not be done when folderSyncEnd=false")
	}
}

func TestFolderSyncDone_DoneWhenZeroTasks(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.folderSyncEnd = true
	// All need counts 0, Completed 0 → 0 >= 0 → done
	if !svc.folderSyncDone() {
		t.Error("should be done when all counts are 0")
	}
}

func TestFolderSyncDone_NotDoneWhenTasksRemain(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.folderSyncEnd = true
	svc.folderSyncTasks = SyncTaskCounter{NeedModify: 2, Completed: 1}
	if svc.folderSyncDone() {
		t.Error("should not be done when Completed < NeedModify")
	}
}

func TestFolderSyncDone_DoneWhenCompleted(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.folderSyncEnd = true
	svc.folderSyncTasks = SyncTaskCounter{NeedModify: 2, Completed: 2}
	if !svc.folderSyncDone() {
		t.Error("should be done when Completed >= sum of needs")
	}
}

// ---- isSyncComplete ----

func allSyncEndTrue(svc *SyncService) {
	svc.noteSyncEnd = true
	svc.fileSyncEnd = true
	svc.folderSyncEnd = true
	svc.configSyncEnd = true
}

func TestIsSyncComplete_AllDoneZeroTasks(t *testing.T) {
	cfg := &config.Config{ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	allSyncEndTrue(svc)
	if !svc.isSyncComplete() {
		t.Error("should be complete when all flags true and zero tasks")
	}
}

func TestIsSyncComplete_NotDoneWhenRequesting(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.isSyncRequesting = true
	allSyncEndTrue(svc)
	if svc.isSyncComplete() {
		t.Error("should not be complete while isSyncRequesting")
	}
}

func TestIsSyncComplete_NotDoneWhenFlagMissing(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.noteSyncEnd = true
	svc.fileSyncEnd = true
	svc.folderSyncEnd = false // missing
	if svc.isSyncComplete() {
		t.Error("should not be complete when folderSyncEnd=false")
	}
}

// SyncEnd=true but completed < total → not done.
func TestIsSyncComplete_SyncEndButTasksRemaining(t *testing.T) {
	cfg := &config.Config{ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	allSyncEndTrue(svc)
	svc.noteSyncTasks = SyncTaskCounter{NeedUpload: 3, Completed: 1} // incomplete
	if svc.isSyncComplete() {
		t.Error("should not be complete when SyncEnd=true but completed < total")
	}
}

func TestIsSyncComplete_ConfigDisabled(t *testing.T) {
	cfg := &config.Config{ConfigSyncEnabled: false}
	svc := newTestService(cfg, nil, "")
	svc.noteSyncEnd = true
	svc.fileSyncEnd = true
	svc.folderSyncEnd = true
	// configSyncEnd is false, but ConfigSyncEnabled=false → should still be complete
	if !svc.isSyncComplete() {
		t.Error("should be complete when ConfigSyncEnabled=false regardless of configSyncEnd")
	}
}

func TestIsSyncComplete_TasksRemaining(t *testing.T) {
	cfg := &config.Config{ConfigSyncEnabled: false}
	svc := newTestService(cfg, nil, "")
	allSyncEndTrue(svc)
	svc.noteSyncTasks = SyncTaskCounter{NeedUpload: 1, Completed: 0}
	if svc.isSyncComplete() {
		t.Error("should not be complete when noteSyncTasks not done")
	}
}

// ---- handleSync guards ----

func TestHandleSync_AlreadySyncing(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.isSyncing = true
	svc.handleSync(false)
	// No panic, isSyncing stays true (was already set)
	svc.mu.Lock()
	if !svc.isSyncing {
		t.Error("isSyncing should remain true")
	}
	svc.mu.Unlock()
}

func TestHandleSync_ManualMode(t *testing.T) {
	cfg := &config.Config{ManualSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	svc.handleSync(false)
	time.Sleep(20 * time.Millisecond)
	svc.mu.Lock()
	syncing := svc.isSyncing
	svc.mu.Unlock()
	if syncing {
		t.Error("isSyncing should be false in manual mode")
	}
}

func TestHandleSync_ResetsFlags(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.noteSyncEnd = true
	svc.fileSyncEnd = true
	svc.noteSyncTasks = SyncTaskCounter{Completed: 5}
	svc.handleSync(false)
	time.Sleep(20 * time.Millisecond)
	svc.mu.Lock()
	if svc.noteSyncEnd {
		t.Error("noteSyncEnd should be reset by handleSync")
	}
	if svc.noteSyncTasks.Completed != 0 {
		t.Error("noteSyncTasks should be reset by handleSync")
	}
	svc.mu.Unlock()
}

// ---- onSyncComplete ----

func TestOnSyncComplete_SetsIsInitSync(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	st := state.New()
	svc := newTestService(nil, st, statePath)
	svc.statePath = statePath

	svc.onSyncComplete(false) // wasInitSync=false → should set IsInitSync=true

	loaded, err := state.Load(statePath)
	if err != nil {
		t.Fatalf("state.Load: %v", err)
	}
	if !loaded.IsInitSync {
		t.Error("IsInitSync should be true after first sync completion")
	}
}

func TestOnSyncComplete_DoesNotUpdateWhenAlreadyInit(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.st.IsInitSync = false
	svc.onSyncComplete(true) // wasInitSync=true → no change needed
	// No panic, no state write (statePath is "")
}

// ---- scanVault ----

func TestScanVault_EmptyVaultPath(t *testing.T) {
	svc := newTestService(nil, nil, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.notes) != 0 || len(result.files) != 0 || len(result.folders) != 0 {
		t.Error("empty vaultPath should produce empty scan result")
	}
}

func TestScanVault_BasicClassification(t *testing.T) {
	dir := t.TempDir()
	// Create vault structure
	os.MkdirAll(filepath.Join(dir, "notes"), 0o755)
	os.MkdirAll(filepath.Join(dir, ".obsidian"), 0o755)
	os.WriteFile(filepath.Join(dir, "note.md"), []byte("# hello"), 0o644)
	os.WriteFile(filepath.Join(dir, "image.png"), []byte("PNG"), 0o644)
	os.WriteFile(filepath.Join(dir, ".obsidian", "app.json"), []byte("{}"), 0o644)
	os.WriteFile(filepath.Join(dir, ".obsidian", "workspace.json"), []byte("{}"), 0o644)
	os.WriteFile(filepath.Join(dir, "notes", "sub.md"), []byte("sub"), 0o644)

	cfg := &config.Config{VaultPath: dir, ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")

	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}

	// notes: note.md + notes/sub.md
	if len(result.notes) != 2 {
		t.Errorf("notes count = %d, want 2", len(result.notes))
	}
	// files: image.png
	if len(result.files) != 1 {
		t.Errorf("files count = %d, want 1", len(result.files))
	}
	// folders: notes/
	if len(result.folders) != 1 {
		t.Errorf("folders count = %d, want 1", len(result.folders))
	}
	// configs: app.json (workspace.json excluded)
	if len(result.configs) != 1 {
		t.Errorf("configs count = %d, want 1 (workspace.json must be excluded): %v", len(result.configs), func() []string {
			paths := make([]string, len(result.configs))
			for i, c := range result.configs {
				paths[i] = c.Path
			}
			return paths
		}())
	}
	if result.configs[0].Path != ".obsidian/app.json" {
		t.Errorf("config path = %q, want .obsidian/app.json", result.configs[0].Path)
	}
}

func TestScanVault_ExcludedFolder(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "private"), 0o755)
	os.WriteFile(filepath.Join(dir, "private", "secret.md"), []byte("secret"), 0o644)
	os.WriteFile(filepath.Join(dir, "public.md"), []byte("public"), 0o644)

	cfg := &config.Config{VaultPath: dir, SyncExcludeFolders: []string{"private"}}
	svc := newTestService(cfg, nil, "")

	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.notes) != 1 || result.notes[0].Path != "public.md" {
		t.Errorf("only public.md should appear, got %v", result.notes)
	}
}

func TestScanVault_BaseHashAndMissing(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "known.md"), []byte("content"), 0o644)
	os.WriteFile(filepath.Join(dir, "new.md"), []byte("new"), 0o644)

	st := state.New()
	st.FileHashMap["known.md"] = state.FileHashEntry{Hash: "oldhash", MTime: 0, Size: 7}

	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, st, "")

	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}

	var knownSnap, newSnap *SnapFile
	for i := range result.notes {
		if result.notes[i].Path == "known.md" {
			knownSnap = &result.notes[i]
		}
		if result.notes[i].Path == "new.md" {
			newSnap = &result.notes[i]
		}
	}
	if knownSnap == nil || newSnap == nil {
		t.Fatal("expected both known.md and new.md in notes")
	}
	if knownSnap.BaseHash == nil || *knownSnap.BaseHash != "oldhash" {
		t.Errorf("known.md should have BaseHash=oldhash, got %v", knownSnap.BaseHash)
	}
	if !newSnap.BaseHashMissing {
		t.Error("new.md should have BaseHashMissing=true")
	}
}

func TestScanVault_IncrementalSkipsUnchanged(t *testing.T) {
	dir := t.TempDir()
	// Write file and immediately capture its mtime
	absPath := filepath.Join(dir, "note.md")
	os.WriteFile(absPath, []byte("content"), 0o644)
	info, _ := os.Stat(absPath)
	mtime := info.ModTime().UnixMilli()

	st := state.New()
	st.FileHashMap["note.md"] = state.FileHashEntry{Hash: "somehash", MTime: mtime, Size: int64(len("content"))}
	st.NoteSyncTime = mtime + 1 // last sync was after file mtime

	cfg := &config.Config{VaultPath: dir}
	svc := newTestService(cfg, st, "")

	result, err := svc.scanVault(true) // isLoadLastTime=true
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.notes) != 0 {
		t.Errorf("incremental scan should skip unchanged note, got %v", result.notes)
	}
}

func TestScanVault_DelNotes(t *testing.T) {
	dir := t.TempDir()
	// File exists in hashMap but not on disk
	st := state.New()
	st.FileHashMap["deleted.md"] = state.FileHashEntry{Hash: "h", MTime: 0, Size: 0}

	cfg := &config.Config{VaultPath: dir, OfflineDeleteSyncEnabled: true}
	svc := newTestService(cfg, st, "")

	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.delNotes) != 1 || result.delNotes[0].Path != "deleted.md" {
		t.Errorf("deleted.md should appear in delNotes, got %v", result.delNotes)
	}
}

func TestScanVault_MissingNotesIncrementalNoOfflineDel(t *testing.T) {
	dir := t.TempDir()
	st := state.New()
	st.FileHashMap["gone.md"] = state.FileHashEntry{Hash: "h", MTime: 0, Size: 0}

	cfg := &config.Config{VaultPath: dir, OfflineDeleteSyncEnabled: false}
	svc := newTestService(cfg, st, "")

	result, err := svc.scanVault(true) // incremental
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.missingNotes) != 1 || result.missingNotes[0].Path != "gone.md" {
		t.Errorf("gone.md should appear in missingNotes: %v", result.missingNotes)
	}
	if len(result.delNotes) != 0 {
		t.Errorf("delNotes should be empty: %v", result.delNotes)
	}
}

// ---- saveState ----

func TestSaveState_EmptyPath(t *testing.T) {
	svc := newTestService(nil, nil, "")
	if err := svc.saveState(); err != nil {
		t.Errorf("saveState with empty path should not error: %v", err)
	}
}

func TestSaveState_WritesAndLoads(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	st := state.New()
	st.WsCount = 42
	svc := newTestService(nil, st, statePath)
	svc.statePath = statePath

	if err := svc.saveState(); err != nil {
		t.Fatalf("saveState: %v", err)
	}
	loaded, err := state.Load(statePath)
	if err != nil {
		t.Fatalf("state.Load: %v", err)
	}
	if loaded.WsCount != 42 {
		t.Errorf("WsCount = %d, want 42", loaded.WsCount)
	}
}

// ---- checkSyncCompletion timeout → onSyncComplete ----

func TestRunCheckSyncCompletion_TimeoutSetsIsInitSync(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	st := state.New()
	svc := newTestService(nil, st, statePath)
	svc.statePath = statePath
	svc.syncTimeout = 30 * time.Millisecond

	// All SyncEnd flags false → never complete → timeout fires onSyncComplete(false)
	svc.runCheckSyncCompletion(false)

	loaded, err := state.Load(statePath)
	if err != nil {
		t.Fatalf("state.Load: %v", err)
	}
	if !loaded.IsInitSync {
		t.Error("IsInitSync should be true after timeout with wasInitSync=false")
	}
}

func TestRunCheckSyncCompletion_CompletesEarly(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	svc := newTestService(nil, nil, statePath)
	svc.statePath = statePath
	svc.syncTimeout = 5 * time.Second // long timeout

	// Pre-set all conditions to done
	svc.noteSyncEnd = true
	svc.fileSyncEnd = true
	svc.folderSyncEnd = true
	svc.configSyncEnd = false // ConfigSyncEnabled=false → not required
	svc.st.IsInitSync = true  // wasInitSync=true → no persist needed

	done := make(chan struct{})
	go func() {
		svc.runCheckSyncCompletion(true)
		close(done)
	}()

	select {
	case <-done:
		// completed early — expected
	case <-time.After(2 * time.Second):
		t.Error("runCheckSyncCompletion should have completed early")
	}
}

// ---- sendSyncRequests payload delivery ----

func TestSendSyncRequests_SendsAllFourMessages(t *testing.T) {
	cfg := &config.Config{
		Vault:             "TestVault",
		ConfigSyncEnabled: true,
	}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	// Pre-set folderSyncDone so the wait loop exits immediately
	svc.folderSyncEnd = true

	result := newScanResult()
	svc.sendSyncRequests(result, "test-ctx", false)

	var actions []string
	for _, msg := range fc.written {
		if idx := strings.Index(msg, "|"); idx > 0 {
			actions = append(actions, msg[:idx])
		}
	}
	want := []string{"FolderSync", "NoteSync", "FileSync", "SettingSync"}
	for _, w := range want {
		found := false
		for _, a := range actions {
			if a == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected %q to be sent, got %v", w, actions)
		}
	}
}

func TestSendSyncRequests_FolderSyncFirst(t *testing.T) {
	cfg := &config.Config{Vault: "V", ConfigSyncEnabled: false}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()
	svc.folderSyncEnd = true

	svc.sendSyncRequests(newScanResult(), "ctx", false)

	if len(fc.written) < 1 {
		t.Fatal("no messages written")
	}
	first := fc.written[0]
	if !strings.HasPrefix(first, "FolderSync|") {
		t.Errorf("first message should be FolderSync, got %q", first)
	}
}

func TestSendSyncRequests_NoSettingSyncWhenDisabled(t *testing.T) {
	cfg := &config.Config{Vault: "V", ConfigSyncEnabled: false}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()
	svc.folderSyncEnd = true

	svc.sendSyncRequests(newScanResult(), "ctx", false)

	for _, msg := range fc.written {
		if strings.HasPrefix(msg, "SettingSync|") {
			t.Error("SettingSync should not be sent when ConfigSyncEnabled=false")
		}
	}
}

// ---- SnapFile JSON serialization ----

func TestSnapFile_BaseHashMissing(t *testing.T) {
	snap := SnapFile{
		Path:            "a.md",
		PathHash:        "ph",
		ContentHash:     "ch",
		MTime:           100,
		CTime:           100,
		Size:            10,
		BaseHashMissing: true,
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), "baseHashMissing") {
		t.Error("baseHashMissing should appear in JSON")
	}
	if strings.Contains(string(data), "baseHash\"") {
		t.Error("baseHash should not appear when nil")
	}
}

func TestSnapFile_BaseHash(t *testing.T) {
	baseHash := "oldhash"
	snap := SnapFile{
		Path:        "b.md",
		PathHash:    "ph",
		ContentHash: "ch",
		BaseHash:    &baseHash,
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"baseHash":"oldhash"`) {
		t.Errorf("baseHash should appear in JSON: %s", data)
	}
	if strings.Contains(string(data), "baseHashMissing") {
		t.Error("baseHashMissing should not appear when false")
	}
}

// ---- scanConfigs branches ----

func TestScanConfigs_NoObsidianDir(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.configs) != 0 {
		t.Errorf("no .obsidian dir should produce empty configs: %v", result.configs)
	}
}

func TestScanConfigs_PluginsDir(t *testing.T) {
	dir := t.TempDir()
	pluginsDir := filepath.Join(dir, ".obsidian", "plugins", "my-plugin")
	os.MkdirAll(pluginsDir, 0o755)
	os.WriteFile(filepath.Join(pluginsDir, "data.json"), []byte("{}"), 0o644)
	os.WriteFile(filepath.Join(pluginsDir, "main.js"), []byte("//"), 0o644)
	os.WriteFile(filepath.Join(pluginsDir, "styles.css"), []byte("p{}"), 0o644)
	os.WriteFile(filepath.Join(pluginsDir, "ignored.txt"), []byte("x"), 0o644)

	cfg := &config.Config{VaultPath: dir, ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	got := make(map[string]bool)
	for _, c := range result.configs {
		got[c.Path] = true
	}
	wantPaths := []string{
		".obsidian/plugins/my-plugin/data.json",
		".obsidian/plugins/my-plugin/main.js",
		".obsidian/plugins/my-plugin/styles.css",
	}
	for _, p := range wantPaths {
		if !got[p] {
			t.Errorf("plugin file %q not scanned, got %v", p, got)
		}
	}
	if got[".obsidian/plugins/my-plugin/ignored.txt"] {
		t.Error("unknown extension .txt should not be scanned in plugins dir")
	}
}

func TestScanConfigs_ExcludesFastNoteSyncPluginData(t *testing.T) {
	dir := t.TempDir()
	pluginDir := filepath.Join(dir, ".obsidian", "plugins", "fast-note-sync")
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "data.json"), []byte(`{"token":"secret"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "main.js"), []byte("// ok"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{VaultPath: dir, ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	got := make(map[string]bool)
	for _, c := range result.configs {
		got[c.Path] = true
	}
	if got[".obsidian/plugins/fast-note-sync/data.json"] {
		t.Fatal("fast-note-sync plugin data.json must not be scanned for SettingSync")
	}
	if !got[".obsidian/plugins/fast-note-sync/main.js"] {
		t.Fatal("non-sensitive fast-note-sync plugin file should still follow normal plugin rules")
	}
}

func TestScanConfigs_ThemesDir(t *testing.T) {
	dir := t.TempDir()
	themeDir := filepath.Join(dir, ".obsidian", "themes", "my-theme")
	os.MkdirAll(themeDir, 0o755)
	os.WriteFile(filepath.Join(themeDir, "theme.css"), []byte("body{}"), 0o644)
	os.WriteFile(filepath.Join(themeDir, "manifest.json"), []byte("{}"), 0o644)
	os.WriteFile(filepath.Join(themeDir, "skip.txt"), []byte("x"), 0o644)

	cfg := &config.Config{VaultPath: dir, ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	got := make(map[string]bool)
	for _, c := range result.configs {
		got[c.Path] = true
	}
	if !got[".obsidian/themes/my-theme/theme.css"] {
		t.Error("theme.css should be scanned")
	}
	if !got[".obsidian/themes/my-theme/manifest.json"] {
		t.Error("manifest.json should be scanned")
	}
	if got[".obsidian/themes/my-theme/skip.txt"] {
		t.Error(".txt should not be scanned in themes dir")
	}
}

func TestScanConfigs_SnippetsDir(t *testing.T) {
	dir := t.TempDir()
	snippetsDir := filepath.Join(dir, ".obsidian", "snippets")
	os.MkdirAll(snippetsDir, 0o755)
	os.WriteFile(filepath.Join(snippetsDir, "snip.css"), []byte(".a{}"), 0o644)
	os.WriteFile(filepath.Join(snippetsDir, "skip.json"), []byte("{}"), 0o644)

	cfg := &config.Config{VaultPath: dir, ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	got := make(map[string]bool)
	for _, c := range result.configs {
		got[c.Path] = true
	}
	if !got[".obsidian/snippets/snip.css"] {
		t.Error("snippet .css should be scanned")
	}
	if got[".obsidian/snippets/skip.json"] {
		t.Error(".json should not be scanned in snippets dir")
	}
}

func TestScanConfigs_OtherDirs(t *testing.T) {
	dir := t.TempDir()
	otherDir := filepath.Join(dir, "custom-config")
	os.MkdirAll(otherDir, 0o755)
	os.WriteFile(filepath.Join(otherDir, "any.txt"), []byte("hello"), 0o644)
	// Need .obsidian so scanConfigs is invoked; addConfig walks otherDir recursively.
	os.MkdirAll(filepath.Join(dir, ".obsidian"), 0o755)

	cfg := &config.Config{
		VaultPath:           dir,
		ConfigSyncEnabled:   true,
		ConfigSyncOtherDirs: []string{"custom-config"},
	}
	svc := newTestService(cfg, nil, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	found := false
	for _, c := range result.configs {
		if c.Path == "custom-config/any.txt" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("custom-config/any.txt should appear in configs: %v", result.configs)
	}
}

func TestScanConfigs_IncrementalSkipUnchanged(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, ".obsidian"), 0o755)
	cfgPath := filepath.Join(dir, ".obsidian", "app.json")
	os.WriteFile(cfgPath, []byte("{}"), 0o644)
	info, _ := os.Stat(cfgPath)
	mtime := info.ModTime().UnixMilli()

	st := state.New()
	st.ConfigHashMap[".obsidian/app.json"] = state.FileHashEntry{Hash: "h", MTime: mtime, Size: 2}
	st.ConfigSyncTime = mtime + 1

	cfg := &config.Config{VaultPath: dir, ConfigSyncEnabled: true}
	svc := newTestService(cfg, st, "")
	result, err := svc.scanVault(true)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	for _, c := range result.configs {
		if c.Path == ".obsidian/app.json" {
			t.Error("unchanged config file should be skipped in incremental mode")
		}
	}
}

func TestScanConfigs_BaseHashAttached(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, ".obsidian"), 0o755)
	os.WriteFile(filepath.Join(dir, ".obsidian", "app.json"), []byte("{}"), 0o644)

	st := state.New()
	st.ConfigHashMap[".obsidian/app.json"] = state.FileHashEntry{Hash: "oldhash", MTime: 0, Size: 2}

	cfg := &config.Config{VaultPath: dir, ConfigSyncEnabled: true}
	svc := newTestService(cfg, st, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	var snap *SnapFile
	for i := range result.configs {
		if result.configs[i].Path == ".obsidian/app.json" {
			snap = &result.configs[i]
		}
	}
	if snap == nil {
		t.Fatal("app.json missing from configs")
	}
	if snap.BaseHash == nil || *snap.BaseHash != "oldhash" {
		t.Errorf("BaseHash = %v, want oldhash", snap.BaseHash)
	}
}

// ---- computeFolderDelMissing branches ----

func TestComputeFolderDelMissing_DelOfflineEnabled(t *testing.T) {
	dir := t.TempDir()
	st := state.New()
	st.FolderSnapshot["gone-folder"] = 100

	cfg := &config.Config{VaultPath: dir, OfflineDeleteSyncEnabled: true}
	svc := newTestService(cfg, st, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.delFolders) != 1 || result.delFolders[0].Path != "gone-folder" {
		t.Errorf("delFolders should contain gone-folder, got %v", result.delFolders)
	}
}

func TestComputeFolderDelMissing_MissingIncremental(t *testing.T) {
	dir := t.TempDir()
	st := state.New()
	st.FolderSnapshot["gone-folder"] = 100

	cfg := &config.Config{VaultPath: dir, OfflineDeleteSyncEnabled: false}
	svc := newTestService(cfg, st, "")
	result, err := svc.scanVault(true) // incremental
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.missingFolders) != 1 || result.missingFolders[0].Path != "gone-folder" {
		t.Errorf("missingFolders should contain gone-folder, got %v", result.missingFolders)
	}
}

func TestComputeFolderDelMissing_ExcludedSkipped(t *testing.T) {
	dir := t.TempDir()
	st := state.New()
	st.FolderSnapshot["private/sub"] = 100

	cfg := &config.Config{
		VaultPath:                dir,
		SyncExcludeFolders:       []string{"private"},
		OfflineDeleteSyncEnabled: true,
	}
	svc := newTestService(cfg, st, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.delFolders) != 0 {
		t.Errorf("excluded folder should not appear in delFolders: %v", result.delFolders)
	}
}

func TestComputeFolderDelMissing_LocalFolderKept(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "kept"), 0o755)

	st := state.New()
	st.FolderSnapshot["kept"] = 100

	cfg := &config.Config{VaultPath: dir, OfflineDeleteSyncEnabled: true}
	svc := newTestService(cfg, st, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.delFolders) != 0 {
		t.Errorf("locally present folder should not appear in delFolders: %v", result.delFolders)
	}
}

// ---- computeConfigDelMissing branches ----

func TestComputeConfigDelMissing_DelOffline(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, ".obsidian"), 0o755)

	st := state.New()
	st.ConfigHashMap[".obsidian/gone.json"] = state.FileHashEntry{Hash: "h"}

	cfg := &config.Config{
		VaultPath:                dir,
		ConfigSyncEnabled:        true,
		OfflineDeleteSyncEnabled: true,
	}
	svc := newTestService(cfg, st, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.delConfigs) != 1 || result.delConfigs[0].Path != ".obsidian/gone.json" {
		t.Errorf("delConfigs should contain gone.json, got %v", result.delConfigs)
	}
}

func TestComputeConfigDelMissing_MissingIncremental(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, ".obsidian"), 0o755)

	st := state.New()
	st.ConfigHashMap[".obsidian/gone.json"] = state.FileHashEntry{Hash: "h"}

	cfg := &config.Config{
		VaultPath:                dir,
		ConfigSyncEnabled:        true,
		OfflineDeleteSyncEnabled: false,
	}
	svc := newTestService(cfg, st, "")
	result, err := svc.scanVault(true)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.missingConfigs) != 1 || result.missingConfigs[0].Path != ".obsidian/gone.json" {
		t.Errorf("missingConfigs should contain gone.json, got %v", result.missingConfigs)
	}
}

func TestComputeConfigDelMissing_SkipsSensitiveStaleState(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".obsidian"), 0o755); err != nil {
		t.Fatal(err)
	}

	st := state.New()
	st.ConfigHashMap[".obsidian/plugins/fast-note-sync/data.json"] = state.FileHashEntry{Hash: "secret"}

	cfg := &config.Config{
		VaultPath:                dir,
		ConfigSyncEnabled:        true,
		OfflineDeleteSyncEnabled: true,
	}
	svc := newTestService(cfg, st, "")
	result, err := svc.scanVault(false)
	if err != nil {
		t.Fatalf("scanVault: %v", err)
	}
	if len(result.delConfigs) != 0 || len(result.missingConfigs) != 0 {
		t.Fatalf("sensitive stale state should not emit del/missing settings: del=%v missing=%v", result.delConfigs, result.missingConfigs)
	}

	cfg.OfflineDeleteSyncEnabled = false
	result, err = svc.scanVault(true)
	if err != nil {
		t.Fatalf("scanVault incremental: %v", err)
	}
	if len(result.delConfigs) != 0 || len(result.missingConfigs) != 0 {
		t.Fatalf("sensitive stale state should stay silent in incremental mode: del=%v missing=%v", result.delConfigs, result.missingConfigs)
	}
}

// ---- payload builders: offlineDel / missing branches (companions to TestBuildFolderSyncPayload_*) ----

func TestBuildNoteSyncPayload_WithOfflineDel(t *testing.T) {
	r := newScanResult()
	r.delNotes = []PathHashFile{{Path: "deleted.md", PathHash: "h"}}
	p := buildNoteSyncPayload("V", 0, "ctx", r, true)
	if _, ok := p["delNotes"]; !ok {
		t.Error("delNotes should be present when offlineDel=true")
	}
}

func TestBuildFileSyncPayload_WithOfflineDel(t *testing.T) {
	r := newScanResult()
	r.delFiles = []PathHashFile{{Path: "old.png", PathHash: "h"}}
	p := buildFileSyncPayload("V", 0, "ctx", r, true)
	if _, ok := p["delFiles"]; !ok {
		t.Error("delFiles should be present when offlineDel=true")
	}
}

func TestBuildSettingSyncPayload_WithOfflineDel(t *testing.T) {
	r := newScanResult()
	r.delConfigs = []PathHashFile{{Path: ".obsidian/gone.json", PathHash: "h"}}
	p := buildSettingSyncPayload("V", 0, "ctx", r, true)
	if _, ok := p["delSettings"]; !ok {
		t.Error("delSettings should be present when offlineDel=true")
	}
}

func TestBuildNoteSyncPayload_MissingNotesIncluded(t *testing.T) {
	r := newScanResult()
	r.missingNotes = []PathHashFile{{Path: "g.md", PathHash: "h"}}
	p := buildNoteSyncPayload("V", 0, "ctx", r, false)
	if _, ok := p["missingNotes"]; !ok {
		t.Error("missingNotes should be present when non-empty")
	}
}

func TestBuildFileSyncPayload_MissingFilesIncluded(t *testing.T) {
	r := newScanResult()
	r.missingFiles = []PathHashFile{{Path: "g.png", PathHash: "h"}}
	p := buildFileSyncPayload("V", 0, "ctx", r, false)
	if _, ok := p["missingFiles"]; !ok {
		t.Error("missingFiles should be present when non-empty")
	}
}

func TestBuildSettingSyncPayload_MissingSettingsIncluded(t *testing.T) {
	r := newScanResult()
	r.missingConfigs = []PathHashFile{{Path: ".obsidian/g.json", PathHash: "h"}}
	p := buildSettingSyncPayload("V", 0, "ctx", r, false)
	if _, ok := p["missingSettings"]; !ok {
		t.Error("missingSettings should be present when non-empty")
	}
}

// ---- sendSyncRequests: side-effect paths ----

func TestSendSyncRequests_OfflineDeleteFillsPending(t *testing.T) {
	cfg := &config.Config{
		Vault:                    "V",
		ConfigSyncEnabled:        true,
		OfflineDeleteSyncEnabled: true,
	}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()
	svc.folderSyncEnd = true

	r := newScanResult()
	r.delNotes = []PathHashFile{{Path: "del.md", PathHash: "h"}}
	r.delFiles = []PathHashFile{{Path: "del.png", PathHash: "h"}}
	r.delConfigs = []PathHashFile{{Path: ".obsidian/del.json", PathHash: "h"}}
	r.delFolders = []PathHashFile{{Path: "del-folder", PathHash: "h"}}

	svc.sendSyncRequests(r, "ctx", false)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if _, ok := svc.pendingDeleteNotePaths["del.md"]; !ok {
		t.Error("pendingDeleteNotePaths should contain del.md")
	}
	if _, ok := svc.pendingDeleteFilePaths["del.png"]; !ok {
		t.Error("pendingDeleteFilePaths should contain del.png")
	}
	if _, ok := svc.pendingDeleteConfigPaths[".obsidian/del.json"]; !ok {
		t.Error("pendingDeleteConfigPaths should contain .obsidian/del.json")
	}
	if _, ok := svc.pendingDeleteFolderPaths["del-folder"]; !ok {
		t.Error("pendingDeleteFolderPaths should contain del-folder")
	}
}

func TestSendSyncRequests_WritesFolderSnapshot(t *testing.T) {
	cfg := &config.Config{Vault: "V"}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()
	svc.folderSyncEnd = true

	r := newScanResult()
	r.folders = []SnapFolder{{Path: "fA", PathHash: "h"}, {Path: "fB", PathHash: "h2"}}

	svc.sendSyncRequests(r, "ctx", false)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if _, ok := svc.st.FolderSnapshot["fA"]; !ok {
		t.Error("FolderSnapshot should contain fA")
	}
	if _, ok := svc.st.FolderSnapshot["fB"]; !ok {
		t.Error("FolderSnapshot should contain fB")
	}
}

func TestSendSyncRequests_PopulatesPendingModifies(t *testing.T) {
	cfg := &config.Config{Vault: "V", ConfigSyncEnabled: true}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()
	svc.folderSyncEnd = true

	r := newScanResult()
	r.notes = []SnapFile{{Path: "a.md", PathHash: "p", ContentHash: "ch-note"}}
	r.configs = []SnapFile{{Path: ".obsidian/app.json", PathHash: "p", ContentHash: "ch-cfg"}}

	svc.sendSyncRequests(r, "ctx", false)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if got := svc.pendingNoteModifies["a.md"]; got != "ch-note" {
		t.Errorf("pendingNoteModifies[a.md] = %q, want ch-note", got)
	}
	if got := svc.pendingConfigModifies[".obsidian/app.json"]; got != "ch-cfg" {
		t.Errorf("pendingConfigModifies = %q, want ch-cfg", got)
	}
}

func TestSendSyncRequests_IncrementalUsesStateLastTime(t *testing.T) {
	cfg := &config.Config{Vault: "V"}
	st := state.New()
	st.NoteSyncTime = 1111
	st.FileSyncTime = 2222
	st.FolderSyncTime = 3333
	st.ConfigSyncTime = 4444
	svc := newTestService(cfg, st, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()
	svc.folderSyncEnd = true

	svc.sendSyncRequests(newScanResult(), "ctx", true)

	hasLastTime := func(action string, want int64) {
		for _, msg := range fc.written {
			if !strings.HasPrefix(msg, action+"|") {
				continue
			}
			body := msg[len(action)+1:]
			var parsed map[string]any
			if err := json.Unmarshal([]byte(body), &parsed); err != nil {
				t.Fatalf("parse %s payload: %v", action, err)
			}
			got, _ := parsed["lastTime"].(float64)
			if int64(got) != want {
				t.Errorf("%s lastTime = %v, want %d", action, got, want)
			}
			return
		}
		t.Errorf("action %s not sent", action)
	}
	hasLastTime("FolderSync", 3333)
	hasLastTime("NoteSync", 1111)
	hasLastTime("FileSync", 2222)
}

// ---- saveState mkdir + atomic write (nested parent dir) ----

func TestSaveState_CreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "deep", "nested", "state.json")
	st := state.New()
	st.WsCount = 7
	svc := newTestService(nil, st, nested)
	svc.statePath = nested
	if err := svc.saveState(); err != nil {
		t.Fatalf("saveState: %v", err)
	}
	loaded, err := state.Load(nested)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.WsCount != 7 {
		t.Errorf("WsCount = %d, want 7", loaded.WsCount)
	}
}

// ---- SyncComplete tests ----

func TestSyncComplete_ClosedAfterOnSyncComplete(t *testing.T) {
	svc := newTestService(nil, nil, "")

	select {
	case <-svc.SyncComplete():
		t.Fatal("SyncComplete channel should not be closed before onSyncComplete")
	default:
	}

	svc.onSyncComplete(false)

	select {
	case <-svc.SyncComplete():
	case <-time.After(time.Second):
		t.Fatal("SyncComplete channel was not closed after onSyncComplete")
	}
}

func TestSyncComplete_IdempotentMultipleCalls(t *testing.T) {
	svc := newTestService(nil, nil, "")

	// Calling onSyncComplete twice must not panic (syncDoneOnce guards close).
	svc.onSyncComplete(true)
	svc.onSyncComplete(true)

	select {
	case <-svc.SyncComplete():
	default:
		t.Fatal("SyncComplete channel should be closed after first onSyncComplete")
	}
}

func TestSyncComplete_NewSyncServiceInitialized(t *testing.T) {
	cfg := config.Default()
	cfg.API = "http://example.com"
	svc := NewSyncService(cfg, state.New(), "/tmp/test.json", "1.0.0")

	if svc.SyncComplete() == nil {
		t.Fatal("SyncComplete() channel should not be nil after NewSyncService")
	}
	select {
	case <-svc.SyncComplete():
		t.Fatal("SyncComplete channel should not be closed on fresh service")
	default:
	}
}
