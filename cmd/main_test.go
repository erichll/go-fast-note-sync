package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/local"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

func execCmd(args ...string) error {
	cmd := newRootCmd()
	cmd.SetArgs(args)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd.Execute()
}

func TestHelpOutput(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"--help"})
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("--help returned error: %v", err)
	}
	if !strings.Contains(buf.String(), "go-fast-note-sync") {
		t.Errorf("help output missing program name, got: %s", buf.String())
	}
}

func TestStartLoadConfigError(t *testing.T) {
	// Pointing --config at a non-existent path should surface the load error.
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist.yaml")
	err := execCmd("start", "--config", missing)
	if err == nil {
		t.Fatal("expected error when config file is missing")
	}
	if !strings.Contains(err.Error(), "load config") {
		t.Errorf("error should mention 'load config', got: %v", err)
	}
}

func TestStartLoadStateError(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	statePath := filepath.Join(dir, "state.json")

	// Generate a config and point state_file at a corrupted file.
	if err := execCmd("init-config", "--config", cfgPath); err != nil {
		t.Fatalf("init-config: %v", err)
	}
	data, _ := os.ReadFile(cfgPath)
	updated := strings.Replace(string(data), "state_file: \"\"", "state_file: \""+statePath+"\"", 1)
	if err := os.WriteFile(cfgPath, []byte(updated), 0o600); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}
	// Corrupt state file.
	if err := os.WriteFile(statePath, []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("write corrupt state: %v", err)
	}

	err := execCmd("start", "--config", cfgPath)
	if err == nil {
		t.Fatal("expected state-load error")
	}
	if !strings.Contains(err.Error(), "load state") {
		t.Errorf("error should mention 'load state', got: %v", err)
	}
}

func TestStartHappyPath(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	statePath := filepath.Join(dir, "state.json")

	// Generate a config file then point state_file at our temp dir.
	if err := execCmd("init-config", "--config", cfgPath); err != nil {
		t.Fatalf("init-config: %v", err)
	}
	// Rewrite the config with API='' (so connectOnce hits build-URL error and exits the goroutine fast)
	// and state_file pointed at our temp path.
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	updated := strings.Replace(string(data), "state_file: \"\"", "state_file: \""+statePath+"\"", 1)
	if err := os.WriteFile(cfgPath, []byte(updated), 0o600); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}

	// Override waitForSignal so start returns immediately.
	orig := waitForSignal
	defer func() { waitForSignal = orig }()
	waitForSignal = func() os.Signal { return syscall.SIGINT }

	if err := execCmd("start", "--config", cfgPath); err != nil {
		t.Fatalf("start happy path: %v", err)
	}
}

type fakeDaemon struct {
	connected  bool
	syncDoneCh chan struct{}
}

func (f *fakeDaemon) Connect() {
	f.connected = true
}

func (f *fakeDaemon) SyncComplete() <-chan struct{} {
	if f.syncDoneCh == nil {
		f.syncDoneCh = make(chan struct{})
	}
	return f.syncDoneCh
}

func (f *fakeDaemon) ShouldWatchDir(string) bool {
	return true
}

func (f *fakeDaemon) HandleLocalModify(local.PathEvent) local.Result {
	return local.Result{}
}

func (f *fakeDaemon) HandleLocalDelete(local.PathEvent) local.Result {
	return local.Result{}
}

func (f *fakeDaemon) HandleLocalRename(local.RenameEvent) local.Result {
	return local.Result{}
}

type fakeLocalWatcher struct {
	closed bool
}

func (f *fakeLocalWatcher) Close() error {
	f.closed = true
	return nil
}

func TestStartWatcherGatedBySyncEnabledAndVaultPath(t *testing.T) {
	origWait := waitForSignal
	origSync := newSyncDaemon
	origWatcher := newLocalWatcher
	defer func() {
		waitForSignal = origWait
		newSyncDaemon = origSync
		newLocalWatcher = origWatcher
	}()
	waitForSignal = func() os.Signal { return syscall.SIGTERM }
	newSyncDaemon = func(_ *config.Config, _ *state.State, _, _ string) syncDaemon {
		return &fakeDaemon{}
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	statePath := filepath.Join(dir, "state.json")
	if err := execCmd("init-config", "--config", cfgPath); err != nil {
		t.Fatalf("init-config: %v", err)
	}
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	vaultPath := filepath.Join(dir, "vault")
	if err := os.MkdirAll(vaultPath, 0o755); err != nil {
		t.Fatal(err)
	}
	updated := string(data)
	updated = strings.Replace(updated, "state_file: \"\"", "state_file: \""+statePath+"\"", 1)
	updated = strings.Replace(updated, "vault_path: \"\"", "vault_path: \""+vaultPath+"\"", 1)
	if err := os.WriteFile(cfgPath, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}

	var started bool
	w := &fakeLocalWatcher{}
	newLocalWatcher = func(path string, delay int, handler local.Handler) (localWatcher, error) {
		started = true
		if path != vaultPath {
			t.Fatalf("watcher path = %q, want %q", path, vaultPath)
		}
		if delay <= 0 {
			t.Fatalf("watcher delay = %d, want positive", delay)
		}
		if handler == nil {
			t.Fatal("handler is nil")
		}
		return w, nil
	}
	if err := execCmd("start", "--config", cfgPath); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !started {
		t.Fatal("watcher was not started")
	}
	if !w.closed {
		t.Fatal("watcher was not closed on shutdown")
	}

	started = false
	w.closed = false
	data, err = os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	disabled := strings.Replace(string(data), "sync_enabled: true", "sync_enabled: false", 1)
	if err := os.WriteFile(cfgPath, []byte(disabled), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := execCmd("start", "--config", cfgPath); err != nil {
		t.Fatalf("start disabled: %v", err)
	}
	if started {
		t.Fatal("watcher started while sync_enabled=false")
	}
}

func TestStatusCmd(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T, dir string) (cfgPath string)
		wantErr    string
		wantOutput []string
	}{
		{
			name: "missing config returns load config error",
			setup: func(t *testing.T, dir string) string {
				return filepath.Join(dir, "does-not-exist.yaml")
			},
			wantErr: "load config",
		},
		{
			name: "missing state file prints never for timestamps",
			setup: func(t *testing.T, dir string) string {
				cfgPath := filepath.Join(dir, "config.yaml")
				if err := execCmd("init-config", "--config", cfgPath); err != nil {
					t.Fatalf("init-config: %v", err)
				}
				return cfgPath
			},
			wantOutput: []string{
				"vault:",
				"api:",
				"note_sync_time: never",
				"file_sync_time: never",
				"config_sync_time: never",
				"folder_sync_time: never",
				"ws_count: 0",
				"is_init_sync: false",
			},
		},
		{
			name: "populated state shows counts and timestamps",
			setup: func(t *testing.T, dir string) string {
				cfgPath := filepath.Join(dir, "config.yaml")
				statePath := filepath.Join(dir, "state.json")
				if err := execCmd("init-config", "--config", cfgPath); err != nil {
					t.Fatalf("init-config: %v", err)
				}
				data, _ := os.ReadFile(cfgPath)
				updated := strings.Replace(string(data), "state_file: \"\"", "state_file: \""+statePath+"\"", 1)
				if err := os.WriteFile(cfgPath, []byte(updated), 0o600); err != nil {
					t.Fatalf("rewrite config: %v", err)
				}
				st := &state.State{
					FileHashMap:           map[string]state.FileHashEntry{"a.md": {}, "b.png": {}},
					ConfigHashMap:         map[string]state.FileHashEntry{".obsidian/app.json": {}},
					FolderSnapshot:        map[string]int64{"notes": 1000},
					PendingNoteModifies:   map[string]string{"c.md": "hash"},
					PendingUploadHashes:   make(map[string]string),
					PendingConfigModifies: make(map[string]string),
					UploadCheckpoints:     make(map[string]state.UploadCheckpoint),
					NoteSyncTime:          1748908800000,
					WsCount:               3,
					IsInitSync:            true,
				}
				if err := state.Save(statePath, st); err != nil {
					t.Fatalf("save state: %v", err)
				}
				return cfgPath
			},
			wantOutput: []string{
				"note_cache: 1",
				"file_cache: 1",
				"setting_cache: 1",
				"folder_cache: 1",
				"ws_count: 3",
				"is_init_sync: true",
				"pending_note_modifies: 1",
				"note_sync_time: 2025-06-03T00:00:00Z",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfgPath := tc.setup(t, dir)

			cmd := newRootCmd()
			cmd.SetArgs([]string{"status", "--config", cfgPath})
			cmd.SilenceErrors = true
			cmd.SilenceUsage = true
			var out bytes.Buffer
			cmd.SetOut(&out)
			err := cmd.Execute()

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, want := range tc.wantOutput {
				if !strings.Contains(out.String(), want) {
					t.Errorf("output missing %q\nfull output:\n%s", want, out.String())
				}
			}
		})
	}
}

func TestSyncCmd_LoadConfigError(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist.yaml")
	err := execCmd("sync", "--config", missing)
	if err == nil || !strings.Contains(err.Error(), "load config") {
		t.Errorf("expected 'load config' error, got %v", err)
	}
}

func TestSyncCmd_Timeout(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	statePath := filepath.Join(dir, "state.json")
	if err := execCmd("init-config", "--config", cfgPath); err != nil {
		t.Fatalf("init-config: %v", err)
	}
	data, _ := os.ReadFile(cfgPath)
	updated := strings.Replace(string(data), "state_file: \"\"", "state_file: \""+statePath+"\"", 1)
	if err := os.WriteFile(cfgPath, []byte(updated), 0o600); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}

	origSync := newSyncDaemon
	defer func() { newSyncDaemon = origSync }()
	fd := &fakeDaemon{}
	newSyncDaemon = func(_ *config.Config, _ *state.State, _, _ string) syncDaemon {
		return fd
	}

	err := execCmd("sync", "--config", cfgPath, "--timeout", "20ms")
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("error should mention 'timed out', got: %v", err)
	}
}

func TestSyncCmd_Success(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	statePath := filepath.Join(dir, "state.json")
	if err := execCmd("init-config", "--config", cfgPath); err != nil {
		t.Fatalf("init-config: %v", err)
	}
	data, _ := os.ReadFile(cfgPath)
	updated := strings.Replace(string(data), "state_file: \"\"", "state_file: \""+statePath+"\"", 1)
	if err := os.WriteFile(cfgPath, []byte(updated), 0o600); err != nil {
		t.Fatalf("rewrite config: %v", err)
	}

	origSync := newSyncDaemon
	defer func() { newSyncDaemon = origSync }()
	done := make(chan struct{})
	close(done)
	fd := &fakeDaemon{syncDoneCh: done}
	newSyncDaemon = func(_ *config.Config, _ *state.State, _, _ string) syncDaemon {
		return fd
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"sync", "--config", cfgPath})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("sync success path: %v", err)
	}
	if !strings.Contains(out.String(), "Sync complete.") {
		t.Errorf("output should contain 'Sync complete.', got: %s", out.String())
	}
	if !fd.connected {
		t.Error("Connect() should have been called")
	}
}

func TestInitConfigWritesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	cmd := newRootCmd()
	cmd.SetArgs([]string{"init-config", "--config", path})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("init-config: unexpected error: %v", err)
	}
	if !strings.Contains(out.String(), path) {
		t.Errorf("expected output to mention path %q, got: %s", path, out.String())
	}
}

func TestInitConfigExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	// Create file first so second call fails
	cmd := newRootCmd()
	cmd.SetArgs([]string{"init-config", "--config", path})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	if err := cmd.Execute(); err != nil {
		t.Fatalf("first init-config: %v", err)
	}
	cmd2 := newRootCmd()
	cmd2.SetArgs([]string{"init-config", "--config", path})
	cmd2.SilenceErrors = true
	cmd2.SilenceUsage = true
	if err := cmd2.Execute(); err == nil {
		t.Error("expected error when config already exists")
	}
}
