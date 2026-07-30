package sync

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/config"
)

// A watcher overflow means an unknown number of filesystem events were dropped,
// so the only safe response is a full re-scan. These tests pin the sync-side
// half of that contract: the watcher reports the overflow, the service acts.

func TestHandleWatchOverflowStartsVaultScan(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		API: "http://test.example.com",
		// Deliberately absent: the scan fails fast, which is a deterministic
		// signal that a scan was actually attempted.
		VaultPath: filepath.Join(dir, "no-such-vault"),
	}
	svc := newTestService(cfg, nil, filepath.Join(dir, "state.json"))

	svc.HandleWatchOverflow()

	select {
	case <-svc.SyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatal("HandleWatchOverflow did not trigger a vault scan")
	}

	if svc.SyncError() == nil {
		t.Fatal("expected the scan of a missing vault to report an error")
	}
}

func TestHandleWatchOverflowRespectsManualSyncMode(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		API:               "http://test.example.com",
		VaultPath:         filepath.Join(dir, "no-such-vault"),
		ManualSyncEnabled: true,
	}
	svc := newTestService(cfg, nil, filepath.Join(dir, "state.json"))

	svc.HandleWatchOverflow()

	select {
	case <-svc.SyncComplete():
		t.Fatal("manual sync mode must not be overridden by a watcher overflow")
	case <-time.After(200 * time.Millisecond):
	}
}
