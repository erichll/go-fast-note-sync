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

func TestHandleWatchOverflowQueuesAcrossInFlightSync(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		API:       "http://test.example.com",
		VaultPath: filepath.Join(dir, "no-such-vault"),
	}
	svc := newTestService(cfg, nil, filepath.Join(dir, "state.json"))
	svc.isSyncing = true
	svc.syncRoundID = 1

	svc.HandleWatchOverflow()
	svc.HandleWatchOverflow() // multiple overflows collapse to one pending recovery

	if !svc.overflowRescanPending {
		t.Fatal("overflow during an in-flight sync must be queued")
	}
	select {
	case <-svc.SyncComplete():
		t.Fatal("queued overflow must not start a second concurrent scan")
	case <-time.After(150 * time.Millisecond):
	}

	// Completing the in-flight round must start exactly one full recovery scan.
	// SyncComplete may already be closed by the first round, so wait on recovery
	// error / pending-flag instead of the once-channel.
	svc.onSyncComplete(1, true)

	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := svc.SyncError(); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queued overflow recovery did not start after the in-flight round finished")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if svc.overflowRescanPending {
		t.Fatal("overflow pending flag must clear after recovery starts")
	}
}
