package sync

import (
	"testing"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/config"
	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/local"
)

func TestEchoWindowMatchesWatcherFallback(t *testing.T) {
	s, _, _ := newLocalEventService(t)
	for _, delay := range []int{-1, 0, 100, 500, 2000} {
		s.cfg.SyncUpdateDelay = delay
		effective := delay
		if effective <= 0 {
			effective = config.Default().SyncUpdateDelay
		}
		want := time.Duration(effective)*time.Millisecond + echoSuppressDelay
		if got := s.echoSuppressWindow(); got != want {
			t.Fatalf("delay=%d window=%v want=%v", delay, got, want)
		}
	}
}

func TestEchoDeferredOldVersionDoesNotUpload(t *testing.T) {
	s, c, v := newLocalEventService(t)
	writeVaultFile(t, v, "note.md", "old")
	token := s.beginRemoteWrite("note.md", h.Text("old"), 3)
	s.mu.Lock()
	s.finishRemoteWriteLocked("note.md", token, h.Text("old"), 1, 3)
	s.mu.Unlock()
	token = s.beginRemoteWrite("note.md", h.Text("new"), 3)
	if r := s.HandleLocalModify(local.PathEvent{Path: "note.md"}); r.Attempted || r.Err != nil {
		t.Fatalf("old callback was not deferred: %+v", r)
	}
	writeVaultFile(t, v, "note.md", "new")
	s.mu.Lock()
	s.finishRemoteWriteLocked("note.md", token, h.Text("new"), 2, 3)
	s.mu.Unlock()
	waitForLocalRechecks(t, s)
	if len(c.Written()) != 0 {
		t.Fatalf("deferred remote echo uploaded: %v", c.Written())
	}
}

func TestEchoLocalReplayParticipatesInCompletion(t *testing.T) {
	s, _, _ := newLocalEventService(t)
	s.mu.Lock()
	s.noteSyncEnd = true
	s.fileSyncEnd = true
	s.folderSyncEnd = true
	s.configSyncEnd = true
	s.mu.Unlock()
	if !s.isSyncComplete() {
		t.Fatal("empty completed service not ready")
	}
	unlock := s.lockPath("note.md")
	s.mu.Lock()
	s.recheckLocalWriteLocked("note.md")
	s.mu.Unlock()
	if s.isSyncComplete() {
		unlock()
		t.Fatal("sync completed while a local recheck was blocked")
	}
	if p := s.syncProgressSnapshot(); p.localRechecks != 1 {
		unlock()
		t.Fatalf("progress omitted replay: %+v", p)
	}
	unlock()
	waitForLocalRechecks(t, s)
	if !s.isSyncComplete() {
		t.Fatal("drained replay still prevents completion")
	}
}

func TestEchoInflightUnknownHashDoesNotSuppressLocalEdit(t *testing.T) {
	s, c, v := newLocalEventService(t)
	writeVaultFile(t, v, "note.md", "local bytes")
	token := s.beginRemoteWrite("note.md", "", 0)
	r := s.HandleLocalModify(local.PathEvent{Path: "note.md"})
	s.abortRemoteWrite("note.md", token)
	if !r.Attempted || r.Err != nil || firstAction(t, c) != "NoteModify" {
		t.Fatalf("unknown in-flight hash swallowed bytes: result=%+v writes=%v", r, c.Written())
	}
}

// Refreshing a marker must preserve the newer expiration deadline.
func TestEchoMarkerRefreshPreservesNewDeadline(t *testing.T) {
	svc, _, _ := newLocalEventService(t)
	svc.cfg.SyncUpdateDelay = 1
	svc.addIgnoredFile("note.md")
	time.Sleep(350 * time.Millisecond)
	refresh := time.Now()
	svc.addIgnoredFile("note.md")
	// At ~200ms after refresh (550ms after initial set), the first
	// 501ms deadline passed, but the refreshed marker must still be present.
	time.Sleep(200 * time.Millisecond)
	svc.mu.Lock()
	_, presentAfterOldTimer := svc.lastSyncMtime["note.md"]
	svc.mu.Unlock()
	if !presentAfterOldTimer {
		t.Fatalf("older timer prematurely cleared refreshed marker after %s", time.Since(refresh))
	}
	// Wait for the refreshed timer itself to expire.
	deadline := refresh.Add(svc.echoSuppressWindow() + 250*time.Millisecond)
	cleared := false
	for time.Now().Before(deadline) {
		svc.mu.Lock()
		_, present := svc.lastSyncMtime["note.md"]
		svc.mu.Unlock()
		if !present {
			cleared = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cleared {
		t.Fatal("refreshed marker did not clear after its own window expired")
	}
}
