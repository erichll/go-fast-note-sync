package sync

import (
	"testing"
	"time"
)

// A stalled round used to end the daemon's sync life: isSyncing went false, the
// error was recorded, and the goroutine returned. Nothing re-triggers a round
// except authentication or a watcher overflow, so a healthy connection would sit
// idle forever. Dropping the connection routes the stall into the reconnect path
// that already exists.
func TestRunCheckSyncCompletion_StallDropsTheConnectionSoReconnectRetries(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.syncTimeout = 30 * time.Millisecond
	conn := newFakeWSConn()
	svc.conn = conn
	svc.syncRoundConn = conn
	svc.isRegister = true
	svc.isSyncing = true
	svc.syncRoundID = 1

	svc.runCheckSyncCompletion(1, false)

	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if !closed {
		t.Error("a stalled sync must drop the connection so the reconnect path runs")
	}
	if svc.SyncError() == nil {
		t.Error("the stall must still be reported")
	}
}

// An unregistered service is shutting down; reconnecting would fight the exit.
func TestRunCheckSyncCompletion_StallLeavesAnUnregisteredServiceAlone(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.syncTimeout = 30 * time.Millisecond
	conn := newFakeWSConn()
	svc.conn = conn
	svc.syncRoundConn = conn
	svc.isRegister = false
	svc.isSyncing = true
	svc.syncRoundID = 1

	svc.runCheckSyncCompletion(1, false)

	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if closed {
		t.Error("a deregistered service must not be forced to reconnect")
	}
}

// A stale completion goroutine must not close a newer round's connection or
// mutate the newer round's isSyncing/syncErr state.
func TestRunCheckSyncCompletion_StaleTimeoutDoesNotAffectNewerRound(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.syncTimeout = 30 * time.Millisecond

	oldConn := newFakeWSConn()
	newConn := newFakeWSConn()
	svc.conn = newConn
	svc.syncRoundConn = newConn
	svc.isRegister = true
	svc.isSyncing = true
	svc.syncRoundID = 2 // newer round already owns the service

	// Stale checker for round 1 must exit without touching the new connection.
	svc.runCheckSyncCompletion(1, false)

	oldConn.mu.Lock()
	oldClosed := oldConn.closed
	oldConn.mu.Unlock()
	newConn.mu.Lock()
	newClosed := newConn.closed
	newConn.mu.Unlock()
	if oldClosed {
		t.Error("stale timeout must not close a connection it no longer owns")
	}
	if newClosed {
		t.Error("stale timeout must not close the newer round's connection")
	}
	if !svc.isSyncing || svc.syncRoundID != 2 {
		t.Fatal("stale timeout must not clear the newer round")
	}
	if svc.SyncError() != nil {
		t.Fatalf("stale timeout must not set syncErr on the newer round: %v", svc.SyncError())
	}
}
