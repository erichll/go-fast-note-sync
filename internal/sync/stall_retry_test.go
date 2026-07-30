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
	svc.isRegister = true

	svc.runCheckSyncCompletion(false)

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
	svc.isRegister = false

	svc.runCheckSyncCompletion(false)

	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if closed {
		t.Error("a deregistered service must not be forced to reconnect")
	}
}
