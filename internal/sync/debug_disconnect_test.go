package sync

import (
	"testing"
	"time"
)

func TestSetDebugDisconnectAfterClosesOwningConnection(t *testing.T) {
	svc := newTestService(nil, nil, "")
	conn := newFakeWSConn()
	svc.conn = conn
	svc.isRegister = true
	svc.SetDebugDisconnectAfter(20 * time.Millisecond)
	svc.scheduleDebugDisconnect(conn)

	deadline := time.Now().Add(time.Second)
	for {
		conn.mu.Lock()
		closed := conn.closed
		conn.mu.Unlock()
		if closed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("debug disconnect did not close the owning connection")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSetDebugDisconnectAfterIgnoresReplacedConnection(t *testing.T) {
	svc := newTestService(nil, nil, "")
	oldConn := newFakeWSConn()
	newConn := newFakeWSConn()
	svc.conn = oldConn
	svc.isRegister = true
	svc.SetDebugDisconnectAfter(20 * time.Millisecond)
	svc.scheduleDebugDisconnect(oldConn)

	// Replace the live connection before the timer fires.
	svc.mu.Lock()
	svc.conn = newConn
	svc.mu.Unlock()

	time.Sleep(80 * time.Millisecond)
	oldConn.mu.Lock()
	oldClosed := oldConn.closed
	oldConn.mu.Unlock()
	newConn.mu.Lock()
	newClosed := newConn.closed
	newConn.mu.Unlock()
	if oldClosed {
		t.Fatal("debug disconnect must not close a connection that is no longer current")
	}
	if newClosed {
		t.Fatal("debug disconnect must not close a newer connection it does not own")
	}
}

func TestSetDebugDisconnectAfterZeroIsNoop(t *testing.T) {
	svc := newTestService(nil, nil, "")
	conn := newFakeWSConn()
	svc.conn = conn
	svc.isRegister = true
	svc.SetDebugDisconnectAfter(0)
	svc.scheduleDebugDisconnect(conn)
	time.Sleep(30 * time.Millisecond)
	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if closed {
		t.Fatal("zero debug disconnect duration must not close the connection")
	}
}
