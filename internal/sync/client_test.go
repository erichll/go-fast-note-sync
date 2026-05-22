package sync

import (
	"bytes"
	"errors"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gorillaws "github.com/gorilla/websocket"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

// waitPingExit waits for all pingLoop goroutines to exit within timeout.
// Callers must set s.isRegister = false before calling, and must ensure
// pingWG.Add(1) has already been called (poll controlWrites first) to avoid
// a concurrent Add/Wait race on the WaitGroup.
func (s *SyncService) waitPingExit(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() { s.pingWG.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// waitForFirstPing polls until conn has recorded at least one WriteControl call,
// confirming that pingLoop is running (and therefore pingWG.Add(1) has been called).
func waitForFirstPing(t *testing.T, f *fakeWSConn, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		n := len(f.controlWrites)
		f.mu.Unlock()
		if n >= 1 {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
	t.Fatal("pingLoop did not fire a WriteControl within timeout")
}

// newKeepaliveService builds a test SyncService wired for M1.9 keepalive tests.
func newKeepaliveService(t *testing.T, cfg *config.Config, dialer *countingDialer) *SyncService {
	t.Helper()
	if cfg == nil {
		cfg = &config.Config{API: "http://test.example.com", APIToken: "tok"}
	}
	svc := newTestService(cfg, state.New(), filepath.Join(t.TempDir(), "state.json"))
	svc.dialer = dialer
	svc.httpDoer = &fakeHTTPDoer{statusCode: 200}
	svc.sleepFn = func(time.Duration) {}
	svc.isRegister = true
	return svc
}

// TestKeepalive_TimeoutTriggersReconnect verifies that expiry of the read
// deadline (no pong received within pongWait) causes the client to reconnect.
func TestKeepalive_TimeoutTriggersReconnect(t *testing.T) {
	origPongWait, origPingInterval := pongWait, pingInterval
	t.Cleanup(func() { pongWait = origPongWait; pingInterval = origPingInterval })
	pongWait = 20 * time.Millisecond
	pingInterval = 1 * time.Millisecond // small so we can confirm Add(1) via controlWrites

	dialer := &countingDialer{
		onDial: make(chan struct{}, 32),
		factory: func() *fakeWSConn {
			f := newFakeWSConn()
			f.blockOnEmpty = true
			return f
		},
	}
	svc := newKeepaliveService(t, nil, dialer)
	go svc.connectOnce()

	// First dial.
	select {
	case <-dialer.onDial:
	case <-time.After(2 * time.Second):
		t.Fatal("first dial did not happen")
	}
	// Second dial after deadline expires (~20ms).
	select {
	case <-dialer.onDial:
	case <-time.After(2 * time.Second):
		t.Fatal("second dial did not happen after pongWait deadline expired")
	}

	if dialer.DialCount() < 2 {
		t.Errorf("dialCount = %d, want >= 2", dialer.DialCount())
	}

	// Wait for the second pingLoop to start (Add(1) called) before calling Wait().
	secondConn := dialer.LastConn()
	waitForFirstPing(t, secondConn, 200*time.Millisecond)

	svc.mu.Lock()
	svc.isRegister = false
	svc.mu.Unlock()
	secondConn.ExpireReadDeadlineNow()
	if !svc.waitPingExit(500 * time.Millisecond) {
		t.Fatal("ping goroutine leaked")
	}
}

// TestKeepalive_PongRefreshesDeadline verifies that receiving a pong resets
// the read deadline, preventing a premature reconnect.
func TestKeepalive_PongRefreshesDeadline(t *testing.T) {
	origPongWait, origPingInterval := pongWait, pingInterval
	t.Cleanup(func() { pongWait = origPongWait; pingInterval = origPingInterval })
	pongWait = 200 * time.Millisecond
	pingInterval = 1 * time.Millisecond // small so we confirm setup quickly

	f := newFakeWSConn()
	f.blockOnEmpty = true
	dialer := &countingDialer{
		onDial:  make(chan struct{}, 32),
		factory: func() *fakeWSConn { return f },
	}
	svc := newKeepaliveService(t, nil, dialer)
	go svc.connectOnce()

	select {
	case <-dialer.onDial:
	case <-time.After(2 * time.Second):
		t.Fatal("dial did not happen")
	}

	// Confirm pingLoop started (Add(1) called, setup complete) before TriggerPong.
	waitForFirstPing(t, f, 200*time.Millisecond)

	beforePong := time.Now()
	f.TriggerPong()

	// Deadline must have been refreshed to approximately now+pongWait.
	dl := f.GetReadDeadline()
	if dl.Before(beforePong.Add(pongWait / 2)) {
		t.Errorf("deadline after pong = %v, want >= %v", dl, beforePong.Add(pongWait/2))
	}

	// Push a normal text message; readLoop should consume it without exiting.
	f.PushMessage(wsTextMessage, []byte(`Authorization|{"code":200}`))
	time.Sleep(20 * time.Millisecond)

	// No reconnect should have occurred.
	if dialer.DialCount() != 1 {
		t.Errorf("dialCount = %d after pong; unexpected reconnect", dialer.DialCount())
	}

	svc.mu.Lock()
	svc.isRegister = false
	svc.mu.Unlock()
	f.ExpireReadDeadlineNow()
	if !svc.waitPingExit(500 * time.Millisecond) {
		t.Fatal("ping goroutine leaked")
	}
}

// TestKeepalive_ConcurrentWriteRace runs Send/SendBinary and pingLoop
// concurrently; go test -race must report no data races.
func TestKeepalive_ConcurrentWriteRace(t *testing.T) {
	origPongWait, origPingInterval, origWriteWait := pongWait, pingInterval, writeWait
	t.Cleanup(func() { pongWait = origPongWait; pingInterval = origPingInterval; writeWait = origWriteWait })
	pongWait = 10 * time.Second
	pingInterval = 1 * time.Millisecond
	writeWait = 10 * time.Millisecond

	f := newFakeWSConn()
	f.blockOnEmpty = true
	dialer := &countingDialer{
		onDial:  make(chan struct{}, 32),
		factory: func() *fakeWSConn { return f },
	}
	svc := newKeepaliveService(t, nil, dialer)
	go svc.connectOnce()

	select {
	case <-dialer.onDial:
	case <-time.After(2 * time.Second):
		t.Fatal("dial did not happen")
	}
	// Confirm pingLoop started before running concurrent sends.
	waitForFirstPing(t, f, 200*time.Millisecond)

	// Concurrent sends while pingLoop hammers WriteControl.
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		for i := 0; i < 200; i++ {
			svc.Send("Ping", "x")              //nolint:errcheck
			svc.SendBinary("00", []byte{0x01}) //nolint:errcheck
		}
	}()
	<-sendDone

	if dialer.DialCount() != 1 {
		t.Errorf("dialCount = %d; unexpected reconnect during race test", dialer.DialCount())
	}

	svc.mu.Lock()
	svc.isRegister = false
	svc.mu.Unlock()
	f.ExpireReadDeadlineNow()
	if !svc.waitPingExit(500 * time.Millisecond) {
		t.Fatal("ping goroutine leaked")
	}
}

// TestKeepalive_NoGoroutineLeak verifies that the pingLoop goroutine exits
// cleanly when the connection is closed (deadline expired).
func TestKeepalive_NoGoroutineLeak(t *testing.T) {
	origPongWait, origPingInterval := pongWait, pingInterval
	t.Cleanup(func() { pongWait = origPongWait; pingInterval = origPingInterval })
	pongWait = 10 * time.Second
	pingInterval = 10 * time.Millisecond

	f := newFakeWSConn()
	f.blockOnEmpty = true
	dialer := &countingDialer{
		onDial:  make(chan struct{}, 32),
		factory: func() *fakeWSConn { return f },
	}
	svc := newKeepaliveService(t, nil, dialer)
	go svc.connectOnce()

	select {
	case <-dialer.onDial:
	case <-time.After(2 * time.Second):
		t.Fatal("dial did not happen")
	}

	// Confirm pingLoop started (Add(1) called) before disabling reconnect + waitPingExit.
	waitForFirstPing(t, f, 200*time.Millisecond)

	svc.mu.Lock()
	svc.isRegister = false
	svc.mu.Unlock()
	f.ExpireReadDeadlineNow()

	if !svc.waitPingExit(500 * time.Millisecond) {
		t.Fatal("ping goroutine leaked after connection close")
	}
	if dialer.DialCount() != 1 {
		t.Errorf("dialCount = %d, want 1", dialer.DialCount())
	}
}

// TestKeepalive_WriteControlFailureNoReconnect verifies that a WriteControl
// failure in pingLoop only logs and does not itself trigger reconnect.
func TestKeepalive_WriteControlFailureNoReconnect(t *testing.T) {
	origPongWait, origPingInterval, origWriteWait := pongWait, pingInterval, writeWait
	t.Cleanup(func() { pongWait = origPongWait; pingInterval = origPingInterval; writeWait = origWriteWait })
	pongWait = 1 * time.Second
	pingInterval = 10 * time.Millisecond
	writeWait = 10 * time.Millisecond

	f := newFakeWSConn()
	f.blockOnEmpty = true
	f.writeControlErr = errors.New("write fail")
	dialer := &countingDialer{
		onDial:  make(chan struct{}, 32),
		factory: func() *fakeWSConn { return f },
	}
	svc := newKeepaliveService(t, nil, dialer)

	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(orig) })

	go svc.connectOnce()

	select {
	case <-dialer.onDial:
	case <-time.After(2 * time.Second):
		t.Fatal("dial did not happen")
	}

	// Poll until at least two ping attempts have been recorded.
	pollDeadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(pollDeadline) {
		f.mu.Lock()
		n := len(f.controlWrites)
		f.mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	f.mu.Lock()
	n := len(f.controlWrites)
	f.mu.Unlock()
	if n < 2 {
		t.Errorf("controlWrites = %d, want >= 2", n)
	}

	if dialer.DialCount() != 1 {
		t.Errorf("dialCount = %d; WriteControl failure must not trigger reconnect", dialer.DialCount())
	}

	// Stop pingLoop before reading buf to avoid a data race on bytes.Buffer.
	svc.mu.Lock()
	svc.isRegister = false
	svc.mu.Unlock()
	f.ExpireReadDeadlineNow()
	if !svc.waitPingExit(500 * time.Millisecond) {
		t.Fatal("ping goroutine leaked")
	}

	// pingLoop has exited; buf is no longer written concurrently.
	if !strings.Contains(buf.String(), "[ws] ping write:") {
		t.Errorf("expected '[ws] ping write:' in log, got: %q", buf.String())
	}
}

// TestKeepalive_InitialPongWaitNotRefreshedByNormalFrames verifies that the
// initial read deadline is wall-clock: only pong frames refresh it; ordinary
// text/binary frames do not.
func TestKeepalive_InitialPongWaitNotRefreshedByNormalFrames(t *testing.T) {
	origPongWait, origPingInterval := pongWait, pingInterval
	t.Cleanup(func() { pongWait = origPongWait; pingInterval = origPingInterval })
	pongWait = 50 * time.Millisecond
	pingInterval = 1 * time.Millisecond // small so we confirm second conn's Add(1) quickly

	dialer := &countingDialer{
		onDial: make(chan struct{}, 32),
		factory: func() *fakeWSConn {
			f := newFakeWSConn()
			f.blockOnEmpty = true
			return f
		},
	}
	svc := newKeepaliveService(t, nil, dialer)
	go svc.connectOnce()

	select {
	case <-dialer.onDial:
	case <-time.After(2 * time.Second):
		t.Fatal("first dial did not happen")
	}

	// Push a normal text frame immediately — must NOT refresh the deadline.
	firstConn := dialer.LastConn()
	firstConn.PushMessage(wsTextMessage, []byte(`ShareSyncRefresh|{"code":200}`))

	// Wait for the second dial (pongWait fires because no pong was received).
	select {
	case <-dialer.onDial:
	case <-time.After(2 * time.Second):
		t.Fatal("second dial did not happen; normal frame must not refresh deadline")
	}

	if dialer.DialCount() < 2 {
		t.Errorf("dialCount = %d, want >= 2", dialer.DialCount())
	}

	// Confirm second pingLoop started before waitPingExit.
	secondConn := dialer.LastConn()
	waitForFirstPing(t, secondConn, 200*time.Millisecond)

	svc.mu.Lock()
	svc.isRegister = false
	svc.mu.Unlock()
	secondConn.ExpireReadDeadlineNow()
	if !svc.waitPingExit(500 * time.Millisecond) {
		t.Fatal("ping goroutine leaked")
	}
}

// TestPingLoop_UsesWriteControl verifies that pingLoop sends pings via
// WriteControl (not WriteMessage).
func TestPingLoop_UsesWriteControl(t *testing.T) {
	origPingInterval := pingInterval
	t.Cleanup(func() { pingInterval = origPingInterval })
	pingInterval = 5 * time.Millisecond

	f := newFakeWSConn()
	f.blockOnEmpty = true
	dialer := &countingDialer{
		onDial:  make(chan struct{}, 32),
		factory: func() *fakeWSConn { return f },
	}
	svc := newKeepaliveService(t, nil, dialer)
	go svc.connectOnce()

	select {
	case <-dialer.onDial:
	case <-time.After(2 * time.Second):
		t.Fatal("dial did not happen")
	}

	waitForFirstPing(t, f, 200*time.Millisecond)

	f.mu.Lock()
	writes := make([]controlFrame, len(f.controlWrites))
	copy(writes, f.controlWrites)
	written := make([]string, len(f.written))
	copy(written, f.written)
	f.mu.Unlock()

	if len(writes) == 0 {
		t.Fatal("no WriteControl calls recorded; pingLoop must use WriteControl")
	}
	if writes[0].messageType != gorillaws.PingMessage {
		t.Errorf("messageType = %d, want PingMessage (%d)", writes[0].messageType, gorillaws.PingMessage)
	}
	for _, w := range written {
		if strings.Contains(strings.ToLower(w), "ping") {
			t.Errorf("ping was sent via WriteMessage instead of WriteControl: %q", w)
		}
	}

	svc.mu.Lock()
	svc.isRegister = false
	svc.mu.Unlock()
	f.ExpireReadDeadlineNow()
	if !svc.waitPingExit(500 * time.Millisecond) {
		t.Fatal("ping goroutine leaked")
	}
}
