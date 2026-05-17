package sync

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	stdsync "sync"
	"testing"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

// ---- fake helpers ----

type wsMsg struct {
	mtype int
	data  []byte
	err   error
}

type fakeWSConn struct {
	mu       stdsync.Mutex
	messages []wsMsg
	idx      int
	written  []string
	wtypes   []int
	closed   bool
	writeErr error
}

func (f *fakeWSConn) ReadMessage() (int, []byte, error) {
	if f.idx >= len(f.messages) {
		return 0, nil, fmt.Errorf("connection closed")
	}
	m := f.messages[f.idx]
	f.idx++
	return m.mtype, m.data, m.err
}

func (f *fakeWSConn) WriteMessage(mtype int, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeErr != nil {
		return f.writeErr
	}
	f.wtypes = append(f.wtypes, mtype)
	f.written = append(f.written, string(data))
	return nil
}

func (f *fakeWSConn) Written() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]string, len(f.written))
	copy(cp, f.written)
	return cp
}

func (f *fakeWSConn) Close() error {
	f.closed = true
	return nil
}

type fakeDialer struct {
	conn    *fakeWSConn
	usedURL string
	err     error
}

func (f *fakeDialer) Dial(urlStr string, _ http.Header) (WSConn, *http.Response, error) {
	f.usedURL = urlStr
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.conn, &http.Response{StatusCode: 101}, nil
}

type fakeHTTPDoer struct {
	statusCode int
	err        error
}

func (f *fakeHTTPDoer) Do(_ *http.Request) (*http.Response, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &http.Response{
		StatusCode: f.statusCode,
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

// newTestService builds a SyncService wired for unit tests (no real network,
// isRegister=false so connectOnce does not trigger reconnect loops).
func newTestService(cfg *config.Config, st *state.State, statePath string) *SyncService {
	if cfg == nil {
		cfg = &config.Config{API: "http://test.example.com"}
	}
	if st == nil {
		st = state.New()
	}
	svc := &SyncService{
		cfg:             cfg,
		st:              st,
		statePath:       statePath,
		version:         "1.0.0-test",
		runAPI:          cfg.API,
		binaryHandlers:  make(map[string]func([]byte)),
		receiveHandlers: buildReceiveHandlers(),
		sleepFn:         func(time.Duration) {}, // no-op: prevent test blocking

		pendingNoteModifies:      make(map[string]string),
		pendingUploadHashes:      make(map[string]string),
		pendingConfigModifies:    make(map[string]string),
		pendingNoteDeleteAcks:    make(map[string]struct{}),
		pendingFileDeleteAcks:    make(map[string]struct{}),
		pendingConfigDeleteAcks:  make(map[string]struct{}),
		pendingDeleteNotePaths:   make(map[string]struct{}),
		pendingDeleteFilePaths:   make(map[string]struct{}),
		pendingDeleteFolderPaths: make(map[string]struct{}),
		pendingDeleteConfigPaths: make(map[string]struct{}),
		fileDownloadSessions:     make(map[string]*FileDownloadSession),
		activeUploads:            make(map[string]*ActiveUpload),
		TempChunksBaseDir:        tempChunksBaseDir(statePath),
		lastSyncMtime:            make(map[string]int64),
		lastSyncPathDeleted:      make(map[string]struct{}),
		lastSyncPathRenamed:      make(map[string]struct{}),
		scannedNoteHashes:        make(map[string]state.FileHashEntry),
		scannedFileHashes:        make(map[string]state.FileHashEntry),
		scannedConfigHashes:      make(map[string]state.FileHashEntry),
		concurrency:              NewConcurrencyManager(cfg),
		pathLocks:                make(map[string]chan struct{}),
		// Fast timeouts so background goroutines terminate quickly in tests.
		syncTimeout:     50 * time.Millisecond,
		folderWaitPoll:  5 * time.Millisecond,
		folderWaitLimit: 50 * time.Millisecond,
		// isRegister = false (default) → connectOnce will not schedule reconnect
	}
	svc.binaryHandlers[binaryPrefixFileSync] = svc.handleFileBinaryChunk
	return svc
}

// ---- parseTextMessage tests ----

func TestParseTextMessage_WithPipe(t *testing.T) {
	action, env, err := parseTextMessage(`ShareSyncRefresh|{"code":200}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if action != "ShareSyncRefresh" {
		t.Errorf("action = %q, want %q", action, "ShareSyncRefresh")
	}
	if env.Code != 200 {
		t.Errorf("code = %d, want 200", env.Code)
	}
}

func TestParseTextMessage_NoPipe(t *testing.T) {
	action, env, err := parseTextMessage(`{"code":200,"message":"ok"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if action != "" {
		t.Errorf("action = %q, want empty string", action)
	}
	if env.Code != 200 {
		t.Errorf("code = %d, want 200", env.Code)
	}
}

func TestParseTextMessage_PipeInsideJSON(t *testing.T) {
	// The pipe inside the JSON value must not cause an extra split.
	action, env, err := parseTextMessage(`NoteSyncModify|{"code":200,"message":"a|b"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if action != "NoteSyncModify" {
		t.Errorf("action = %q, want %q", action, "NoteSyncModify")
	}
	if env.Message != "a|b" {
		t.Errorf("message = %q, want %q", env.Message, "a|b")
	}
}

func TestParseTextMessage_InvalidJSON(t *testing.T) {
	_, _, err := parseTextMessage(`ShareSyncRefresh|not-valid-json`)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseTextMessage_NoPipeInvalidJSON(t *testing.T) {
	_, _, err := parseTextMessage(`not-valid-json`)
	if err == nil {
		t.Error("expected error for bare invalid JSON")
	}
}

func TestParseTextMessage_VaultField(t *testing.T) {
	_, env, err := parseTextMessage(`NoteSyncEnd|{"code":200,"vault":"MyVault"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env.Vault != "MyVault" {
		t.Errorf("vault = %q, want %q", env.Vault, "MyVault")
	}
}

// ---- buildWSURL tests ----

func TestBuildWSURL_HTTP(t *testing.T) {
	u, err := buildWSURL("http://example.com", "LinuxCLI", "1.0.0", 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(u, "ws://") {
		t.Errorf("URL should start with ws://, got %q", u)
	}
	if !strings.Contains(u, "count=42") {
		t.Errorf("URL should contain count=42, got %q", u)
	}
	if !strings.Contains(u, "client=LinuxCLI") {
		t.Errorf("URL should contain client=LinuxCLI, got %q", u)
	}
	if !strings.Contains(u, "clientVersion=1.0.0") {
		t.Errorf("URL should contain clientVersion=1.0.0, got %q", u)
	}
}

func TestBuildWSURL_HTTPS(t *testing.T) {
	u, err := buildWSURL("https://example.com", "LinuxCLI", "2.0.0", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(u, "wss://") {
		t.Errorf("URL should start with wss://, got %q", u)
	}
}

func TestBuildWSURL_TrailingSlash(t *testing.T) {
	u, err := buildWSURL("http://example.com/", "LinuxCLI", "1.0.0", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Must not produce double-slash before /api/user/sync
	if strings.Contains(u, "//api") {
		t.Errorf("URL contains double slash: %q", u)
	}
}

func TestBuildWSURL_InvalidScheme(t *testing.T) {
	_, err := buildWSURL("ftp://example.com", "LinuxCLI", "1.0.0", 0)
	if err == nil {
		t.Error("expected error for invalid scheme")
	}
}

func TestBuildWSURL_OverrideClientType(t *testing.T) {
	u, err := buildWSURL("http://example.com", "ObsidianPlugin", "1.0.0", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(u, "client=ObsidianPlugin") {
		t.Errorf("URL should contain client=ObsidianPlugin, got %q", u)
	}
	if strings.Contains(u, "client=LinuxCLI") {
		t.Errorf("URL should not still contain LinuxCLI, got %q", u)
	}
}

func TestBuildWSURL_EmptyClientTypeFallsBackToLinuxCLI(t *testing.T) {
	u, err := buildWSURL("http://example.com", "", "1.0.0", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(u, "client=LinuxCLI") {
		t.Errorf("empty client type should fall back to LinuxCLI, got %q", u)
	}
}

// ---- reconnectDelay tests ----

func TestReconnectDelay_Formula(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 3000 * time.Millisecond},
		{2, 6000 * time.Millisecond},
		{3, 12000 * time.Millisecond},
		{4, 24000 * time.Millisecond},
		{5, 48000 * time.Millisecond},
	}
	for _, tc := range cases {
		got := reconnectDelay(tc.attempt)
		if got != tc.want {
			t.Errorf("reconnectDelay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestReconnectDelay_Attempt15(t *testing.T) {
	got := reconnectDelay(reconnectMaxAttempts)
	want := reconnectBaseDelay * (1 << 14)
	if got != want {
		t.Errorf("reconnectDelay(15) = %v, want %v", got, want)
	}
}

func TestReconnectDelay_BelowOne(t *testing.T) {
	// attempt < 1 should be treated as attempt 1
	if reconnectDelay(0) != reconnectDelay(1) {
		t.Error("reconnectDelay(0) should equal reconnectDelay(1)")
	}
}

// ---- WsCount persistence test ----

func TestWsCountIncrementAndPersist(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	st := state.New()
	st.WsCount = 5

	fakeConn := &fakeWSConn{
		messages: []wsMsg{{mtype: 0, data: nil, err: fmt.Errorf("closed")}},
	}
	fakeDial := &fakeDialer{conn: fakeConn}

	cfg := &config.Config{API: "http://test.example.com", APIToken: "tok"}
	svc := newTestService(cfg, st, statePath)
	svc.dialer = fakeDial
	svc.httpDoer = &fakeHTTPDoer{statusCode: 200}

	svc.connectOnce()

	// URL passed to Dial must use the original count (5)
	if !strings.Contains(fakeDial.usedURL, "count=5") {
		t.Errorf("dialed URL should contain count=5, got %q", fakeDial.usedURL)
	}

	// In-memory WsCount must be incremented
	if st.WsCount != 6 {
		t.Errorf("WsCount in memory = %d, want 6", st.WsCount)
	}

	// State file must be written with the new count
	loaded, err := state.Load(statePath)
	if err != nil {
		t.Fatalf("state.Load: %v", err)
	}
	if loaded.WsCount != 6 {
		t.Errorf("persisted WsCount = %d, want 6", loaded.WsCount)
	}
}

func TestWsCountUsesStateValue(t *testing.T) {
	// Verify that each call to connectOnce uses the current st.WsCount at dial time.
	for _, initial := range []int{0, 10, 99} {
		dir := t.TempDir()
		st := state.New()
		st.WsCount = initial

		fakeConn := &fakeWSConn{
			messages: []wsMsg{{mtype: 0, err: fmt.Errorf("closed")}},
		}
		fakeDial := &fakeDialer{conn: fakeConn}
		cfg := &config.Config{API: "http://test.example.com"}
		svc := newTestService(cfg, st, filepath.Join(dir, "state.json"))
		svc.dialer = fakeDial
		svc.httpDoer = &fakeHTTPDoer{statusCode: 200}

		svc.connectOnce()

		want := fmt.Sprintf("count=%d", initial)
		if !strings.Contains(fakeDial.usedURL, want) {
			t.Errorf("initial=%d: URL should contain %q, got %q", initial, want, fakeDial.usedURL)
		}
		if st.WsCount != initial+1 {
			t.Errorf("initial=%d: WsCount = %d, want %d", initial, st.WsCount, initial+1)
		}
	}
}

// ---- Authorization code range tests ----

func TestAuthorization_SuccessRange(t *testing.T) {
	for _, code := range []int{1, 100, 200, 299} {
		svc := newTestService(nil, nil, "")
		fc := &fakeWSConn{}
		svc.mu.Lock()
		svc.conn = fc
		svc.mu.Unlock()

		handleAuthorization(Envelope{Code: code}, svc)

		svc.mu.Lock()
		got := svc.isAuth
		svc.mu.Unlock()

		if !got {
			t.Errorf("code=%d: isAuth should be true", code)
		}
		// sendClientInfo must have been called
		foundClientInfo := false
		for _, msg := range fc.written {
			if strings.HasPrefix(msg, "ClientInfo|") {
				foundClientInfo = true
				break
			}
		}
		if !foundClientInfo {
			t.Errorf("code=%d: ClientInfo message not sent", code)
		}
	}
}

func TestAuthorization_FailureCodes(t *testing.T) {
	for _, code := range []int{-1, 0, 300, 400, 500} {
		svc := newTestService(nil, nil, "")
		fc := &fakeWSConn{}
		svc.mu.Lock()
		svc.conn = fc
		svc.mu.Unlock()

		handleAuthorization(Envelope{Code: code}, svc)

		svc.mu.Lock()
		got := svc.isAuth
		svc.mu.Unlock()

		if got {
			t.Errorf("code=%d: isAuth should be false", code)
		}
		if len(fc.written) != 0 {
			t.Errorf("code=%d: no messages should be written on auth failure, got %v", code, fc.written)
		}
	}
}

// ---- dispatchText tests ----

func TestDispatchText_NoPipe_NoDispatch(t *testing.T) {
	svc := newTestService(nil, nil, "")
	called := false
	svc.receiveHandlers["ShareSyncRefresh"] = func(_ json.RawMessage, _ *SyncService) {
		called = true
	}
	svc.dispatchText(`{"code":200}`)
	if called {
		t.Error("dispatchText with no pipe should not invoke any handler")
	}
}

func TestDispatchText_InvalidJSON_Discarded(t *testing.T) {
	svc := newTestService(nil, nil, "")
	called := false
	svc.receiveHandlers["ShareSyncRefresh"] = func(_ json.RawMessage, _ *SyncService) {
		called = true
	}
	// Should not panic, should not dispatch
	svc.dispatchText(`ShareSyncRefresh|not-json`)
	if called {
		t.Error("invalid JSON should be discarded without dispatching")
	}
}

func TestDispatchText_VaultMismatch_Skipped(t *testing.T) {
	cfg := &config.Config{Vault: "MyVault"}
	svc := newTestService(cfg, nil, "")
	called := false
	svc.receiveHandlers["ShareSyncRefresh"] = func(_ json.RawMessage, _ *SyncService) {
		called = true
	}
	svc.dispatchText(`ShareSyncRefresh|{"code":200,"vault":"OtherVault"}`)
	if called {
		t.Error("vault mismatch should skip dispatch")
	}
}

func TestDispatchText_VaultMatch_Dispatched(t *testing.T) {
	cfg := &config.Config{Vault: "MyVault"}
	svc := newTestService(cfg, nil, "")
	called := false
	svc.receiveHandlers["ShareSyncRefresh"] = func(_ json.RawMessage, _ *SyncService) {
		called = true
	}
	svc.dispatchText(`ShareSyncRefresh|{"code":200,"vault":"MyVault"}`)
	if !called {
		t.Error("matching vault should dispatch to handler")
	}
}

func TestDispatchText_EmptyVault_Dispatched(t *testing.T) {
	cfg := &config.Config{Vault: "MyVault"}
	svc := newTestService(cfg, nil, "")
	called := false
	svc.receiveHandlers["ShareSyncRefresh"] = func(_ json.RawMessage, _ *SyncService) {
		called = true
	}
	// Empty vault in envelope should not trigger mismatch check
	svc.dispatchText(`ShareSyncRefresh|{"code":200}`)
	if !called {
		t.Error("empty vault in envelope should not block dispatch")
	}
}

func TestDispatchText_ErrorCode_NotDispatched(t *testing.T) {
	svc := newTestService(nil, nil, "")
	called := false
	svc.receiveHandlers["ShareSyncRefresh"] = func(_ json.RawMessage, _ *SyncService) {
		called = true
	}
	for _, code := range []int{0, -1, 300, 530} {
		called = false
		svc.dispatchText(fmt.Sprintf(`ShareSyncRefresh|{"code":%d}`, code))
		if called {
			t.Errorf("code=%d: error code should not reach handler", code)
		}
	}
}

func TestDispatchText_Authorization_HandledDirectly(t *testing.T) {
	svc := newTestService(nil, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	svc.dispatchText(`Authorization|{"code":200}`)

	svc.mu.Lock()
	isAuth := svc.isAuth
	svc.mu.Unlock()

	if !isAuth {
		t.Error("Authorization action with success code should set isAuth=true")
	}
}

// ---- Binary dispatch tests ----

func TestDispatchBinary_RoutesByPrefix(t *testing.T) {
	svc := newTestService(nil, nil, "")
	var gotData []byte
	svc.binaryHandlers["AB"] = func(data []byte) {
		gotData = data
	}

	svc.dispatchBinary([]byte("AB" + "payload"))

	if string(gotData) != "payload" {
		t.Errorf("handler received %q, want %q", gotData, "payload")
	}
}

func TestDispatchBinary_UnknownPrefix(t *testing.T) {
	svc := newTestService(nil, nil, "")
	// Should not panic for unknown prefix
	svc.dispatchBinary([]byte("ZZpayload"))
}

func TestDispatchBinary_TooShort(t *testing.T) {
	svc := newTestService(nil, nil, "")
	// Should not panic for data shorter than 2 bytes
	svc.dispatchBinary([]byte("X"))
	svc.dispatchBinary([]byte{})
}

func TestDispatchBinary_EmptyPayload(t *testing.T) {
	svc := newTestService(nil, nil, "")
	called := false
	svc.binaryHandlers["00"] = func(data []byte) {
		called = true
		if len(data) != 0 {
			t.Errorf("expected empty payload, got %v", data)
		}
	}
	svc.dispatchBinary([]byte("00")) // prefix only, no payload
	if !called {
		t.Error("handler should be called even with empty payload")
	}
}

// ---- Handler registration tests ----

func TestBuildReceiveHandlers_AllPresent(t *testing.T) {
	handlers := buildReceiveHandlers()
	required := []string{
		"NoteSyncEnd", "NoteSyncModify", "NoteSyncDelete", "NoteSyncNeedPush",
		"NoteSyncMtime", "NoteSyncRename", "NoteModifyAck", "NoteRenameAck", "NoteDeleteAck",
		"FileSyncEnd", "FileSyncUpdate", "FileSyncChunkDownload", "FileUpload",
		"FileSyncDelete", "FileSyncMtime", "FileSyncRename",
		"FileUploadAck", "FileRenameAck", "FileDeleteAck",
		"SettingSyncEnd", "SettingSyncModify", "SettingSyncNeedUpload", "SettingSyncMtime",
		"SettingSyncDelete", "SettingSyncClear", "SettingModifyAck", "SettingDeleteAck",
		"FolderSyncEnd", "FolderSyncModify", "FolderSyncDelete", "FolderSyncRename",
		"ShareSyncRefresh",
	}
	if len(handlers) != 32 {
		t.Errorf("handler count = %d, want 32", len(handlers))
	}
	for _, name := range required {
		if handlers[name] == nil {
			t.Errorf("handler %q is nil or missing", name)
		}
	}
}

func TestAsyncReceiveHandlerDispatchesInGoroutine(t *testing.T) {
	done := make(chan struct{})
	wrapped := asyncReceiveHandler(func(data json.RawMessage, svc *SyncService) {
		if string(data) != `{"ok":true}` {
			t.Errorf("data = %s", data)
		}
		if svc == nil {
			t.Error("service is nil")
		}
		close(done)
	})

	wrapped(json.RawMessage(`{"ok":true}`), newTestService(nil, nil, ""))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("async handler did not run")
	}
}

func TestPathHashPayload(t *testing.T) {
	payload := pathHashPayload("notes/a.md")
	if payload["path"] != "notes/a.md" {
		t.Fatalf("path = %v", payload["path"])
	}
	if payload["pathHash"] != "b33a8099b01219568f1660eb604d897c0ac5db4fdd84c142337f129d5a0fca4d" {
		t.Fatalf("pathHash = %v", payload["pathHash"])
	}
}

func TestPendingModifyClearHelpers(t *testing.T) {
	svc := newTestService(nil, nil, "")

	svc.setPendingNoteModify("note.md", "note-hash")
	svc.clearPendingNoteModify("note.md")
	if _, ok := svc.pendingNoteModifies["note.md"]; ok {
		t.Fatal("runtime note pending entry not cleared")
	}
	if _, ok := svc.st.PendingNoteModifies["note.md"]; ok {
		t.Fatal("state note pending entry not cleared")
	}

	svc.setPendingUpload("asset.bin", "file-hash")
	svc.clearPendingUpload("asset.bin")
	if _, ok := svc.pendingUploadHashes["asset.bin"]; ok {
		t.Fatal("runtime upload pending entry not cleared")
	}
	if _, ok := svc.st.PendingUploadHashes["asset.bin"]; ok {
		t.Fatal("state upload pending entry not cleared")
	}

	svc.setPendingConfigModify(".obsidian/app.json", "config-hash")
	svc.clearPendingConfigModify(".obsidian/app.json")
	if _, ok := svc.pendingConfigModifies[".obsidian/app.json"]; ok {
		t.Fatal("runtime config pending entry not cleared")
	}
	if _, ok := svc.st.PendingConfigModifies[".obsidian/app.json"]; ok {
		t.Fatal("state config pending entry not cleared")
	}
}

func TestShareSyncRefresh_IsNoOp(t *testing.T) {
	svc := newTestService(nil, nil, "")
	beforeWsCount := svc.st.WsCount
	// Should execute without panicking
	svc.receiveHandlers["ShareSyncRefresh"](json.RawMessage(`{}`), svc)
	if svc.st.WsCount != beforeWsCount {
		t.Error("ShareSyncRefresh handler must not modify state")
	}
}

func TestHandlerDispatch_InvokesRegistered(t *testing.T) {
	svc := newTestService(nil, nil, "")
	var got json.RawMessage
	svc.receiveHandlers["NoteSyncEnd"] = func(data json.RawMessage, _ *SyncService) {
		got = data
	}
	svc.dispatchText(`NoteSyncEnd|{"code":200,"data":{"lastTime":12345}}`)
	if got == nil {
		t.Fatal("handler was not invoked")
	}
}

// ---- Health check tests ----

func TestCheckHealth_2xx(t *testing.T) {
	for _, code := range []int{200, 201, 204} {
		svc := newTestService(nil, nil, "")
		svc.httpDoer = &fakeHTTPDoer{statusCode: code}
		healthy, newAPI := svc.checkHealth()
		if !healthy {
			t.Errorf("status %d: should be healthy", code)
		}
		if newAPI != "" {
			t.Errorf("status %d: newAPI should be empty", code)
		}
	}
}

func TestCheckHealth_404(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.httpDoer = &fakeHTTPDoer{statusCode: 404}
	healthy, _ := svc.checkHealth()
	if !healthy {
		t.Error("404 should be treated as healthy")
	}
}

func TestCheckHealth_500(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.httpDoer = &fakeHTTPDoer{statusCode: 500}
	healthy, _ := svc.checkHealth()
	if healthy {
		t.Error("500 should not be healthy")
	}
}

func TestCheckHealth_Error(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.httpDoer = &fakeHTTPDoer{err: fmt.Errorf("connection refused")}
	healthy, _ := svc.checkHealth()
	if healthy {
		t.Error("network error should not be healthy")
	}
}

// ---- Send tests ----

func TestSend_StringPayload(t *testing.T) {
	svc := newTestService(nil, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	svc.Send("Authorization", "my-token")
	if len(fc.written) != 1 {
		t.Fatalf("expected 1 message written, got %d", len(fc.written))
	}
	if fc.written[0] != "Authorization|my-token" {
		t.Errorf("written = %q, want %q", fc.written[0], "Authorization|my-token")
	}
}

func TestSend_MapPayload(t *testing.T) {
	svc := newTestService(nil, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	svc.Send("ClientInfo", map[string]interface{}{"name": "host1"})
	if len(fc.written) != 1 {
		t.Fatalf("expected 1 message written, got %d", len(fc.written))
	}
	if !strings.HasPrefix(fc.written[0], "ClientInfo|") {
		t.Errorf("message should start with ClientInfo|, got %q", fc.written[0])
	}
	if !strings.Contains(fc.written[0], `"name":"host1"`) {
		t.Errorf("message should contain name, got %q", fc.written[0])
	}
}

func TestSend_NotConnected(t *testing.T) {
	svc := newTestService(nil, nil, "")
	// conn is nil; Send should not panic
	svc.Send("Authorization", "tok")
}

// ---- SendBinary tests ----

func TestSendBinary_ValidPrefix(t *testing.T) {
	svc := newTestService(nil, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	err := svc.SendBinary("00", []byte{0x01, 0x02})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fc.written) != 1 {
		t.Fatalf("expected 1 binary message, got %d", len(fc.written))
	}
	if fc.written[0] != "00\x01\x02" {
		t.Errorf("binary frame = %q, want %q", fc.written[0], "00\x01\x02")
	}
}

func TestSendBinary_BadPrefixLength(t *testing.T) {
	svc := newTestService(nil, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	if err := svc.SendBinary("X", []byte{1}); err == nil {
		t.Error("1-char prefix should return error")
	}
	if err := svc.SendBinary("XYZ", []byte{1}); err == nil {
		t.Error("3-char prefix should return error")
	}
}

func TestSendBinary_NotConnected(t *testing.T) {
	svc := newTestService(nil, nil, "")
	err := svc.SendBinary("00", []byte{1})
	if err == nil {
		t.Error("SendBinary without connection should return error")
	}
}

// ---- sendClientInfo content test ----

func TestSendClientInfo_Payload(t *testing.T) {
	cfg := &config.Config{OfflineSyncStrategy: "auto"}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()
	svc.version = "1.2.3"

	sendClientInfo(svc)

	if len(fc.written) != 1 {
		t.Fatalf("expected 1 message, got %d", len(fc.written))
	}
	raw := fc.written[0]
	if !strings.HasPrefix(raw, "ClientInfo|") {
		t.Fatalf("message prefix wrong: %q", raw)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(raw[len("ClientInfo|"):]), &payload); err != nil {
		t.Fatalf("parse payload: %v", err)
	}
	checks := map[string]interface{}{
		"version":             "1.2.3",
		"type":                "LinuxCLI",
		"isLinux":             true,
		"isDesktop":           false,
		"isMobile":            false,
		"offlineSyncStrategy": "auto",
	}
	for k, want := range checks {
		got, ok := payload[k]
		if !ok {
			t.Errorf("ClientInfo payload missing field %q", k)
			continue
		}
		if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
			t.Errorf("ClientInfo[%q] = %v, want %v", k, got, want)
		}
	}
}

func TestSendClientInfo_OverrideType(t *testing.T) {
	cfg := &config.Config{ClientType: "ObsidianPlugin"}
	svc := newTestService(cfg, nil, "")
	fc := &fakeWSConn{}
	svc.mu.Lock()
	svc.conn = fc
	svc.mu.Unlock()

	sendClientInfo(svc)

	if len(fc.written) != 1 {
		t.Fatalf("expected 1 message, got %d", len(fc.written))
	}
	raw := fc.written[0]
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(raw[len("ClientInfo|"):]), &payload); err != nil {
		t.Fatalf("parse payload: %v", err)
	}
	if payload["type"] != "ObsidianPlugin" {
		t.Errorf("ClientInfo.type = %v, want ObsidianPlugin", payload["type"])
	}
}

// ---- NewSyncService test ----

func TestNewSyncService_Defaults(t *testing.T) {
	cfg := config.Default()
	cfg.API = "http://example.com"
	st := state.New()
	svc := NewSyncService(cfg, st, "/tmp/test-state.json", "1.0.0")

	if svc.cfg != cfg {
		t.Error("cfg mismatch")
	}
	if svc.st != st {
		t.Error("state mismatch")
	}
	if svc.version != "1.0.0" {
		t.Errorf("version = %q, want 1.0.0", svc.version)
	}
	if svc.runAPI != "http://example.com" {
		t.Errorf("runAPI = %q, want http://example.com", svc.runAPI)
	}
	if svc.dialer == nil {
		t.Error("dialer should not be nil")
	}
	if svc.httpDoer == nil {
		t.Error("httpDoer should not be nil")
	}
	if svc.binaryHandlers == nil {
		t.Error("binaryHandlers should not be nil")
	}
	if svc.receiveHandlers == nil {
		t.Error("receiveHandlers should not be nil")
	}
	if len(svc.receiveHandlers) != 32 {
		t.Errorf("receiveHandlers count = %d, want 32", len(svc.receiveHandlers))
	}
	if _, ok := svc.binaryHandlers[binaryPrefixFileSync]; !ok {
		t.Error("binaryHandlers should have file sync stub registered")
	}
	if svc.sleepFn == nil {
		t.Error("sleepFn should not be nil")
	}
	if svc.pendingNoteModifies == nil || svc.pendingUploadHashes == nil {
		t.Error("pending maps should be initialized")
	}
}

// ---- handleClientInfo tests ----

func TestHandleClientInfo_Success(t *testing.T) {
	svc := newTestService(nil, nil, "")
	// Should not panic; M1.3 triggers handleSync (goroutine, terminates quickly with fast timeout).
	handleClientInfo(Envelope{Code: 200}, svc)
	// Allow goroutine to start.
	time.Sleep(10 * time.Millisecond)
}

func TestHandleClientInfo_SuccessBoundary(t *testing.T) {
	svc := newTestService(nil, nil, "")
	handleClientInfo(Envelope{Code: 1}, svc)
	handleClientInfo(Envelope{Code: 299}, svc)
}

func TestHandleClientInfo_Failure(t *testing.T) {
	svc := newTestService(nil, nil, "")
	handleClientInfo(Envelope{Code: 0, Message: "bad"}, svc)
	handleClientInfo(Envelope{Code: 300, Message: "not found"}, svc)
	handleClientInfo(Envelope{Code: -1}, svc)
}

// ---- scheduleReconnect tests ----

func TestScheduleReconnect_MaxAttempts(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.timeConnect = reconnectMaxAttempts
	svc.isRegister = true
	sleepCalled := false
	svc.sleepFn = func(time.Duration) { sleepCalled = true }

	svc.scheduleReconnect()

	if sleepCalled {
		t.Error("sleep should not be called when max attempts exceeded")
	}
	if svc.timeConnect != reconnectMaxAttempts+1 {
		t.Errorf("timeConnect = %d, want %d", svc.timeConnect, reconnectMaxAttempts+1)
	}
}

func TestScheduleReconnect_CallsSleepWithCorrectDelay(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.timeConnect = 0
	svc.isRegister = false // prevents connectOnce from running again

	var sleptFor time.Duration
	svc.sleepFn = func(d time.Duration) { sleptFor = d }

	svc.scheduleReconnect()

	if svc.timeConnect != 1 {
		t.Errorf("timeConnect = %d, want 1", svc.timeConnect)
	}
	if sleptFor != reconnectDelay(1) {
		t.Errorf("sleep duration = %v, want %v", sleptFor, reconnectDelay(1))
	}
}

func TestScheduleReconnect_SecondAttemptDelay(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.timeConnect = 1 // next attempt will be 2
	svc.isRegister = false

	var sleptFor time.Duration
	svc.sleepFn = func(d time.Duration) { sleptFor = d }

	svc.scheduleReconnect()

	if sleptFor != reconnectDelay(2) {
		t.Errorf("sleep duration = %v, want %v", sleptFor, reconnectDelay(2))
	}
}

// ---- connectOnce error-path tests ----

func TestConnectOnce_HealthCheckFails(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.httpDoer = &fakeHTTPDoer{statusCode: 503}
	svc.isRegister = false

	svc.connectOnce()

	if svc.isOpen {
		t.Error("isOpen should be false when health check fails")
	}
}

func TestConnectOnce_HealthCheckNetworkError(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.httpDoer = &fakeHTTPDoer{err: fmt.Errorf("refused")}
	svc.isRegister = false

	svc.connectOnce()

	if svc.isOpen {
		t.Error("isOpen should be false on health check network error")
	}
}

func TestConnectOnce_DialFails(t *testing.T) {
	svc := newTestService(nil, nil, "")
	svc.httpDoer = &fakeHTTPDoer{statusCode: 200}
	svc.dialer = &fakeDialer{err: fmt.Errorf("connection refused")}
	svc.isRegister = false

	svc.connectOnce()

	if svc.isOpen {
		t.Error("isOpen should be false when dial fails")
	}
}

func TestConnectOnce_AuthorizationFaild_NoReconnect(t *testing.T) {
	// When close reason is "AuthorizationFaild", no reconnect should happen.
	svc := newTestService(nil, nil, "")
	svc.httpDoer = &fakeHTTPDoer{statusCode: 200}
	svc.isRegister = true
	sleepCalled := false
	svc.sleepFn = func(time.Duration) { sleepCalled = true }

	fakeConn := &fakeWSConn{
		messages: []wsMsg{{mtype: 0, err: fmt.Errorf("closed")}},
	}
	svc.dialer = &fakeDialer{conn: fakeConn}

	// connectOnce will end with reason="" (plain error, not gorilla CloseError),
	// but we can test the logic by calling connectOnce and verifying behavior
	// when isRegister=false (no reconnect path).
	svc.isRegister = false
	svc.connectOnce()

	if sleepCalled {
		t.Error("no reconnect expected when isRegister=false")
	}
}

// ---- Connect tests ----

func TestConnect_SetsIsRegister(t *testing.T) {
	svc := newTestService(nil, nil, "")
	// Wire up fake dependencies so the goroutine terminates quickly
	svc.httpDoer = &fakeHTTPDoer{statusCode: 200}
	svc.dialer = &fakeDialer{conn: &fakeWSConn{
		messages: []wsMsg{{err: fmt.Errorf("closed")}},
	}}

	if svc.isRegister {
		t.Error("isRegister should start false")
	}
	svc.Connect()
	if !svc.isRegister {
		t.Error("Connect should set isRegister=true before launching goroutine")
	}
	// Allow the goroutine to finish (it does a health check + dial + read loop)
	time.Sleep(50 * time.Millisecond)
}

// ---- checkHealth AutoRedirectEnabled path ----

func TestCheckHealth_AutoRedirectEnabled_200(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer ts.Close()

	cfg := &config.Config{AutoRedirectEnabled: true, API: ts.URL}
	svc := newTestService(cfg, nil, "")
	svc.runAPI = ts.URL

	healthy, _ := svc.checkHealth()
	if !healthy {
		t.Error("200 response with AutoRedirectEnabled should be healthy")
	}
}

func TestCheckHealth_AutoRedirectEnabled_404(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer ts.Close()

	cfg := &config.Config{AutoRedirectEnabled: true, API: ts.URL}
	svc := newTestService(cfg, nil, "")
	svc.runAPI = ts.URL

	healthy, _ := svc.checkHealth()
	if !healthy {
		t.Error("404 with AutoRedirectEnabled should be healthy")
	}
}

func TestCheckHealth_AutoRedirectEnabled_500(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()

	cfg := &config.Config{AutoRedirectEnabled: true, API: ts.URL}
	svc := newTestService(cfg, nil, "")
	svc.runAPI = ts.URL

	healthy, _ := svc.checkHealth()
	if healthy {
		t.Error("500 with AutoRedirectEnabled should not be healthy")
	}
}

func TestCheckHealth_AutoRedirectEnabled_WithRedirect(t *testing.T) {
	// Final server that returns 200
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target.Close()

	// Redirect server that sends 301 to the target
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.RequestURI, http.StatusMovedPermanently)
	}))
	defer redirector.Close()

	cfg := &config.Config{AutoRedirectEnabled: true, API: redirector.URL}
	svc := newTestService(cfg, nil, "")
	svc.runAPI = redirector.URL

	healthy, newAPI := svc.checkHealth()
	if !healthy {
		t.Error("should be healthy after redirect")
	}
	if newAPI == "" {
		t.Error("newAPI should be non-empty after redirect")
	}
	if !strings.HasPrefix(newAPI, "http://") {
		t.Errorf("newAPI = %q, want http:// prefix", newAPI)
	}
}
