package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	stdsync "sync"
	"time"

	gorillaws "github.com/gorilla/websocket"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

const (
	reconnectBaseDelay   = 1000 * time.Millisecond
	reconnectMaxDelay    = 30 * time.Minute
	reconnectMaxAttempts = 15
	healthCheckTimeout   = 10 * time.Second

	wsTextMessage   = gorillaws.TextMessage
	wsBinaryMessage = gorillaws.BinaryMessage

	binaryPrefixFileSync = "00"
)

// Keepalive timing vars; var (not const) so tests can override.
var (
	pingInterval = 54 * time.Second // ≈ pongWait * 0.6; first ping fires at t≈54s
	pongWait     = 90 * time.Second // max wall-clock window before conn is judged dead
	writeWait    = 10 * time.Second // write deadline for ping control frames
)

var nonReconnectCloseReasons = map[string]struct{}{
	"AuthorizationFaild":    {},
	"ClientClose":           {},
	"kicked by admin":       {},
	"TokenRotatedOrRevoked": {},
	"broadcast failed":      {},
}

// WSConn abstracts a WebSocket connection for testability.
type WSConn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	SetReadDeadline(t time.Time) error
	SetPongHandler(h func(appData string) error)
	Close() error
}

// Dialer abstracts WebSocket dialing for testability.
type Dialer interface {
	Dial(urlStr string, header http.Header) (WSConn, *http.Response, error)
}

// HTTPDoer abstracts HTTP client requests for testability.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// gorillaDialer wraps gorilla/websocket.Dialer to implement Dialer.
type gorillaDialer struct{}

func (g *gorillaDialer) Dial(urlStr string, header http.Header) (WSConn, *http.Response, error) {
	conn, resp, err := gorillaws.DefaultDialer.Dial(urlStr, header)
	if err != nil {
		return nil, resp, err
	}
	return conn, resp, nil
}

// SyncTaskCounter tracks per-module sync task progress.
type SyncTaskCounter struct {
	NeedUpload    int
	NeedModify    int
	NeedDelete    int
	NeedSyncMtime int
	Completed     int
}

// SyncService manages the WebSocket connection lifecycle and sync state.
type SyncService struct {
	cfg       *config.Config
	st        *state.State
	statePath string
	version   string

	runAPI string // may be updated when AutoRedirectEnabled follows a redirect

	dialer   Dialer
	httpDoer HTTPDoer

	mu         stdsync.Mutex
	conn       WSConn
	isOpen     bool
	isAuth     bool
	isRegister bool

	isSyncing        bool
	isSyncRequesting bool

	binaryHandlers  map[string]func([]byte)
	receiveHandlers map[string]func(json.RawMessage, *SyncService)

	timeConnect int
	sleepFn     func(time.Duration)

	// configurable for testing
	syncTimeout     time.Duration
	folderWaitPoll  time.Duration
	folderWaitLimit time.Duration

	// pending hash maps (mirrored to State for crash recovery)
	pendingNoteModifies   map[string]string
	pendingUploadHashes   map[string]string
	pendingConfigModifies map[string]string

	// pending delete sets (cleared after corresponding SyncEnd)
	pendingNoteDeleteAcks    map[string]struct{}
	pendingFileDeleteAcks    map[string]struct{}
	pendingConfigDeleteAcks  map[string]struct{}
	pendingDeleteNotePaths   map[string]struct{}
	pendingDeleteFilePaths   map[string]struct{}
	pendingDeleteFolderPaths map[string]struct{}
	pendingDeleteConfigPaths map[string]struct{}

	// rename FIFO queues (TCP ordering guarantees correct Ack pairing)
	pendingNoteRenames []struct{ OldPath, NewPath, ContentHash string }
	pendingFileRenames []struct{ OldPath, NewPath, ContentHash string }

	fileDownloadSessions map[string]*FileDownloadSession
	activeUploads        map[string]*ActiveUpload
	TempChunksBaseDir    string
	writeMu              stdsync.Mutex

	// sync task counters
	noteSyncTasks   SyncTaskCounter
	fileSyncTasks   SyncTaskCounter
	configSyncTasks SyncTaskCounter
	folderSyncTasks SyncTaskCounter

	// sync phase flags
	noteSyncEnd   bool
	fileSyncEnd   bool
	configSyncEnd bool
	folderSyncEnd bool

	// echo suppression / scan caches
	lastSyncMtime       map[string]int64
	lastSyncPathDeleted map[string]struct{}
	lastSyncPathRenamed map[string]struct{}
	scannedNoteHashes   map[string]state.FileHashEntry
	scannedFileHashes   map[string]state.FileHashEntry
	scannedConfigHashes map[string]state.FileHashEntry

	concurrency *ConcurrencyManager
	pathLocks   map[string]chan struct{}

	pingWG stdsync.WaitGroup
}

// NewSyncService creates a SyncService with production defaults.
func NewSyncService(cfg *config.Config, st *state.State, statePath, version string) *SyncService {
	s := &SyncService{
		cfg:             cfg,
		st:              st,
		statePath:       statePath,
		version:         version,
		runAPI:          cfg.API,
		dialer:          &gorillaDialer{},
		httpDoer:        http.DefaultClient,
		binaryHandlers:  make(map[string]func([]byte)),
		receiveHandlers: buildReceiveHandlers(),
		sleepFn:         time.Sleep,

		pendingNoteModifies:      copyStringMap(st.PendingNoteModifies),
		pendingUploadHashes:      copyStringMap(st.PendingUploadHashes),
		pendingConfigModifies:    copyStringMap(st.PendingConfigModifies),
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
	}
	if cfg.SyncTimeoutSeconds > 0 {
		s.syncTimeout = time.Duration(cfg.SyncTimeoutSeconds) * time.Second
	}
	s.binaryHandlers[binaryPrefixFileSync] = s.handleFileBinaryChunk
	return s
}

// buildWSURL constructs the WebSocket connection URL.
// count is the value of State.WsCount used as the query parameter.
// clientType is used for the `client` query parameter; defaults to "LinuxCLI"
// when empty.
func buildWSURL(apiURL, clientType, version string, count int) (string, error) {
	var wsBase string
	switch {
	case strings.HasPrefix(apiURL, "https://"):
		wsBase = "wss://" + apiURL[8:]
	case strings.HasPrefix(apiURL, "http://"):
		wsBase = "ws://" + apiURL[7:]
	default:
		return "", fmt.Errorf("API URL must start with http:// or https://, got %q", apiURL)
	}
	wsBase = strings.TrimRight(wsBase, "/")

	if clientType == "" {
		clientType = "LinuxCLI"
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return fmt.Sprintf(
		"%s/api/user/sync?lang=%s&count=%d&client=%s&clientName=%s&clientVersion=%s",
		wsBase,
		systemLocale(),
		count,
		url.QueryEscape(clientType),
		url.QueryEscape(hostname),
		url.QueryEscape(version),
	), nil
}

func systemLocale() string {
	for _, env := range []string{"LANG", "LANGUAGE", "LC_ALL"} {
		v := os.Getenv(env)
		if v == "" {
			continue
		}
		if idx := strings.IndexAny(v, "_."); idx != -1 {
			v = v[:idx]
		}
		if v != "" {
			return v
		}
	}
	return "en"
}

// reconnectDelay returns the backoff delay for the n-th reconnect attempt (1-based).
// Formula: 1s, 1s, 1s, 2s, 4s..., capped at 30m.
func reconnectDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt <= 3 {
		return reconnectBaseDelay
	}
	shift := attempt - 3
	if shift > 30 {
		shift = 30
	}
	delay := reconnectBaseDelay * (1 << uint(shift))
	if delay > reconnectMaxDelay {
		return reconnectMaxDelay
	}
	return delay
}

// Connect starts the connection lifecycle in a goroutine.
func (s *SyncService) Connect() {
	s.mu.Lock()
	s.isRegister = true
	s.mu.Unlock()
	go s.connectOnce()
}

// connectOnce performs one full connection attempt: health check → dial → read loop.
func (s *SyncService) connectOnce() {
	healthy, newAPI := s.checkHealth()
	if newAPI != "" {
		s.runAPI = newAPI
	}
	if !healthy {
		log.Printf("[ws] health check failed, scheduling reconnect")
		s.scheduleReconnect()
		return
	}

	wsURL, err := buildWSURL(s.runAPI, s.cfg.ClientType, s.version, s.st.WsCount)
	if err != nil {
		log.Printf("[ws] build URL: %v", err)
		s.scheduleReconnect()
		return
	}

	conn, _, err := s.dialer.Dial(wsURL, nil)
	if err != nil {
		log.Printf("[ws] dial: %v", err)
		s.scheduleReconnect()
		return
	}

	// Increment and persist WsCount after a successful dial.
	s.st.WsCount++
	if saveErr := state.Save(s.statePath, s.st); saveErr != nil {
		log.Printf("[ws] save state after connect: %v", saveErr)
	}

	// Set up keepalive on the local conn before handing it to readLoop.
	if err := conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		log.Printf("[ws] set read deadline: %v", err)
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	done := make(chan struct{})
	s.pingWG.Add(1)
	go s.pingLoop(conn, done)

	s.mu.Lock()
	s.conn = conn
	s.isOpen = true
	s.isAuth = false
	s.timeConnect = 0
	s.mu.Unlock()

	log.Printf("[ws] connected (wsCount=%d)", s.st.WsCount)
	if err := s.Send("Authorization", s.cfg.APIToken); err != nil {
		log.Printf("[ws] failed to send Authorization: %v", err)
	}

	closeReason := s.readLoop()
	close(done)

	s.mu.Lock()
	s.isOpen = false
	s.isAuth = false
	s.isSyncing = false
	s.isSyncRequesting = false
	s.mu.Unlock()

	if nonReconnectCloseReason(closeReason) {
		s.mu.Lock()
		s.isRegister = false
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	shouldReconnect := s.isRegister
	s.mu.Unlock()
	if shouldReconnect {
		s.scheduleReconnect()
	}
}

// checkHealth performs GET /api/health.
// Returns (healthy, newRuntimeAPI). newRuntimeAPI is non-empty only when
// AutoRedirectEnabled detects a redirect, signalling the caller to update runAPI.
func (s *SyncService) checkHealth() (bool, string) {
	base := strings.TrimRight(s.runAPI, "/")
	healthURL := base + "/api/health"
	ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
	defer cancel()

	if s.cfg.AutoRedirectEnabled {
		var redirectURL string
		client := &http.Client{
			Timeout: healthCheckTimeout,
			CheckRedirect: func(req *http.Request, _ []*http.Request) error {
				redirectURL = req.URL.String()
				return nil
			},
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			return false, ""
		}
		resp, err := client.Do(req)
		if err != nil {
			return false, ""
		}
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()

		var newAPI string
		if redirectURL != "" {
			if u, parseErr := url.Parse(redirectURL); parseErr == nil {
				newAPI = u.Scheme + "://" + u.Host
			}
		}
		return resp.StatusCode/100 == 2 || resp.StatusCode == 404, newAPI
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return false, ""
	}
	resp, err := s.httpDoer.Do(req)
	if err != nil {
		return false, ""
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	resp.Body.Close()
	return resp.StatusCode/100 == 2 || resp.StatusCode == 404, ""
}

// readLoop reads messages from the connection until it closes.
// Returns the WebSocket close reason string.
func (s *SyncService) readLoop() string {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.conn != nil {
			s.conn.Close() //nolint:errcheck
			s.conn = nil
		}
		s.mu.Unlock()
	}()

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			reason := extractWSCloseReason(err)
			log.Printf("[ws] read closed: %v", err)
			return reason
		}
		switch msgType {
		case wsTextMessage:
			s.dispatchText(string(data))
		case wsBinaryMessage:
			s.dispatchBinary(data)
		}
	}
}

func extractWSCloseReason(err error) string {
	var ce *gorillaws.CloseError
	if errors.As(err, &ce) {
		return ce.Text
	}
	return ""
}

func nonReconnectCloseReason(reason string) bool {
	_, ok := nonReconnectCloseReasons[reason]
	return ok
}

// scheduleReconnect waits for the exponential backoff delay then retries.
func (s *SyncService) scheduleReconnect() {
	s.timeConnect++
	if s.timeConnect > reconnectMaxAttempts {
		log.Printf("[ws] max reconnect attempts (%d) reached, giving up", reconnectMaxAttempts)
		return
	}
	delay := reconnectDelay(s.timeConnect)
	log.Printf("[ws] reconnecting in %v (attempt %d/%d)", delay, s.timeConnect, reconnectMaxAttempts)
	s.sleepFn(delay)
	s.mu.Lock()
	shouldReconnect := s.isRegister
	s.mu.Unlock()
	if shouldReconnect {
		s.connectOnce()
	}
}

// pingLoop sends periodic WebSocket ping control frames until done is closed.
// It uses WriteControl (not WriteMessage) to avoid contending with the write mutex.
func (s *SyncService) pingLoop(conn WSConn, done <-chan struct{}) {
	defer s.pingWG.Done()
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := conn.WriteControl(gorillaws.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				log.Printf("[ws] ping write: %v", err)
			}
		}
	}
}

// Send writes a text frame formatted as "ACTION|payload".
func (s *SyncService) Send(action string, payload interface{}) error {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()

	if conn == nil {
		err := fmt.Errorf("not connected")
		log.Printf("[ws] send: %v (action=%s)", err, action)
		return err
	}

	var body string
	switch v := payload.(type) {
	case string:
		body = v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			log.Printf("[ws] send marshal: %v", err)
			return err
		}
		body = string(b)
	}

	s.writeMu.Lock()
	err := conn.WriteMessage(wsTextMessage, []byte(action+"|"+body))
	s.writeMu.Unlock()
	if err != nil {
		log.Printf("[ws] send %s: %v", action, err)
		return err
	}
	return nil
}

// SendBinary writes a binary frame with a 2-byte ASCII prefix prepended.
func (s *SyncService) SendBinary(prefix string, data []byte) error {
	if len(prefix) != 2 {
		return fmt.Errorf("binary prefix must be exactly 2 characters, got %q", prefix)
	}
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("not connected")
	}
	frame := make([]byte, 2+len(data))
	copy(frame[:2], prefix)
	copy(frame[2:], data)
	s.writeMu.Lock()
	err := conn.WriteMessage(wsBinaryMessage, frame)
	s.writeMu.Unlock()
	return err
}

// dispatchBinary routes a binary message to the handler registered for its 2-byte prefix.
func (s *SyncService) dispatchBinary(data []byte) {
	if len(data) < 2 {
		log.Printf("[ws] binary message too short: %d bytes", len(data))
		return
	}
	prefix := string(data[:2])
	handler, ok := s.binaryHandlers[prefix]
	if !ok {
		log.Printf("[ws] no binary handler for prefix %q", prefix)
		return
	}
	handler(data[2:])
}
