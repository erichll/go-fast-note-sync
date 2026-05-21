package sync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/config"
	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

const testSessionID = "12345678-1234-1234-1234-123456789abc"

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func TestFileBinaryPayloadEncodeDecode(t *testing.T) {
	payload, err := encodeFileBinaryPayload(testSessionID, 7, []byte("chunk"))
	if err != nil {
		t.Fatalf("encodeFileBinaryPayload: %v", err)
	}
	sessionID, chunkIndex, data, err := decodeFileBinaryPayload(payload)
	if err != nil {
		t.Fatalf("decodeFileBinaryPayload: %v", err)
	}
	if sessionID != testSessionID || chunkIndex != 7 || string(data) != "chunk" {
		t.Fatalf("decoded payload = (%q, %d, %q)", sessionID, chunkIndex, string(data))
	}
	if _, _, _, err := decodeFileBinaryPayload(payload[:39]); err == nil {
		t.Fatal("short payload should be rejected")
	}
}

func TestHandleFileSyncUpdateCreatesTempSessionAndRequestsChunks(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, Vault: "vault"}
	svc := newTestService(cfg, nil, filepath.Join(dir, "state.json"))
	svc.conn = &fakeWSConn{}

	msg, _ := json.Marshal(receiveContentMessage{
		Path:        "assets/a.bin",
		PathHash:    h.Path("assets/a.bin"),
		ContentHash: h.Content([]byte("abc")),
		Size:        3,
		MTime:       100,
		LastTime:    200,
	})
	handleFileSyncUpdate(msg, svc)

	svc.mu.Lock()
	_, hasTemp := svc.fileDownloadSessions["temp_assets/a.bin"]
	completed := svc.fileSyncTasks.Completed
	svc.mu.Unlock()
	if !hasTemp {
		t.Fatal("temp download session was not created")
	}
	if completed != 1 {
		t.Fatalf("Completed = %d, want 1", completed)
	}
	writes := svc.conn.(*fakeWSConn).written
	if len(writes) != 1 || !strings.HasPrefix(writes[0], "FileChunkDownload|") {
		t.Fatalf("written = %#v, want FileChunkDownload", writes)
	}
	if svc.isSyncComplete() {
		t.Fatal("sync must not complete while a temp download session is active")
	}
}

func TestFileDownloadChunksMergeAndUpdateState(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	cfg := &config.Config{VaultPath: dir, Vault: "vault"}
	svc := newTestService(cfg, state.New(), statePath)
	svc.TempChunksBaseDir = filepath.Join(dir, "chunks")
	svc.conn = &fakeWSConn{}

	content := []byte("hello world")
	contentHash := h.Content(content)
	update, _ := json.Marshal(receiveContentMessage{
		Path:        "assets/file.bin",
		PathHash:    h.Path("assets/file.bin"),
		ContentHash: contentHash,
		Size:        int64(len(content)),
		MTime:       1234,
		LastTime:    9000,
	})
	handleFileSyncUpdate(update, svc)

	chunkMeta, _ := json.Marshal(fileSyncChunkDownloadMessage{
		Path:        "assets/file.bin",
		ContentHash: contentHash,
		MTime:       1234,
		SessionID:   testSessionID,
		TotalChunks: 2,
		Size:        int64(len(content)),
	})
	handleFileSyncChunkDownload(chunkMeta, svc)
	payload1, _ := encodeFileBinaryPayload(testSessionID, 1, content[5:])
	payload0, _ := encodeFileBinaryPayload(testSessionID, 0, content[:5])
	svc.handleFileBinaryChunk(payload1)
	svc.handleFileBinaryChunk(payload0)

	waitFor(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		return len(svc.fileDownloadSessions) == 0
	})
	got, err := os.ReadFile(filepath.Join(dir, "assets", "file.bin"))
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("downloaded content = %q, want %q", string(got), string(content))
	}
	svc.mu.Lock()
	entry := svc.st.FileHashMap["assets/file.bin"]
	lastTime := svc.st.FileSyncTime
	svc.mu.Unlock()
	if entry.Hash != contentHash || entry.Size != int64(len(content)) {
		t.Fatalf("FileHashMap entry = %+v", entry)
	}
	if lastTime != 9000 {
		t.Fatalf("FileSyncTime = %d, want 9000", lastTime)
	}
}

func TestFileDownloadEmptyFileCompletes(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, Vault: "vault"}
	svc := newTestService(cfg, state.New(), filepath.Join(dir, "state.json"))
	svc.TempChunksBaseDir = filepath.Join(dir, "chunks")
	svc.conn = &fakeWSConn{}

	update, _ := json.Marshal(receiveContentMessage{
		Path:        "empty.bin",
		PathHash:    h.Path("empty.bin"),
		ContentHash: h.Content(nil),
		Size:        0,
		MTime:       1000,
		LastTime:    2000,
	})
	handleFileSyncUpdate(update, svc)
	chunkMeta, _ := json.Marshal(fileSyncChunkDownloadMessage{
		Path:        "empty.bin",
		ContentHash: h.Content(nil),
		MTime:       1000,
		SessionID:   testSessionID,
		TotalChunks: 0,
		Size:        0,
	})
	handleFileSyncChunkDownload(chunkMeta, svc)

	waitFor(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		return len(svc.fileDownloadSessions) == 0
	})
	info, err := os.Stat(filepath.Join(dir, "empty.bin"))
	if err != nil {
		t.Fatalf("stat empty file: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("empty file size = %d", info.Size())
	}
}

func TestFileDownloadHashMismatchAbortsWithoutStateCommit(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, Vault: "vault"}
	svc := newTestService(cfg, state.New(), filepath.Join(dir, "state.json"))
	svc.TempChunksBaseDir = filepath.Join(dir, "chunks")
	svc.conn = &fakeWSConn{}

	update, _ := json.Marshal(receiveContentMessage{
		Path:        "bad.bin",
		PathHash:    h.Path("bad.bin"),
		ContentHash: h.Content([]byte("expected")),
		Size:        3,
		MTime:       100,
		LastTime:    200,
	})
	handleFileSyncUpdate(update, svc)
	chunkMeta, _ := json.Marshal(fileSyncChunkDownloadMessage{
		Path:        "bad.bin",
		ContentHash: h.Content([]byte("expected")),
		MTime:       100,
		SessionID:   testSessionID,
		TotalChunks: 1,
		Size:        3,
	})
	handleFileSyncChunkDownload(chunkMeta, svc)
	payload, _ := encodeFileBinaryPayload(testSessionID, 0, []byte("bad"))
	svc.handleFileBinaryChunk(payload)

	waitFor(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		return len(svc.fileDownloadSessions) == 0
	})
	if _, err := os.Stat(filepath.Join(dir, "bad.bin")); !os.IsNotExist(err) {
		t.Fatalf("bad.bin should not be written, stat err=%v", err)
	}
	svc.mu.Lock()
	_, committed := svc.st.FileHashMap["bad.bin"]
	svc.mu.Unlock()
	if committed {
		t.Fatal("hash mismatch should not commit FileHashMap")
	}
}

func TestFileSyncUpdateSkipsLargeFileAndRejectsMD(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, Vault: "vault", BinarySyncLimitEnabled: true}
	svc := newTestService(cfg, nil, filepath.Join(dir, "state.json"))
	large, _ := json.Marshal(receiveContentMessage{Path: "big.bin", Size: binarySyncSizeLimit + 1, LastTime: 500})
	handleFileSyncUpdate(large, svc)
	md, _ := json.Marshal(receiveContentMessage{Path: "note.md", Size: 1})
	handleFileSyncUpdate(md, svc)
	svc.mu.Lock()
	completed := svc.fileSyncTasks.Completed
	lastTime := svc.st.FileSyncTime
	sessions := len(svc.fileDownloadSessions)
	svc.mu.Unlock()
	if completed != 2 || lastTime != 500 || sessions != 0 {
		t.Fatalf("completed=%d lastTime=%d sessions=%d", completed, lastTime, sessions)
	}
}

func TestFileSyncUpdateSendFailureCleansTempSession(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, Vault: "vault"}
	svc := newTestService(cfg, nil, filepath.Join(dir, "state.json"))
	svc.conn = &fakeWSConn{writeErr: os.ErrPermission}
	msg, _ := json.Marshal(receiveContentMessage{
		Path:     "assets/fail.bin",
		PathHash: h.Path("assets/fail.bin"),
		Size:     3,
	})
	handleFileSyncUpdate(msg, svc)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.fileDownloadSessions) != 0 {
		t.Fatalf("sessions after send failure = %d, want 0", len(svc.fileDownloadSessions))
	}
	if svc.fileSyncTasks.Completed != 0 {
		t.Fatalf("Completed = %d, want 0 on send failure", svc.fileSyncTasks.Completed)
	}
}

func TestFileSyncChunkDownloadDisorderEmptyThenUpdateCompletes(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, Vault: "vault"}
	svc := newTestService(cfg, state.New(), filepath.Join(dir, "state.json"))
	svc.TempChunksBaseDir = filepath.Join(dir, "chunks")
	svc.conn = &fakeWSConn{}

	chunkMeta, _ := json.Marshal(fileSyncChunkDownloadMessage{
		Path:        "late-empty.bin",
		ContentHash: h.Content(nil),
		MTime:       77,
		SessionID:   testSessionID,
		TotalChunks: 0,
		Size:        0,
	})
	handleFileSyncChunkDownload(chunkMeta, svc)
	svc.mu.Lock()
	beforeUpdateSessions := len(svc.fileDownloadSessions)
	svc.mu.Unlock()
	if beforeUpdateSessions != 1 {
		t.Fatalf("sessions before update = %d, want 1", beforeUpdateSessions)
	}

	update, _ := json.Marshal(receiveContentMessage{
		Path:        "late-empty.bin",
		PathHash:    h.Path("late-empty.bin"),
		ContentHash: h.Content(nil),
		Size:        0,
		MTime:       77,
		LastTime:    88,
	})
	handleFileSyncUpdate(update, svc)
	waitFor(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		return len(svc.fileDownloadSessions) == 0
	})
	if len(svc.conn.(*fakeWSConn).written) != 0 {
		t.Fatalf("disorder empty completion should not send FileChunkDownload, wrote %#v", svc.conn.(*fakeWSConn).written)
	}
}

func TestHandleFileBinaryChunkRejectsUnknownAndDuplicate(t *testing.T) {
	dir := t.TempDir()
	svc := newTestService(&config.Config{VaultPath: dir}, nil, filepath.Join(dir, "state.json"))
	svc.TempChunksBaseDir = filepath.Join(dir, "chunks")
	unknown, _ := encodeFileBinaryPayload(testSessionID, 0, []byte("x"))
	svc.handleFileBinaryChunk(unknown)

	svc.fileDownloadSessions[testSessionID] = &FileDownloadSession{
		Path:        "dup.bin",
		SessionID:   testSessionID,
		TotalChunks: 2,
		Size:        2,
		TempDir:     filepath.Join(dir, "chunks", testSessionID),
		Received:    make(map[uint32]struct{}),
		ContentHash: h.Content([]byte("ab")),
	}
	first, _ := encodeFileBinaryPayload(testSessionID, 0, []byte("a"))
	svc.handleFileBinaryChunk(first)
	svc.handleFileBinaryChunk(first)
	svc.mu.Lock()
	received := len(svc.fileDownloadSessions[testSessionID].Received)
	svc.mu.Unlock()
	if received != 1 {
		t.Fatalf("duplicate chunk should count once, received=%d", received)
	}
}

func TestHandleFileBinaryChunkInvalidIndexAbortsAndReleasesSlot(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, ConcurrencyControlEnabled: true, MaxConcurrentUploads: 1}
	svc := newTestService(cfg, state.New(), filepath.Join(dir, "state.json"))
	svc.TempChunksBaseDir = filepath.Join(dir, "chunks")
	svc.concurrency.WaitForSlot("download_bad.bin", false, 0)
	svc.fileDownloadSessions[testSessionID] = &FileDownloadSession{
		Path:        "bad.bin",
		SessionID:   testSessionID,
		TotalChunks: 2,
		Size:        2,
		TempDir:     filepath.Join(dir, "chunks", testSessionID),
		Received:    make(map[uint32]struct{}),
		ContentHash: h.Content([]byte("ab")),
		SlotHeld:    true,
	}

	outOfRange, _ := encodeFileBinaryPayload(testSessionID, 3, []byte("z"))
	svc.handleFileBinaryChunk(outOfRange)

	svc.mu.Lock()
	_, exists := svc.fileDownloadSessions[testSessionID]
	_, committed := svc.st.FileHashMap["bad.bin"]
	svc.mu.Unlock()
	if exists {
		t.Fatal("invalid chunk index should abort the download session")
	}
	if committed {
		t.Fatal("invalid chunk index must not commit file state")
	}
	if got := len(svc.concurrency.slots); got != 0 {
		t.Fatalf("held slot count = %d, want 0", got)
	}
}

func TestHandleFileUploadSendsBinaryChunksAndWaitsForAckSlot(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	cfg := &config.Config{VaultPath: dir, Vault: "vault"}
	svc := newTestService(cfg, state.New(), statePath)
	conn := &fakeWSConn{}
	svc.conn = conn
	if err := os.WriteFile(filepath.Join(dir, "a.bin"), []byte("abcdef"), 0o644); err != nil {
		t.Fatal(err)
	}

	msg, _ := json.Marshal(fileUploadMessage{
		Path:      "a.bin",
		PathHash:  h.Path("a.bin"),
		SessionID: testSessionID,
		ChunkSize: 3,
	})
	handleFileUpload(msg, svc)

	waitFor(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		return svc.fileSyncTasks.Completed == 1 && len(svc.activeUploads) == 0
	})
	if len(conn.written) != 2 {
		t.Fatalf("binary writes = %d, want 2", len(conn.written))
	}
	for i, typ := range conn.wtypes {
		if typ != wsBinaryMessage {
			t.Fatalf("write %d type = %d, want binary", i, typ)
		}
	}
	svc.mu.Lock()
	pending := svc.pendingUploadHashes["a.bin"]
	_, checkpointStillExists := svc.st.UploadCheckpoints[h.Path("a.bin")]
	svc.mu.Unlock()
	if pending != h.Content([]byte("abcdef")) {
		t.Fatalf("pending upload hash = %q", pending)
	}
	if !checkpointStillExists {
		t.Fatal("non-final upload checkpoint should exist before FileUploadAck")
	}

	ack, _ := json.Marshal(receivePathMessage{Path: "a.bin", LastTime: 777})
	handleFileUploadAck(ack, svc)
	svc.mu.Lock()
	_, pendingStillExists := svc.pendingUploadHashes["a.bin"]
	_, checkpointStillExists = svc.st.UploadCheckpoints[h.Path("a.bin")]
	entry := svc.st.FileHashMap["a.bin"]
	lastTime := svc.st.FileSyncTime
	svc.mu.Unlock()
	if pendingStillExists || checkpointStillExists {
		t.Fatal("FileUploadAck should clear pending upload and checkpoint")
	}
	if entry.Hash != h.Content([]byte("abcdef")) || lastTime != 777 {
		t.Fatalf("ack state entry=%+v lastTime=%d", entry, lastTime)
	}
}

func TestHandleFileUploadSkipsTerminalBranches(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, Vault: "vault", ReadOnlySyncEnabled: true}
	svc := newTestService(cfg, nil, filepath.Join(dir, "state.json"))
	msg, _ := json.Marshal(fileUploadMessage{Path: "readonly.bin"})
	handleFileUpload(msg, svc)

	cfg.ReadOnlySyncEnabled = false
	md, _ := json.Marshal(fileUploadMessage{Path: "note.md"})
	handleFileUpload(md, svc)
	missing, _ := json.Marshal(fileUploadMessage{Path: "missing.bin"})
	handleFileUpload(missing, svc)
	cfg.BinarySyncLimitEnabled = true
	largePath := filepath.Join(dir, "large.bin")
	f, err := os.Create(largePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(binarySyncSizeLimit + 1); err != nil {
		t.Fatal(err)
	}
	f.Close()
	large, _ := json.Marshal(fileUploadMessage{Path: "large.bin"})
	handleFileUpload(large, svc)

	svc.mu.Lock()
	completed := svc.fileSyncTasks.Completed
	active := len(svc.activeUploads)
	svc.mu.Unlock()
	if completed != 4 || active != 0 {
		t.Fatalf("completed=%d active=%d, want 4/0", completed, active)
	}
}

func TestHandleFileUploadSendFailureKeepsActiveUploadForTimeout(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, Vault: "vault"}
	svc := newTestService(cfg, state.New(), filepath.Join(dir, "state.json"))
	svc.conn = &fakeWSConn{writeErr: os.ErrPermission}
	if err := os.WriteFile(filepath.Join(dir, "fail.bin"), []byte("abcdef"), 0o644); err != nil {
		t.Fatal(err)
	}
	msg, _ := json.Marshal(fileUploadMessage{
		Path:      "fail.bin",
		PathHash:  h.Path("fail.bin"),
		SessionID: testSessionID,
		ChunkSize: 3,
	})
	handleFileUpload(msg, svc)
	waitFor(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		up := svc.activeUploads["fail.bin"]
		return up != nil && !up.SlotHeld
	})
	svc.mu.Lock()
	completed := svc.fileSyncTasks.Completed
	_, active := svc.activeUploads["fail.bin"]
	svc.mu.Unlock()
	if completed != 0 || !active {
		t.Fatalf("send failure completed=%d active=%v, want completed 0 and active true", completed, active)
	}
}

func TestSendFileUploadCheckWritesPendingAndActiveUpload(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{VaultPath: dir, Vault: "vault"}
	svc := newTestService(cfg, state.New(), filepath.Join(dir, "state.json"))
	svc.conn = &fakeWSConn{}
	if err := os.WriteFile(filepath.Join(dir, "local.bin"), []byte("local"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := svc.SendFileUploadCheck("local.bin"); err != nil {
		t.Fatalf("SendFileUploadCheck: %v", err)
	}
	svc.mu.Lock()
	pending := svc.pendingUploadHashes["local.bin"]
	upload := svc.activeUploads["local.bin"]
	svc.mu.Unlock()
	if pending != h.Content([]byte("local")) {
		t.Fatalf("pending hash = %q", pending)
	}
	if upload == nil || !upload.SlotHeld || upload.SessionID != "" {
		t.Fatalf("active upload = %+v", upload)
	}
	writes := svc.conn.(*fakeWSConn).written
	if len(writes) != 1 || !strings.HasPrefix(writes[0], "FileUploadCheck|") {
		t.Fatalf("written = %#v", writes)
	}
}

func TestUploadCheckpointResumeAndExpiry(t *testing.T) {
	dir := t.TempDir()
	svc := newTestService(&config.Config{VaultPath: dir}, state.New(), filepath.Join(dir, "state.json"))
	pathHash := h.Path("resume.bin")
	contentHash := h.Content([]byte("abcdef"))
	svc.st.UploadCheckpoints[pathHash] = state.UploadCheckpoint{
		SessionID:      testSessionID,
		PathHash:       pathHash,
		ContentHash:    contentHash,
		LastChunkIndex: 1,
		Timestamp:      time.Now().UnixMilli(),
	}
	if got := svc.uploadStartChunk(pathHash, testSessionID, contentHash, 4); got != 2 {
		t.Fatalf("resume chunk = %d, want 2", got)
	}
	svc.st.UploadCheckpoints[pathHash] = state.UploadCheckpoint{
		SessionID:      testSessionID,
		PathHash:       pathHash,
		ContentHash:    contentHash,
		LastChunkIndex: 1,
		Timestamp:      time.Now().Add(-uploadCheckpointMaxAge - time.Second).UnixMilli(),
	}
	if got := svc.uploadStartChunk(pathHash, testSessionID, contentHash, 4); got != 0 {
		t.Fatalf("expired checkpoint start = %d, want 0", got)
	}
	svc.mu.Lock()
	_, exists := svc.st.UploadCheckpoints[pathHash]
	svc.mu.Unlock()
	if exists {
		t.Fatal("expired checkpoint should be removed")
	}
}

func TestCleanupActiveFileTransfersOnTimeout(t *testing.T) {
	dir := t.TempDir()
	svc := newTestService(&config.Config{VaultPath: dir}, state.New(), filepath.Join(dir, "state.json"))
	chunkDir := filepath.Join(dir, "chunks", testSessionID)
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	svc.fileDownloadSessions[testSessionID] = &FileDownloadSession{
		Path:      "a.bin",
		SessionID: testSessionID,
		TempDir:   chunkDir,
		Received:  make(map[uint32]struct{}),
		SlotHeld:  true,
	}
	pathHash := h.Path("upload.bin")
	svc.activeUploads["upload.bin"] = &ActiveUpload{Path: "upload.bin", PathHash: pathHash, SlotHeld: true}
	svc.pendingUploadHashes["upload.bin"] = "hash"
	svc.st.PendingUploadHashes["upload.bin"] = "hash"
	svc.st.UploadCheckpoints[pathHash] = state.UploadCheckpoint{PathHash: pathHash}

	svc.cleanupActiveFileTransfersOnTimeout()
	svc.mu.Lock()
	downloads := len(svc.fileDownloadSessions)
	uploads := len(svc.activeUploads)
	_, pending := svc.pendingUploadHashes["upload.bin"]
	_, checkpoint := svc.st.UploadCheckpoints[pathHash]
	svc.mu.Unlock()
	if downloads != 0 || uploads != 0 || pending || checkpoint {
		t.Fatalf("cleanup left downloads=%d uploads=%d pending=%v checkpoint=%v", downloads, uploads, pending, checkpoint)
	}
	if _, err := os.Stat(chunkDir); !os.IsNotExist(err) {
		t.Fatalf("chunk dir should be removed, err=%v", err)
	}
}
