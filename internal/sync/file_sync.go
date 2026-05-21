package sync

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

const (
	binarySyncSizeLimit    int64 = 128 * 1024 * 1024
	defaultFileChunkSize         = 1024 * 1024
	uploadCheckpointMaxAge       = 20 * time.Minute
)

type FileDownloadSession struct {
	Path        string
	ContentHash string
	CTime       int64
	MTime       int64
	LastTime    int64
	SessionID   string
	TotalChunks int
	Size        int64
	TempDir     string
	Received    map[uint32]struct{}
	SlotHeld    bool
	Merging     bool
	Cancelled   bool
}

type ActiveUpload struct {
	Path      string
	PathHash  string
	SessionID string
	Cancelled bool
	SlotHeld  bool
}

type fileSyncChunkDownloadMessage struct {
	Path        string `json:"path"`
	ContentHash string `json:"contentHash"`
	CTime       int64  `json:"ctime"`
	MTime       int64  `json:"mtime"`
	SessionID   string `json:"sessionId"`
	ChunkSize   int    `json:"chunkSize"`
	TotalChunks int    `json:"totalChunks"`
	Size        int64  `json:"size"`
}

type fileUploadMessage struct {
	Path      string `json:"path"`
	PathHash  string `json:"pathHash"`
	CTime     int64  `json:"ctime"`
	MTime     int64  `json:"mtime"`
	SessionID string `json:"sessionId"`
	ChunkSize int    `json:"chunkSize"`
}

func tempChunksBaseDir(statePath string) string {
	if statePath == "" {
		statePath = state.DefaultPath()
	}
	return filepath.Join(filepath.Dir(statePath), "temp-chunks")
}

func encodeFileBinaryPayload(sessionID string, chunkIndex uint32, data []byte) ([]byte, error) {
	if len(sessionID) != 36 {
		return nil, fmt.Errorf("sessionId must be 36 bytes, got %d", len(sessionID))
	}
	payload := make([]byte, 40+len(data))
	copy(payload[:36], sessionID)
	binary.BigEndian.PutUint32(payload[36:40], chunkIndex)
	copy(payload[40:], data)
	return payload, nil
}

func decodeFileBinaryPayload(payload []byte) (string, uint32, []byte, error) {
	if len(payload) < 40 {
		return "", 0, nil, fmt.Errorf("file binary payload too short: %d", len(payload))
	}
	sessionID := string(payload[:36])
	chunkIndex := binary.BigEndian.Uint32(payload[36:40])
	return sessionID, chunkIndex, payload[40:], nil
}

// handleFileSyncEnd processes the FileSyncEnd message:
// sets task counts, clears pending deletes, commits scanned hashes, updates lastTime.
func handleFileSyncEnd(data json.RawMessage, s *SyncService) {
	var syncData SyncEndData
	if err := json.Unmarshal(data, &syncData); err != nil {
		log.Printf("[handler] FileSyncEnd parse: %v", err)
		return
	}

	s.mu.Lock()
	s.fileSyncTasks.NeedUpload = syncData.NeedUploadCount
	s.fileSyncTasks.NeedModify = syncData.NeedModifyCount
	s.fileSyncTasks.NeedSyncMtime = syncData.NeedSyncMtimeCount
	s.fileSyncTasks.NeedDelete = syncData.NeedDeleteCount

	for path := range s.pendingDeleteFilePaths {
		delete(s.st.FileHashMap, path)
	}
	s.pendingDeleteFilePaths = make(map[string]struct{})

	scanned := s.scannedFileHashes
	s.scannedFileHashes = make(map[string]state.FileHashEntry)

	if syncData.LastTime > s.st.FileSyncTime {
		s.st.FileSyncTime = syncData.LastTime
	}
	s.mu.Unlock()

	s.mu.Lock()
	for path, entry := range scanned {
		if existing, ok := s.st.FileHashMap[path]; !ok || existing.MTime <= entry.MTime {
			s.st.FileHashMap[path] = entry
		}
	}
	s.fileSyncEnd = true
	s.mu.Unlock()

	if err := s.saveState(); err != nil {
		log.Printf("[handler] FileSyncEnd save: %v", err)
	}
	log.Printf("[handler] FileSyncEnd: lastTime=%d need={upload:%d modify:%d mtime:%d delete:%d}",
		syncData.LastTime, syncData.NeedUploadCount, syncData.NeedModifyCount,
		syncData.NeedSyncMtimeCount, syncData.NeedDeleteCount)
}

func handleFileSyncUpdate(data json.RawMessage, s *SyncService) {
	var msg receiveContentMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FileSyncUpdate parse: %v", err)
		s.incrementCompleted("file")
		return
	}

	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || s.isVaultFileExcluded(msg.Path) || strings.HasSuffix(strings.ToLower(msg.Path), ".md") {
		s.incrementCompleted("file")
		return
	}
	if s.cfg.BinarySyncLimitEnabled && msg.Size > binarySyncSizeLimit {
		log.Printf("[handler] FileSyncUpdate skip large file %q size=%d", rp.Rel, msg.Size)
		s.updateSyncTime("file", msg.LastTime)
		s.incrementCompleted("file")
		return
	}

	slotKey := "download_" + rp.Rel
	var session *FileDownloadSession
	var tempKey string
	var createdTemp bool

	s.mu.Lock()
	for key, candidate := range s.fileDownloadSessions {
		if !strings.HasPrefix(key, "temp_") && candidate.Path == rp.Rel {
			session = candidate
			break
		}
	}
	s.mu.Unlock()

	s.concurrency.WaitForSlot(slotKey, false, -10)

	if session != nil {
		s.mu.Lock()
		session.ContentHash = msg.ContentHash
		session.Size = msg.Size
		session.CTime = msg.CTime
		session.MTime = msg.MTime
		session.LastTime = msg.LastTime
		session.SlotHeld = true
		totalChunks := session.TotalChunks
		s.mu.Unlock()
		if totalChunks == 0 {
			s.completeEmptyDownloadSession(session)
			s.incrementCompleted("file")
			return
		}
	} else {
		tempKey = "temp_" + rp.Rel
		session = &FileDownloadSession{
			Path:        rp.Rel,
			ContentHash: msg.ContentHash,
			CTime:       msg.CTime,
			MTime:       msg.MTime,
			LastTime:    msg.LastTime,
			Size:        msg.Size,
			Received:    make(map[uint32]struct{}),
			SlotHeld:    true,
		}
		s.mu.Lock()
		s.fileDownloadSessions[tempKey] = session
		s.mu.Unlock()
		createdTemp = true
	}

	if err := s.Send("FileChunkDownload", map[string]interface{}{"vault": s.cfg.Vault, "path": rp.Rel, "pathHash": msg.PathHash}); err != nil {
		s.mu.Lock()
		if session.SlotHeld {
			session.SlotHeld = false
			s.concurrency.ReleaseSlot(slotKey)
		}
		if createdTemp {
			delete(s.fileDownloadSessions, tempKey)
		}
		s.mu.Unlock()
		log.Printf("[handler] FileSyncUpdate request chunk %q: %v", rp.Rel, err)
		return
	}
	s.incrementCompleted("file")
}

func handleFileSyncChunkDownload(data json.RawMessage, s *SyncService) {
	var msg fileSyncChunkDownloadMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FileSyncChunkDownload parse: %v", err)
		return
	}
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || s.isVaultFileExcluded(msg.Path) || strings.HasSuffix(strings.ToLower(msg.Path), ".md") {
		return
	}
	if msg.SessionID == "" {
		log.Printf("[handler] FileSyncChunkDownload missing sessionId path=%q", rp.Rel)
		return
	}

	tempDir := filepath.Join(s.TempChunksBaseDir, msg.SessionID)
	tempKey := "temp_" + rp.Rel
	var session *FileDownloadSession
	disorder := false

	s.mu.Lock()
	if temp := s.fileDownloadSessions[tempKey]; temp != nil {
		session = &FileDownloadSession{
			Path:        rp.Rel,
			ContentHash: msg.ContentHash,
			CTime:       msg.CTime,
			MTime:       msg.MTime,
			LastTime:    temp.LastTime,
			SessionID:   msg.SessionID,
			TotalChunks: msg.TotalChunks,
			Size:        msg.Size,
			TempDir:     tempDir,
			Received:    make(map[uint32]struct{}),
			SlotHeld:    temp.SlotHeld,
		}
		s.fileDownloadSessions[msg.SessionID] = session
		delete(s.fileDownloadSessions, tempKey)
	} else {
		disorder = true
		session = &FileDownloadSession{
			Path:        rp.Rel,
			ContentHash: msg.ContentHash,
			CTime:       msg.CTime,
			MTime:       msg.MTime,
			LastTime:    0,
			SessionID:   msg.SessionID,
			TotalChunks: msg.TotalChunks,
			Size:        msg.Size,
			TempDir:     tempDir,
			Received:    make(map[uint32]struct{}),
			SlotHeld:    false,
		}
		s.fileDownloadSessions[msg.SessionID] = session
		log.Printf("[handler] FileSyncChunkDownload arrived before FileSyncUpdate path=%q session=%s", rp.Rel, msg.SessionID)
	}
	s.mu.Unlock()

	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		log.Printf("[handler] FileSyncChunkDownload mkdir %q: %v", tempDir, err)
		s.abortDownloadSession(msg.SessionID, "mkdir failed")
		return
	}
	if msg.TotalChunks == 0 {
		if err := os.WriteFile(filepath.Join(tempDir, "merged"), nil, 0o600); err != nil {
			log.Printf("[handler] FileSyncChunkDownload empty write %q: %v", rp.Rel, err)
			s.abortDownloadSession(msg.SessionID, "empty write failed")
			return
		}
		if disorder {
			return
		}
		s.completeEmptyDownloadSession(session)
	}
}

func handleFileUpload(data json.RawMessage, s *SyncService) {
	var msg fileUploadMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FileUpload parse: %v", err)
		s.incrementCompleted("file")
		return
	}
	if s.cfg.ReadOnlySyncEnabled {
		s.incrementCompleted("file")
		return
	}
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || s.isVaultFileExcluded(msg.Path) || strings.HasSuffix(strings.ToLower(msg.Path), ".md") {
		s.incrementCompleted("file")
		return
	}
	info, err := os.Stat(rp.Abs)
	if err != nil {
		s.incrementCompleted("file")
		return
	}
	if s.cfg.BinarySyncLimitEnabled && info.Size() > binarySyncSizeLimit {
		log.Printf("[handler] FileUpload skip large file %q size=%d", rp.Rel, info.Size())
		s.incrementCompleted("file")
		return
	}

	s.mu.Lock()
	upload := s.activeUploads[rp.Rel]
	if upload == nil {
		upload = &ActiveUpload{Path: rp.Rel, PathHash: msg.PathHash}
		s.activeUploads[rp.Rel] = upload
	}
	upload.SessionID = msg.SessionID
	upload.PathHash = msg.PathHash
	slotHeld := upload.SlotHeld
	s.mu.Unlock()
	if !slotHeld {
		s.concurrency.WaitForSlot(rp.Rel, false, 10)
		s.mu.Lock()
		upload.SlotHeld = true
		s.mu.Unlock()
	}

	chunkSize := msg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultFileChunkSize
	}
	go s.runFileUpload(rp, msg, upload, chunkSize)
}

func handleFileSyncDelete(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FileSyncDelete parse: %v", err)
		s.incrementCompleted("file")
		return
	}
	defer s.incrementCompleted("file")
	s.updateSyncTime("file", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || s.isVaultFileExcluded(msg.Path) || strings.HasSuffix(strings.ToLower(msg.Path), ".md") {
		return
	}
	unlock := s.lockPath(rp.Rel)
	defer unlock()
	if err := os.Remove(rp.Abs); err != nil && !os.IsNotExist(err) {
		log.Printf("[handler] FileSyncDelete remove %q: %v", rp.Rel, err)
		return
	}
	s.markDeleted(rp.Rel)
	s.mu.Lock()
	delete(s.st.FileHashMap, rp.Rel)
	delete(s.pendingUploadHashes, rp.Rel)
	delete(s.st.PendingUploadHashes, rp.Rel)
	delete(s.lastSyncMtime, rp.Rel)
	s.mu.Unlock()
	s.saveStateLog("FileSyncDelete")
}

func handleFileSyncMtime(data json.RawMessage, s *SyncService) {
	var msg receiveMtimeMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FileSyncMtime parse: %v", err)
		s.incrementCompleted("file")
		return
	}
	defer s.incrementCompleted("file")
	s.updateSyncTime("file", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || s.isVaultFileExcluded(msg.Path) {
		return
	}
	unlock := s.lockPath(rp.Rel)
	defer unlock()
	if msg.MTime > 0 {
		tm := unixMilli(msg.MTime)
		_ = os.Chtimes(rp.Abs, tm, tm)
	}
	s.addIgnoredFile(rp.Rel)
	s.mu.Lock()
	s.lastSyncMtime[rp.Rel] = msg.MTime
	pending, ok := s.pendingUploadHashes[rp.Rel]
	s.mu.Unlock()
	if ok {
		if entry, err := fileEntry(rp.Abs, pending); err == nil {
			s.mu.Lock()
			s.st.FileHashMap[rp.Rel] = entry
			delete(s.pendingUploadHashes, rp.Rel)
			delete(s.st.PendingUploadHashes, rp.Rel)
			s.mu.Unlock()
			s.saveStateLog("FileSyncMtime")
		}
	}
}

func handleFileSyncRename(data json.RawMessage, s *SyncService) {
	var msg receiveRenameMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FileSyncRename parse: %v", err)
		s.incrementCompleted("file")
		return
	}
	defer s.incrementCompleted("file")
	s.updateSyncTime("file", msg.LastTime)
	oldRP, oldErr := s.resolveVaultPath(msg.OldPath)
	newRP, newErr := s.resolveVaultPath(msg.Path)
	if oldErr != nil || newErr != nil || s.isVaultFileExcluded(msg.OldPath) || s.isVaultFileExcluded(msg.Path) {
		return
	}
	unlock := s.lockPath(oldRP.Rel + "->" + newRP.Rel)
	defer unlock()
	if _, err := os.Stat(oldRP.Abs); err == nil {
		if err := os.MkdirAll(filepath.Dir(newRP.Abs), 0o755); err != nil {
			return
		}
		s.addIgnoredFile(newRP.Rel)
		if err := os.Rename(oldRP.Abs, newRP.Abs); err != nil {
			log.Printf("[handler] FileSyncRename rename %q -> %q: %v", oldRP.Rel, newRP.Rel, err)
			return
		}
		s.markRenamed(oldRP.Rel, newRP.Rel)
		s.mu.Lock()
		entry := s.st.FileHashMap[oldRP.Rel]
		delete(s.st.FileHashMap, oldRP.Rel)
		entry.Hash = msg.ContentHash
		if info, statErr := os.Stat(newRP.Abs); statErr == nil {
			entry.MTime = info.ModTime().UnixMilli()
			entry.Size = info.Size()
		}
		s.st.FileHashMap[newRP.Rel] = entry
		s.mu.Unlock()
		s.saveStateLog("FileSyncRename")
		return
	}
	if msg.ContentHash != "" {
		got, _, size, err := h.File(newRP.Abs)
		if err == nil && got == msg.ContentHash && (msg.Size == 0 || msg.Size == size) {
			s.mu.Lock()
			s.st.FileHashMap[newRP.Rel] = state.FileHashEntry{Hash: got, Size: size}
			if info, statErr := os.Stat(newRP.Abs); statErr == nil {
				e := s.st.FileHashMap[newRP.Rel]
				e.MTime = info.ModTime().UnixMilli()
				s.st.FileHashMap[newRP.Rel] = e
			}
			delete(s.st.FileHashMap, oldRP.Rel)
			s.mu.Unlock()
			return
		}
	}
	if err := s.Send("FileRePush", map[string]interface{}{"vault": s.cfg.Vault, "path": newRP.Rel, "pathHash": h.Path(newRP.Rel)}); err != nil {
		log.Printf("[handler] FileSyncRename repush %q: %v", newRP.Rel, err)
	}
}

func handleFileUploadAck(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FileUploadAck parse: %v", err)
		return
	}
	s.updateSyncTime("file", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	var rel string
	if err == nil {
		rel = rp.Rel
	}

	pathHash := msg.PathHash
	if pathHash == "" && rel != "" {
		pathHash = h.Path(rel)
	}
	if pathHash == "" {
		log.Printf("[handler] FileUploadAck missing path/pathHash; checkpoint cleanup skipped")
	} else {
		s.mu.Lock()
		delete(s.st.UploadCheckpoints, pathHash)
		s.mu.Unlock()
	}
	if err != nil {
		return
	}
	defer s.concurrency.ReleaseSlot(rel)
	s.mu.Lock()
	pending, ok := s.pendingUploadHashes[rel]
	s.mu.Unlock()
	if !ok {
		return
	}
	if entry, err := fileEntry(rp.Abs, pending); err == nil {
		s.mu.Lock()
		s.st.FileHashMap[rel] = entry
		delete(s.pendingUploadHashes, rel)
		delete(s.st.PendingUploadHashes, rel)
		s.mu.Unlock()
		s.saveStateLog("FileUploadAck")
	}
}

func handleFileRenameAck(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	_ = json.Unmarshal(data, &msg)
	s.updateSyncTime("file", msg.LastTime)
	defer s.concurrency.ReleaseFifoSlot()
	s.mu.Lock()
	if len(s.pendingFileRenames) == 0 {
		s.mu.Unlock()
		return
	}
	item := s.pendingFileRenames[0]
	s.pendingFileRenames = s.pendingFileRenames[1:]
	entry := s.st.FileHashMap[item.OldPath]
	delete(s.st.FileHashMap, item.OldPath)
	entry.Hash = item.ContentHash
	if rp, err := s.resolveVaultPath(item.NewPath); err == nil {
		if info, statErr := os.Stat(rp.Abs); statErr == nil {
			entry.MTime = info.ModTime().UnixMilli()
			entry.Size = info.Size()
		}
	}
	s.st.FileHashMap[item.NewPath] = entry
	s.mu.Unlock()
	s.saveStateLog("FileRenameAck")
}

func handleFileDeleteAck(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FileDeleteAck parse: %v", err)
		return
	}
	s.updateSyncTime("file", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil {
		return
	}
	defer s.concurrency.ReleaseSlot(rp.Rel)
	s.mu.Lock()
	if _, ok := s.pendingFileDeleteAcks[rp.Rel]; ok {
		delete(s.st.FileHashMap, rp.Rel)
		delete(s.pendingFileDeleteAcks, rp.Rel)
	}
	s.mu.Unlock()
	s.saveStateLog("FileDeleteAck")
}

func (s *SyncService) handleFileBinaryChunk(payload []byte) {
	sessionID, chunkIndex, chunkData, err := decodeFileBinaryPayload(payload)
	if err != nil {
		log.Printf("[handler] file binary decode: %v", err)
		return
	}
	s.mu.Lock()
	session := s.fileDownloadSessions[sessionID]
	if session == nil {
		s.mu.Unlock()
		return
	}
	if chunkIndex >= uint32(session.TotalChunks) {
		s.mu.Unlock()
		log.Printf("[handler] file binary chunk out of range session=%s chunk=%d total=%d", sessionID, chunkIndex, session.TotalChunks)
		s.abortDownloadSession(sessionID, "invalid chunk index")
		return
	}
	if _, ok := session.Received[chunkIndex]; ok {
		s.mu.Unlock()
		return
	}
	tempDir := session.TempDir
	s.mu.Unlock()

	if tempDir == "" {
		log.Printf("[handler] file binary missing temp dir session=%s", sessionID)
		s.abortDownloadSession(sessionID, "missing temp dir")
		return
	}
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		log.Printf("[handler] file binary mkdir session=%s: %v", sessionID, err)
		s.abortDownloadSession(sessionID, "mkdir failed")
		return
	}
	if err := os.WriteFile(filepath.Join(tempDir, strconv.FormatUint(uint64(chunkIndex), 10)), chunkData, 0o600); err != nil {
		log.Printf("[handler] file binary write session=%s chunk=%d: %v", sessionID, chunkIndex, err)
		s.abortDownloadSession(sessionID, "chunk write failed")
		return
	}

	var complete *FileDownloadSession
	s.mu.Lock()
	session = s.fileDownloadSessions[sessionID]
	if session == nil || session.Cancelled {
		s.mu.Unlock()
		return
	}
	session.Received[chunkIndex] = struct{}{}
	if len(session.Received) == session.TotalChunks && !session.Merging {
		session.Merging = true
		complete = session
	}
	s.mu.Unlock()
	if complete != nil {
		go s.mergeDownloadSession(complete)
	}
}

func (s *SyncService) completeEmptyDownloadSession(session *FileDownloadSession) {
	s.mu.Lock()
	if current := s.fileDownloadSessions[session.SessionID]; current != session {
		s.mu.Unlock()
		return
	}
	session.Merging = true
	s.mu.Unlock()
	go s.mergeDownloadSession(session)
}

func (s *SyncService) abortDownloadSession(sessionID, reason string) {
	var session *FileDownloadSession
	s.mu.Lock()
	session = s.fileDownloadSessions[sessionID]
	if session != nil {
		delete(s.fileDownloadSessions, sessionID)
		if session.SlotHeld {
			session.SlotHeld = false
			s.concurrency.ReleaseSlot("download_" + session.Path)
		}
		session.Cancelled = true
	}
	s.mu.Unlock()
	if session != nil && session.TempDir != "" {
		_ = os.RemoveAll(session.TempDir)
	}
	log.Printf("[handler] abort download session=%s reason=%s", sessionID, reason)
}

func (s *SyncService) mergeDownloadSession(session *FileDownloadSession) {
	unlock := s.lockPath(session.Path)
	defer unlock()

	staged := filepath.Join(session.TempDir, "merged")
	if err := os.MkdirAll(session.TempDir, 0o755); err != nil {
		s.abortDownloadSession(session.SessionID, "merge mkdir failed")
		return
	}
	out, err := os.OpenFile(staged, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		s.abortDownloadSession(session.SessionID, "merge open failed")
		return
	}
	for i := 0; i < session.TotalChunks; i++ {
		in, err := os.Open(filepath.Join(session.TempDir, strconv.Itoa(i)))
		if err != nil {
			out.Close()
			s.abortDownloadSession(session.SessionID, "missing chunk")
			return
		}
		_, copyErr := io.Copy(out, in)
		closeErr := in.Close()
		if copyErr != nil || closeErr != nil {
			out.Close()
			s.abortDownloadSession(session.SessionID, "merge copy failed")
			return
		}
	}
	if err := out.Close(); err != nil {
		s.abortDownloadSession(session.SessionID, "merge close failed")
		return
	}
	gotHash, _, gotSize, err := h.File(staged)
	if err != nil {
		s.abortDownloadSession(session.SessionID, "merge hash failed")
		return
	}
	if gotSize != session.Size {
		s.abortDownloadSession(session.SessionID, "size mismatch")
		return
	}
	if session.ContentHash != "" && gotHash != session.ContentHash {
		s.abortDownloadSession(session.SessionID, "hash mismatch")
		return
	}
	if session.ContentHash == "" {
		session.ContentHash = gotHash
	}

	rp, err := s.resolveVaultPath(session.Path)
	if err != nil {
		s.abortDownloadSession(session.SessionID, "safe path failed")
		return
	}
	if err := os.MkdirAll(filepath.Dir(rp.Abs), 0o755); err != nil {
		s.abortDownloadSession(session.SessionID, "mkdir parent failed")
		return
	}
	s.addIgnoredFile(rp.Rel)

	s.mu.Lock()
	current := s.fileDownloadSessions[session.SessionID]
	if current != session || session.Cancelled {
		s.mu.Unlock()
		_ = os.RemoveAll(session.TempDir)
		return
	}
	if err := os.Rename(staged, rp.Abs); err != nil {
		s.mu.Unlock()
		s.abortDownloadSession(session.SessionID, "replace failed")
		return
	}
	if session.MTime > 0 {
		tm := unixMilli(session.MTime)
		_ = os.Chtimes(rp.Abs, tm, tm)
	}
	s.st.FileHashMap[rp.Rel] = state.FileHashEntry{Hash: session.ContentHash, MTime: session.MTime, Size: session.Size}
	s.lastSyncMtime[rp.Rel] = session.MTime
	if session.LastTime > s.st.FileSyncTime {
		s.st.FileSyncTime = session.LastTime
	}
	delete(s.fileDownloadSessions, session.SessionID)
	if session.SlotHeld {
		session.SlotHeld = false
		s.concurrency.ReleaseSlot("download_" + session.Path)
	}
	s.mu.Unlock()
	_ = os.RemoveAll(session.TempDir)
	s.saveStateLog("FileDownloadComplete")
}

func (s *SyncService) runFileUpload(rp resolvedPath, msg fileUploadMessage, upload *ActiveUpload, chunkSize int) {
	content, err := os.ReadFile(rp.Abs)
	if err != nil {
		log.Printf("[handler] FileUpload read %q: %v", rp.Rel, err)
		s.releaseFailedUploadSlot(upload)
		return
	}
	contentHash := h.Content(content)
	totalChunks := 1
	if len(content) > 0 {
		totalChunks = (len(content) + chunkSize - 1) / chunkSize
	}
	pathHash := msg.PathHash
	if pathHash == "" {
		pathHash = h.Path(rp.Rel)
	}
	startChunk := s.uploadStartChunk(pathHash, msg.SessionID, contentHash, totalChunks)

	s.mu.Lock()
	if upload.Cancelled {
		s.mu.Unlock()
		s.cancelUpload(rp.Rel)
		return
	}
	s.pendingUploadHashes[rp.Rel] = contentHash
	s.st.PendingUploadHashes[rp.Rel] = contentHash
	s.mu.Unlock()
	s.saveStateLog("FileUploadPending")

	for i := startChunk; i < totalChunks; i++ {
		s.mu.Lock()
		cancelled := upload.Cancelled
		s.mu.Unlock()
		if cancelled {
			s.cancelUpload(rp.Rel)
			return
		}
		start := i * chunkSize
		end := start + chunkSize
		if end > len(content) {
			end = len(content)
		}
		payload, err := encodeFileBinaryPayload(msg.SessionID, uint32(i), content[start:end])
		if err != nil {
			log.Printf("[handler] FileUpload encode %q: %v", rp.Rel, err)
			s.releaseFailedUploadSlot(upload)
			return
		}
		if err := s.SendBinary(binaryPrefixFileSync, payload); err != nil {
			log.Printf("[handler] FileUpload send %q chunk=%d: %v", rp.Rel, i, err)
			s.releaseFailedUploadSlot(upload)
			return
		}
		if i < totalChunks-1 {
			s.mu.Lock()
			s.st.UploadCheckpoints[pathHash] = state.UploadCheckpoint{
				SessionID:      msg.SessionID,
				PathHash:       pathHash,
				ContentHash:    contentHash,
				LastChunkIndex: i,
				Timestamp:      time.Now().UnixMilli(),
			}
			s.mu.Unlock()
			s.saveStateLog("FileUploadCheckpoint")
		}
	}

	s.mu.Lock()
	delete(s.activeUploads, rp.Rel)
	s.fileSyncTasks.Completed++
	s.mu.Unlock()
}

func (s *SyncService) uploadStartChunk(pathHash, sessionID, contentHash string, totalChunks int) int {
	s.mu.Lock()
	cp, ok := s.st.UploadCheckpoints[pathHash]
	s.mu.Unlock()
	if !ok {
		return 0
	}
	if time.Since(time.UnixMilli(cp.Timestamp)) > uploadCheckpointMaxAge ||
		cp.SessionID != sessionID ||
		cp.ContentHash != contentHash ||
		cp.LastChunkIndex < 0 ||
		cp.LastChunkIndex >= totalChunks-1 {
		s.mu.Lock()
		delete(s.st.UploadCheckpoints, pathHash)
		s.mu.Unlock()
		s.saveStateLog("FileUploadCheckpointExpired")
		return 0
	}
	return cp.LastChunkIndex + 1
}

func (s *SyncService) releaseFailedUploadSlot(upload *ActiveUpload) {
	s.mu.Lock()
	if upload.SlotHeld {
		upload.SlotHeld = false
		s.concurrency.ReleaseSlot(upload.Path)
	}
	s.mu.Unlock()
}

func (s *SyncService) cancelUpload(rel string) {
	s.mu.Lock()
	upload := s.activeUploads[rel]
	if upload != nil {
		upload.Cancelled = true
		if upload.SlotHeld {
			upload.SlotHeld = false
			s.concurrency.ReleaseSlot(rel)
		}
		if upload.PathHash != "" {
			delete(s.st.UploadCheckpoints, upload.PathHash)
		}
	}
	delete(s.activeUploads, rel)
	delete(s.pendingUploadHashes, rel)
	delete(s.st.PendingUploadHashes, rel)
	s.mu.Unlock()
	s.saveStateLog("FileUploadCancel")
}

func (s *SyncService) SendFileUploadCheck(raw string) error {
	rp, err := s.resolveVaultPath(raw)
	if err != nil {
		return err
	}
	if strings.HasSuffix(strings.ToLower(rp.Rel), ".md") || s.isVaultFileExcluded(rp.Rel) {
		return fmt.Errorf("not an uploadable file: %s", rp.Rel)
	}
	info, err := os.Stat(rp.Abs)
	if err != nil {
		return err
	}
	if s.cfg.BinarySyncLimitEnabled && info.Size() > binarySyncSizeLimit {
		return fmt.Errorf("file exceeds binary sync limit: %s", rp.Rel)
	}
	contentHash, _, size, err := h.File(rp.Abs)
	if err != nil {
		return err
	}
	pathHash := h.Path(rp.Rel)
	payload := map[string]interface{}{
		"vault":       s.cfg.Vault,
		"path":        rp.Rel,
		"pathHash":    pathHash,
		"contentHash": contentHash,
		"mtime":       info.ModTime().UnixMilli(),
		"ctime":       info.ModTime().UnixMilli(),
		"size":        size,
	}
	s.mu.Lock()
	if base, ok := s.st.FileHashMap[rp.Rel]; ok && base.Hash != "" {
		payload["baseHash"] = base.Hash
	} else {
		payload["baseHashMissing"] = true
	}
	s.mu.Unlock()
	s.concurrency.WaitForSlot(rp.Rel, false, 10)
	if err := s.Send("FileUploadCheck", payload); err != nil {
		s.concurrency.ReleaseSlot(rp.Rel)
		return err
	}
	s.mu.Lock()
	s.pendingUploadHashes[rp.Rel] = contentHash
	s.st.PendingUploadHashes[rp.Rel] = contentHash
	s.activeUploads[rp.Rel] = &ActiveUpload{Path: rp.Rel, PathHash: pathHash, SlotHeld: true}
	s.mu.Unlock()
	s.saveStateLog("FileUploadCheck")
	return nil
}
