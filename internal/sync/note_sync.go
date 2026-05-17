package sync

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

// handleNoteSyncEnd processes the NoteSyncEnd message:
// sets task counts, clears pending deletes/modifies, commits scanned hashes, updates lastTime.
func handleNoteSyncEnd(data json.RawMessage, s *SyncService) {
	var syncData SyncEndData
	if err := json.Unmarshal(data, &syncData); err != nil {
		log.Printf("[handler] NoteSyncEnd parse: %v", err)
		return
	}

	s.mu.Lock()
	s.noteSyncTasks.NeedUpload = syncData.NeedUploadCount
	s.noteSyncTasks.NeedModify = syncData.NeedModifyCount
	s.noteSyncTasks.NeedSyncMtime = syncData.NeedSyncMtimeCount
	s.noteSyncTasks.NeedDelete = syncData.NeedDeleteCount

	// Remove pending-delete paths from hashMap.
	for path := range s.pendingDeleteNotePaths {
		delete(s.st.FileHashMap, path)
	}
	s.pendingDeleteNotePaths = make(map[string]struct{})

	// Extract scanned hashes; keep pending modifies for Ack/Mtime handlers.
	scanned := s.scannedNoteHashes
	s.scannedNoteHashes = make(map[string]state.FileHashEntry)

	if syncData.LastTime > s.st.NoteSyncTime {
		s.st.NoteSyncTime = syncData.LastTime
	}
	s.mu.Unlock()

	s.mu.Lock()
	// Commit scanned hashes (only if newer than existing entry).
	for path, entry := range scanned {
		if existing, ok := s.st.FileHashMap[path]; !ok || existing.MTime <= entry.MTime {
			s.st.FileHashMap[path] = entry
		}
	}
	s.noteSyncEnd = true
	s.mu.Unlock()

	if err := s.saveState(); err != nil {
		log.Printf("[handler] NoteSyncEnd save: %v", err)
	}
	log.Printf("[handler] NoteSyncEnd: lastTime=%d need={upload:%d modify:%d mtime:%d delete:%d}",
		syncData.LastTime, syncData.NeedUploadCount, syncData.NeedModifyCount,
		syncData.NeedSyncMtimeCount, syncData.NeedDeleteCount)
}

func handleNoteSyncModify(data json.RawMessage, s *SyncService) {
	var msg receiveContentMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] NoteSyncModify parse: %v", err)
		s.incrementCompleted("note")
		return
	}
	defer s.incrementCompleted("note")
	s.updateSyncTime("note", msg.LastTime)

	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || !strings.HasSuffix(strings.ToLower(msg.Path), ".md") || s.isVaultFileExcluded(msg.Path) {
		if err != nil {
			log.Printf("[handler] NoteSyncModify path %q: %v", msg.Path, err)
		}
		return
	}
	unlock := s.lockPath(rp.Rel)
	defer unlock()

	if err := os.MkdirAll(filepath.Dir(rp.Abs), 0o755); err != nil {
		log.Printf("[handler] NoteSyncModify mkdir %q: %v", rp.Rel, err)
		return
	}
	s.addIgnoredFile(rp.Rel)
	if err := os.WriteFile(rp.Abs, []byte(msg.Content), 0o644); err != nil {
		log.Printf("[handler] NoteSyncModify write %q: %v", rp.Rel, err)
		return
	}
	mtime := msg.MTime
	if mtime > 0 {
		tm := unixMilli(mtime)
		_ = os.Chtimes(rp.Abs, tm, tm)
	}
	entry, err := fileEntry(rp.Abs, msg.ContentHash)
	if err != nil {
		return
	}
	if entry.Hash == "" {
		entry.Hash = h.Content([]byte(msg.Content))
	}
	s.mu.Lock()
	s.st.FileHashMap[rp.Rel] = entry
	delete(s.pendingNoteModifies, rp.Rel)
	delete(s.st.PendingNoteModifies, rp.Rel)
	delete(s.pendingNoteDeleteAcks, rp.Rel)
	s.mu.Unlock()
	s.saveStateLog("NoteSyncModify")
}

func handleNoteSyncNeedPush(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] NoteSyncNeedPush parse: %v", err)
		s.incrementCompleted("note")
		return
	}
	s.updateSyncTime("note", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || s.cfg.ReadOnlySyncEnabled || s.isVaultFileExcluded(msg.Path) || !strings.HasSuffix(strings.ToLower(msg.Path), ".md") {
		if err != nil {
			log.Printf("[handler] NoteSyncNeedPush path %q: %v", msg.Path, err)
		}
		s.incrementCompleted("note")
		return
	}
	if _, err := os.Stat(rp.Abs); err != nil {
		s.incrementCompleted("note")
		return
	}
	if err := s.sendFileContentModify("NoteModify", rp, s.setPendingNoteModify, func() { s.incrementCompleted("note") }); err != nil {
		log.Printf("[handler] NoteSyncNeedPush send %q: %v", rp.Rel, err)
	}
}

func handleNoteSyncMtime(data json.RawMessage, s *SyncService) {
	var msg receiveMtimeMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] NoteSyncMtime parse: %v", err)
		s.incrementCompleted("note")
		return
	}
	defer s.incrementCompleted("note")
	s.updateSyncTime("note", msg.LastTime)
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
	pending, ok := s.pendingNoteModifies[rp.Rel]
	s.mu.Unlock()
	if ok {
		if entry, err := fileEntry(rp.Abs, pending); err == nil {
			s.mu.Lock()
			s.st.FileHashMap[rp.Rel] = entry
			delete(s.pendingNoteModifies, rp.Rel)
			delete(s.st.PendingNoteModifies, rp.Rel)
			s.mu.Unlock()
			s.saveStateLog("NoteSyncMtime")
		}
	}
}

func handleNoteSyncDelete(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] NoteSyncDelete parse: %v", err)
		s.incrementCompleted("note")
		return
	}
	defer s.incrementCompleted("note")
	s.updateSyncTime("note", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || s.isVaultFileExcluded(msg.Path) {
		return
	}
	unlock := s.lockPath(rp.Rel)
	defer unlock()
	if err := os.Remove(rp.Abs); err != nil && !os.IsNotExist(err) {
		log.Printf("[handler] NoteSyncDelete remove %q: %v", rp.Rel, err)
		return
	}
	s.markDeleted(rp.Rel)
	s.mu.Lock()
	delete(s.st.FileHashMap, rp.Rel)
	delete(s.pendingNoteModifies, rp.Rel)
	delete(s.st.PendingNoteModifies, rp.Rel)
	delete(s.lastSyncMtime, rp.Rel)
	s.mu.Unlock()
	s.saveStateLog("NoteSyncDelete")
}

func handleNoteSyncRename(data json.RawMessage, s *SyncService) {
	var msg receiveRenameMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] NoteSyncRename parse: %v", err)
		s.incrementCompleted("note")
		return
	}
	defer s.incrementCompleted("note")
	s.updateSyncTime("note", msg.LastTime)
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
			log.Printf("[handler] NoteSyncRename rename %q -> %q: %v", oldRP.Rel, newRP.Rel, err)
			return
		}
		s.markRenamed(oldRP.Rel, newRP.Rel)
		s.mu.Lock()
		entry := s.st.FileHashMap[oldRP.Rel]
		delete(s.st.FileHashMap, oldRP.Rel)
		entry.Hash = msg.ContentHash
		if entry.Hash == "" {
			entry.Hash = h.Content([]byte{})
		}
		if info, statErr := os.Stat(newRP.Abs); statErr == nil {
			entry.MTime = info.ModTime().UnixMilli()
			entry.Size = info.Size()
		}
		s.st.FileHashMap[newRP.Rel] = entry
		s.mu.Unlock()
		s.saveStateLog("NoteSyncRename")
		return
	}
	if msg.ContentHash != "" {
		if got, _, _, err := h.File(newRP.Abs); err == nil && got == msg.ContentHash {
			s.mu.Lock()
			s.st.FileHashMap[newRP.Rel] = state.FileHashEntry{Hash: got}
			if info, statErr := os.Stat(newRP.Abs); statErr == nil {
				e := s.st.FileHashMap[newRP.Rel]
				e.MTime = info.ModTime().UnixMilli()
				e.Size = info.Size()
				s.st.FileHashMap[newRP.Rel] = e
			}
			delete(s.st.FileHashMap, oldRP.Rel)
			s.mu.Unlock()
			return
		}
	}
	if err := s.Send("NoteRePush", map[string]interface{}{"vault": s.cfg.Vault, "path": newRP.Rel, "pathHash": h.Path(newRP.Rel)}); err != nil {
		log.Printf("[handler] NoteSyncRename repush %q: %v", newRP.Rel, err)
	}
}

func handleNoteModifyAck(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] NoteModifyAck parse: %v", err)
		return
	}
	s.updateSyncTime("note", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil {
		return
	}
	defer s.concurrency.ReleaseSlot(rp.Rel)
	s.mu.Lock()
	pending, ok := s.pendingNoteModifies[rp.Rel]
	s.mu.Unlock()
	if !ok {
		return
	}
	if entry, err := fileEntry(rp.Abs, pending); err == nil {
		s.mu.Lock()
		s.st.FileHashMap[rp.Rel] = entry
		delete(s.pendingNoteModifies, rp.Rel)
		delete(s.st.PendingNoteModifies, rp.Rel)
		s.mu.Unlock()
		s.saveStateLog("NoteModifyAck")
	}
}

func handleNoteRenameAck(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	_ = json.Unmarshal(data, &msg)
	s.updateSyncTime("note", msg.LastTime)
	defer s.concurrency.ReleaseFifoSlot()
	s.mu.Lock()
	if len(s.pendingNoteRenames) == 0 {
		s.mu.Unlock()
		return
	}
	item := s.pendingNoteRenames[0]
	s.pendingNoteRenames = s.pendingNoteRenames[1:]
	entry := s.st.FileHashMap[item.OldPath]
	delete(s.st.FileHashMap, item.OldPath)
	entry.Hash = item.ContentHash
	if entry.Hash == "" {
		entry.Hash = h.Content([]byte{})
	}
	if rp, err := s.resolveVaultPath(item.NewPath); err == nil {
		if info, statErr := os.Stat(rp.Abs); statErr == nil {
			entry.MTime = info.ModTime().UnixMilli()
			entry.Size = info.Size()
		}
	}
	s.st.FileHashMap[item.NewPath] = entry
	s.mu.Unlock()
	s.saveStateLog("NoteRenameAck")
}

func handleNoteDeleteAck(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] NoteDeleteAck parse: %v", err)
		return
	}
	s.updateSyncTime("note", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil {
		return
	}
	defer s.concurrency.ReleaseSlot(rp.Rel)
	s.mu.Lock()
	if _, ok := s.pendingNoteDeleteAcks[rp.Rel]; ok {
		delete(s.st.FileHashMap, rp.Rel)
		delete(s.pendingNoteDeleteAcks, rp.Rel)
	}
	s.mu.Unlock()
	s.saveStateLog("NoteDeleteAck")
}

func unixMilli(ms int64) time.Time {
	return time.Unix(0, ms*int64(time.Millisecond))
}

// commitPendingModifies resolves pending path→contentHash entries to FileHashEntry
// by stat-ing each file. Returns only successfully resolved entries.
func commitPendingModifies(pending map[string]string, vaultPath string) map[string]state.FileHashEntry {
	out := make(map[string]state.FileHashEntry, len(pending))
	if vaultPath == "" {
		return out
	}
	for path, contentHash := range pending {
		info, err := os.Stat(filepath.Join(vaultPath, filepath.FromSlash(path)))
		if err != nil {
			continue
		}
		out[path] = state.FileHashEntry{
			Hash:  contentHash,
			MTime: info.ModTime().UnixMilli(),
			Size:  info.Size(),
		}
	}
	return out
}
