package sync

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

// handleSettingSyncEnd processes the SettingSyncEnd message:
// sets task counts, clears pending deletes/modifies, commits scanned hashes, updates lastTime.
func handleSettingSyncEnd(data json.RawMessage, s *SyncService) {
	var syncData SyncEndData
	if err := json.Unmarshal(data, &syncData); err != nil {
		log.Printf("[handler] SettingSyncEnd parse: %v", err)
		return
	}

	hasUpdates := (syncData.NeedUploadCount + syncData.NeedModifyCount + syncData.NeedSyncMtimeCount + syncData.NeedDeleteCount) > 0

	s.mu.Lock()
	s.configSyncTasks.NeedUpload = syncData.NeedUploadCount
	s.configSyncTasks.NeedModify = syncData.NeedModifyCount
	s.configSyncTasks.NeedSyncMtime = syncData.NeedSyncMtimeCount
	s.configSyncTasks.NeedDelete = syncData.NeedDeleteCount

	for path := range s.pendingDeleteConfigPaths {
		delete(s.st.ConfigHashMap, path)
	}
	s.pendingDeleteConfigPaths = make(map[string]struct{})

	scanned := s.scannedConfigHashes
	s.scannedConfigHashes = make(map[string]state.FileHashEntry)

	// Only update lastTime when there are actual updates (mirrors reference behaviour).
	if hasUpdates && syncData.LastTime > s.st.ConfigSyncTime {
		s.st.ConfigSyncTime = syncData.LastTime
	}
	s.mu.Unlock()

	s.mu.Lock()
	for path, entry := range scanned {
		if existing, ok := s.st.ConfigHashMap[path]; !ok || existing.MTime <= entry.MTime {
			s.st.ConfigHashMap[path] = entry
		}
	}
	s.configSyncEnd = true
	s.mu.Unlock()

	if err := s.saveState(); err != nil {
		log.Printf("[handler] SettingSyncEnd save: %v", err)
	}
	log.Printf("[handler] SettingSyncEnd: lastTime=%d need={upload:%d modify:%d mtime:%d delete:%d}",
		syncData.LastTime, syncData.NeedUploadCount, syncData.NeedModifyCount,
		syncData.NeedSyncMtimeCount, syncData.NeedDeleteCount)
}

func handleSettingSyncModify(data json.RawMessage, s *SyncService) {
	var msg receiveContentMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] SettingSyncModify parse: %v", err)
		s.incrementCompleted("config")
		return
	}
	defer s.incrementCompleted("config")
	if isLocalStorageSettingPath(msg.Path) || isSensitivePluginConfigPath(msg.Path) {
		log.Printf("[handler] SettingSyncModify skip excluded config path %q", msg.Path)
		return
	}
	s.updateSyncTime("config", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || !s.isConfigSyncPathAllowed(rp.Rel) {
		if err != nil {
			log.Printf("[handler] SettingSyncModify path %q: %v", msg.Path, err)
		}
		return
	}
	unlock := s.lockPath(rp.Rel)
	defer unlock()
	if err := os.MkdirAll(filepath.Dir(rp.Abs), 0o755); err != nil {
		return
	}
	s.addIgnoredFile(rp.Rel)
	if err := os.WriteFile(rp.Abs, []byte(msg.Content), 0o644); err != nil {
		log.Printf("[handler] SettingSyncModify write %q: %v", rp.Rel, err)
		return
	}
	if msg.MTime > 0 {
		tm := unixMilli(msg.MTime)
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
	s.st.ConfigHashMap[rp.Rel] = entry
	delete(s.pendingConfigModifies, rp.Rel)
	delete(s.st.PendingConfigModifies, rp.Rel)
	delete(s.pendingConfigDeleteAcks, rp.Rel)
	s.mu.Unlock()
	s.saveStateLog("SettingSyncModify")
}

func handleSettingSyncNeedUpload(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] SettingSyncNeedUpload parse: %v", err)
		s.incrementCompleted("config")
		return
	}
	if isLocalStorageSettingPath(msg.Path) || isSensitivePluginConfigPath(msg.Path) {
		log.Printf("[handler] SettingSyncNeedUpload skip excluded config path %q", msg.Path)
		s.incrementCompleted("config")
		return
	}
	s.updateSyncTime("config", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || s.cfg.ReadOnlySyncEnabled || !s.isConfigSyncPathAllowed(rp.Rel) {
		s.incrementCompleted("config")
		return
	}
	if _, err := os.Stat(rp.Abs); err != nil {
		s.incrementCompleted("config")
		return
	}
	if err := s.sendFileContentModify("SettingModify", rp, s.setPendingConfigModify, func() { s.incrementCompleted("config") }); err != nil {
		log.Printf("[handler] SettingSyncNeedUpload send %q: %v", rp.Rel, err)
	}
}

func handleSettingSyncMtime(data json.RawMessage, s *SyncService) {
	var msg receiveMtimeMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] SettingSyncMtime parse: %v", err)
		s.incrementCompleted("config")
		return
	}
	defer s.incrementCompleted("config")
	if isLocalStorageSettingPath(msg.Path) || isSensitivePluginConfigPath(msg.Path) {
		return
	}
	s.updateSyncTime("config", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || !s.isConfigSyncPathAllowed(rp.Rel) {
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
	pending, ok := s.pendingConfigModifies[rp.Rel]
	s.mu.Unlock()
	if ok {
		if entry, err := fileEntry(rp.Abs, pending); err == nil {
			s.mu.Lock()
			s.st.ConfigHashMap[rp.Rel] = entry
			delete(s.pendingConfigModifies, rp.Rel)
			delete(s.st.PendingConfigModifies, rp.Rel)
			s.mu.Unlock()
			s.saveStateLog("SettingSyncMtime")
		}
	}
}

func handleSettingSyncDelete(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] SettingSyncDelete parse: %v", err)
		s.incrementCompleted("config")
		return
	}
	defer s.incrementCompleted("config")
	if isLocalStorageSettingPath(msg.Path) || isSensitivePluginConfigPath(msg.Path) {
		return
	}
	s.updateSyncTime("config", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil || !s.isConfigSyncPathAllowed(rp.Rel) {
		return
	}
	unlock := s.lockPath(rp.Rel)
	defer unlock()
	if err := os.Remove(rp.Abs); err != nil && !os.IsNotExist(err) {
		log.Printf("[handler] SettingSyncDelete remove %q: %v", rp.Rel, err)
		return
	}
	s.markDeleted(rp.Rel)
	s.mu.Lock()
	delete(s.st.ConfigHashMap, rp.Rel)
	delete(s.pendingConfigModifies, rp.Rel)
	delete(s.st.PendingConfigModifies, rp.Rel)
	delete(s.lastSyncMtime, rp.Rel)
	s.mu.Unlock()
	s.saveStateLog("SettingSyncDelete")
}

func handleSettingSyncClear(_ json.RawMessage, s *SyncService) {
	s.mu.Lock()
	s.st.ConfigSyncTime = 0
	s.configSyncTasks.Completed++
	s.mu.Unlock()
	s.saveStateLog("SettingSyncClear")
}

func handleSettingModifyAck(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] SettingModifyAck parse: %v", err)
		return
	}
	s.updateSyncTime("config", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil {
		return
	}
	defer s.concurrency.ReleaseSlot(rp.Rel)
	s.mu.Lock()
	pending, ok := s.pendingConfigModifies[rp.Rel]
	s.mu.Unlock()
	if !ok {
		return
	}
	if entry, err := fileEntry(rp.Abs, pending); err == nil {
		s.mu.Lock()
		s.st.ConfigHashMap[rp.Rel] = entry
		delete(s.pendingConfigModifies, rp.Rel)
		delete(s.st.PendingConfigModifies, rp.Rel)
		s.mu.Unlock()
		s.saveStateLog("SettingModifyAck")
	}
}

func handleSettingDeleteAck(data json.RawMessage, s *SyncService) {
	var msg receivePathMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] SettingDeleteAck parse: %v", err)
		return
	}
	s.updateSyncTime("config", msg.LastTime)
	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil {
		return
	}
	defer s.concurrency.ReleaseSlot(rp.Rel)
	s.mu.Lock()
	if _, ok := s.pendingConfigDeleteAcks[rp.Rel]; ok {
		delete(s.st.ConfigHashMap, rp.Rel)
		delete(s.pendingConfigDeleteAcks, rp.Rel)
	}
	s.mu.Unlock()
	s.saveStateLog("SettingDeleteAck")
}
