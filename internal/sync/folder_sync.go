package sync

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"
)

type folderSyncMsgData struct {
	Path     string `json:"path"`
	MTime    int64  `json:"mtime"`
	LastTime int64  `json:"lastTime"`
	PathHash string `json:"pathHash"`
}

// handleFolderSyncEnd sets the folderSyncTasks need counts (BEFORE setting the flag).
func handleFolderSyncEnd(data json.RawMessage, s *SyncService) {
	var syncData SyncEndData
	if err := json.Unmarshal(data, &syncData); err != nil {
		log.Printf("[handler] FolderSyncEnd parse: %v", err)
		return
	}

	s.mu.Lock()
	// Set need counts first, then set the flag (folderSyncDone checks both).
	s.folderSyncTasks.NeedUpload = syncData.NeedUploadCount
	s.folderSyncTasks.NeedModify = syncData.NeedModifyCount
	s.folderSyncTasks.NeedSyncMtime = syncData.NeedSyncMtimeCount
	s.folderSyncTasks.NeedDelete = syncData.NeedDeleteCount

	pendingDeletes := s.pendingDeleteFolderPaths
	s.pendingDeleteFolderPaths = make(map[string]struct{})

	for path := range pendingDeletes {
		delete(s.st.FolderSnapshot, path)
	}
	s.st.FolderSyncTime = syncData.LastTime
	s.folderSyncEnd = true
	s.mu.Unlock()

	if err := s.saveState(); err != nil {
		log.Printf("[handler] FolderSyncEnd save: %v", err)
	}
	log.Printf("[handler] FolderSyncEnd: lastTime=%d need={upload:%d modify:%d mtime:%d delete:%d}",
		syncData.LastTime, syncData.NeedUploadCount, syncData.NeedModifyCount,
		syncData.NeedSyncMtimeCount, syncData.NeedDeleteCount)
}

// handleFolderSyncModify creates the local directory and updates folderSnapshot.
// folderSyncTasks.Completed is incremented unconditionally (even when path is excluded).
func handleFolderSyncModify(data json.RawMessage, s *SyncService) {
	var msg folderSyncMsgData
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FolderSyncModify parse: %v", err)
		s.mu.Lock()
		s.folderSyncTasks.Completed++
		s.mu.Unlock()
		return
	}

	defer func() {
		s.mu.Lock()
		s.folderSyncTasks.Completed++
		s.mu.Unlock()
	}()
	s.updateSyncTime("folder", msg.LastTime)

	if s.isFolderPathExcluded(msg.Path) {
		return
	}

	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil {
		log.Printf("[handler] FolderSyncModify path %q: %v", msg.Path, err)
		return
	}

	unlock := s.lockPath(rp.Rel)
	defer unlock()
	if err := os.MkdirAll(rp.Abs, 0o755); err != nil {
		log.Printf("[handler] FolderSyncModify mkdir %q: %v", rp.Rel, err)
	}

	mtime := msg.MTime
	if mtime == 0 {
		mtime = time.Now().UnixMilli()
	}
	s.mu.Lock()
	s.st.FolderSnapshot[rp.Rel] = mtime
	s.mu.Unlock()

}

// handleFolderSyncDelete deletes the local directory (if empty) and removes it from folderSnapshot.
// folderSyncTasks.Completed is incremented unconditionally.
func handleFolderSyncDelete(data json.RawMessage, s *SyncService) {
	var msg folderSyncMsgData
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FolderSyncDelete parse: %v", err)
		s.mu.Lock()
		s.folderSyncTasks.Completed++
		s.mu.Unlock()
		return
	}

	defer func() {
		s.mu.Lock()
		s.folderSyncTasks.Completed++
		s.mu.Unlock()
	}()
	s.updateSyncTime("folder", msg.LastTime)

	if s.isFolderPathExcluded(msg.Path) {
		return
	}

	rp, err := s.resolveVaultPath(msg.Path)
	if err != nil {
		log.Printf("[handler] FolderSyncDelete path %q: %v", msg.Path, err)
		return
	}

	unlock := s.lockPath(rp.Rel)
	defer unlock()
	info, err := os.Stat(rp.Abs)
	if err != nil || !info.IsDir() {
		// Directory doesn't exist or is not a directory; still clean up snapshot.
		s.mu.Lock()
		delete(s.st.FolderSnapshot, rp.Rel)
		s.mu.Unlock()
		return
	}

	entries, err := os.ReadDir(rp.Abs)
	if err != nil {
		log.Printf("[handler] FolderSyncDelete readdir %q: %v", rp.Rel, err)
		return
	}
	if len(entries) > 0 {
		log.Printf("[handler] FolderSyncDelete: folder %q not empty (%d entries), skipping delete", rp.Rel, len(entries))
		return
	}

	if err := os.Remove(rp.Abs); err != nil {
		log.Printf("[handler] FolderSyncDelete remove %q: %v", rp.Rel, err)
		return
	}
	s.markDeleted(rp.Rel)

	s.mu.Lock()
	delete(s.st.FolderSnapshot, rp.Rel)
	s.mu.Unlock()

}

func handleFolderSyncRename(data json.RawMessage, s *SyncService) {
	var msg receiveRenameMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[handler] FolderSyncRename parse: %v", err)
		s.incrementCompleted("folder")
		return
	}
	defer s.incrementCompleted("folder")
	s.updateSyncTime("folder", msg.LastTime)
	oldRP, oldErr := s.resolveVaultPath(msg.OldPath)
	newRP, newErr := s.resolveVaultPath(msg.Path)
	if oldErr != nil || newErr != nil || s.isFolderPathExcluded(msg.OldPath) || s.isFolderPathExcluded(msg.Path) {
		return
	}
	unlock := s.lockPath(oldRP.Rel + "->" + newRP.Rel)
	defer unlock()
	if _, err := os.Stat(oldRP.Abs); err == nil {
		if err := os.MkdirAll(filepath.Dir(newRP.Abs), 0o755); err != nil {
			return
		}
		if err := os.Rename(oldRP.Abs, newRP.Abs); err != nil {
			log.Printf("[handler] FolderSyncRename rename %q -> %q: %v", oldRP.Rel, newRP.Rel, err)
			return
		}
		s.markRenamed(oldRP.Rel, newRP.Rel)
	} else if _, statErr := os.Stat(newRP.Abs); os.IsNotExist(statErr) {
		if err := os.MkdirAll(newRP.Abs, 0o755); err != nil {
			log.Printf("[handler] FolderSyncRename mkdir %q: %v", newRP.Rel, err)
			return
		}
	}
	mtime := msg.MTime
	if mtime == 0 {
		mtime = time.Now().UnixMilli()
	}
	s.mu.Lock()
	delete(s.st.FolderSnapshot, oldRP.Rel)
	s.st.FolderSnapshot[newRP.Rel] = mtime
	s.mu.Unlock()
	s.saveStateLog("FolderSyncRename")
}
