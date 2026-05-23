package sync

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

// --- Protocol payload types ---

// SnapFile represents a file snapshot sent in NoteSync/FileSync/SettingSync requests.
type SnapFile struct {
	Path            string  `json:"path"`
	PathHash        string  `json:"pathHash"`
	ContentHash     string  `json:"contentHash"`
	MTime           int64   `json:"mtime"`
	CTime           int64   `json:"ctime"`
	Size            int64   `json:"size"`
	BaseHash        *string `json:"baseHash,omitempty"`
	BaseHashMissing bool    `json:"baseHashMissing,omitempty"`
}

// SnapFolder represents a folder snapshot sent in FolderSync requests.
type SnapFolder struct {
	Path     string `json:"path"`
	PathHash string `json:"pathHash"`
}

// PathHashFile represents a path+hash pair used in del/missing lists.
type PathHashFile struct {
	Path     string `json:"path"`
	PathHash string `json:"pathHash"`
}

// SyncEndData is the payload received in *SyncEnd messages.
type SyncEndData struct {
	LastTime           int64 `json:"lastTime"`
	NeedUploadCount    int   `json:"needUploadCount"`
	NeedModifyCount    int   `json:"needModifyCount"`
	NeedSyncMtimeCount int   `json:"needSyncMtimeCount"`
	NeedDeleteCount    int   `json:"needDeleteCount"`
}

// scanResult holds the vault scan output.
type scanResult struct {
	notes   []SnapFile
	files   []SnapFile
	folders []SnapFolder
	configs []SnapFile

	delNotes   []PathHashFile
	delFiles   []PathHashFile
	delFolders []PathHashFile
	delConfigs []PathHashFile

	missingNotes   []PathHashFile
	missingFiles   []PathHashFile
	missingFolders []PathHashFile
	missingConfigs []PathHashFile

	scannedNoteHashes   map[string]state.FileHashEntry
	scannedFileHashes   map[string]state.FileHashEntry
	scannedConfigHashes map[string]state.FileHashEntry
}

func newScanResult() *scanResult {
	return &scanResult{
		notes:               []SnapFile{},
		files:               []SnapFile{},
		folders:             []SnapFolder{},
		configs:             []SnapFile{},
		delNotes:            []PathHashFile{},
		delFiles:            []PathHashFile{},
		delFolders:          []PathHashFile{},
		delConfigs:          []PathHashFile{},
		missingNotes:        []PathHashFile{},
		missingFiles:        []PathHashFile{},
		missingFolders:      []PathHashFile{},
		missingConfigs:      []PathHashFile{},
		scannedNoteHashes:   make(map[string]state.FileHashEntry),
		scannedFileHashes:   make(map[string]state.FileHashEntry),
		scannedConfigHashes: make(map[string]state.FileHashEntry),
	}
}

// newUUID returns a random UUID v4 string.
func newUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		now := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(now >> (uint(i) * 8))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// handleSync is called by handleClientInfo after successful auth.
// isLoadLastTime=true → incremental; false → full.
func (s *SyncService) handleSync(isLoadLastTime bool) {
	s.mu.Lock()
	if s.isSyncing {
		s.mu.Unlock()
		log.Printf("[sync] handleSync: already syncing, skipping")
		return
	}
	if s.cfg.ManualSyncEnabled {
		s.mu.Unlock()
		log.Printf("[sync] handleSync: manual sync mode, skipping auto-trigger")
		return
	}
	s.isSyncing = true
	s.noteSyncEnd = false
	s.fileSyncEnd = false
	s.configSyncEnd = false
	s.folderSyncEnd = false
	s.noteSyncTasks = SyncTaskCounter{}
	s.fileSyncTasks = SyncTaskCounter{}
	s.configSyncTasks = SyncTaskCounter{}
	s.folderSyncTasks = SyncTaskCounter{}
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			s.isSyncing = false
			s.mu.Unlock()
		}()

		result, err := s.scanVault(isLoadLastTime)
		if err != nil {
			log.Printf("[sync] vault scan failed: %v", err)
			return
		}

		s.mu.Lock()
		s.scannedNoteHashes = result.scannedNoteHashes
		s.scannedFileHashes = result.scannedFileHashes
		s.scannedConfigHashes = result.scannedConfigHashes
		s.isSyncRequesting = true
		s.mu.Unlock()

		context := newUUID()
		s.sendSyncRequests(result, context, isLoadLastTime)

		s.mu.Lock()
		s.isSyncRequesting = false
		s.mu.Unlock()

		go s.runCheckSyncCompletion(isLoadLastTime)
	}()
}

// sendSyncRequests sends FolderSync first, waits for folderSyncDone, then Note/File/Setting.
func (s *SyncService) sendSyncRequests(result *scanResult, context string, isLoadLastTime bool) {
	s.mu.Lock()
	lastFolderTime := int64(0)
	lastNoteTime := int64(0)
	lastFileTime := int64(0)
	lastConfigTime := int64(0)
	if isLoadLastTime {
		lastFolderTime = s.st.FolderSyncTime
		lastNoteTime = s.st.NoteSyncTime
		lastFileTime = s.st.FileSyncTime
		lastConfigTime = s.st.ConfigSyncTime
	}
	s.mu.Unlock()

	// Step 1: FolderSync first
	if err := s.Send("FolderSync", buildFolderSyncPayload(s.cfg.Vault, lastFolderTime, context, result, s.cfg.OfflineDeleteSyncEnabled)); err != nil {
		log.Printf("[sync] send FolderSync: %v", err)
	} else {
		now := time.Now().UnixMilli()
		s.mu.Lock()
		for _, f := range result.folders {
			s.st.FolderSnapshot[f.Path] = now
		}
		if s.cfg.OfflineDeleteSyncEnabled {
			for _, d := range result.delFolders {
				s.pendingDeleteFolderPaths[d.Path] = struct{}{}
			}
		}
		s.mu.Unlock()
	}

	// Step 2: wait for folderSyncDone (50ms poll, configurable timeout)
	poll := s.folderWaitPoll
	if poll == 0 {
		poll = 50 * time.Millisecond
	}
	limit := s.folderWaitLimit
	if limit == 0 {
		limit = 10 * time.Second
	}
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if s.folderSyncDone() {
			break
		}
		time.Sleep(poll)
	}

	// Step 3: NoteSync
	if err := s.Send("NoteSync", buildNoteSyncPayload(s.cfg.Vault, lastNoteTime, context, result, s.cfg.OfflineDeleteSyncEnabled)); err != nil {
		log.Printf("[sync] send NoteSync: %v", err)
	} else {
		s.mu.Lock()
		for _, n := range result.notes {
			s.pendingNoteModifies[n.Path] = n.ContentHash
			s.st.PendingNoteModifies[n.Path] = n.ContentHash
		}
		if s.cfg.OfflineDeleteSyncEnabled {
			for _, d := range result.delNotes {
				s.pendingDeleteNotePaths[d.Path] = struct{}{}
			}
		}
		s.mu.Unlock()
		if err := s.saveState(); err != nil {
			log.Printf("[sync] save state after NoteSync: %v", err)
		}
	}

	// Step 4: FileSync
	if err := s.Send("FileSync", buildFileSyncPayload(s.cfg.Vault, lastFileTime, context, result, s.cfg.OfflineDeleteSyncEnabled)); err != nil {
		log.Printf("[sync] send FileSync: %v", err)
	} else if s.cfg.OfflineDeleteSyncEnabled {
		s.mu.Lock()
		for _, d := range result.delFiles {
			s.pendingDeleteFilePaths[d.Path] = struct{}{}
		}
		s.mu.Unlock()
	}

	// Step 5: SettingSync (independent of FolderSync wait)
	if s.cfg.ConfigSyncEnabled {
		if err := s.Send("SettingSync", buildSettingSyncPayload(s.cfg.Vault, lastConfigTime, context, result, s.cfg.OfflineDeleteSyncEnabled)); err != nil {
			log.Printf("[sync] send SettingSync: %v", err)
		} else {
			s.mu.Lock()
			for _, c := range result.configs {
				s.pendingConfigModifies[c.Path] = c.ContentHash
				s.st.PendingConfigModifies[c.Path] = c.ContentHash
			}
			if s.cfg.OfflineDeleteSyncEnabled {
				for _, d := range result.delConfigs {
					s.pendingDeleteConfigPaths[d.Path] = struct{}{}
				}
			}
			s.mu.Unlock()
			if err := s.saveState(); err != nil {
				log.Printf("[sync] save state after SettingSync: %v", err)
			}
		}
	}
}

// --- Payload builders (pure functions for testability) ---

func buildFolderSyncPayload(vault string, lastTime int64, context string, r *scanResult, offlineDel bool) map[string]interface{} {
	p := map[string]interface{}{
		"vault":    vault,
		"lastTime": lastTime,
		"folders":  r.folders,
		"context":  context,
	}
	if offlineDel {
		p["delFolders"] = r.delFolders
	}
	if len(r.missingFolders) > 0 {
		p["missingFolders"] = r.missingFolders
	}
	return p
}

func buildNoteSyncPayload(vault string, lastTime int64, context string, r *scanResult, offlineDel bool) map[string]interface{} {
	p := map[string]interface{}{
		"vault":    vault,
		"lastTime": lastTime,
		"notes":    r.notes,
		"context":  context,
	}
	if offlineDel {
		p["delNotes"] = r.delNotes
	}
	if len(r.missingNotes) > 0 {
		p["missingNotes"] = r.missingNotes
	}
	return p
}

func buildFileSyncPayload(vault string, lastTime int64, context string, r *scanResult, offlineDel bool) map[string]interface{} {
	p := map[string]interface{}{
		"vault":    vault,
		"lastTime": lastTime,
		"files":    r.files,
		"context":  context,
	}
	if offlineDel {
		p["delFiles"] = r.delFiles
	}
	if len(r.missingFiles) > 0 {
		p["missingFiles"] = r.missingFiles
	}
	return p
}

func buildSettingSyncPayload(vault string, lastTime int64, context string, r *scanResult, offlineDel bool) map[string]interface{} {
	p := map[string]interface{}{
		"vault":    vault,
		"lastTime": lastTime,
		"settings": r.configs,
		"cover":    lastTime == 0,
		"context":  context,
	}
	if offlineDel {
		p["delSettings"] = r.delConfigs
	}
	if len(r.missingConfigs) > 0 {
		p["missingSettings"] = r.missingConfigs
	}
	return p
}

// folderSyncDone returns true when FolderSyncEnd has arrived and all folder tasks are complete.
func (s *SyncService) folderSyncDone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.folderSyncEnd {
		return false
	}
	total := s.folderSyncTasks.NeedUpload + s.folderSyncTasks.NeedModify + s.folderSyncTasks.NeedSyncMtime + s.folderSyncTasks.NeedDelete
	return s.folderSyncTasks.Completed >= total
}

// runCheckSyncCompletion polls for completion with a configurable timeout fallback.
func (s *SyncService) runCheckSyncCompletion(wasInitSync bool) {
	timeout := s.syncTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		done := s.isSyncComplete()
		if done || time.Now().After(deadline) {
			if !done {
				log.Printf("[sync] checkSyncCompletion: timeout, forcing completion")
				s.cleanupActiveFileTransfersOnTimeout()
			} else {
				log.Printf("[sync] checkSyncCompletion: sync complete")
			}
			s.onSyncComplete(wasInitSync)
			return
		}
	}
}

// isSyncComplete returns true when all sync modules have finished.
// Completion requires *SyncEnd to arrive because SyncEnd handlers commit sync timestamps,
// clear pending-delete hashes, and update scanned caches. The 30s timeout in
// runCheckSyncCompletion acts as the safety net when SyncEnd is lost.
func (s *SyncService) isSyncComplete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isSyncRequesting {
		return false
	}
	noteDone := s.noteSyncEnd && s.noteSyncTasks.Completed >= (s.noteSyncTasks.NeedUpload+s.noteSyncTasks.NeedModify+s.noteSyncTasks.NeedSyncMtime+s.noteSyncTasks.NeedDelete)
	fileDone := s.fileSyncEnd && s.fileSyncTasks.Completed >= (s.fileSyncTasks.NeedUpload+s.fileSyncTasks.NeedModify+s.fileSyncTasks.NeedSyncMtime+s.fileSyncTasks.NeedDelete)
	fileTransfersDone := len(s.fileDownloadSessions) == 0 && len(s.activeUploads) == 0
	folderDone := s.folderSyncEnd && s.folderSyncTasks.Completed >= (s.folderSyncTasks.NeedUpload+s.folderSyncTasks.NeedModify+s.folderSyncTasks.NeedSyncMtime+s.folderSyncTasks.NeedDelete)
	configDone := !s.cfg.ConfigSyncEnabled || (s.configSyncEnd && s.configSyncTasks.Completed >= (s.configSyncTasks.NeedUpload+s.configSyncTasks.NeedModify+s.configSyncTasks.NeedSyncMtime+s.configSyncTasks.NeedDelete))
	return noteDone && fileDone && fileTransfersDone && folderDone && configDone
}

func (s *SyncService) cleanupActiveFileTransfersOnTimeout() {
	var removeDirs []string
	s.mu.Lock()
	for key, session := range s.fileDownloadSessions {
		session.Cancelled = true
		if session.SlotHeld {
			session.SlotHeld = false
			s.concurrency.ReleaseSlot("download_" + session.Path)
		}
		if !session.Merging && session.TempDir != "" {
			removeDirs = append(removeDirs, session.TempDir)
		}
		delete(s.fileDownloadSessions, key)
	}
	for rel, upload := range s.activeUploads {
		upload.Cancelled = true
		if upload.SlotHeld {
			upload.SlotHeld = false
			s.concurrency.ReleaseSlot(rel)
		}
		if upload.PathHash != "" {
			delete(s.st.UploadCheckpoints, upload.PathHash)
		}
		delete(s.pendingUploadHashes, rel)
		delete(s.st.PendingUploadHashes, rel)
	}
	s.activeUploads = make(map[string]*ActiveUpload)
	s.mu.Unlock()
	for _, dir := range removeDirs {
		_ = os.RemoveAll(dir)
	}
	s.concurrency.Clear()
	s.saveStateLog("FileTransferTimeoutCleanup")
}

// onSyncComplete finalizes the sync round: persists IsInitSync when this was the first sync.
func (s *SyncService) onSyncComplete(wasInitSync bool) {
	log.Printf("[sync] sync round complete (wasInitSync=%v)", wasInitSync)
	if !wasInitSync {
		s.mu.Lock()
		s.st.IsInitSync = true
		s.mu.Unlock()
		if err := s.saveState(); err != nil {
			log.Printf("[sync] persist IsInitSync: %v", err)
		}
	}
	s.syncDoneOnce.Do(func() { close(s.syncDoneCh) })
}

// --- Vault scanning ---

const obsidianConfigDir = ".obsidian"

var configHardExcludes = map[string]bool{
	"workspace.json":        true,
	"workspace-mobile.json": true,
}

// scanVault walks the vault directory and builds a scan result.
func (s *SyncService) scanVault(isLoadLastTime bool) (*scanResult, error) {
	result := newScanResult()
	vaultPath := s.cfg.VaultPath
	if vaultPath == "" {
		return result, nil
	}

	// Snapshot shared state to avoid holding mutex during file I/O.
	s.mu.Lock()
	fileHashMap := copyFileHashMap(s.st.FileHashMap)
	configHashMap := copyFileHashMap(s.st.ConfigHashMap)
	folderSnapshot := copyFolderSnapshot(s.st.FolderSnapshot)
	pendingNoteModifies := copyStringMap(s.pendingNoteModifies)
	pendingUploadHashes := copyStringMap(s.pendingUploadHashes)
	pendingConfigModifies := copyStringMap(s.pendingConfigModifies)
	lastNoteTime := s.st.NoteSyncTime
	lastFileTime := s.st.FileSyncTime
	lastFolderTime := s.st.FolderSyncTime
	lastConfigTime := s.st.ConfigSyncTime
	s.mu.Unlock()

	localNotePaths := make(map[string]struct{})
	localFilePaths := make(map[string]struct{})
	localFolderPaths := make(map[string]struct{})

	err := filepath.WalkDir(vaultPath, func(absPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		relPath, err := filepath.Rel(vaultPath, absPath)
		if err != nil {
			return nil
		}
		relPath = filepath.ToSlash(relPath)
		if relPath == "." {
			return nil
		}

		// Skip .obsidian entirely — handled separately by scanConfigs.
		if d.IsDir() && relPath == obsidianConfigDir {
			return filepath.SkipDir
		}

		if d.IsDir() {
			if s.isFolderPathExcluded(relPath) {
				return filepath.SkipDir
			}
			localFolderPaths[relPath] = struct{}{}

			if isLoadLastTime {
				if _, inSnapshot := folderSnapshot[relPath]; inSnapshot {
					if info, statErr := d.Info(); statErr == nil {
						if info.ModTime().UnixMilli() < lastFolderTime {
							return nil
						}
					}
				}
			}

			result.folders = append(result.folders, SnapFolder{
				Path:     relPath,
				PathHash: h.Path(relPath),
			})
			return nil
		}

		// File handling.
		if s.isVaultFileExcluded(relPath) {
			return nil
		}

		info, statErr := d.Info()
		if statErr != nil {
			return nil
		}
		mtime := info.ModTime().UnixMilli()
		size := info.Size()

		if strings.HasSuffix(strings.ToLower(relPath), ".md") {
			localNotePaths[relPath] = struct{}{}
			if isLoadLastTime {
				if _, ok := fileHashMap[relPath]; ok && mtime < lastNoteTime {
					if _, pending := pendingNoteModifies[relPath]; !pending {
						return nil
					}
				}
			}
			contentHash, fromCache, hashErr := h.FileCached(absPath, fileHashMapToCache(fileHashMap[relPath]))
			if hashErr != nil {
				log.Printf("[scan] hash note %q: %v", relPath, hashErr)
				return nil
			}
			if !fromCache {
				result.scannedNoteHashes[relPath] = state.FileHashEntry{Hash: contentHash, MTime: mtime, Size: size}
			}
			snap := SnapFile{
				Path:        relPath,
				PathHash:    h.Path(relPath),
				ContentHash: contentHash,
				MTime:       mtime,
				CTime:       mtime,
				Size:        size,
			}
			if existing, ok := fileHashMap[relPath]; ok {
				snap.BaseHash = &existing.Hash
			} else {
				snap.BaseHashMissing = true
			}
			result.notes = append(result.notes, snap)

		} else {
			localFilePaths[relPath] = struct{}{}
			if isLoadLastTime {
				if _, ok := fileHashMap[relPath]; ok && mtime < lastFileTime {
					if _, pending := pendingUploadHashes[relPath]; !pending {
						return nil
					}
				}
			}
			contentHash, fromCache, hashErr := h.FileCached(absPath, fileHashMapToCache(fileHashMap[relPath]))
			if hashErr != nil {
				log.Printf("[scan] hash file %q: %v", relPath, hashErr)
				return nil
			}
			if !fromCache {
				result.scannedFileHashes[relPath] = state.FileHashEntry{Hash: contentHash, MTime: mtime, Size: size}
			}
			snap := SnapFile{
				Path:        relPath,
				PathHash:    h.Path(relPath),
				ContentHash: contentHash,
				MTime:       mtime,
				CTime:       mtime,
				Size:        size,
			}
			if existing, ok := fileHashMap[relPath]; ok {
				snap.BaseHash = &existing.Hash
			} else {
				snap.BaseHashMissing = true
			}
			result.files = append(result.files, snap)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk vault %q: %w", vaultPath, err)
	}

	s.computeDelMissing(fileHashMap, localNotePaths, localFilePaths, isLoadLastTime, result)
	s.computeFolderDelMissing(folderSnapshot, localFolderPaths, isLoadLastTime, result)

	if s.cfg.ConfigSyncEnabled {
		if scanErr := s.scanConfigs(vaultPath, configHashMap, pendingConfigModifies, lastConfigTime, isLoadLastTime, result); scanErr != nil {
			log.Printf("[scan] config scan: %v", scanErr)
		}
		s.computeConfigDelMissing(configHashMap, result, isLoadLastTime)
	}

	return result, nil
}

func (s *SyncService) computeDelMissing(fileHashMap map[string]state.FileHashEntry, localNotes, localFiles map[string]struct{}, isLoadLastTime bool, result *scanResult) {
	for path := range fileHashMap {
		if s.isVaultFileExcluded(path) {
			continue
		}
		isNote := strings.HasSuffix(strings.ToLower(path), ".md")
		item := PathHashFile{Path: path, PathHash: h.Path(path)}
		if isNote {
			if _, local := localNotes[path]; local {
				continue
			}
			if s.cfg.OfflineDeleteSyncEnabled {
				result.delNotes = append(result.delNotes, item)
			} else if isLoadLastTime {
				result.missingNotes = append(result.missingNotes, item)
			}
		} else {
			if _, local := localFiles[path]; local {
				continue
			}
			if s.cfg.OfflineDeleteSyncEnabled {
				result.delFiles = append(result.delFiles, item)
			} else if isLoadLastTime {
				result.missingFiles = append(result.missingFiles, item)
			}
		}
	}
}

func (s *SyncService) computeFolderDelMissing(folderSnapshot map[string]int64, localFolders map[string]struct{}, isLoadLastTime bool, result *scanResult) {
	for path := range folderSnapshot {
		if s.isFolderPathExcluded(path) {
			continue
		}
		if _, local := localFolders[path]; local {
			continue
		}
		item := PathHashFile{Path: path, PathHash: h.Path(path)}
		if s.cfg.OfflineDeleteSyncEnabled {
			result.delFolders = append(result.delFolders, item)
		} else if isLoadLastTime {
			result.missingFolders = append(result.missingFolders, item)
		}
	}
}

func (s *SyncService) computeConfigDelMissing(configHashMap map[string]state.FileHashEntry, result *scanResult, isLoadLastTime bool) {
	localConfigs := make(map[string]struct{}, len(result.configs))
	for _, c := range result.configs {
		localConfigs[c.Path] = struct{}{}
	}
	for path := range configHashMap {
		if !s.isConfigSyncPathAllowed(path) {
			continue
		}
		if _, local := localConfigs[path]; local {
			continue
		}
		item := PathHashFile{Path: path, PathHash: h.Path(path)}
		if s.cfg.OfflineDeleteSyncEnabled {
			result.delConfigs = append(result.delConfigs, item)
		} else if isLoadLastTime {
			result.missingConfigs = append(result.missingConfigs, item)
		}
	}
}

// scanConfigs scans .obsidian/ and ConfigSyncOtherDirs for config files.
func (s *SyncService) scanConfigs(vaultPath string, configHashMap map[string]state.FileHashEntry, pendingConfigModifies map[string]string, lastConfigTime int64, isLoadLastTime bool, result *scanResult) error {
	configDir := filepath.Join(vaultPath, obsidianConfigDir)
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		return nil
	}

	addConfig := func(absPath, relPath string) {
		if !s.isConfigSyncPathAllowed(relPath) {
			return
		}
		info, err := os.Stat(absPath)
		if err != nil {
			return
		}
		mtime := info.ModTime().UnixMilli()
		size := info.Size()

		if isLoadLastTime {
			if _, ok := configHashMap[relPath]; ok && mtime < lastConfigTime {
				if _, pending := pendingConfigModifies[relPath]; !pending {
					return
				}
			}
		}

		contentHash, fromCache, hashErr := h.FileCached(absPath, fileHashMapToCache(configHashMap[relPath]))
		if hashErr != nil {
			log.Printf("[scan] hash config %q: %v", relPath, hashErr)
			return
		}
		if !fromCache {
			result.scannedConfigHashes[relPath] = state.FileHashEntry{Hash: contentHash, MTime: mtime, Size: size}
		}

		snap := SnapFile{
			Path:        relPath,
			PathHash:    h.Path(relPath),
			ContentHash: contentHash,
			MTime:       mtime,
			CTime:       mtime,
			Size:        size,
		}
		if existing, ok := configHashMap[relPath]; ok {
			snap.BaseHash = &existing.Hash
		} else {
			snap.BaseHashMissing = true
		}
		result.configs = append(result.configs, snap)
	}

	// .obsidian/ root JSON files (hard excludes applied)
	if entries, err := os.ReadDir(configDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if !strings.HasSuffix(name, ".json") || configHardExcludes[name] {
				continue
			}
			addConfig(filepath.Join(configDir, name), obsidianConfigDir+"/"+name)
		}
	}

	// plugins/*/{ .json, .js, .css }
	pluginsDir := filepath.Join(configDir, "plugins")
	if _, err := os.Stat(pluginsDir); err == nil {
		if pEntries, err := os.ReadDir(pluginsDir); err == nil {
			for _, plugin := range pEntries {
				if !plugin.IsDir() {
					continue
				}
				pluginDir := filepath.Join(pluginsDir, plugin.Name())
				if files, err := os.ReadDir(pluginDir); err == nil {
					for _, f := range files {
						if f.IsDir() {
							continue
						}
						ext := strings.ToLower(filepath.Ext(f.Name()))
						if ext == ".json" || ext == ".js" || ext == ".css" {
							relPath := obsidianConfigDir + "/plugins/" + plugin.Name() + "/" + f.Name()
							addConfig(filepath.Join(pluginDir, f.Name()), relPath)
						}
					}
				}
			}
		}
	}

	// themes/*/{ .css, .json }
	themesDir := filepath.Join(configDir, "themes")
	if _, err := os.Stat(themesDir); err == nil {
		if tEntries, err := os.ReadDir(themesDir); err == nil {
			for _, theme := range tEntries {
				if !theme.IsDir() {
					continue
				}
				themeDir := filepath.Join(themesDir, theme.Name())
				if files, err := os.ReadDir(themeDir); err == nil {
					for _, f := range files {
						if f.IsDir() {
							continue
						}
						ext := strings.ToLower(filepath.Ext(f.Name()))
						if ext == ".css" || ext == ".json" {
							relPath := obsidianConfigDir + "/themes/" + theme.Name() + "/" + f.Name()
							addConfig(filepath.Join(themeDir, f.Name()), relPath)
						}
					}
				}
			}
		}
	}

	// snippets/*.css
	snippetsDir := filepath.Join(configDir, "snippets")
	if _, err := os.Stat(snippetsDir); err == nil {
		if sEntries, err := os.ReadDir(snippetsDir); err == nil {
			for _, f := range sEntries {
				if !f.IsDir() && strings.ToLower(filepath.Ext(f.Name())) == ".css" {
					relPath := obsidianConfigDir + "/snippets/" + f.Name()
					addConfig(filepath.Join(snippetsDir, f.Name()), relPath)
				}
			}
		}
	}

	// ConfigSyncOtherDirs (recursive)
	for _, dir := range s.cfg.ConfigSyncOtherDirs {
		absDir := filepath.Join(vaultPath, dir)
		filepath.WalkDir(absDir, func(absPath string, d fs.DirEntry, wErr error) error { //nolint:errcheck
			if wErr != nil || d.IsDir() {
				return nil
			}
			relPath, relErr := filepath.Rel(vaultPath, absPath)
			if relErr != nil {
				return nil
			}
			addConfig(absPath, filepath.ToSlash(relPath))
			return nil
		})
	}

	return nil
}

// isVaultFileExcluded returns true if a vault-relative file path should be excluded.
func (s *SyncService) isVaultFileExcluded(relPath string) bool {
	for _, w := range s.cfg.SyncExcludeWhitelist {
		if relPath == w || strings.HasPrefix(relPath, w+"/") {
			return false
		}
	}
	for _, folder := range s.cfg.SyncExcludeFolders {
		if relPath == folder || strings.HasPrefix(relPath, folder+"/") {
			return true
		}
	}
	ext := strings.ToLower(filepath.Ext(relPath))
	for _, excludeExt := range s.cfg.SyncExcludeExtensions {
		e := strings.ToLower(excludeExt)
		if !strings.HasPrefix(e, ".") {
			e = "." + e
		}
		if ext == e {
			return true
		}
	}
	return false
}

// isFolderPathExcluded returns true if a vault-relative folder path should be excluded.
func (s *SyncService) isFolderPathExcluded(relPath string) bool {
	for _, w := range s.cfg.SyncExcludeWhitelist {
		if relPath == w || strings.HasPrefix(relPath, w+"/") {
			return false
		}
	}
	for _, folder := range s.cfg.SyncExcludeFolders {
		if relPath == folder || strings.HasPrefix(relPath, folder+"/") {
			return true
		}
	}
	return false
}

// --- Map copy helpers ---

func copyFileHashMap(m map[string]state.FileHashEntry) map[string]state.FileHashEntry {
	out := make(map[string]state.FileHashEntry, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyFolderSnapshot(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyStringMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// fileHashMapToCache converts a state.FileHashEntry to a hash.CacheEntry.
// Returns nil when the hash is empty (no cached entry).
func fileHashMapToCache(e state.FileHashEntry) *h.CacheEntry {
	if e.Hash == "" {
		return nil
	}
	return &h.CacheEntry{Hash: e.Hash, MTime: e.MTime, Size: e.Size}
}

// saveState marshals s.st under the mutex, then writes to disk without the mutex.
func (s *SyncService) saveState() error {
	if s.statePath == "" {
		return nil
	}
	s.mu.Lock()
	data, err := json.MarshalIndent(s.st, "", "  ")
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	if mkErr := os.MkdirAll(filepath.Dir(s.statePath), 0o755); mkErr != nil {
		return fmt.Errorf("create state dir: %w", mkErr)
	}
	return state.WriteFileAtomic(s.statePath, data)
}
