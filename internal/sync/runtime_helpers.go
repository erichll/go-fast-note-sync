package sync

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/config"
	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

const echoSuppressDelay = 500 * time.Millisecond

// ConcurrencyManager limits local upload-style operations and provides one FIFO
// lane for rename operations whose Ack does not carry a path.
type ConcurrencyManager struct {
	enabled bool
	slots   chan struct{}
	fifo    chan struct{}
}

func NewConcurrencyManager(cfg *config.Config) *ConcurrencyManager {
	max := 1
	enabled := false
	if cfg != nil {
		enabled = cfg.ConcurrencyControlEnabled
		max = cfg.MaxConcurrentUploads
	}
	if max <= 0 {
		max = 1
	}
	return &ConcurrencyManager{
		enabled: enabled,
		slots:   make(chan struct{}, max),
		fifo:    make(chan struct{}, 1),
	}
}

func (m *ConcurrencyManager) WaitForSlot(_ string, fifo bool, _ int) {
	if m == nil || !m.enabled {
		return
	}
	if fifo {
		m.fifo <- struct{}{}
		return
	}
	m.slots <- struct{}{}
}

func (m *ConcurrencyManager) ReleaseSlot(_ string) {
	if m == nil || !m.enabled {
		return
	}
	select {
	case <-m.slots:
	default:
	}
}

func (m *ConcurrencyManager) ReleaseFifoSlot() {
	if m == nil || !m.enabled {
		return
	}
	select {
	case <-m.fifo:
	default:
	}
}

func (m *ConcurrencyManager) Clear() {
	if m == nil {
		return
	}
	for {
		select {
		case <-m.slots:
		default:
			goto fifo
		}
	}
fifo:
	for {
		select {
		case <-m.fifo:
		default:
			return
		}
	}
}

type resolvedPath struct {
	Rel string
	Abs string
}

type receiveContentMessage struct {
	Path        string `json:"path"`
	PathHash    string `json:"pathHash"`
	Content     string `json:"content"`
	ContentHash string `json:"contentHash"`
	CTime       int64  `json:"ctime"`
	MTime       int64  `json:"mtime"`
	Size        int64  `json:"size"`
	LastTime    int64  `json:"lastTime"`
}

type receivePathMessage struct {
	Path     string `json:"path"`
	PathHash string `json:"pathHash"`
	LastTime int64  `json:"lastTime"`
}

type receiveMtimeMessage struct {
	Path     string `json:"path"`
	CTime    int64  `json:"ctime"`
	MTime    int64  `json:"mtime"`
	LastTime int64  `json:"lastTime"`
}

type receiveRenameMessage struct {
	OldPath     string `json:"oldPath"`
	OldPathHash string `json:"oldPathHash"`
	Path        string `json:"path"`
	PathHash    string `json:"pathHash"`
	ContentHash string `json:"contentHash"`
	CTime       int64  `json:"ctime"`
	MTime       int64  `json:"mtime"`
	Size        int64  `json:"size"`
	LastTime    int64  `json:"lastTime"`
}

func normalizeSyncPath(raw string) (string, error) {
	p := strings.TrimSpace(strings.ReplaceAll(raw, "\\", "/"))
	if p == "" {
		return "", fmt.Errorf("path is empty")
	}
	if strings.HasPrefix(p, "/") || filepath.IsAbs(p) || path.IsAbs(p) {
		return "", fmt.Errorf("absolute path rejected: %q", raw)
	}
	clean := path.Clean(p)
	if clean == "." || clean == "" {
		return "", fmt.Errorf("path is empty")
	}
	if clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") {
		return "", fmt.Errorf("path escapes vault: %q", raw)
	}
	return clean, nil
}

func (s *SyncService) resolveVaultPath(raw string) (resolvedPath, error) {
	rel, err := normalizeSyncPath(raw)
	if err != nil {
		return resolvedPath{}, err
	}
	if s.cfg == nil || s.cfg.VaultPath == "" {
		return resolvedPath{}, fmt.Errorf("vault path is empty")
	}
	vaultAbs, err := filepath.Abs(s.cfg.VaultPath)
	if err != nil {
		return resolvedPath{}, err
	}
	abs, err := filepath.Abs(filepath.Join(vaultAbs, filepath.FromSlash(rel)))
	if err != nil {
		return resolvedPath{}, err
	}
	back, err := filepath.Rel(vaultAbs, abs)
	if err != nil {
		return resolvedPath{}, err
	}
	if back == ".." || strings.HasPrefix(back, ".."+string(filepath.Separator)) || filepath.IsAbs(back) {
		return resolvedPath{}, fmt.Errorf("path escapes vault: %q", raw)
	}
	return resolvedPath{Rel: rel, Abs: abs}, nil
}

func (s *SyncService) isConfigSyncPathAllowed(rel string) bool {
	rel, err := normalizeSyncPath(rel)
	if err != nil {
		return false
	}
	if strings.HasPrefix(rel, obsidianConfigDir+"/_localStorage/") || rel == obsidianConfigDir+"/_localStorage" {
		return false
	}
	if strings.HasPrefix(rel, obsidianConfigDir+"/") {
		parts := strings.Split(rel, "/")
		ext := strings.ToLower(path.Ext(rel))
		switch {
		case len(parts) == 2:
			return ext == ".json" && !configHardExcludes[parts[1]]
		case len(parts) == 4 && parts[1] == "plugins":
			return ext == ".json" || ext == ".js" || ext == ".css"
		case len(parts) == 4 && parts[1] == "themes":
			return ext == ".json" || ext == ".css"
		case len(parts) == 3 && parts[1] == "snippets":
			return ext == ".css"
		}
	}
	for _, dir := range s.cfg.ConfigSyncOtherDirs {
		allowed, err := normalizeSyncPath(dir)
		if err != nil || allowed == "" {
			continue
		}
		if rel == allowed || strings.HasPrefix(rel, allowed+"/") {
			return true
		}
	}
	return false
}

func isLocalStorageSettingPath(raw string) bool {
	rel, err := normalizeSyncPath(raw)
	if err != nil {
		return false
	}
	return rel == obsidianConfigDir+"/_localStorage" || strings.HasPrefix(rel, obsidianConfigDir+"/_localStorage/")
}

func (s *SyncService) lockPath(key string) func() {
	const attempts = 10
	const delay = 50 * time.Millisecond
	s.mu.Lock()
	ch := s.pathLocks[key]
	if ch == nil {
		ch = make(chan struct{}, 1)
		s.pathLocks[key] = ch
	}
	s.mu.Unlock()

	for i := 0; i < attempts; i++ {
		select {
		case ch <- struct{}{}:
			return func() { <-ch }
		default:
			time.Sleep(delay)
		}
	}
	ch <- struct{}{}
	return func() { <-ch }
}

func (s *SyncService) addIgnoredFile(rel string) {
	s.mu.Lock()
	s.lastSyncMtime[rel] = time.Now().UnixMilli()
	s.mu.Unlock()
	time.AfterFunc(echoSuppressDelay, func() {
		s.mu.Lock()
		delete(s.lastSyncMtime, rel)
		s.mu.Unlock()
	})
}

func (s *SyncService) markDeleted(rel string) {
	s.mu.Lock()
	s.lastSyncPathDeleted[rel] = struct{}{}
	s.mu.Unlock()
	time.AfterFunc(echoSuppressDelay, func() {
		s.mu.Lock()
		delete(s.lastSyncPathDeleted, rel)
		s.mu.Unlock()
	})
}

func (s *SyncService) markRenamed(oldRel, newRel string) {
	s.mu.Lock()
	s.lastSyncPathRenamed[oldRel] = struct{}{}
	s.lastSyncPathRenamed[newRel] = struct{}{}
	s.mu.Unlock()
	time.AfterFunc(echoSuppressDelay, func() {
		s.mu.Lock()
		delete(s.lastSyncPathRenamed, oldRel)
		delete(s.lastSyncPathRenamed, newRel)
		s.mu.Unlock()
	})
}

func fileEntry(absPath, contentHash string) (state.FileHashEntry, error) {
	info, err := os.Stat(absPath)
	if err != nil {
		return state.FileHashEntry{}, err
	}
	return state.FileHashEntry{Hash: contentHash, MTime: info.ModTime().UnixMilli(), Size: info.Size()}, nil
}

func (s *SyncService) updateSyncTime(module string, lastTime int64) {
	if lastTime <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch module {
	case "note":
		if lastTime > s.st.NoteSyncTime {
			s.st.NoteSyncTime = lastTime
		}
	case "file":
		if lastTime > s.st.FileSyncTime {
			s.st.FileSyncTime = lastTime
		}
	case "config":
		if lastTime > s.st.ConfigSyncTime {
			s.st.ConfigSyncTime = lastTime
		}
	case "folder":
		if lastTime > s.st.FolderSyncTime {
			s.st.FolderSyncTime = lastTime
		}
	}
}

func (s *SyncService) incrementCompleted(module string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch module {
	case "note":
		s.noteSyncTasks.Completed++
	case "file":
		s.fileSyncTasks.Completed++
	case "config":
		s.configSyncTasks.Completed++
	case "folder":
		s.folderSyncTasks.Completed++
	}
}

func (s *SyncService) saveStateLog(context string) {
	if err := s.saveState(); err != nil {
		log.Printf("[sync] save state after %s: %v", context, err)
	}
}

func pathHashPayload(rel string) map[string]interface{} {
	return map[string]interface{}{
		"path":     rel,
		"pathHash": h.Path(rel),
	}
}

func (s *SyncService) setPendingNoteModify(rel, contentHash string) {
	s.mu.Lock()
	s.pendingNoteModifies[rel] = contentHash
	s.st.PendingNoteModifies[rel] = contentHash
	s.mu.Unlock()
}

func (s *SyncService) clearPendingNoteModify(rel string) {
	s.mu.Lock()
	delete(s.pendingNoteModifies, rel)
	delete(s.st.PendingNoteModifies, rel)
	s.mu.Unlock()
}

func (s *SyncService) setPendingUpload(rel, contentHash string) {
	s.mu.Lock()
	s.pendingUploadHashes[rel] = contentHash
	s.st.PendingUploadHashes[rel] = contentHash
	s.mu.Unlock()
}

func (s *SyncService) clearPendingUpload(rel string) {
	s.mu.Lock()
	delete(s.pendingUploadHashes, rel)
	delete(s.st.PendingUploadHashes, rel)
	s.mu.Unlock()
}

func (s *SyncService) setPendingConfigModify(rel, contentHash string) {
	s.mu.Lock()
	s.pendingConfigModifies[rel] = contentHash
	s.st.PendingConfigModifies[rel] = contentHash
	s.mu.Unlock()
}

func (s *SyncService) clearPendingConfigModify(rel string) {
	s.mu.Lock()
	delete(s.pendingConfigModifies, rel)
	delete(s.st.PendingConfigModifies, rel)
	s.mu.Unlock()
}

func (s *SyncService) sendFileContentModify(action string, rp resolvedPath, pending func(string, string), completed func()) error {
	content, err := os.ReadFile(rp.Abs)
	if err != nil {
		return err
	}
	hash := h.Content(content)
	info, err := os.Stat(rp.Abs)
	if err != nil {
		return err
	}
	payload := map[string]interface{}{
		"vault":       s.cfg.Vault,
		"path":        rp.Rel,
		"pathHash":    h.Path(rp.Rel),
		"content":     string(content),
		"contentHash": hash,
		"mtime":       info.ModTime().UnixMilli(),
		"ctime":       info.ModTime().UnixMilli(),
	}
	s.concurrency.WaitForSlot(rp.Rel, false, 0)
	if err := s.Send(action, payload); err != nil {
		s.concurrency.ReleaseSlot(rp.Rel)
		return err
	}
	pending(rp.Rel, hash)
	if completed != nil {
		completed()
	}
	s.saveStateLog(action)
	return nil
}

func (s *SyncService) sendRename(action, oldRaw, newRaw string, pending func(string, string, string)) error {
	oldRP, err := s.resolveVaultPath(oldRaw)
	if err != nil {
		return err
	}
	newRP, err := s.resolveVaultPath(newRaw)
	if err != nil {
		return err
	}
	contentHash := ""
	if b, readErr := os.ReadFile(newRP.Abs); readErr == nil {
		contentHash = h.Content(b)
	} else {
		s.mu.Lock()
		if entry, ok := s.st.FileHashMap[oldRP.Rel]; ok {
			contentHash = entry.Hash
		}
		s.mu.Unlock()
	}
	payload := map[string]interface{}{
		"vault":       s.cfg.Vault,
		"oldPath":     oldRP.Rel,
		"oldPathHash": h.Path(oldRP.Rel),
		"path":        newRP.Rel,
		"pathHash":    h.Path(newRP.Rel),
	}
	s.concurrency.WaitForSlot(newRP.Rel, true, 0)
	if err := s.Send(action, payload); err != nil {
		s.concurrency.ReleaseFifoSlot()
		return err
	}
	pending(oldRP.Rel, newRP.Rel, contentHash)
	s.saveStateLog(action)
	return nil
}

func (s *SyncService) SendNoteDelete(raw string) error {
	rp, err := s.resolveVaultPath(raw)
	if err != nil {
		return err
	}
	s.concurrency.WaitForSlot(rp.Rel, false, 0)
	if err := s.Send("NoteDelete", map[string]interface{}{"vault": s.cfg.Vault, "path": rp.Rel, "pathHash": h.Path(rp.Rel)}); err != nil {
		s.concurrency.ReleaseSlot(rp.Rel)
		return err
	}
	s.mu.Lock()
	s.pendingNoteDeleteAcks[rp.Rel] = struct{}{}
	s.mu.Unlock()
	return nil
}

func (s *SyncService) SendFileDelete(raw string) error {
	rp, err := s.resolveVaultPath(raw)
	if err != nil {
		return err
	}
	s.cancelUpload(rp.Rel)
	s.concurrency.WaitForSlot(rp.Rel, false, 0)
	if err := s.Send("FileDelete", map[string]interface{}{"vault": s.cfg.Vault, "path": rp.Rel, "pathHash": h.Path(rp.Rel)}); err != nil {
		s.concurrency.ReleaseSlot(rp.Rel)
		return err
	}
	s.mu.Lock()
	s.pendingFileDeleteAcks[rp.Rel] = struct{}{}
	s.mu.Unlock()
	return nil
}

func (s *SyncService) SendSettingDelete(raw string) error {
	rp, err := s.resolveVaultPath(raw)
	if err != nil {
		return err
	}
	if !s.isConfigSyncPathAllowed(rp.Rel) {
		return fmt.Errorf("setting path outside sync scope: %s", rp.Rel)
	}
	s.concurrency.WaitForSlot(rp.Rel, false, 0)
	if err := s.Send("SettingDelete", map[string]interface{}{"vault": s.cfg.Vault, "path": rp.Rel, "pathHash": h.Path(rp.Rel)}); err != nil {
		s.concurrency.ReleaseSlot(rp.Rel)
		return err
	}
	s.mu.Lock()
	s.pendingConfigDeleteAcks[rp.Rel] = struct{}{}
	s.mu.Unlock()
	return nil
}

func (s *SyncService) SendNoteRename(oldPath, newPath string) error {
	return s.sendRename("NoteRename", oldPath, newPath, func(oldRel, newRel, contentHash string) {
		s.mu.Lock()
		s.pendingNoteRenames = append(s.pendingNoteRenames, struct{ OldPath, NewPath, ContentHash string }{oldRel, newRel, contentHash})
		s.mu.Unlock()
	})
}

func (s *SyncService) SendFileRename(oldPath, newPath string) error {
	oldRP, err := s.resolveVaultPath(oldPath)
	if err != nil {
		return err
	}
	s.cancelUpload(oldRP.Rel)
	return s.sendRename("FileRename", oldPath, newPath, func(oldRel, newRel, contentHash string) {
		s.mu.Lock()
		s.pendingFileRenames = append(s.pendingFileRenames, struct{ OldPath, NewPath, ContentHash string }{oldRel, newRel, contentHash})
		s.mu.Unlock()
	})
}

func (s *SyncService) SendSettingRename(oldPath, newPath string) error {
	oldRP, err := s.resolveVaultPath(oldPath)
	if err != nil {
		return err
	}
	newRP, err := s.resolveVaultPath(newPath)
	if err != nil {
		return err
	}
	if !s.isConfigSyncPathAllowed(oldRP.Rel) || !s.isConfigSyncPathAllowed(newRP.Rel) {
		return fmt.Errorf("setting path outside sync scope")
	}
	if err := s.SendSettingDelete(oldRP.Rel); err != nil {
		return err
	}
	return s.sendFileContentModify("SettingModify", newRP, s.setPendingConfigModify, nil)
}

func (s *SyncService) SendFolderModify(raw string) error {
	rp, err := s.resolveVaultPath(raw)
	if err != nil {
		return err
	}
	if s.isFolderPathExcluded(rp.Rel) {
		return fmt.Errorf("folder excluded: %s", rp.Rel)
	}
	payload := map[string]interface{}{
		"vault":    s.cfg.Vault,
		"path":     rp.Rel,
		"pathHash": h.Path(rp.Rel),
		"mtime":    time.Now().UnixMilli(),
	}
	if err := s.Send("FolderModify", payload); err != nil {
		return err
	}
	s.mu.Lock()
	s.st.FolderSnapshot[rp.Rel] = time.Now().UnixMilli()
	s.mu.Unlock()
	return nil
}

func (s *SyncService) SendFolderDelete(raw string) error {
	rp, err := s.resolveVaultPath(raw)
	if err != nil {
		return err
	}
	if err := s.Send("FolderDelete", map[string]interface{}{"vault": s.cfg.Vault, "path": rp.Rel, "pathHash": h.Path(rp.Rel)}); err != nil {
		return err
	}
	s.mu.Lock()
	s.pendingDeleteFolderPaths[rp.Rel] = struct{}{}
	s.mu.Unlock()
	return nil
}

func (s *SyncService) SendFolderRename(oldPath, newPath string) error {
	oldRP, err := s.resolveVaultPath(oldPath)
	if err != nil {
		return err
	}
	newRP, err := s.resolveVaultPath(newPath)
	if err != nil {
		return err
	}
	payload := map[string]interface{}{
		"vault":       s.cfg.Vault,
		"oldPath":     oldRP.Rel,
		"oldPathHash": h.Path(oldRP.Rel),
		"path":        newRP.Rel,
		"pathHash":    h.Path(newRP.Rel),
		"mtime":       time.Now().UnixMilli(),
	}
	if err := s.Send("FolderRename", payload); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.st.FolderSnapshot, oldRP.Rel)
	s.st.FolderSnapshot[newRP.Rel] = time.Now().UnixMilli()
	s.mu.Unlock()
	return nil
}
