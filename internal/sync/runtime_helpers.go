package sync

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/config"
	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/local"
	"github.com/erichll/go-fast-note-sync/internal/state"
)

const echoSuppressDelay = 500 * time.Millisecond

type echoWriteState struct {
	token    uint64
	inFlight bool
	hasSnap  bool
	hash     string
	prevHash string
	recheck  bool // a callback saw the preceding version during this write
	mtime    int64
	size     int64
	timer    *time.Timer
}

type echoTokenTimer struct {
	token uint64
	timer *time.Timer
}

type echoRenameState struct {
	token  uint64
	oldRel string
	newRel string
	timer  *time.Timer
}

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
	PageIndex   *int   `json:"pageIndex,omitempty"`
}

type receivePathMessage struct {
	Path      string `json:"path"`
	PathHash  string `json:"pathHash"`
	LastTime  int64  `json:"lastTime"`
	PageIndex *int   `json:"pageIndex,omitempty"`
}

type receiveMtimeMessage struct {
	Path      string `json:"path"`
	CTime     int64  `json:"ctime"`
	MTime     int64  `json:"mtime"`
	LastTime  int64  `json:"lastTime"`
	PageIndex *int   `json:"pageIndex,omitempty"`
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
	PageIndex   *int   `json:"pageIndex,omitempty"`
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
	if isFilesystemJunkPath(rel) {
		return false
	}
	if isSensitivePluginConfigPath(rel) {
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

func isSensitivePluginConfigPath(rel string) bool {
	rel, err := normalizeSyncPath(rel)
	if err != nil {
		return false
	}
	return rel == obsidianConfigDir+"/plugins/fast-note-sync/data.json"
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

func (s *SyncService) echoSuppressWindow() time.Duration {
	// Match watcher.newWithBackend's fallback for non-positive delays too.
	delay := config.Default().SyncUpdateDelay
	if s != nil && s.cfg != nil && s.cfg.SyncUpdateDelay > 0 {
		delay = s.cfg.SyncUpdateDelay
	}
	return time.Duration(delay)*time.Millisecond + echoSuppressDelay
}

func (s *SyncService) ensureEchoMapsLocked() {
	if s.lastSyncMtime == nil {
		s.lastSyncMtime = make(map[string]int64)
	}
	if s.lastSyncPathDeleted == nil {
		s.lastSyncPathDeleted = make(map[string]struct{})
	}
	if s.lastSyncPathRenamed == nil {
		s.lastSyncPathRenamed = make(map[string]struct{})
	}
	if s.echoWriteMeta == nil {
		s.echoWriteMeta = make(map[string]echoWriteState)
	}
	if s.echoDeleteMeta == nil {
		s.echoDeleteMeta = make(map[string]echoTokenTimer)
	}
	if s.echoRenameMeta == nil {
		s.echoRenameMeta = make(map[string]echoRenameState)
	}
}

func (s *SyncService) startEchoWriteTimerLocked(rel string, token uint64, window time.Duration) *time.Timer {
	return time.AfterFunc(window, func() {
		s.mu.Lock()
		if cur, ok := s.echoWriteMeta[rel]; ok && cur.token == token && !cur.inFlight {
			delete(s.echoWriteMeta, rel)
			delete(s.lastSyncMtime, rel)
		}
		s.mu.Unlock()
	})
}

func (s *SyncService) beginRemoteWrite(rel, contentHash string, size int64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureEchoMapsLocked()
	s.clearDeleteEchoLocked(rel)
	s.echoSeq++
	token := s.echoSeq
	var prevHash string
	var recheck bool
	if old, ok := s.echoWriteMeta[rel]; ok {
		recheck = old.recheck
		if old.timer != nil {
			old.timer.Stop()
		}
		if old.hasSnap {
			prevHash = old.hash
		}
	}
	now := time.Now().UnixMilli()
	s.lastSyncMtime[rel] = now
	s.echoWriteMeta[rel] = echoWriteState{
		token:    token,
		inFlight: true,
		hasSnap:  contentHash != "",
		hash:     contentHash,
		prevHash: prevHash,
		recheck:  recheck,
		mtime:    now,
		size:     size,
	}
	return token
}

func (s *SyncService) finishRemoteWriteLocked(rel string, token uint64, contentHash string, mtime, size int64) {
	s.ensureEchoMapsLocked()
	cur, ok := s.echoWriteMeta[rel]
	if !ok || cur.token != token {
		return
	}
	if cur.timer != nil {
		cur.timer.Stop()
	}
	if mtime > 0 {
		s.lastSyncMtime[rel] = mtime
	}
	window := s.echoSuppressWindow()
	timer := s.startEchoWriteTimerLocked(rel, token, window)
	s.echoWriteMeta[rel] = echoWriteState{
		token:    token,
		inFlight: false,
		hasSnap:  true,
		hash:     contentHash,
		mtime:    mtime,
		size:     size,
		timer:    timer,
	}
	if cur.recheck {
		s.recheckLocalWriteLocked(rel)
	}
}

// A callback matching the preceding remote version may be either a delayed
// echo or a genuine undo after replacement. Re-read it only after the remote
// operation settles. Never send under s.mu or while retaining the path lock.
func (s *SyncService) recheckLocalWriteLocked(rel string) {
	s.localRechecks++
	go func() {
		defer func() {
			s.mu.Lock()
			s.localRechecks--
			s.mu.Unlock()
		}()
		unlock := s.lockPath(rel)
		unlock()
		s.HandleLocalModify(local.PathEvent{Path: rel})
	}()
}

func (s *SyncService) finishRemoteWriteBaseline(rel, abs string, token uint64, msgMTime int64, baselineHash string, baselineSize int64) {
	diskMTime := msgMTime
	if info, err := os.Stat(abs); err == nil {
		diskMTime = info.ModTime().UnixMilli()
	}
	s.mu.Lock()
	s.finishRemoteWriteLocked(rel, token, baselineHash, diskMTime, baselineSize)
	s.lastSyncMtime[rel] = msgMTime
	s.mu.Unlock()
}

func (s *SyncService) abortRemoteWrite(rel string, token uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.echoWriteMeta[rel]; ok && cur.token == token {
		if cur.timer != nil {
			cur.timer.Stop()
		}
		delete(s.echoWriteMeta, rel)
		delete(s.lastSyncMtime, rel)
		if cur.recheck {
			s.recheckLocalWriteLocked(rel)
		}
	}
}

// Real local moves invalidate evidence for both endpoints, including a
// destination-only rename marker which clearDeleteEcho intentionally retains.
func (s *SyncService) invalidateLocalRenameEchoes(oldRel, newRel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rel := range []string{oldRel, newRel} {
		s.clearRemoteWriteLocked(rel)
		s.clearDeleteEchoLocked(rel)
		if cur, ok := s.echoRenameMeta[rel]; ok && cur.timer != nil {
			cur.timer.Stop()
		}
		delete(s.echoRenameMeta, rel)
		delete(s.lastSyncPathRenamed, rel)
	}
}

func (s *SyncService) clearRemoteWriteLocked(rel string) {
	if cur, ok := s.echoWriteMeta[rel]; ok && cur.timer != nil {
		cur.timer.Stop()
	}
	delete(s.echoWriteMeta, rel)
	delete(s.lastSyncMtime, rel)
}

func (s *SyncService) clearRemoteWrite(rel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearRemoteWriteLocked(rel)
}

func (s *SyncService) clearDeleteEchoLocked(rel string) {
	if cur, ok := s.echoDeleteMeta[rel]; ok && cur.timer != nil {
		cur.timer.Stop()
	}
	delete(s.echoDeleteMeta, rel)
	delete(s.lastSyncPathDeleted, rel)
	if cur, ok := s.echoRenameMeta[rel]; ok && cur.oldRel == rel {
		if cur.timer != nil {
			cur.timer.Stop()
		}
		delete(s.echoRenameMeta, rel)
		delete(s.lastSyncPathRenamed, rel)
	}
}

func (s *SyncService) clearDeleteEcho(rel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearDeleteEchoLocked(rel)
}

func (s *SyncService) addIgnoredFile(rel string) {
	s.mu.Lock()
	s.ensureEchoMapsLocked()
	s.clearDeleteEchoLocked(rel)
	s.echoSeq++
	token := s.echoSeq
	if old, ok := s.echoWriteMeta[rel]; ok && old.timer != nil {
		old.timer.Stop()
	}
	now := time.Now().UnixMilli()
	s.lastSyncMtime[rel] = now
	window := s.echoSuppressWindow()
	timer := s.startEchoWriteTimerLocked(rel, token, window)
	s.echoWriteMeta[rel] = echoWriteState{token: token, mtime: now, timer: timer}
	s.mu.Unlock()
}

func (s *SyncService) markDeleted(rel string) {
	s.mu.Lock()
	s.ensureEchoMapsLocked()
	s.echoSeq++
	token := s.echoSeq
	if old, ok := s.echoDeleteMeta[rel]; ok && old.timer != nil {
		old.timer.Stop()
	}
	s.lastSyncPathDeleted[rel] = struct{}{}
	window := s.echoSuppressWindow()
	timer := time.AfterFunc(window, func() {
		s.mu.Lock()
		if cur, ok := s.echoDeleteMeta[rel]; ok && cur.token == token {
			delete(s.echoDeleteMeta, rel)
			delete(s.lastSyncPathDeleted, rel)
		}
		s.mu.Unlock()
	})
	s.echoDeleteMeta[rel] = echoTokenTimer{token: token, timer: timer}
	s.mu.Unlock()
}

func (s *SyncService) markRenamed(oldRel, newRel string) {
	s.mu.Lock()
	s.ensureEchoMapsLocked()
	s.echoSeq++
	token := s.echoSeq
	window := s.echoSuppressWindow()
	for _, p := range []string{oldRel, newRel} {
		if p == "" {
			continue
		}
		if old, ok := s.echoRenameMeta[p]; ok && old.timer != nil {
			old.timer.Stop()
		}
		s.lastSyncPathRenamed[p] = struct{}{}
		pathKey := p
		timer := time.AfterFunc(window, func() {
			s.mu.Lock()
			if cur, ok := s.echoRenameMeta[pathKey]; ok && cur.token == token {
				delete(s.echoRenameMeta, pathKey)
				delete(s.lastSyncPathRenamed, pathKey)
			}
			s.mu.Unlock()
		})
		s.echoRenameMeta[p] = echoRenameState{token: token, oldRel: oldRel, newRel: newRel, timer: timer}
	}
	s.mu.Unlock()
}

func pathConfirmedMissing(abs string) (bool, error) {
	_, err := os.Lstat(abs)
	if err == nil {
		return false, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	return false, err
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
	s.incrementCompletedPage(module, nil)
}

func (s *SyncService) incrementCompletedPage(module string, pageIndex *int) {
	s.mu.Lock()
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
	action, payload, send := s.completeSyncPageLocked(module, pageIndex)
	s.mu.Unlock()
	if send {
		if err := s.Send(action, payload); err != nil {
			log.Printf("[sync] send %s: %v", action, err)
		} else {
			log.Printf("[sync] sent %s pageIndex=%v context=%q", action, payload["pageIndex"], payload["context"])
		}
	}
}

func (s *SyncService) completeSyncPage(module string, pageIndex *int) {
	s.mu.Lock()
	action, payload, send := s.completeSyncPageLocked(module, pageIndex)
	s.mu.Unlock()
	if send {
		if err := s.Send(action, payload); err != nil {
			log.Printf("[sync] send %s: %v", action, err)
		} else {
			log.Printf("[sync] sent %s pageIndex=%v context=%q", action, payload["pageIndex"], payload["context"])
		}
	}
}

func (s *SyncService) startSyncPaging(module, context string) {
	s.mu.Lock()
	total := s.syncTaskTotalLocked(module)
	if total == 0 {
		s.mu.Unlock()
		return
	}
	tracker := &syncPageTracker{Context: context, Pages: make(map[int]*syncPage)}
	s.syncPages[module] = tracker
	action := syncPageAckAction(module)
	payload := map[string]interface{}{"context": context, "vault": s.cfg.Vault, "pageIndex": -1}
	s.mu.Unlock()
	if err := s.Send(action, payload); err != nil {
		log.Printf("[sync] send %s: %v", action, err)
	} else {
		log.Printf("[sync] sent %s pageIndex=-1 context=%q", action, context)
	}
}

func (s *SyncService) handleSyncPage(module string, env Envelope) {
	var pageData struct {
		PageIndex  *int `json:"pageIndex"`
		TotalCount int  `json:"totalCount"`
		IsLast     bool `json:"isLast"`
	}
	if err := json.Unmarshal(env.Data, &pageData); err != nil {
		log.Printf("[sync] parse %s page: %v", module, err)
		return
	}
	s.mu.Lock()
	tracker := s.syncPages[module]
	if tracker == nil {
		tracker = &syncPageTracker{Context: env.Context, Pages: make(map[int]*syncPage)}
		s.syncPages[module] = tracker
	}
	if env.Context != "" {
		tracker.Context = env.Context
	}
	pageIndex := env.PageIndex
	if pageData.PageIndex != nil {
		pageIndex = *pageData.PageIndex
	}
	if _, exists := tracker.Pages[pageIndex]; !exists {
		tracker.Pages[pageIndex] = &syncPage{TotalCount: pageData.TotalCount, IsLast: pageData.IsLast}
		log.Printf("[sync] registered %s page=%d total=%d last=%t context=%q", module, pageIndex, pageData.TotalCount, pageData.IsLast, tracker.Context)
	}
	action, payload, send := s.advanceSyncPageAckLocked(module, tracker)
	s.mu.Unlock()
	if send {
		if err := s.Send(action, payload); err != nil {
			log.Printf("[sync] send %s: %v", action, err)
		} else {
			log.Printf("[sync] sent %s pageIndex=%d context=%q", action, payload["pageIndex"], payload["context"])
		}
	}
}

func (s *SyncService) completeSyncPageLocked(module string, pageIndex *int) (string, map[string]interface{}, bool) {
	if pageIndex == nil {
		return "", nil, false
	}
	tracker := s.syncPages[module]
	if tracker == nil {
		return "", nil, false
	}
	page := tracker.Pages[*pageIndex]
	if page == nil || page.CompletedCount >= page.TotalCount {
		return "", nil, false
	}
	page.CompletedCount++
	if page.CompletedCount == page.TotalCount {
		log.Printf("[sync] completed %s page=%d total=%d last=%t", module, *pageIndex, page.TotalCount, page.IsLast)
	}
	return s.advanceSyncPageAckLocked(module, tracker)
}

func (s *SyncService) advanceSyncPageAckLocked(module string, tracker *syncPageTracker) (string, map[string]interface{}, bool) {
	highest := -1
	for {
		page := tracker.Pages[tracker.AckWatermark]
		if page == nil || page.IsLast || page.CompletedCount < page.TotalCount {
			break
		}
		highest = tracker.AckWatermark
		delete(tracker.Pages, tracker.AckWatermark)
		tracker.AckWatermark++
	}
	if highest < 0 {
		return "", nil, false
	}
	return syncPageAckAction(module), map[string]interface{}{
		"context": tracker.Context, "vault": s.cfg.Vault, "pageIndex": highest,
	}, true
}

func (s *SyncService) syncTaskTotalLocked(module string) int {
	var tasks SyncTaskCounter
	switch module {
	case "note":
		tasks = s.noteSyncTasks
	case "file":
		tasks = s.fileSyncTasks
	case "config":
		tasks = s.configSyncTasks
	case "folder":
		tasks = s.folderSyncTasks
	}
	return tasks.NeedUpload + tasks.NeedModify + tasks.NeedSyncMtime + tasks.NeedDelete
}

func syncPageAckAction(module string) string {
	switch module {
	case "note":
		return "NoteSyncPageAck"
	case "file":
		return "FileSyncPageAck"
	case "config":
		return "SettingSyncPageAck"
	case "folder":
		return "FolderSyncPageAck"
	default:
		return ""
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
	hash := h.FileContent(content)
	if action == "NoteModify" {
		hash = h.Text(string(content))
	}
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
	if action == "NoteModify" {
		s.mu.Lock()
		if meta, ok := s.echoWriteMeta[rp.Rel]; ok && meta.inFlight && meta.hash != "" {
			// The incoming revision is already known remotely even if its
			// local write has not finalized. Preserve optimistic conflict
			// checks for an edit delivered during that write.
			payload["baseHash"] = meta.hash
		} else if entry, ok := s.st.FileHashMap[rp.Rel]; ok && entry.Hash != "" {
			payload["baseHash"] = entry.Hash
		} else {
			payload["baseHashMissing"] = true
		}
		s.mu.Unlock()
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
		if action == "FileRename" {
			contentHash = h.FileContent(b)
		} else {
			contentHash = h.Text(string(b))
		}
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
	now := time.Now().UnixMilli()
	payload := map[string]interface{}{
		"vault":    s.cfg.Vault,
		"path":     rp.Rel,
		"pathHash": h.Path(rp.Rel),
		"mtime":    now,
	}
	if err := s.Send("FolderModify", payload); err != nil {
		return err
	}
	s.mu.Lock()
	s.st.FolderSnapshot[rp.Rel] = now
	s.mu.Unlock()
	if err := s.saveState(); err != nil {
		return fmt.Errorf("save folder snapshot after FolderModify: %w", err)
	}
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
