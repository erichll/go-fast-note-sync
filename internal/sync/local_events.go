package sync

import (
	"errors"
	"log"
	"os"
	"strings"

	h "github.com/erichll/go-fast-note-sync/internal/hash"
	"github.com/erichll/go-fast-note-sync/internal/local"
)

type localCategory int

const (
	localCategorySkip localCategory = iota
	localCategoryNote
	localCategoryFile
	localCategorySetting
	localCategoryFolder
)

func localSkipped() local.Result {
	return local.Result{}
}

func localAttempt(err error) local.Result {
	return local.Result{Attempted: true, Err: err}
}

func (s *SyncService) localReady() bool {
	if s == nil || s.cfg == nil {
		return false
	}
	if !s.cfg.SyncEnabled || s.cfg.ManualSyncEnabled || s.cfg.ReadOnlySyncEnabled {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isOpen && s.isAuth
}

func (s *SyncService) computeLocalCategoryHash(abs string, cat localCategory) (string, bool) {
	b, err := os.ReadFile(abs)
	if err != nil {
		return "", false
	}
	switch cat {
	case localCategoryNote:
		return h.Text(string(b)), true
	case localCategoryFile, localCategorySetting:
		return h.FileContent(b), true
	default:
		return "", false
	}
}

func (s *SyncService) isWriteEchoed(rp resolvedPath, cat localCategory) bool {
	for {
		s.mu.Lock()
		_, inMap := s.lastSyncMtime[rp.Rel]
		meta, hasMeta := s.echoWriteMeta[rp.Rel]
		s.mu.Unlock()

		if !inMap {
			return false
		}
		if !hasMeta {
			// Legacy marker-only callers; new file operations carry metadata.
			return true
		}
		if !meta.hasSnap || meta.hash == "" {
			// Folder creation has no content hash. Unknown file content,
			// including an mtime-only reply, is never proof of an echo.
			return cat == localCategoryFolder && !meta.inFlight
		}

		info, err := os.Stat(rp.Abs)
		if err != nil {
			return false
		}
		sameAsRemote, ambiguous := false, false
		if meta.size == 0 || info.Size() == meta.size || (meta.inFlight && meta.prevHash != "") {
			if gotHash, ok := s.computeLocalCategoryHash(rp.Abs, cat); ok {
				sameAsRemote = gotHash == meta.hash
				ambiguous = !sameAsRemote && meta.inFlight && meta.prevHash != "" && gotHash == meta.prevHash
			}
		}
		s.mu.Lock()
		cur, current := s.echoWriteMeta[rp.Rel]
		if !current || cur.token != meta.token || cur.inFlight != meta.inFlight {
			s.mu.Unlock()
			// Hashing can overlap another replacement or finalization. Do
			// not classify new bytes using an obsolete operation snapshot.
			continue
		}
		if ambiguous {
			// It may be a late old-version echo OR a genuine undo after
			// replacement. Retain it for recheck after finalization/abort.
			cur.recheck = true
			s.echoWriteMeta[rp.Rel] = cur
			s.mu.Unlock()
			return true
		}
		s.mu.Unlock()
		if !sameAsRemote && !meta.inFlight {
			s.abortRemoteWrite(rp.Rel, meta.token)
		}
		return sameAsRemote
	}
}

func (s *SyncService) isDeleteEchoed(rel string) bool {
	s.mu.Lock()
	_, delEcho := s.lastSyncPathDeleted[rel]
	renMeta, hasRenMeta := s.echoRenameMeta[rel]
	_, renInMap := s.lastSyncPathRenamed[rel]
	renOldEcho := renInMap && hasRenMeta && renMeta.oldRel == rel
	echoed := delEcho || renOldEcho
	s.mu.Unlock()
	return echoed
}

func (s *SyncService) isRenameEchoed(oldRP, newRP resolvedPath, oldCat, newCat localCategory) bool {
	s.mu.Lock()
	_, oldInMap := s.lastSyncPathRenamed[oldRP.Rel]
	_, newInMap := s.lastSyncPathRenamed[newRP.Rel]
	oldMeta, hasOldMeta := s.echoRenameMeta[oldRP.Rel]
	newMeta, hasNewMeta := s.echoRenameMeta[newRP.Rel]
	_, writeInMap := s.lastSyncMtime[newRP.Rel]
	s.mu.Unlock()

	oldEcho := false
	if oldInMap {
		if !hasOldMeta {
			oldEcho = true
		} else if oldMeta.oldRel == oldRP.Rel && oldMeta.newRel == newRP.Rel {
			oldEcho = true
		}
	}
	newEcho := false
	if newInMap {
		if !hasNewMeta {
			newEcho = true
		} else if newMeta.oldRel == oldRP.Rel && newMeta.newRel == newRP.Rel {
			newEcho = true
		}
	}
	remoteWriteEcho := false
	// A destination write marker can explain an internal staging rename,
	// but not a real local move from another synchronized source path.
	if writeInMap && newRP.Rel != "" && oldCat == localCategorySkip {
		remoteWriteEcho = s.isWriteEchoed(newRP, newCat)
	}
	return oldEcho || newEcho || remoteWriteEcho
}

// ShouldWatchDir lets the watcher skip excluded directory trees while keeping
// .obsidian visible for setting-file changes.
func (s *SyncService) ShouldWatchDir(raw string) bool {
	rel := strings.TrimSpace(strings.ReplaceAll(raw, "\\", "/"))
	if rel == "" || rel == "." {
		return true
	}
	clean, err := normalizeSyncPath(rel)
	if err != nil {
		return false
	}
	if isFilesystemJunkPath(clean) {
		return false
	}
	if clean == obsidianConfigDir || strings.HasPrefix(clean, obsidianConfigDir+"/") {
		return true
	}
	if s.cfg.ConfigSyncEnabled {
		for _, dir := range s.cfg.ConfigSyncOtherDirs {
			allowed, err := normalizeSyncPath(dir)
			if err == nil && (literalPathPrefix(clean, allowed) || literalPathPrefix(allowed, clean)) {
				return true
			}
		}
	}
	return s.shouldTraverseVaultDir(clean)
}

func (s *SyncService) HandleLocalModify(ev local.PathEvent) local.Result {
	if !s.localReady() {
		return localSkipped()
	}
	rp, err := s.resolveVaultPath(ev.Path)
	if err != nil {
		return localSkipped()
	}
	cat := s.localCategory(rp.Rel, ev.IsDir)
	if s.isWriteEchoed(rp, cat) {
		return localSkipped()
	}
	if cat == localCategorySkip {
		return localSkipped()
	}

	if ev.IsDir {
		if info, err := os.Stat(rp.Abs); err != nil || !info.IsDir() {
			return localSkipped()
		}
		s.clearDeleteEcho(rp.Rel)
		return localAttempt(s.SendFolderModify(rp.Rel))
	}

	if _, err := os.Stat(rp.Abs); err != nil {
		return localSkipped()
	}
	s.clearDeleteEcho(rp.Rel)
	switch cat {
	case localCategoryNote:
		return localAttempt(s.sendFileContentModify("NoteModify", rp, s.setPendingNoteModify, nil))
	case localCategoryFile:
		return localAttempt(s.SendFileUploadCheck(rp.Rel))
	case localCategorySetting:
		return localAttempt(s.sendFileContentModify("SettingModify", rp, s.setPendingConfigModify, nil))
	default:
		return localSkipped()
	}
}

func (s *SyncService) HandleLocalDelete(ev local.PathEvent) local.Result {
	if !s.localReady() {
		return localSkipped()
	}
	rp, err := s.resolveVaultPath(ev.Path)
	if err != nil {
		return localSkipped()
	}
	if s.isDeleteEchoed(rp.Rel) {
		return localSkipped()
	}
	cat := s.localCategory(rp.Rel, ev.IsDir)
	if cat == localCategorySkip {
		return localSkipped()
	}
	attempted, sendErr := s.sendLocalDeleteForCategory(cat, rp)
	if !attempted {
		return localSkipped()
	}
	return localAttempt(sendErr)
}

func (s *SyncService) HandleLocalRename(ev local.RenameEvent) local.Result {
	if !s.localReady() {
		return localSkipped()
	}
	oldRP, oldErr := s.resolveVaultPath(ev.OldPath)
	newRP, newErr := s.resolveVaultPath(ev.NewPath)
	if oldErr != nil && newErr != nil {
		return localSkipped()
	}
	if oldErr == nil && newErr == nil && oldRP.Rel == newRP.Rel {
		return s.HandleLocalModify(local.PathEvent{Path: newRP.Rel, IsDir: ev.NewIsDir})
	}

	oldCat := localCategorySkip
	newCat := localCategorySkip
	if oldErr == nil {
		oldCat = s.localCategory(oldRP.Rel, ev.OldIsDir)
	}
	if newErr == nil {
		newCat = s.localCategory(newRP.Rel, ev.NewIsDir)
	}
	if s.isRenameEchoed(oldRP, newRP, oldCat, newCat) {
		return localSkipped()
	}

	if oldCat != localCategorySkip {
		missing, statErr := pathConfirmedMissing(oldRP.Abs)
		if statErr != nil {
			log.Printf("[sync] skip local rename old path %q: stat error: %v", oldRP.Rel, statErr)
			oldCat = localCategorySkip
		} else if !missing {
			log.Printf("[sync] local rename old path %q still exists; degrading to modify on new path", oldRP.Rel)
			oldCat = localCategorySkip
		}
	}

	if oldCat == localCategorySkip && newCat == localCategorySkip {
		return localSkipped()
	}
	// We have classified a real local move (not a remote echo). Once it
	// changes the path identity, old remote evidence cannot suppress a later
	// undo/redo even when the same bytes return to the same destination.
	if oldCat != localCategorySkip || newCat != localCategorySkip {
		s.invalidateLocalRenameEchoes(oldRP.Rel, newRP.Rel)
	}
	if oldCat == newCat && oldCat != localCategorySkip {
		switch oldCat {
		case localCategoryNote:
			return localAttempt(s.SendNoteRename(oldRP.Rel, newRP.Rel))
		case localCategoryFile:
			return localAttempt(s.SendFileRename(oldRP.Rel, newRP.Rel))
		case localCategorySetting:
			return s.sendVerifiedSettingRename(oldRP, newRP)
		case localCategoryFolder:
			return localAttempt(s.SendFolderRename(oldRP.Rel, newRP.Rel))
		}
	}

	attempted := false
	if oldCat != localCategorySkip {
		delAttempted, err := s.sendLocalDeleteForCategory(oldCat, oldRP)
		if delAttempted {
			attempted = true
		}
		if err != nil {
			return local.Result{Attempted: delAttempted, Err: err}
		}
	}
	if newCat != localCategorySkip {
		modAttempted, err := s.sendLocalModifyForCategory(newCat, newRP)
		if modAttempted {
			attempted = true
		}
		if err != nil {
			return local.Result{Attempted: attempted, Err: err}
		}
	}
	return local.Result{Attempted: attempted}
}

func (s *SyncService) sendVerifiedSettingRename(oldRP, newRP resolvedPath) local.Result {
	delAttempted, err := s.sendLocalDeleteForCategory(localCategorySetting, oldRP)
	if err != nil {
		return local.Result{Attempted: delAttempted, Err: err}
	}
	modAttempted, err := s.sendLocalModifyForCategory(localCategorySetting, newRP)
	return local.Result{Attempted: delAttempted || modAttempted, Err: err}
}

func (s *SyncService) localCategory(rel string, isDir bool) localCategory {
	if isDir {
		if s.isFolderPathExcluded(rel) || rel == obsidianConfigDir || strings.HasPrefix(rel, obsidianConfigDir+"/") {
			return localCategorySkip
		}
		return localCategoryFolder
	}
	if s.cfg.ConfigSyncEnabled && s.isConfigSyncPathAllowed(rel) {
		return localCategorySetting
	}
	if rel == obsidianConfigDir || strings.HasPrefix(rel, obsidianConfigDir+"/") {
		return localCategorySkip
	}
	if s.isVaultFileExcluded(rel) {
		return localCategorySkip
	}
	if strings.HasSuffix(strings.ToLower(rel), ".md") {
		return localCategoryNote
	}
	return localCategoryFile
}

func (s *SyncService) sendLocalDeleteForCategory(cat localCategory, rp resolvedPath) (bool, error) {
	var action string
	switch cat {
	case localCategoryNote:
		action = "NoteDelete"
	case localCategoryFile:
		action = "FileDelete"
	case localCategorySetting:
		if !s.isConfigSyncPathAllowed(rp.Rel) {
			return false, nil
		}
		action = "SettingDelete"
	case localCategoryFolder:
		action = "FolderDelete"
	default:
		return false, nil
	}

	unlockPre := s.lockPath(rp.Rel)
	if s.isDeleteEchoed(rp.Rel) {
		unlockPre()
		return false, nil
	}
	missing, statErr := pathConfirmedMissing(rp.Abs)
	if statErr != nil {
		unlockPre()
		log.Printf("[sync] skip local delete for %q: stat error: %v", rp.Rel, statErr)
		return false, statErr
	}
	if !missing {
		unlockPre()
		log.Printf("[sync] skip local delete for existing path %q", rp.Rel)
		return false, nil
	}
	s.clearRemoteWrite(rp.Rel)
	if cat == localCategoryFile {
		s.cancelUpload(rp.Rel)
	}
	unlockPre()

	usesSlot := cat != localCategoryFolder
	if usesSlot {
		s.concurrency.WaitForSlot(rp.Rel, false, 0)
	}

	unlockSend := s.lockPath(rp.Rel)
	defer unlockSend()
	if s.isDeleteEchoed(rp.Rel) {
		if usesSlot {
			s.concurrency.ReleaseSlot(rp.Rel)
		}
		return false, nil
	}
	if missing, statErr := pathConfirmedMissing(rp.Abs); statErr != nil || !missing {
		if usesSlot {
			s.concurrency.ReleaseSlot(rp.Rel)
		}
		return false, statErr
	}
	s.clearRemoteWrite(rp.Rel)
	if err := s.Send(action, map[string]interface{}{"vault": s.cfg.Vault, "path": rp.Rel, "pathHash": h.Path(rp.Rel)}); err != nil {
		if usesSlot {
			s.concurrency.ReleaseSlot(rp.Rel)
		}
		return true, err
	}
	s.mu.Lock()
	switch cat {
	case localCategoryNote:
		s.pendingNoteDeleteAcks[rp.Rel] = struct{}{}
	case localCategoryFile:
		s.pendingFileDeleteAcks[rp.Rel] = struct{}{}
	case localCategorySetting:
		s.pendingConfigDeleteAcks[rp.Rel] = struct{}{}
	case localCategoryFolder:
		s.pendingDeleteFolderPaths[rp.Rel] = struct{}{}
	}
	s.mu.Unlock()
	return true, nil
}

func (s *SyncService) sendLocalModifyForCategory(cat localCategory, rp resolvedPath) (bool, error) {
	info, err := os.Stat(rp.Abs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if cat == localCategoryFolder && !info.IsDir() {
		return false, nil
	}
	s.clearDeleteEcho(rp.Rel)
	switch cat {
	case localCategoryNote:
		return true, s.sendFileContentModify("NoteModify", rp, s.setPendingNoteModify, nil)
	case localCategoryFile:
		return true, s.SendFileUploadCheck(rp.Rel)
	case localCategorySetting:
		return true, s.sendFileContentModify("SettingModify", rp, s.setPendingConfigModify, nil)
	case localCategoryFolder:
		return true, s.SendFolderModify(rp.Rel)
	default:
		return false, nil
	}
}

// HandleWatchOverflow responds to a dropped-event burst from the OS watcher.
//
// An overflow means an unknown set of local writes produced no event. Those
// writes are invisible to the incremental path forever: an incremental scan
// skips files whose mtime predates the last sync time, so a file written
// during the overflow window is never re-offered. A full re-scan is the only
// way to find them, so this deliberately ignores the persisted sync
// timestamps. If a sync is already in flight the recovery is queued so the
// overflow cannot be lost, and multiple overflows collapse to one eventual
// full reconciliation.
func (s *SyncService) HandleWatchOverflow() {
	log.Printf("[sync] watcher overflow: requesting full re-scan to recover dropped events")
	s.mu.Lock()
	if s.cfg != nil && s.cfg.ManualSyncEnabled {
		s.mu.Unlock()
		log.Printf("[sync] watcher overflow: manual sync mode, ignoring")
		return
	}
	if s.isSyncing {
		s.overflowRescanPending = true
		s.mu.Unlock()
		log.Printf("[sync] watcher overflow: full re-scan queued until current round finishes")
		return
	}
	s.mu.Unlock()
	go s.handleSync(false)
}
