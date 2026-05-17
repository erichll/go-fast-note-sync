package sync

import (
	"errors"
	"os"
	"strings"

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
	if clean == obsidianConfigDir || strings.HasPrefix(clean, obsidianConfigDir+"/") {
		return true
	}
	return !s.isFolderPathExcluded(clean)
}

func (s *SyncService) HandleLocalModify(ev local.PathEvent) local.Result {
	if !s.localReady() {
		return localSkipped()
	}
	rp, err := s.resolveVaultPath(ev.Path)
	if err != nil {
		return localSkipped()
	}
	s.mu.Lock()
	_, echoed := s.lastSyncMtime[rp.Rel]
	s.mu.Unlock()
	if echoed {
		return localSkipped()
	}

	if ev.IsDir {
		if s.localCategory(rp.Rel, true) != localCategoryFolder {
			return localSkipped()
		}
		return localAttempt(s.SendFolderModify(rp.Rel))
	}

	if _, err := os.Stat(rp.Abs); err != nil {
		return localSkipped()
	}
	switch s.localCategory(rp.Rel, false) {
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
	s.mu.Lock()
	_, echoed := s.lastSyncPathDeleted[rp.Rel]
	s.mu.Unlock()
	if echoed {
		return localSkipped()
	}

	switch s.localCategory(rp.Rel, ev.IsDir) {
	case localCategoryNote:
		return localAttempt(s.SendNoteDelete(rp.Rel))
	case localCategoryFile:
		return localAttempt(s.SendFileDelete(rp.Rel))
	case localCategorySetting:
		return localAttempt(s.SendSettingDelete(rp.Rel))
	case localCategoryFolder:
		return localAttempt(s.SendFolderDelete(rp.Rel))
	default:
		return localSkipped()
	}
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
	s.mu.Lock()
	_, oldEcho := s.lastSyncPathRenamed[oldRP.Rel]
	_, newEcho := s.lastSyncPathRenamed[newRP.Rel]
	s.mu.Unlock()
	if oldEcho || newEcho {
		return localSkipped()
	}

	oldCat := localCategorySkip
	newCat := localCategorySkip
	if oldErr == nil {
		oldCat = s.localCategory(oldRP.Rel, ev.OldIsDir)
	}
	if newErr == nil {
		newCat = s.localCategory(newRP.Rel, ev.NewIsDir)
	}

	if oldCat == localCategorySkip && newCat == localCategorySkip {
		return localSkipped()
	}
	if oldCat == newCat && oldCat != localCategorySkip {
		switch oldCat {
		case localCategoryNote:
			return localAttempt(s.SendNoteRename(oldRP.Rel, newRP.Rel))
		case localCategoryFile:
			return localAttempt(s.SendFileRename(oldRP.Rel, newRP.Rel))
		case localCategorySetting:
			return localAttempt(s.SendSettingRename(oldRP.Rel, newRP.Rel))
		case localCategoryFolder:
			return localAttempt(s.SendFolderRename(oldRP.Rel, newRP.Rel))
		}
	}

	attempted := false
	if oldCat != localCategorySkip {
		attempted = true
		if err := s.sendLocalDeleteForCategory(oldCat, oldRP.Rel); err != nil {
			return localAttempt(err)
		}
	}
	if newCat != localCategorySkip {
		attempted = true
		if err := s.sendLocalModifyForCategory(newCat, newRP); err != nil {
			return localAttempt(err)
		}
	}
	return local.Result{Attempted: attempted}
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

func (s *SyncService) sendLocalDeleteForCategory(cat localCategory, rel string) error {
	switch cat {
	case localCategoryNote:
		return s.SendNoteDelete(rel)
	case localCategoryFile:
		return s.SendFileDelete(rel)
	case localCategorySetting:
		return s.SendSettingDelete(rel)
	case localCategoryFolder:
		return s.SendFolderDelete(rel)
	default:
		return nil
	}
}

func (s *SyncService) sendLocalModifyForCategory(cat localCategory, rp resolvedPath) error {
	if cat != localCategoryFolder {
		if _, err := os.Stat(rp.Abs); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
	}
	switch cat {
	case localCategoryNote:
		return s.sendFileContentModify("NoteModify", rp, s.setPendingNoteModify, nil)
	case localCategoryFile:
		return s.SendFileUploadCheck(rp.Rel)
	case localCategorySetting:
		return s.sendFileContentModify("SettingModify", rp, s.setPendingConfigModify, nil)
	case localCategoryFolder:
		return s.SendFolderModify(rp.Rel)
	default:
		return nil
	}
}
