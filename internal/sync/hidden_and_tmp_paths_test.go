package sync

import (
	"testing"

	"github.com/erichll/go-fast-note-sync/internal/config"
)

// Obsidian desktop saves notes via atomic write, transiently creating
// "Note.md.tmp.<PID>.<hex>" before renaming; a watcher or scan can catch the
// temp file mid-flight and upload it as content. The official plugin's users
// add a ".*\.tmp(\.|$)" sync exclusion for exactly this — mirror it so no
// per-device configuration is needed.
func TestIsVaultFileExcluded_AtomicTempFiles(t *testing.T) {
	s := newTestService(nil, nil, "")
	for _, rel := range []string{
		"Note.md.tmp.9428.206d0d60351e",
		"People/Pi.eyyj.md.tmp.9428.206d0d60351e",
		"notes.tmp",
		"Deep/Dir/scratch.tmp",
		"Note.MD.TMP.1.AB",
	} {
		if !s.isVaultFileExcluded(rel) {
			t.Errorf("expected atomic-write temp path to be excluded: %q", rel)
		}
	}
	for _, rel := range []string{
		"Note.md",
		"Notes/tmp.md",
		"Notes/atmp.md",
		"Notes/my.tmpnotes.md",
	} {
		if s.isVaultFileExcluded(rel) {
			t.Errorf("expected legitimate path to sync: %q", rel)
		}
	}
}

// Obsidian never indexes dot-files or dot-directories, so the official plugin
// never synced them (.git, .trash, Syncthing's .stfolder, plugin dot-dirs like
// .makemd). A CLI client walking the real filesystem must skip them too, or it
// creates files the plugin can never delete. ".obsidian" is exempt: when
// config sync is enabled it is owned by the dedicated config-path logic.
func TestIsVaultFileExcluded_DotSegments(t *testing.T) {
	s := newTestService(nil, nil, "")
	for _, rel := range []string{
		".git/config",
		".trash/old note.md",
		"Projects/repo/.git/objects/ab/cdef",
		".stfolder",
		"Syncthing/.stfolder/markers/x",
		".shards",
		".stignore",
		"Notes/.DS_Store_notes.md",
	} {
		if !s.isVaultFileExcluded(rel) {
			t.Errorf("expected dot-segment path to be excluded: %q", rel)
		}
	}
	if s.isVaultFileExcluded(".obsidian/app.json") {
		t.Error(".obsidian must be exempt from the dot-segment skip")
	}
	if s.isVaultFileExcluded("Notes/plain.md") {
		t.Error("plain path should still sync")
	}
}

// sync_exclude_folders rules match any path segment, mirroring the official
// plugin's ".*(^|/)name(?=/|$)" regex semantics: users write "_temp" or
// "__pycache__" expecting nested matches.
func TestIsFolderPathExcluded_SegmentMatch(t *testing.T) {
	cfg := &config.Config{SyncExcludeFolders: []string{"_temp", "__pycache__", ".git"}}
	s := newTestService(cfg, nil, "")
	for _, rel := range []string{
		"_temp",
		"_temp/x.md",
		"Projects/repo/_temp",
		"Projects/repo/_temp/x.md",
		"a/b/__pycache__",
		"Projects/repo/.git",
		"Projects/repo/.git/objects",
	} {
		if !s.isFolderPathExcluded(rel) {
			t.Errorf("expected folder rule segment match: %q", rel)
		}
	}
	for _, rel := range []string{
		"temp",
		"Projects/repo/temp",
		"Projects/repo/_tempx",
		"Projects/repo/_temp.md",
	} {
		if s.isFolderPathExcluded(rel) {
			t.Errorf("expected lookalike folder to sync: %q", rel)
		}
	}
}

// The whitelist still wins over dot-segment and folder rules (junk stays
// excluded regardless).
func TestIsVaultFileExcluded_WhitelistBeatsDotSegment(t *testing.T) {
	cfg := &config.Config{
		SyncExcludeFolders:   []string{"_temp"},
		SyncExcludeWhitelist: []string{"Keep"},
	}
	s := newTestService(cfg, nil, "")
	if s.isVaultFileExcluded("Keep/.gitignore") {
		t.Error("whitelisted dot-file should sync")
	}
	if s.isVaultFileExcluded("Keep/.git/config") {
		t.Error("whitelisted dot-dir content should sync")
	}
	if !s.isVaultFileExcluded("Other/.git/config") {
		t.Error("non-whitelisted dot-dir content must stay excluded")
	}
}
