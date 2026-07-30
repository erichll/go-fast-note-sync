package sync

import (
	"testing"

	"github.com/erichll/go-fast-note-sync/internal/config"
)

// AppleDouble sidecars (._Foo) and .DS_Store are macOS filesystem artifacts.
// They carry no vault content, and the Obsidian plugin has never tracked them,
// so a client that syncs them creates files the plugin can never remove.
func TestIsVaultFileExcluded_AppleDouble(t *testing.T) {
	s := newTestService(nil, nil, "")
	for _, rel := range []string{
		"._Note.md",
		"People/._Steve.md",
		"Attachments/._image.png",
		"a/b/c/._deep",
	} {
		if !s.isVaultFileExcluded(rel) {
			t.Errorf("expected AppleDouble path to be excluded: %q", rel)
		}
	}
}

func TestIsVaultFileExcluded_DSStore(t *testing.T) {
	s := newTestService(nil, nil, "")
	for _, rel := range []string{
		".DS_Store",
		"People/.DS_Store",
		"People/.ds_store",
	} {
		if !s.isVaultFileExcluded(rel) {
			t.Errorf("expected .DS_Store path to be excluded: %q", rel)
		}
	}
}

// A user whitelist must not be able to resurrect filesystem junk.
func TestIsVaultFileExcluded_WhitelistDoesNotResurrectJunk(t *testing.T) {
	cfg := &config.Config{SyncExcludeWhitelist: []string{"Attachments"}}
	s := newTestService(cfg, nil, "")
	if !s.isVaultFileExcluded("Attachments/._image.png") {
		t.Error("whitelist must not un-exclude AppleDouble files")
	}
	if !s.isVaultFileExcluded("Attachments/.DS_Store") {
		t.Error("whitelist must not un-exclude .DS_Store")
	}
	if s.isVaultFileExcluded("Attachments/image.png") {
		t.Error("whitelisted real file should still sync")
	}
}

// Names that merely resemble junk must keep syncing.
func TestIsVaultFileExcluded_LookalikesStillSync(t *testing.T) {
	s := newTestService(nil, nil, "")
	for _, rel := range []string{
		"Templates/_Template.md",
		"Notes/my._notes.md",
		"Notes/.DS_Store_notes.md",
		"Notes/DS_Store.md",
		"_Inbox/Note.md",
	} {
		if s.isVaultFileExcluded(rel) {
			t.Errorf("expected legitimate path to sync: %q", rel)
		}
	}
}

// AppleDouble files ending in .md are the dangerous case: without a junk rule
// they are classified as notes and their binary payload is sent as note text.
func TestLocalCategory_JunkIsSkipped(t *testing.T) {
	s := newTestService(nil, nil, "")
	for _, rel := range []string{
		"._Note.md",
		"People/._Steve.md",
		".DS_Store",
		"People/.DS_Store",
	} {
		if got := s.localCategory(rel, false); got != localCategorySkip {
			t.Errorf("localCategory(%q) = %v, want localCategorySkip", rel, got)
		}
	}
	if got := s.localCategory("People/Steve.md", false); got != localCategoryNote {
		t.Errorf("localCategory of a real note = %v, want localCategoryNote", got)
	}
}
