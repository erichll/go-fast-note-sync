package watcher

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/erichll/go-fast-note-sync/internal/local"
)

type fakeBackend struct {
	events  chan fsnotify.Event
	errors  chan error
	mu      sync.Mutex
	added   []string
	removed []string
	closed  bool
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		events: make(chan fsnotify.Event, 32),
		errors: make(chan error, 1),
	}
}

func (f *fakeBackend) Add(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.added = append(f.added, filepath.Clean(name))
	return nil
}

func (f *fakeBackend) Remove(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, filepath.Clean(name))
	return nil
}

func (f *fakeBackend) Close() error {
	f.mu.Lock()
	if !f.closed {
		f.closed = true
		close(f.events)
		close(f.errors)
	}
	f.mu.Unlock()
	return nil
}

func (f *fakeBackend) Events() <-chan fsnotify.Event {
	return f.events
}

func (f *fakeBackend) Errors() <-chan error {
	return f.errors
}

type recordingHandler struct {
	skipPrefix string
	events     chan string
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{skipPrefix: "excluded", events: make(chan string, 32)}
}

func (h *recordingHandler) ShouldWatchDir(rel string) bool {
	if rel == ".obsidian" || strings.HasPrefix(rel, ".obsidian/") {
		return true
	}
	return rel != h.skipPrefix && !strings.HasPrefix(rel, h.skipPrefix+"/")
}

func (h *recordingHandler) HandleLocalModify(ev local.PathEvent) local.Result {
	h.events <- "modify:" + ev.Path + dirSuffix(ev.IsDir)
	return local.Result{Attempted: true}
}

func (h *recordingHandler) HandleLocalDelete(ev local.PathEvent) local.Result {
	h.events <- "delete:" + ev.Path + dirSuffix(ev.IsDir)
	return local.Result{Attempted: true}
}

func (h *recordingHandler) HandleLocalRename(ev local.RenameEvent) local.Result {
	h.events <- "rename:" + ev.OldPath + "->" + ev.NewPath + dirSuffix(ev.OldIsDir || ev.NewIsDir)
	return local.Result{Attempted: true}
}

func dirSuffix(isDir bool) string {
	if isDir {
		return ":dir"
	}
	return ":file"
}

func waitEvent(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for watcher event")
		return ""
	}
}

func assertNoEvent(t *testing.T, ch <-chan string) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Fatalf("unexpected event: %s", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestInitialRecursiveWatchSkipsExcludedAndKeepsObsidian(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{"notes/nested", "excluded/child", ".obsidian/plugins/demo"} {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(rel)), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
	}
	fb := newFakeBackend()
	w, err := newWithBackend(root, 5, newRecordingHandler(), fb)
	if err != nil {
		t.Fatalf("new watcher: %v", err)
	}
	defer w.Close()

	var rels []string
	for _, abs := range fb.added {
		rel, _ := filepath.Rel(root, abs)
		rels = append(rels, filepath.ToSlash(rel))
	}
	for _, want := range []string{".", "notes", "notes/nested", ".obsidian", ".obsidian/plugins", ".obsidian/plugins/demo"} {
		if !slices.Contains(rels, want) {
			t.Fatalf("watched dirs = %#v, missing %s", rels, want)
		}
	}
	for _, got := range rels {
		if strings.HasPrefix(got, "excluded") {
			t.Fatalf("excluded directory was watched: %#v", rels)
		}
	}
}

func TestDebounceCollapsesModifyAndSkipsChmodOnly(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.md")
	if err := os.WriteFile(path, []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	fb := newFakeBackend()
	h := newRecordingHandler()
	w, err := newWithBackend(root, 10, h, fb)
	if err != nil {
		t.Fatalf("new watcher: %v", err)
	}
	defer w.Close()

	fb.events <- fsnotify.Event{Name: path, Op: fsnotify.Create}
	fb.events <- fsnotify.Event{Name: path, Op: fsnotify.Write}
	fb.events <- fsnotify.Event{Name: path, Op: fsnotify.Chmod}
	if ev := waitEvent(t, h.events); ev != "modify:a.md:file" {
		t.Fatalf("event = %s", ev)
	}
	assertNoEvent(t, h.events)
}

func TestCreatedDirectoryAddsRecursiveWatches(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "newdir")
	child := filepath.Join(dir, "child")
	if err := os.MkdirAll(child, 0o755); err != nil {
		t.Fatal(err)
	}
	fb := newFakeBackend()
	h := newRecordingHandler()
	w, err := newWithBackend(root, 5, h, fb)
	if err != nil {
		t.Fatalf("new watcher: %v", err)
	}
	defer w.Close()

	fb.events <- fsnotify.Event{Name: dir, Op: fsnotify.Create}
	if ev := waitEvent(t, h.events); ev != "modify:newdir:dir" {
		t.Fatalf("event = %s", ev)
	}
	var rels []string
	for _, abs := range fb.added {
		rel, _ := filepath.Rel(root, abs)
		rels = append(rels, filepath.ToSlash(rel))
	}
	if !slices.Contains(rels, "newdir") || !slices.Contains(rels, "newdir/child") {
		t.Fatalf("added dirs = %#v, want newdir and child", rels)
	}
}

func TestRenamePairAndFallback(t *testing.T) {
	t.Run("pair", func(t *testing.T) {
		root := t.TempDir()
		oldPath := filepath.Join(root, "old.md")
		newPath := filepath.Join(root, "new.md")
		if err := os.WriteFile(newPath, []byte("new"), 0o644); err != nil {
			t.Fatal(err)
		}
		fb := newFakeBackend()
		h := newRecordingHandler()
		w, err := newWithBackend(root, 20, h, fb)
		if err != nil {
			t.Fatalf("new watcher: %v", err)
		}
		defer w.Close()

		fb.events <- fsnotify.Event{Name: oldPath, Op: fsnotify.Rename}
		fb.events <- fsnotify.Event{Name: newPath, Op: fsnotify.Create}
		if ev := waitEvent(t, h.events); ev != "rename:old.md->new.md:file" {
			t.Fatalf("event = %s", ev)
		}
	})
	t.Run("fallback delete", func(t *testing.T) {
		root := t.TempDir()
		oldPath := filepath.Join(root, "old.md")
		fb := newFakeBackend()
		h := newRecordingHandler()
		w, err := newWithBackend(root, 10, h, fb)
		if err != nil {
			t.Fatalf("new watcher: %v", err)
		}
		defer w.Close()

		fb.events <- fsnotify.Event{Name: oldPath, Op: fsnotify.Rename}
		if ev := waitEvent(t, h.events); ev != "delete:old.md:file" {
			t.Fatalf("event = %s", ev)
		}
	})
}

func TestCloseCancelsPendingTimers(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.md")
	if err := os.WriteFile(path, []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	fb := newFakeBackend()
	h := newRecordingHandler()
	w, err := newWithBackend(root, 100, h, fb)
	if err != nil {
		t.Fatalf("new watcher: %v", err)
	}
	fb.events <- fsnotify.Event{Name: path, Op: fsnotify.Write}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	assertNoEvent(t, h.events)
}
