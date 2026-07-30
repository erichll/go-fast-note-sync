package watcher

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/fsnotify/fsnotify"
)

// When the OS event queue overflows, events are lost silently. inotify reports
// IN_Q_OVERFLOW and ReadDirectoryChangesW reports a buffer overflow; fsnotify
// surfaces both as ErrEventOverflow on the Errors channel. The only correct
// response is a full re-scan, because the dropped events are unrecoverable.
func TestEventOverflowRequestsFullRescan(t *testing.T) {
	root := t.TempDir()
	be := newFakeBackend()
	h := newRecordingHandler()

	w, err := newWithBackend(root, 5, h, be)
	if err != nil {
		t.Fatalf("newWithBackend: %v", err)
	}
	defer func() { _ = w.Close() }()

	be.errors <- fsnotify.ErrEventOverflow

	if got := waitEvent(t, h.events); got != "overflow" {
		t.Fatalf("got %q, want %q", got, "overflow")
	}
}

// A wrapped overflow error must still be recognised.
func TestWrappedEventOverflowRequestsFullRescan(t *testing.T) {
	root := t.TempDir()
	be := newFakeBackend()
	h := newRecordingHandler()

	w, err := newWithBackend(root, 5, h, be)
	if err != nil {
		t.Fatalf("newWithBackend: %v", err)
	}
	defer func() { _ = w.Close() }()

	be.errors <- errors.New("watching " + root + ": " + fsnotify.ErrEventOverflow.Error())
	assertNoEvent(t, h.events)

	be.errors <- errors.Join(errors.New("watch failed"), fsnotify.ErrEventOverflow)
	if got := waitEvent(t, h.events); got != "overflow" {
		t.Fatalf("got %q, want %q", got, "overflow")
	}
}

// Ordinary watcher errors must not trigger a vault-wide re-scan.
func TestNonOverflowErrorDoesNotRescan(t *testing.T) {
	root := t.TempDir()
	be := newFakeBackend()
	h := newRecordingHandler()

	w, err := newWithBackend(root, 5, h, be)
	if err != nil {
		t.Fatalf("newWithBackend: %v", err)
	}
	defer func() { _ = w.Close() }()

	be.errors <- errors.New("permission denied")
	assertNoEvent(t, h.events)
}

// Overflow recovery must re-register recursive watches for directories that
// appeared while events were dropped.
func TestEventOverflowReconcilesMissingDirectoryWatches(t *testing.T) {
	root := t.TempDir()
	be := newFakeBackend()
	h := newRecordingHandler()

	w, err := newWithBackend(root, 5, h, be)
	if err != nil {
		t.Fatalf("newWithBackend: %v", err)
	}
	defer func() { _ = w.Close() }()

	missed := filepath.Join(root, "missed", "nested")
	if err := os.MkdirAll(missed, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	be.errors <- fsnotify.ErrEventOverflow
	if got := waitEvent(t, h.events); got != "overflow" {
		t.Fatalf("got %q, want %q", got, "overflow")
	}

	var rels []string
	for _, abs := range be.added {
		rel, _ := filepath.Rel(root, abs)
		rels = append(rels, filepath.ToSlash(rel))
	}
	for _, want := range []string{"missed", "missed/nested"} {
		if !slices.Contains(rels, want) {
			t.Fatalf("watched dirs = %#v, missing %s after overflow reconcile", rels, want)
		}
	}
}
