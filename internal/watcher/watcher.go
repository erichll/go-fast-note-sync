package watcher

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/local"
)

type backend interface {
	Add(string) error
	Remove(string) error
	Close() error
	Events() <-chan fsnotify.Event
	Errors() <-chan error
}

type fsnotifyBackend struct {
	w *fsnotify.Watcher
}

func newFSNotifyBackend() (backend, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return &fsnotifyBackend{w: w}, nil
}

func (b *fsnotifyBackend) Add(name string) error {
	return b.w.Add(name)
}

func (b *fsnotifyBackend) Remove(name string) error {
	return b.w.Remove(name)
}

func (b *fsnotifyBackend) Close() error {
	return b.w.Close()
}

func (b *fsnotifyBackend) Events() <-chan fsnotify.Event {
	return b.w.Events
}

func (b *fsnotifyBackend) Errors() <-chan error {
	return b.w.Errors
}

type scheduled struct {
	timer *time.Timer
	done  sync.Once
}

type pendingRename struct {
	oldRel string
	oldAbs string
	isDir  bool
	timer  *time.Timer
	done   sync.Once
}

// Watcher owns fsnotify lifecycle and emits debounced local events.
type Watcher struct {
	root    string
	delay   time.Duration
	handler local.Handler
	backend backend

	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
	wg     sync.WaitGroup

	mu            sync.Mutex
	watchedDirs   map[string]string
	timers        map[string]*scheduled
	pendingRename *pendingRename
	closed        bool
}

// New creates and starts a production fsnotify watcher for vaultPath.
func New(vaultPath string, syncUpdateDelay int, handler local.Handler) (*Watcher, error) {
	b, err := newFSNotifyBackend()
	if err != nil {
		return nil, err
	}
	w, err := newWithBackend(vaultPath, syncUpdateDelay, handler, b)
	if err != nil {
		_ = b.Close()
		return nil, err
	}
	return w, nil
}

func newWithBackend(vaultPath string, syncUpdateDelay int, handler local.Handler, b backend) (*Watcher, error) {
	delay := time.Duration(syncUpdateDelay) * time.Millisecond
	if delay <= 0 {
		delay = time.Duration(config.Default().SyncUpdateDelay) * time.Millisecond
	}
	root, err := filepath.Abs(vaultPath)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &Watcher{
		root:        root,
		delay:       delay,
		handler:     handler,
		backend:     b,
		ctx:         ctx,
		cancel:      cancel,
		watchedDirs: make(map[string]string),
		timers:      make(map[string]*scheduled),
	}
	if err := w.addRecursive(root); err != nil {
		cancel()
		return nil, err
	}
	w.wg.Add(1)
	go w.loop()
	return w, nil
}

// Close stops event processing, cancels pending timers, and closes fsnotify.
func (w *Watcher) Close() error {
	var err error
	w.once.Do(func() {
		w.mu.Lock()
		w.closed = true
		for key, s := range w.timers {
			if s.timer.Stop() {
				s.done.Do(w.wg.Done)
			}
			delete(w.timers, key)
		}
		if w.pendingRename != nil {
			if w.pendingRename.timer.Stop() {
				w.pendingRename.done.Do(w.wg.Done)
			}
			w.pendingRename = nil
		}
		w.mu.Unlock()
		w.cancel()
		err = w.backend.Close()
		w.wg.Wait()
	})
	return err
}

func (w *Watcher) loop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case ev, ok := <-w.backend.Events():
			if !ok {
				return
			}
			w.handleEvent(ev)
		case err, ok := <-w.backend.Errors():
			if !ok {
				return
			}
			if err != nil {
				log.Printf("[watcher] fsnotify: %v", err)
			}
		}
	}
}

func (w *Watcher) handleEvent(ev fsnotify.Event) {
	if ev.Name == "" {
		return
	}
	rel, err := w.rel(ev.Name)
	if err != nil || rel == "." {
		return
	}
	if ev.Op&fsnotify.Chmod != 0 && ev.Op&^(fsnotify.Chmod) == 0 {
		return
	}
	if ev.Op&fsnotify.Rename != 0 {
		w.handleRenameAway(ev.Name, rel)
		return
	}
	if ev.Op&fsnotify.Remove != 0 {
		isDir := w.removeKnownDir(ev.Name)
		w.schedulePath("delete", rel, isDir, func() {
			w.handler.HandleLocalDelete(local.PathEvent{Path: rel, IsDir: isDir})
		})
		return
	}
	if ev.Op&fsnotify.Create != 0 {
		isDir := w.statIsDir(ev.Name)
		if isDir {
			if err := w.addRecursive(ev.Name); err != nil {
				log.Printf("[watcher] add created dir %q: %v", rel, err)
			}
		}
		if w.pairRename(ev.Name, rel, isDir) {
			return
		}
		w.schedulePath("modify", rel, isDir, func() {
			w.handler.HandleLocalModify(local.PathEvent{Path: rel, IsDir: isDir})
		})
		return
	}
	if ev.Op&fsnotify.Write != 0 {
		isDir := w.statIsDir(ev.Name)
		w.schedulePath("modify", rel, isDir, func() {
			w.handler.HandleLocalModify(local.PathEvent{Path: rel, IsDir: isDir})
		})
	}
}

func (w *Watcher) handleRenameAway(abs, rel string) {
	isDir := w.removeKnownDir(abs)
	w.mu.Lock()
	if w.pendingRename != nil {
		w.flushPendingRenameLocked()
	}
	pr := &pendingRename{oldRel: rel, oldAbs: abs, isDir: isDir}
	w.wg.Add(1)
	pr.timer = time.AfterFunc(w.delay, func() {
		defer pr.done.Do(w.wg.Done)
		w.mu.Lock()
		if w.pendingRename != pr || w.closed {
			w.mu.Unlock()
			return
		}
		w.pendingRename = nil
		w.mu.Unlock()
		w.handler.HandleLocalDelete(local.PathEvent{Path: rel, IsDir: isDir})
	})
	w.pendingRename = pr
	w.mu.Unlock()
}

func (w *Watcher) pairRename(_ string, newRel string, newIsDir bool) bool {
	w.mu.Lock()
	pr := w.pendingRename
	if pr == nil {
		w.mu.Unlock()
		return false
	}
	w.pendingRename = nil
	if pr.timer.Stop() {
		pr.done.Do(w.wg.Done)
	}
	oldRel := pr.oldRel
	oldIsDir := pr.isDir
	w.mu.Unlock()

	w.schedulePath("rename", oldRel+"->"+newRel, oldIsDir || newIsDir, func() {
		w.handler.HandleLocalRename(local.RenameEvent{
			OldPath:  oldRel,
			NewPath:  newRel,
			OldIsDir: oldIsDir,
			NewIsDir: newIsDir,
		})
	})
	return true
}

func (w *Watcher) flushPendingRenameLocked() {
	pr := w.pendingRename
	if pr == nil {
		return
	}
	w.pendingRename = nil
	if pr.timer.Stop() {
		pr.done.Do(w.wg.Done)
	}
	oldRel := pr.oldRel
	isDir := pr.isDir
	w.schedulePathLocked("delete", oldRel, isDir, func() {
		w.handler.HandleLocalDelete(local.PathEvent{Path: oldRel, IsDir: isDir})
	})
}

func (w *Watcher) schedulePath(kind, rel string, isDir bool, fn func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.schedulePathLocked(kind, rel, isDir, fn)
}

func (w *Watcher) schedulePathLocked(kind, rel string, _ bool, fn func()) {
	if w.closed {
		return
	}
	key := kind + ":" + rel
	if old := w.timers[key]; old != nil {
		if old.timer.Stop() {
			old.done.Do(w.wg.Done)
		}
	}
	s := &scheduled{}
	w.wg.Add(1)
	s.timer = time.AfterFunc(w.delay, func() {
		defer s.done.Do(w.wg.Done)
		w.mu.Lock()
		if w.timers[key] != s || w.closed {
			w.mu.Unlock()
			return
		}
		delete(w.timers, key)
		w.mu.Unlock()
		fn()
	})
	w.timers[key] = s
}

func (w *Watcher) addRecursive(root string) error {
	return filepath.WalkDir(root, func(abs string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			log.Printf("[watcher] walk %q: %v", abs, walkErr)
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		rel, err := w.rel(abs)
		if err != nil {
			return nil
		}
		if rel != "." && !w.handler.ShouldWatchDir(rel) {
			return filepath.SkipDir
		}
		if err := w.backend.Add(abs); err != nil {
			if abs == w.root {
				return err
			}
			log.Printf("[watcher] add %q: %v", rel, err)
			return filepath.SkipDir
		}
		w.mu.Lock()
		w.watchedDirs[abs] = rel
		w.mu.Unlock()
		return nil
	})
}

func (w *Watcher) removeKnownDir(abs string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.watchedDirs[abs]
	if !ok {
		return false
	}
	for dir := range w.watchedDirs {
		if dir == abs || strings.HasPrefix(dir, abs+string(filepath.Separator)) {
			_ = w.backend.Remove(dir)
			delete(w.watchedDirs, dir)
		}
	}
	return true
}

func (w *Watcher) statIsDir(abs string) bool {
	info, err := os.Stat(abs)
	return err == nil && info.IsDir()
}

func (w *Watcher) rel(abs string) (string, error) {
	abs, err := filepath.Abs(abs)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(w.root, abs)
	if err != nil {
		return "", err
	}
	if rel == "." {
		return ".", nil
	}
	if strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", os.ErrInvalid
	}
	return filepath.ToSlash(rel), nil
}
