// Regression coverage for issue #15 using native filesystem events.
package sync

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	stdsync "sync"
	"testing"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/local"
	"github.com/erichll/go-fast-note-sync/internal/watcher"
)

type observedFile struct {
	Exists bool   `json:"exists"`
	SHA256 string `json:"sha256,omitempty"`
	Error  string `json:"error,omitempty"`
}

func readObservedFile(abs string) observedFile {
	b, err := os.ReadFile(abs)
	if err != nil {
		return observedFile{Error: err.Error()}
	}
	return observedFile{Exists: true, SHA256: fmt.Sprintf("%x", sha256.Sum256(b))}
}

type observedOutbound struct {
	Action  string       `json:"action"`
	Path    string       `json:"path"`
	OldPath string       `json:"oldPath,omitempty"`
	NS      int64        `json:"elapsed_ns"`
	Before  observedFile `json:"before"`
	After   observedFile `json:"after"`
}

type watcherProbe struct {
	svc       *SyncService
	fake      *fakeWSConn
	vault     string
	start     time.Time
	mu        stdsync.Mutex
	rows      []observedOutbound
	callbacks map[string]int
	overflow  int
}

// Embed the existing fake so all methods except observed writes are unchanged.
type observedWSConn struct {
	*fakeWSConn
	p *watcherProbe
}

func (c *observedWSConn) WriteMessage(typ int, b []byte) error {
	action, body, _ := strings.Cut(string(b), "|")
	var payload struct {
		Path    string `json:"path"`
		OldPath string `json:"oldPath"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		return err
	}
	p := c.p
	abs := filepath.Join(p.vault, filepath.FromSlash(payload.Path))
	row := observedOutbound{Action: action, Path: payload.Path, OldPath: payload.OldPath, NS: time.Since(p.start).Nanoseconds(), Before: readObservedFile(abs)}
	err := c.fakeWSConn.WriteMessage(typ, b)
	row.After = readObservedFile(abs)
	if err == nil {
		p.mu.Lock()
		p.rows = append(p.rows, row)
		p.mu.Unlock()
	}
	return err
}

func (p *watcherProbe) ShouldWatchDir(path string) bool { return p.svc.ShouldWatchDir(path) }

func (p *watcherProbe) recordCallback(kind string) {
	p.mu.Lock()
	p.callbacks[kind]++
	p.mu.Unlock()
}

func (p *watcherProbe) HandleLocalModify(e local.PathEvent) local.Result {
	p.recordCallback("modify")
	return p.svc.HandleLocalModify(e)
}

func (p *watcherProbe) HandleLocalDelete(e local.PathEvent) local.Result {
	p.recordCallback("delete")
	return p.svc.HandleLocalDelete(e)
}

func (p *watcherProbe) HandleLocalRename(e local.RenameEvent) local.Result {
	p.recordCallback("rename")
	return p.svc.HandleLocalRename(e)
}

func (p *watcherProbe) HandleWatchOverflow() {
	p.mu.Lock()
	p.overflow++
	p.mu.Unlock()
	p.svc.HandleWatchOverflow()
}

func newWatcherIntegrationService(t *testing.T, delay int, initial map[string]string) (*watcherProbe, *watcher.Watcher) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native issue15 matrix currently targets macOS and Linux")
	}
	svc, fake, vault := newLocalEventService(t)
	svc.cfg.ConfigSyncEnabled = true
	svc.cfg.SyncUpdateDelay = delay
	for path, content := range initial {
		writeVaultFile(t, vault, path, content)
	}
	p := &watcherProbe{svc: svc, fake: fake, vault: vault, start: time.Now(), callbacks: map[string]int{}}
	svc.conn = &observedWSConn{fakeWSConn: fake, p: p}
	w, err := watcher.New(vault, delay, p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	return p, w
}

func applyRemoteNoteToWatcher(p *watcherProbe, path, content string) {
	b, _ := json.Marshal(receiveContentMessage{Path: path, Content: content, MTime: time.Now().UnixMilli()})
	handleNoteSyncModify(b, p.svc)
}

func isStressRemotePath(rel string) (bool, bool) {
	base := filepath.Base(rel)
	if !strings.HasPrefix(base, "n") || !strings.HasSuffix(base, ".md") {
		return false, false
	}
	idx, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(base, "n"), ".md"))
	if err != nil {
		return false, false
	}
	return idx%2 == 0, true
}

func (p *watcherProbe) summary(t *testing.T, delay int, scenario string) map[string]int {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	counts := map[string]int{}
	var dmPairs []map[string]interface{}
	var mdPairs []map[string]interface{}
	for i, row := range p.rows {
		counts[row.Action]++
		if row.Action == "NoteDelete" && row.Before.Exists {
			counts["NoteDeleteExisting"]++
			if row.After.Exists && row.After.SHA256 == row.Before.SHA256 {
				counts["NoteDeleteExistingUnchanged"]++
			}
			if isRemote, ok := isStressRemotePath(row.Path); ok {
				if isRemote {
					counts["RemoteNoteDeleteExisting"]++
				} else {
					counts["LocalNoteDeleteExisting"]++
				}
			}
		}
		if row.Action == "NoteDelete" {
			for _, later := range p.rows[i+1:] {
				if later.Path == row.Path && later.Action == "NoteModify" {
					dmPairs = append(dmPairs, map[string]interface{}{"path": row.Path, "gap_ns": later.NS - row.NS})
					break
				}
			}
		}
		if row.Action == "NoteModify" {
			if isRemote, ok := isStressRemotePath(row.Path); ok && isRemote {
				counts["RemoteNoteModify"]++
			}
			for _, later := range p.rows[i+1:] {
				if later.Path == row.Path && later.Action == "NoteDelete" && later.Before.Exists {
					mdPairs = append(mdPairs, map[string]interface{}{"path": row.Path, "gap_ns": later.NS - row.NS})
					break
				}
			}
		}
	}
	counts["DeleteModifyPairs"] = len(dmPairs)
	counts["ModifyDeletePairs"] = len(mdPairs)
	t.Logf("OBSERVATION scenario=%s delay=%d counts=%v callbacks=%v overflow=%d", scenario, delay, counts, p.callbacks, p.overflow)
	p.fake.mu.Lock()
	n := len(p.fake.written)
	p.fake.mu.Unlock()
	if n != len(p.rows) {
		t.Errorf("fake captured %d, observer captured %d", n, len(p.rows))
	}
	if p.overflow > 0 {
		t.Log("OVERFLOW: recovery ran; analyze separately from normal native events")
	}
	return counts
}

func TestWatcherIntegrationEventMatrix(t *testing.T) {
	for _, delay := range []int{500, 2000, 100} {
		for _, scenario := range []string{"remote_modify", "config_delete_existing", "config_delete_noop", "local_atomic", "local_direct", "local_delete"} {
			t.Run(fmt.Sprintf("%d/%s", delay, scenario), func(t *testing.T) {
				initial := map[string]string{"note.md": "initial\n", ".obsidian/keep.json": "{}"}
				if scenario == "config_delete_existing" {
					initial[".obsidian/app.json"] = "{\"old\":true}"
				}
				p, w := newWatcherIntegrationService(t, delay, initial)
				target := filepath.Join(p.vault, "note.md")
				switch scenario {
				case "remote_modify":
					applyRemoteNoteToWatcher(p, "note.md", "remote content\n")
				case "config_delete_existing", "config_delete_noop":
					b, _ := json.Marshal(receivePathMessage{Path: ".obsidian/app.json"})
					handleSettingSyncDelete(b, p.svc)
				case "local_atomic":
					if err := atomicWriteFile(target, []byte("local atomic\n")); err != nil {
						t.Fatal(err)
					}
				case "local_direct":
					if err := os.WriteFile(target, []byte("local direct\n"), 0o644); err != nil {
						t.Fatal(err)
					}
				case "local_delete":
					if err := os.Remove(target); err != nil {
						t.Fatal(err)
					}
				}
				// Deliberate bounded observation window: includes both debounce and echo expiry.
				time.Sleep(time.Duration(delay)*time.Millisecond + 800*time.Millisecond)
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				counts := p.summary(t, delay, scenario)
				if counts["NoteDeleteExisting"] != 0 {
					t.Errorf("false NoteDelete emitted while target exists: counts=%v", counts)
				}
				if counts["DeleteModifyPairs"] != 0 || counts["ModifyDeletePairs"] != 0 {
					t.Errorf("stale delete/modify pair observed: D->M=%d M->D=%d counts=%v", counts["DeleteModifyPairs"], counts["ModifyDeletePairs"], counts)
				}
				switch scenario {
				case "remote_modify":
					if counts["NoteDelete"] != 0 || counts["NoteModify"] != 0 || len(p.rows) != 0 {
						t.Errorf("remote_modify emitted outbound echo messages: counts=%v rows=%+v", counts, p.rows)
					}
					if readObservedFile(target).SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte("remote content\n"))) {
						t.Error("remote handler did not preserve expected content")
					}
				case "config_delete_existing":
					if counts["SettingDelete"] != 0 || len(p.rows) != 0 {
						t.Errorf("config_delete_existing emitted outbound echo messages: counts=%v rows=%+v", counts, p.rows)
					}
					if _, err := os.Stat(filepath.Join(p.vault, ".obsidian/app.json")); !os.IsNotExist(err) {
						t.Errorf(".obsidian/app.json should be deleted, stat err=%v", err)
					}
				case "config_delete_noop":
					if counts["SettingDelete"] != 0 || len(p.rows) != 0 || p.callbacks["delete"] != 0 {
						t.Errorf("config_delete_noop expected 0 callbacks and 0 outbound messages: counts=%v callbacks=%v", counts, p.callbacks)
					}
				case "local_atomic":
					if counts["NoteModify"] == 0 || counts["NoteDelete"] != 0 || counts["NoteRename"] != 0 {
						t.Errorf("local_atomic expected only NoteModify: counts=%v", counts)
					}
				case "local_direct":
					if counts["NoteModify"] == 0 || counts["NoteDelete"] != 0 {
						t.Errorf("local_direct expected only NoteModify: counts=%v", counts)
					}
				case "local_delete":
					if counts["NoteDelete"] == 0 || counts["NoteModify"] != 0 {
						t.Errorf("local_delete expected NoteDelete only: counts=%v", counts)
					}
				}
			})
		}
	}
}

func TestWatcherIntegrationConcurrentAtomicWrites(t *testing.T) {
	for _, delay := range []int{500, 2000, 100} {
		for _, level := range []struct{ files, workers, loops int }{{8, 2, 3}, {64, 8, 3}, {256, 16, 3}} {
			t.Run(fmt.Sprintf("%d/files%d_workers%d_loops%d", delay, level.files, level.workers, level.loops), func(t *testing.T) {
				initial := map[string]string{}
				for i := 0; i < level.files; i++ {
					initial[fmt.Sprintf("notes/n%04d.md", i)] = "initial\n"
				}
				p, w := newWatcherIntegrationService(t, delay, initial)
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				jobs := make(chan int, level.files)
				for i := 0; i < level.files; i++ {
					jobs <- i
				}
				close(jobs)
				var wg stdsync.WaitGroup
				errs := make(chan error, level.workers)
				for worker := 0; worker < level.workers; worker++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := range jobs {
							for loop := 0; loop < level.loops; loop++ {
								if ctx.Err() != nil {
									errs <- ctx.Err()
									return
								}
								path := fmt.Sprintf("notes/n%04d.md", i)
								content := fmt.Sprintf("file=%d loop=%d remote=%t\n", i, loop, i%2 == 0)
								if i%2 == 0 {
									applyRemoteNoteToWatcher(p, path, content)
								} else if err := atomicWriteFile(filepath.Join(p.vault, path), []byte(content)); err != nil {
									errs <- err
									return
								}
							}
						}
					}()
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					t.Error(err)
				}
				time.Sleep(time.Duration(delay)*time.Millisecond + 800*time.Millisecond)
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < level.files; i++ {
					path := fmt.Sprintf("notes/n%04d.md", i)
					want := fmt.Sprintf("file=%d loop=%d remote=%t\n", i, level.loops-1, i%2 == 0)
					got := readObservedFile(filepath.Join(p.vault, path))
					if got.SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(want))) {
						t.Errorf("final content mismatch %s: %+v", path, got)
					}
				}
				counts := p.summary(t, delay, "stress")
				if counts["NoteDelete"] != 0 || counts["NoteDeleteExisting"] != 0 || counts["RemoteNoteDeleteExisting"] != 0 || counts["LocalNoteDeleteExisting"] != 0 {
					t.Errorf("stress emitted false NoteDelete: total=%d existing=%d remoteExisting=%d localExisting=%d",
						counts["NoteDelete"], counts["NoteDeleteExisting"], counts["RemoteNoteDeleteExisting"], counts["LocalNoteDeleteExisting"])
				}
				if counts["RemoteNoteModify"] != 0 {
					t.Errorf("stress emitted remote NoteModify echo: RemoteNoteModify=%d counts=%v", counts["RemoteNoteModify"], counts)
				}
				if counts["DeleteModifyPairs"] != 0 || counts["ModifyDeletePairs"] != 0 {
					t.Errorf("stress emitted delete/modify pairs: D->M=%d M->D=%d", counts["DeleteModifyPairs"], counts["ModifyDeletePairs"])
				}
			})
		}
	}
}

// MINIMAL CONTROL: passes svc directly to watcher.New and keeps stock fakeWSConn
// with no watcherProbe/observedWSConn wrappers, eliminating callback/write sampling overhead.
func TestWatcherIntegrationDirectHandlerMatrix(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native issue15 matrix currently targets macOS and Linux")
	}
	for _, delay := range []int{500, 2000, 100} {
		for _, scenario := range []string{"remote_modify", "config_delete_existing", "config_delete_noop", "local_atomic", "local_direct", "local_delete"} {
			t.Run(fmt.Sprintf("%d/%s", delay, scenario), func(t *testing.T) {
				svc, fake, vault := newLocalEventService(t)
				svc.cfg.ConfigSyncEnabled = true
				svc.cfg.SyncUpdateDelay = delay
				writeVaultFile(t, vault, "note.md", "initial\n")
				writeVaultFile(t, vault, ".obsidian/keep.json", "{}")
				if scenario == "config_delete_existing" {
					writeVaultFile(t, vault, ".obsidian/app.json", "{\"old\":true}")
				}
				w, err := watcher.New(vault, delay, svc)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := w.Close(); err != nil {
						t.Error(err)
					}
				})
				target := filepath.Join(vault, "note.md")
				switch scenario {
				case "remote_modify":
					b, _ := json.Marshal(receiveContentMessage{Path: "note.md", Content: "remote content\n", MTime: time.Now().UnixMilli()})
					handleNoteSyncModify(b, svc)
				case "config_delete_existing", "config_delete_noop":
					b, _ := json.Marshal(receivePathMessage{Path: ".obsidian/app.json"})
					handleSettingSyncDelete(b, svc)
				case "local_atomic":
					if err := atomicWriteFile(target, []byte("local atomic\n")); err != nil {
						t.Fatal(err)
					}
				case "local_direct":
					if err := os.WriteFile(target, []byte("local direct\n"), 0o644); err != nil {
						t.Fatal(err)
					}
				case "local_delete":
					if err := os.Remove(target); err != nil {
						t.Fatal(err)
					}
				}
				time.Sleep(time.Duration(delay)*time.Millisecond + 800*time.Millisecond)
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				fake.mu.Lock()
				written := append([]string(nil), fake.written...)
				fake.mu.Unlock()
				var actions []string
				for _, msg := range written {
					act, _, _ := strings.Cut(msg, "|")
					actions = append(actions, act)
				}
				_, statErr := os.Stat(target)
				t.Logf("MINIMAL_CONTROL scenario=%s delay=%d actions=%v note_exists=%t", scenario, delay, actions, statErr == nil)
				switch scenario {
				case "remote_modify", "config_delete_existing", "config_delete_noop":
					if len(actions) != 0 {
						t.Errorf("scenario %s expected 0 actions, got %v", scenario, actions)
					}
				case "local_atomic", "local_direct":
					if len(actions) == 0 {
						t.Errorf("scenario %s expected NoteModify, got none", scenario)
					}
					for _, act := range actions {
						if act != "NoteModify" {
							t.Errorf("scenario %s emitted unexpected action %s in %v", scenario, act, actions)
						}
					}
				case "local_delete":
					if len(actions) != 1 || actions[0] != "NoteDelete" {
						t.Errorf("scenario local_delete expected [NoteDelete], got %v", actions)
					}
				}
			})
		}
	}
}

func TestWatcherIntegrationLocalChangesAfterRemoteWrite(t *testing.T) {
	t.Run("immediate_local_edit_not_swallowed_native", func(t *testing.T) {
		p, w := newWatcherIntegrationService(t, 200, map[string]string{"note.md": "initial\n"})
		target := filepath.Join(p.vault, "note.md")
		applyRemoteNoteToWatcher(p, "note.md", "remote content v1\n")
		// Immediately edit locally while still inside the echo suppression window.
		if err := atomicWriteFile(target, []byte("user local edit v2\n")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		counts := p.summary(t, 200, "immediate_local_edit")
		if counts["NoteModify"] == 0 || counts["NoteDelete"] != 0 {
			t.Fatalf("expected immediate local edit after remote write to emit NoteModify without NoteDelete, got counts=%v", counts)
		}
		p.fake.mu.Lock()
		lastMsg := p.fake.written[len(p.fake.written)-1]
		p.fake.mu.Unlock()
		if !strings.Contains(lastMsg, "user local edit v2") {
			t.Fatalf("expected outbound NoteModify to carry user local edit v2, got %s", lastMsg)
		}
	})

	t.Run("immediate_local_delete_not_swallowed_native", func(t *testing.T) {
		p, w := newWatcherIntegrationService(t, 200, map[string]string{"note.md": "initial\n"})
		target := filepath.Join(p.vault, "note.md")
		applyRemoteNoteToWatcher(p, "note.md", "remote content v1\n")
		// Immediately delete locally while lastSyncMtime is still active.
		if err := os.Remove(target); err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		counts := p.summary(t, 200, "immediate_local_delete")
		if counts["NoteDelete"] != 1 {
			t.Fatalf("expected immediate local delete after remote write to emit 1 NoteDelete, got counts=%v", counts)
		}
	})

	t.Run("immediate_local_rename_not_swallowed_native_and_direct", func(t *testing.T) {
		p, w := newWatcherIntegrationService(t, 200, map[string]string{"note.md": "initial\n"})
		oldTarget := filepath.Join(p.vault, "note.md")
		newTarget := filepath.Join(p.vault, "renamed.md")
		applyRemoteNoteToWatcher(p, "note.md", "remote content v1\n")
		if err := os.Rename(oldTarget, newTarget); err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		counts := p.summary(t, 200, "immediate_local_rename")
		// On macOS kqueue, os.Rename to a new filename may deliver CREATE(new) before RENAME(old)
		// (emitting NoteDelete(old)+NoteModify(new)) or RENAME(old) before CREATE(new) (emitting NoteRename).
		// In neither case may the remote write echo window swallow the local rename.
		pairedRename := counts["NoteRename"] == 1 && counts["NoteDelete"] == 0
		splitRename := counts["NoteDelete"] == 1 && counts["NoteModify"] == 1 && counts["NoteDeleteExisting"] == 0
		if !pairedRename && !splitRename {
			t.Fatalf("expected immediate local rename after remote write to sync (NoteRename or NoteDelete+NoteModify), got counts=%v", counts)
		}

		// Also verify direct HandleLocalRename when RENAME->CREATE pairs while remote write echo is active on oldPath.
		svc, conn, vault := newLocalEventService(t)
		writeVaultFile(t, vault, "note.md", "initial\n")
		b, _ := json.Marshal(receiveContentMessage{Path: "note.md", Content: "remote content v1\n", MTime: time.Now().UnixMilli()})
		handleNoteSyncModify(b, svc)
		if err := os.Rename(filepath.Join(vault, "note.md"), filepath.Join(vault, "renamed.md")); err != nil {
			t.Fatal(err)
		}
		res := svc.HandleLocalRename(local.RenameEvent{OldPath: "note.md", NewPath: "renamed.md"})
		if !res.Attempted || res.Err != nil || firstAction(t, conn) != "NoteRename" {
			t.Fatalf("expected direct HandleLocalRename after remote write to emit NoteRename, got res=%+v writes=%v", res, conn.written)
		}
	})
}

func TestWatcherIntegrationRenamesAndStagingIsolation(t *testing.T) {
	t.Run("same_and_cross_category_and_excluded_renames", func(t *testing.T) {
		p, w := newWatcherIntegrationService(t, 150, map[string]string{
			"same_old.md":    "same\n",
			"cross_old.md":   "cross\n",
			"excluded/ex.md": "from_excluded\n",
			"to_exclude.md":  "to_excluded\n",
		})
		if err := os.Rename(filepath.Join(p.vault, "same_old.md"), filepath.Join(p.vault, "same_new.md")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(350 * time.Millisecond)
		if err := os.Rename(filepath.Join(p.vault, "cross_old.md"), filepath.Join(p.vault, "cross_new.bin")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(350 * time.Millisecond)
		if err := os.Rename(filepath.Join(p.vault, "excluded/ex.md"), filepath.Join(p.vault, "promoted.md")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(350 * time.Millisecond)
		if err := os.Rename(filepath.Join(p.vault, "to_exclude.md"), filepath.Join(p.vault, "excluded/demoted.md")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(400 * time.Millisecond)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		counts := p.summary(t, 150, "renames")
		if counts["FileUploadCheck"] != 1 {
			t.Errorf("expected 1 FileUploadCheck (cross_new.bin), got %v", counts)
		}
		if counts["NoteDeleteExisting"] != 0 {
			t.Errorf("expected 0 NoteDeleteExisting, got %v", counts)
		}
		// same_old->same_new emits either NoteRename(1) or NoteDelete(same_old)+NoteModify(same_new).
		// cross_old->cross_new.bin emits NoteDelete(cross_old)+FileUploadCheck(cross_new.bin).
		// excluded/ex.md->promoted.md emits NoteModify(promoted.md).
		// to_exclude.md->excluded/demoted.md emits NoteDelete(to_exclude.md).
		samePaired := counts["NoteRename"] == 1 && counts["NoteModify"] == 1 && counts["NoteDelete"] == 2
		sameSplit := counts["NoteRename"] == 0 && counts["NoteModify"] == 2 && counts["NoteDelete"] == 3
		if !samePaired && !sameSplit {
			t.Errorf("unexpected rename outbound counts: %v", counts)
		}

		// Deterministic verification of HandleLocalRename for excluded<->normal and cross-category paths.
		svc, conn, vault := newLocalEventService(t)
		writeVaultFile(t, vault, "promoted.md", "from_excluded\n")
		if res := svc.HandleLocalRename(local.RenameEvent{OldPath: "excluded/ex.md", NewPath: "promoted.md"}); !res.Attempted || res.Err != nil {
			t.Fatalf("excluded->normal HandleLocalRename failed: %+v", res)
		}
		writeVaultFile(t, vault, "excluded/demoted.md", "to_excluded\n")
		if res := svc.HandleLocalRename(local.RenameEvent{OldPath: "to_exclude.md", NewPath: "excluded/demoted.md"}); !res.Attempted || res.Err != nil {
			t.Fatalf("normal->excluded HandleLocalRename failed: %+v", res)
		}
		if len(conn.written) != 2 || !strings.HasPrefix(conn.written[0], "NoteModify|") || !strings.HasPrefix(conn.written[1], "NoteDelete|") {
			t.Fatalf("expected NoteModify(promoted.md) then NoteDelete(to_exclude.md), got %v", conn.written)
		}
	})

	t.Run("concurrent_tmp_staging_does_not_mis_pair", func(t *testing.T) {
		p, w := newWatcherIntegrationService(t, 150, map[string]string{"a.md": "initial a\n"})
		var wg stdsync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				_ = atomicWriteFile(filepath.Join(p.vault, "a.md"), []byte(fmt.Sprintf("atomic a %d\n", i)))
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				_ = os.WriteFile(filepath.Join(p.vault, fmt.Sprintf("other_%d.md", i)), []byte("other\n"), 0o644)
			}
		}()
		wg.Wait()
		time.Sleep(450 * time.Millisecond)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		counts := p.summary(t, 150, "concurrent_tmp_staging")
		if counts["NoteRename"] != 0 || counts["NoteDelete"] != 0 {
			t.Fatalf("concurrent .tmp-* staging caused false rename or delete: counts=%v", counts)
		}
	})
}
