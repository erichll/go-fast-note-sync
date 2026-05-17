package local

// PathEvent describes one vault-relative local filesystem path.
// Path must be slash-separated and relative to the vault root.
type PathEvent struct {
	Path  string
	IsDir bool
}

// RenameEvent describes a local rename with directory hints for both sides.
// Deleted or renamed-away paths cannot reliably be classified after fsnotify
// emits the event, so watcher supplies the best hint it has.
type RenameEvent struct {
	OldPath  string
	NewPath  string
	OldIsDir bool
	NewIsDir bool
}

// Result reports whether the sync layer selected an outbound send.
// Attempted=false with Err=nil means the event was skipped by policy, scope,
// echo suppression, readiness, or filesystem race handling.
type Result struct {
	Attempted bool
	Err       error
}

// Handler is the narrow contract used by the watcher package.
type Handler interface {
	ShouldWatchDir(rel string) bool
	HandleLocalModify(PathEvent) Result
	HandleLocalDelete(PathEvent) Result
	HandleLocalRename(RenameEvent) Result
}
