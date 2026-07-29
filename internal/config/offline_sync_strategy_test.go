package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The sync service selects its conflict-handling behaviour from the strategy the
// client reports in ClientInfo, and it recognizes exactly these values. An
// unrecognized value is not rejected by the server -- it simply matches none of
// the branches, which silently disables conflict detection and lets the client
// overwrite server content. So the client must never send one.
var serverRecognizedStrategies = []string{"", "manualMerge", "ignoreTimeMerge", "newTimeMerge"}

func TestDefaultOfflineSyncStrategyIsServerRecognized(t *testing.T) {
	got := Default().OfflineSyncStrategy
	for _, want := range serverRecognizedStrategies {
		if got == want {
			return
		}
	}
	t.Fatalf("default offline_sync_strategy %q is not recognized by the sync service; want one of %v",
		got, serverRecognizedStrategies)
}

func TestLoadAcceptsEveryServerRecognizedStrategy(t *testing.T) {
	for _, strategy := range serverRecognizedStrategies {
		t.Run("strategy="+strategy, func(t *testing.T) {
			path := writeStrategyConfig(t, strategy)
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load(%q): %v", strategy, err)
			}
			if cfg.OfflineSyncStrategy != strategy {
				t.Errorf("got %q, want %q", cfg.OfflineSyncStrategy, strategy)
			}
		})
	}
}

func TestLoadRejectsUnrecognizedOfflineSyncStrategy(t *testing.T) {
	path := writeStrategyConfig(t, "auto")

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected Load to reject an offline_sync_strategy the server does not recognize")
	}
	// The message has to name the setting and the accepted values, otherwise the
	// operator has no way to tell what went wrong.
	if !strings.Contains(err.Error(), "offline_sync_strategy") {
		t.Errorf("error should name the setting, got: %v", err)
	}
	for _, want := range []string{"manualMerge", "ignoreTimeMerge", "newTimeMerge"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should list %q as an accepted value, got: %v", want, err)
		}
	}
}

func writeStrategyConfig(t *testing.T, strategy string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "api: \"http://test.example.com\"\noffline_sync_strategy: \"" + strategy + "\"\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
