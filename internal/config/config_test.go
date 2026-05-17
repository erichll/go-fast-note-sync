package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefault(t *testing.T) {
	d := Default()
	if d.OfflineSyncStrategy != "auto" {
		t.Errorf("expected offline_sync_strategy=auto, got %q", d.OfflineSyncStrategy)
	}
	if d.SyncUpdateDelay != 500 {
		t.Errorf("expected sync_update_delay=500, got %d", d.SyncUpdateDelay)
	}
	if !d.SyncEnabled {
		t.Error("expected sync_enabled=true")
	}
	if !d.ConcurrencyControlEnabled {
		t.Error("expected concurrency_control_enabled=true")
	}
	if d.MaxConcurrentUploads != 3 {
		t.Errorf("expected max_concurrent_uploads=3, got %d", d.MaxConcurrentUploads)
	}
}

func TestDefaultPath(t *testing.T) {
	p := DefaultPath()
	if p == "" {
		t.Error("DefaultPath should not be empty")
	}
	if filepath.Base(p) != "config.yaml" {
		t.Errorf("expected config.yaml, got %q", filepath.Base(p))
	}
}

func TestWriteDefaultAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "config.yaml")

	if err := WriteDefault(path); err != nil {
		t.Fatalf("WriteDefault: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("config file not created: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.OfflineSyncStrategy != "auto" {
		t.Errorf("expected auto, got %q", cfg.OfflineSyncStrategy)
	}
	if cfg.SyncUpdateDelay != 500 {
		t.Errorf("expected 500, got %d", cfg.SyncUpdateDelay)
	}
	if !cfg.SyncEnabled {
		t.Error("expected sync_enabled=true after roundtrip")
	}
}

func TestWriteDefaultConflict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	if err := WriteDefault(path); err != nil {
		t.Fatalf("first WriteDefault: %v", err)
	}
	if err := WriteDefault(path); err == nil {
		t.Error("expected error when file already exists")
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error loading nonexistent file")
	}
}

func TestLoadWithCustomValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `api: "https://example.com"
api_token: "tok123"
vault: "MyVault"
vault_path: "/home/user/vault"
sync_enabled: false
max_concurrent_uploads: 5
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.API != "https://example.com" {
		t.Errorf("api: got %q", cfg.API)
	}
	if cfg.APIToken != "tok123" {
		t.Errorf("api_token: got %q", cfg.APIToken)
	}
	if cfg.SyncEnabled {
		t.Error("sync_enabled should be false")
	}
	if cfg.MaxConcurrentUploads != 5 {
		t.Errorf("max_concurrent_uploads: got %d", cfg.MaxConcurrentUploads)
	}
	// defaults still applied for unset fields
	if cfg.OfflineSyncStrategy != "auto" {
		t.Errorf("offline_sync_strategy default: got %q", cfg.OfflineSyncStrategy)
	}
}

// ---- env var expansion ----

func TestExpandEnv_NoVars(t *testing.T) {
	out, err := expandEnv("plain text with no vars")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "plain text with no vars" {
		t.Errorf("got %q", out)
	}
}

func TestExpandEnv_SingleVar(t *testing.T) {
	t.Setenv("SMOKE_TEST_VAL", "hello")
	out, err := expandEnv("v: ${SMOKE_TEST_VAL}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "v: hello" {
		t.Errorf("got %q", out)
	}
}

func TestExpandEnv_MultipleVars(t *testing.T) {
	t.Setenv("SMOKE_A", "alpha")
	t.Setenv("SMOKE_B", "beta")
	out, err := expandEnv("a=${SMOKE_A} b=${SMOKE_B} a2=${SMOKE_A}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "a=alpha b=beta a2=alpha" {
		t.Errorf("got %q", out)
	}
}

func TestExpandEnv_MissingVar(t *testing.T) {
	os.Unsetenv("SMOKE_MISSING_X")
	_, err := expandEnv("v: ${SMOKE_MISSING_X}")
	if err == nil {
		t.Fatal("expected error for undefined env var")
	}
	if !strings.Contains(err.Error(), "SMOKE_MISSING_X") {
		t.Errorf("error should mention missing var, got: %v", err)
	}
}

func TestExpandEnv_MissingVarsDeduped(t *testing.T) {
	os.Unsetenv("SMOKE_MISSING_DUP")
	_, err := expandEnv("a=${SMOKE_MISSING_DUP} b=${SMOKE_MISSING_DUP}")
	if err == nil {
		t.Fatal("expected error")
	}
	// Should appear once in the error, not twice.
	if strings.Count(err.Error(), "SMOKE_MISSING_DUP") != 1 {
		t.Errorf("missing var should be reported once, got: %v", err)
	}
}

func TestExpandEnv_EmptyVarAccepted(t *testing.T) {
	// Defined-but-empty is NOT missing; ${EMPTY} expands to "".
	t.Setenv("SMOKE_EMPTY", "")
	out, err := expandEnv("v: '${SMOKE_EMPTY}'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "v: ''" {
		t.Errorf("got %q", out)
	}
}

func TestExpandEnv_BareDollarLeftAlone(t *testing.T) {
	// Only ${VAR} is expanded; bare $FOO stays literal so tokens/URLs with '$'
	// don't get clobbered.
	t.Setenv("SMOKE_BARE", "should-not-substitute")
	out, err := expandEnv("token: 'abc$SMOKE_BARE'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "token: 'abc$SMOKE_BARE'" {
		t.Errorf("bare $VAR should be left alone, got %q", out)
	}
}

func TestLoadWithEnvVarSubstitution(t *testing.T) {
	t.Setenv("SMOKE_API", "https://envserver.example.com")
	t.Setenv("SMOKE_TOKEN", "env-secret-123")

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `api: "${SMOKE_API}"
api_token: "${SMOKE_TOKEN}"
vault: "EnvVault"
vault_path: "/tmp/vault"
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.API != "https://envserver.example.com" {
		t.Errorf("API not expanded, got %q", cfg.API)
	}
	if cfg.APIToken != "env-secret-123" {
		t.Errorf("APIToken not expanded, got %q", cfg.APIToken)
	}
	if cfg.Vault != "EnvVault" {
		t.Errorf("Vault literal not preserved, got %q", cfg.Vault)
	}
}

func TestLoadMissingEnvVar(t *testing.T) {
	os.Unsetenv("SMOKE_LOAD_MISSING")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `api: "${SMOKE_LOAD_MISSING}"`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when referenced env var is unset")
	}
	if !strings.Contains(err.Error(), "SMOKE_LOAD_MISSING") {
		t.Errorf("error should name the missing var, got: %v", err)
	}
}
