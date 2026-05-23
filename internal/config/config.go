package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/viper"
	"go.yaml.in/yaml/v3"
)

// DefaultClientType is the default client identifier sent to the server via the
// WebSocket URL `client=` parameter and the ClientInfo `type` field.
const DefaultClientType = "GoFastNoteSync"

type Config struct {
	API      string `yaml:"api"       mapstructure:"api"`
	APIToken string `yaml:"api_token"  mapstructure:"api_token"`

	Vault     string `yaml:"vault"      mapstructure:"vault"`
	VaultPath string `yaml:"vault_path" mapstructure:"vault_path"`

	// ClientType identifies the client to the server. Defaults to DefaultClientType
	// ("GoFastNoteSync"). Override if the server token scope restricts `c:<value>`.
	ClientType string `yaml:"client_type" mapstructure:"client_type"`

	SyncEnabled       bool `yaml:"sync_enabled"        mapstructure:"sync_enabled"`
	ConfigSyncEnabled bool `yaml:"config_sync_enabled" mapstructure:"config_sync_enabled"`

	OfflineDeleteSyncEnabled bool   `yaml:"offline_delete_sync_enabled" mapstructure:"offline_delete_sync_enabled"`
	ReadOnlySyncEnabled      bool   `yaml:"readonly_sync_enabled"       mapstructure:"readonly_sync_enabled"`
	ManualSyncEnabled        bool   `yaml:"manual_sync_enabled"         mapstructure:"manual_sync_enabled"`
	OfflineSyncStrategy      string `yaml:"offline_sync_strategy"       mapstructure:"offline_sync_strategy"`
	SyncUpdateDelay          int    `yaml:"sync_update_delay"           mapstructure:"sync_update_delay"`
	BinarySyncLimitEnabled   bool   `yaml:"binary_sync_limit_enabled"   mapstructure:"binary_sync_limit_enabled"`

	ConcurrencyControlEnabled bool `yaml:"concurrency_control_enabled" mapstructure:"concurrency_control_enabled"`
	MaxConcurrentUploads      int  `yaml:"max_concurrent_uploads"      mapstructure:"max_concurrent_uploads"`

	SyncExcludeFolders    []string `yaml:"sync_exclude_folders"     mapstructure:"sync_exclude_folders"`
	SyncExcludeExtensions []string `yaml:"sync_exclude_extensions"  mapstructure:"sync_exclude_extensions"`
	SyncExcludeWhitelist  []string `yaml:"sync_exclude_whitelist"   mapstructure:"sync_exclude_whitelist"`
	ConfigSyncOtherDirs   []string `yaml:"config_sync_other_dirs"   mapstructure:"config_sync_other_dirs"`

	StartupDelay        int    `yaml:"startup_delay"         mapstructure:"startup_delay"`
	AutoRedirectEnabled bool   `yaml:"auto_redirect_enabled" mapstructure:"auto_redirect_enabled"`
	StateFile           string `yaml:"state_file"            mapstructure:"state_file"`

	// SyncTimeoutSeconds bounds how long a sync round waits for all
	// *SyncEnd handlers + acks before force-completing. Defaults to 60s in
	// the daemon (`internal/sync.runCheckSyncCompletion`); set higher when
	// the vault is large enough that a fresh client cannot consume the
	// server's initial-sync payload within the default window. 0 keeps the
	// 60s default.
	SyncTimeoutSeconds int `yaml:"sync_timeout_seconds" mapstructure:"sync_timeout_seconds"`
}

func Default() *Config {
	return &Config{
		API:                       "",
		APIToken:                  "",
		Vault:                     "",
		VaultPath:                 "",
		ClientType:                DefaultClientType,
		SyncEnabled:               true,
		ConfigSyncEnabled:         true,
		OfflineDeleteSyncEnabled:  false,
		ReadOnlySyncEnabled:       false,
		ManualSyncEnabled:         false,
		OfflineSyncStrategy:       "auto",
		SyncUpdateDelay:           500,
		BinarySyncLimitEnabled:    true,
		ConcurrencyControlEnabled: true,
		MaxConcurrentUploads:      3,
		SyncExcludeFolders:        []string{},
		SyncExcludeExtensions:     []string{},
		SyncExcludeWhitelist:      []string{},
		ConfigSyncOtherDirs:       []string{},
		StartupDelay:              0,
		AutoRedirectEnabled:       true,
		StateFile:                 "",
		SyncTimeoutSeconds:        0,
	}
}

func DefaultPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "config.yaml"
	}
	return filepath.Join(home, ".config", "go-fast-note-sync", "config.yaml")
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	expanded, err := expandEnv(string(raw))
	if err != nil {
		return nil, fmt.Errorf("expand env in %s: %w", path, err)
	}

	v := viper.New()
	v.SetConfigType("yaml")
	setDefaults(v)

	if err := v.ReadConfig(bytes.NewReader([]byte(expanded))); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	return &cfg, nil
}

// envRefRegexp matches ${VAR} references where VAR is a POSIX-shell-like identifier.
// Only the braced form is expanded; bare $VAR is left alone to avoid surprises with
// tokens or URLs that legitimately contain '$'.
var envRefRegexp = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv replaces every ${VAR} occurrence with os.Getenv(VAR).
// Returns an error listing any referenced variables that are unset, so typos
// like ${SYNC_TONK} fail loudly rather than silently producing an empty value.
func expandEnv(s string) (string, error) {
	var missing []string
	seen := make(map[string]struct{})
	out := envRefRegexp.ReplaceAllStringFunc(s, func(match string) string {
		name := match[2 : len(match)-1]
		val, ok := os.LookupEnv(name)
		if !ok {
			if _, dup := seen[name]; !dup {
				seen[name] = struct{}{}
				missing = append(missing, name)
			}
			return match
		}
		return val
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("undefined env var(s): %s", strings.Join(missing, ", "))
	}
	return out, nil
}

func WriteDefault(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create config file: %w", err)
	}
	defer f.Close()

	enc := yaml.NewEncoder(f)
	enc.SetIndent(2)
	if err := enc.Encode(Default()); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return enc.Close()
}

func setDefaults(v *viper.Viper) {
	d := Default()
	v.SetDefault("client_type", d.ClientType)
	v.SetDefault("sync_enabled", d.SyncEnabled)
	v.SetDefault("config_sync_enabled", d.ConfigSyncEnabled)
	v.SetDefault("offline_delete_sync_enabled", d.OfflineDeleteSyncEnabled)
	v.SetDefault("readonly_sync_enabled", d.ReadOnlySyncEnabled)
	v.SetDefault("manual_sync_enabled", d.ManualSyncEnabled)
	v.SetDefault("offline_sync_strategy", d.OfflineSyncStrategy)
	v.SetDefault("sync_update_delay", d.SyncUpdateDelay)
	v.SetDefault("binary_sync_limit_enabled", d.BinarySyncLimitEnabled)
	v.SetDefault("concurrency_control_enabled", d.ConcurrencyControlEnabled)
	v.SetDefault("max_concurrent_uploads", d.MaxConcurrentUploads)
	v.SetDefault("startup_delay", d.StartupDelay)
	v.SetDefault("auto_redirect_enabled", d.AutoRedirectEnabled)
	v.SetDefault("sync_timeout_seconds", d.SyncTimeoutSeconds)
}
