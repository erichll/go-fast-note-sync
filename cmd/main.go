package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/erichll/go-fast-note-sync/internal/config"
	"github.com/erichll/go-fast-note-sync/internal/local"
	"github.com/erichll/go-fast-note-sync/internal/state"
	syncsvc "github.com/erichll/go-fast-note-sync/internal/sync"
	"github.com/erichll/go-fast-note-sync/internal/watcher"
	"github.com/spf13/cobra"
)

const cliVersion = "0.1.0-dev"

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "go-fast-note-sync",
		Short: "Fast Note sync daemon",
	}
	root.AddCommand(
		newStartCmd(),
		newStatusCmd(),
		newSyncCmd(),
		newInitConfigCmd(),
	)
	return root
}

// waitForSignal blocks until a termination signal arrives. Overridden in tests.
var waitForSignal = func() os.Signal {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	return <-sigCh
}

type syncDaemon interface {
	local.Handler
	Connect()
	SyncComplete() <-chan struct{}
}

type localWatcher interface {
	Close() error
}

var newSyncDaemon = func(cfg *config.Config, st *state.State, statePath, version string) syncDaemon {
	return syncsvc.NewSyncService(cfg, st, statePath, version)
}

var newLocalWatcher = func(vaultPath string, syncUpdateDelay int, handler local.Handler) (localWatcher, error) {
	return watcher.New(vaultPath, syncUpdateDelay, handler)
}

func newStartCmd() *cobra.Command {
	var cfgPath string
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the sync daemon (foreground)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				cfgPath = config.DefaultPath()
			}
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			statePath := cfg.StateFile
			if statePath == "" {
				statePath = state.DefaultPath()
			}
			st, err := state.Load(statePath)
			if err != nil {
				return fmt.Errorf("load state: %w", err)
			}
			svc := newSyncDaemon(cfg, st, statePath, cliVersion)
			svc.Connect()
			var w localWatcher
			if cfg.SyncEnabled && cfg.VaultPath != "" {
				w, err = newLocalWatcher(cfg.VaultPath, cfg.SyncUpdateDelay, svc)
				if err != nil {
					return fmt.Errorf("start watcher: %w", err)
				}
			}
			sig := waitForSignal()
			if w != nil {
				if err := w.Close(); err != nil {
					return fmt.Errorf("stop watcher after %s: %w", sig, err)
				}
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "received %s, exiting\n", sig)
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "path to config file (default: ~/.config/go-fast-note-sync/config.yaml)")
	return cmd
}

func newStatusCmd() *cobra.Command {
	var cfgPath string
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show sync status (offline)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				cfgPath = config.DefaultPath()
			}
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			statePath := cfg.StateFile
			if statePath == "" {
				statePath = state.DefaultPath()
			}
			st, err := state.Load(statePath)
			if err != nil {
				return fmt.Errorf("load state: %w", err)
			}

			fmtTime := func(ms int64) string {
				if ms == 0 {
					return "never"
				}
				return time.UnixMilli(ms).UTC().Format(time.RFC3339)
			}

			noteCount := 0
			fileCount := 0
			for path := range st.FileHashMap {
				if strings.HasSuffix(strings.ToLower(path), ".md") {
					noteCount++
				} else {
					fileCount++
				}
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "vault: %s\n", cfg.Vault)
			fmt.Fprintf(out, "api: %s\n", cfg.API)
			fmt.Fprintf(out, "sync_enabled: %v\n", cfg.SyncEnabled)
			fmt.Fprintf(out, "config_sync_enabled: %v\n", cfg.ConfigSyncEnabled)
			fmt.Fprintf(out, "readonly_sync_enabled: %v\n", cfg.ReadOnlySyncEnabled)
			fmt.Fprintf(out, "manual_sync_enabled: %v\n", cfg.ManualSyncEnabled)
			fmt.Fprintf(out, "note_sync_time: %s\n", fmtTime(st.NoteSyncTime))
			fmt.Fprintf(out, "file_sync_time: %s\n", fmtTime(st.FileSyncTime))
			fmt.Fprintf(out, "config_sync_time: %s\n", fmtTime(st.ConfigSyncTime))
			fmt.Fprintf(out, "folder_sync_time: %s\n", fmtTime(st.FolderSyncTime))
			fmt.Fprintf(out, "note_cache: %d\n", noteCount)
			fmt.Fprintf(out, "file_cache: %d\n", fileCount)
			fmt.Fprintf(out, "setting_cache: %d\n", len(st.ConfigHashMap))
			fmt.Fprintf(out, "folder_cache: %d\n", len(st.FolderSnapshot))
			fmt.Fprintf(out, "ws_count: %d\n", st.WsCount)
			fmt.Fprintf(out, "is_init_sync: %v\n", st.IsInitSync)
			fmt.Fprintf(out, "pending_note_modifies: %d\n", len(st.PendingNoteModifies))
			fmt.Fprintf(out, "pending_upload_hashes: %d\n", len(st.PendingUploadHashes))
			fmt.Fprintf(out, "pending_config_modifies: %d\n", len(st.PendingConfigModifies))
			fmt.Fprintf(out, "upload_checkpoints: %d\n", len(st.UploadCheckpoints))
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "path to config file (default: ~/.config/go-fast-note-sync/config.yaml)")
	return cmd
}

const defaultSyncTimeout = 60 * time.Second

func newSyncCmd() *cobra.Command {
	var cfgPath string
	var timeoutFlag time.Duration
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Trigger a one-shot sync and exit",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				cfgPath = config.DefaultPath()
			}
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			statePath := cfg.StateFile
			if statePath == "" {
				statePath = state.DefaultPath()
			}
			st, err := state.Load(statePath)
			if err != nil {
				return fmt.Errorf("load state: %w", err)
			}
			cfg.ManualSyncEnabled = false
			svc := newSyncDaemon(cfg, st, statePath, cliVersion)
			svc.Connect()
			ctx, cancel := context.WithTimeout(cmd.Context(), timeoutFlag)
			defer cancel()
			select {
			case <-svc.SyncComplete():
				fmt.Fprintln(cmd.OutOrStdout(), "Sync complete.")
				return nil
			case <-ctx.Done():
				return fmt.Errorf("sync timed out after %v", timeoutFlag)
			}
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "path to config file (default: ~/.config/go-fast-note-sync/config.yaml)")
	cmd.Flags().DurationVar(&timeoutFlag, "timeout", defaultSyncTimeout, "maximum time to wait for sync completion")
	return cmd
}

func newInitConfigCmd() *cobra.Command {
	var cfgPath string
	cmd := &cobra.Command{
		Use:   "init-config",
		Short: "Generate default configuration file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				cfgPath = config.DefaultPath()
			}
			if err := config.WriteDefault(cfgPath); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Config written to %s\n", cfgPath)
			return nil
		},
	}
	cmd.Flags().StringVar(&cfgPath, "config", "", "path to write config (default: ~/.config/go-fast-note-sync/config.yaml)")
	return cmd
}
