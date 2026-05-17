package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
	return &cobra.Command{
		Use:   "status",
		Short: "Show sync status",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("not implemented")
		},
	}
}

func newSyncCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sync",
		Short: "Trigger a manual sync",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("not implemented")
		},
	}
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
