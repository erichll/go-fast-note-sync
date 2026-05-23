# go-fast-note-sync

> **English** | [中文](README.zh-CN.md)

A Linux/headless Go CLI daemon that syncs a local [Obsidian](https://obsidian.md) vault with a self-hosted [fast-note-sync-service](https://github.com/haierkeys/fast-note-sync-service) instance over WebSocket — no desktop, no GUI required.

## Features

- **Bidirectional sync** — notes, attachments, settings, and folders
- **Live file watching** — incremental push on local change via fsnotify, with debounce and echo suppression
- **Binary file chunked transfer** — resumable multi-chunk upload/download with checkpoints
- **Startup sync** — full vault reconciliation on connect, then incremental deltas
- **Automatic reconnect** — on-failure reconnect with configurable restart
- **systemd-ready** — ships a user-service unit file for unattended background operation
- **Config via YAML** — environment variable expansion (`${VAR}`) supported
- **Persistent state** — atomic JSON state file; crash-safe pending hashes and upload checkpoints

## Requirements

- Go 1.24.5+
- A reachable `fast-note-sync-service` instance with a valid API token
- Linux (primary target); other POSIX systems may work but are untested

## Installation

### From source

```bash
git clone https://github.com/erichll/go-fast-note-sync.git
cd go-fast-note-sync
go build -o ~/.local/bin/go-fast-note-sync ./cmd
```

### Verify

```bash
go-fast-note-sync --help
```

## Quick Start

**1. Generate default config**

```bash
go-fast-note-sync init-config
# writes ~/.config/go-fast-note-sync/config.yaml
```

**2. Edit config**

```yaml
# ~/.config/go-fast-note-sync/config.yaml
api: https://your-fast-note-sync-service.example.com
api_token: ${FAST_NOTE_TOKEN}   # or paste token directly
vault: MyVault
vault_path: /home/you/vault/MyVault
sync_enabled: true
config_sync_enabled: true
```

**3. Start the daemon**

```bash
go-fast-note-sync start
# or with explicit config path:
go-fast-note-sync start --config /path/to/config.yaml
```

**Check sync status (offline)**

```bash
go-fast-note-sync status
# prints vault, api, sync switches, last sync times, cache counts, and pending counts
```

**Trigger a one-shot sync and exit**

```bash
go-fast-note-sync sync
# connects, waits for the startup sync round to complete, then exits 0
# use --timeout to override the default 60s wait:
go-fast-note-sync sync --timeout 120s
```

## Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `api` | — | Base URL of the sync service |
| `api_token` | — | Auth token (supports `${ENV_VAR}`) |
| `vault` | — | Vault name on the server |
| `vault_path` | — | Absolute local path to the vault |
| `client_type` | `GoFastNoteSync` | Client identifier sent to server; override when the token scope restricts `c:<value>` |
| `sync_enabled` | `true` | Enable note/attachment sync |
| `config_sync_enabled` | `true` | Enable `.obsidian` settings sync |
| `offline_delete_sync_enabled` | `false` | Push local deletes that happened while offline |
| `sync_update_delay` | `500` | Debounce delay in ms for local file events |
| `binary_sync_limit_enabled` | `true` | Skip files larger than 128 MiB |
| `concurrency_control_enabled` | `true` | Enable upload slot control |
| `max_concurrent_uploads` | `3` | Max parallel file uploads |
| `sync_exclude_folders` | `[]` | Vault-relative folder paths to exclude |
| `sync_exclude_extensions` | `[]` | File extensions to exclude |
| `startup_delay` | `0` | Seconds to wait before first connect |
| `sync_timeout_seconds` | `60` | Max seconds to wait for a sync round |
| `state_file` | auto | Override default state file path |

Default state path: `~/.local/share/go-fast-note-sync/state.json`

## Token & Client Type

`client_type` is sent to the server as the `?client=` query parameter in the WebSocket URL and as the `type` field in the `ClientInfo` handshake message. The default value is `GoFastNoteSync`.

### Creating a token in the management console

When creating or editing a token, the console shows a **Client restriction** field (客户端限制). Whatever you enter there must exactly match the `client_type` in your config file, or the WebSocket handshake will be rejected with `AuthorizationFaild` (error code 315).

| Management console "Client restriction" | Config `client_type` | Result |
|-----------------------------------------|----------------------|--------|
| `GoFastNoteSync` | `GoFastNoteSync` (default) | ✅ connects |
| *(left empty / no restriction)* | any value | ✅ connects |
| `GoFastNoteSync` | `MyCustomClient` | ❌ rejected |

**Recommended:** set the Client restriction to `GoFastNoteSync` when issuing a token for this daemon — no config change needed since that is the default.

If you need a custom identifier, set both sides to the same value:

```yaml
client_type: MyCustomClient
```

## systemd User Service

Copy the unit file and enable it:

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/go-fast-note-sync.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now go-fast-note-sync
```

The service reads `~/.config/go-fast-note-sync/config.yaml` by default. Optionally drop secrets into `~/.config/go-fast-note-sync/sync.env`:

```bash
# sync.env
FAST_NOTE_TOKEN=your_token_here
```

Check status and logs:

```bash
systemctl --user status go-fast-note-sync
journalctl --user -u go-fast-note-sync -f
```

## Docker Deployment

A multi-stage Dockerfile and Compose file are provided in `deploy/docker/`. See **[deploy/docker/README.md](deploy/docker/README.md)** for the full operator quickstart covering build, configuration, volume layout, and inotify tuning.

## Development

```bash
# Format
go fmt ./...

# Lint (requires golangci-lint)
golangci-lint run ./...

# Test with race detector
go test -race ./... -coverprofile=coverage.out -covermode=atomic

# Build
go build ./...
```

Coverage gate: total and `internal/sync` statement coverage must both stay ≥ 80%.

## Project Structure

```
cmd/              CLI entry point (start / status / sync / init-config)
internal/
  config/         YAML config loading and defaults
  state/          Atomic JSON state persistence
  hash/           SHA-256 content and path hashing
  local/          Watcher-to-sync event contract
  sync/           WebSocket client, startup sync, protocol handlers
  watcher/        fsnotify watcher integration
deploy/systemd/   systemd user-service unit
deploy/docker/    Docker multi-stage build and Compose deployment
docs/             Design documents and protocol reference
test/smoke/       Integration smoke test harness
```

## Protocol

The daemon implements the WebSocket sync protocol used by the `obsidian-fast-note-sync` plugin:

- Health check: `GET /api/health`
- WebSocket: `ws(s)://[api]/api/user/sync?lang=&count=&client=&clientName=&clientVersion=`
- Text frames: `ACTION|JSON`
- Binary frames: 2-byte prefix (`00`) + 36-byte session ID + 4-byte chunk index + data

Sync modules: **NoteSync**, **FileSync**, **SettingSync**, **FolderSync** — each with a `*SyncEnd` completion signal.

## License

[Apache 2.0](LICENSE)
