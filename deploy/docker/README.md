# Docker Deployment

Run `go-fast-note-sync` as a container on a Linux host.

## 1. Prerequisites

- **Docker** with BuildKit enabled (Docker Engine 20.10+, default since Engine 23.0 and Docker Desktop 4.x)
- **Linux host** — inotify is required for live file watching; Docker Desktop on macOS/Windows uses a different FS event bridge and is not the primary target
- A reachable `fast-note-sync-service` instance with a valid API token

## 2. Build

From the **project root**:

```bash
DOCKER_BUILDKIT=1 docker build -f deploy/docker/Dockerfile -t go-fast-note-sync:latest .
```

`DOCKER_BUILDKIT=1` is the explicit fallback for Docker Engine < 23.0; newer engines and Docker Desktop 4.x+ have BuildKit on by default. You can omit the prefix on those versions.

Alternatively, build via Compose from `deploy/docker/`:

```bash
docker compose build
```

The Dockerfile uses a multi-stage build: Go 1.24.5-alpine compiles a static binary; `alpine:3.21` provides CA certificates and a non-root user.

## 3. Configure

**a. Copy the environment file and set absolute host paths:**

```bash
cp deploy/docker/.env.example deploy/docker/.env
# edit deploy/docker/.env:
#   VAULT_PATH=/absolute/path/to/obsidian-vault
#   CONFIG_DIR=/absolute/path/to/go-fast-note-sync/config
#   STATE_DIR=/absolute/path/to/go-fast-note-sync/state
```

Use absolute paths so Compose behavior is independent of the working directory.

Note: `.env` is read automatically by `docker compose` but is **not** loaded into your interactive shell. To use `$VAULT_PATH` / `$CONFIG_DIR` / `$STATE_DIR` in shell commands, either replace them with real paths or source the file first:

```bash
set -a; . deploy/docker/.env; set +a
```

**b. Create host directories:**

```bash
mkdir -p $VAULT_PATH $CONFIG_DIR $STATE_DIR
```

**c. Fix ownership — the container runs as `appuser` (non-root).**

Discover the container UID:

```bash
docker run --rm --entrypoint id go-fast-note-sync:latest -u
```

Then grant write access:

```bash
chown <uid> $VAULT_PATH $CONFIG_DIR $STATE_DIR
```

Docker user-namespace mapping is an alternative if the host already uses it.

**d. Copy and fill in the config:**

```bash
cp deploy/docker/config.yaml.example $CONFIG_DIR/config.yaml
# edit $CONFIG_DIR/config.yaml:
#   api: https://your-server.example.com
#   api_token: your-token-here
#   vault: YourVaultName
```

`vault_path: /vault` is already set to match the container volume mount — do not change it.

## 4. Run

From `deploy/docker/`:

```bash
docker compose up -d
```

## 5. Volume Layout

| Mount point (container) | `.env` variable | Example host path | Purpose |
|-------------------------|-----------------|-------------------|---------|
| `/vault` | `VAULT_PATH` | `/home/user/obsidian-vault` | Obsidian vault (read-write; daemon writes incoming remote changes) |
| `/home/appuser/.config/go-fast-note-sync` | `CONFIG_DIR` | `/home/user/.config/go-fast-note-sync` | `config.yaml` and optional `sync.env` |
| `/home/appuser/.local/share/go-fast-note-sync` | `STATE_DIR` | `/home/user/.local/share/go-fast-note-sync` | `state.json` and `temp-chunks/` (crash-recovery upload chunks) |

Both `VAULT_PATH` and `STATE_DIR` must be writable by the container user because the daemon writes remote changes to the vault and persists state and temporary chunk files.

`STATE_DIR` must cover the full state directory, not just `state.json`, because `temp-chunks/` lives as a sibling: `<state-dir>/temp-chunks/`.

## 6. inotify Note

For large vaults, raise the inotify watch limit on the host:

```bash
echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

## 7. Logs

```bash
docker compose logs -f go-fast-note-sync
```

Or with plain Docker:

```bash
docker logs -f <container-name>
```
