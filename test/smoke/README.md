# Smoke Test Catalog

This directory holds the live-service smoke suite for `go-fast-note-sync`.
Cases run against a real `fast-note-sync-service` instance (provided via
`test/smoke/.env`) and assert behaviour by inspecting `daemon.log` and
artifacts under `test/smoke/run/`. Most cases now use the service's read-only
HTTP APIs as the server-side oracle, then verify B-side local disk content for
download behaviour. `state.json` remains an internal-state assertion surface for
settings, restart, pending/checkpoint, and diagnostics.

## Prerequisites

- Linux host with `bash`, `go`, `jq`, `curl`, `sha256sum`, `flock`, `awk`, `sed`, `grep`.
- A reachable backend whose health endpoint returns 2xx (or 404, the protocol's
  legacy reachable indicator).
- `test/smoke/.env` exporting:

  ```text
  SYNC_API=https://your-fast-note-sync-service.example
  SYNC_TOKEN=eyJ...
  ```

  Both keys must be present; the harness aborts otherwise.

## Layout

```text
test/smoke/
├── .env                     # not in git; SYNC_API + SYNC_TOKEN
├── README.md                # this file
├── lib/
│   ├── common.sh            # shared shell harness
│   └── cleanup-vault.sh     # drain smoke/* + plugins/smoke-* from the shared vault
├── cases/                   # one shell script per smoke case
│   ├── 01-handshake-empty.sh
│   ├── 02-startup-uplink.sh
│   ├── 03-startup-downlink.sh
│   ├── 04-binary-upload.sh
│   ├── 05-binary-download.sh
│   ├── 06-watch-note-modify.sh
│   ├── 07-watch-file-modify.sh
│   ├── 08-watch-setting-modify.sh
│   ├── 09-watch-folder-create.sh
│   ├── 10-watch-rename.sh
│   ├── 11-watch-delete.sh
│   ├── 12-state-persist.sh
│   ├── 13-reconnect.sh       # may be BLOCKED — see Plan.md M1.7.5
│   └── 14-sensitive-config-exclusion.sh
└── run/                     # not in git; per-case run artifacts (`<case>-<UTC>/`)
```

## How to run

Run one case:

```bash
bash test/smoke/cases/01-handshake-empty.sh
```

Run the whole suite:

```bash
bash test/smoke/run-all.sh
```

Every case writes its working tree under
`test/smoke/run/<case-id>-<UTC-stamp>/`. Inspect `daemon.log`, `state.json`,
and `server/*.json` there when something fails. Cases exit non-zero on
assertion failure and leave the run dir intact for triage.

## Conventions

- **Single shared vault.** All cases run against one server-side vault named
  `Test` by default (override with `SMOKE_VAULT=<name>`). This avoids creating
  one new vault per smoke run on the live service.
- **Per-case path scoping.** Because cases share the vault, each case must
  scope its seed paths so they cannot collide on the server:
  - notes / attachments / folders → `smoke/<case-id>/<UTC-stamp>/...`
    (helper: `case_path_prefix "${CASE_ID}"`)
  - setting files → `.obsidian/plugins/smoke-<case-id>-<UTC-stamp>/data.json`
    (helper: `case_setting_path "${CASE_ID}"`)
- **Local isolation.** `vault_path` and `state_file` live under
  `test/smoke/run/<case-id>-<stamp>/{a,b}/` so each case run has its own local
  filesystem state regardless of the shared vault.
- **Uplink verification** prefers read-only service HTTP APIs where they are
  stable: `/api/note/histories` + `/api/note/history` for note content hashes,
  and `/api/files` for M1.5 chunk attachment `contentHash`. Rename-only note
  revisions assert active path rotation with `/api/notes` and verify content on
  B-side disk because note history may not expose rename-only content. Startup
  attachment, setting, and folder surfaces still use focused local state or
  B-side disk checks where API timing is not a reliable acceptance signal.
- **Downlink verification** boots a fresh client B against the same vault and
  asserts on-disk presence + sha256 match for the case's scoped path. B's
  `state.json` is diagnostic, not the primary success signal. B's init sync
  pulls everything the shared vault contains; expect long initial-sync times
  (`wait_for_sync_round` on B is set to 600 s).
- Daemon configs produced by `bootstrap_client` omit the `client_type:` YAML field so
  the daemon uses `config.Default()` (`GoFastNoteSync`). `SMOKE_CLIENT_TYPE` controls
  only the HTTP API headers used by the smoke harness; it does not inject a `client_type:`
  field into the daemon YAML. Pass an explicit third argument to `bootstrap_client` to
  override the daemon's client type for a specific case.
- Vault growth is unbounded across many runs. To reset, run
  `bash test/smoke/lib/cleanup-vault.sh` — it boots a scratch daemon against
  the shared vault, `rm -rf`'s every `smoke/`-prefixed path locally, and lets
  the watcher push `*Delete` protocol messages back to the server. Final
  verification prints `0 smoke entries, 0 plugins/smoke-* dirs` on success.
- **Heavily loaded vault caveat.** A large shared vault can still slow B-side
  init sync, but most cases no longer use `state.json` as the remote-success
  oracle. Run `cleanup-vault.sh` to drain smoke-prefixed paths if B-side
  downloads become too slow.

## Case index

Status legend: ✅ passed end-to-end against the live service (latest run logged
under `run/<case>-<UTC>`) · ⛔ may report BLOCKED depending on host/session
capabilities (see Plan.md / Documentation.md).

| ID | Milestone | What it proves | Mode | Status |
|----|-----------|----------------|------|--------|
| 01 | M1.2 | Handshake: `[ws] connected`, `[auth] authorization successful`, `[auth] ClientInfo acknowledged`, and one `sync round complete` line. Does not assert `need={0 0 0 0}` because the shared `Test` vault carries prior content. | single client | ✅ |
| 02 | M1.3 | Startup uplink: seed note + attachment + `.obsidian/app.json` + folder; verify note on the server via HTTP oracle and attachment/setting/folder in focused local state. | single client | ✅ |
| 03 | M1.3+M1.4 | Startup downlink: empty client B joins the vault populated by A and downloads notes/attachments/settings/folders with disk sha256 verification. | two clients (A→B) | ✅ |
| 04 | M1.5 | Binary upload: 1.5 MiB random attachment uploads via `FileSyncEnd need.upload>=1`; verify server-side file `contentHash`. | single client | ✅ |
| 05 | M1.5 | Binary download: client B downloads the attachment seeded by A with sha256 match. | two clients (A→B) | ✅ |
| 06 | M1.6 | Watcher note modify: A edits a note after init sync; server note history reflects the new hash; fresh B mirrors the file on disk. | two clients (A→B) | ✅ |
| 07 | M1.6 | Watcher attachment modify: A modifies a binary; server file `contentHash` updates; B mirrors. | two clients (A→B) | ✅ |
| 08 | M1.6 | Watcher setting modify: A edits a plugin setting file; A records the setting hash and B mirrors the file on disk. | two clients (A→B) | ✅ |
| 09 | M1.6 | Watcher folder create: A creates a new folder + nested note; server note history and B-side disk content prove the subtree synced. | two clients (A→B) | ✅ |
| 10 | M1.6 | Watcher rename: A renames a note; server active note path rotates and B has only the new file on disk. | two clients (A→B) | ✅ |
| 11 | M1.6 | Watcher delete: A deletes a note; server active note path disappears and B's vault does not contain the file. | two clients (A→B) | ✅ |
| 12 | M1.3 | State persistence: seed under the case prefix, init sync, stop, restart; second run keeps the seeded note + setting hashes byte-stable, `ws_count` increments by exactly 1, `is_init_sync` stays true. Server may legitimately re-push `need.upload>=1` on reconnect; the assertion is on client-side invariance for the seeded paths. | single client | ✅ |
| 13 | M1.9 | In-process reconnect: SIGSTOP/SIGCONT around `RECONNECT_PAUSE_SEC` (default 90 s, ≥ pongWait); the client self-reconnects via its own read deadline without relying on the server closing the socket. Expect `[ws] reconnecting in …` and `wsCount=2`. | single client | ✅ |
| 14 | M1.8 | Sensitive config exclusion: `.obsidian/plugins/fast-note-sync/data.json` is present and modified locally but never enters config hash/pending state, and a fresh B client does not materialize it from downlink. | two clients (A→B) | ✅ |

## Authoring a new case

1. Copy the closest existing case under `cases/`.
2. Pick the next free numeric prefix and a short slug.
3. Source `lib/common.sh`, call `case_init`, allocate `RUN_DIR=$(mkrun <id>)`,
   and bootstrap clients with `bootstrap_client "$RUN_DIR/a" "$VAULT"`.
4. Use the existing helpers — `start_daemon`, `wait_for_sync_round`,
   `wait_for_server_note_hash`, `wait_for_server_path`,
   `wait_for_disk_sha256`, `assert_sha256_match`, `stop_daemon` — instead of
   rolling your own.
5. End the script with `log "case <id> PASS"`; non-zero `set -e` exits handle
   the FAIL path implicitly.
6. Add a row to the table above with the case's "what it proves" sentence.
