#!/usr/bin/env bash
# test/smoke/lib/common.sh
# Shared smoke harness for go-fast-note-sync.
# Source this file from cases/*.sh, or run it directly with `selftest` to verify
# the host environment.

set -o errexit
set -o pipefail
set -o nounset

# --------------------------------------------------------------------------- #
# Bootstrap: locate repo + load env.

SMOKE_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SMOKE_DIR="$(cd "${SMOKE_LIB_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SMOKE_DIR}/../.." && pwd)"
SMOKE_ENV_FILE="${SMOKE_ENV_FILE:-${SMOKE_DIR}/.env}"
SMOKE_RUN_ROOT="${SMOKE_DIR}/run"

log() { printf '[smoke %s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { printf '[smoke FATAL] %s\n' "$*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

ensure_prereqs() {
  need_cmd bash
  need_cmd go
  need_cmd jq
  need_cmd curl
  need_cmd sha256sum
  need_cmd flock
  need_cmd awk
  need_cmd sed
  need_cmd grep
}

load_env() {
  [ -f "${SMOKE_ENV_FILE}" ] || die "env file not found: ${SMOKE_ENV_FILE}"
  # shellcheck disable=SC1090
  set -a; . "${SMOKE_ENV_FILE}"; set +a
  [ -n "${SYNC_API:-}" ]   || die "SYNC_API not set in ${SMOKE_ENV_FILE}"
  [ -n "${SYNC_TOKEN:-}" ] || die "SYNC_TOKEN not set in ${SMOKE_ENV_FILE}"
  export SYNC_API SYNC_TOKEN
}

mask_token() {
  local t="${1:-}"
  local n=${#t}
  if [ "$n" -le 12 ]; then printf '****'; else printf '%s…%s' "${t:0:8}" "${t: -4}"; fi
}

# --------------------------------------------------------------------------- #
# Service reachability.

require_service_reachable() {
  load_env
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "${SYNC_API%/}/api/health" || true)"
  case "$code" in
    2*|404) log "service reachable (HTTP ${code} on /api/health)";;
    *) die "service unreachable: GET ${SYNC_API%/}/api/health => HTTP ${code:-no-response}";;
  esac
}

# --------------------------------------------------------------------------- #
# Server-side HTTP API observation.
#
# These helpers are read-only and are used as an external oracle for live smoke
# tests. The service requires the same client identity headers as the Obsidian
# plugin; without them, scoped tokens may return "Auth token Scope restricted".

# api_client_type: HTTP API header value for x-client; serves token scope checks only.
# Default matches the Go daemon's DefaultClientType. Override via SMOKE_CLIENT_TYPE.
api_client_type() { printf '%s\n' "${SMOKE_CLIENT_TYPE:-GoFastNoteSync}"; }
api_client_name() { printf '%s\n' "${SMOKE_CLIENT_NAME:-smoke-harness}"; }
api_client_version() { printf '%s\n' "${SMOKE_CLIENT_VERSION:-0.1.0-dev}"; }

smoke_api_get() {
  local endpoint="$1"
  shift
  curl -fsS \
    -H "Authorization: Bearer ${SYNC_TOKEN}" \
    -H "x-client: $(api_client_type)" \
    -H "x-client-name: $(api_client_name)" \
    -H "x-client-version: $(api_client_version)" \
    "${SYNC_API%/}${endpoint}" "$@"
}

path_hash() {
  printf '%s' "$1" | sha256sum | awk '{print $1}'
}

# server_path_record <note|file> <path> <out_json>
# Writes the matching list record to <out_json> and returns 0 if present.
server_path_record() {
  local kind="$1" rel="$2" out="$3"
  local endpoint
  case "$kind" in
    note) endpoint="/api/notes" ;;
    file) endpoint="/api/files" ;;
    *) die "server_path_record: unknown kind ${kind}" ;;
  esac

  local tmp page total_pages
  tmp="$(mktemp "${TMPDIR:-/tmp}/smoke-api-${kind}.XXXXXX")"
  page=1
  total_pages=1
  while [ "$page" -le "$total_pages" ]; do
    if ! smoke_api_get "$endpoint" -G \
        --data-urlencode "vault=$(unique_vault_name)" \
        --data-urlencode "page=${page}" \
        --data-urlencode "pageSize=100" \
        --data-urlencode "isRecycle=false" \
        --data-urlencode "keyword=${rel}" > "${tmp}"; then
      rm -f "${tmp}"
      return 2
    fi
    if jq -e --arg p "$rel" '.data.list[]? | select(.path == $p)' "${tmp}" > "${out}"; then
      rm -f "${tmp}"
      return 0
    fi
    total_pages="$(jq -r '.data.pager.totalPages // 1' "${tmp}" 2>/dev/null || printf '1')"
    [ "${total_pages}" -ge 1 ] 2>/dev/null || total_pages=1
    page=$((page + 1))
  done
  rm -f "${tmp}"
  return 1
}

# wait_for_server_path <note|file> <path> [<content_hash>] [<timeout_sec>=180]
# For notes this verifies presence. For files, pass a sha256 content hash to
# verify the server-side contentHash reported by /api/files.
wait_for_server_path() {
  local kind="$1" rel="$2" expected_hash="${3:-}" timeout="${4:-180}"
  local end actual record
  end=$(( $(date +%s) + timeout ))
  record="$(mktemp "${TMPDIR:-/tmp}/smoke-record-${kind}.XXXXXX")"
  actual=""
  while [ "$(date +%s)" -lt "$end" ]; do
    if server_path_record "$kind" "$rel" "$record"; then
      if [ -z "$expected_hash" ]; then
        rm -f "${record}"
        return 0
      fi
      actual="$(jq -r '.contentHash // .hash // ""' "${record}")"
      if [ "$actual" = "$expected_hash" ]; then
        rm -f "${record}"
        return 0
      fi
    fi
    sleep 1
  done
  log "wait_for_server_path: ${kind} ${rel} expected hash=${expected_hash:-<present>}, got ${actual:-<missing>} after ${timeout}s"
  rm -f "${record}"
  return 1
}

# wait_for_server_note_hash <path> <sha256> [<timeout_sec>=180]
# /api/notes confirms presence only; note history detail carries content.
wait_for_server_note_hash() {
  local rel="$1" expected_hash="$2" timeout="${3:-180}"
  local end histories detail latest_id actual ph
  end=$(( $(date +%s) + timeout ))
  histories="$(mktemp "${TMPDIR:-/tmp}/smoke-note-histories.XXXXXX")"
  detail="$(mktemp "${TMPDIR:-/tmp}/smoke-note-detail.XXXXXX")"
  ph="$(path_hash "$rel")"
  actual=""
  while [ "$(date +%s)" -lt "$end" ]; do
    if smoke_api_get "/api/note/histories" -G \
        --data-urlencode "vault=$(unique_vault_name)" \
        --data-urlencode "path=${rel}" \
        --data-urlencode "pathHash=${ph}" \
        --data-urlencode "page=1" \
        --data-urlencode "pageSize=1" > "${histories}"; then
      latest_id="$(jq -r '.data.list[0].id // ""' "${histories}" 2>/dev/null || true)"
      if [ -n "$latest_id" ] && [ "$latest_id" != "null" ]; then
        if smoke_api_get "/api/note/history" -G --data-urlencode "id=${latest_id}" > "${detail}"; then
          actual="$(jq -j 'if ((.data.diffs // []) | length) > 0 then (.data.diffs[] | select(.Type != -1) | .Text) else (.data.content // "") end' "${detail}" | sha256sum | awk '{print $1}')"
          if [ "$actual" = "$expected_hash" ]; then
            rm -f "${histories}" "${detail}"
            return 0
          fi
        fi
      fi
    fi
    sleep 1
  done
  log "wait_for_server_note_hash: ${rel} expected hash=${expected_hash}, got ${actual:-<missing>} after ${timeout}s"
  rm -f "${histories}" "${detail}"
  return 1
}

wait_for_server_absent() {
  local kind="$1" rel="$2" timeout="${3:-180}"
  local end record
  end=$(( $(date +%s) + timeout ))
  record="$(mktemp "${TMPDIR:-/tmp}/smoke-record-${kind}.XXXXXX")"
  while [ "$(date +%s)" -lt "$end" ]; do
    if ! server_path_record "$kind" "$rel" "$record"; then
      rm -f "${record}"
      return 0
    fi
    sleep 1
  done
  log "wait_for_server_absent: ${kind} ${rel} still present after ${timeout}s"
  rm -f "${record}"
  return 1
}

server_snapshot() {
  local prefix="$1" out_dir="$2"
  mkdir -p "$out_dir"
  smoke_api_get "/api/notes" -G \
    --data-urlencode "vault=$(unique_vault_name)" \
    --data-urlencode "page=1" \
    --data-urlencode "pageSize=100" \
    --data-urlencode "isRecycle=false" \
    --data-urlencode "keyword=${prefix}" > "${out_dir}/server-notes.json" || true
  smoke_api_get "/api/files" -G \
    --data-urlencode "vault=$(unique_vault_name)" \
    --data-urlencode "page=1" \
    --data-urlencode "pageSize=100" \
    --data-urlencode "isRecycle=false" \
    --data-urlencode "keyword=${prefix}" > "${out_dir}/server-files.json" || true
}

# --------------------------------------------------------------------------- #
# Binary build (cached by Go).

binary_path() { printf '%s/test/smoke/.bin/go-fast-note-sync' "${REPO_ROOT}"; }

build_binary() {
  local out; out="$(binary_path)"
  mkdir -p "$(dirname "$out")"
  ( cd "${REPO_ROOT}" && go build -o "$out" ./cmd ) || die "go build failed"
  printf '%s\n' "$out"
}

# --------------------------------------------------------------------------- #
# Run-directory helpers.

utc_stamp() { date -u +%Y%m%dT%H%M%SZ; }

mkrun() {
  local case_id="$1"
  local stamp; stamp="$(utc_stamp)"
  local dir="${SMOKE_RUN_ROOT}/${case_id}-${stamp}"
  mkdir -p "$dir"
  printf '%s\n' "$dir"
}

# Vault namespace. All smoke cases share a single server-side vault (default
# "Test") so the live service doesn't accumulate one new vault per run. Cases
# must scope their seed paths via case_path_prefix / case_setting_path so they
# do not collide with each other or with prior runs.
unique_vault_name() {
  printf '%s\n' "${SMOKE_VAULT:-Test}"
}

# Returns a vault-relative path prefix scoped to this case + run.
# Notes/attachments/folders for a case must live under this prefix.
# Call ONCE per script and reuse the result so the timestamp doesn't drift.
case_path_prefix() {
  local case_id="$1"
  printf 'smoke/%s/%s\n' "${case_id}" "$(utc_stamp)"
}

# Returns a setting path under .obsidian/plugins/<unique>/data.json. The path
# satisfies isConfigSyncPathAllowed's 4-part-plugin rule, so it routes through
# the SettingSync code path without colliding with other cases.
case_setting_path() {
  local case_id="$1"
  printf '.obsidian/plugins/smoke-%s-%s/data.json\n' "${case_id}" "$(utc_stamp)"
}

# --------------------------------------------------------------------------- #
# Config rendering.
#
# render_config <out_path> <vault> <vault_path> <state_file> [client_type]
# Omits client_type from YAML when arg 5 is absent or empty, so the daemon uses
# config.Default() (GoFastNoteSync). Pass an explicit value to override.
render_config() {
  local out="$1" vault="$2" vault_path="$3" state_file="$4"
  local sync_timeout="${SMOKE_SYNC_TIMEOUT_SECONDS:-180}"
  mkdir -p "$(dirname "$out")"
  mkdir -p "$vault_path"
  {
    cat <<YAML
api: "\${SYNC_API}"
api_token: "\${SYNC_TOKEN}"
vault: "${vault}"
vault_path: "${vault_path}"
YAML
    if [ $# -ge 5 ] && [ -n "${5}" ]; then
      printf 'client_type: "%s"\n' "${5}"
    fi
    cat <<YAML
sync_enabled: true
config_sync_enabled: true
offline_delete_sync_enabled: false
readonly_sync_enabled: false
manual_sync_enabled: false
offline_sync_strategy: auto
sync_update_delay: 300
binary_sync_limit_enabled: true
concurrency_control_enabled: true
max_concurrent_uploads: 3
sync_exclude_folders: []
sync_exclude_extensions: []
sync_exclude_whitelist: []
config_sync_other_dirs: []
startup_delay: 0
auto_redirect_enabled: true
state_file: "${state_file}"
sync_timeout_seconds: ${sync_timeout}
YAML
  } > "$out"
}

# --------------------------------------------------------------------------- #
# Daemon lifecycle.
#
# start_daemon <config_path> <log_path>
# Returns the PID on stdout.
start_daemon() {
  local config_path="$1" log_path="$2"
  local bin; bin="$(binary_path)"
  [ -x "$bin" ] || die "binary not built: $bin (call build_binary first)"
  mkdir -p "$(dirname "$log_path")"
  : > "$log_path"
  ( SYNC_API="$SYNC_API" SYNC_TOKEN="$SYNC_TOKEN" "$bin" start --config "$config_path" >>"$log_path" 2>&1 ) &
  local pid=$!
  printf '%s\n' "$pid"
}

# stop_daemon <pid> [<signal>]
stop_daemon() {
  local pid="$1" sig="${2:-TERM}"
  if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then return 0; fi
  kill -"$sig" "$pid" 2>/dev/null || true
  local i
  for i in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 0.5
  done
  kill -KILL "$pid" 2>/dev/null || true
  return 1
}

# --------------------------------------------------------------------------- #
# Log assertions and waits.
#
# wait_for_log <log_path> <pattern> [<timeout_sec>=60]
wait_for_log() {
  local log_path="$1" pattern="$2" timeout="${3:-60}"
  local end=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$end" ]; do
    if [ -f "$log_path" ] && grep -qE "$pattern" "$log_path"; then return 0; fi
    sleep 0.5
  done
  log "wait_for_log: pattern not found within ${timeout}s: ${pattern}"
  log "wait_for_log: tail of ${log_path}:"
  if [ -f "$log_path" ]; then tail -n 50 "$log_path" >&2 || true; fi
  return 1
}

assert_log_contains() {
  local log_path="$1" pattern="$2"
  grep -qE "$pattern" "$log_path" || { log "missing in ${log_path}: ${pattern}"; return 1; }
}

assert_log_contains_literal() {
  local log_path="$1" needle="$2"
  grep -qF "$needle" "$log_path" || { log "missing literal in ${log_path}: ${needle}"; return 1; }
}

assert_log_not_contains() {
  local log_path="$1" pattern="$2"
  if grep -qE "$pattern" "$log_path"; then log "unexpected in ${log_path}: ${pattern}"; return 1; fi
  return 0
}

assert_log_not_contains_literal() {
  local log_path="$1" needle="$2"
  if grep -qF "$needle" "$log_path"; then log "unexpected literal in ${log_path}: ${needle}"; return 1; fi
  return 0
}

# wait_for_sync_round <log_path> [<timeout_sec>=90]
# Matches "[sync] sync round complete" emitted after each completed round.
wait_for_sync_round() {
  wait_for_log "$1" '\[sync\] sync round complete' "${2:-90}"
}

# --------------------------------------------------------------------------- #
# State.json inspection.

read_state_json() {
  local state_file="$1" jq_expr="$2"
  [ -f "$state_file" ] || { log "state file missing: ${state_file}"; return 1; }
  jq -r "$jq_expr" "$state_file"
}

# wait_for_state <state_file> <jq_expr> <expected> [<timeout_sec>=60]
# Polls state.json until `jq -r <expr>` equals <expected>.
wait_for_state() {
  local state_file="$1" jq_expr="$2" expected="$3" timeout="${4:-60}"
  local end=$(( $(date +%s) + timeout ))
  local actual=""
  while [ "$(date +%s)" -lt "$end" ]; do
    if [ -f "$state_file" ]; then
      actual="$(jq -r "$jq_expr" "$state_file" 2>/dev/null || true)"
      if [ "$actual" = "$expected" ]; then return 0; fi
    fi
    sleep 0.5
  done
  log "wait_for_state: expected ${jq_expr}=${expected}, got ${actual:-<missing>} after ${timeout}s"
  return 1
}

# wait_for_all_pending_empty <state_file> [<timeout_sec>=120]
# Acks land in state.json asynchronously after a sync round completes
# (sendFileContentModify increments the Completed counter on send, while the
# *Ack handler clears pending maps and upload checkpoints separately). Cases
# that touch upload paths must wait for this cleanup before stopping the
# daemon, otherwise state.json still reflects the in-flight upload.
#
# WARNING: on a shared vault (single "Test" namespace) the live service may
# legitimately keep other cases' paths in pending_* while it re-pushes them on
# every reconnect. Prefer wait_for_path_pending_clear for per-path assertions.
wait_for_all_pending_empty() {
  local state_file="$1" timeout="${2:-120}"
  wait_for_state "$state_file" '.pending_note_modifies | length'   "0" "$timeout"
  wait_for_state "$state_file" '.pending_upload_hashes | length'   "0" "$timeout"
  wait_for_state "$state_file" '.pending_config_modifies | length' "0" "$timeout"
  wait_for_state "$state_file" '.upload_checkpoints | length'      "0" "$timeout"
}

# wait_for_path_pending_clear <state_file> <path> [<timeout_sec>=180]
# Waits until <path> is absent from every pending_* map (note, upload, config)
# and from upload_checkpoints. Use this instead of wait_for_all_pending_empty
# when other cases' paths may still be in flight on the shared vault.
wait_for_path_pending_clear() {
  local state_file="$1" path="$2" timeout="${3:-180}"
  wait_for_state "$state_file" ".pending_note_modifies   | has(\"${path}\")" "false" "$timeout"
  wait_for_state "$state_file" ".pending_upload_hashes   | has(\"${path}\")" "false" "$timeout"
  wait_for_state "$state_file" ".pending_config_modifies | has(\"${path}\")" "false" "$timeout"
  wait_for_state "$state_file" ".upload_checkpoints      | has(\"${path}\")" "false" "$timeout"
}

# --------------------------------------------------------------------------- #
# Hash helpers.

sha256_file() {
  [ -f "$1" ] || { log "sha256_file: missing ${1}"; return 1; }
  sha256sum "$1" | awk '{print $1}'
}

assert_sha256_match() {
  local label="$1" a="$2" b="$3"
  local ha hb
  ha="$(sha256_file "$a")"
  hb="$(sha256_file "$b")"
  if [ "$ha" != "$hb" ]; then
    log "${label}: sha256 mismatch"; log "  a: ${a} => ${ha}"; log "  b: ${b} => ${hb}"
    return 1
  fi
  log "${label}: sha256 match ${ha:0:12}…"
  return 0
}

wait_for_disk_sha256() {
  local path="$1" expected="$2" timeout="${3:-240}"
  local end actual=""
  end=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$end" ]; do
    if [ -f "$path" ]; then
      actual="$(sha256_file "$path")"
      if [ "$actual" = "$expected" ]; then return 0; fi
    fi
    sleep 1
  done
  log "wait_for_disk_sha256: expected ${path} sha256=${expected}, got ${actual:-<missing>} after ${timeout}s"
  return 1
}

wait_for_disk_absent() {
  local path="$1" timeout="${2:-120}"
  local end
  end=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$end" ]; do
    [ ! -e "$path" ] && return 0
    sleep 1
  done
  log "wait_for_disk_absent: ${path} still exists after ${timeout}s"
  return 1
}

# --------------------------------------------------------------------------- #
# Two-client orchestration helpers.
#
# Seeds and naming convention used across cases:
#   <run_dir>/a/{config.yaml,state.json,daemon.log,vault/}
#   <run_dir>/b/{config.yaml,state.json,daemon.log,vault/}
#
# bootstrap_client <run_dir>/<a|b> <vault> [<client_type>]
# Creates config.yaml + empty vault dir. Returns the config path on stdout.
# When client_type is omitted, render_config is called without arg 5 so the
# daemon uses config.Default() (GoFastNoteSync) without an explicit field.
bootstrap_client() {
  local client_dir="$1" vault="$2"
  local cfg="${client_dir}/config.yaml"
  local vault_path="${client_dir}/vault"
  local state_file="${client_dir}/state.json"
  if [ $# -ge 3 ] && [ -n "${3}" ]; then
    render_config "$cfg" "$vault" "$vault_path" "$state_file" "${3}"
  else
    render_config "$cfg" "$vault" "$vault_path" "$state_file"
  fi
  printf '%s\n' "$cfg"
}

# run_initial_sync <client_dir>
# Starts the daemon, waits for the first sync round, then stops it. Returns PID
# via stdout for callers that need to inspect logs while running.
run_initial_sync() {
  local client_dir="$1"
  local cfg="${client_dir}/config.yaml" log="${client_dir}/daemon.log"
  local pid; pid="$(start_daemon "$cfg" "$log")"
  if ! wait_for_sync_round "$log" 90; then
    stop_daemon "$pid" TERM || true
    die "initial sync did not complete in ${client_dir}"
  fi
  stop_daemon "$pid" TERM || die "failed to stop daemon ${pid}"
}

# --------------------------------------------------------------------------- #
# Case framework.

case_init() {
  ensure_prereqs
  load_env
  require_service_reachable
  build_binary >/dev/null
  log "harness ready: API=${SYNC_API} TOKEN=$(mask_token "$SYNC_TOKEN") binary=$(binary_path)"
}

# --------------------------------------------------------------------------- #
# Self-test.

selftest() {
  ensure_prereqs
  load_env
  log "REPO_ROOT=${REPO_ROOT}"
  log "SMOKE_DIR=${SMOKE_DIR}"
  log "SMOKE_ENV_FILE=${SMOKE_ENV_FILE}"
  log "SYNC_API=${SYNC_API}"
  log "SYNC_TOKEN=$(mask_token "$SYNC_TOKEN")"
  log "binary path=$(binary_path)"
  log "building binary (cached when up-to-date)…"
  build_binary >/dev/null
  log "selftest OK"
}

# Run selftest when invoked directly: `bash test/smoke/lib/common.sh selftest`.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  case "${1:-}" in
    selftest) selftest;;
    *) die "usage: $(basename "$0") selftest";;
  esac
fi
