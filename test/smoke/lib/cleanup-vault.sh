#!/usr/bin/env bash
# test/smoke/lib/cleanup-vault.sh
#
# Drain server-side smoke/* and .obsidian/plugins/smoke-* from the shared
# vault (default "Test", overridable via SMOKE_VAULT) by:
#   1. Booting a daemon and letting init sync write what the server has into a
#      scratch local vault.
#   2. `rm -rf` the smoke/-prefixed paths so the watcher pushes
#      Note/File/Setting/Folder *Delete protocol messages back to the server.
#   3. Waiting for the deletes to drain.
#   4. Wiping the scratch state + vault and repeating up to N iterations so
#      paths that arrived late in init sync get caught next round.
#
# Usage:
#   bash test/smoke/lib/cleanup-vault.sh [iterations=3] [download_wait=240] [delete_wait=240]
#
# Environment:
#   SMOKE_VAULT                  vault name (default "Test")
#   SMOKE_SYNC_TIMEOUT_SECONDS   daemon sync_timeout_seconds (default 180)
#
# This script is idempotent. Re-run until the final-snapshot summary shows the
# scratch vault has no smoke/ subdir.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
. "${SCRIPT_DIR}/common.sh"

ITERATIONS="${1:-3}"
DOWNLOAD_WAIT="${2:-240}"
DELETE_WAIT="${3:-240}"

case_init
VAULT="$(unique_vault_name)"
RUN_DIR="${SMOKE_RUN_ROOT}/cleanup-$(utc_stamp)"
mkdir -p "${RUN_DIR}"
log "VAULT=${VAULT}  RUN_DIR=${RUN_DIR}"
log "iterations=${ITERATIONS} download_wait=${DOWNLOAD_WAIT}s delete_wait=${DELETE_WAIT}s"

count_smoke_paths() {
  local vault_dir="$1"
  local notes_files=0 plugin_dirs=0
  if [ -d "${vault_dir}/smoke" ]; then
    notes_files=$(find "${vault_dir}/smoke" \( -type f -o -type d \) 2>/dev/null | wc -l)
  fi
  if [ -d "${vault_dir}/.obsidian/plugins" ]; then
    plugin_dirs=$(find "${vault_dir}/.obsidian/plugins" -maxdepth 1 -type d -name 'smoke-*' 2>/dev/null | wc -l)
  fi
  printf '%d smoke entries, %d plugins/smoke-* dirs' "${notes_files}" "${plugin_dirs}"
}

for iter in $(seq 1 "${ITERATIONS}"); do
  CLIENT_DIR="${RUN_DIR}/iter-${iter}"
  CFG="$(bootstrap_client "${CLIENT_DIR}" "${VAULT}")"
  VAULT_PATH="${CLIENT_DIR}/vault"
  LOG="${CLIENT_DIR}/daemon.log"
  STATE="${CLIENT_DIR}/state.json"

  log ""
  log "===== iteration ${iter}/${ITERATIONS} ====="
  log "starting daemon, downloading server state for up to ${DOWNLOAD_WAIT}s"
  PID="$(start_daemon "${CFG}" "${LOG}")"
  trap 'stop_daemon "${PID}" TERM >/dev/null 2>&1 || true' EXIT

  # Let init sync make as much progress as possible.
  sleep "${DOWNLOAD_WAIT}"
  log "iter ${iter}: downloaded — $(count_smoke_paths "${VAULT_PATH}")"

  # rm -rf the smoke/ subtree and any smoke-prefixed plugin dirs. The watcher
  # (still running) will translate each removal into a *Delete protocol message.
  if [ -d "${VAULT_PATH}/smoke" ]; then
    log "iter ${iter}: rm -rf vault/smoke/"
    rm -rf "${VAULT_PATH}/smoke"
  else
    log "iter ${iter}: no vault/smoke/ to delete"
  fi
  if compgen -G "${VAULT_PATH}/.obsidian/plugins/smoke-*" > /dev/null; then
    log "iter ${iter}: rm -rf vault/.obsidian/plugins/smoke-*"
    rm -rf "${VAULT_PATH}/.obsidian/plugins/smoke-"*
  else
    log "iter ${iter}: no plugins/smoke-* to delete"
  fi

  log "iter ${iter}: waiting ${DELETE_WAIT}s for watcher to push deletes"
  sleep "${DELETE_WAIT}"

  stop_daemon "${PID}" TERM
  trap - EXIT

  # Wipe local scratch so the next iteration does a fresh init sync against
  # the (hopefully smaller) server state.
  rm -rf "${CLIENT_DIR}"
  log "iter ${iter}: done"
done

# Final verification pass: download once more and report what's left.
FINAL_DIR="${RUN_DIR}/final-check"
CFG="$(bootstrap_client "${FINAL_DIR}" "${VAULT}")"
VAULT_PATH="${FINAL_DIR}/vault"
LOG="${FINAL_DIR}/daemon.log"

log ""
log "===== final verification: downloading once more to count leftovers ====="
PID="$(start_daemon "${CFG}" "${LOG}")"
trap 'stop_daemon "${PID}" TERM >/dev/null 2>&1 || true' EXIT
sleep "${DOWNLOAD_WAIT}"
LEFT="$(count_smoke_paths "${VAULT_PATH}")"
stop_daemon "${PID}" TERM
trap - EXIT

log "final leftovers: ${LEFT}"
log "artifacts: ${RUN_DIR}"
if printf '%s\n' "${LEFT}" | grep -q '^0 smoke entries, 0 plugins'; then
  log "cleanup PASS — server has no smoke-prefixed paths."
  exit 0
else
  log "cleanup PARTIAL — re-run the script to try again, or increase iterations/waits."
  exit 1
fi
