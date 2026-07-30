#!/usr/bin/env bash
# 18-debug-disconnect-reconnect.sh — deterministic reconnect via debug hook.
#
# What it proves:
#   - `go-fast-note-sync start --debug-disconnect-after <duration>` closes the
#     live WebSocket once after a successful dial.
#   - The existing reconnect path re-authenticates and starts a fresh sync
#     round without manual network interruption or SIGSTOP.
#
# Shared vaults may retain large tombstone sets. Warm a checkpoint first so the
# disconnect/reconnect path is not drowned by a multi-thousand-item first scan.
# Case 13 remains the SIGSTOP/pongWait path.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="18-debug-disconnect-reconnect"
DISCONNECT_AFTER="${SMOKE_DEBUG_DISCONNECT_AFTER:-2s}"
SYNC_WAIT_SEC="${SMOKE_DEBUG_DISCONNECT_SYNC_WAIT:-900}"

case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}"
log "DISCONNECT_AFTER=${DISCONNECT_AFTER}"
log "SYNC_WAIT_SEC=${SYNC_WAIT_SEC}"

# Ensure the warm pass never inherits a disconnect hook from the caller env.
unset SMOKE_DEBUG_DISCONNECT_AFTER || true

A_DIR="${RUN_DIR}/a"
CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
LOG="${A_DIR}/daemon.log"
STATE="${A_DIR}/state.json"
PID=""

cleanup() {
  stop_daemon "${PID:-}" TERM >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "warming checkpoint before debug disconnect"
PID="$(start_daemon "${CFG}" "${LOG}")"
wait_for_sync_round "${LOG}" "${SYNC_WAIT_SEC}"
stop_daemon "${PID}" TERM
PID=""

WARM_WS="$(read_state_json "${STATE}" '.ws_count')"
[ -n "${WARM_WS}" ] && [ "${WARM_WS}" != "null" ] || die "warm ws_count missing"
WANT_WS="$((WARM_WS + 2))"
log "warm ws_count=${WARM_WS}; expect final ws_count=${WANT_WS} after dial+forced reconnect"

# Truncate the warm-run log so later assertions only see the disconnect pass.
: > "${LOG}"

export SMOKE_DEBUG_DISCONNECT_AFTER="${DISCONNECT_AFTER}"
log "starting disconnect/reconnect pass (SMOKE_DEBUG_DISCONNECT_AFTER=${SMOKE_DEBUG_DISCONNECT_AFTER})"
PID="$(start_daemon "${CFG}" "${LOG}")"

# First dial of this pass uses warm_ws+1; forced reconnect uses warm_ws+2.
wait_for_log "${LOG}" "\[ws\] connected \(wsCount=$((WARM_WS + 1))\)" 60
wait_for_log "${LOG}" '\[debug\] closing connection after' 60
wait_for_log "${LOG}" '\[ws\] reconnecting in ' 60
wait_for_log "${LOG}" "\[ws\] connected \(wsCount=$((WARM_WS + 2))\)" 120
wait_for_sync_round "${LOG}" "${SYNC_WAIT_SEC}"
assert_log_not_contains_literal "${LOG}" 'max reconnect attempts'

stop_daemon "${PID}" TERM
PID=""
trap - EXIT

GOT_WS="$(read_state_json "${STATE}" '.ws_count')"
[ "${GOT_WS}" = "${WANT_WS}" ] \
  || die "expected ws_count=${WANT_WS} after warm + debug disconnect reconnect, got ${GOT_WS}"

log "case ${CASE_ID} PASS"
