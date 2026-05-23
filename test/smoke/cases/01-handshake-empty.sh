#!/usr/bin/env bash
# 01-handshake-empty.sh — M1.2 handshake on a fresh local vault.
#
# What it proves:
#   - Health check + ws dial + Authorization + ClientInfo all succeed.
#   - All four *SyncEnd handlers fire and a `[sync] sync round complete` line
#     is emitted.
#
# Note: this case runs against the shared "Test" vault so it does not assert
# `need={0 0 0 0}` — prior smoke runs leave content on the server. The local
# vault on disk is fresh; we just verify the handshake path and that init sync
# reaches completion against whatever the server reports.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="01-handshake-empty"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}"

A_DIR="${RUN_DIR}/a"
CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
LOG="${A_DIR}/daemon.log"
STATE="${A_DIR}/state.json"

PID="$(start_daemon "${CFG}" "${LOG}")"
trap 'stop_daemon "${PID}" TERM >/dev/null 2>&1 || true' EXIT

wait_for_log "${LOG}" '\[ws\] connected \(wsCount=1\)' 30
wait_for_log "${LOG}" '\[auth\] authorization successful' 30
wait_for_log "${LOG}" '\[auth\] ClientInfo acknowledged' 30
wait_for_log "${LOG}" '\[sync\] sync round complete' 240

assert_log_contains_literal "${LOG}" 'FolderSyncEnd: lastTime'
assert_log_contains_literal "${LOG}" 'NoteSyncEnd: lastTime'
assert_log_contains_literal "${LOG}" 'FileSyncEnd: lastTime'
assert_log_contains_literal "${LOG}" 'SettingSyncEnd: lastTime'

stop_daemon "${PID}" TERM
trap - EXIT

[ "$(read_state_json "${STATE}" '.ws_count')" = "1" ] \
  || die "expected ws_count=1, got $(read_state_json "${STATE}" '.ws_count')"

log "case ${CASE_ID} PASS"
