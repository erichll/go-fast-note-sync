#!/usr/bin/env bash
# 15-status-offline.sh — status command (offline: reads config + state, no network).
#
# What it proves:
#   A. Missing config → non-zero exit, output contains "load config".
#   B. Valid config + no state file → exit 0, all timestamps "never", all counts 0.
#   C. Valid config + state written by a real sync round → is_init_sync: true,
#      ws_count: 1, note_sync_time is an RFC3339 timestamp (not "never").

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="15-status-offline"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
BIN="$(binary_path)"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}"

# --------------------------------------------------------------------------- #
# A: missing config file → non-zero exit + "load config" in output.
log "A: missing config → error"
MISSING_CFG="${RUN_DIR}/does-not-exist.yaml"
A_OUT="$("$BIN" status --config "$MISSING_CFG" 2>&1 || true)"
printf '%s\n' "$A_OUT" | grep -q "load config" \
  || die "A: expected 'load config' in output, got: ${A_OUT}"
set +e; "$BIN" status --config "$MISSING_CFG" >/dev/null 2>&1; A_RC=$?; set -e
[ "$A_RC" -ne 0 ] || die "A: expected non-zero exit for missing config, got 0"
log "A: PASS"

# --------------------------------------------------------------------------- #
# B: valid config, no state file → all timestamps "never", all counts 0.
log "B: fresh state → never timestamps and zero counts"
B_DIR="${RUN_DIR}/b"
CFG_B="$(bootstrap_client "${B_DIR}" "${VAULT}")"
# Remove state file so status falls back to a zero-state.
rm -f "${B_DIR}/state.json"
B_OUT="$("$BIN" status --config "$CFG_B")"

check_b() {
  local label="$1" pattern="$2"
  printf '%s\n' "$B_OUT" | grep -q "$pattern" \
    || { log "B: missing field ${label}"; printf '%s\n' "$B_OUT" >&2; die "B: FAIL"; }
}
check_b "vault:"                  "^vault:"
check_b "api:"                    "^api:"
check_b "sync_enabled:"           "^sync_enabled:"
check_b "note_sync_time: never"   "^note_sync_time: never"
check_b "file_sync_time: never"   "^file_sync_time: never"
check_b "config_sync_time: never" "^config_sync_time: never"
check_b "folder_sync_time: never" "^folder_sync_time: never"
check_b "ws_count: 0"             "^ws_count: 0"
check_b "is_init_sync: false"     "^is_init_sync: false"
check_b "note_cache: 0"           "^note_cache: 0"
check_b "file_cache: 0"           "^file_cache: 0"
check_b "setting_cache: 0"        "^setting_cache: 0"
check_b "folder_cache: 0"         "^folder_cache: 0"
log "B: PASS"

# --------------------------------------------------------------------------- #
# C: after a real sync round → is_init_sync true, ws_count 1, timestamps set.
log "C: post-sync state → populated timestamps"
C_DIR="${RUN_DIR}/c"
CFG_C="$(bootstrap_client "${C_DIR}" "${VAULT}")"
STATE_C="${C_DIR}/state.json"
LOG_C="${C_DIR}/daemon.log"

PID="$(start_daemon "${CFG_C}" "${LOG_C}")"
trap 'stop_daemon "${PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${LOG_C}" 240
wait_for_state "${STATE_C}" '.is_init_sync' "true" 30
stop_daemon "${PID}" TERM
trap - EXIT

C_OUT="$("$BIN" status --config "$CFG_C")"

check_c() {
  local label="$1" pattern="$2"
  printf '%s\n' "$C_OUT" | grep -q "$pattern" \
    || { log "C: missing field ${label}"; printf '%s\n' "$C_OUT" >&2; die "C: FAIL"; }
}
check_c "is_init_sync: true" "^is_init_sync: true"
check_c "ws_count: 1"        "^ws_count: 1"
# note_sync_time must be an RFC3339 timestamp, not "never".
printf '%s\n' "$C_OUT" | grep -qE "^note_sync_time: [0-9]{4}-" \
  || { log "C: note_sync_time still 'never' after sync; output:"; printf '%s\n' "$C_OUT" >&2; die "C: FAIL"; }
log "C: PASS"

log "case ${CASE_ID} PASS"
