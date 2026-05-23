#!/usr/bin/env bash
# 16-sync-oneshot.sh — sync command (one-shot: connect → complete → exit 0).
#
# What it proves:
#   A. `sync --timeout 10ms` exits non-zero and output contains "timed out".
#   B. `sync` against the real service exits 0, prints "Sync complete.", and
#      writes state.json with is_init_sync=true and ws_count=1.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="16-sync-oneshot"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
BIN="$(binary_path)"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}"

# --------------------------------------------------------------------------- #
# A: very short timeout → non-zero exit + "timed out" in output.
log "A: --timeout 10ms → non-zero exit"
A_DIR="${RUN_DIR}/a"
CFG_A="$(bootstrap_client "${A_DIR}" "${VAULT}")"
A_OUT="$("$BIN" sync --config "$CFG_A" --timeout 10ms 2>&1 || true)"
set +e; "$BIN" sync --config "$CFG_A" --timeout 10ms >/dev/null 2>&1; A_RC=$?; set -e
[ "$A_RC" -ne 0 ] || die "A: expected non-zero exit for 10ms timeout, got 0"
printf '%s\n' "$A_OUT" | grep -qi "timed out" \
  || die "A: expected 'timed out' in output, got: ${A_OUT}"
log "A: PASS"

# --------------------------------------------------------------------------- #
# B: successful one-shot sync → exit 0 + "Sync complete." + state updated.
log "B: full one-shot sync → exit 0 and state persisted"
B_DIR="${RUN_DIR}/b"
CFG_B="$(bootstrap_client "${B_DIR}" "${VAULT}")"
STATE_B="${B_DIR}/state.json"
B_OUT="$("$BIN" sync --config "$CFG_B" --timeout 240s)"
printf '%s\n' "$B_OUT" | grep -q "Sync complete\." \
  || die "B: 'Sync complete.' not in output: ${B_OUT}"
[ -f "${STATE_B}" ] \
  || die "B: state.json not written after sync"
[ "$(read_state_json "${STATE_B}" '.is_init_sync')" = "true" ] \
  || die "B: is_init_sync not true after sync, got: $(read_state_json "${STATE_B}" '.is_init_sync')"
[ "$(read_state_json "${STATE_B}" '.ws_count')" = "1" ] \
  || die "B: ws_count not 1 after sync, got: $(read_state_json "${STATE_B}" '.ws_count')"
log "B: PASS"

log "case ${CASE_ID} PASS"
