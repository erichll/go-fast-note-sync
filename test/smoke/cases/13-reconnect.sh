#!/usr/bin/env bash
# 13-reconnect.sh — in-process reconnect smoke.
#
# What it proves:
#   - SIGSTOP'ing the daemon for at least pongWait (90 s) and then SIGCONT
#     causes the client to reconnect.  Since M1.9 the client owns the
#     keepalive deadline (SetReadDeadline via gorilla + pingLoop); the Linux
#     kernel's TCP read deadline does not stop during SIGSTOP, so after
#     RECONNECT_PAUSE_SEC >= pongWait the very first ReadMessage after SIGCONT
#     returns i/o timeout and the client self-reconnects without relying on the
#     server closing the socket.
#
# The BLOCKED exit path below is retained as a safety net (e.g. if the daemon
# starts but the first sync round takes longer than expected).  It should not
# trigger for RECONNECT_PAUSE_SEC >= 90.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="13-reconnect"
RECONNECT_PAUSE_SEC="${RECONNECT_PAUSE_SEC:-90}"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}"
log "RECONNECT_PAUSE_SEC=${RECONNECT_PAUSE_SEC}"

A_DIR="${RUN_DIR}/a"
CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
LOG="${A_DIR}/daemon.log"
STATE="${A_DIR}/state.json"

PID="$(start_daemon "${CFG}" "${LOG}")"
trap 'kill -CONT "${PID}" 2>/dev/null || true; stop_daemon "${PID}" TERM >/dev/null 2>&1 || true' EXIT

wait_for_log "${LOG}" '\[ws\] connected \(wsCount=1\)' 30
wait_for_log "${LOG}" '\[sync\] sync round complete' 90

log "SIGSTOP pid=${PID}; sleeping ${RECONNECT_PAUSE_SEC}s"
kill -STOP "${PID}"
sleep "${RECONNECT_PAUSE_SEC}"
log "SIGCONT pid=${PID}; watching for reconnect logs"
kill -CONT "${PID}"

if ! wait_for_log "${LOG}" '\[ws\] reconnecting in ' 60; then
  log "BLOCKED: client did not reconnect within 60s after SIGCONT"
  log "BLOCKED: ensure RECONNECT_PAUSE_SEC (${RECONNECT_PAUSE_SEC}) >= pongWait (90s)"
  log "BLOCKED: keep run dir for triage: ${RUN_DIR}"
  stop_daemon "${PID}" TERM
  trap - EXIT
  exit 78  # EX_CONFIG — distinguishable from a FAIL exit
fi

wait_for_log "${LOG}" '\[ws\] connected \(wsCount=2\)' 120
wait_for_log "${LOG}" '\[sync\] sync round complete' 120
assert_log_not_contains_literal "${LOG}" 'max reconnect attempts'

stop_daemon "${PID}" TERM
trap - EXIT

[ "$(read_state_json "${STATE}" '.ws_count')" = "2" ] \
  || die "expected ws_count=2 after reconnect, got $(read_state_json "${STATE}" '.ws_count')"

log "case ${CASE_ID} PASS"
