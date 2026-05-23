#!/usr/bin/env bash
# 11-watch-delete.sh — M1.6 watcher uplink for note delete.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="11-watch-delete"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
PREFIX="$(case_path_prefix "${CASE_ID}")"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}  PREFIX=${PREFIX}"

A_DIR="${RUN_DIR}/a"; B_DIR="${RUN_DIR}/b"
A_CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
B_CFG="$(bootstrap_client "${B_DIR}" "${VAULT}")"
A_VAULT="${A_DIR}/vault"; B_VAULT="${B_DIR}/vault"
A_LOG="${A_DIR}/daemon.log"; B_LOG="${B_DIR}/daemon.log"

NOTE_REL="${PREFIX}/notes/doomed.md"
SURVIVOR_REL="${PREFIX}/notes/survivor.md"
A_NOTE="${A_VAULT}/${NOTE_REL}";    B_NOTE="${B_VAULT}/${NOTE_REL}"
A_SURV="${A_VAULT}/${SURVIVOR_REL}"; B_SURV="${B_VAULT}/${SURVIVOR_REL}"
mkdir -p "$(dirname "${A_NOTE}")"
printf 'this note will be deleted\n' > "${A_NOTE}"
printf 'this note must survive\n'    > "${A_SURV}"
SURV_HASH="$(sha256_file "${A_SURV}")"

A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 240
wait_for_server_path "note" "${NOTE_REL}" "" 180
wait_for_server_note_hash "${SURVIVOR_REL}" "${SURV_HASH}" 180

log "deleting note via watcher path"
# Settle: when init sync force-completed (e.g. shared vault was heavy), the WS
# may break right around the same instant and reconnect. Sleeping a few seconds
# avoids racing the watcher's delete event with the in-progress reconnect.
sleep 5
rm "${A_NOTE}"

wait_for_server_absent "note" "${NOTE_REL}" 240
wait_for_server_note_hash "${SURVIVOR_REL}" "${SURV_HASH}" 120
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
stop_daemon "${A_PID}" TERM
trap - EXIT

B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
trap 'stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_sha256 "${B_SURV}" "${SURV_HASH}" 300
wait_for_disk_absent "${B_NOTE}" 60
stop_daemon "${B_PID}" TERM
trap - EXIT

[ ! -f "${B_NOTE}" ] || die "expected ${B_NOTE} to be absent after delete downlink"
[ -f "${B_SURV}" ]   || die "expected ${B_SURV} to be present on B"
assert_sha256_match "survivor" "${A_SURV}" "${B_SURV}"

log "case ${CASE_ID} PASS"
