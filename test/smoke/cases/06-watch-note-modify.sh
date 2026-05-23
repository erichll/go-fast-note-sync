#!/usr/bin/env bash
# 06-watch-note-modify.sh — M1.6 watcher uplink for note modify.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="06-watch-note-modify"
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

NOTE_REL="${PREFIX}/notes/watch.md"
A_NOTE="${A_VAULT}/${NOTE_REL}"
B_NOTE="${B_VAULT}/${NOTE_REL}"
mkdir -p "$(dirname "${A_NOTE}")"
printf 'initial content for %s\n' "${CASE_ID}" > "${A_NOTE}"
INITIAL_HASH="$(sha256_file "${A_NOTE}")"

log "starting A; waiting for initial sync"
A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 240
wait_for_server_note_hash "${NOTE_REL}" "${INITIAL_HASH}" 180

log "modifying note via watcher path"
printf 'modified content for %s at %s\n' "${CASE_ID}" "$(utc_stamp)" > "${A_NOTE}"
MODIFIED_HASH="$(sha256_file "${A_NOTE}")"
[ "${INITIAL_HASH}" != "${MODIFIED_HASH}" ] || die "modify did not change content"

wait_for_server_note_hash "${NOTE_REL}" "${MODIFIED_HASH}" 240
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
stop_daemon "${A_PID}" TERM
trap - EXIT

log "starting B; expecting downlink"
B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
trap 'stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_sha256 "${B_NOTE}" "${MODIFIED_HASH}" 300
stop_daemon "${B_PID}" TERM
trap - EXIT

[ -f "${B_NOTE}" ] || die "expected ${B_NOTE} after downlink"
assert_sha256_match "note" "${A_NOTE}" "${B_NOTE}"

log "case ${CASE_ID} PASS"
