#!/usr/bin/env bash
# 10-watch-rename.sh — M1.6 watcher uplink for note rename.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="10-watch-rename"
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

OLD_REL="${PREFIX}/notes/before.md"
NEW_REL="${PREFIX}/notes/after.md"
A_OLD="${A_VAULT}/${OLD_REL}"; A_NEW="${A_VAULT}/${NEW_REL}"
B_OLD="${B_VAULT}/${OLD_REL}"; B_NEW="${B_VAULT}/${NEW_REL}"
mkdir -p "$(dirname "${A_OLD}")"
printf 'rename payload for %s\n' "${CASE_ID}" > "${A_OLD}"
NOTE_HASH="$(sha256_file "${A_OLD}")"

A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 240
wait_for_server_note_hash "${OLD_REL}" "${NOTE_HASH}" 180

log "renaming note via watcher path"
mv "${A_OLD}" "${A_NEW}"

# A rename rotates the active server path, but the note-history oracle does not
# reliably expose content for rename-only revisions. B-side disk sha256 below is
# the content assertion.
wait_for_server_path "note" "${NEW_REL}" "" 240
wait_for_server_absent "note" "${OLD_REL}" 240
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
stop_daemon "${A_PID}" TERM
trap - EXIT

B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
trap 'stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_sha256 "${B_NEW}" "${NOTE_HASH}" 300
wait_for_disk_absent "${B_OLD}" 60
stop_daemon "${B_PID}" TERM
trap - EXIT

[ -f "${B_NEW}" ]  || die "expected ${B_NEW} to exist after rename downlink"
[ ! -f "${B_OLD}" ] || die "old path ${B_OLD} should not exist after rename downlink"
assert_sha256_match "renamed-note" "${A_NEW}" "${B_NEW}"

log "case ${CASE_ID} PASS"
