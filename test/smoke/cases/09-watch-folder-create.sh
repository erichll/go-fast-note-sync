#!/usr/bin/env bash
# 09-watch-folder-create.sh — M1.6 watcher uplink for folder create.
#
# Notes:
#   - SendFolderModify updates folder_snapshot only in memory; without a
#     FolderModifyAck handler, A's state.json may not reflect the new folder
#     until some other ack saves state. The case co-mutates a nested note so
#     the note ack flushes state, and verifies B-side downlink either way.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="09-watch-folder-create"
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

# Seed a placeholder note inside the case prefix so init sync has work to do
# and the case prefix folder exists before we mutate.
SEED_NOTE_REL="${PREFIX}/notes/seed.md"
mkdir -p "$(dirname "${A_VAULT}/${SEED_NOTE_REL}")"
printf 'seed\n' > "${A_VAULT}/${SEED_NOTE_REL}"

NEW_FOLDER_REL="${PREFIX}/new-folder"
NESTED_NOTE_REL="${NEW_FOLDER_REL}/inside.md"

A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 240
wait_for_server_note_hash "${SEED_NOTE_REL}" "$(sha256_file "${A_VAULT}/${SEED_NOTE_REL}")" 180
sleep 2  # let the watcher's recursive walk settle before mutating

log "creating folder + nested note via watcher path: ${NEW_FOLDER_REL}/"
mkdir "${A_VAULT}/${NEW_FOLDER_REL}"
printf 'inside content for %s\n' "${CASE_ID}" > "${A_VAULT}/${NESTED_NOTE_REL}"
NESTED_HASH="$(sha256_file "${A_VAULT}/${NESTED_NOTE_REL}")"

# The nested note is the observable server-side proof that the new folder
# subtree was picked up by the watcher.
wait_for_server_note_hash "${NESTED_NOTE_REL}" "${NESTED_HASH}" 240
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
stop_daemon "${A_PID}" TERM
trap - EXIT

log "starting B; expecting downlink to mirror new folder"
B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
trap 'stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_sha256 "${B_VAULT}/${NESTED_NOTE_REL}" "${NESTED_HASH}" 300
stop_daemon "${B_PID}" TERM
trap - EXIT

[ -d "${B_VAULT}/${NEW_FOLDER_REL}" ] || die "expected ${B_VAULT}/${NEW_FOLDER_REL} to be created on B"
[ -f "${B_VAULT}/${NESTED_NOTE_REL}" ] || die "expected nested note ${B_VAULT}/${NESTED_NOTE_REL} on B"
assert_sha256_match "nested-note" "${A_VAULT}/${NESTED_NOTE_REL}" "${B_VAULT}/${NESTED_NOTE_REL}"

log "case ${CASE_ID} PASS"
