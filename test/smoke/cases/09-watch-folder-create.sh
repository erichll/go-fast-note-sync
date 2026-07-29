#!/usr/bin/env bash
# 09-watch-folder-create.sh — M1.6 watcher uplink for folder create.
#
# Verifies that a successful FolderModify write immediately persists
# folder_snapshot without relying on an unrelated note/config/file ack, and
# that the snapshot survives an immediate daemon restart.

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
A_STATE="${A_DIR}/state.json"
A_LOG="${A_DIR}/daemon.log"; B_LOG="${B_DIR}/daemon.log"

NEW_FOLDER_REL="${PREFIX}/new-folder"
# Pre-create only the parent path so the watcher's initial recursive walk
# registers it. Creating the whole nested path with mkdir -p after startup can
# race watch registration and lose the deepest directory event.
mkdir -p "${A_VAULT}/${PREFIX}"

A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 240
sleep 2  # let the watcher's recursive walk settle before mutating

log "creating empty folder via watcher path: ${NEW_FOLDER_REL}/"
mkdir "${A_VAULT}/${NEW_FOLDER_REL}"
wait_for_state_nonempty "${A_STATE}" ".folder_snapshot[\"${NEW_FOLDER_REL}\"] // \"\"" 90
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
stop_daemon "${A_PID}" TERM
trap - EXIT

# Restart A immediately and prove the persisted snapshot is still present.
A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 240
wait_for_state_nonempty "${A_STATE}" ".folder_snapshot[\"${NEW_FOLDER_REL}\"] // \"\"" 30
stop_daemon "${A_PID}" TERM
trap - EXIT

log "starting B; expecting downlink to mirror new folder"
B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
trap 'stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${B_LOG}" 600
stop_daemon "${B_PID}" TERM
trap - EXIT

[ -d "${B_VAULT}/${NEW_FOLDER_REL}" ] || die "expected ${B_VAULT}/${NEW_FOLDER_REL} to be created on B"

log "case ${CASE_ID} PASS"
