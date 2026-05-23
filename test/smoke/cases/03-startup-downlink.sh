#!/usr/bin/env bash
# 03-startup-downlink.sh — M1.3+M1.4 startup sync downlink onto a fresh client.
#
# What it proves:
#   - Client A seeds note/attachment/setting/folder under a unique path prefix.
#   - Client B with an empty local vault joins the same shared `vault` and
#     downloads A's seeded artefacts with byte-for-byte sha256 match.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="03-startup-downlink"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
PREFIX="$(case_path_prefix "${CASE_ID}")"
SETTING_REL="$(case_setting_path "${CASE_ID}")"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}  PREFIX=${PREFIX}"

A_DIR="${RUN_DIR}/a"; B_DIR="${RUN_DIR}/b"
A_CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
B_CFG="$(bootstrap_client "${B_DIR}" "${VAULT}")"
A_VAULT="${A_DIR}/vault"; B_VAULT="${B_DIR}/vault"
A_LOG="${A_DIR}/daemon.log"; B_LOG="${B_DIR}/daemon.log"
A_STATE="${A_DIR}/state.json"

NOTE_REL="${PREFIX}/notes/downlink-note.md"
FILE_REL="${PREFIX}/assets/downlink.bin"
mkdir -p "$(dirname "${A_VAULT}/${NOTE_REL}")" \
         "$(dirname "${A_VAULT}/${FILE_REL}")" \
         "$(dirname "${A_VAULT}/${SETTING_REL}")" \
         "${A_VAULT}/${PREFIX}/folders/nested"
printf 'downlink note %s\n' "${CASE_ID}" > "${A_VAULT}/${NOTE_REL}"
printf 'downlink attachment %s\n' "${CASE_ID}" > "${A_VAULT}/${FILE_REL}"
printf '{"case":"%s","ts":"%s"}\n' "${CASE_ID}" "$(utc_stamp)" > "${A_VAULT}/${SETTING_REL}"
A_NOTE_HASH="$(sha256_file "${A_VAULT}/${NOTE_REL}")"
A_FILE_HASH="$(sha256_file "${A_VAULT}/${FILE_REL}")"
A_CFG_HASH="$(sha256_file "${A_VAULT}/${SETTING_REL}")"

log "running initial sync on A"
A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 240
wait_for_server_note_hash "${NOTE_REL}" "${A_NOTE_HASH}" 180
wait_for_state "${A_STATE}" ".file_hash_map[\"${FILE_REL}\"].hash // \"\"" "${A_FILE_HASH}" 90
wait_for_state "${A_STATE}" ".config_hash_map[\"${SETTING_REL}\"].hash // \"\"" "${A_CFG_HASH}" 90
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
stop_daemon "${A_PID}" TERM
trap - EXIT

log "running initial sync on B (empty local vault)"
B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
trap 'stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_sha256 "${B_VAULT}/${NOTE_REL}" "${A_NOTE_HASH}" 300
wait_for_disk_sha256 "${B_VAULT}/${FILE_REL}" "${A_FILE_HASH}" 300
wait_for_disk_sha256 "${B_VAULT}/${SETTING_REL}" "${A_CFG_HASH}" 300
stop_daemon "${B_PID}" TERM
trap - EXIT

assert_sha256_match "note"       "${A_VAULT}/${NOTE_REL}"    "${B_VAULT}/${NOTE_REL}"
assert_sha256_match "attachment" "${A_VAULT}/${FILE_REL}"    "${B_VAULT}/${FILE_REL}"
assert_sha256_match "setting"    "${A_VAULT}/${SETTING_REL}" "${B_VAULT}/${SETTING_REL}"

log "case ${CASE_ID} PASS"
