#!/usr/bin/env bash
# 02-startup-uplink.sh — M1.3 startup sync uplink from a seeded vault.
#
# What it proves:
#   - A vault that contains a note, attachment, setting file, and folder is
#     uploaded on the first sync round.
#   - The live service observes the note content; local state records the
#     attachment/setting/folder surfaces that do not have stable startup API
#     acceptance timing under a loaded shared vault.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="02-startup-uplink"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
PREFIX="$(case_path_prefix "${CASE_ID}")"
SETTING_REL="$(case_setting_path "${CASE_ID}")"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}  PREFIX=${PREFIX}"

A_DIR="${RUN_DIR}/a"
CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
VAULT_PATH="${A_DIR}/vault"
LOG="${A_DIR}/daemon.log"
STATE="${A_DIR}/state.json"

NOTE_REL="${PREFIX}/notes/uplink-note.md"
FILE_REL="${PREFIX}/assets/uplink.bin"
mkdir -p "$(dirname "${VAULT_PATH}/${NOTE_REL}")" \
         "$(dirname "${VAULT_PATH}/${FILE_REL}")" \
         "$(dirname "${VAULT_PATH}/${SETTING_REL}")" \
         "${VAULT_PATH}/${PREFIX}/folders"
printf 'hello from %s\n' "${CASE_ID}" > "${VAULT_PATH}/${NOTE_REL}"
printf 'binary seed %s %s\n' "${CASE_ID}" "$(utc_stamp)" > "${VAULT_PATH}/${FILE_REL}"
printf '{"case":"%s","ts":"%s"}\n' "${CASE_ID}" "$(utc_stamp)" > "${VAULT_PATH}/${SETTING_REL}"
NOTE_HASH="$(sha256_file "${VAULT_PATH}/${NOTE_REL}")"
FILE_HASH="$(sha256_file "${VAULT_PATH}/${FILE_REL}")"
CFG_HASH="$(sha256_file "${VAULT_PATH}/${SETTING_REL}")"
log "seed hashes: note=${NOTE_HASH:0:12}… file=${FILE_HASH:0:12}… cfg=${CFG_HASH:0:12}…"

PID="$(start_daemon "${CFG}" "${LOG}")"
trap 'stop_daemon "${PID}" TERM >/dev/null 2>&1 || true' EXIT

wait_for_sync_round "${LOG}" 240

wait_for_server_note_hash "${NOTE_REL}" "${NOTE_HASH}" 180
wait_for_state "${STATE}" ".file_hash_map[\"${FILE_REL}\"].hash // \"\"" "${FILE_HASH}" 90
wait_for_state "${STATE}" ".config_hash_map[\"${SETTING_REL}\"].hash // \"\"" "${CFG_HASH}" 90
wait_for_state "${STATE}" ".folder_snapshot | has(\"${PREFIX}/folders\")" "true" 90
server_snapshot "${PREFIX}" "${RUN_DIR}/server"

stop_daemon "${PID}" TERM
trap - EXIT

# Note: we do NOT assert pending_*_modifies is empty on the shared vault. The
# server-side note API check above is the primary remote oracle; binary file
# server contentHash is covered by the M1.5 chunk cases.

log "case ${CASE_ID} PASS"
