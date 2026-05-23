#!/usr/bin/env bash
# 12-state-persist.sh — state persistence across daemon restart.
#
# What it proves:
#   - state.json round-trips. The first run uploads seeded content under the
#     case's path prefix and persists hash maps; the second run on the same
#     state.json keeps the seeded entries byte-stable, increments ws_count
#     by exactly 1, and leaves is_init_sync=true.
#
# Note: the server may legitimately replay FolderSync/NoteSync/SettingSync
# entries on reconnect; the client-side invariant is what we assert here.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="12-state-persist"
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
LOG1="${A_DIR}/daemon-1.log"
LOG2="${A_DIR}/daemon-2.log"
STATE="${A_DIR}/state.json"

NOTE_REL="${PREFIX}/notes/persist.md"
mkdir -p "$(dirname "${VAULT_PATH}/${NOTE_REL}")" "$(dirname "${VAULT_PATH}/${SETTING_REL}")"
printf 'persist note %s\n' "${CASE_ID}" > "${VAULT_PATH}/${NOTE_REL}"
printf '{"case":"%s"}\n' "${CASE_ID}" > "${VAULT_PATH}/${SETTING_REL}"
NOTE_HASH="$(sha256_file "${VAULT_PATH}/${NOTE_REL}")"
CFG_HASH="$(sha256_file "${VAULT_PATH}/${SETTING_REL}")"

log "first run: upload seed"
PID="$(start_daemon "${CFG}" "${LOG1}")"
trap 'stop_daemon "${PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${LOG1}" 240
wait_for_state "${STATE}" ".file_hash_map[\"${NOTE_REL}\"].hash // \"\"" "${NOTE_HASH}" 90
wait_for_state "${STATE}" ".config_hash_map[\"${SETTING_REL}\"].hash // \"\"" "${CFG_HASH}" 90
stop_daemon "${PID}" TERM
trap - EXIT
SNAPSHOT_FIRST="${A_DIR}/state-after-1.json"
cp "${STATE}" "${SNAPSHOT_FIRST}"
[ "$(read_state_json "${STATE}" '.ws_count')" = "1" ] \
  || die "expected ws_count=1 after first run, got $(read_state_json "${STATE}" '.ws_count')"

log "second run: hash maps must stay invariant for the seeded paths"
PID="$(start_daemon "${CFG}" "${LOG2}")"
trap 'stop_daemon "${PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${LOG2}" 240
wait_for_state "${STATE}" '.is_init_sync' "true" 30
stop_daemon "${PID}" TERM
trap - EXIT


[ "$(read_state_json "${STATE}" '.ws_count')" = "2" ] \
  || die "expected ws_count=2 after second run, got $(read_state_json "${STATE}" '.ws_count')"
[ "$(read_state_json "${STATE}" '.is_init_sync')" = "true" ] \
  || die "expected is_init_sync=true after second run"

# The seeded paths' hashes must round-trip unchanged across the restart even
# if other entries (from the shared Test vault) shift due to server replay.
SEED_NOTE_HASH_AFTER="$(read_state_json "${STATE}"          ".file_hash_map[\"${NOTE_REL}\"].hash // \"\"")"
SEED_CFG_HASH_AFTER="$(read_state_json  "${STATE}"          ".config_hash_map[\"${SETTING_REL}\"].hash // \"\"")"
SEED_NOTE_HASH_BEFORE="$(read_state_json "${SNAPSHOT_FIRST}" ".file_hash_map[\"${NOTE_REL}\"].hash // \"\"")"
SEED_CFG_HASH_BEFORE="$(read_state_json  "${SNAPSHOT_FIRST}" ".config_hash_map[\"${SETTING_REL}\"].hash // \"\"")"
[ "${SEED_NOTE_HASH_BEFORE}" = "${SEED_NOTE_HASH_AFTER}" ] \
  || die "seeded note hash regressed across restart: ${SEED_NOTE_HASH_BEFORE} → ${SEED_NOTE_HASH_AFTER}"
[ "${SEED_CFG_HASH_BEFORE}" = "${SEED_CFG_HASH_AFTER}" ] \
  || die "seeded setting hash regressed across restart: ${SEED_CFG_HASH_BEFORE} → ${SEED_CFG_HASH_AFTER}"

log "case ${CASE_ID} PASS"
