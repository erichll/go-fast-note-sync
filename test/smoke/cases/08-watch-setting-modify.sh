#!/usr/bin/env bash
# 08-watch-setting-modify.sh — M1.6 watcher uplink for setting modify.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="08-watch-setting-modify"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
SETTING_REL="$(case_setting_path "${CASE_ID}")"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}  SETTING_REL=${SETTING_REL}"

A_DIR="${RUN_DIR}/a"; B_DIR="${RUN_DIR}/b"
A_CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
B_CFG="$(bootstrap_client "${B_DIR}" "${VAULT}")"
A_VAULT="${A_DIR}/vault"; B_VAULT="${B_DIR}/vault"
A_LOG="${A_DIR}/daemon.log"; B_LOG="${B_DIR}/daemon.log"
A_STATE="${A_DIR}/state.json"

A_SET="${A_VAULT}/${SETTING_REL}"
B_SET="${B_VAULT}/${SETTING_REL}"
mkdir -p "$(dirname "${A_SET}")"
printf '{"case":"%s","v":1}\n' "${CASE_ID}" > "${A_SET}"
INITIAL_HASH="$(sha256_file "${A_SET}")"

A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 240
wait_for_state "${A_STATE}" ".config_hash_map[\"${SETTING_REL}\"].hash // \"\"" "${INITIAL_HASH}" 90

log "rewriting setting via watcher path"
printf '{"case":"%s","v":2,"ts":"%s"}\n' "${CASE_ID}" "$(utc_stamp)" > "${A_SET}"
MODIFIED_HASH="$(sha256_file "${A_SET}")"
[ "${INITIAL_HASH}" != "${MODIFIED_HASH}" ] || die "modify did not change content"

wait_for_state "${A_STATE}" ".config_hash_map[\"${SETTING_REL}\"].hash // \"\"" "${MODIFIED_HASH}" 240
stop_daemon "${A_PID}" TERM
trap - EXIT

B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
trap 'stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_sha256 "${B_SET}" "${MODIFIED_HASH}" 300
stop_daemon "${B_PID}" TERM
trap - EXIT

[ -f "${B_SET}" ] || die "expected ${B_SET} after downlink"
assert_sha256_match "setting" "${A_SET}" "${B_SET}"

log "case ${CASE_ID} PASS"
