#!/usr/bin/env bash
# 14-sensitive-config-exclusion.sh — M1.8 sensitive plugin-local config guard.
#
# What it proves:
#   - `.obsidian/plugins/fast-note-sync/data.json` is not included in startup
#     SettingSync state even when present on disk before the daemon starts.
#   - Watcher changes to that path do not create config pending/hash state.
#   - A fresh B client does not materialize that sensitive path from the shared
#     server vault during downlink.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="14-sensitive-config-exclusion"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
SENSITIVE_REL=".obsidian/plugins/fast-note-sync/data.json"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}  SENSITIVE_REL=${SENSITIVE_REL}"

A_DIR="${RUN_DIR}/a"; B_DIR="${RUN_DIR}/b"
A_CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
B_CFG="$(bootstrap_client "${B_DIR}" "${VAULT}")"
A_VAULT="${A_DIR}/vault"; B_VAULT="${B_DIR}/vault"
A_LOG="${A_DIR}/daemon.log"; B_LOG="${B_DIR}/daemon.log"
A_STATE="${A_DIR}/state.json"

A_SENSITIVE="${A_VAULT}/${SENSITIVE_REL}"
B_SENSITIVE="${B_VAULT}/${SENSITIVE_REL}"
mkdir -p "$(dirname "${A_SENSITIVE}")"
printf '{"case":"%s","token":"must-not-sync","v":1}\n' "${CASE_ID}" > "${A_SENSITIVE}"

A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 240

wait_for_state "${A_STATE}" ".config_hash_map | has(\"${SENSITIVE_REL}\")" "false" 30
wait_for_state "${A_STATE}" ".pending_config_modifies | has(\"${SENSITIVE_REL}\")" "false" 30

log "rewriting sensitive config via watcher path"
printf '{"case":"%s","token":"must-not-sync","v":2,"ts":"%s"}\n' "${CASE_ID}" "$(utc_stamp)" > "${A_SENSITIVE}"
sleep 2
wait_for_state "${A_STATE}" ".config_hash_map | has(\"${SENSITIVE_REL}\")" "false" 30
wait_for_state "${A_STATE}" ".pending_config_modifies | has(\"${SENSITIVE_REL}\")" "false" 30

stop_daemon "${A_PID}" TERM
trap - EXIT

B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
trap 'stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_absent "${B_SENSITIVE}" 60
stop_daemon "${B_PID}" TERM
trap - EXIT

log "case ${CASE_ID} PASS"
