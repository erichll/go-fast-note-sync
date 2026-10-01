#!/usr/bin/env bash
# M2.4: new, empty, and overwritten attachment commits inside a whitelist.
# This validates real-service Linux behavior, NOT Windows/SMB ACL inheritance.
set -o errexit
set -o pipefail
set -o nounset
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/../lib/common.sh"
CASE_ID="19-attachment-commit-isolation"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
PREFIX="$(case_path_prefix "${CASE_ID}")"
A_DIR="${RUN_DIR}/a"; B_DIR="${RUN_DIR}/b"
A_CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
B_CFG="$(bootstrap_client "${B_DIR}" "${VAULT}")"
A_VAULT="${A_DIR}/vault"; B_VAULT="${B_DIR}/vault"
A_LOG="${A_DIR}/daemon.log"; B_LOG="${B_DIR}/daemon.log"
A_PID=""; B_PID=""
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true; stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
ATTACH="${PREFIX}/assets/payload.bin"
EMPTY="${PREFIX}/assets/empty.bin"
TEMP="${PREFIX}/assets/.payload.bin.tmp-held"
JUNK="${PREFIX}/assets/payload.tmp.backup"
for cfg in "${A_CFG}" "${B_CFG}"; do
  sed -i "s|^sync_exclude_whitelist:.*|sync_exclude_whitelist: [\"${PREFIX}/assets\"]|" "${cfg}"
  grep -Fq "sync_exclude_whitelist: [\"${PREFIX}/assets\"]" "${cfg}" || die "whitelist configuration not applied"
done
mkdir -p "${A_VAULT}/${PREFIX}/assets" "${B_VAULT}/${PREFIX}/assets"
dd if=/dev/urandom of="${A_VAULT}/${ATTACH}" bs=1024 count=2048 status=none
: > "${A_VAULT}/${EMPTY}"
INITIAL_HASH="$(sha256_file "${A_VAULT}/${ATTACH}")"
A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
wait_for_sync_round "${A_LOG}" 300
wait_for_server_path file "${ATTACH}" "$(file_content_hash "${A_VAULT}/${ATTACH}")" 300
wait_for_server_path file "${EMPTY}" "$(file_content_hash "${A_VAULT}/${EMPTY}")" 300
stop_daemon "${A_PID}" TERM; A_PID=""
# Retain representative candidates across startup and watcher events. Actual
# randomly named pre-replacement candidates are covered deterministically in Go.
printf 'must not upload\n' > "${B_VAULT}/${TEMP}"
printf 'must not upload\n' > "${B_VAULT}/${JUNK}"
B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_sha256 "${B_VAULT}/${ATTACH}" "${INITIAL_HASH}" 360
wait_for_disk_sha256 "${B_VAULT}/${EMPTY}" "$(sha256_file "${A_VAULT}/${EMPTY}")" 360
[ "$(stat -c '%a' "${B_VAULT}/${ATTACH}")" = 644 ] || die "new attachment mode is not 0644"
printf 'watcher must not upload either\n' >> "${B_VAULT}/${TEMP}"
printf 'watcher must not upload either\n' >> "${B_VAULT}/${JUNK}"
sleep 3
assert_server_path_absent file "${TEMP}"
assert_server_path_absent file "${JUNK}"
stop_daemon "${B_PID}" TERM; B_PID=""
# Upload a new version, then restart B with its retained state and existing mode.
sleep 1
dd if=/dev/urandom of="${A_VAULT}/${ATTACH}" bs=1024 count=2048 status=none
UPDATED_HASH="$(sha256_file "${A_VAULT}/${ATTACH}")"
A_LOG="${A_DIR}/overwrite.log"
A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
wait_for_sync_round "${A_LOG}" 300
wait_for_server_path file "${ATTACH}" "$(file_content_hash "${A_VAULT}/${ATTACH}")" 300
stop_daemon "${A_PID}" TERM; A_PID=""
chmod 640 "${B_VAULT}/${ATTACH}"
B_LOG="${B_DIR}/overwrite.log"
B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_sha256 "${B_VAULT}/${ATTACH}" "${UPDATED_HASH}" 360
[ "$(stat -c '%a' "${B_VAULT}/${ATTACH}")" = 640 ] || die "overwrite did not preserve mode 0640"
sleep 3
assert_server_path_absent file "${TEMP}"
assert_server_path_absent file "${JUNK}"
wait_for_state "${B_DIR}/state.json" ".file_hash_map | has(\"${TEMP}\")" false 30
wait_for_state "${B_DIR}/state.json" ".pending_upload_hashes | has(\"${TEMP}\")" false 30
stop_daemon "${B_PID}" TERM; B_PID=""
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
log "case ${CASE_ID} PASS"
