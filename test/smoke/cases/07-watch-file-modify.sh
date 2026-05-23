#!/usr/bin/env bash
# 07-watch-file-modify.sh — M1.6 watcher uplink for attachment modify.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="07-watch-file-modify"
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

FILE_REL="${PREFIX}/assets/watch.bin"
A_FILE="${A_VAULT}/${FILE_REL}"
B_FILE="${B_VAULT}/${FILE_REL}"
mkdir -p "$(dirname "${A_FILE}")"
dd if=/dev/urandom of="${A_FILE}" bs=1024 count=64 status=none  # 64 KiB
INITIAL_HASH="$(sha256_file "${A_FILE}")"

A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 240
wait_for_server_path "file" "${FILE_REL}" "${INITIAL_HASH}" 180

log "rewriting attachment via watcher path"
dd if=/dev/urandom of="${A_FILE}" bs=1024 count=128 status=none  # 128 KiB
MODIFIED_HASH="$(sha256_file "${A_FILE}")"
[ "${INITIAL_HASH}" != "${MODIFIED_HASH}" ] || die "modify did not change content"

wait_for_server_path "file" "${FILE_REL}" "${MODIFIED_HASH}" 300
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
stop_daemon "${A_PID}" TERM
trap - EXIT

B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
trap 'stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_sha256 "${B_FILE}" "${MODIFIED_HASH}" 360
stop_daemon "${B_PID}" TERM
trap - EXIT

[ -f "${B_FILE}" ] || die "expected ${B_FILE} after downlink"
assert_sha256_match "attachment" "${A_FILE}" "${B_FILE}"

log "case ${CASE_ID} PASS"
