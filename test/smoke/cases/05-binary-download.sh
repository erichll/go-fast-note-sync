#!/usr/bin/env bash
# 05-binary-download.sh — M1.5 binary chunk download.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="05-binary-download"
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

ATTACH_REL="${PREFIX}/assets/payload.bin"
A_ATTACH="${A_VAULT}/${ATTACH_REL}"
B_ATTACH="${B_VAULT}/${ATTACH_REL}"
mkdir -p "$(dirname "${A_ATTACH}")"
dd if=/dev/urandom of="${A_ATTACH}" bs=1024 count=2048 status=none  # 2 MiB
SEED_HASH="$(sha256_file "${A_ATTACH}")"
log "seed attachment: ${ATTACH_REL} (2 MiB) sha256=${SEED_HASH:0:12}…"

log "uploading from A"
A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${A_LOG}" 300
wait_for_server_path "file" "${ATTACH_REL}" "${SEED_HASH}" 300
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
stop_daemon "${A_PID}" TERM
trap - EXIT

log "downloading to B (empty local vault)"
B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
trap 'stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT
wait_for_sync_round "${B_LOG}" 600
wait_for_disk_sha256 "${B_ATTACH}" "${SEED_HASH}" 360
stop_daemon "${B_PID}" TERM
trap - EXIT

[ -f "${B_ATTACH}" ] || die "expected ${B_ATTACH} to exist after download"
assert_sha256_match "attachment" "${A_ATTACH}" "${B_ATTACH}"

log "case ${CASE_ID} PASS"
