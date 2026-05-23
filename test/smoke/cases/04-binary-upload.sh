#!/usr/bin/env bash
# 04-binary-upload.sh — M1.5 binary chunk upload.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="04-binary-upload"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
PREFIX="$(case_path_prefix "${CASE_ID}")"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT}  PREFIX=${PREFIX}"

A_DIR="${RUN_DIR}/a"
CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
VAULT_PATH="${A_DIR}/vault"
LOG="${A_DIR}/daemon.log"

ATTACH_REL="${PREFIX}/assets/large.bin"
ATTACH_ABS="${VAULT_PATH}/${ATTACH_REL}"
mkdir -p "$(dirname "${ATTACH_ABS}")"
dd if=/dev/urandom of="${ATTACH_ABS}" bs=1024 count=1536 status=none
SEED_HASH="$(sha256_file "${ATTACH_ABS}")"
log "seed attachment: ${ATTACH_REL} (1.5 MiB) sha256=${SEED_HASH:0:12}…"

PID="$(start_daemon "${CFG}" "${LOG}")"
trap 'stop_daemon "${PID}" TERM >/dev/null 2>&1 || true' EXIT

wait_for_sync_round "${LOG}" 240
assert_log_contains_literal "${LOG}" 'FileSyncEnd: lastTime'
# FileSyncEnd need.upload>=1 confirms at least one upload was scheduled. With a
# shared vault other cases may have left pending modifies that bump the count;
# the assertion is "we asked the server to accept an upload", not "exactly 1".
assert_log_contains "${LOG}" 'FileSyncEnd:.*need=\{upload:[1-9]'

wait_for_server_path "file" "${ATTACH_REL}" "${SEED_HASH}" 240
server_snapshot "${PREFIX}" "${RUN_DIR}/server"

stop_daemon "${PID}" TERM
trap - EXIT

# Note: we do not assert pending_upload_hashes / upload_checkpoints empty here.
# The server-side file contentHash above is the primary upload oracle; case 05
# also verifies B-side binary download + sha256 match.

log "case ${CASE_ID} PASS"
