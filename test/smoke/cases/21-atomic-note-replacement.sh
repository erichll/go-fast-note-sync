#!/usr/bin/env bash
# M2.5: real-service convergence after atomic note saves and remote application,
# with both default and widened debounce; genuine edits, moves and deletes work.
# Exact zero-Delete/zero-echo frame assertions live in the Go native tests:
# HTTP polling here is NOT proof that no transient deletion ever occurred.
set -o errexit
set -o pipefail
set -o nounset
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/../lib/common.sh"
CASE_ID="21-atomic-note-replacement"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
PREFIX="$(case_path_prefix "${CASE_ID}")"
A_PID=""; B_PID=""
trap 'stop_daemon "${A_PID}" TERM >/dev/null 2>&1 || true; stop_daemon "${B_PID}" TERM >/dev/null 2>&1 || true' EXIT

for delay in 500 2000; do
  A_DIR="${RUN_DIR}/${delay}/a"; B_DIR="${RUN_DIR}/${delay}/b"
  A_CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
  B_CFG="$(bootstrap_client "${B_DIR}" "${VAULT}")"
  A_VAULT="${A_DIR}/vault"; B_VAULT="${B_DIR}/vault"
  A_LOG="${A_DIR}/daemon.log"; B_LOG="${B_DIR}/daemon.log"
  NOTE="${PREFIX}/delay-${delay}/note.md"
  MOVED="${PREFIX}/delay-${delay}/moved.md"
  TEMP="${PREFIX}/delay-${delay}/.note.md.tmp-smoke"
  for cfg in "${A_CFG}" "${B_CFG}"; do
    sed -i "s/^sync_update_delay:.*/sync_update_delay: ${delay}/" "${cfg}"
    grep -qx "sync_update_delay: ${delay}" "${cfg}" || die "debounce config not applied"
  done
  mkdir -p "$(dirname "${A_VAULT}/${NOTE}")"
  printf 'seed %s\n' "${delay}" > "${A_VAULT}/${NOTE}"
  A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
  wait_for_sync_round "${A_LOG}" 300
  wait_for_server_note_hash "${NOTE}" "$(sha256_file "${A_VAULT}/${NOTE}")" 180
  B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
  wait_for_sync_round "${B_LOG}" 300
  wait_for_disk_sha256 "${B_VAULT}/${NOTE}" "$(sha256_file "${A_VAULT}/${NOTE}")" 180

  # Save by creating a candidate in the destination directory and atomically
  # replacing the existing target. B applies the resulting remote update.
  for version in 1 2 3 4 5; do
    printf 'atomic version %s delay %s\n' "${version}" "${delay}" > "${A_VAULT}/${TEMP}"
    mv -f "${A_VAULT}/${TEMP}" "${A_VAULT}/${NOTE}"
  done
  HASH="$(sha256_file "${A_VAULT}/${NOTE}")"
  wait_for_server_note_hash "${NOTE}" "${HASH}" 180
  wait_for_disk_sha256 "${B_VAULT}/${NOTE}" "${HASH}" 180

  # A real edit on the receiving client must not be swallowed as a write echo.
  printf 'receiving client edit delay %s\n' "${delay}" > "${B_VAULT}/${TEMP}"
  mv -f "${B_VAULT}/${TEMP}" "${B_VAULT}/${NOTE}"
  HASH="$(sha256_file "${B_VAULT}/${NOTE}")"
  wait_for_server_note_hash "${NOTE}" "${HASH}" 180
  wait_for_disk_sha256 "${A_VAULT}/${NOTE}" "${HASH}" 180

  mv "${B_VAULT}/${NOTE}" "${B_VAULT}/${MOVED}"
  wait_for_server_note_hash "${MOVED}" "${HASH}" 180
  wait_for_server_absent note "${NOTE}" 180
  wait_for_disk_sha256 "${A_VAULT}/${MOVED}" "${HASH}" 180
  wait_for_disk_absent "${A_VAULT}/${NOTE}" 180
  mv "${B_VAULT}/${MOVED}" "${B_VAULT}/${NOTE}"
  wait_for_server_note_hash "${NOTE}" "${HASH}" 180
  wait_for_server_absent note "${MOVED}" 180
  wait_for_disk_sha256 "${A_VAULT}/${NOTE}" "${HASH}" 180

  rm "${B_VAULT}/${NOTE}"
  wait_for_server_absent note "${NOTE}" 180
  assert_server_path_absent note "${NOTE}"
  wait_for_disk_absent "${A_VAULT}/${NOTE}" 180
  assert_server_path_absent file "${TEMP}"
  stop_daemon "${B_PID}" TERM; B_PID=""
  stop_daemon "${A_PID}" TERM; A_PID=""
done
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
log "case ${CASE_ID} PASS"
