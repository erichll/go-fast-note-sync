#!/usr/bin/env bash
# M2.4: excluded ancestors stay traversable, without false offline deletions.
set -o errexit
set -o pipefail
set -o nounset
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/../lib/common.sh"
CASE_ID="20-exclusion-whitelist-reconcile"
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
KEEP="${PREFIX}/.hidden/keep.md"
SUB="${PREFIX}/.hidden/sub"
ALLOWED="${PREFIX}/nested/blocked/allowed"
DELETED="${SUB}/delete.md"
for cfg in "${A_CFG}" "${B_CFG}"; do
  sed -i "s|^sync_exclude_folders:.*|sync_exclude_folders: [\"blocked\"]|; s|^sync_exclude_whitelist:.*|sync_exclude_whitelist: [\"${KEEP}\", \"${SUB}\", \"${ALLOWED}\"]|" "${cfg}"
  grep -Fq "sync_exclude_whitelist: [\"${KEEP}\", \"${SUB}\", \"${ALLOWED}\"]" "${cfg}" || die "whitelist configuration not applied"
done
KEPT=("${KEEP}" "${SUB}/note.md" "${ALLOWED}/note.md")
IGNORED_NOTES=("${PREFIX}/.hidden/sibling.md" "${PREFIX}/nested/blocked/other.md" "${SUB}/name.tmp/other.md")
IGNORED_FILES=("${SUB}/.image.png.tmp-held" "${SUB}/image.tmp" "${SUB}/image.tmp.backup")
for rel in "${KEPT[@]}" "${DELETED}" "${IGNORED_NOTES[@]}" "${IGNORED_FILES[@]}"; do
  mkdir -p "$(dirname "${A_VAULT}/${rel}")"
  printf 'seed %s\n' "${rel}" > "${A_VAULT}/${rel}"
done
mkdir -p "${A_VAULT}/${SUB}/assets"
printf 'attachment\n' > "${A_VAULT}/${SUB}/assets/image.bin"
A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
wait_for_sync_round "${A_LOG}" 300
for rel in "${KEPT[@]}" "${DELETED}"; do
  wait_for_server_note_hash "${rel}" "$(sha256_file "${A_VAULT}/${rel}")" 300
  wait_for_state_nonempty "${A_DIR}/state.json" ".file_hash_map[\"${rel}\"].hash" 30
done
wait_for_server_path file "${SUB}/assets/image.bin" "$(file_content_hash "${A_VAULT}/${SUB}/assets/image.bin")" 300
# Prove the recursive watcher reaches a file under a hidden ancestor, rather
# than merely allowing its startup scan predicate.
sleep 1
printf 'watcher update\n' >> "${A_VAULT}/${KEEP}"
wait_for_server_note_hash "${KEEP}" "$(sha256_file "${A_VAULT}/${KEEP}")" 180
for rel in "${IGNORED_NOTES[@]}" "${IGNORED_FILES[@]}"; do
  printf 'ignored watcher update\n' >> "${A_VAULT}/${rel}"
done
sleep 3
for rel in "${IGNORED_NOTES[@]}"; do assert_server_path_absent note "${rel}"; done
for rel in "${IGNORED_FILES[@]}"; do assert_server_path_absent file "${rel}"; done
stop_daemon "${A_PID}" TERM; A_PID=""
# Retain state: remove only one whitelisted file while offline, then enable
# offline deletion. Existing whitelisted siblings must not enter delNotes.
rm "${A_VAULT}/${DELETED}"
sed -i 's/^offline_delete_sync_enabled:.*/offline_delete_sync_enabled: true/' "${A_CFG}"
A_LOG="${A_DIR}/reconcile.log"
A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
wait_for_sync_round "${A_LOG}" 300
wait_for_server_absent note "${DELETED}" 180
assert_server_path_absent note "${DELETED}"
for rel in "${KEPT[@]}"; do
  wait_for_server_note_hash "${rel}" "$(sha256_file "${A_VAULT}/${rel}")" 180
done
wait_for_server_path file "${SUB}/assets/image.bin" "$(file_content_hash "${A_VAULT}/${SUB}/assets/image.bin")" 180
stop_daemon "${A_PID}" TERM; A_PID=""
# Fresh B independently verifies survivors and excluded siblings/artifacts.
B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
wait_for_sync_round "${B_LOG}" 600
for rel in "${KEPT[@]}" "${SUB}/assets/image.bin"; do
  wait_for_disk_sha256 "${B_VAULT}/${rel}" "$(sha256_file "${A_VAULT}/${rel}")" 360
done
for rel in "${IGNORED_NOTES[@]}"; do
  assert_server_path_absent note "${rel}"
  [ ! -e "${B_VAULT}/${rel}" ] || die "excluded sibling downloaded: ${rel}"
done
for rel in "${IGNORED_FILES[@]}"; do
  assert_server_path_absent file "${rel}"
  [ ! -e "${B_VAULT}/${rel}" ] || die "temporary file downloaded: ${rel}"
done
[ ! -e "${B_VAULT}/${DELETED}" ] || die "offline-deleted note survived"
stop_daemon "${B_PID}" TERM; B_PID=""
server_snapshot "${PREFIX}" "${RUN_DIR}/server"
log "case ${CASE_ID} PASS"
