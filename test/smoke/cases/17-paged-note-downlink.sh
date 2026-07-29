#!/usr/bin/env bash
# 17-paged-note-downlink.sh — service 3.6 paged NoteSync downlink.
#
# What it proves:
#   - Client A uploads more notes than one default server download page.
#   - Checkpointed client B receives multiple NoteSyncPage messages,
#     acknowledges the non-final page, completes the round, and materializes
#     every note.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
. "${SCRIPT_DIR}/../lib/common.sh"

CASE_ID="17-paged-note-downlink"
case_init
RUN_DIR="$(mkrun "${CASE_ID}")"
VAULT="$(unique_vault_name)"
PREFIX="$(case_path_prefix "${CASE_ID}")"
NOTE_COUNT="${SMOKE_PAGING_NOTE_COUNT:-205}"
log "RUN_DIR=${RUN_DIR}"
log "VAULT=${VAULT} PREFIX=${PREFIX} NOTE_COUNT=${NOTE_COUNT}"

[ "${NOTE_COUNT}" -gt 200 ] || die "SMOKE_PAGING_NOTE_COUNT must be greater than 200"

A_DIR="${RUN_DIR}/a"; B_DIR="${RUN_DIR}/b"
A_CFG="$(bootstrap_client "${A_DIR}" "${VAULT}")"
B_CFG="$(bootstrap_client "${B_DIR}" "${VAULT}")"
A_VAULT="${A_DIR}/vault"; B_VAULT="${B_DIR}/vault"
A_LOG="${A_DIR}/daemon.log"; B_LOG="${B_DIR}/daemon.log"
NOTE_DIR="${PREFIX}/notes"
A_PID=""
B_PID=""

cleanup() {
  stop_daemon "${A_PID:-}" TERM >/dev/null 2>&1 || true
  stop_daemon "${B_PID:-}" TERM >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Both clients first consume any retained server tombstones and persist the
# same clean checkpoint. The subsequent B run therefore sees only this case's
# 205 new notes, even though the token is restricted to the shared Test vault.
log "warming A and B checkpoints before pagination seed"
A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
wait_for_sync_round "${A_LOG}" 900
stop_daemon "${A_PID}" TERM
A_PID=""

B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
wait_for_sync_round "${B_LOG}" 900
stop_daemon "${B_PID}" TERM
B_PID=""

mkdir -p "${A_VAULT}/${NOTE_DIR}"
for index in $(seq 1 "${NOTE_COUNT}"); do
  printf 'paged note %03d of %03d\n' "${index}" "${NOTE_COUNT}" \
    > "${A_VAULT}/${NOTE_DIR}/note-$(printf '%03d' "${index}").md"
done

FIRST_REL="${NOTE_DIR}/note-001.md"
LAST_REL="${NOTE_DIR}/note-$(printf '%03d' "${NOTE_COUNT}").md"
FIRST_HASH="$(sha256_file "${A_VAULT}/${FIRST_REL}")"
LAST_HASH="$(sha256_file "${A_VAULT}/${LAST_REL}")"

log "uploading ${NOTE_COUNT} notes from A"
A_PID="$(start_daemon "${A_CFG}" "${A_LOG}")"
wait_for_sync_round "${A_LOG}" 900
wait_for_server_note_hash "${FIRST_REL}" "${FIRST_HASH}" 300
wait_for_server_note_hash "${LAST_REL}" "${LAST_HASH}" 300
stop_daemon "${A_PID}" TERM
A_PID=""

log "downloading paged note set to checkpointed client B"
B_PID="$(start_daemon "${B_CFG}" "${B_LOG}")"
wait_for_sync_round "${B_LOG}" 900
wait_for_disk_sha256 "${B_VAULT}/${FIRST_REL}" "${FIRST_HASH}" 300
wait_for_disk_sha256 "${B_VAULT}/${LAST_REL}" "${LAST_HASH}" 300

REGISTERED_PAGES="$(grep -c 'registered note page=' "${B_LOG}" || true)"
[ "${REGISTERED_PAGES}" -ge 2 ] \
  || die "expected at least two NoteSync pages, got ${REGISTERED_PAGES}"
assert_log_contains "${B_LOG}" 'sent NoteSyncPageAck pageIndex=-1'
assert_log_contains "${B_LOG}" 'sent NoteSyncPageAck pageIndex=0'

DOWNLOADED_COUNT="$(find "${B_VAULT}/${NOTE_DIR}" -type f -name '*.md' | wc -l | tr -d ' ')"
[ "${DOWNLOADED_COUNT}" -eq "${NOTE_COUNT}" ] \
  || die "downloaded ${DOWNLOADED_COUNT}/${NOTE_COUNT} paged notes"

# Let debounced fsnotify echoes drain before testing graceful shutdown after a
# large downlink.
sleep 2
stop_daemon "${B_PID}" TERM
B_PID=""

log "case ${CASE_ID} PASS"
