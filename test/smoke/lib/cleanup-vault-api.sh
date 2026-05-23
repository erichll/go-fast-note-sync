#!/usr/bin/env bash
# test/smoke/lib/cleanup-vault-api.sh
#
# Direct API-based vault cleanup: deletes every smoke-prefixed note, file, and
# setting from the shared vault using DELETE endpoints. Much faster than the
# daemon-based cleanup-vault.sh and does not depend on the watcher being
# functional.
#
# Usage:
#   bash test/smoke/lib/cleanup-vault-api.sh
#
# Environment:
#   SMOKE_VAULT   vault name (default "Test")
#   SYNC_API      set via .env (required)
#   SYNC_TOKEN    set via .env (required)
#
# Exit status: 0 if all deletions succeed; non-zero on first error.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
. "${SCRIPT_DIR}/common.sh"

load_env
VAULT="$(unique_vault_name)"

smoke_api_delete() {
  local endpoint="$1" body="$2"
  curl -fsS -X DELETE \
    -H "Authorization: Bearer ${SYNC_TOKEN}" \
    -H "x-client: $(api_client_type)" \
    -H "x-client-name: $(api_client_name)" \
    -H "x-client-version: $(api_client_version)" \
    -H "Content-Type: application/json" \
    -d "$body" \
    "${SYNC_API%/}${endpoint}"
}

# delete_all_notes: paginate GET /api/notes?keyword=smoke/ and DELETE each.
delete_all_notes() {
  local page=1 page_size=100 total_rows=1 deleted=0
  while [ $(( (page - 1) * page_size )) -lt "$total_rows" ]; do
    local resp
    resp="$(smoke_api_get "/api/notes" -G \
        --data-urlencode "vault=${VAULT}" \
        --data-urlencode "page=${page}" \
        --data-urlencode "pageSize=${page_size}" \
        --data-urlencode "isRecycle=false" \
        --data-urlencode "keyword=smoke/")"
    total_rows="$(printf '%s\n' "$resp" | jq -r '.data.pager.totalRows // 0')"
    local paths path ph
    paths="$(printf '%s\n' "$resp" | jq -r '.data.list[]? | .path')"
    while IFS= read -r path; do
      [ -n "$path" ] || continue
      ph="$(path_hash "$path")"
      smoke_api_delete "/api/note" \
        "{\"vault\":\"${VAULT}\",\"path\":\"${path}\",\"pathHash\":\"${ph}\"}" >/dev/null
      log "deleted note: ${path}"
      deleted=$((deleted + 1))
    done <<< "$paths"
    page=$((page + 1))
  done
  log "notes deleted: ${deleted}"
}

# delete_all_files: paginate GET /api/files?keyword=smoke/ and DELETE each.
delete_all_files() {
  local page=1 page_size=100 total_rows=1 deleted=0
  while [ $(( (page - 1) * page_size )) -lt "$total_rows" ]; do
    local resp
    resp="$(smoke_api_get "/api/files" -G \
        --data-urlencode "vault=${VAULT}" \
        --data-urlencode "page=${page}" \
        --data-urlencode "pageSize=${page_size}" \
        --data-urlencode "isRecycle=false" \
        --data-urlencode "keyword=smoke/")"
    total_rows="$(printf '%s\n' "$resp" | jq -r '.data.pager.totalRows // 0')"
    local paths path ph
    paths="$(printf '%s\n' "$resp" | jq -r '.data.list[]? | .path')"
    while IFS= read -r path; do
      [ -n "$path" ] || continue
      ph="$(path_hash "$path")"
      smoke_api_delete "/api/file" \
        "{\"vault\":\"${VAULT}\",\"path\":\"${path}\",\"pathHash\":\"${ph}\"}" >/dev/null
      log "deleted file: ${path}"
      deleted=$((deleted + 1))
    done <<< "$paths"
    page=$((page + 1))
  done
  log "files deleted: ${deleted}"
}

# delete_smoke_settings: GET /api/settings (no keyword filter) and DELETE
# entries whose path starts with .obsidian/plugins/smoke-.
delete_smoke_settings() {
  local page=1 page_size=100 deleted=0
  while true; do
    local resp
    resp="$(smoke_api_get "/api/settings" -G \
        --data-urlencode "vault=${VAULT}" \
        --data-urlencode "page=${page}" \
        --data-urlencode "pageSize=${page_size}")"
    local count
    count="$(printf '%s\n' "$resp" | jq -r '.data.list | length')"
    [ "$count" -gt 0 ] || break
    local paths path ph
    paths="$(printf '%s\n' "$resp" | jq -r '.data.list[]? | select(.path | startswith(".obsidian/plugins/smoke-")) | .path')"
    while IFS= read -r path; do
      [ -n "$path" ] || continue
      ph="$(path_hash "$path")"
      smoke_api_delete "/api/setting" \
        "{\"vault\":\"${VAULT}\",\"path\":\"${path}\",\"pathHash\":\"${ph}\"}" >/dev/null
      log "deleted setting: ${path}"
      deleted=$((deleted + 1))
    done <<< "$paths"
    # Settings API returns all entries; stop if we got a partial page.
    [ "$count" -ge "$page_size" ] || break
    page=$((page + 1))
  done
  log "settings deleted: ${deleted}"
}

log "API cleanup: vault=${VAULT} api=${SYNC_API}"
delete_all_notes
delete_all_files
delete_smoke_settings
log "API cleanup complete"
