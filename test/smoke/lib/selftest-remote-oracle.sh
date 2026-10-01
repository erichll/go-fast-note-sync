#!/usr/bin/env bash
# Offline-only regression checks; never contact the service.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/common.sh"
unique_vault_name() { printf 'MockOnly\n'; }
fixture='{"code":1,"status":true,"data":{"list":[],"pager":{"totalPages":1}}}'
smoke_api_get() { printf '%s\n' "$fixture"; }
record="$(mktemp "${TMPDIR:-/tmp}/smoke-oracle-record.XXXXXX")"
payload="$(mktemp "${TMPDIR:-/tmp}/smoke-hash-payload.XXXXXX")"
trap 'rm -f "$record" "$payload"' EXIT
[ "$(file_content_hash "$payload")" = 0 ]
printf '\377' > "$payload"
[ "$(file_content_hash "$payload")" = 255 ]
printf 'attachment\n' > "$payload"
[ "$(file_content_hash "$payload")" = -738997433 ]
printf 'Mock protocol contentHash vectors PASS.\n'
if server_path_record file test.bin "$record"; then exit 1; else [ "$?" -eq 1 ]; fi
assert_server_path_absent file test.bin
fixture='{"code":1,"status":true,"data":{"list":null,"pager":{"totalRows":0}}}'
if server_path_record file test.bin "$record"; then exit 1; else [ "$?" -eq 1 ]; fi
assert_server_path_absent file test.bin
fixture='{"code":1,"status":true,"data":{"list":[{"path":"test.bin"}],"pager":{"totalPages":1}}}'
server_path_record file test.bin "$record"
if (assert_server_path_absent file test.bin) >/dev/null 2>&1; then exit 1; fi
for fixture in '{"error":"unauthorized"}' 'not JSON' '{"code":1,"status":true,"data":{"list":null}}' '{"code":0,"status":false,"data":{"list":[]}}'; do
  if server_path_record file test.bin "$record" 2>/dev/null; then exit 1; else [ "$?" -eq 2 ]; fi
  if (assert_server_path_absent file test.bin) >/dev/null 2>&1; then exit 1; fi
done
smoke_api_get() { return 1; }
if server_path_record file test.bin "$record"; then exit 1; else [ "$?" -eq 2 ]; fi
if (assert_server_path_absent file test.bin) >/dev/null 2>&1; then exit 1; fi
cases=("${SMOKE_DIR}"/cases/0[1-9]-*.sh "${SMOKE_DIR}"/cases/1[0-9]-*.sh "${SMOKE_DIR}"/cases/20-*.sh)
[ "${#cases[@]}" -eq 20 ]
printf 'Offline remote-oracle selftest PASS; suite discovers 20 cases. No live API called.\n'
