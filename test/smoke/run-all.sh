#!/usr/bin/env bash
# test/smoke/run-all.sh
#
# Wrapper that:
#   1. Runs cleanup-vault-api.sh before EACH case so every case starts against
#      a clean server-side vault. This ensures FileSyncEnd need.modify=0 at
#      case start and makes cases fully independent of each other.
#      Pass --no-cleanup to skip cleanup (useful for debugging a single case).
#      Pass --cleanup-daemon to use the daemon-based cleanup instead of the API.
#   2. Runs every smoke case under `cases/0[1-9]-*.sh` and `cases/1[0-4]-*.sh`
#      in order, capturing each case's stdout/stderr.
#   3. Writes a markdown summary to `run/all-<UTC>/summary.md`.
#
# Exit status: 0 only if every case PASSes. Otherwise non-zero.

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
. "${SCRIPT_DIR}/lib/common.sh"

DO_CLEANUP=1
CLEANUP_DAEMON=0
CLEANUP_ITERATIONS="${SMOKE_CLEANUP_ITERATIONS:-2}"
CLEANUP_DOWNLOAD_WAIT="${SMOKE_CLEANUP_DOWNLOAD_WAIT:-90}"
CLEANUP_DELETE_WAIT="${SMOKE_CLEANUP_DELETE_WAIT:-90}"

for arg in "$@"; do
  case "$arg" in
    --no-cleanup)    DO_CLEANUP=0 ;;
    --cleanup-daemon) CLEANUP_DAEMON=1 ;;
    -h|--help)
      cat <<'USAGE'
Usage: test/smoke/run-all.sh [--no-cleanup] [--cleanup-daemon]

Drains the shared smoke vault, then runs every case in test/smoke/cases/
in numeric order. Writes a markdown summary to test/smoke/run/all-<UTC>/.

Cleanup uses the server DELETE API (fast) by default.
Use --cleanup-daemon to use the daemon-based watcher cleanup instead.

Env knobs:
  SMOKE_VAULT                       vault name (default "Test")
  SMOKE_CLEANUP_ITERATIONS          daemon cleanup iterations (default 2)
  SMOKE_CLEANUP_DOWNLOAD_WAIT       daemon cleanup per-iter download wait (default 90s)
  SMOKE_CLEANUP_DELETE_WAIT         daemon cleanup per-iter delete wait   (default 90s)
USAGE
      exit 0
      ;;
    *) die "unknown argument: $arg" ;;
  esac
done

case_init
STAMP="$(utc_stamp)"
SUITE_DIR="${SMOKE_RUN_ROOT}/all-${STAMP}"
mkdir -p "${SUITE_DIR}"
SUMMARY="${SUITE_DIR}/summary.md"
log "suite run dir: ${SUITE_DIR}"

run_cleanup() {
  local log_path="$1"
  if [ "${CLEANUP_DAEMON}" -eq 1 ]; then
    bash "${SCRIPT_DIR}/lib/cleanup-vault.sh" \
      "${CLEANUP_ITERATIONS}" "${CLEANUP_DOWNLOAD_WAIT}" "${CLEANUP_DELETE_WAIT}" \
      > "${log_path}" 2>&1
  else
    bash "${SCRIPT_DIR}/lib/cleanup-vault-api.sh" > "${log_path}" 2>&1
  fi
}

# --------------------------------------------------------------------------- #
# Run cases (cleanup before each one)
log ""
log "===== running cases 01-16 (cleanup before each) ====="

declare -a RESULTS
PASS=0
FAIL=0
BLOCKED=0
CASE_GLOB=( "${SMOKE_DIR}"/cases/0[1-9]-*.sh "${SMOKE_DIR}"/cases/1[0-6]-*.sh )

for c in "${CASE_GLOB[@]}"; do
  [ -f "$c" ] || continue
  name=$(basename "$c" .sh)
  case_log="${SUITE_DIR}/${name}.log"
  cleanup_log="${SUITE_DIR}/${name}-cleanup.log"

  if [ "${DO_CLEANUP}" -eq 1 ]; then
    log "  cleanup before ${name}…"
    if ! run_cleanup "${cleanup_log}"; then
      log "  cleanup PARTIAL (continuing; see ${cleanup_log})"
    fi
  fi

  log "→ ${name}"
  set +e
  bash "$c" > "${case_log}" 2>&1
  status=$?
  set -e
  if [ "${status}" -eq 0 ] && grep -q "PASS$" "${case_log}"; then
    log "  PASS"
    RESULTS+=("PASS|${name}")
    PASS=$((PASS + 1))
  elif [ "${status}" -eq 78 ] || grep -q "BLOCKED:" "${case_log}"; then
    log "  BLOCKED — tail: $(tail -1 "${case_log}")"
    RESULTS+=("BLOCKED|${name}")
    BLOCKED=$((BLOCKED + 1))
  else
    log "  FAIL — tail: $(tail -1 "${case_log}")"
    RESULTS+=("FAIL|${name}")
    FAIL=$((FAIL + 1))
  fi
done

# --------------------------------------------------------------------------- #
# Step 3: write summary

{
  echo "# Smoke suite run ${STAMP}"
  echo ""
  echo "- API: \`${SYNC_API}\`"
  echo "- Vault: \`$(unique_vault_name)\`"
  echo "- Cleanup: **$([ "${DO_CLEANUP}" -eq 1 ] && echo "per-case API" || echo "SKIPPED")**"
  echo "- Cases: **${PASS} PASS / ${FAIL} FAIL / ${BLOCKED} BLOCKED** of $((PASS + FAIL + BLOCKED))"
  echo ""
  echo "| Status | Case | Artifact |"
  echo "|--------|------|----------|"
  for row in "${RESULTS[@]}"; do
    IFS='|' read -r status name <<< "$row"
    echo "| ${status} | ${name} | \`${name}.log\` |"
  done
} > "${SUMMARY}"

log ""
log "summary: ${PASS} PASS / ${FAIL} FAIL / ${BLOCKED} BLOCKED"
log "summary file: ${SUMMARY}"

[ "${FAIL}" -eq 0 ] && [ "${BLOCKED}" -eq 0 ]
