#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../.." && pwd)

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for run_all.sh" >&2
  exit 1
fi

# shellcheck source=scripts/preflight/common.sh
source "$SCRIPT_DIR/common.sh"

unique_suffix() {
  # %N is not portable on macOS/BSD; fall back to $$ if unsupported
  if suffix=$(date +%s%N 2>/dev/null) && [[ $suffix != *N ]]; then
    printf '%s\n' "$suffix"
  else
    printf '%s\n' "$(date +%s)_$$"
  fi
}

AUDIT_DIR=${PREFLIGHT_AUDIT_DIR:-$ROOT_DIR/artifacts/preflight}
HISTORY_FILE=${PREFLIGHT_HISTORY_FILE:-$AUDIT_DIR/history.jsonl}
READY_MARKER=${PREFLIGHT_READY_MARKER:-$AUDIT_DIR/READY}
# Accept correctly spelled env var, keep backwards-compat with the old typo
READY_SPACING_SEC=${PREFLIGHT_READINESS_SPACING_SEC:-${PREFLIGHT_READYNESS_SPACING_SEC:-600}}

mkdir -p "$AUDIT_DIR"

run_with_audit() {
  local label=$1
  shift
  local script=$1
  shift
  local ts
  ts=$(date -u +"%Y%m%dT%H%M%SZ")
  local audit_path="$AUDIT_DIR/${label}_${ts}_$(unique_suffix).json"
  if PREFLIGHT_AUDIT_PATH="$audit_path" "$script" "$@"; then
    printf '%s\n' "$audit_path"
    return 0
  else
    printf '%s\n' "$audit_path"
    return 1
  fi
}

main() {
  export NEW_DAS_DISCOVERY=${NEW_DAS_DISCOVERY:-1}
  export EXIT_FEATURES_ON=${EXIT_FEATURES_ON:-1}
  export RL_WEIGHTS_DISABLED=${RL_WEIGHTS_DISABLED:-1}

  if [[ -z ${HELIUS_API_KEY:-} ]]; then
    local env_file="$ROOT_DIR/etc/solhunter/env.production"
    if [[ -f $env_file ]]; then
      local helius_key
      helius_key=$(grep -E '^HELIUS_API_KEY=' "$env_file" | tail -n 1 | cut -d= -f2-)
      if [[ -n $helius_key ]]; then
        export HELIUS_API_KEY="$helius_key"
      fi
    fi
  fi

  local env_audit
  local env_status="pass"
  if ! env_audit=$(run_with_audit env_doctor "$SCRIPT_DIR/env_doctor.sh"); then
    env_status="fail"
  fi

  local bus_audit
  local bus_status="pass"
  if ! bus_audit=$(run_with_audit bus_smoke "$SCRIPT_DIR/bus_smoke.sh"); then
    bus_status="fail"
  fi

  local -a preflight_runs=()
  local modes=(1 0)
  local original_micro=${MICRO_MODE:-}
  for mode in "${modes[@]}"; do
    local mode_label
    if [[ $mode == 1 ]]; then
      mode_label="micro_on"
    else
      mode_label="micro_off"
    fi
    local ts
    ts=$(date -u +"%Y%m%dT%H%M%SZ")
    local log_path="$AUDIT_DIR/preflight_${mode_label}_${ts}.log"
    local audit_path="$AUDIT_DIR/preflight_${mode_label}_${ts}_$(unique_suffix).json"
    local status="pass"
    if ! MICRO_MODE=$mode PREFLIGHT_AUDIT_PATH="$audit_path" "$SCRIPT_DIR/preflight_smoke.sh" | tee "$log_path"; then
      status="fail"
    fi
    preflight_runs+=("$(jq -n --arg mode "$mode_label" --arg status "$status" --arg audit "$audit_path" --arg log "$log_path" '{mode:$mode,status:$status,audit:$audit,log:$log}')")
  done
  if [[ -n ${original_micro:-} ]]; then
    export MICRO_MODE=$original_micro
  else
    unset MICRO_MODE || true
  fi

  local preflight_json='[]'
  if (( ${#preflight_runs[@]} )); then
    preflight_json=$(printf '%s\n' "${preflight_runs[@]}" | jq -s '.')
  fi

  local overall_status
  overall_status=$(jq -n --arg env "$env_status" --arg bus "$bus_status" --argjson runs "$preflight_json" '
    def runs_all_pass: (runs | length) == 0 or (runs | all(.[]; .status == "pass"));
    if ($env == "pass" and $bus == "pass" and runs_all_pass) then "pass" else "fail" end
  ')

  local timestamp
  timestamp=$(iso_timestamp)
  local epoch
  epoch=$(date +%s)
  local commit
  commit=$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)

  local history_entry
  history_entry=$(jq -n \
    --arg ts "$timestamp" \
    --argjson epoch "$epoch" \
    --arg commit "$commit" \
    --arg status "$overall_status" \
    --arg env_status "$env_status" \
    --arg env_audit "${env_audit:-}" \
    --arg bus_status "$bus_status" \
    --arg bus_audit "${bus_audit:-}" \
    --argjson preflight "$preflight_json" \
    '{ts:$ts,epoch:$epoch,commit:$commit,status:$status,env:{status:$env_status,audit:($env_audit // null)},bus:{status:$bus_status,audit:($bus_audit // null)},preflight:$preflight}')

  printf '%s\n' "$history_entry" >> "$HISTORY_FILE"
  log INFO "History appended to $HISTORY_FILE"

  if [[ $overall_status == "pass" ]]; then
    local readiness_json
    readiness_json=$(jq -s --argjson spacing "$READY_SPACING_SEC" '
      map(select(.status == "pass"))
      | sort_by(.epoch)
      | if length < 2 then empty else {previous:.[-2],current:.[-1],delta_sec:(.[-1].epoch - .[-2].epoch)} end
      | select(.delta_sec >= $spacing)
    ' "$HISTORY_FILE")
    if [[ -n $readiness_json ]]; then
      printf '%s\n' "$readiness_json" > "$READY_MARKER"
      log INFO "Readiness marker updated: $READY_MARKER"
    else
      rm -f "$READY_MARKER"
    fi
  else
    rm -f "$READY_MARKER"
  fi

  if [[ $overall_status != "pass" ]]; then
    exit 1
  fi
}

main "$@"
