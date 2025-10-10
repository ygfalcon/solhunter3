#!/usr/bin/env bash
# Shared helpers for preflight smoke tests.

set -o pipefail

RED="\033[31m"
GRN="\033[32m"
YLW="\033[33m"
BLU="\033[34m"
BOLD="\033[1m"
RESET="\033[0m"

log() {
  printf "%b[%s]%b %s\n" "$BLU" "$1" "$RESET" "${*:2}"
}

pass() {
  printf "%b%s%b\n" "$GRN" "$*" "$RESET"
}

warn() {
  printf "%b%s%b\n" "$YLW" "$*" "$RESET"
}

fail() {
  printf "%b%s%b\n" "$RED" "$*" "$RESET" >&2
}

abort() {
  fail "$*"
  exit 1
}

require_env() {
  local name=$1
  if [[ -z ${!name:-} ]]; then
    fail "Environment variable $name is not set"
    return 1
  fi
  return 0
}

require_command() {
  local cmd=$1
  if ! command -v "$cmd" >/dev/null 2>&1; then
    fail "Required command '$cmd' not found in PATH"
    return 1
  fi
  return 0
}

run_with_timeout() {
  local timeout=$1
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$timeout" "$@"
  else
    "$@"
  fi
}

redis() {
  if [[ -z ${REDIS_URL:-} ]]; then
    abort "REDIS_URL is required for redis-cli invocations"
  fi
  redis-cli -u "$REDIS_URL" "$@"
}

latest_stream_value() {
  local stream=$1
  redis --raw XREVRANGE "$stream" + - COUNT 1 2>/dev/null | tail -n 1
}

await_stream_json() {
  local stream=$1
  local jq_filter=$2
  local timeout=${3:-10}
  local interval=${4:-0.5}
  local start=$SECONDS
  local deadline=$((start + timeout))
  while (( SECONDS <= deadline )); do
    local payload
    payload=$(latest_stream_value "$stream")
    if [[ -n $payload ]]; then
      if jq -e "$jq_filter" <<<"$payload" >/dev/null 2>&1; then
        printf '%s' "$payload"
        return 0
      fi
    fi
    sleep "$interval"
  done
  return 1
}

unique_suffix() {
  date +%s%N
}

#########################
# Redis cleanup helpers #
#########################

declare -a _PREFLIGHT_REDIS_CLEANUP_CMDS=()

register_stream_cleanup() {
  local stream=$1
  local id=$2
  _PREFLIGHT_REDIS_CLEANUP_CMDS+=("XDEL $stream $id")
}

register_key_cleanup() {
  local key=$1
  _PREFLIGHT_REDIS_CLEANUP_CMDS+=("DEL $key")
}

cleanup_redis_artifacts() {
  local cmd
  for cmd in "${_PREFLIGHT_REDIS_CLEANUP_CMDS[@]}"; do
    # shellcheck disable=SC2086
    redis $cmd >/dev/null 2>&1 || true
  done
}

if [[ -z ${PREFLIGHT_CLEANUP_REGISTERED:-} ]]; then
  PREFLIGHT_CLEANUP_REGISTERED=1
  trap cleanup_redis_artifacts EXIT
fi

redis_publish_json() {
  local stream=$1
  local json_payload=$2
  local field=${3:-json}
  local id
  if ! id=$(redis --raw XADD "$stream" '*' "$field" "$json_payload"); then
    return 1
  fi
  register_stream_cleanup "$stream" "$id"
  printf '%s' "$id"
}

redis_set_temp() {
  local key=$1
  local ttl=$2
  local value=$3
  if redis SETEX "$key" "$ttl" "$value" >/dev/null; then
    register_key_cleanup "$key"
    return 0
  fi
  return 1
}

################################
# Audit/event recording helpers #
################################

PREFLIGHT_CAN_AUDIT=0
if command -v jq >/dev/null 2>&1; then
  PREFLIGHT_CAN_AUDIT=1
fi

init_audit() {
  local script_name=$1
  AUDIT_SCRIPT_NAME=$script_name
  AUDIT_EVENTS=()
  AUDIT_START_EPOCH=$(date +%s)
  AUDIT_START_ISO=$(date -Is)
}

record_audit() {
  [[ $PREFLIGHT_CAN_AUDIT -eq 1 ]] || return 0
  local status=$1
  local message=$2
  local data=${3:-}
  local ts
  ts=$(date -Is)
  local event
  if [[ -n $data ]]; then
    event=$(jq -n --arg status "$status" --arg message "$message" --arg ts "$ts" --argjson data "$data" '{status:$status,message:$message,ts:$ts,data:$data}')
  else
    event=$(jq -n --arg status "$status" --arg message "$message" --arg ts "$ts" '{status:$status,message:$message,ts:$ts}')
  fi
  AUDIT_EVENTS+=("$event")
}

emit_audit() {
  [[ $PREFLIGHT_CAN_AUDIT -eq 1 ]] || return 0
  local final_status=$1
  local extra_json=${2:-}
  local end_epoch=$(date +%s)
  local end_iso
  end_iso=$(date -Is)
  local duration=$((end_epoch - AUDIT_START_EPOCH))
  local events_json="[]"
  if (( ${#AUDIT_EVENTS[@]} )); then
    events_json=$(printf '%s\n' "${AUDIT_EVENTS[@]}" | jq -s '.')
  fi
  local env_block
  env_block=$(jq -n \
    --arg mode "${MODE:-}" \
    --arg micro "${MICRO_MODE:-}" \
    --arg redis "${REDIS_URL:-}" \
    --arg script "$AUDIT_SCRIPT_NAME" \
    '{mode:$mode,micro_mode:$micro,redis_url:$redis,script:$script}')
  local commit
  commit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
  local audit_json
  if [[ -n $extra_json ]]; then
    audit_json=$(jq -n \
      --arg script "$AUDIT_SCRIPT_NAME" \
      --arg status "$final_status" \
      --arg start "$AUDIT_START_ISO" \
      --arg end "$end_iso" \
      --arg commit "$commit" \
      --arg host "$(hostname)" \
      --argjson env "$env_block" \
      --argjson events "$events_json" \
      --argjson extra "$extra_json" \
      --argjson duration "$duration" \
      '{script:$script,status:$status,start:$start,end:$end,duration_sec:$duration,commit:$commit,host:$host,env:$env,events:$events,extra:$extra}')
  else
    audit_json=$(jq -n \
      --arg script "$AUDIT_SCRIPT_NAME" \
      --arg status "$final_status" \
      --arg start "$AUDIT_START_ISO" \
      --arg end "$end_iso" \
      --arg commit "$commit" \
      --arg host "$(hostname)" \
      --argjson env "$env_block" \
      --argjson events "$events_json" \
      --argjson duration "$duration" \
      '{script:$script,status:$status,start:$start,end:$end,duration_sec:$duration,commit:$commit,host:$host,env:$env,events:$events}')
  fi

  local out_dir=${PREFLIGHT_AUDIT_DIR:-}
  local out_path=${PREFLIGHT_AUDIT_PATH:-}
  if [[ -z $out_path && -n $out_dir ]]; then
    mkdir -p "$out_dir"
    local ts
    ts=$(date -u +"%Y%m%dT%H%M%SZ")
    out_path="$out_dir/${AUDIT_SCRIPT_NAME}_${ts}_$(unique_suffix).json"
  fi
  if [[ -n $out_path ]]; then
    printf '%s\n' "$audit_json" >"$out_path"
    log INFO "audit saved to $out_path"
  else
    printf '%s\n' "$audit_json"
  fi
}
