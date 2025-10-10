#!/usr/bin/env bash
# Shared helpers for preflight smoke tests.

set -euo pipefail

RED="\033[31m"
GRN="\033[32m"
YLW="\033[33m"
BLU="\033[34m"
BOLD="\033[1m"
RESET="\033[0m"

: "${PRE_FLIGHT_COMMIT:=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"

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

now_ms() {
  date +%s%3N
}

ensure_artifact_dir() {
  local dir=${1:-"artifacts/preflight"}
  mkdir -p "$dir"
  printf '%s' "$dir"
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

