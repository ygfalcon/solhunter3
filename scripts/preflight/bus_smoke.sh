#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/preflight/common.sh
source "$SCRIPT_DIR/common.sh"

STREAMS=(
  x:discovery.candidates
  x:token.snap
  x:market.ohlcv.5m
  x:market.depth
  x:mint.golden
  x:trade.suggested
  x:vote.decisions
  x:virt.fills
  x:live.fills
)

main() {
  local failures=0
  local -a stream_entries=()
  local ttl_key="preflight:cursor:$(unique_suffix)"

  cleanup() {
    for entry in "${stream_entries[@]}"; do
      IFS='|' read -r stream id <<<"$entry"
      if [[ -n $stream && -n $id ]]; then
        redis XDEL "$stream" "$id" >/dev/null 2>&1 || true
      fi
    done
    if [[ -n $ttl_key ]]; then
      redis DEL "$ttl_key" >/dev/null 2>&1 || true
    fi
  }
  trap cleanup EXIT

  log INFO "solhunter preflight: bus smoke"

  require_env REDIS_URL || failures=$((failures + 1))
  require_command redis-cli || failures=$((failures + 1))

  printf '\n'
  log INFO "stream append/read checks"
  for stream in "${STREAMS[@]}"; do
    local marker="preflight-$(unique_suffix)"
    local id
    if ! id=$(redis --raw XADD "$stream" * smoke "$marker" 2>/dev/null); then
      fail "stream $stream: unable to append"
      failures=$((failures + 1))
      continue
    fi
    stream_entries+=("$stream|$id")

    local latest
    latest=$(latest_stream_value "$stream")
    if [[ -n $latest && $latest == *"$marker"* ]]; then
      pass "stream $stream"
    else
      fail "stream $stream: append/read mismatch"
      failures=$((failures + 1))
    fi
  done

  printf '\n'
  log INFO "key-value TTL sanity"
  if redis SETEX "$ttl_key" 30 smoke >/dev/null; then
    local ttl
    ttl=$(redis TTL "$ttl_key")
    if [[ $ttl -le 30 && $ttl -gt 0 ]]; then
      pass "TTL bounded ($ttl s)"
    else
      fail "TTL unexpected value ($ttl)"
      failures=$((failures + 1))
    fi
  else
    fail "unable to set TTL key"
    failures=$((failures + 1))
  fi

  printf '\n'
  if ((failures == 0)); then
    pass "BUS SMOKE: PASS"
  else
    fail "BUS SMOKE: FAIL ($failures issues)"
    exit 2
  fi
}

main "$@"
