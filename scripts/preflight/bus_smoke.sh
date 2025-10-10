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
  local -a check_details=()
  log INFO "solhunter preflight: bus smoke"
  init_audit bus_smoke

  require_env REDIS_URL || failures=$((failures + 1))
  require_command redis-cli || failures=$((failures + 1))

  printf '\n'
  log INFO "stream append/read checks"
  for stream in "${STREAMS[@]}"; do
    local marker="preflight-$(unique_suffix)"
    local payload
    payload=$(jq -n -c --arg marker "$marker" --arg stream "$stream" --arg ts "$(date -Is)" '{marker:$marker,stream:$stream,ts:$ts}')
    local id
    if ! id=$(redis_publish_json "$stream" "$payload" json); then
      fail "stream $stream: unable to append"
      failures=$((failures + 1))
      check_details+=("$(jq -n --arg stream "$stream" '{type:"stream",name:$stream,status:"fail",reason:"append"}')")
      record_audit fail "stream:$stream:append"
      continue
    fi

    local latest
    latest=$(latest_stream_value "$stream")
    if [[ -n $latest && $latest == *"$marker"* ]]; then
      pass "stream $stream"
      check_details+=("$(jq -n --arg stream "$stream" '{type:"stream",name:$stream,status:"pass"}')")
      record_audit pass "stream:$stream"
    else
      fail "stream $stream: append/read mismatch"
      failures=$((failures + 1))
      check_details+=("$(jq -n --arg stream "$stream" '{type:"stream",name:$stream,status:"fail",reason:"mismatch"}')")
      record_audit fail "stream:$stream:mismatch"
    fi
  done

  printf '\n'
  log INFO "key-value TTL sanity"
  local ttl_key="preflight:cursor:$(unique_suffix)"
  if redis_set_temp "$ttl_key" 30 smoke; then
    local ttl
    ttl=$(redis TTL "$ttl_key")
    if [[ $ttl -le 30 && $ttl -gt 0 ]]; then
      pass "TTL bounded ($ttl s)"
      check_details+=("$(jq -n --arg key "$ttl_key" --argjson ttl "$ttl" '{type:"kv",key:$key,status:"pass",ttl:$ttl}')")
      record_audit pass "ttl:$ttl_key"
    else
      fail "TTL unexpected value ($ttl)"
      failures=$((failures + 1))
      check_details+=("$(jq -n --arg key "$ttl_key" --argjson ttl "$ttl" '{type:"kv",key:$key,status:"fail",ttl:$ttl}')")
      record_audit fail "ttl:$ttl_key"
    fi
  else
    fail "unable to set TTL key"
    failures=$((failures + 1))
    check_details+=("$(jq -n --arg key "$ttl_key" '{type:"kv",key:$key,status:"fail",reason:"set"}')")
    record_audit fail "ttl:$ttl_key:set"
  fi

  local extra_json
  if (( ${#check_details[@]} )); then
    extra_json=$(printf '%s\n' "${check_details[@]}" | jq -s '{checks:.}')
  else
    extra_json='{"checks":[]}'
  fi

  printf '\n'
  if ((failures == 0)); then
    pass "BUS SMOKE: PASS"
    emit_audit pass "$extra_json"
  else
    fail "BUS SMOKE: FAIL ($failures issues)"
    emit_audit fail "$extra_json"
    exit 2
  fi
}

main "$@"
