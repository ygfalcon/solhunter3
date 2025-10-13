#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/preflight/common.sh
source "$SCRIPT_DIR/common.sh"

main() {
  local failures=0
  local -a check_details=()

  log INFO "solhunter preflight: environment doctor"
  init_audit env_doctor

  for cmd in bash jq redis-cli curl; do
    if require_command "$cmd"; then
      pass "command: $cmd"
      check_details+=("$(jq -n --arg name "$cmd" '{type:"command",name:$name,status:"pass"}')")
      record_audit pass "command:$cmd"
    else
      ((failures++))
      check_details+=("$(jq -n --arg name "$cmd" '{type:"command",name:$name,status:"fail"}')")
      record_audit fail "command:$cmd"
    fi
  done

  local required_envs=(MODE NEW_DAS_DISCOVERY EXIT_FEATURES_ON RL_WEIGHTS_DISABLED MICRO_MODE REDIS_URL SOLANA_RPC_URL HELIUS_API_KEY)
  for env_name in "${required_envs[@]}"; do
    if require_env "$env_name"; then
      pass "env: $env_name=${!env_name}"
      check_details+=("$(jq -n --arg name "$env_name" --arg value "${!env_name}" '{type:"env",name:$name,value:$value,status:"pass"}')")
      record_audit pass "env:$env_name"
    else
      ((failures++))
      check_details+=("$(jq -n --arg name "$env_name" '{type:"env",name:$name,status:"fail"}')")
      record_audit fail "env:$env_name"
    fi
  done

  printf '\n'
  log INFO "checking Solana RPC health"
  if [[ ${SOLANA_RPC_URL:-} ]]; then
    if run_with_timeout 5 curl -sS -X POST "$SOLANA_RPC_URL" -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' \
      | jq -re '.result or .error' >/dev/null; then
      pass "RPC reachable"
      check_details+=("$(jq -n '{type:"rpc",status:"pass"}')")
      record_audit pass "rpc:getHealth"
    else
      fail "RPC health probe failed"
      check_details+=("$(jq -n '{type:"rpc",status:"fail"}')")
      record_audit fail "rpc:getHealth"
      ((failures++))
    fi
  fi

  printf '\n'
  log INFO "checking Helius DAS proxy"
  if [[ ${HELIUS_API_KEY:-} ]]; then
    local helius_key_lc
    helius_key_lc=$(printf '%s' "$HELIUS_API_KEY" | tr '[:upper:]' '[:lower:]')
    if [[ $helius_key_lc == skip ]]; then
      warn "DAS probe skipped (HELIUS_API_KEY=skip)"
      check_details+=("$(jq -n '{type:"das",status:"skipped"}')")
      record_audit warn "helius:skip"
    else
      local das_url="https://api.helius.xyz/v0/addresses/tokens?api-key=${HELIUS_API_KEY}&addresses=So11111111111111111111111111111111111111112"
      if run_with_timeout 5 curl -sS "$das_url" | jq -re '
        try (((if type == "array" then (.[0]? | .address? // .mint?)
          elif type == "object" then (
            .address? // .mint? //
            ((.items? // .result? // .data? // .tokens? // []) |
              (if type=="array" then (.[0]? | .address? // .mint?) else empty end))
          )
          else empty end) // empty)) catch empty
      ' >/dev/null; then
        pass "DAS reachable"
        check_details+=("$(jq -n '{type:"das",status:"pass"}')")
        record_audit pass "helius:tokens"
      else
        fail "DAS probe failed"
        check_details+=("$(jq -n '{type:"das",status:"fail"}')")
        record_audit fail "helius:tokens"
        ((failures++))
      fi
    fi
  fi

  printf '\n'
  log INFO "checking Redis availability"
  if redis PING 2>/dev/null | grep -q PONG; then
    pass "Redis ping"
    check_details+=("$(jq -n '{type:"redis",status:"pass"}')")
    record_audit pass "redis:PING"
  else
    fail "Redis ping failed"
    check_details+=("$(jq -n '{type:"redis",status:"fail"}')")
    record_audit fail "redis:PING"
    ((failures++))
  fi

  printf '\n'
  local check_ui=${CHECK_UI_HEALTH:-0}
  if [[ $check_ui == 1 || -n ${UI_HEALTH_URL:-} ]]; then
    local ui_url=${UI_HEALTH_URL:-http://127.0.0.1:3000/healthz}
    log INFO "checking UI health at $ui_url"
    if run_with_timeout 5 curl -fsS "$ui_url" | jq -e '.status? or .ok? or true' >/dev/null 2>&1; then
      pass "UI health ok"
      check_details+=("$(jq -n --arg url "$ui_url" '{type:"ui",url:$url,status:"pass"}')")
      record_audit pass "ui:$ui_url"
    else
      warn "UI health probe failed"
      check_details+=("$(jq -n --arg url "$ui_url" '{type:"ui",url:$url,status:"fail"}')")
      record_audit fail "ui:$ui_url"
      ((failures++))
    fi
  fi

  local extra_json
  if (( ${#check_details[@]} )); then
    extra_json=$(printf '%s\n' "${check_details[@]}" | jq -s '{checks:.}')
  else
    extra_json='{"checks":[]}'
  fi

  printf '\n'
  if ((failures == 0)); then
    pass "ENV DOCTOR: PASS"
    emit_audit pass "$extra_json"
  else
    fail "ENV DOCTOR: FAIL ($failures issues)"
    emit_audit fail "$extra_json"
    exit 1
  fi
}

main "$@"
