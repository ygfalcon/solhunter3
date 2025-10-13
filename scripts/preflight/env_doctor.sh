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

  local defaults=(
    NEW_DAS_DISCOVERY=1
    EXIT_FEATURES_ON=1
    RL_WEIGHTS_DISABLED=0
  )

  local kv name value
  for kv in "${defaults[@]}"; do
    name=${kv%%=*}
    value=${kv#*=}
    if [[ -z ${!name:-} ]]; then
      export "$name=$value"
      log INFO "defaulting $name=$value"
    fi
  done

  local required_envs=(
    MODE
    MICRO_MODE
    REDIS_URL
    SOLANA_RPC_URL
    HELIUS_API_KEY
    NEW_DAS_DISCOVERY
    EXIT_FEATURES_ON
    RL_WEIGHTS_DISABLED
  )
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
  if [[ ${HELIUS_API_KEY:-} || ${HELIUS_API_KEYS:-} || ${HELIUS_API_TOKEN:-} ]]; then
    local helius_key="${HELIUS_API_KEY:-}"
    if [[ -z $helius_key && -n ${HELIUS_API_KEYS:-} ]]; then
      helius_key=${HELIUS_API_KEYS%%,*}
    fi
    if [[ -z $helius_key && -n ${HELIUS_API_TOKEN:-} ]]; then
      helius_key=$HELIUS_API_TOKEN
    fi
    local helius_key_lc
    helius_key_lc=$(printf '%s' "$helius_key" | tr '[:upper:]' '[:lower:]')
    if [[ $helius_key_lc == skip ]]; then
      warn "DAS probe skipped (HELIUS_API_KEY=skip)"
      check_details+=("$(jq -n '{type:"das",status:"skipped"}')")
      record_audit warn "helius:skip"
    elif [[ -n $helius_key ]]; then
      local das_base=${DAS_RPC_URL:-${HELIUS_DAS_RPC_URL:-https://mainnet.helius-rpc.com}}
      das_base=${das_base%/}
      local das_url=$das_base
      if [[ $das_url != *"api-key="* ]]; then
        local delimiter='?'
        if [[ $das_url == *"?"* ]]; then
          delimiter='&'
        fi
        das_url="${das_url}${delimiter}api-key=${helius_key}"
      fi
      local das_payload='{"jsonrpc":"2.0","id":"env-doctor","method":"searchAssets","params":{"tokenType":"fungible","page":1,"limit":1,"sortBy":"created","sortDirection":"desc"}}'
      if run_with_timeout 5 curl -fsS -X POST "$das_url" \
        -H "Content-Type: application/json" \
        -d "$das_payload" | jq -re '
          def items_from:
            if type == "array" then .
            elif type == "object" then (
              (.items? // empty | select(type == "array")),
              (.tokens? // empty | select(type == "array")),
              (.assets? // empty | select(type == "array")),
              (.result? | items_from)
            )
            else empty end;
          .result? as $result | ($result | items_from) | length > 0
        ' >/dev/null; then
        pass "DAS reachable (JSON-RPC)"
        check_details+=("$(jq -n '{type:"das",status:"pass"}')")
        record_audit pass "helius:jsonrpc"
      else
        fail "DAS probe failed (JSON-RPC)"
        check_details+=("$(jq -n '{type:"das",status:"fail"}')")
        record_audit fail "helius:jsonrpc"
        ((failures++))
      fi
    else
      fail "DAS probe failed (no usable API key)"
      check_details+=("$(jq -n '{type:"das",status:"fail"}')")
      record_audit fail "helius:missing-key"
      ((failures++))
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
