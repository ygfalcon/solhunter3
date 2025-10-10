#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/preflight/common.sh
source "$SCRIPT_DIR/common.sh"

main() {
  local failures=0
  log INFO "solhunter preflight: environment doctor"

  for cmd in bash jq redis-cli curl; do
    if require_command "$cmd"; then
      pass "command: $cmd"
    else
      ((failures++))
    fi
  done

  local required_envs=(MODE NEW_DAS_DISCOVERY EXIT_FEATURES_ON RL_WEIGHTS_DISABLED MICRO_MODE REDIS_URL SOLANA_RPC_URL HELIUS_API_KEY)
  for env_name in "${required_envs[@]}"; do
    if require_env "$env_name"; then
      pass "env: $env_name=${!env_name}"
    else
      ((failures++))
    fi
  done

  printf '\n'
  log INFO "checking Solana RPC health"
  if [[ ${SOLANA_RPC_URL:-} ]]; then
    if run_with_timeout 5 curl -sS -X POST "$SOLANA_RPC_URL" -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' \
      | jq -re '.result or .error' >/dev/null; then
      pass "RPC reachable"
    else
      fail "RPC health probe failed"
      ((failures++))
    fi
  fi

  printf '\n'
  log INFO "checking Helius DAS proxy"
  if [[ ${HELIUS_API_KEY:-} ]]; then
    local das_url="https://api.helius.xyz/v0/addresses/tokens?api-key=${HELIUS_API_KEY}&addresses=So11111111111111111111111111111111111111112"
    if run_with_timeout 5 curl -sS "$das_url" | jq -re '.[0].address' >/dev/null; then
      pass "DAS reachable"
    else
      fail "DAS probe failed"
      ((failures++))
    fi
  fi

  printf '\n'
  log INFO "checking Redis availability"
  if redis PING 2>/dev/null | grep -q PONG; then
    pass "Redis ping"
  else
    fail "Redis ping failed"
    ((failures++))
  fi

  printf '\n'
  if ((failures == 0)); then
    pass "ENV DOCTOR: PASS"
  else
    fail "ENV DOCTOR: FAIL ($failures issues)"
    exit 1
  fi
}

main "$@"
