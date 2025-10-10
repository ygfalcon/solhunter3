#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/preflight/common.sh
source "$SCRIPT_DIR/common.sh"

main() {
  local failures=0
  local rpc_ok=false
  local das_ok=false
  local redis_ok=false
  local ui_health_ok=false
  local ui_shadow_ok=false
  local artifact_dir
  artifact_dir=$(ensure_artifact_dir)

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
      rpc_ok=true
    else
      fail "RPC health probe failed"
      ((failures++))
    fi
  fi

  printf '\n'
  log INFO "checking Helius DAS proxy"
  if [[ ${HELIUS_API_KEY:-} ]]; then
    local das_url="https://api.helius.xyz/v0/addresses/tokens?api-key=${HELIUS_API_KEY}&addresses=So1111111111111111111111111111111111111112"
    if run_with_timeout 5 curl -sS "$das_url" | jq -re '.[0].address' >/dev/null; then
      pass "DAS reachable"
      das_ok=true
    else
      fail "DAS probe failed"
      ((failures++))
    fi
  fi

  printf '\n'
  log INFO "checking Redis availability"
  if redis PING 2>/dev/null | grep -q PONG; then
    pass "Redis ping"
    redis_ok=true
  else
    fail "Redis ping failed"
    ((failures++))
  fi

  printf '\n'
  log INFO "checking UI health endpoints"
  local ui_base="${UI_BASE_URL:-http://127.0.0.1:${UI_PORT:-5001}}"
  local ui_health_url="${UI_HEALTH_URL:-$ui_base/health}"
  if run_with_timeout 5 curl -sS "$ui_health_url" | jq -re '.ok == true' >/dev/null; then
    pass "UI health"
    ui_health_ok=true
  else
    warn "UI health check failed"
  fi

  local ui_shadow_url="${UI_SHADOW_URL:-$ui_base/swarm/shadow}"
  if payload=$(run_with_timeout 5 curl -sS "$ui_shadow_url" 2>/dev/null); then
    if jq -e '.virtual_fills | type == "array"' <<<"$payload" >/dev/null 2>&1; then
      local stale_count
      stale_count=$(jq '[.virtual_fills[] | select(.stale == true)] | length' <<<"$payload" 2>/dev/null || echo 0)
      if [[ ${stale_count:-0} -eq 0 ]]; then
        pass "UI shadow panel fresh"
        ui_shadow_ok=true
      else
        warn "UI shadow panel reports $stale_count stale entries"
      fi
    fi
  fi

  printf '\n'
  if ((failures == 0)); then
    pass "ENV DOCTOR: PASS"
  else
    fail "ENV DOCTOR: FAIL ($failures issues)"
    exit 1
  fi

  local timestamp
  timestamp=$(date -Iseconds)
  cat >"$artifact_dir/env_report.json" <<JSON
{
  "ts": "${timestamp}",
  "commit": "${PRE_FLIGHT_COMMIT}",
  "mode": "${MODE:-}",
  "micro_mode": "${MICRO_MODE:-}",
  "rpc_ok": ${rpc_ok},
  "das_ok": ${das_ok},
  "redis_ok": ${redis_ok},
  "ui_health_ok": ${ui_health_ok},
  "ui_shadow_fresh": ${ui_shadow_ok}
}
JSON
}

main "$@"
