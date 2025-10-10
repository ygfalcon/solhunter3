#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/preflight/common.sh
source "$SCRIPT_DIR/common.sh"

PREFLIGHT_MINT=${PREFLIGHT_MINT:-So11111111111111111111111111111111111111112}
PREFLIGHT_SYMBOL=${PREFLIGHT_SYMBOL:-SOL}
PREFLIGHT_NAME=${PREFLIGHT_NAME:-Solana}
PREFLIGHT_VENUE=${PREFLIGHT_VENUE:-raydium}
PREFLIGHT_DECIMALS=${PREFLIGHT_DECIMALS:-9}
PREFLIGHT_NOTIONAL=${PREFLIGHT_NOTIONAL:-50}
PREFLIGHT_MAX_SPREAD_BPS=${PREFLIGHT_MAX_SPREAD_BPS:-25}
PREFLIGHT_MIN_DEPTH_1=${PREFLIGHT_MIN_DEPTH_1:-20000}
PREFLIGHT_MIN_DEPTH_5=${PREFLIGHT_MIN_DEPTH_5:-80000}
PREFLIGHT_BAD_SPREAD_BPS=${PREFLIGHT_BAD_SPREAD_BPS:-85}
PREFLIGHT_BAD_DEPTH_1=${PREFLIGHT_BAD_DEPTH_1:-3000}
PREFLIGHT_BAD_DEPTH_5=${PREFLIGHT_BAD_DEPTH_5:-9000}

JQ=${JQ:-jq}

publish_json() {
  local stream=$1
  local json_payload=$2
  redis XADD "$stream" * json "$json_payload" >/dev/null
}

await_new_stream_event() {
  local stream=$1
  local baseline_len=$2
  local jq_filter=$3
  local timeout=${4:-10}
  local deadline=$((SECONDS + timeout))
  while (( SECONDS <= deadline )); do
    local len
    len=$(redis XLEN "$stream")
    if (( len > baseline_len )); then
      local payload
      payload=$(latest_stream_value "$stream")
      if jq -e "$jq_filter" <<<"$payload" >/dev/null 2>&1; then
        printf '%s' "$payload"
        return 0
      fi
    fi
    sleep 1
  done
  return 1
}

main() {
  local failures=0
  require_command "$JQ" || failures=$((failures + 1))
  require_command redis-cli || failures=$((failures + 1))
  require_env REDIS_URL || failures=$((failures + 1))

  (( failures == 0 )) || abort "missing requirements"

  local now
  now=$(date +%s)

  log INFO "solhunter preflight: end-to-end smoke"
  printf '\n'
  log INFO "1) publishing base fixtures"

  local token_snapshot
  token_snapshot=$($JQ -n --arg mint "$PREFLIGHT_MINT" --arg symbol "$PREFLIGHT_SYMBOL" --arg name "$PREFLIGHT_NAME" \
    --arg program Tokenkeg --argjson decimals "$PREFLIGHT_DECIMALS" --argjson asof "$now" \
    '{mint:$mint,symbol:$symbol,name:$name,decimals:$decimals,token_program:$program,flags:{},asof:$asof}')
  publish_json x:token.snap "$token_snapshot"

  local ohlcv
  ohlcv=$($JQ -n --arg mint "$PREFLIGHT_MINT" --argjson o 150 --argjson h 152 --argjson l 149 --argjson c 151 \
    --argjson vol_usd 120000 --argjson trades 420 --argjson buyers 260 --argjson zret 2.1 --argjson zvol 2.6 --argjson asof "$now" \
    '{mint:$mint,o:$o,h:$h,l:$l,c:$c,vol_usd:$vol_usd,trades:$trades,buyers:$buyers,zret:$zret,zvol:$zvol,asof_close:$asof}')
  publish_json x:market.ohlcv.5m "$ohlcv"

  local depth
  depth=$($JQ -n --arg mint "$PREFLIGHT_MINT" --arg venue "$PREFLIGHT_VENUE" --argjson mid 151 --argjson spread 18 \
    --argjson depth1 "$PREFLIGHT_MIN_DEPTH_1" --argjson depth2 45000 --argjson depth5 "$PREFLIGHT_MIN_DEPTH_5" --argjson asof "$((now + 1))" \
    '{mint:$mint,venue:$venue,mid_usd:$mid,spread_bps:$spread,depth_pct:{"1":$depth1,"2":$depth2,"5":$depth5},asof:$asof}')
  publish_json x:market.depth "$depth"

  sleep 1

  local golden
  if golden=$(await_stream_json "x:mint.golden" "select(.mint == \"$PREFLIGHT_MINT\" and (.hash|type==\"string\" and (.hash|length>0)))" 15); then
    local golden_hash
    golden_hash=$($JQ -r '.hash' <<<"$golden")
    pass "Golden emitted with hash=$golden_hash"

    local golden_len_before
    golden_len_before=$(redis XLEN x:mint.golden)
    publish_json x:market.ohlcv.5m "$ohlcv"
    sleep 1
    local golden_len_after
    golden_len_after=$(redis XLEN x:mint.golden)
    if (( golden_len_after == golden_len_before )); then
      pass "Golden publish-on-change respected"
    else
      warn "Golden stream length increased (possible re-publication)"
    fi
  else
    fail "Golden snapshot not observed"
    exit 3
  fi

  printf '\n'
  log INFO "2) suggestion voting idempotency"
  local sugg
  sugg=$($JQ -n --arg agent momentum --arg mint "$PREFLIGHT_MINT" --arg side buy --argjson notional "$PREFLIGHT_NOTIONAL" \
    --argjson max_slippage 30 --argjson stop 400 --argjson take 120 --argjson time_stop 120 --argjson confidence 0.72 \
    --arg hash "$($JQ -r '.hash' <<<"$golden")" --argjson ttl 3 \
    '{agent:$agent,mint:$mint,side:$side,notional_usd:$notional,max_slippage_bps:$max_slippage,risk:{stop_bps:$stop,take_bps:$take,time_stop_sec:$time_stop},confidence:$confidence,inputs_hash:$hash,ttl_sec:$ttl}')
  local sugg_alt
  sugg_alt=$($JQ -n --arg agent conviction --arg mint "$PREFLIGHT_MINT" --arg side buy --argjson notional "$PREFLIGHT_NOTIONAL" \
    --argjson max_slippage 30 --argjson stop 400 --argjson take 120 --argjson time_stop 120 --argjson confidence 0.66 \
    --arg hash "$($JQ -r '.hash' <<<"$golden")" --argjson ttl 3 \
    '{agent:$agent,mint:$mint,side:$side,notional_usd:$notional,max_slippage_bps:$max_slippage,risk:{stop_bps:$stop,take_bps:$take,time_stop_sec:$time_stop},confidence:$confidence,inputs_hash:$hash,ttl_sec:$ttl}')

  local vote_len_before
  vote_len_before=$(redis XLEN x:vote.decisions)
  publish_json x:trade.suggested "$sugg"
  publish_json x:trade.suggested "$sugg_alt"

  local decision
  if decision=$(await_new_stream_event "x:vote.decisions" "$vote_len_before" "select(.mint == \"$PREFLIGHT_MINT\" and .side == \"buy\")" 15); then
    local coid
    coid=$($JQ -r '.clientOrderId // empty' <<<"$decision")
    if [[ -n $coid ]]; then
      pass "Decision emitted (clientOrderId=$coid)"
    else
      warn "Decision missing clientOrderId"
    fi
  else
    fail "No decision observed"
    exit 3
  fi

  local vote_len_after
  vote_len_after=$(redis XLEN x:vote.decisions)
  publish_json x:trade.suggested "$sugg"
  publish_json x:trade.suggested "$sugg_alt"
  sleep 2
  local vote_len_post
  vote_len_post=$(redis XLEN x:vote.decisions)
  if (( vote_len_post == vote_len_after )); then
    pass "Idempotency preserved (no duplicate decision)"
  else
    fail "Vote stream grew unexpectedly"
    exit 3
  fi

  printf '\n'
  log INFO "3) shadow executor virtual fills"
  local virt_len_before
  virt_len_before=$(redis XLEN x:virt.fills)
  publish_json x:vote.decisions "$($JQ -n --arg mint "$PREFLIGHT_MINT" --arg side buy --argjson notional "$PREFLIGHT_NOTIONAL" \
    --argjson score 0.7 --arg hash "$($JQ -r '.hash' <<<"$golden")" --arg coid "preflight-buy-$(unique_suffix)" --argjson ts "$((now + 2))" \
    '{mint:$mint,side:$side,notional_usd:$notional,score:$score,snapshot_hash:$hash,clientOrderId:$coid,agents:["momentum","conviction"],ts:$ts}')"

  local virt
  if virt=$(await_new_stream_event "x:virt.fills" "$virt_len_before" "select(.mint == \"$PREFLIGHT_MINT\" and .route == \"VIRTUAL\")" 15); then
    pass "Virtual fill observed"
  else
    fail "No virtual fill observed"
    exit 3
  fi

  printf '\n'
  log INFO "4) must-exit reflex"
  local shock_depth
  shock_depth=$($JQ -n --arg mint "$PREFLIGHT_MINT" --arg venue "$PREFLIGHT_VENUE" --argjson mid 149.4 --argjson spread 150 \
    --argjson depth1 1200 --argjson depth2 3000 --argjson depth5 6000 --argjson asof "$((now + 3))" \
    '{mint:$mint,venue:$venue,mid_usd:$mid,spread_bps:$spread,depth_pct:{"1":$depth1,"2":$depth2,"5":$depth5},asof:$asof}')
  local virt_len_pre_exit
  virt_len_pre_exit=$(redis XLEN x:virt.fills)
  publish_json x:market.depth "$shock_depth"
  local shock_start=$SECONDS
  local exit_fill
  if exit_fill=$(await_new_stream_event "x:virt.fills" "$virt_len_pre_exit" "select(.mint == \"$PREFLIGHT_MINT\" and .side == \"sell\")" 10); then
    local latency=$((SECONDS - shock_start))
    pass "Must-exit SELL observed (~${latency}s)"
  else
    fail "Must-exit SELL not observed within timeout"
    exit 3
  fi

  if [[ ${MICRO_MODE:-0} == 1 ]]; then
    printf '\n'
    log INFO "5) micro-mode entry guards"
    local bad_depth
    bad_depth=$($JQ -n --arg mint "$PREFLIGHT_MINT" --arg venue "$PREFLIGHT_VENUE" --argjson mid 151 --argjson spread "$PREFLIGHT_BAD_SPREAD_BPS" \
      --argjson depth1 "$PREFLIGHT_BAD_DEPTH_1" --argjson depth2 6000 --argjson depth5 "$PREFLIGHT_BAD_DEPTH_5" --argjson asof "$((now + 5))" \
      '{mint:$mint,venue:$venue,mid_usd:$mid,spread_bps:$spread,depth_pct:{"1":$depth1,"2":$depth2,"5":$depth5},asof:$asof}')
    publish_json x:market.depth "$bad_depth"
    local vote_len_baseline
    vote_len_baseline=$(redis XLEN x:vote.decisions)
    publish_json x:trade.suggested "$sugg"
    sleep 2
    local vote_len_final
    vote_len_final=$(redis XLEN x:vote.decisions)
    if (( vote_len_final == vote_len_baseline )); then
      pass "Micro-mode blocked new decision under poor liquidity"
    else
      fail "Micro-mode permitted decision despite guard"
      exit 3
    fi
  fi

  printf '\n'
  pass "PREFLIGHT: ALL CHECKS PASSED"
}

main "$@"
