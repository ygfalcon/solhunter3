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
PREFLIGHT_BAD_SPREAD_BPS=${PREFLIGHT_BAD_SPREAD_BPS:-85}
PREFLIGHT_BAD_DEPTH_1=${PREFLIGHT_BAD_DEPTH_1:-3000}
PREFLIGHT_BAD_DEPTH_5=${PREFLIGHT_BAD_DEPTH_5:-9000}
PREFLIGHT_DUPLICATE_BURST=${PREFLIGHT_DUPLICATE_BURST:-10}

GOLDEN_SLO_MS=${GOLDEN_SLO_MS:-300}
DECISION_TO_FILL_SLO_MS=${DECISION_TO_FILL_SLO_MS:-150}
MUST_EXIT_SLO_MS=${MUST_EXIT_SLO_MS:-5000}

JQ=${JQ:-jq}

RUN_ID="preflight-$(unique_suffix)"
STREAM_INSERTS=()
ARTIFACT_DIR=$(ensure_artifact_dir)
ENV_REPORT_PATH="$ARTIFACT_DIR/env_report.json"
UI_BASE="${UI_BASE_URL:-http://127.0.0.1:${UI_PORT:-5001}}"

cleanup() {
  for entry in "${STREAM_INSERTS[@]}"; do
    IFS='|' read -r stream id <<<"$entry"
    if [[ -n $stream && -n $id ]]; then
      redis XDEL "$stream" "$id" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup EXIT

publish_json() {
  local stream=$1
  local payload=$2
  local id
  if ! id=$(redis --raw XADD "$stream" * json "$payload" 2>/dev/null); then
    abort "failed to publish to $stream"
  fi
  STREAM_INSERTS+=("$stream|$id")
  printf '%s' "$id"
}

await_stream_event() {
  local stream=$1
  local jq_filter=$2
  local timeout=${3:-15}
  local interval=${4:-0.5}
  local deadline=$((SECONDS + timeout))
  while (( SECONDS <= deadline )); do
    local raw
    raw=$(redis --raw XREVRANGE "$stream" + - COUNT 10 2>/dev/null || true)
    if [[ -n $raw ]]; then
      local IFS=$'\n'
      local lines=($raw)
      local total=${#lines[@]}
      for ((i=0; i+1<total; i+=2)); do
        local payload=${lines[i+1]}
        if [[ -z $payload ]]; then
          continue
        fi
        if $JQ -e "$jq_filter" <<<"$payload" >/dev/null 2>&1; then
          printf '%s' "$payload"
          return 0
        fi
      done
    fi
    sleep "$interval"
  done
  return 1
}

await_new_stream_event() {
  local stream=$1
  local baseline_len=$2
  local jq_filter=$3
  local timeout=${4:-15}
  local interval=${5:-0.5}
  local deadline=$((SECONDS + timeout))
  while (( SECONDS <= deadline )); do
    local current_len
    current_len=$(redis XLEN "$stream")
    if (( current_len > baseline_len )); then
      local count=$(( current_len - baseline_len ))
      local raw
      raw=$(redis --raw XREVRANGE "$stream" + - COUNT $((count + 5)) 2>/dev/null || true)
      if [[ -n $raw ]]; then
        local IFS=$'\n'
        local lines=($raw)
        local total=${#lines[@]}
        for ((i=0; i+1<total; i+=2)); do
          local payload=${lines[i+1]}
          if [[ -z $payload ]]; then
            continue
          fi
          if $JQ -e "$jq_filter" <<<"$payload" >/dev/null 2>&1; then
            printf '%s' "$payload"
            return 0
          fi
        done
      fi
    fi
    sleep "$interval"
  done
  return 1
}

extract_env_bool() {
  local key=$1
  if [[ -f $ENV_REPORT_PATH ]]; then
    $JQ -er --arg key "$key" '.[$key]' "$ENV_REPORT_PATH" 2>/dev/null || echo null
  else
    echo null
  fi
}

main() {
  require_command "$JQ"
  require_command redis-cli
  require_env REDIS_URL

  log INFO "solhunter preflight: end-to-end checklist (run=$RUN_ID)"

  local report_ts
  report_ts=$(date -Iseconds)

  declare -A report
  report[ts]=$report_ts
  report[commit]=$PRE_FLIGHT_COMMIT
  report[mode]=${MODE:-}
  report[micro_mode]=${MICRO_MODE:-}
  report[rpc_ok]=$(extract_env_bool "rpc_ok")
  report[das_ok]=$(extract_env_bool "das_ok")
  report[redis_ok]=$(extract_env_bool "redis_ok")
  report[ui_health_ok]=$(extract_env_bool "ui_health_ok")
  report[ui_shadow_fresh]=$(extract_env_bool "ui_shadow_fresh")
  report[golden_publish_on_change_ok]=false
  report[decision_idempotent_ok]=false
  report[duplicate_flood_ok]=false
  report[virtual_fill_ok]=false
  report[pnl_sanity_ok]=false
  report[micro_gates_enforced]=false

  local now
  now=$(date +%s)

  log INFO "1) publish token/market fixtures"
  local token_snapshot
  token_snapshot=$($JQ -n --arg mint "$PREFLIGHT_MINT" --arg symbol "$PREFLIGHT_SYMBOL" --arg name "$PREFLIGHT_NAME" \
    --arg program Tokenkeg --argjson decimals "$PREFLIGHT_DECIMALS" --argjson asof "$now" --arg run "$RUN_ID" \
    '{mint:$mint,symbol:$symbol,name:$name,decimals:$decimals,token_program:$program,flags:{},asof:$asof,_preflight:$run}')
  publish_json x:token.snap "$token_snapshot" >/dev/null

  local ohlcv
  ohlcv=$($JQ -n --arg mint "$PREFLIGHT_MINT" --argjson o 150 --argjson h 152 --argjson l 149 --argjson c 151 \
    --argjson vol_usd 120000 --argjson trades 420 --argjson buyers 260 --argjson zret 2.1 --argjson zvol 2.6 --argjson asof "$now" --arg run "$RUN_ID" \
    '{mint:$mint,o:$o,h:$h,l:$l,c:$c,vol_usd:$vol_usd,trades:$trades,buyers:$buyers,zret:$zret,zvol:$zvol,asof_close:$asof,_preflight:$run}')
  publish_json x:market.ohlcv.5m "$ohlcv" >/dev/null

  local depth
  local depth_asof=$((now + 1))
  depth=$($JQ -n --arg mint "$PREFLIGHT_MINT" --arg venue "$PREFLIGHT_VENUE" --argjson mid 151 --argjson spread 18 \
    --argjson depth1 25000 --argjson depth2 45000 --argjson depth5 90000 --argjson asof "$depth_asof" --arg run "$RUN_ID" \
    '{mint:$mint,venue:$venue,mid_usd:$mid,spread_bps:$spread,depth_pct:{"1":$depth1,"2":$depth2,"5":$depth5},asof:$asof,_preflight:$run}')
  local depth_start_ms
  depth_start_ms=$(now_ms)
  local golden_len_start
  golden_len_start=$(redis XLEN x:mint.golden)
  publish_json x:market.depth "$depth" >/dev/null

  local golden
  if ! golden=$(await_new_stream_event "x:mint.golden" "$golden_len_start" "select(.mint == \"$PREFLIGHT_MINT\" and (.meta.asof|tonumber? == $now))" 30); then
    fail "Golden snapshot not observed"
    exit 3
  fi
  local golden_latency_ms=$(( $(now_ms) - depth_start_ms ))
  local golden_latency_s
  golden_latency_s=$(awk -v ms="$golden_latency_ms" 'BEGIN { printf "%.3f", ms/1000 }')
  report[golden_latency_s]=$golden_latency_s
  if (( golden_latency_ms > GOLDEN_SLO_MS )); then
    fail "Golden publish latency ${golden_latency_ms}ms exceeds SLO ${GOLDEN_SLO_MS}ms"
    exit 3
  fi
  local golden_hash
  golden_hash=$($JQ -r '.hash' <<<"$golden")
  report[golden_hash]=$golden_hash
  pass "Golden emitted hash=$golden_hash latency=${golden_latency_s}s"

  local golden_len_before
  golden_len_before=$(redis XLEN x:mint.golden)
  publish_json x:market.ohlcv.5m "$ohlcv" >/dev/null
  sleep 1
  local golden_len_after
  golden_len_after=$(redis XLEN x:mint.golden)
  if (( golden_len_after == golden_len_before )); then
    pass "Golden publish-on-change respected"
    report[golden_publish_on_change_ok]=true
  else
    fail "Golden re-published on identical inputs"
    exit 3
  fi

  printf '\n'
  log INFO "2) decision idempotency and virtual fill latency"
  local vote_len_before
  vote_len_before=$(redis XLEN x:vote.decisions)

  local suggestion_a
  suggestion_a=$($JQ -n --arg agent momentum --arg mint "$PREFLIGHT_MINT" --arg side buy --argjson notional "$PREFLIGHT_NOTIONAL" \
    --argjson max_slippage 30 --argjson stop 400 --argjson take 120 --argjson time_stop 120 --argjson confidence 0.72 --arg hash "$golden_hash" --arg run "$RUN_ID" \
    '{agent:$agent,mint:$mint,side:$side,notional_usd:$notional,max_slippage_bps:$max_slippage,risk:{stop_bps:$stop,take_bps:$take,time_stop_sec:$time_stop},confidence:$confidence,inputs_hash:$hash,ttl_sec:3,_preflight:$run}')
  local suggestion_b
  suggestion_b=$($JQ -n --arg agent conviction --arg mint "$PREFLIGHT_MINT" --arg side buy --argjson notional "$PREFLIGHT_NOTIONAL" \
    --argjson max_slippage 30 --argjson stop 400 --argjson take 120 --argjson time_stop 120 --argjson confidence 0.66 --arg hash "$golden_hash" --arg run "$RUN_ID" \
    '{agent:$agent,mint:$mint,side:$side,notional_usd:$notional,max_slippage_bps:$max_slippage,risk:{stop_bps:$stop,take_bps:$take,time_stop_sec:$time_stop},confidence:$confidence,inputs_hash:$hash,ttl_sec:3,_preflight:$run}')

  publish_json x:trade.suggested "$suggestion_a" >/dev/null
  publish_json x:trade.suggested "$suggestion_b" >/dev/null

  local decision
  if ! decision=$(await_new_stream_event "x:vote.decisions" "$vote_len_before" "select(.mint == \"$PREFLIGHT_MINT\" and .snapshot_hash == \"$golden_hash\" and .side == \"buy\")" 20); then
    fail "Decision not observed"
    exit 3
  fi
  local decision_time_ms
  decision_time_ms=$(now_ms)
  local client_order_id
  client_order_id=$($JQ -r '.clientOrderId // .client_order_id' <<<"$decision")
  local duplicate_count
  duplicate_count=$($JQ -r '._duplicate_count // 1' <<<"$decision")
  local idempotent
  idempotent=$($JQ -r '._idempotent // false' <<<"$decision")
  if [[ $duplicate_count -gt 1 ]]; then
    fail "Decision $client_order_id flagged duplicate count=$duplicate_count"
    exit 3
  fi
  if [[ $idempotent != "true" ]]; then
    fail "Decision $client_order_id reported non-idempotent"
    exit 3
  fi
  report[decision_idempotent_ok]=true
  pass "Decision $client_order_id accepted (dup=$duplicate_count)"

  local virt_len_before
  virt_len_before=$(redis XLEN x:virt.fills)
  local fill
  if ! fill=$(await_new_stream_event "x:virt.fills" "$virt_len_before" "select(.order_id == \"$client_order_id\")" 15); then
    fail "Virtual fill for $client_order_id not observed"
    exit 3
  fi
  local fill_latency_ms=$(( $(now_ms) - decision_time_ms ))
  local fill_latency_s
  fill_latency_s=$(awk -v ms="$fill_latency_ms" 'BEGIN { printf "%.3f", ms/1000 }')
  report[virtual_fill_ok]=true
  report[decision_to_virtual_fill_latency_s]=$fill_latency_s
  if (( fill_latency_ms > DECISION_TO_FILL_SLO_MS )); then
    fail "Decision→Virtual fill latency ${fill_latency_ms}ms exceeds SLO ${DECISION_TO_FILL_SLO_MS}ms"
    exit 3
  fi
  pass "Virtual fill observed latency=${fill_latency_s}s"

  local buy_qty
  buy_qty=$($JQ -r '.qty_base' <<<"$fill")
  local buy_price
  buy_price=$($JQ -r '.price_usd' <<<"$fill")
  local buy_fees
  buy_fees=$($JQ -r '.fees_usd // 0' <<<"$fill")

  publish_json x:trade.suggested "$suggestion_a" >/dev/null
  publish_json x:trade.suggested "$suggestion_b" >/dev/null
  sleep 2
  local vote_len_after
  vote_len_after=$(redis XLEN x:vote.decisions)
  if (( vote_len_after == vote_len_before + 1 )); then
    report[decision_idempotent_ok]=true
  else
    fail "Vote stream length unexpected after duplicate suggestions"
    exit 3
  fi

  printf '\n'
  log INFO "3) duplicate flood test"
  local burst_before
  burst_before=$(redis XLEN x:vote.decisions)
  for ((i=0; i< PREFLIGHT_DUPLICATE_BURST; i++)); do
    publish_json x:trade.suggested "$suggestion_a" >/dev/null
  done
  sleep 2
  local burst_after
  burst_after=$(redis XLEN x:vote.decisions)
  local latest_decision
  latest_decision=$(latest_stream_value x:vote.decisions)
  local latest_dup
  latest_dup=$($JQ -r '._duplicate_count // 1' <<<"$latest_decision")
  if (( burst_after - burst_before <= 1 )) && (( latest_dup <= 1 )); then
    pass "Duplicate flood suppressed"
    report[duplicate_flood_ok]=true
  else
    fail "Duplicate flood produced extra decisions"
    exit 3
  fi

  printf '\n'
  log INFO "4) partial exit and PnL sanity"
  local sell_notional
  sell_notional=$(awk -v n="$PREFLIGHT_NOTIONAL" 'BEGIN { printf "%.2f", n/2 }')
  local sell_coid="${client_order_id}-partial"
  local sell_baseline
  sell_baseline=$(redis XLEN x:virt.fills)
  local manual_sell
  manual_sell=$($JQ -n --arg mint "$PREFLIGHT_MINT" --arg side sell --argjson notional "$sell_notional" --arg hash "$golden_hash" --arg coid "$sell_coid" --argjson ts "$((now + 4))" --arg run "$RUN_ID" \
    '{mint:$mint,side:$side,notional_usd:$notional,score:0.6,snapshot_hash:$hash,clientOrderId:$coid,agents:["preflight"],ts:$ts,_preflight:$run}')
  publish_json x:vote.decisions "$manual_sell" >/dev/null
  sell_baseline=$(redis XLEN x:virt.fills)
  local sell_fill
  if ! sell_fill=$(await_new_stream_event "x:virt.fills" "$sell_baseline" "select(.order_id == \"$sell_coid\")" 15); then
    fail "Virtual fill for partial sell not observed"
    exit 3
  fi
  local sell_qty
  sell_qty=$($JQ -r '.qty_base' <<<"$sell_fill")
  local sell_price
  sell_price=$($JQ -r '.price_usd' <<<"$sell_fill")
  local sell_fees
  sell_fees=$($JQ -r '.fees_usd // 0' <<<"$sell_fill")

  local expected_realized
  expected_realized=$(python - <<PY
import json
buy_qty = float(${buy_qty:-0})
buy_price = float(${buy_price:-0})
buy_fees = float(${buy_fees:-0})
sell_qty = float(${sell_qty:-0})
sell_price = float(${sell_price:-0})
sell_fees = float(${sell_fees:-0})
if buy_qty <= 0 or sell_qty <= 0:
    print('0.0')
else:
    avg_cost = (buy_price * buy_qty + buy_fees) / buy_qty
    realized = (sell_price - avg_cost) * sell_qty - sell_fees
    print(f"{realized:.6f}")
PY
  )

  local positions_json
  positions_json=$(run_with_timeout 5 curl -sS "$UI_BASE/api/paper/positions" 2>/dev/null || true)
  local reported_realized
  if [[ -n $positions_json ]]; then
    reported_realized=$(python - <<PY
import json, sys
from math import isnan
mint = "$PREFLIGHT_MINT"
try:
    data = json.load(sys.stdin)
except Exception:
    print('')
    sys.exit(0)
if isinstance(data, dict):
    candidates = data.get('positions') or data.get('paper_positions') or data.get('entries') or []
else:
    candidates = data
if isinstance(candidates, dict):
    candidates = candidates.get('paper_positions') or candidates.get('positions') or []
value = None
for entry in candidates:
    if not isinstance(entry, dict):
        continue
    mint_value = entry.get('mint') or entry.get('token') or entry.get('symbol')
    if mint_value == mint:
        value = entry.get('realized_usd')
        break
if value is None:
    print('')
else:
    print(value)
PY
<<<"$positions_json")
  fi

  if [[ -z $reported_realized ]]; then
    fail "Unable to read realized PnL from /api/paper/positions"
    exit 3
  fi

  local pnl_diff
  pnl_diff=$(python - <<PY
from math import fabs
expected = float(${expected_realized:-0})
reported = float(${reported_realized:-0})
diff = fabs(expected - reported)
print(f"{diff:.6f}")
PY
  )
  report[pnl_realized_expected_usd]=$expected_realized
  report[pnl_reported_usd]=$reported_realized
  report[pnl_diff_usd]=$pnl_diff
  if (( $(python - <<PY
from math import fabs
print(int(fabs(float(${pnl_diff:-0})) > 0.01)) )
PY
) )); then
    fail "Realized PnL mismatch expected=${expected_realized} reported=${reported_realized}"
    exit 3
  fi
  report[pnl_sanity_ok]=true
  pass "PnL sanity within tolerance (diff=$pnl_diff)"

  printf '\n'
  log INFO "5) must-exit latency"
  local shock
  shock=$($JQ -n --arg mint "$PREFLIGHT_MINT" --arg venue "$PREFLIGHT_VENUE" --argjson mid 149.4 --argjson spread 150 \
    --argjson depth1 1200 --argjson depth2 3000 --argjson depth5 6000 --argjson asof "$((now + 6))" --arg run "$RUN_ID" \
    '{mint:$mint,venue:$venue,mid_usd:$mid,spread_bps:$spread,depth_pct:{"1":$depth1,"2":$depth2,"5":$depth5},asof:$asof,_preflight:$run}')
  local must_exit_start_ms
  must_exit_start_ms=$(now_ms)
  local virt_len_pre_exit
  virt_len_pre_exit=$(redis XLEN x:virt.fills)
  publish_json x:market.depth "$shock" >/dev/null
  local exit_fill
  if ! exit_fill=$(await_new_stream_event "x:virt.fills" "$virt_len_pre_exit" "select(.mint == \"$PREFLIGHT_MINT\" and .side == \"sell\")" 10); then
    fail "Must-exit SELL not observed"
    exit 3
  fi
  local must_exit_latency_ms=$(( $(now_ms) - must_exit_start_ms ))
  local must_exit_latency_s
  must_exit_latency_s=$(awk -v ms="$must_exit_latency_ms" 'BEGIN { printf "%.3f", ms/1000 }')
  report[must_exit_sell_latency_s]=$must_exit_latency_s
  if (( must_exit_latency_ms > MUST_EXIT_SLO_MS )); then
    fail "Must-exit latency ${must_exit_latency_ms}ms exceeds ${MUST_EXIT_SLO_MS}ms"
    exit 3
  fi
  pass "Must-exit SELL observed latency=${must_exit_latency_s}s"

  printf '\n'
  log INFO "6) micro-mode liquidity gates"
  local bad_depth
  bad_depth=$($JQ -n --arg mint "$PREFLIGHT_MINT" --arg venue "$PREFLIGHT_VENUE" --argjson mid 151 --argjson spread "$PREFLIGHT_BAD_SPREAD_BPS" \
    --argjson depth1 "$PREFLIGHT_BAD_DEPTH_1" --argjson depth2 6000 --argjson depth5 "$PREFLIGHT_BAD_DEPTH_5" --argjson asof "$((now + 8))" --arg run "$RUN_ID" \
    '{mint:$mint,venue:$venue,mid_usd:$mid,spread_bps:$spread,depth_pct:{"1":$depth1,"2":$depth2,"5":$depth5},asof:$asof,_preflight:$run}')
  local golden_len_poor
  golden_len_poor=$(redis XLEN x:mint.golden)
  publish_json x:market.depth "$bad_depth" >/dev/null
  sleep 1
  local poor_golden
  if ! poor_golden=$(await_new_stream_event "x:mint.golden" "$golden_len_poor" "select(.mint == \"$PREFLIGHT_MINT\" and (.liq.depth_pct[\"1\"] // 0 | tonumber? ) <= $PREFLIGHT_BAD_DEPTH_1 + 1)" 20); then
    warn "Unable to observe golden snapshot for bad depth"
  fi
  local gates_vote_before
  gates_vote_before=$(redis XLEN x:vote.decisions)
  publish_json x:trade.suggested "$suggestion_a" >/dev/null
  sleep 2
  local gates_vote_after
  gates_vote_after=$(redis XLEN x:vote.decisions)
  if [[ ${MICRO_MODE:-0} == 1 ]]; then
    if (( gates_vote_after == gates_vote_before )); then
      pass "Micro-mode blocked decision under poor liquidity"
      report[micro_gates_enforced]=true
    else
      fail "Micro-mode leaked decision despite poor liquidity"
      exit 3
    fi
  else
    if (( gates_vote_after > gates_vote_before )); then
      pass "Normal mode permitted decision under poor liquidity"
      report[micro_gates_enforced]=false
    else
      warn "Normal mode did not produce decision under poor liquidity"
      report[micro_gates_enforced]=false
    fi
  fi

  printf '\n'
  pass "PREFLIGHT: ALL CHECKS PASSED"

  local archive_ts
  archive_ts=$(date -u +"%Y%m%dT%H%M%SZ")
  local archive_path="$ARTIFACT_DIR/${archive_ts}-${PRE_FLIGHT_COMMIT}-micro${MICRO_MODE:-0}.json"
  local latest_path="$ARTIFACT_DIR/preflight_report.json"

  python - <<PY
import json
from pathlib import Path
report = {
    "ts": "${report[ts]}",
    "commit": "${report[commit]}",
    "mode": "${report[mode]}",
    "micro_mode": "${report[micro_mode]}",
    "rpc_ok": ${report[rpc_ok]},
    "das_ok": ${report[das_ok]},
    "redis_ok": ${report[redis_ok]},
    "ui_health_ok": ${report[ui_health_ok]},
    "ui_shadow_fresh": ${report[ui_shadow_fresh]},
    "golden_hash": "${report[golden_hash]}",
    "golden_latency_s": ${report[golden_latency_s]},
    "golden_publish_on_change_ok": ${report[golden_publish_on_change_ok]},
    "decision_idempotent_ok": ${report[decision_idempotent_ok]},
    "duplicate_flood_ok": ${report[duplicate_flood_ok]},
    "virtual_fill_ok": ${report[virtual_fill_ok]},
    "decision_to_virtual_fill_latency_s": ${report[decision_to_virtual_fill_latency_s]},
    "pnl_realized_expected_usd": ${report[pnl_realized_expected_usd]},
    "pnl_reported_usd": ${report[pnl_reported_usd]},
    "pnl_diff_usd": ${report[pnl_diff_usd]},
    "pnl_sanity_ok": ${report[pnl_sanity_ok]},
    "must_exit_sell_latency_s": ${report[must_exit_sell_latency_s]},
    "micro_gates_enforced": ${report[micro_gates_enforced]}
}
latest = Path("$latest_path")
latest.write_text(json.dumps(report, indent=2))
archive = Path("$archive_path")
archive.write_text(json.dumps(report, indent=2))
PY

  printf '%s\n' "$archive_path"
}

main "$@"
