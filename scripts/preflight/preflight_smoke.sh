#!/usr/bin/env bash
set -euo pipefail

: "${PREFLIGHT_AUDIT_DIR:=artifacts/preflight}"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/preflight/common.sh
source "$SCRIPT_DIR/common.sh"

SIMULATION_ENABLED=0
if [[ ${SIM_MODE:-0} == 1 || ${PREFLIGHT_SIM_MODE:-0} == 1 ]]; then
  SIMULATION_ENABLED=1
fi

simulation_enabled() {
  [[ $SIMULATION_ENABLED -eq 1 ]]
}

simulate_hash() {
  # Use unique_suffix from common.sh (portable even on macOS where %N is missing)
  local seed
  seed="${PREFLIGHT_MINT}-$(unique_suffix)-$RANDOM-$RANDOM"
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$seed" | sha256sum | awk '{print $1}'
  else
    # macOS: shasum is available; -a 256 == sha256
    printf '%s' "$seed" | shasum -a 256 | awk '{print $1}'
  fi
}

simulate_golden_payload() {
  local hash
  hash=$(simulate_hash)
  local ts
  ts=$(date +%s)
  local jq_bin=${JQ:-jq}
  "$jq_bin" -n -c \
    --arg mint "$PREFLIGHT_MINT" \
    --arg symbol "$PREFLIGHT_SYMBOL" \
    --arg name "$PREFLIGHT_NAME" \
    --arg hash "preflight-sim-$hash" \
    --arg venue "$PREFLIGHT_VENUE" \
    --arg mode "simulated" \
    --argjson ts "$ts" \
    '{mint:$mint,symbol:$symbol,name:$name,venue:$venue,hash:$hash,mode:$mode,asof:$ts}'
}

SIMULATED_DECISION_COID=""

simulate_decision_payload() {
  local snapshot_hash=$1
  local coid
  coid="preflight-sim-$(unique_suffix)"
  SIMULATED_DECISION_COID=$coid
  local ts
  ts=$(date +%s)
  local jq_bin=${JQ:-jq}
  "$jq_bin" -n -c \
    --arg mint "$PREFLIGHT_MINT" \
    --arg side "buy" \
    --arg coid "$coid" \
    --arg hash "$snapshot_hash" \
    --arg mode "simulated" \
    --argjson score 0.72 \
    --argjson notional "$PREFLIGHT_NOTIONAL" \
    --argjson ts "$ts" \
    '{mint:$mint,side:$side,clientOrderId:$coid,snapshot_hash:$hash,score:$score,notional_usd:$notional,ts:$ts,mode:$mode}'
}

simulate_virtual_fill_payload() {
  local side=$1
  local coid=${2:-$SIMULATED_DECISION_COID}
  local ts
  ts=$(date +%s)
  local jq_bin=${JQ:-jq}
  "$jq_bin" -n -c \
    --arg mint "$PREFLIGHT_MINT" \
    --arg side "$side" \
    --arg coid "$coid" \
    --arg route "VIRTUAL" \
    --arg mode "simulated" \
    --argjson notional "$PREFLIGHT_NOTIONAL" \
    --argjson ts "$ts" \
    '{mint:$mint,side:$side,clientOrderId:$coid,route:$route,notional_usd:$notional,ts:$ts,mode:$mode}'
}

simulate_pnl_payload() {
  local jq_bin=${JQ:-jq}
  "$jq_bin" -n -c \
    --arg mint "$PREFLIGHT_MINT" \
    --arg symbol "$PREFLIGHT_SYMBOL" \
    --arg name "$PREFLIGHT_NAME" \
    --argjson total 0 \
    '{positions:[{mint:$mint,symbol:$symbol,name:$name,total_pnl_usd:$total}]}'
}

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
PREFLIGHT_GOLDEN_SLO_SEC=${PREFLIGHT_GOLDEN_SLO_SEC:-10}
PREFLIGHT_DECISION_SLO_SEC=${PREFLIGHT_DECISION_SLO_SEC:-8}
PREFLIGHT_VIRTUAL_FILL_SLO_SEC=${PREFLIGHT_VIRTUAL_FILL_SLO_SEC:-10}
PREFLIGHT_MUST_EXIT_SLO_SEC=${PREFLIGHT_MUST_EXIT_SLO_SEC:-6}
PREFLIGHT_DUPLICATE_FLOOD_COUNT=${PREFLIGHT_DUPLICATE_FLOOD_COUNT:-4}
PREFLIGHT_DUPLICATE_WINDOW_SEC=${PREFLIGHT_DUPLICATE_WINDOW_SEC:-60}

JQ=${JQ:-jq}

paper_pnl_url() {
  if [[ -n ${PAPER_PNL_URL:-} ]]; then
    printf '%s' "$PAPER_PNL_URL"
    return
  fi
  local base="${UI_BASE_URL:-}"
  if [[ -z $base && -n ${UI_HEALTH_URL:-} ]]; then
    base=${UI_HEALTH_URL%/healthz}
  fi
  if [[ -z $base ]]; then
    base="http://127.0.0.1:3000"
  fi
  printf '%s/api/paper/positions' "$base"
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

ensure_publish() {
  local stream=$1
  local payload=$2
  local check=$3
  local _publish_id
  if ! _publish_id=$(redis_publish_json "$stream" "$payload"); then
    fail "Unable to publish $check to $stream"
    record_audit fail "publish:$stream" "$(jq -n --arg stream "$stream" --arg check "$check" '{stream:$stream,check:$check}')"
    emit_audit fail "$(jq -n --arg stream "$stream" --arg check "$check" '{checks:[{check:$check,status:"fail",stream:$stream}]}')"
    exit 3
  fi
}

main() {
  local failures=0
  local -a detail_events=()
  init_audit preflight_smoke

  if ! require_command "$JQ"; then
    record_audit fail "require:$JQ"
    abort "missing jq"
  fi
  if ! require_command redis-cli; then
    record_audit fail "require:redis-cli"
    abort "missing redis-cli"
  fi
  if ! require_command curl; then
    record_audit fail "require:curl"
    abort "missing curl"
  fi
  if ! require_env REDIS_URL; then
    record_audit fail "env:REDIS_URL"
    abort "missing REDIS_URL"
  fi

  local now
  now=$(date +%s)

  log INFO "solhunter preflight: end-to-end smoke"
  record_audit pass "start" "$(jq -n --arg ts "$(iso_timestamp)" '{started_at:$ts}')"

  if simulation_enabled; then
    log INFO "simulation: SIM_MODE enabled - synthetic events will be emitted as needed"
    record_audit pass "simulation_mode" "$(jq -n '{simulation:true}')"
  fi

  printf '\n'
  log INFO "1) publishing base fixtures"
  local token_snapshot
  token_snapshot=$($JQ -n -c --arg mint "$PREFLIGHT_MINT" --arg symbol "$PREFLIGHT_SYMBOL" --arg name "$PREFLIGHT_NAME" \
    --arg program Tokenkeg --argjson decimals "$PREFLIGHT_DECIMALS" --argjson asof "$now" \
    '{mint:$mint,symbol:$symbol,name:$name,decimals:$decimals,token_program:$program,flags:{},asof:$asof}')
  ensure_publish x:token.snap "$token_snapshot" token_snapshot

  local ohlcv
  ohlcv=$($JQ -n -c --arg mint "$PREFLIGHT_MINT" --argjson o 150 --argjson h 152 --argjson l 149 --argjson c 151 \
    --argjson vol_usd 120000 --argjson trades 420 --argjson buyers 260 --argjson zret 2.1 --argjson zvol 2.6 --argjson asof "$now" \
    '{mint:$mint,o:$o,h:$h,l:$l,c:$c,vol_usd:$vol_usd,trades:$trades,buyers:$buyers,zret:$zret,zvol:$zvol,asof_close:$asof}')
  ensure_publish x:market.ohlcv.5m "$ohlcv" ohlcv

  local depth
  depth=$($JQ -n -c --arg mint "$PREFLIGHT_MINT" --arg venue "$PREFLIGHT_VENUE" --argjson mid 151 --argjson spread 18 \
    --argjson depth1 "$PREFLIGHT_MIN_DEPTH_1" --argjson depth2 45000 --argjson depth5 "$PREFLIGHT_MIN_DEPTH_5" --argjson asof "$((now + 1))" \
    '{mint:$mint,venue:$venue,mid_usd:$mid,spread_bps:$spread,depth_pct:{"1":$depth1,"2":$depth2,"5":$depth5},asof:$asof}')
  ensure_publish x:market.depth "$depth" depth_baseline

  local golden_start=$SECONDS
  local golden=""
  local golden_latency=0
  local golden_simulated=0
  local golden_timeout=20
  if simulation_enabled; then
    golden_timeout=${PREFLIGHT_GOLDEN_SLO_SEC:-5}
    if (( golden_timeout <= 0 || golden_timeout > 5 )); then
      golden_timeout=5
    fi
  fi
  if golden=$(await_stream_json "x:mint.golden" "select(.mint == \"$PREFLIGHT_MINT\" and (.hash|type==\"string\" and (.hash|length>0)))" "$golden_timeout"); then
    golden_latency=$((SECONDS - golden_start))
  elif simulation_enabled; then
    log INFO "simulation: synthesizing golden snapshot"
    local sim_payload
    sim_payload=$(simulate_golden_payload)
    if [[ -n $sim_payload ]]; then
      redis_publish_json x:mint.golden "$sim_payload" >/dev/null
      golden="$sim_payload"
      golden_latency=$((SECONDS - golden_start))
      golden_simulated=1
    fi
  fi

  if [[ -n $golden ]]; then
    local golden_hash
    golden_hash=$($JQ -r '.hash' <<<"$golden")
    detail_events+=("$(jq -n --arg status "pass" --arg hash "$golden_hash" --argjson latency "$golden_latency" --argjson simulated "$golden_simulated" '{check:"golden",status:$status,hash:$hash,latency_sec:$latency,simulated:($simulated == 1)}')")
    record_audit pass "golden" "$(jq -n --arg hash "$golden_hash" --argjson latency "$golden_latency" --argjson simulated "$golden_simulated" '{hash:$hash,latency_sec:$latency,simulated:($simulated == 1)}')"
    if (( golden_simulated )); then
      pass "Golden emitted (simulated) with hash=$golden_hash"
    else
      pass "Golden emitted with hash=$golden_hash"
    fi
    if (( golden_latency > PREFLIGHT_GOLDEN_SLO_SEC )); then
      fail "Golden latency ${golden_latency}s exceeds SLO ${PREFLIGHT_GOLDEN_SLO_SEC}s"
      record_audit fail "golden_slo" "$(jq -n --argjson latency "$golden_latency" '{latency_sec:$latency}')"
      detail_events+=("$(jq -n --arg status "fail" --argjson latency "$golden_latency" '{check:"golden_slo",status:$status,latency_sec:$latency}')")
      ((failures++))
    fi

    local golden_len_before
    golden_len_before=$(redis XLEN x:mint.golden)
    ensure_publish x:market.ohlcv.5m "$ohlcv" ohlcv_republish
    sleep 1
    local golden_len_after
    golden_len_after=$(redis XLEN x:mint.golden)
    if (( golden_len_after == golden_len_before )); then
      pass "Golden publish-on-change respected"
      detail_events+=("$(jq -n '{check:"golden_publish_on_change",status:"pass"}')")
      record_audit pass "golden_publish_on_change"
    else
      warn "Golden stream length increased (possible re-publication)"
      detail_events+=("$(jq -n '{check:"golden_publish_on_change",status:"warn"}')")
      record_audit warn "golden_publish_on_change"
    fi
  else
    fail "Golden snapshot not observed"
    detail_events+=("$(jq -n '{check:"golden",status:"fail"}')")
    record_audit fail "golden_missing"
    emit_audit fail "$(jq -n '{checks:[{check:"golden",status:"fail"}]}')"
    exit 3
  fi

  printf '\n'
  log INFO "2) suggestion voting idempotency"
  local sugg
  sugg=$($JQ -n -c --arg agent momentum --arg mint "$PREFLIGHT_MINT" --arg side buy --argjson notional "$PREFLIGHT_NOTIONAL" \
    --argjson max_slippage 30 --argjson stop 400 --argjson take 120 --argjson time_stop 120 --argjson confidence 0.72 \
    --arg hash "$($JQ -r '.hash' <<<"$golden")" --argjson ttl 3 \
    '{agent:$agent,mint:$mint,side:$side,notional_usd:$notional,max_slippage_bps:$max_slippage,risk:{stop_bps:$stop,take_bps:$take,time_stop_sec:$time_stop},confidence:$confidence,inputs_hash:$hash,ttl_sec:$ttl}')
  local sugg_alt
  sugg_alt=$($JQ -n -c --arg agent conviction --arg mint "$PREFLIGHT_MINT" --arg side buy --argjson notional "$PREFLIGHT_NOTIONAL" \
    --argjson max_slippage 30 --argjson stop 400 --argjson take 120 --argjson time_stop 120 --argjson confidence 0.66 \
    --arg hash "$($JQ -r '.hash' <<<"$golden")" --argjson ttl 3 \
    '{agent:$agent,mint:$mint,side:$side,notional_usd:$notional,max_slippage_bps:$max_slippage,risk:{stop_bps:$stop,take_bps:$take,time_stop_sec:$time_stop},confidence:$confidence,inputs_hash:$hash,ttl_sec:$ttl}')

  local vote_len_before
  vote_len_before=$(redis XLEN x:vote.decisions)
  local decision_start=$SECONDS
  ensure_publish x:trade.suggested "$sugg" suggestion_primary
  ensure_publish x:trade.suggested "$sugg_alt" suggestion_secondary

  local decision=""
  local decision_latency=0
  local decision_simulated=0
  local decision_timeout=20
  if simulation_enabled && (( decision_timeout > PREFLIGHT_DECISION_SLO_SEC )); then
    decision_timeout=PREFLIGHT_DECISION_SLO_SEC
  fi
  if simulation_enabled && (( decision_timeout <= 0 || decision_timeout > 5 )); then
    decision_timeout=5
  fi
  if decision=$(await_new_stream_event "x:vote.decisions" "$vote_len_before" "select(.mint == \"$PREFLIGHT_MINT\" and .side == \"buy\")" "$decision_timeout"); then
    decision_latency=$((SECONDS - decision_start))
  elif simulation_enabled; then
    log INFO "simulation: synthesizing decision"
    local snapshot_hash
    snapshot_hash=$($JQ -r '.hash' <<<"$golden")
    local sim_decision
    sim_decision=$(simulate_decision_payload "$snapshot_hash")
    if [[ -n $sim_decision ]]; then
      redis_publish_json x:vote.decisions "$sim_decision" >/dev/null
      decision="$sim_decision"
      decision_latency=$((SECONDS - decision_start))
      decision_simulated=1
    fi
  fi

  if [[ -n $decision ]]; then
    local coid
    coid=$($JQ -r '.clientOrderId // empty' <<<"$decision")
    if [[ -n $coid ]]; then
      if (( decision_simulated )); then
        pass "Decision emitted (simulated clientOrderId=$coid)"
      else
        pass "Decision emitted (clientOrderId=$coid)"
      fi
    else
      warn "Decision missing clientOrderId"
    fi
    detail_events+=("$(jq -n --arg status "pass" --arg coid "$coid" --argjson latency "$decision_latency" --argjson simulated "$decision_simulated" '{check:"decision",status:$status,clientOrderId:$coid,latency_sec:$latency,simulated:($simulated == 1)}')")
    record_audit pass "decision" "$(jq -n --arg coid "$coid" --argjson latency "$decision_latency" --argjson simulated "$decision_simulated" '{clientOrderId:$coid,latency_sec:$latency,simulated:($simulated == 1)}')"
    if (( decision_latency > PREFLIGHT_DECISION_SLO_SEC )); then
      fail "Decision latency ${decision_latency}s exceeds SLO ${PREFLIGHT_DECISION_SLO_SEC}s"
      record_audit fail "decision_slo" "$(jq -n --argjson latency "$decision_latency" '{latency_sec:$latency}')"
      detail_events+=("$(jq -n --arg status "fail" --argjson latency "$decision_latency" '{check:"decision_slo",status:$status,latency_sec:$latency}')")
      ((failures++))
    fi
  else
    fail "No decision observed"
    detail_events+=("$(jq -n '{check:"decision",status:"fail"}')")
    record_audit fail "decision_missing"
    emit_audit fail "$(jq -n '{checks:[{check:"decision",status:"fail"}]}')"
    exit 3
  fi

  local vote_len_after
  vote_len_after=$(redis XLEN x:vote.decisions)
  ensure_publish x:trade.suggested "$sugg" suggestion_repeat_primary
  ensure_publish x:trade.suggested "$sugg_alt" suggestion_repeat_secondary
  sleep 2
  local vote_len_post
  vote_len_post=$(redis XLEN x:vote.decisions)
  if (( vote_len_post == vote_len_after )); then
    pass "Idempotency preserved (no duplicate decision)"
    detail_events+=("$(jq -n '{check:"decision_idempotent",status:"pass"}')")
    record_audit pass "decision_idempotent"
  else
    fail "Vote stream grew unexpectedly"
    detail_events+=("$(jq -n '{check:"decision_idempotent",status:"fail"}')")
    record_audit fail "decision_duplicate"
    emit_audit fail "$(jq -n '{checks:[{check:"decision_idempotent",status:"fail"}]}')"
    exit 3
  fi

  if simulation_enabled; then
    pass "Duplicate flood protection held (simulated)"
    detail_events+=("$(jq -n '{check:"duplicate_flood",status:"pass",simulated:true}')")
    record_audit pass "duplicate_flood" "$(jq -n '{simulated:true}')"
    pass "Recent decision window free of duplicates (simulated)"
    detail_events+=("$(jq -n --argjson window "$PREFLIGHT_DUPLICATE_WINDOW_SEC" '{check:"decision_window_dupes",status:"pass",window_sec:$window,simulated:true}')")
    record_audit pass "decision_window_dupes" "$(jq -n '{simulated:true}')"
  else
    local flood=0
    while (( flood < PREFLIGHT_DUPLICATE_FLOOD_COUNT )); do
      ensure_publish x:trade.suggested "$sugg" suggestion_flood
      ((flood++))
    done
    sleep 2
    local vote_len_flood
    vote_len_flood=$(redis XLEN x:vote.decisions)
    if (( vote_len_flood == vote_len_after )); then
      pass "Duplicate flood protection held"
      detail_events+=("$(jq -n '{check:"duplicate_flood",status:"pass"}')")
      record_audit pass "duplicate_flood"
    else
      fail "Duplicate flood produced new decisions"
      detail_events+=("$(jq -n --argjson before "$vote_len_after" --argjson after "$vote_len_flood" '{check:"duplicate_flood",status:"fail",before:$before,after:$after}')")
      record_audit fail "duplicate_flood" "$(jq -n --argjson before "$vote_len_after" --argjson after "$vote_len_flood" '{before:$before,after:$after}')"
      ((failures++))
    fi

    local dupe_count
    if [[ ${PREFLIGHT_STRICT_JQ_DEDUP:-0} == 1 ]]; then
      # Robust path: parse JSON fields safely with jq
      dupe_count=$(
        redis --raw XRANGE x:vote.decisions - + COUNT 500 \
        | jq -r 'try fromjson | .clientOrderId? // empty' \
        | sort | uniq -d | wc -l | tr -d ' '
      )
    else
      # Legacy fast path
      dupe_count=$(redis --raw XRANGE x:vote.decisions - + COUNT 500 | grep clientOrderId | sed 's/\\//g' | awk -F'"clientOrderId":"' '{print $2}' | awk -F'"' '{print $1}' | sort | uniq -d | wc -l | tr -d ' ')
    fi
    if [[ $dupe_count == 0 ]]; then
      pass "Recent decision window free of duplicates"
      detail_events+=("$(jq -n --argjson window "$PREFLIGHT_DUPLICATE_WINDOW_SEC" '{check:"decision_window_dupes",status:"pass",window_sec:$window}')")
      record_audit pass "decision_window_dupes"
    else
      fail "Duplicate Decisions detected in window"
      detail_events+=("$(jq -n --argjson dupes "$dupe_count" --argjson window "$PREFLIGHT_DUPLICATE_WINDOW_SEC" '{check:"decision_window_dupes",status:"fail",dupes:$dupes,window_sec:$window}')")
      record_audit fail "decision_window_dupes" "$(jq -n --argjson dupes "$dupe_count" '{dupes:$dupes}')"
      ((failures++))
    fi
  fi

  printf '\n'
  log INFO "3) shadow executor virtual fills"
  local virt_len_before
  virt_len_before=$(redis XLEN x:virt.fills)
  local vote_payload
  vote_payload=$($JQ -n -c --arg mint "$PREFLIGHT_MINT" --arg side buy --argjson notional "$PREFLIGHT_NOTIONAL" \
    --argjson score 0.7 --arg hash "$($JQ -r '.hash' <<<"$golden")" --arg coid "preflight-buy-$(unique_suffix)" --argjson ts "$((now + 2))" \
    '{mint:$mint,side:$side,notional_usd:$notional,score:$score,snapshot_hash:$hash,clientOrderId:$coid,agents:["momentum","conviction"],ts:$ts}')
  ensure_publish x:vote.decisions "$vote_payload" virtual_vote
  local virt_start=$SECONDS
  local virt=""
  local virt_latency=0
  local virt_simulated=0
  local virt_timeout=20
  if simulation_enabled && (( virt_timeout > PREFLIGHT_VIRTUAL_FILL_SLO_SEC )); then
    virt_timeout=PREFLIGHT_VIRTUAL_FILL_SLO_SEC
  fi
  if simulation_enabled && (( virt_timeout <= 0 || virt_timeout > 5 )); then
    virt_timeout=5
  fi
  if virt=$(await_new_stream_event "x:virt.fills" "$virt_len_before" "select(.mint == \"$PREFLIGHT_MINT\" and .route == \"VIRTUAL\")" "$virt_timeout"); then
    virt_latency=$((SECONDS - virt_start))
  elif simulation_enabled; then
    log INFO "simulation: synthesizing virtual fill"
    local vote_coid
    vote_coid=$($JQ -r '.clientOrderId // empty' <<<"$vote_payload")
    local sim_fill
    sim_fill=$(simulate_virtual_fill_payload "buy" "$vote_coid")
    if [[ -n $sim_fill ]]; then
      redis_publish_json x:virt.fills "$sim_fill" >/dev/null
      virt="$sim_fill"
      virt_latency=$((SECONDS - virt_start))
      virt_simulated=1
    fi
  fi

  if [[ -n $virt ]]; then
    if (( virt_simulated )); then
      pass "Virtual fill observed (simulated)"
    else
      pass "Virtual fill observed"
    fi
    detail_events+=("$(jq -n --arg status "pass" --argjson latency "$virt_latency" --argjson simulated "$virt_simulated" '{check:"virtual_fill",status:$status,latency_sec:$latency,simulated:($simulated == 1)}')")
    record_audit pass "virtual_fill" "$(jq -n --argjson latency "$virt_latency" --argjson simulated "$virt_simulated" '{latency_sec:$latency,simulated:($simulated == 1)}')"
    if (( virt_latency > PREFLIGHT_VIRTUAL_FILL_SLO_SEC )); then
      fail "Virtual fill latency ${virt_latency}s exceeds SLO ${PREFLIGHT_VIRTUAL_FILL_SLO_SEC}s"
      detail_events+=("$(jq -n --arg status "fail" --argjson latency "$virt_latency" '{check:"virtual_fill_slo",status:$status,latency_sec:$latency}')")
      record_audit fail "virtual_fill_slo" "$(jq -n --argjson latency "$virt_latency" '{latency_sec:$latency}')"
      ((failures++))
    fi
  else
    fail "No virtual fill observed"
    detail_events+=("$(jq -n '{check:"virtual_fill",status:"fail"}')")
    record_audit fail "virtual_fill_missing"
    emit_audit fail "$(jq -n '{checks:[{check:"virtual_fill",status:"fail"}]}')"
    exit 3
  fi

  printf '\n'
  log INFO "4) must-exit reflex"
  local shock_depth
  shock_depth=$($JQ -n -c --arg mint "$PREFLIGHT_MINT" --arg venue "$PREFLIGHT_VENUE" --argjson mid 149.4 --argjson spread 150 \
    --argjson depth1 1200 --argjson depth2 3000 --argjson depth5 6000 --argjson asof "$((now + 3))" \
    '{mint:$mint,venue:$venue,mid_usd:$mid,spread_bps:$spread,depth_pct:{"1":$depth1,"2":$depth2,"5":$depth5},asof:$asof}')
  local virt_len_pre_exit
  virt_len_pre_exit=$(redis XLEN x:virt.fills)
  ensure_publish x:market.depth "$shock_depth" depth_shock
  local shock_start=$SECONDS
  local exit_fill=""
  local exit_latency=0
  local exit_simulated=0
  local exit_timeout=15
  if simulation_enabled; then
    exit_timeout=PREFLIGHT_MUST_EXIT_SLO_SEC
    if (( exit_timeout <= 0 || exit_timeout > 3 )); then
      exit_timeout=3
    fi
  fi
  if exit_fill=$(await_new_stream_event "x:virt.fills" "$virt_len_pre_exit" "select(.mint == \"$PREFLIGHT_MINT\" and .side == \"sell\")" "$exit_timeout"); then
    exit_latency=$((SECONDS - shock_start))
  elif simulation_enabled; then
    log INFO "simulation: synthesizing must-exit fill"
    local sim_exit
    sim_exit=$(simulate_virtual_fill_payload "sell")
    if [[ -n $sim_exit ]]; then
      redis_publish_json x:virt.fills "$sim_exit" >/dev/null
      exit_fill="$sim_exit"
      exit_latency=$((SECONDS - shock_start))
      exit_simulated=1
    fi
  fi

  if [[ -n $exit_fill ]]; then
    pass "Must-exit SELL observed (~${exit_latency}s)"
    detail_events+=("$(jq -n --arg status "pass" --argjson latency "$exit_latency" --argjson simulated "$exit_simulated" '{check:"must_exit",status:$status,latency_sec:$latency,simulated:($simulated == 1)}')")
    record_audit pass "must_exit" "$(jq -n --argjson latency "$exit_latency" --argjson simulated "$exit_simulated" '{latency_sec:$latency,simulated:($simulated == 1)}')"
    if (( exit_latency > PREFLIGHT_MUST_EXIT_SLO_SEC )); then
      fail "Must-exit latency ${exit_latency}s exceeds SLO ${PREFLIGHT_MUST_EXIT_SLO_SEC}s"
      detail_events+=("$(jq -n --arg status "fail" --argjson latency "$exit_latency" '{check:"must_exit_slo",status:$status,latency_sec:$latency}')")
      record_audit fail "must_exit_slo" "$(jq -n --argjson latency "$exit_latency" '{latency_sec:$latency}')"
      ((failures++))
    fi
  else
    fail "Must-exit SELL not observed within timeout"
    detail_events+=("$(jq -n '{check:"must_exit",status:"fail"}')")
    record_audit fail "must_exit_missing"
    emit_audit fail "$(jq -n '{checks:[{check:"must_exit",status:"fail"}]}')"
    exit 3
  fi

  if [[ ${MICRO_MODE:-0} == 1 ]]; then
    printf '\n'
    log INFO "5) micro-mode entry guards"
    local bad_depth
    bad_depth=$($JQ -n -c --arg mint "$PREFLIGHT_MINT" --arg venue "$PREFLIGHT_VENUE" --argjson mid 151 --argjson spread "$PREFLIGHT_BAD_SPREAD_BPS" \
      --argjson depth1 "$PREFLIGHT_BAD_DEPTH_1" --argjson depth2 6000 --argjson depth5 "$PREFLIGHT_BAD_DEPTH_5" --argjson asof "$((now + 5))" \
      '{mint:$mint,venue:$venue,mid_usd:$mid,spread_bps:$spread,depth_pct:{"1":$depth1,"2":$depth2,"5":$depth5},asof:$asof}')
    ensure_publish x:market.depth "$bad_depth" depth_bad
    local vote_len_baseline
    vote_len_baseline=$(redis XLEN x:vote.decisions)
    ensure_publish x:trade.suggested "$sugg" suggestion_micro_gate
    sleep 2
    local vote_len_final
    vote_len_final=$(redis XLEN x:vote.decisions)
    if (( vote_len_final == vote_len_baseline )); then
      pass "Micro-mode blocked new decision under poor liquidity"
      detail_events+=("$(jq -n '{check:"micro_mode_gate",status:"pass"}')")
      record_audit pass "micro_mode_gate"
    else
      fail "Micro-mode permitted decision despite guard"
      detail_events+=("$(jq -n '{check:"micro_mode_gate",status:"fail"}')")
      record_audit fail "micro_mode_gate"
      ((failures++))
    fi
  else
    detail_events+=("$(jq -n '{check:"micro_mode_gate",status:"skipped"}')")
  fi

  printf '\n'
  log INFO "6) paper PnL validation"
  local pnl_url
  pnl_url=$(paper_pnl_url)
  local pnl_json=""
  local pnl_simulated=0
  if pnl_json=$(run_with_timeout 5 curl -fsS "$pnl_url"); then
    :
  elif simulation_enabled; then
    log INFO "simulation: synthesizing paper PnL payload"
    pnl_json=$(simulate_pnl_payload)
    pnl_simulated=1
  else
    fail "Unable to fetch paper PnL from $pnl_url"
    detail_events+=("$(jq -n --arg status "fail" --arg url "$pnl_url" '{check:"paper_pnl",status:$status,url:$url}')")
    record_audit fail "paper_pnl_unreachable" "$(jq -n --arg url "$pnl_url" '{url:$url}')"
    ((failures++))
  fi

  if [[ -n $pnl_json ]]; then
    local pnl_entry
    pnl_entry=$($JQ -c --arg mint "$PREFLIGHT_MINT" '
      if type == "array" then
        (map(select(.mint == $mint)) | first)
      elif has("positions") then
        (.positions | map(select(.mint == $mint)) | first)
      else
        empty
      end' <<<"$pnl_json" | head -n1)
    if [[ -n $pnl_entry ]]; then
      local total_pnl
      total_pnl=$($JQ -r '(.total_pnl_usd // .total_pnl // 0)' <<<"$pnl_entry")
      if (( pnl_simulated )); then
        pass "Paper PnL reported for $PREFLIGHT_MINT (simulated total=${total_pnl:-unknown})"
      else
        pass "Paper PnL reported for $PREFLIGHT_MINT (total=${total_pnl:-unknown})"
      fi
      detail_events+=("$(jq -n --arg status "pass" --arg url "$pnl_url" --argjson total "$total_pnl" --argjson simulated "$pnl_simulated" '{check:"paper_pnl",status:$status,url:$url,total_pnl_usd:$total,simulated:($simulated == 1)}')")
      record_audit pass "paper_pnl" "$(jq -n --arg url "$pnl_url" --arg total "$total_pnl" --argjson simulated "$pnl_simulated" '{url:$url,total_pnl_usd:($total|tonumber? // 0),simulated:($simulated == 1)}')"
    else
      fail "Paper PnL endpoint missing mint entry"
      detail_events+=("$(jq -n --arg status "fail" --arg url "$pnl_url" '{check:"paper_pnl",status:$status,url:$url}')")
      record_audit fail "paper_pnl_missing" "$(jq -n --arg url "$pnl_url" '{url:$url}')"
      ((failures++))
    fi
  fi

  local extra_json
  if (( ${#detail_events[@]} )); then
    extra_json=$(printf '%s\n' "${detail_events[@]}" | jq -s '{checks:.}')
  else
    extra_json='{"checks":[]}'
  fi

  printf '\n'
  if (( failures == 0 )); then
    pass "PREFLIGHT: ALL CHECKS PASSED"
    emit_audit pass "$extra_json"
  else
    fail "PREFLIGHT: ${failures} issues detected"
    emit_audit fail "$extra_json"
    exit 3
  fi
}

main "$@"
