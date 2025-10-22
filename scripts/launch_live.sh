#!/usr/bin/env bash
set -euo pipefail

timestamp() {
  date '+%Y-%m-%dT%H:%M:%S%z'
}

log_info() {
  local msg=$*
  printf '[%s] [launch_live] %s\n' "$(timestamp)" "$msg"
}

log_warn() {
  local msg=$*
  printf '[%s] [launch_live][warn] %s\n' "$(timestamp)" "$msg" >&2
}

EXIT_KEYS=1
EXIT_PREFLIGHT=2
EXIT_CONNECTIVITY=3
EXIT_HEALTH=4
EXIT_DEPS=5

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DEFAULT_ARTIFACT_ROOT="$ROOT_DIR/artifacts"
DEFAULT_RUN_ID="prelaunch"
export RUNTIME_ARTIFACT_ROOT="${RUNTIME_ARTIFACT_ROOT:-$DEFAULT_ARTIFACT_ROOT}"
export RUNTIME_RUN_ID="${RUNTIME_RUN_ID:-$DEFAULT_RUN_ID}"
# Keep in sync with DEFAULT_RUNTIME_WORKFLOW in
# solhunter_zero/runtime/runtime_wiring.py
DEFAULT_RUNTIME_WORKFLOW="golden-multi-stage-golden-stream"
if [[ -z ${RUNTIME_WORKFLOW:-} ]]; then
  export RUNTIME_WORKFLOW="$DEFAULT_RUNTIME_WORKFLOW"
  log_info "RUNTIME_WORKFLOW defaulted to $RUNTIME_WORKFLOW"
else
  log_info "RUNTIME_WORKFLOW preset to $RUNTIME_WORKFLOW"
fi
if [[ -z ${SOLHUNTER_WORKFLOW:-} ]]; then
  export SOLHUNTER_WORKFLOW="$RUNTIME_WORKFLOW"
fi
ART_DIR="$RUNTIME_ARTIFACT_ROOT/$RUNTIME_RUN_ID"
ARTIFACT_DIR="$ART_DIR"
LOG_DIR="$ARTIFACT_DIR/logs"
mkdir -p "$ARTIFACT_DIR" "$LOG_DIR"
log_info "Runtime artifacts will be written to $ARTIFACT_DIR (logs in $LOG_DIR)"

# Ensure the repository root is always importable when invoking helper scripts.
# "launch_live.sh" may be executed before the package is installed (e.g. from a
# fresh clone), so python invocations need the project root on PYTHONPATH so
# that modules like ``solhunter_zero`` can be imported successfully.
if [[ -n ${PYTHONPATH:-} ]]; then
  export PYTHONPATH="$ROOT_DIR:$PYTHONPATH"
else
  export PYTHONPATH="$ROOT_DIR"
fi

usage() {
  cat <<'EOF'
Usage: bash scripts/launch_live.sh --env <env-file> --micro <0|1> [--canary] --budget <usd> --risk <ratio> --preflight <runs> --soak <seconds> [--config <path>]
EOF
}

need_val() {
  # ensure a flag expecting a value actually has one
  if [[ -z ${2:-} || ${2:-} == --* ]]; then
    echo "Flag $1 requires a value" >&2
    usage
    exit 1
  fi
}

ENV_FILE=""
MICRO_FLAG=""
CANARY_MODE=0
CANARY_BUDGET=""
CANARY_RISK=""
PREFLIGHT_RUNS=2
SOAK_DURATION=180
CONFIG_PATH=""

declare -a POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --env)
      need_val "$1" "${2:-}"; ENV_FILE=$2
      shift 2
      ;;
    --micro)
      need_val "$1" "${2:-}"; MICRO_FLAG=$2
      shift 2
      ;;
    --canary)
      CANARY_MODE=1
      shift
      ;;
    --budget)
      need_val "$1" "${2:-}"; CANARY_BUDGET=$2
      shift 2
      ;;
    --risk)
      need_val "$1" "${2:-}"; CANARY_RISK=$2
      shift 2
      ;;
    --preflight)
      need_val "$1" "${2:-}"; PREFLIGHT_RUNS=$2
      shift 2
      ;;
    --soak)
      need_val "$1" "${2:-}"; SOAK_DURATION=$2
      shift 2
      ;;
    --config)
      need_val "$1" "${2:-}"; CONFIG_PATH=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

if [[ ${#POSITIONAL[@]} -gt 0 ]]; then
  echo "Unexpected arguments: ${POSITIONAL[*]}" >&2
  usage
  exit 1
fi

if [[ -z $ENV_FILE ]]; then
  echo "--env is required" >&2
  usage
  exit 1
fi

if [[ -z $MICRO_FLAG ]]; then
  echo "--micro is required" >&2
  usage
  exit 1
fi

if [[ $MICRO_FLAG != "0" && $MICRO_FLAG != "1" ]]; then
  echo "--micro must be 0 or 1" >&2
  exit 1
fi

if ! [[ $PREFLIGHT_RUNS =~ ^[0-9]+$ ]]; then
  echo "--preflight must be an integer" >&2
  exit 1
fi

if [[ $PREFLIGHT_RUNS -ne 2 ]]; then
  echo "--preflight must be 2 to cover micro on/off passes" >&2
  exit $EXIT_PREFLIGHT
fi

if [[ ! -f $ENV_FILE ]]; then
  echo "Environment file $ENV_FILE not found" >&2
  exit $EXIT_KEYS
fi

if [[ $CANARY_MODE -eq 1 ]]; then
  if [[ -z $CANARY_BUDGET || -z $CANARY_RISK ]]; then
    echo "--canary requires --budget and --risk" >&2
    exit 1
  fi
fi

if ! [[ $SOAK_DURATION =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  echo "--soak must be numeric" >&2
  exit 1
fi

log_info "Starting launch_live with env=$ENV_FILE micro=$MICRO_FLAG canary=$CANARY_MODE preflight_runs=$PREFLIGHT_RUNS soak=${SOAK_DURATION}s"

log_info "Ensuring Python dependencies are installed"
DEPS_LOG="$LOG_DIR/deps_install.log"
if ! python3 -m scripts.deps 2>&1 | tee "$DEPS_LOG" >/dev/null; then
  log_warn "Failed to install Python dependencies (see $DEPS_LOG)"
  exit $EXIT_DEPS
fi

MANIFEST_RAW=""
validate_env_file() {
  python3 - "$ENV_FILE" <<'PY'
import json
import os
import re
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
if not env_path.exists():
    print(f"Environment file {env_path} missing", file=sys.stderr)
    raise SystemExit(1)
pattern_strings = [
    r"REDACTED",
    r"YOUR[_-]",
    r"\$\{",
    r"EXAMPLE",
    r"XXXX",
    r"CHANGE_ME",
    r"PLACEHOLDER",
]
patterns = [re.compile(pat, re.IGNORECASE) for pat in pattern_strings]
values: dict[str, str] = {}
for idx, raw_line in enumerate(env_path.read_text().splitlines(), start=1):
    line = raw_line.strip()
    if not line or line.startswith('#'):
        continue
    if '=' not in line:
        continue
    key, value = line.split('=', 1)
    key = key.strip()
    value = value.strip().strip('"').strip("'")
    values[key] = value
    if value == "":
        print(f"missing value for {key}", file=sys.stderr)
        raise SystemExit(1)
    for pat in patterns:
        if pat.search(value):
            print(f"placeholder detected for {key}: {pat.pattern}", file=sys.stderr)
            raise SystemExit(1)
manifest = []
for key, value in sorted(values.items()):
    lowered = key.lower()
    if any(tok in lowered for tok in ("secret", "key", "token", "pass", "private", "pwd")):
        masked = "***"
    else:
        masked = value
    manifest.append(f"{key}={masked}")
print("\n".join(manifest))
PY
}

log_info "Validating environment file $ENV_FILE"
if ! MANIFEST_RAW=$(validate_env_file); then
  exit $EXIT_KEYS
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# Ensure Jupiter websocket uses the stats endpoint unless explicitly overridden.
export JUPITER_WS_URL="${JUPITER_WS_URL:-wss://stats.jup.ag/ws}"

if [[ -n $CONFIG_PATH ]]; then
  export CONFIG_PATH
fi

PROVIDER_STATUS=""
log_info "Checking configured provider credentials and connectivity"
if ! PROVIDER_STATUS=$(python3 - <<'PY'
from solhunter_zero.production import Provider, assert_providers_ok, format_configured_providers
import sys
providers = [
    Provider("Solana", ("SOLANA_RPC_URL", "SOLANA_WS_URL")),
    Provider("Helius", ("HELIUS_API_KEY",)),
    Provider("Redis", ("REDIS_URL",), optional=True),
    Provider("UI", ("UI_WS_URL",), optional=True),
    Provider("Helius-DAS", ("DAS_BASE_URL",), optional=True),
]
try:
    assert_providers_ok(providers)
except Exception as exc:
    print(exc, file=sys.stderr)
    raise SystemExit(1)
print(format_configured_providers(providers))
PY
); then
  exit $EXIT_KEYS
fi

log_info "Environment manifest (sensitive values masked)"
printf '%s\n' "$MANIFEST_RAW"
log_info "Provider status"
printf '%s\n' "$PROVIDER_STATUS"

ensure_redis() {
  python3 - <<'PY'
import os
from solhunter_zero.redis_util import ensure_local_redis_if_needed
urls = []
for key in ("REDIS_URL", "BROKER_URL", "BROKER_URLS"):
    raw = os.getenv(key)
    if not raw:
        continue
    parts = [p.strip() for p in raw.split(',') if p.strip()]
    urls.extend(parts)
ensure_local_redis_if_needed(urls)
PY
}

log_info "Ensuring Redis availability"
ensure_redis
log_info "Redis helper completed"

redis_health() {
  if [[ -z ${REDIS_URL:-} ]]; then
    return 0
  fi
  if command -v redis-cli >/dev/null 2>&1; then
    redis-cli -u "$REDIS_URL" PING >/dev/null 2>&1
  else
    python3 - <<'PY'
import os
import asyncio
from solhunter_zero.production import ConnectivityChecker
async def _check():
    checker = ConnectivityChecker()
    try:
        ok = await checker.check("redis")
        if not ok:
            raise SystemExit(1)
    except AttributeError:
        for target in checker.targets:
            if target.get("name") == "redis":
                result = await checker._probe_redis(target["name"], target["url"])
                if not result.ok:
                    raise SystemExit(1)
asyncio.run(_check())
PY
  fi
}

log_info "Running Redis health check"
if ! redis_health; then
  log_warn "Redis health check failed"
  exit $EXIT_HEALTH
fi
log_info "Redis health check passed"

declare -a CHILD_PIDS=()
register_child() {
  CHILD_PIDS+=("$1")
}

start_log_stream() {
  local log=$1
  local label=$2
  touch "$log"
  local __last_line=""
  {
    tail -n +1 -F "$log" 2>/dev/null |
      while IFS= read -r line; do
        if [[ ${line} != "${__last_line:-}" ]]; then
          printf '[%s] [%s] %s\n' "$(timestamp)" "$label" "$line"
        fi
        __last_line=$line
      done
  } &
  local tail_pid=$!
  register_child "$tail_pid"
}

cleanup() {
  if [[ -z ${CHILD_PIDS+x} ]]; then
    return
  fi
  for pid in "${CHILD_PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}

trap cleanup EXIT
trap 'cleanup; exit $EXIT_HEALTH' ERR
trap 'cleanup; exit 0' INT TERM

start_controller() {
  local mode=$1
  local log=$2
  local notify=$3
  local args=("$ROOT_DIR/scripts/live_runtime_controller.py" "--mode" "$mode" "--micro" "$MICRO_FLAG")
  if [[ -n $CONFIG_PATH ]]; then
    args+=("--config" "$CONFIG_PATH")
  fi
  if [[ $mode == "live" && $CANARY_MODE -eq 1 ]]; then
    args+=("--canary-budget" "$CANARY_BUDGET" "--canary-risk" "$CANARY_RISK")
  fi
  if [[ -n $notify ]]; then
    args+=("--notify" "$notify")
  fi
  python3 "${args[@]}" >"$log" 2>&1 &
  local pid=$!
  register_child "$pid"
  printf '%s\n' "$pid"
}

print_log_excerpt() {
  local log=$1
  local reason=${2:-}
  local header="---- Runtime log excerpt ($log) ----"
  if [[ -n $reason ]]; then
    echo "$reason" >&2
  fi
  if [[ -f $log ]]; then
    echo "$header" >&2
    tail -n 200 "$log" >&2 || true
    echo "---- End runtime log ----" >&2
  else
    echo "Log file $log not found" >&2
  fi
}

print_ui_location() {
  local runtime_log=$1
  local art_dir=$2
  local ui_line=""
  if [[ -f $runtime_log ]]; then
    ui_line="$(grep -m1 -E 'UI_READY url=' "$runtime_log" || true)"
  fi
  if [[ -z $ui_line && -n $art_dir && -f "$art_dir/ui_url.txt" ]]; then
    ui_line="UI_READY url=$(cat "$art_dir/ui_url.txt")"
  fi
  if [[ -n $ui_line ]]; then
    local ui_url
    ui_url="$(echo "$ui_line" | sed -E 's/.*url=([^ ]+).*/\1/')"
    if [[ -n $ui_url ]]; then
      echo ""
      echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
      echo "  UI available at: $ui_url"
      echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    else
      echo "UI readiness line detected but URL parsing failed: $ui_line"
    fi
  else
    echo "UI URL not detected yet; check $runtime_log for binding lines."
  fi
}

READY_TIMEOUT="${READY_TIMEOUT:-120}"
wait_for_ready() {
  local log=$1
  local notify=$2
  local pid=${3:-}
  local waited=0
  while [[ $waited -lt $READY_TIMEOUT ]]; do
    if [[ -n $notify && -f $notify ]]; then
      return 0
    fi
    if grep -q "RUNTIME_READY" "$log" 2>/dev/null; then
      return 0
    fi
    if [[ -n $pid ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
      print_log_excerpt "$log" "Runtime process $pid exited early"
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
  print_log_excerpt "$log" "Timed out waiting for runtime readiness"
  return 1
}

PAPER_LOG="$LOG_DIR/paper_runtime.log"
PAPER_NOTIFY="$ARTIFACT_DIR/paper_ready"
rm -f "$PAPER_NOTIFY"
log_info "Launching runtime controller (mode=paper, log=$PAPER_LOG)"
PAPER_PID=$(start_controller "paper" "$PAPER_LOG" "$PAPER_NOTIFY")
start_log_stream "$PAPER_LOG" "paper"
log_info "Waiting for paper runtime readiness"
if ! wait_for_ready "$PAPER_LOG" "$PAPER_NOTIFY" "$PAPER_PID"; then
  log_warn "Paper runtime failed to become ready"
  exit $EXIT_HEALTH
fi

log_info "Paper runtime ready (PID=$PAPER_PID)"
print_ui_location "$PAPER_LOG" "$ART_DIR"

run_preflight() {
  MODE=paper MICRO_MODE=1 bash "$ROOT_DIR/scripts/preflight/run_all.sh"
}

log_info "Running preflight suite"
if ! run_preflight; then
  log_warn "Preflight suite failed"
  exit $EXIT_PREFLIGHT
fi
log_info "Preflight suite completed successfully"

kill "$PAPER_PID" >/dev/null 2>&1 || true
wait "$PAPER_PID" 2>/dev/null || true
log_info "Paper runtime stopped after preflight"

log_info "Starting connectivity soak for ${SOAK_DURATION}s"
connectivity_soak() {
  python3 - <<'PY'
import json
import os
from pathlib import Path
from solhunter_zero.production import ConnectivityChecker

duration = float(os.environ.get("SOAK_DURATION", "0"))
output_path = Path(os.environ.get("SOAK_REPORT", "artifacts/prelaunch/connectivity_report.json"))
checker = ConnectivityChecker()
async def _run():
    summary = await checker.run_soak(duration=duration, output_path=output_path)
    return {
        "duration": summary.duration,
        "reconnect_count": summary.reconnect_count,
        "metrics": summary.metrics,
        "report": str(output_path),
    }

import asyncio
result = asyncio.run(_run())
print(json.dumps(result))
PY
}

export SOAK_DURATION="$SOAK_DURATION"
export SOAK_REPORT="$ARTIFACT_DIR/connectivity_report.json"
SOAK_RESULT=""
if ! SOAK_RESULT=$(connectivity_soak); then
  log_warn "Connectivity soak failed"
  exit $EXIT_CONNECTIVITY
fi

log_info "Connectivity soak complete: $SOAK_RESULT"

export MODE=live
# keep env aligned with the flag we passed to the controller
export MICRO_MODE="$MICRO_FLAG"
if [[ $CANARY_MODE -eq 1 ]]; then
  export CANARY_MODE=1
  export CANARY_BUDGET_USD=$CANARY_BUDGET
  export CANARY_RISK_CAP=$CANARY_RISK
fi

LIVE_LOG="$LOG_DIR/live_runtime.log"
LIVE_NOTIFY="$ARTIFACT_DIR/live_ready"
rm -f "$LIVE_NOTIFY"
log_info "Launching runtime controller (mode=live, log=$LIVE_LOG)"
LIVE_PID=$(start_controller "live" "$LIVE_LOG" "$LIVE_NOTIFY")
start_log_stream "$LIVE_LOG" "live"
log_info "Waiting for live runtime readiness"
if ! wait_for_ready "$LIVE_LOG" "$LIVE_NOTIFY" "$LIVE_PID"; then
  log_warn "Live runtime failed to become ready"
  exit $EXIT_HEALTH
fi

log_info "Live runtime ready (PID=$LIVE_PID)"
print_ui_location "$LIVE_LOG" "$ART_DIR"
micro_label=$([[ "$MICRO_FLAG" == "1" ]] && echo "on" || echo "off")
canary_label=$([[ $CANARY_MODE -eq 1 ]] && echo " | Canary limits applied" || echo "")
GO_NO_GO="GO/NO-GO: Keys OK | Services OK | Preflight PASSED (2/2) | Soak PASSED | MODE=live | MICRO=${micro_label}${canary_label}"
log_info "$GO_NO_GO"

wait "$LIVE_PID"
status=$?
if [[ $status -ne 0 ]]; then
  print_log_excerpt "$LIVE_LOG" "Live runtime exited with status $status"
  log_warn "Live runtime exited with status $status"
  exit $EXIT_HEALTH
fi

log_info "Live runtime exited cleanly"

exit 0
