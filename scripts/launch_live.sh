#!/usr/bin/env bash
set -euo pipefail

EXIT_KEYS=1
EXIT_PREFLIGHT=2
EXIT_CONNECTIVITY=3
EXIT_HEALTH=4

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ARTIFACT_DIR="$ROOT_DIR/artifacts/prelaunch"
LOG_DIR="$ARTIFACT_DIR/logs"
mkdir -p "$LOG_DIR"

usage() {
  cat <<'EOF'
Usage: bash scripts/launch_live.sh --env <env-file> --micro <0|1> [--canary] --budget <usd> --risk <ratio> --preflight <runs> --soak <seconds> [--config <path>]
EOF
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
      ENV_FILE=$2
      shift 2
      ;;
    --micro)
      MICRO_FLAG=$2
      shift 2
      ;;
    --canary)
      CANARY_MODE=1
      shift
      ;;
    --budget)
      CANARY_BUDGET=$2
      shift 2
      ;;
    --risk)
      CANARY_RISK=$2
      shift 2
      ;;
    --preflight)
      PREFLIGHT_RUNS=$2
      shift 2
      ;;
    --soak)
      SOAK_DURATION=$2
      shift 2
      ;;
    --config)
      CONFIG_PATH=$2
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

if ! MANIFEST_RAW=$(validate_env_file); then
  exit $EXIT_KEYS
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

if [[ -n $CONFIG_PATH ]]; then
  export CONFIG_PATH
fi

PROVIDER_STATUS=""
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

printf '%s\n' "$MANIFEST_RAW"
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

ensure_redis

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
    for target in checker.targets:
        if target.get("name") == "redis":
            result = await checker._probe_redis(target["name"], target["url"])
            if not result.ok:
                raise SystemExit(1)
asyncio.run(_check())
PY
  fi
}

if ! redis_health; then
  echo "Redis health check failed" >&2
  exit $EXIT_HEALTH
fi

declare -a CHILD_PIDS=()
register_child() {
  CHILD_PIDS+=("$1")
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
  echo "$pid"
}

wait_for_ready() {
  local log=$1
  local notify=$2
  local pid=${3:-}
  local waited=0
  while [[ $waited -lt 120 ]]; do
    if [[ -n $notify && -f $notify ]]; then
      return 0
    fi
    if grep -q "RUNTIME_READY" "$log" 2>/dev/null; then
      return 0
    fi
    if [[ -n $pid ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "Runtime process $pid exited early" >&2
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
  echo "Timed out waiting for runtime readiness" >&2
  return 1
}

PAPER_LOG="$LOG_DIR/paper_runtime.log"
PAPER_NOTIFY="$ARTIFACT_DIR/paper_ready"
rm -f "$PAPER_NOTIFY"
PAPER_PID=$(start_controller "paper" "$PAPER_LOG" "$PAPER_NOTIFY")
if ! wait_for_ready "$PAPER_LOG" "$PAPER_NOTIFY" "$PAPER_PID"; then
  echo "Paper runtime failed to become ready" >&2
  exit $EXIT_HEALTH
fi

echo "Paper runtime ready (PID=$PAPER_PID)"

run_preflight() {
  MODE=paper MICRO_MODE=1 bash "$ROOT_DIR/scripts/preflight/run_all.sh"
}

if ! run_preflight; then
  echo "Preflight suite failed" >&2
  exit $EXIT_PREFLIGHT
fi

kill "$PAPER_PID" >/dev/null 2>&1 || true
wait "$PAPER_PID" 2>/dev/null || true

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
  echo "Connectivity soak failed" >&2
  exit $EXIT_CONNECTIVITY
fi

echo "Connectivity soak complete: $SOAK_RESULT"

export MODE=live
export MICRO_MODE=1
if [[ $CANARY_MODE -eq 1 ]]; then
  export CANARY_MODE=1
  export CANARY_BUDGET_USD=$CANARY_BUDGET
  export CANARY_RISK_CAP=$CANARY_RISK
fi

LIVE_LOG="$LOG_DIR/live_runtime.log"
LIVE_NOTIFY="$ARTIFACT_DIR/live_ready"
rm -f "$LIVE_NOTIFY"
LIVE_PID=$(start_controller "live" "$LIVE_LOG" "$LIVE_NOTIFY")
if ! wait_for_ready "$LIVE_LOG" "$LIVE_NOTIFY" "$LIVE_PID"; then
  echo "Live runtime failed to become ready" >&2
  exit $EXIT_HEALTH
fi

echo "Live runtime ready (PID=$LIVE_PID)"
GO_NO_GO="GO/NO-GO: Keys OK | Services OK | Preflight PASSED (2/2) | Soak PASSED | MODE=live | MICRO=on | Canary limits applied"
echo "$GO_NO_GO"

tail -n0 -f "$LIVE_LOG" &
TAIL_PID=$!
register_child "$TAIL_PID"

wait "$LIVE_PID"
status=$?
if [[ $status -ne 0 ]]; then
  echo "Live runtime exited with status $status" >&2
  exit $EXIT_HEALTH
fi

exit 0
