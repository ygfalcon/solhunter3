#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

export ENVIRONMENT=production
export SOLHUNTER_MODE="${SOLHUNTER_MODE:-paper}"
export GOLDEN_PIPELINE=1
export EVENT_BUS_URL="ws://127.0.0.1:8779"
export BROKER_CHANNEL="solhunter-events-v3"
export REDIS_URL="redis://localhost:6379/1"
export PRICE_PROVIDERS="pyth,dexscreener,birdeye,synthetic"
export SEED_TOKENS="So11111111111111111111111111111111111111112,EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9,Es9vMFrzaCERzSi1jS6t4G8iKrrf5gkP8KkP4dDLf2N9"

pytest -q tests/golden_pipeline/test_golden_demo.py "$@"

REPORT_JSON="$ROOT_DIR/artifacts/demo/report.json"
FRAMES_DIR="$ROOT_DIR/artifacts/demo/frames"

if [[ ! -f $REPORT_JSON ]]; then
  echo "Golden Demo completed but $REPORT_JSON was not produced" >&2
  exit 1
fi

SUMMARY=$(python3 - <<'PY'
import json
from tools.demo_payloads import REPORT_JSON_PATH, REPORT_MARKDOWN_PATH, ARTIFACT_DIR

report_path = REPORT_JSON_PATH
summary_path = REPORT_MARKDOWN_PATH
frames_dir = ARTIFACT_DIR

payload = json.loads(report_path.read_text(encoding="utf-8"))
status = str(payload.get("status", "UNKNOWN")).upper()
counts = (
    payload.get("discovery_count", 0),
    payload.get("golden_count", 0),
    payload.get("suggestion_count", 0),
)
checks = payload.get("checks") or {}
lines = [
    f"Golden Demo status: {status}",
    f"Counts: discovery={counts[0]} golden={counts[1]} suggestions={counts[2]}",
]
for name, ok in sorted(checks.items()):
    label = name.replace("_", " ")
    lines.append(f"Check {label}: {'PASS' if ok else 'FAIL'}")
lines.append(f"Frames directory: {frames_dir.resolve()}")
lines.append(f"Markdown report: {summary_path.resolve()}")
print("\n".join(lines))
PY
)

echo "$SUMMARY"
if [[ $SUMMARY == Golden\ Demo\ status:\ FAIL* ]]; then
  exit 1
fi

echo
echo "Artifacts written to $FRAMES_DIR"
ls -1 "$FRAMES_DIR" || echo "(no frames captured)"
