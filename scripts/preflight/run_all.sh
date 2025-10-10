#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/preflight/common.sh
source "$SCRIPT_DIR/common.sh"

ARTIFACT_DIR=$(ensure_artifact_dir)

failure_banner() {
  cat <<'MSG' >&2
Likely cause → fix
  • No Golden snapshot → verify coalescer and join subscriptions are running.
  • Duplicate decisions → ensure KV idempotency on (mint, side, inputs_hash).
  • Missing virtual fills → confirm the shadow executor listens to x:vote.decisions.
  • Must-exit latency high → inspect bypass wiring from monitor to executor.
  • Micro gates leaking → enforce liquidity gates before voting and at final guard.
MSG
}

trap 'status=$?; if (( status != 0 )); then failure_banner; fi' EXIT

run_micro_suite() {
  local micro=$1
  log INFO "==== Running preflight for MICRO_MODE=$micro ===="
  MICRO_MODE=$micro "$SCRIPT_DIR/env_doctor.sh"
  MICRO_MODE=$micro "$SCRIPT_DIR/bus_smoke.sh"
  MICRO_MODE=$micro "$SCRIPT_DIR/preflight_smoke.sh"
  local latest
  latest=$(ls -1t "$ARTIFACT_DIR"/*-micro${micro}.json 2>/dev/null | head -n1 || true)
  if [[ -z $latest ]]; then
    abort "Unable to locate preflight report for MICRO_MODE=$micro"
  fi
  printf '%s\n' "$latest"
}

main() {
  local -a run_entries=()
  for micro in 1 0; do
    local report_path
    report_path=$(run_micro_suite "$micro")
    run_entries+=("${micro}:${report_path}")
  done

  local marker_info
  marker_info=$(python - <<'PY' "$ARTIFACT_DIR" "${MODE:-}" "${PRE_FLIGHT_COMMIT}" "${run_entries[@]}"
import json
import sys
import time
from pathlib import Path


def build_entry(arg: str) -> tuple[int, Path]:
    micro_str, path = arg.split(":", 1)
    return int(micro_str), Path(path)


artifact_dir = Path(sys.argv[1])
mode = sys.argv[2]
commit = sys.argv[3]
entries = [build_entry(arg) for arg in sys.argv[4:]]
now = time.time()
summary = {
    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
    "ts_epoch": now,
    "mode": mode,
    "commit": commit,
    "reports": [],
}
for micro, path in entries:
    data = json.loads(path.read_text())
    summary["reports"].append(
        {
            "micro_mode": micro,
            "report": str(path),
            "ts": data.get("ts"),
            "golden_hash": data.get("golden_hash"),
        }
    )

history_path = artifact_dir / "run_history.jsonl"
previous = None
if history_path.exists():
    lines = [line.strip() for line in history_path.read_text().splitlines() if line.strip()]
    if lines:
        previous = json.loads(lines[-1])
with history_path.open("a") as fh:
    fh.write(json.dumps(summary) + "\n")

marker_path = artifact_dir / "latest-pass.marker"
status = {"state": "pending", "reason": "first_run", "diff": None}
if previous is not None:
    prev_epoch = previous.get("ts_epoch")
    diff = None
    if isinstance(prev_epoch, (int, float)):
        diff = summary["ts_epoch"] - prev_epoch
    prev_hashes = {str(r.get("micro_mode")): r.get("golden_hash") for r in previous.get("reports", [])}
    curr_hashes = {str(r.get("micro_mode")): r.get("golden_hash") for r in summary["reports"]}
    hash_changed = any(
        curr_hashes.get(key) and curr_hashes.get(key) != prev_hashes.get(key)
        for key in curr_hashes
    )
    if diff is not None and diff >= 900 and hash_changed:
        marker_path.write_text(summary["ts"] + "\n")
        status = {"state": "ready", "diff": diff}
    else:
        if marker_path.exists():
            marker_path.unlink()
        status = {
            "state": "pending",
            "reason": "hash_unchanged" if hash_changed is False else "need_second_pass",
            "diff": diff,
        }
else:
    if marker_path.exists():
        marker_path.unlink()

print(json.dumps(status))
PY
  )

  if [[ -n $marker_info ]]; then
    local marker_state
    marker_state=$(jq -r '.state' <<<"$marker_info")
    if [[ $marker_state == "ready" ]]; then
      local diff
      diff=$(jq -r '.diff' <<<"$marker_info")
      pass "Launch gate satisfied – consecutive runs ${diff}s apart"
    else
      local reason
      reason=$(jq -r '.reason // "need_second_pass"' <<<"$marker_info")
      warn "Launch marker pending (${reason}). Run the suite again in ≥15m with fresh market data."
    fi
  fi
}

main "$@"
