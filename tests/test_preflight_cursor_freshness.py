import subprocess
from textwrap import dedent
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _extract_function(source: str, name: str) -> str:
    marker = f"{name}() {{"
    start = source.find(marker)
    if start == -1:
        raise ValueError(f"Function {name} not found in bus_smoke.sh")
    brace_depth = 0
    end = None
    for idx, char in enumerate(source[start:], start=start):
        if char == "{":
            brace_depth += 1
        elif char == "}":
            brace_depth -= 1
            if brace_depth == 0:
                end = idx + 1
                break
    if end is None:
        raise ValueError(f"Failed to parse function {name} from bus_smoke.sh")
    return source[start:end] + "\n"


def test_cursor_freshness_flags_stale_cursor(tmp_path: Path) -> None:
    source = (REPO_ROOT / "scripts" / "preflight" / "bus_smoke.sh").read_text()
    cursor_fn = _extract_function(source, "check_cursor_freshness")

    bash_template = dedent(
        """
        set -euo pipefail
        failures=0
        check_details=()
        REDIS_URL=redis://example.test:6379/0
        __CURSOR_FN__
        redis() {
          if [[ $1 == "TTL" ]]; then
            echo 5
          else
            echo "unexpected redis command: $*" >&2
            exit 99
          fi
        }
        jq() {
          echo '{"stub":true}'
        }
        log() { :; }
        pass() { echo "PASS:$*"; }
        warn() { echo "WARN:$*"; }
        fail() { echo "FAIL:$*" >&2; }
        record_audit() { :; }
        PREFLIGHT_CURSOR_MAX_AGE_SEC=3
        DISCOVERY_RECENT_TTL_SEC=20
        check_cursor_freshness
        echo "FAILURES=$failures"
        echo "DETAIL=${check_details[0]-}"
        """
    )
    bash_script = bash_template.replace("__CURSOR_FN__", cursor_fn)

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert "FAILURES=1" in completed.stdout
    assert "backfill" in completed.stderr
    assert "redis-cli -u \"redis://example.test:6379/0\" DEL discovery:cursor" in completed.stderr
    assert "stale" in completed.stderr
    assert "stub" in completed.stdout
