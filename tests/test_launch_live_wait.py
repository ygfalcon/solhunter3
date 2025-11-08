from __future__ import annotations

from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]


def _extract_function(source: str, name: str) -> str:
    marker = f"{name}() {{"
    start = source.find(marker)
    if start == -1:
        raise ValueError(f"Function {name} not found in launch_live.sh")
    brace_depth = 0
    end = None
    for idx in range(start, len(source)):
        char = source[idx]
        if char == '{':
            brace_depth += 1
        elif char == '}':
            brace_depth -= 1
            if brace_depth == 0:
                end = idx + 1
                break
    if end is None:
        raise ValueError(f"Failed to parse function {name} from launch_live.sh")
    # Include the trailing newline for cleanliness.
    return source[start:end] + "\n"


def test_wait_for_ready_accepts_disabled(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = _extract_function(source, "print_log_excerpt") + _extract_function(source, "wait_for_ready")

    log_path = tmp_path / "runtime.log"
    log_path.write_text(
        "\n".join(
            [
                "[ts] UI_READY url=http://localhost:1234",  # UI ready marker
                "[ts] Event bus: connected",  # Event bus connected marker
                "[ts] stage=golden:start ok=True detail=disabled",  # Golden pipeline disabled marker
                "[ts] RUNTIME_READY",  # Runtime ready marker
            ]
        )
    )

    bash_script = (
        "set -euo pipefail\n"
        + functions
        + f"READY_TIMEOUT=2\nwait_for_ready '{log_path}' ''\n"
    )

    subprocess.run(["bash", "-c", bash_script], check=True, cwd=REPO_ROOT)
