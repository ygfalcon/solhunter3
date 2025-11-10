from __future__ import annotations

import json
import shlex
import subprocess
import sys
from textwrap import dedent
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _extract_function(source: str, name: str) -> str:
    marker = f"{name}() {{"
    start = source.find(marker)
    if start == -1:
        raise ValueError(f"Function {name} not found in launch_live.sh")
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
        raise ValueError(f"Failed to parse function {name} from launch_live.sh")
    return source[start:end] + "\n"


def test_validate_connectivity_soak_aborts_on_failures(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = (
        _extract_function(source, "timestamp")
        + _extract_function(source, "log_info")
        + _extract_function(source, "log_warn")
        + _extract_function(source, "validate_connectivity_soak")
    )

    python_bin = shlex.quote(sys.executable)
    soak_payload = json.dumps(
        {
            "duration": 5,
            "reconnect_count": 1,
            "metrics": {
                "ui-ws": {"errors": {"disconnect": 2}},
            },
        }
    )
    report_path = tmp_path / "connectivity.json"

    bash_script = dedent(
        f"""
        set -euo pipefail
        {functions}
        PYTHON_BIN={python_bin}
        if validate_connectivity_soak '{soak_payload}' '{report_path}'; then
            exit 0
        else
            exit 23
        fi
        """
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 23
    assert "Connectivity soak reported failures:" in completed.stderr
    assert "reconnect_count=1" in completed.stderr
    assert "ui-ws errors: disconnect=2" in completed.stderr
    assert f"report={report_path}" in completed.stderr
