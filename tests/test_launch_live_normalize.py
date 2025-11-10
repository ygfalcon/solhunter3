from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


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
    return source[start:end] + "\n"


def test_normalize_preserves_credentials_and_query() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    function = _extract_function(source, "normalize_bus_configuration")

    redis_url = "redis://:pass@host:6379/1?ssl=true"
    env = os.environ.copy()
    env.update(
        {
            "PYTHON_BIN": sys.executable,
            "REDIS_URL": redis_url,
            "MINT_STREAM_REDIS_URL": redis_url,
            "MEMPOOL_STREAM_REDIS_URL": redis_url,
            "AMM_WATCH_REDIS_URL": redis_url,
        }
    )

    bash_script = (
        "set -euo pipefail\n"
        + function
        + "output=$(normalize_bus_configuration)\n"
        + "printf 'MANIFEST:%s\\n' \"$output\"\n"
        + "printf 'REDIS:%s\\n' \"$REDIS_URL\"\n"
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        check=True,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )

    stdout = completed.stdout.strip().splitlines()
    assert any(line.startswith("REDIS:") for line in stdout)
    manifest_line = next(line for line in stdout if line.startswith("MANIFEST:"))
    redis_line = next(line for line in stdout if line.startswith("REDIS:"))

    assert "redis://:pass@host:6379/1?ssl=true" in redis_line
    assert "redis://:***@host:6379/1?ssl=true" in manifest_line
