from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


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


def test_check_ui_health_retries(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = "".join(
        _extract_function(source, name)
        for name in ("timestamp", "log_info", "log_warn", "extract_ui_url", "check_ui_health")
    )

    log_path = tmp_path / "runtime.log"
    log_path.write_text("[ts] UI_READY url=http://localhost:1234\n")

    stub_state = tmp_path / "health_stub"
    sitecustomize = tmp_path / "sitecustomize.py"
    sitecustomize.write_text(
        "from __future__ import annotations\n"
        "import os\n"
        "from pathlib import Path\n"
        "from urllib import request\n"
        "from urllib.error import URLError\n"
        "STATE_FILE = Path(os.environ['UI_HEALTH_STUB_FILE'])\n"
        "class _Response:\n"
        "    status = 200\n"
        "    def __enter__(self):\n"
        "        return self\n"
        "    def __exit__(self, exc_type, exc, tb):\n"
        "        return False\n"
        "def _should_fail() -> bool:\n"
        "    if STATE_FILE.exists():\n"
        "        return False\n"
        "    STATE_FILE.write_text('used')\n"
        "    return True\n"
        "def fake_urlopen(*args, **kwargs):\n"
        "    if _should_fail():\n"
        "        raise URLError('temporary failure')\n"
        "    return _Response()\n"
        "request.urlopen = fake_urlopen\n"
    )

    env = os.environ.copy()
    env["PYTHON_BIN"] = sys.executable
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = f"{tmp_path}:{existing_pythonpath}" if existing_pythonpath else str(tmp_path)
    env["UI_HEALTH_STUB_FILE"] = str(stub_state)

    bash_script = "set -euo pipefail\n" + functions + f"check_ui_health '{log_path}'\n"

    subprocess.run(["bash", "-c", bash_script], check=True, cwd=REPO_ROOT, env=env)
