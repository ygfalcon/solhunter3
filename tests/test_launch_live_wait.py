from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from textwrap import dedent


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


def test_acquire_runtime_lock_reclaims_remote_host(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = dedent(
        """
        log_info() {
          :
        }

        log_warn() {
          :
        }
        """
    ) + _extract_function(source, "acquire_runtime_lock")

    fake_store = tmp_path / "redis_store.json"
    fake_store.write_text("{}")
    redis_stub = dedent(
        """
        import json
        import os

        _STORE_PATH = os.environ["FAKE_REDIS_PATH"]

        def _load() -> dict[str, str]:
            try:
                with open(_STORE_PATH, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except FileNotFoundError:
                return {}
            except json.JSONDecodeError:
                return {}

        def _dump(data: dict[str, str]) -> None:
            tmp_path = f"{_STORE_PATH}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(data, fh)
            os.replace(tmp_path, _STORE_PATH)

        class Redis:
            def __init__(self) -> None:
                pass

            @classmethod
            def from_url(cls, url: str, socket_timeout: float | None = None) -> "Redis":
                return cls()

            def get(self, key: str) -> bytes | None:
                value = _load().get(key)
                if value is None:
                    return None
                if isinstance(value, bytes):
                    return value
                return str(value).encode("utf-8")

            def set(self, key: str, value: str | bytes, nx: bool = False) -> bool:
                data = _load()
                if nx and key in data:
                    return False
                if isinstance(value, bytes):
                    payload = value.decode("utf-8", "ignore")
                else:
                    payload = value
                data[key] = payload
                _dump(data)
                return True

            def delete(self, key: str) -> None:
                data = _load()
                if key in data:
                    data.pop(key)
                    _dump(data)

            def close(self) -> None:  # pragma: no cover - API parity only
                pass
        """
    )
    (tmp_path / "redis.py").write_text(redis_stub)

    channel = "solhunter-events-v3"
    key = f"solhunter:runtime:lock:{channel}"
    remote_lock = {
        "pid": 4321,
        "channel": channel,
        "host": "remote-host",
        "token": "stale-token",
        "ts": 1234567890.0,
    }
    fake_store.write_text(json.dumps({key: json.dumps(remote_lock)}))

    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH")
    if current_pythonpath:
        pythonpath = f"{tmp_path}:{current_pythonpath}"
    else:
        pythonpath = str(tmp_path)
    env.update(
        {
            "PYTHON_BIN": sys.executable,
            "PYTHONPATH": pythonpath,
            "FAKE_REDIS_PATH": str(fake_store),
            "REDIS_URL": "redis://localhost:6379/1",
            "BROKER_CHANNEL": channel,
        }
    )

    bash_script = (
        "set -euo pipefail\n"
        + functions
        + "acquire_runtime_lock\n"
        + "printf '%s %s\n' \"$RUNTIME_LOCK_KEY\" \"$RUNTIME_LOCK_TOKEN\"\n"
    )

    result = subprocess.run(
        ["bash", "-c", bash_script],
        capture_output=True,
        check=True,
        cwd=REPO_ROOT,
        env=env,
        text=True,
    )

    output_lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert output_lines, "Expected runtime lock output"
    lock_key, token = output_lines[-1].split()
    assert lock_key == key
    assert token
    assert "Treating as stale" in result.stderr

    payload_store = json.loads(fake_store.read_text())
    payload = json.loads(payload_store[lock_key])
    assert payload["host"] == socket.gethostname()
    assert payload["token"] == token
