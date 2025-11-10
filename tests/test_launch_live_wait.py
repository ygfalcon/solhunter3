from __future__ import annotations

import os
import shlex
import socket
import subprocess
import sys
import threading
import time
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


def test_wait_for_socket_release_backoff() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    wait_function = _extract_function(source, "wait_for_socket_release")

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        host, port = server_sock.getsockname()

        def _release_socket() -> None:
            time.sleep(5.2)
            server_sock.close()

        releaser = threading.Thread(target=_release_socket, daemon=True)
        releaser.start()

        env = os.environ.copy()
        env.update(
            {
                "EVENT_BUS_URL": f"ws://{host}:{port}",
                "EVENT_BUS_RELEASE_TIMEOUT": "8",
                "PYTHON_BIN": sys.executable,
            }
        )

        bash_script = (
            "set -euo pipefail\n"
            + wait_function
            + "wait_for_socket_release\n"
        )

        completed = subprocess.run(
            ["bash", "-c", bash_script],
            check=True,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            env=env,
        )
        releaser.join()

        assert f"free {host} {port}" in completed.stdout
    finally:
        if 'releaser' in locals():
            releaser.join(timeout=6)
        # Ensure the socket is closed if the releaser thread exited early.
        try:
            server_sock.close()
        except OSError:
            pass


def test_normalize_bus_configuration_exports() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    normalize_fn = _extract_function(source, "normalize_bus_configuration")

    bash_script = dedent(
        """
        set -euo pipefail
        PYTHON_BIN=%(python)s
        export BROKER_CHANNEL='test-channel'
        export EVENT_BUS_URL='wss://bus.example/ws'
        export REDIS_URL='cache.example:6380/1'
        export MINT_STREAM_REDIS_URL='cache.example:6380/1'
        export MEMPOOL_STREAM_REDIS_URL='cache.example:6380/1'
        export AMM_WATCH_REDIS_URL='cache.example:6380/1'
        %(normalize)s
        normalize_bus_configuration
        """
        % {
            "python": shlex.quote(sys.executable),
            "normalize": normalize_fn,
        }
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        env=os.environ.copy(),
    )

    exports: dict[str, str] = {}
    for line in completed.stdout.strip().splitlines():
        assert line.startswith("export "), line
        assignment = shlex.split(line[len("export "):])[0]
        key, value = assignment.split("=", 1)
        exports[key] = value

    assert exports["BROKER_CHANNEL"] == "test-channel"
    assert exports["EVENT_BUS_URL"] == "wss://bus.example/ws"
    assert exports["REDIS_URL"] == "redis://cache.example:6380/1"
    assert exports["MINT_STREAM_REDIS_URL"] == "redis://cache.example:6380/1"
    assert exports["MEMPOOL_STREAM_REDIS_URL"] == "redis://cache.example:6380/1"
    assert exports["AMM_WATCH_REDIS_URL"] == "redis://cache.example:6380/1"
    assert exports["MINT_STREAM_BROKER_CHANNEL"] == "test-channel"
    assert exports["MEMPOOL_STREAM_BROKER_CHANNEL"] == "test-channel"
    assert exports["AMM_WATCH_BROKER_CHANNEL"] == "test-channel"
    assert "RUNTIME_MANIFEST channel=test-channel" in completed.stderr
