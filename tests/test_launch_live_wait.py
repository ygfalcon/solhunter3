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


def _build_socket_check_script(source: str, success_template: str, failure_template: str) -> str:
    functions = "".join(
        _extract_function(source, name)
        for name in ("timestamp", "log_info", "log_warn", "wait_for_socket_release", "check_event_bus_socket")
    )

    return (
        "set -euo pipefail\n"
        + functions
        + "EXIT_SOCKET=99\n"
        + f"if ! check_event_bus_socket {shlex.quote(success_template)} {shlex.quote(failure_template)}; then\n"
        + "  exit $EXIT_SOCKET\n"
        + "fi\n"
    )


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


def test_wait_for_socket_release_skips_remote_hosts() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    wait_function = _extract_function(source, "wait_for_socket_release")

    env = os.environ.copy()
    env.update(
        {
            "EVENT_BUS_URL": "wss://example.com/ws",
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

    assert "skip example.com 8779" in completed.stdout


def test_initial_socket_check_aborts_when_busy() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    bash_script = _build_socket_check_script(
        source,
        "Event bus port %s:%s ready",
        "Event bus port %s:%s is still in use after the grace window; aborting launch",
    )

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        host, port = server_sock.getsockname()

        def _accept_loop() -> None:
            try:
                while True:
                    conn, _ = server_sock.accept()
                    conn.close()
            except OSError:
                pass

        acceptor = threading.Thread(target=_accept_loop, daemon=True)
        acceptor.start()

        env = os.environ.copy()
        env.update(
            {
                "PYTHON_BIN": sys.executable,
                "EVENT_BUS_URL": f"ws://{host}:{port}",
                "EVENT_BUS_RELEASE_TIMEOUT": "0.2",
            }
        )

        completed = subprocess.run(
            ["bash", "-c", bash_script],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )

        assert completed.returncode == 99
        assert (
            f"Event bus port {host}:{port} is still in use after the grace window; aborting launch"
            in completed.stderr
        )
    finally:
        try:
            server_sock.close()
        except OSError:
            pass
        if 'acceptor' in locals():
            acceptor.join(timeout=1)


def test_second_socket_check_reports_busy_port() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    bash_script = _build_socket_check_script(
        source,
        "Event bus port %s:%s ready for live launch",
        "Event bus port %s:%s remained busy after shutting down the paper runtime; aborting live launch",
    )

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        host, port = server_sock.getsockname()

        def _accept_loop() -> None:
            try:
                while True:
                    conn, _ = server_sock.accept()
                    conn.close()
            except OSError:
                pass

        acceptor = threading.Thread(target=_accept_loop, daemon=True)
        acceptor.start()

        env = os.environ.copy()
        env.update(
            {
                "PYTHON_BIN": sys.executable,
                "EVENT_BUS_URL": f"ws://{host}:{port}",
                "EVENT_BUS_RELEASE_TIMEOUT": "0.2",
            }
        )

        completed = subprocess.run(
            ["bash", "-c", bash_script],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )

        assert completed.returncode == 99
        assert (
            f"Event bus port {host}:{port} remained busy after shutting down the paper runtime; aborting live launch"
            in completed.stderr
        )
    finally:
        try:
            server_sock.close()
        except OSError:
            pass
        if 'acceptor' in locals():
            acceptor.join(timeout=1)


def test_socket_check_reports_ready_message() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    bash_script = _build_socket_check_script(
        source,
        "Event bus port %s:%s ready for live launch",
        "Event bus port %s:%s remained busy after shutting down the paper runtime; aborting live launch",
    )

    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        host, port = probe.getsockname()
    finally:
        probe.close()

    env = os.environ.copy()
    env.update(
        {
            "PYTHON_BIN": sys.executable,
            "EVENT_BUS_URL": f"ws://{host}:{port}",
            "EVENT_BUS_RELEASE_TIMEOUT": "0.2",
        }
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert completed.returncode == 0
    assert (
        f"Event bus port {host}:{port} ready for live launch"
        in completed.stdout
    )


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
        export BROKER_URL='cache.example:6380/1'
        export BROKER_URLS='cache.example:6380/1 , cache.example:6380/1'
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
    assert exports["BROKER_URL"] == "redis://cache.example:6380/1"
    assert exports["BROKER_URLS"] == "redis://cache.example:6380/1,redis://cache.example:6380/1"
    assert exports["MINT_STREAM_BROKER_CHANNEL"] == "test-channel"
    assert exports["MEMPOOL_STREAM_BROKER_CHANNEL"] == "test-channel"
    assert exports["AMM_WATCH_BROKER_CHANNEL"] == "test-channel"
    assert "RUNTIME_MANIFEST channel=test-channel" in completed.stderr


def test_validate_env_file_handles_export(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    marker = "validate_env_file() {"
    start = source.find(marker)
    if start == -1:
        raise ValueError("validate_env_file not found in launch_live.sh")
    end = source.find("\n}\n", start)
    if end == -1:
        raise ValueError("validate_env_file did not terminate as expected")
    validate_fn = source[start : end + len("\n}\n")]

    env_file = tmp_path / "env"
    env_file.write_text(
        "\n".join(
            [
                "API_KEY=alpha",  # simple assignment
                "export PUBLIC_URL=https://service.invalid",  # exported assignment
                "export SECRET_TOKEN=beta",  # exported secret should be masked
            ]
        )
    )

    env = os.environ.copy()
    env.update(
        {
            "PYTHON_BIN": sys.executable,
            "ENV_FILE": str(env_file),
        }
    )

    bash_script = dedent(
        """
        set -euo pipefail
        %(validate)s
        validate_env_file
        """
        % {"validate": validate_fn}
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )

    output_lines = completed.stdout.strip().splitlines()
    assert "API_KEY=***" in output_lines
    assert "SECRET_TOKEN=***" in output_lines
    assert "PUBLIC_URL=https://service.invalid" in output_lines
