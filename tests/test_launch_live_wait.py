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

import pytest


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


def test_ensure_virtualenv_respects_skip_flag(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    ensure_fn = _extract_function(source, "ensure_virtualenv")
    detect_fn = _extract_function(source, "detect_pip_online")

    venv_dir = tmp_path / "venv"
    bin_dir = venv_dir / "bin"
    bin_dir.mkdir(parents=True)
    python_bin = bin_dir / "python3"
    python_bin.write_text("#!/usr/bin/env bash\nexit 0\n")
    python_bin.chmod(0o755)

    activate = bin_dir / "activate"
    activate.write_text("#!/usr/bin/env bash\nexport VIRTUAL_ENV=$VENV_DIR\n")
    activate.chmod(0o755)

    pip_log = tmp_path / "pip_called.log"
    pip_bin = bin_dir / "pip"
    pip_bin.write_text(
        "#!/usr/bin/env bash\n"
        f"printf '%s\\n' \"$*\" >> {shlex.quote(str(pip_log))}\n"
        "exit 0\n"
    )
    pip_bin.chmod(0o755)

    bash_script = dedent(
        f"""
        set -euo pipefail
        ROOT_DIR={shlex.quote(str(REPO_ROOT))}
        VENV_DIR={shlex.quote(str(venv_dir))}
        PYTHON_BIN={shlex.quote(str(python_bin))}
        PIP_BIN={shlex.quote(str(pip_bin))}
        LOG_DIR={shlex.quote(str(tmp_path))}
        export LAUNCH_LIVE_SKIP_PIP=1
        log_info() {{ printf '%s\\n' "$*"; }}
        log_warn() {{ printf '%s\\n' "$*" >&2; }}
        {detect_fn}
        {ensure_fn}
        ensure_virtualenv
        """
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "LAUNCH_LIVE_SKIP_PIP is set" in completed.stdout
    assert not pip_log.exists()


def test_start_log_stream_preserves_existing_content(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    start_log_stream_fn = _extract_function(source, "start_log_stream")

    log_path = tmp_path / "runtime.log"
    log_path.write_text("MARKER\n")

    bash_script = dedent(
        """
        set -euo pipefail
        declare -a CHILD_PIDS=()
        timestamp() { echo ts; }
        register_child() {
          CHILD_PIDS+=("$1")
        }
        """
        + start_log_stream_fn
        + dedent(
            f"""
        log_file={shlex.quote(str(log_path))}
        start_log_stream "$log_file" "test"
        sleep 1
        echo 'AFTER' >> "$log_file"
        sleep 1
        for pid in "${{CHILD_PIDS[@]}}"; do
          kill "$pid" >/dev/null 2>&1 || true
        done
        for pid in $(jobs -p); do
          kill "$pid" >/dev/null 2>&1 || true
        done
        for pid in $(pgrep -f "tail -n \\+1 -F $log_file"); do
          kill "$pid" >/dev/null 2>&1 || true
        done
        sleep 1
        """
        )
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    assert "[ts] [test] MARKER" in lines
    assert "[ts] [test] AFTER" in lines
    assert log_path.read_text().splitlines() == ["MARKER", "AFTER"]


@pytest.mark.parametrize(
    "golden_line",
    [
        "[ts] stage=golden:start ok=True detail=disabled",
        "[ts] golden:start disabled",
    ],
)
def test_wait_for_ready_accepts_disabled(tmp_path: Path, golden_line: str) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = _extract_function(source, "print_log_excerpt") + _extract_function(source, "wait_for_ready")

    log_path = tmp_path / "runtime.log"
    log_path.write_text(
        "\n".join(
            [
                "[ts] UI_READY url=http://localhost:1234",  # UI ready marker
                "[ts] UI_WS_READY status=ok rl_ws=ws://localhost:2345 events_ws=ws://localhost:3456 logs_ws=ws://localhost:4567",
                "[ts] Event bus: connected",  # Event bus connected marker
                golden_line,
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


def test_check_ui_health_skips_when_disabled_flag(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = (
        _extract_function(source, "timestamp")
        + _extract_function(source, "log_info")
        + _extract_function(source, "log_warn")
        + _extract_function(source, "extract_ui_url")
        + _extract_function(source, "check_ui_health")
    )

    log_path = tmp_path / "runtime.log"
    log_path.write_text("")

    python_bin = shlex.quote(sys.executable)
    bash_script = (
        "set -euo pipefail\n"
        + functions
        + f"PYTHON_BIN={python_bin}\n"
        + "UI_DISABLE_HTTP_SERVER=1\n"
        + f"check_ui_health '{log_path}'\n"
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert "UI health check skipped: HTTP server disabled (UI_DISABLE_HTTP_SERVER=1" in completed.stdout


def test_check_ui_health_skips_when_port_zero(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = (
        _extract_function(source, "timestamp")
        + _extract_function(source, "log_info")
        + _extract_function(source, "log_warn")
        + _extract_function(source, "extract_ui_url")
        + _extract_function(source, "check_ui_health")
    )

    log_path = tmp_path / "runtime.log"
    log_path.write_text("[ts] UI_READY url=unavailable\n")

    python_bin = shlex.quote(sys.executable)
    bash_script = (
        "set -euo pipefail\n"
        + functions
        + f"PYTHON_BIN={python_bin}\n"
        + f"check_ui_health '{log_path}'\n"
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert "UI health check skipped: HTTP server disabled" in completed.stdout
    assert "ui_url=unavailable" in completed.stdout


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

    assert "free example.com 8779 remote" in completed.stdout


def test_launch_live_ui_port_busy_exit() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = (
        _extract_function(source, "timestamp")
        + _extract_function(source, "log_info")
        + _extract_function(source, "log_warn")
        + _extract_function(source, "wait_for_ui_socket_release")
    )

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        host, port = server_sock.getsockname()

        env = os.environ.copy()
        env.update(
            {
                "PYTHON_BIN": sys.executable,
                "UI_HOST": host,
                "UI_PORT": str(port),
                "UI_SOCKET_RELEASE_TIMEOUT": "0",
            }
        )

        bash_script = (
            "set -euo pipefail\n"
            "EXIT_SOCKET=7\n"
            + functions
            + dedent(
                """
        UI_SOCKET_STATE_RAW=$(wait_for_ui_socket_release)
        read -r UI_SOCKET_STATE UI_SOCKET_HOST UI_SOCKET_PORT UI_SOCKET_REASON <<<"$UI_SOCKET_STATE_RAW"
        UI_SOCKET_HOST=${UI_SOCKET_HOST:-${UI_HOST:-127.0.0.1}}
        UI_SOCKET_PORT=${UI_SOCKET_PORT:-${UI_PORT:-5001}}
        UI_SOCKET_REASON=${UI_SOCKET_REASON:-}
        if [[ $UI_SOCKET_STATE == "busy" ]]; then
          log_warn "UI port ${UI_SOCKET_HOST}:${UI_SOCKET_PORT} is still in use after the grace window; aborting launch"
          log_warn "Hint: terminate the process bound to ${UI_SOCKET_HOST}:${UI_SOCKET_PORT} or adjust UI_HOST/UI_PORT before retrying"
          exit $EXIT_SOCKET
        else
          case $UI_SOCKET_REASON in
            remote)
              log_info "UI port ${UI_SOCKET_HOST}:${UI_SOCKET_PORT} ready (remote endpoint; skipping local socket release verification)"
              ;;
            disabled)
              log_info "UI port ${UI_SOCKET_HOST}:${UI_SOCKET_PORT} check skipped (UI port disabled)"
              ;;
            *)
              log_info "UI port ${UI_SOCKET_HOST}:${UI_SOCKET_PORT} ready"
              ;;
          esac
        fi
        """
            )
        )

        completed = subprocess.run(
            ["bash", "-c", bash_script],
            check=False,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            env=env,
        )

        assert completed.returncode == 7
        assert "UI port" in completed.stderr
        assert "Hint: terminate the process bound" in completed.stderr
    finally:
        server_sock.close()


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


def test_normalize_bus_configuration_preserves_credentials_and_redacts_logs() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    normalize_fn = _extract_function(source, "normalize_bus_configuration")

    redis_url = "redis://:pass@host:6379/1?ssl=true"

    bash_script = dedent(
        """
        set -euo pipefail
        PYTHON_BIN=%(python)s
        export BROKER_CHANNEL='secure-channel'
        export REDIS_URL=%(redis)s
        export MINT_STREAM_REDIS_URL=%(redis)s
        export MEMPOOL_STREAM_REDIS_URL=%(redis)s
        export AMM_WATCH_REDIS_URL=%(redis)s
        export BROKER_URL=%(redis)s
        export BROKER_URLS=%(redis)s
        %(normalize)s
        normalize_bus_configuration
        """
        % {
            "python": shlex.quote(sys.executable),
            "redis": shlex.quote(redis_url),
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

    assert exports["REDIS_URL"] == redis_url
    assert exports["MINT_STREAM_REDIS_URL"] == redis_url
    assert exports["MEMPOOL_STREAM_REDIS_URL"] == redis_url
    assert exports["AMM_WATCH_REDIS_URL"] == redis_url
    assert exports["BROKER_URL"] == redis_url
    assert exports["BROKER_URLS"] == redis_url

    assert "redis://****@host:6379/1?ssl=true" in completed.stderr
    assert redis_url not in completed.stderr

def test_normalize_bus_configuration_canonicalizes_broker_url() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    normalize_fn = _extract_function(source, "normalize_bus_configuration")

    env = os.environ.copy()
    env.update(
        {
            "PYTHON_BIN": sys.executable,
            "REDIS_URL": "redis://localhost:6380/1",
            "MINT_STREAM_REDIS_URL": "redis://localhost:6380/1",
            "MEMPOOL_STREAM_REDIS_URL": "redis://localhost:6380/1",
            "AMM_WATCH_REDIS_URL": "redis://localhost:6380/1",
            "BROKER_URL": "redis://localhost:6380/2",
        }
    )

    bash_script = (
        "set -euo pipefail\n"
        + normalize_fn
        + "normalize_bus_configuration\n"
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
    )

    assert "export BROKER_URL=redis://localhost:6380/1" in completed.stdout
    assert "redis://localhost:6380/2" not in completed.stdout


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
