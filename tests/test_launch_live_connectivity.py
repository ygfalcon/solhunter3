from __future__ import annotations

import json
import os
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


def _write_stub_connectivity(tmp_path: Path) -> Path:
    root = tmp_path / "stub_pkg"
    production_dir = root / "solhunter_zero" / "production"
    production_dir.mkdir(parents=True)
    (root / "solhunter_zero" / "__init__.py").write_text("__all__ = ['production']\n")
    stub_source = dedent(
        """
        import os
        from types import SimpleNamespace


        class ConnectivityChecker:
            def __init__(self, *_, **__):
                self.mode = os.environ.get("CONNECTIVITY_TEST_MODE", "loopback")

            async def check_all(self):
                if self.mode == "remote-fail":
                    return [
                        SimpleNamespace(
                            name="redis",
                            target="redis://redis.example:6379/0",
                            ok=True,
                            latency_ms=4.2,
                            status="ok",
                            status_code=200,
                            error=None,
                        ),
                        SimpleNamespace(
                            name="solana-rpc",
                            target="https://rpc.example",
                            ok=False,
                            latency_ms=None,
                            status=None,
                            status_code=None,
                            error="rpc timeout",
                        ),
                        SimpleNamespace(
                            name="solana-ws",
                            target="wss://ws.example",
                            ok=True,
                            latency_ms=5.0,
                            status="ok",
                            status_code=101,
                            error=None,
                        ),
                    ]
                return [
                    SimpleNamespace(
                        name="redis",
                        target="redis://127.0.0.1:6379/0",
                        ok=False,
                        latency_ms=None,
                        status=None,
                        status_code=None,
                        error="connection refused",
                    ),
                    SimpleNamespace(
                        name="solana-rpc",
                        target="https://rpc.example",
                        ok=True,
                        latency_ms=12.5,
                        status="ok",
                        status_code=200,
                        error=None,
                    ),
                    SimpleNamespace(
                        name="solana-ws",
                        target="wss://ws.example",
                        ok=True,
                        latency_ms=5.0,
                        status="ok",
                        status_code=101,
                        error=None,
                    ),
                ]

            async def _probe_ws(self, name, url):
                return SimpleNamespace(
                    name=name,
                    target=url,
                    ok=True,
                    latency_ms=6.4,
                    status="ok",
                    status_code=101,
                    error=None,
                )
        """
    )
    (production_dir / "__init__.py").write_text(stub_source)
    return root


def _run_connectivity_function(
    source: str, env: dict[str, str], *, cwd: Path
) -> subprocess.CompletedProcess:
    functions = _extract_function(source, "run_connectivity_probes")
    bash_script = dedent(
        f"""
        set -euo pipefail
        {functions}
        if CONNECTIVITY_REPORT=$(run_connectivity_probes --grace-loopback 2>&1); then
            printf '%s\n' "$CONNECTIVITY_REPORT"
            exit 0
        else
            printf '%s\n' "$CONNECTIVITY_REPORT"
            exit 23
        fi
        """
    )
    return subprocess.run(
        ["bash", "-c", bash_script],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def test_run_connectivity_probes_grace_allows_loopback(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    stub_root = _write_stub_connectivity(tmp_path)
    env = os.environ.copy()
    python_path = env.get("PYTHONPATH")
    env.update(
        {
            "PYTHON_BIN": sys.executable,
            "EVENT_BUS_URL": "wss://bus.example/ws",
            "CONNECTIVITY_TEST_MODE": "loopback",
            "PYTHONPATH": f"{stub_root}{os.pathsep}{python_path}" if python_path else str(stub_root),
        }
    )

    completed = _run_connectivity_function(source, env, cwd=stub_root)

    assert completed.returncode == 0
    assert "Redis: WARN" in completed.stdout
    assert "loopback target unreachable (deferred)" in completed.stdout
    assert "connectivity check deferred for loopback target" in completed.stdout


def test_run_connectivity_probes_grace_still_flags_remote_failures(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    stub_root = _write_stub_connectivity(tmp_path)
    env = os.environ.copy()
    python_path = env.get("PYTHONPATH")
    env.update(
        {
            "PYTHON_BIN": sys.executable,
            "EVENT_BUS_URL": "wss://bus.example/ws",
            "CONNECTIVITY_TEST_MODE": "remote-fail",
            "PYTHONPATH": f"{stub_root}{os.pathsep}{python_path}" if python_path else str(stub_root),
        }
    )

    completed = _run_connectivity_function(source, env, cwd=stub_root)

    assert completed.returncode == 23
    assert "Solana RPC: FAIL" in completed.stdout
    assert "Solana RPC connectivity check failed" in completed.stdout


def test_launch_live_rechecks_connectivity_after_runtime() -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    contents = script_path.read_text()
    assert "run_connectivity_probes --grace-loopback" in contents
    assert "Re-running connectivity probes with paper runtime online" in contents
