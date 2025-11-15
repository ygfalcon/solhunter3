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


def _extract_ui_skip_helpers(source: str) -> str:
    functions = [
        "connectivity_flag_truthy",
        "connectivity_ui_offline_requested",
        "connectivity_ui_probes_skipped",
        "connectivity_skip_ui_probes_push",
        "connectivity_skip_ui_probes_pop",
    ]
    return "".join(_extract_function(source, name) for name in functions)


def _extract_connectivity_soak_block(source: str) -> str:
    start_marker = "# Preserve the caller's CONNECTIVITY_SKIP_UI_PROBES configuration while the"
    start = source.find(start_marker)
    if start == -1:
        raise ValueError("Failed to locate connectivity soak block in launch_live.sh")
    pop_marker = "connectivity_skip_ui_probes_pop"
    pop_index = source.find(pop_marker, start)
    if pop_index == -1:
        raise ValueError("Failed to locate connectivity soak cleanup in launch_live.sh")
    end = source.find("\nfi", pop_index)
    if end == -1:
        raise ValueError("Failed to locate connectivity soak block terminator in launch_live.sh")
    end = source.find("\n", end + 1)
    if end == -1:
        end = len(source)
    return source[start:end]


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


def test_connectivity_skip_ui_probes_state_restored_after_soak(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    soak_block = _extract_connectivity_soak_block(source)
    helpers = _extract_ui_skip_helpers(source)

    report_path = tmp_path / "connectivity_report.json"
    artifact_dir = tmp_path / "artifacts"
    stub_root = tmp_path / "stub"
    package_dir = stub_root / "solhunter_zero"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    stub_source = (
        "from __future__ import annotations\n"
        "from pathlib import Path\n\n"
        "class _Summary:\n"
        "    def __init__(self) -> None:\n"
        "        self.duration = 0.0\n"
        "        self.reconnect_count = 0\n"
        "        self.metrics = {}\n\n"
        "class ConnectivityChecker:\n"
        "    async def run_soak(self, duration: float, output_path):\n"
        "        path = Path(output_path)\n"
        "        path.parent.mkdir(parents=True, exist_ok=True)\n"
        "        path.write_text('{}', encoding='utf-8')\n"
        "        return _Summary()\n"
    )
    (package_dir / "production.py").write_text(stub_source, encoding="utf-8")
    python_bin = shlex.quote(sys.executable)
    preserved_value = "restore-me"
    quoted_report = shlex.quote(str(report_path))
    quoted_value = shlex.quote(preserved_value)
    quoted_artifact_dir = shlex.quote(str(artifact_dir))
    quoted_stub_root = shlex.quote(str(stub_root))

    bash_script = dedent(
        f"""
        set -euo pipefail
        timestamp() {{ printf 'stub'; }}
        log_info() {{ :; }}
        log_warn() {{ :; }}
        runtime_lock_ttl_check() {{ return 0; }}
        export CONNECTIVITY_SKIP_UI_PROBES={quoted_value}
        export SOAK_DURATION=0
        export SOAK_REPORT={quoted_report}
        export ARTIFACT_DIR={quoted_artifact_dir}
        export PYTHON_BIN={python_bin}
        export PYTHONPATH={quoted_stub_root}
        export EXIT_CONNECTIVITY=99
        {helpers}
        {soak_block}
        if [[ -n ${{CONNECTIVITY_SKIP_UI_PROBES+x}} ]]; then
            printf 'restored=%s\n' "$CONNECTIVITY_SKIP_UI_PROBES"
        else
            printf 'restored=__unset__\n'
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

    assert completed.returncode == 0, completed.stderr
    assert "restored=restore-me" in completed.stdout


def test_connectivity_skip_ui_probes_headless_launch_retains_flag(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    soak_block = _extract_connectivity_soak_block(source)
    helpers = _extract_ui_skip_helpers(source)

    report_path = tmp_path / "connectivity_report.json"
    artifact_dir = tmp_path / "artifacts"
    stub_root = tmp_path / "stub_headless"
    package_dir = stub_root / "solhunter_zero"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    stub_source = (
        "from __future__ import annotations\n"
        "from pathlib import Path\n\n"
        "class _Summary:\n"
        "    def __init__(self) -> None:\n"
        "        self.duration = 0.0\n"
        "        self.reconnect_count = 0\n"
        "        self.metrics = {}\n\n"
        "class ConnectivityChecker:\n"
        "    async def run_soak(self, duration: float, output_path):\n"
        "        path = Path(output_path)\n"
        "        path.parent.mkdir(parents=True, exist_ok=True)\n"
        "        path.write_text('{}', encoding='utf-8')\n"
        "        return _Summary()\n"
    )
    (package_dir / "production.py").write_text(stub_source, encoding="utf-8")
    python_bin = shlex.quote(sys.executable)
    quoted_report = shlex.quote(str(report_path))
    quoted_artifact_dir = shlex.quote(str(artifact_dir))
    quoted_stub_root = shlex.quote(str(stub_root))

    bash_script = dedent(
        f"""
        set -euo pipefail
        timestamp() {{ printf 'stub'; }}
        log_info() {{ :; }}
        log_warn() {{ :; }}
        runtime_lock_ttl_check() {{ return 0; }}
        export UI_DISABLE_HTTP_SERVER=1
        export SOAK_DURATION=0
        export SOAK_REPORT={quoted_report}
        export ARTIFACT_DIR={quoted_artifact_dir}
        export PYTHON_BIN={python_bin}
        export PYTHONPATH={quoted_stub_root}
        export EXIT_CONNECTIVITY=99
        {helpers}
        {soak_block}
        if [[ -n ${{CONNECTIVITY_SKIP_UI_PROBES+x}} ]]; then
            printf 'headless_flag=%s\n' "$CONNECTIVITY_SKIP_UI_PROBES"
        else
            printf 'headless_flag=__unset__\n'
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

    assert completed.returncode == 0, completed.stderr
    assert "headless_flag=1" in completed.stdout


def _write_connectivity_stub(target_dir: Path) -> None:
    package_dir = target_dir / "solhunter_zero"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    stub_source = (
        "from __future__ import annotations\n"
        "import os\n"
        "from pathlib import Path\n\n"
        "class _Result:\n"
        "    def __init__(self, name: str, target: str, ok: bool = True, error: str | None = None) -> None:\n"
        "        self.name = name\n"
        "        self.target = target\n"
        "        self.ok = ok\n"
        "        self.error = error\n"
        "        self.status = None\n"
        "        self.status_code = None\n"
        "        self.latency_ms = 12.3\n\n"
        "class ConnectivityChecker:\n"
        "    def __init__(self) -> None:\n"
        "        marker = os.environ.get(\"PROBE_MARKER\")\n"
        "        self._marker = Path(marker) if marker else None\n\n"
        "    async def check_all(self):\n"
        "        return [\n"
        "            _Result(\"redis\", \"redis://localhost/0\"),\n"
        "            _Result(\"solana-rpc\", \"https://solana-rpc.local\"),\n"
        "            _Result(\"solana-ws\", \"wss://solana-ws.local\"),\n"
        "            _Result(\"helius-rest\", \"https://helius-rest.local\"),\n"
        "            _Result(\"helius-das\", \"https://helius-das.local\"),\n"
        "        ]\n\n"
        "    async def _probe_ws(self, name: str, target: str):\n"
        "        if self._marker is not None:\n"
        "            self._marker.write_text(f\"{name} {target}\", encoding=\"utf-8\")\n"
        "        return _Result(name, target, ok=False, error=\"unreachable\")\n"
    )
    (package_dir / "production.py").write_text(stub_source, encoding="utf-8")


def _create_python_wrapper(target_dir: Path) -> Path:
    wrapper = target_dir / "python_wrapper.py"
    wrapper.write_text(
        dedent(
            """\
            #!/usr/bin/env python3
            import os
            import runpy
            import sys

            stub_root = os.environ.get("STUB_ROOT")
            if stub_root:
                sys.path.insert(0, stub_root)

            args = sys.argv[1:]
            if not args:
                sys.exit(1)

            if args[0] == "-":
                sys.argv = args
                code = sys.stdin.read()
                exec(compile(code, "<stdin>", "exec"), {"__name__": "__main__"})
            else:
                sys.argv = args
                runpy.run_path(args[0], run_name="__main__")
            """
        ),
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return wrapper


def test_run_connectivity_probes_skips_local_event_bus(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    helpers = _extract_ui_skip_helpers(source)
    run_probes = helpers + _extract_function(source, "run_connectivity_probes")

    stub_root = tmp_path / "stub_local"
    _write_connectivity_stub(stub_root)

    python_wrapper = _create_python_wrapper(tmp_path)
    python_bin = shlex.quote(str(python_wrapper))
    quoted_stub_root = shlex.quote(str(stub_root))
    marker_path = tmp_path / "marker_local.txt"
    quoted_marker = shlex.quote(str(marker_path))

    bash_script = dedent(
        f"""
        set -euo pipefail
        {run_probes}
        export PYTHON_BIN={python_bin}
        export STUB_ROOT={quoted_stub_root}
        export EVENT_BUS_URL=ws://127.0.0.1:8779
        export PROBE_MARKER={quoted_marker}
        run_connectivity_probes
        """
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert (
        "Event bus: SKIPPED → runtime-managed local endpoint ws://127.0.0.1:8779 "
        "(post-launch readiness checks will verify availability)" in completed.stdout
    )
    assert not marker_path.exists()


def test_run_connectivity_probes_requires_remote_event_bus(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    helpers = _extract_ui_skip_helpers(source)
    run_probes = helpers + _extract_function(source, "run_connectivity_probes")

    stub_root = tmp_path / "stub_remote"
    _write_connectivity_stub(stub_root)

    python_wrapper = _create_python_wrapper(tmp_path)
    python_bin = shlex.quote(str(python_wrapper))
    quoted_stub_root = shlex.quote(str(stub_root))
    marker_path = tmp_path / "marker_remote.txt"
    quoted_marker = shlex.quote(str(marker_path))
    bus_url = "wss://bus.example.com/ws"

    bash_script = dedent(
        f"""
        set -euo pipefail
        {run_probes}
        export PYTHON_BIN={python_bin}
        export STUB_ROOT={quoted_stub_root}
        export EVENT_BUS_URL={bus_url}
        export PROBE_MARKER={quoted_marker}
        run_connectivity_probes
        """
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 1
    assert (
        "Event bus: FAIL (unreachable) → wss://bus.example.com/ws (12.3 ms)" in completed.stdout
    )
    assert (
        "Event bus connectivity check failed: unreachable (wss://bus.example.com/ws)" in completed.stderr
    )
    assert marker_path.read_text(encoding="utf-8") == "event-bus wss://bus.example.com/ws"


def test_run_connectivity_probes_retains_skip_flag_for_headless(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    helpers = _extract_ui_skip_helpers(source)
    run_probes = helpers + _extract_function(source, "run_connectivity_probes")

    stub_root = tmp_path / "stub_headless_probes"
    _write_connectivity_stub(stub_root)

    python_wrapper = _create_python_wrapper(tmp_path)
    python_bin = shlex.quote(str(python_wrapper))
    quoted_stub_root = shlex.quote(str(stub_root))

    bash_script = dedent(
        f"""
        set -euo pipefail
        {run_probes}
        export PYTHON_BIN={python_bin}
        export STUB_ROOT={quoted_stub_root}
        export UI_DISABLE_HTTP_SERVER=1
        export EVENT_BUS_URL=ws://127.0.0.1:8779
        run_connectivity_probes
        if [[ -n ${{CONNECTIVITY_SKIP_UI_PROBES+x}} ]]; then
            printf 'flag=%s\n' "$CONNECTIVITY_SKIP_UI_PROBES"
        else
            printf 'flag=__unset__\n'
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

    assert completed.returncode == 0, completed.stderr
    assert "flag=1" in completed.stdout


def test_redis_health_falls_back_when_redis_cli_lacks_url_support(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = _extract_function(source, "redis_health")

    python_wrapper = tmp_path / "python_wrapper.py"
    python_wrapper.write_text(
        dedent(
            """\
            #!/usr/bin/env python3
            import os
            import runpy
            import sys

            stub_root = os.environ.get("STUB_PRODUCTION_PATH")
            if stub_root:
                sys.path.insert(0, stub_root)

            args = sys.argv[1:]
            if not args:
                sys.exit(1)

            if args[0] == "-":
                sys.argv = args
                code = sys.stdin.read()
                exec(compile(code, "<stdin>", "exec"), {"__name__": "__main__"})
            else:
                sys.argv = args
                runpy.run_path(args[0], run_name="__main__")
            """
        ),
        encoding="utf-8",
    )
    python_wrapper.chmod(0o755)
    python_bin = shlex.quote(str(python_wrapper))

    fake_cli_dir = tmp_path / "bin"
    fake_cli_dir.mkdir()
    fake_cli = fake_cli_dir / "redis-cli"
    fake_cli.write_text(
        "#!/usr/bin/env bash\n"
        "printf 'redis-cli: unknown option -- u\n' >&2\n"
        "exit 1\n",
        encoding="utf-8",
    )
    fake_cli.chmod(0o755)

    stub_root = tmp_path / "stub"
    package_dir = stub_root / "solhunter_zero"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    stub_source = (
        "from __future__ import annotations\n\n"
        "import os\n"
        "from pathlib import Path\n\n"
        "class _Result:\n"
        "    def __init__(self) -> None:\n"
        "        self.ok = True\n\n"
        "class ConnectivityChecker:\n"
        "    async def _probe_redis(self, name: str, target: str):\n"
        "        record = os.environ.get(\"REDIS_PROBE_RECORD\")\n"
        "        if record:\n"
        "            path = Path(record)\n"
        "            with path.open(\"a\", encoding=\"utf-8\") as handle:\n"
        "                handle.write(f\"{name} {target}\\n\")\n"
        "        return _Result()\n"
    )
    (package_dir / "production.py").write_text(stub_source, encoding="utf-8")

    record_path = tmp_path / "redis_fallback.txt"

    bash_script = dedent(
        f"""
        set -euo pipefail
        {functions}
        PYTHON_BIN={python_bin}
        export REDIS_URL=redis://localhost:6379/0
        if redis_health; then
            echo 'fallback-ok'
        else
            exit 23
        fi
        """
    )

    env = os.environ.copy()
    existing_path = env.get("PATH", "")
    env["PATH"] = f"{fake_cli_dir}:{existing_path}" if existing_path else str(fake_cli_dir)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{stub_root}:{existing_pythonpath}" if existing_pythonpath else str(stub_root)
    )
    env["REDIS_PROBE_RECORD"] = str(record_path)
    env["STUB_PRODUCTION_PATH"] = str(stub_root)

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert completed.returncode == 0, completed.stderr
    assert "fallback-ok" in completed.stdout
    assert record_path.read_text(encoding="utf-8").strip() == "redis-1 redis://localhost:6379/0"
