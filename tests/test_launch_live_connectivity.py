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
