from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_launch_live_missing_config(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_file.write_text(f"PYTHONPATH={tmp_path / 'alt_pythonpath'}\n")

    proc = subprocess.run(
        [
            "bash",
            "scripts/launch_live.sh",
            "--env",
            str(env_file),
            "--micro",
            "0",
            "--config",
            "/nonexistent",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
    )

    assert proc.returncode == 1
    assert "Config file /nonexistent must exist and be readable" in proc.stderr


def test_launch_live_missing_keypair(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_file.write_text(
        "\n".join(
            [
                f"PYTHONPATH={tmp_path / 'alt_pythonpath'}",
                "KEYPAIR_PATH=/no/such/key.json",
            ]
        )
        + "\n"
    )

    env = os.environ.copy()
    env["LAUNCH_LIVE_SKIP_PIP"] = "1"

    proc = subprocess.run(
        [
            "bash",
            "scripts/launch_live.sh",
            "--env",
            str(env_file),
            "--micro",
            "0",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env=env,
    )

    assert proc.returncode == 1
    assert "Live trading requires a readable signing keypair" in proc.stderr
    assert "/no/such/key.json" in proc.stderr


def test_launch_live_invalid_preflight_value(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_file.write_text(f"PYTHONPATH={tmp_path / 'alt_pythonpath'}\n")

    env = os.environ.copy()
    env["LAUNCH_LIVE_SKIP_PIP"] = "1"

    proc = subprocess.run(
        [
            "bash",
            "scripts/launch_live.sh",
            "--env",
            str(env_file),
            "--micro",
            "0",
            "--preflight",
            "3",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env=env,
    )

    assert proc.returncode == 2
    assert (
        "Invalid --preflight value '3'; set to 1 for the active micro mode or 2 to cover both micro states."
        in proc.stderr
    )


def test_launch_live_exports_solhunter_config(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        textwrap.dedent(
            """
            solana_rpc_url = "https://example.com/rpc"
            dex_base_url = "https://example.com/dex"
            agents = ["alpha"]

            [agent_weights]
            alpha = 1.0
            """
        ).strip()
        + "\n"
    )

    venv_dir = REPO_ROOT / ".venv"
    if venv_dir.exists():
        shutil.rmtree(venv_dir)
    bin_dir = venv_dir / "bin"
    bin_dir.mkdir(parents=True)

    log_file = tmp_path / "python_env.log"
    python_wrapper = bin_dir / "python3"
    python_wrapper.write_text(
        textwrap.dedent(
            f"""
            #!/usr/bin/env bash
            set -euo pipefail
            LOG_PATH=\"{str(log_file).replace('\\', '\\\\').replace('"', '\\"')}\"
            REAL_PYTHON=\"{sys.executable.replace('\\', '\\\\').replace('"', '\\"')}\"
            echo \"CONFIG:${{SOLHUNTER_CONFIG-__UNSET__}}\" >> "$LOG_PATH"
            exec "$REAL_PYTHON" "$@"
            """
        ).strip()
        + "\n"
    )
    os.chmod(python_wrapper, 0o755)

    activate_path = bin_dir / "activate"
    activate_path.write_text(
        textwrap.dedent(
            f"""
            VIRTUAL_ENV=\"{str(venv_dir).replace('\\', '\\\\').replace('"', '\\"')}\"
            export VIRTUAL_ENV
            export PATH=\"$VIRTUAL_ENV/bin:$PATH\"
            """
        ).strip()
        + "\n"
    )

    env_file = tmp_path / "env"
    alt_pythonpath = tmp_path / "alt_pythonpath"
    alt_pythonpath.mkdir()
    env_file.write_text(f"PYTHONPATH={alt_pythonpath}\n")

    env = os.environ.copy()
    env["LAUNCH_LIVE_SKIP_PIP"] = "1"
    env["SOLHUNTER_CONFIG"] = "preexisting"

    try:
        proc = subprocess.run(
            [
                "bash",
                "scripts/launch_live.sh",
                "--env",
                str(env_file),
                "--micro",
                "0",
                "--config",
                str(config_path),
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            env=env,
        )
    finally:
        shutil.rmtree(venv_dir, ignore_errors=True)

    assert proc.returncode == 1
    assert "Live trading requires a signing keypair" in proc.stderr

    log_lines = (log_file.read_text().splitlines() if log_file.exists() else [])
    assert f"CONFIG:preexisting" in log_lines
    assert f"CONFIG:{config_path}" in log_lines
