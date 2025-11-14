from __future__ import annotations

import os
from pathlib import Path
import subprocess


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
