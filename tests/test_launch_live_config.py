from __future__ import annotations

from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_launch_live_missing_config(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_file.write_text("")

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
