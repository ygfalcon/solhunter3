from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_executable(path: Path, contents: str) -> None:
    path.write_text(contents)
    path.chmod(0o755)


def test_env_doctor_fails_with_missing_config(tmp_path: Path) -> None:
    script = REPO_ROOT / "scripts" / "preflight" / "env_doctor.sh"

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

    _write_executable(
        bin_dir / "curl",
        "#!/usr/bin/env bash\n"
        "printf '{\"result\":\"ok\"}'\n",
    )

    _write_executable(
        bin_dir / "redis-cli",
        "#!/usr/bin/env bash\n"
        "if [[ $* == *\"PING\"* ]]; then\n"
        "  echo PONG\n"
        "  exit 0\n"
        "fi\n"
        "exit 0\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "MODE": "paper",
            "MICRO_MODE": "1",
            "REDIS_URL": "redis://localhost:6379/0",
            "SOLANA_RPC_URL": "http://localhost:8899",
            "HELIUS_API_KEY": "skip",
            "NEW_DAS_DISCOVERY": "1",
            "EXIT_FEATURES_ON": "1",
            "RL_WEIGHTS_DISABLED": "1",
            "CHECK_UI_HEALTH": "0",
            "CONFIG_PATH": str(tmp_path / "missing-config.toml"),
            "PATH": f"{bin_dir}:{env['PATH']}",
        }
    )

    completed = subprocess.run(
        ["bash", str(script)],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    assert "config:" in completed.stderr
