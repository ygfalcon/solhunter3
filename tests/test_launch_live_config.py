from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
LAUNCH_LIVE_PATH = REPO_ROOT / "scripts" / "launch_live.sh"
_LAUNCH_LIVE_SOURCE = LAUNCH_LIVE_PATH.read_text()


def _extract_python_snippet(source: str, marker: str) -> str:
    start = source.index(marker)
    start = source.index("<<'PY'\n", start) + len("<<'PY'\n")
    end = source.index("\nPY", start)
    return source[start:end]


CHECK_LIVE_KEYPAIR_SNIPPET = _extract_python_snippet(
    _LAUNCH_LIVE_SOURCE,
    'if ! keypair_report=$(CONFIG_PATH="$CONFIG_PATH" "$PYTHON_BIN" - <<\'PY\'',
)


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


def test_launch_live_config_relative_keypair(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    keypair_path = config_dir / "id.json"
    keypair_path.write_text("[]", encoding="utf-8")

    config_path = config_dir / "live.toml"
    config_path.write_text("solana_keypair = \"id.json\"\n", encoding="utf-8")

    stub_root = tmp_path / "stubs"
    pkg_dir = stub_root / "solhunter_zero"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (pkg_dir / "config.py").write_text(
        "def load_config(path):\n    return {'solana_keypair': 'id.json'}\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["CONFIG_PATH"] = str(config_path)
    env["MODE"] = "live"
    env.pop("KEYPAIR_PATH", None)
    env.pop("SOLANA_KEYPAIR", None)
    env["PYTHONPATH"] = str(stub_root)

    completed = subprocess.run(
        [sys.executable, "-c", CHECK_LIVE_KEYPAIR_SNIPPET],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        env=env,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == ""
    assert completed.stderr.strip() == ""


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
