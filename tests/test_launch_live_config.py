from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


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


VALIDATE_ENV_SNIPPET = _extract_python_snippet(
    _LAUNCH_LIVE_SOURCE,
    '"$PYTHON_BIN" - "$ENV_FILE" <<\'PY\'',
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
    keypair_path.write_text(json.dumps([0] * 64), encoding="utf-8")

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


def test_launch_live_keypair_invalid_json(tmp_path: Path) -> None:
    keypair_path = tmp_path / "id.json"
    keypair_path.write_text("not json", encoding="utf-8")

    env = os.environ.copy()
    env["MODE"] = "live"
    env["KEYPAIR_PATH"] = str(keypair_path)

    completed = subprocess.run(
        [sys.executable, "-c", CHECK_LIVE_KEYPAIR_SNIPPET],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        env=env,
    )

    assert completed.returncode == 1
    assert "could not be decoded as JSON" in completed.stdout


def test_launch_live_keypair_invalid_schema(tmp_path: Path) -> None:
    keypair_path = tmp_path / "id.json"
    keypair_path.write_text(json.dumps([0] * 10), encoding="utf-8")

    env = os.environ.copy()
    env["MODE"] = "live"
    env["KEYPAIR_PATH"] = str(keypair_path)

    completed = subprocess.run(
        [sys.executable, "-c", CHECK_LIVE_KEYPAIR_SNIPPET],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        env=env,
    )

    assert completed.returncode == 1
    assert "expected JSON array of 64 integers" in completed.stdout


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


def test_validate_env_allows_shell_expansion(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_file.write_text("DATA_DIR=${HOME}/.config\n", encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, "-c", VALIDATE_ENV_SNIPPET, str(env_file)],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "DATA_DIR" in completed.stdout


def test_validate_env_rejects_placeholder_tokens(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_file.write_text("DATA_DIR=${REPLACE_ME}\n", encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, "-c", VALIDATE_ENV_SNIPPET, str(env_file)],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 1
    assert "placeholder detected for DATA_DIR" in completed.stderr
