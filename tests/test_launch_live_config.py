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
    'if ! keypair_report=$(CONFIG_PATH="$CONFIG_PATH" ENV_FILE_PATH="$ENV_FILE" "$PYTHON_BIN" - <<\'PY\'',
)


VALIDATE_ENV_SNIPPET = _extract_python_snippet(
    _LAUNCH_LIVE_SOURCE,
    '"$PYTHON_BIN" - "$ENV_FILE" <<\'PY\'',
)


def _build_complete_env(tmp_path: Path) -> dict[str, str]:
    keypair_path = tmp_path / "id.json"
    keypair_path.write_text("[]", encoding="utf-8")

    return {
        "SOLANA_RPC_URL": "https://solana.rpc",
        "SOLANA_WS_URL": "wss://solana.ws",
        "REDIS_URL": "redis://localhost:6379/0",
        "KEYPAIR_PATH": str(keypair_path),
        "EVENT_BUS_URL": "redis://localhost:6379/1",
        "HELIUS_API_KEY": "helius-api-key",
        "HELIUS_API_KEYS": "helius-api-keys",
        "HELIUS_API_TOKEN": "helius-token",
        "HELIUS_RPC_URL": "https://helius.rpc",
        "HELIUS_WS_URL": "wss://helius.ws",
        "HELIUS_PRICE_RPC_URL": "https://helius.price.rpc",
        "HELIUS_PRICE_REST_URL": "https://helius.price.rest",
        "HELIUS_PRICE_BASE_URL": "https://helius.price.base",
        "BIRDEYE_API_KEY": "birdeye-key",
        "SOLSCAN_API_KEY": "solscan-key",
        "DEX_BASE_URL": "https://dex.base",
        "DEX_TESTNET_URL": "https://dex.testnet",
        "ORCA_API_URL": "https://orca.api",
        "RAYDIUM_API_URL": "https://raydium.api",
        "PHOENIX_API_URL": "https://phoenix.api",
        "METEORA_API_URL": "https://meteora.api",
        "JUPITER_WS_URL": "wss://jupiter.ws",
        "JITO_RPC_URL": "https://jito.rpc",
        "JITO_AUTH": "jito-auth",
        "JITO_WS_URL": "wss://jito.ws",
        "JITO_WS_AUTH": "jito-ws-auth",
        "NEWS_FEEDS": "https://news.feed",
        "TWITTER_FEEDS": "https://twitter.feed",
        "DISCORD_FEEDS": "https://discord.feed",
    }


def _write_env_file(path: Path, values: dict[str, str]) -> None:
    payload = "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"
    path.write_text(payload, encoding="utf-8")


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


def test_launch_live_config_missing_from_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_file.write_text(
        "\n".join(
            [
                f"PYTHONPATH={tmp_path / 'alt_pythonpath'}",
                "CONFIG_PATH=/nonexistent",
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


def test_launch_live_skips_keypair_when_upcoming_mode_paper(tmp_path: Path) -> None:
    env = os.environ.copy()
    env.pop("KEYPAIR_PATH", None)
    env.pop("SOLANA_KEYPAIR", None)
    env.pop("CONFIG_PATH", None)
    env["UPCOMING_CONTROLLER_MODE"] = "paper"
    env["MODE"] = "live"

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


def test_launch_live_env_relative_keypair(tmp_path: Path) -> None:
    env_dir = tmp_path / "envdir"
    env_dir.mkdir()

    keypair_path = env_dir / "keys" / "id.json"
    keypair_path.parent.mkdir(parents=True, exist_ok=True)
    keypair_path.write_text("[]", encoding="utf-8")

    env_file = env_dir / "live.env"
    env_file.write_text("KEYPAIR_PATH=keys/id.json\nMODE=live\n", encoding="utf-8")

    env = os.environ.copy()
    env["ENV_FILE_PATH"] = str(env_file)
    env["KEYPAIR_PATH"] = "keys/id.json"
    env.pop("SOLANA_KEYPAIR", None)
    env["MODE"] = "live"

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


def test_validate_env_allows_shell_expansion(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_values = _build_complete_env(tmp_path)
    env_values["DATA_DIR"] = "${HOME}/.config"
    _write_env_file(env_file, env_values)

    completed = subprocess.run(
        [sys.executable, "-c", VALIDATE_ENV_SNIPPET, str(env_file)],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "DATA_DIR" in completed.stdout


def test_validate_env_masks_credentialed_urls(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_values = _build_complete_env(tmp_path)
    env_values["DATABASE_URL"] = "https://user:pass@secure.test"
    _write_env_file(env_file, env_values)

    completed = subprocess.run(
        [sys.executable, "-c", VALIDATE_ENV_SNIPPET, str(env_file)],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "DATABASE_URL=***" in completed.stdout
    assert "https://user:pass@secure.test" not in completed.stdout


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


def test_validate_env_reports_runbook_keys(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_values = _build_complete_env(tmp_path)
    env_values.pop("HELIUS_RPC_URL")
    _write_env_file(env_file, env_values)

    completed = subprocess.run(
        [sys.executable, "-c", VALIDATE_ENV_SNIPPET, str(env_file)],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 1
    assert "Environment file is missing required entries:" in completed.stderr
    assert "HELIUS_RPC_URL (Helius RPC URL)" in completed.stderr


def test_validate_env_rejects_runbook_placeholders(tmp_path: Path) -> None:
    env_file = tmp_path / "env"
    env_values = _build_complete_env(tmp_path)
    env_values["BIRDEYE_API_KEY"] = "YOUR_BIRDEYE_KEY"
    _write_env_file(env_file, env_values)

    completed = subprocess.run(
        [sys.executable, "-c", VALIDATE_ENV_SNIPPET, str(env_file)],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 1
    assert "placeholder detected for BIRDEYE_API_KEY" in completed.stderr
