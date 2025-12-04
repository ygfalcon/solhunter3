from __future__ import annotations

import os
from pathlib import Path
import shlex
import subprocess
import sys
from textwrap import dedent


REPO_ROOT = Path(__file__).resolve().parents[1]
LAUNCH_LIVE_PATH = REPO_ROOT / "scripts" / "launch_live.sh"
_LAUNCH_LIVE_SOURCE = LAUNCH_LIVE_PATH.read_text()


def _extract_function(source: str, name: str) -> str:
    marker = f"{name}() {{"
    start = source.find(marker)
    if start == -1:
        raise ValueError(f"Function {name} not found in launch_live.sh")
    brace_depth = 0
    end = None
    for idx in range(start, len(source)):
        char = source[idx]
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


def test_launch_live_offline_install_falls_back_to_artifacts(tmp_path: Path) -> None:
    ensure_fn = _extract_function(_LAUNCH_LIVE_SOURCE, "ensure_virtualenv")
    detect_fn = _extract_function(_LAUNCH_LIVE_SOURCE, "detect_pip_online")
    remediation_fn = _extract_function(
        _LAUNCH_LIVE_SOURCE, "append_offline_remediation_hint"
    )
    fallback_fn = _extract_function(_LAUNCH_LIVE_SOURCE, "artifact_fallback_install")

    venv_dir = tmp_path / "venv"
    bin_dir = venv_dir / "bin"
    bin_dir.mkdir(parents=True)

    python_bin = bin_dir / "python3"
    python_bin.write_text("#!/usr/bin/env bash\nexit 0\n")
    python_bin.chmod(0o755)

    activate = bin_dir / "activate"
    activate.write_text("#!/usr/bin/env bash\nexport VIRTUAL_ENV=$VENV_DIR\n")
    activate.chmod(0o755)

    cache_dir = tmp_path / "pip_cache"
    cache_dir.mkdir()

    artifact_root = tmp_path / "artifacts"
    wheel_dir = artifact_root / "wheels"
    wheel_dir.mkdir(parents=True)
    (wheel_dir / "example-0.1-py3-none-any.whl").write_text("wheel", encoding="utf-8")

    lock_file = artifact_root / "requirements-art.lock"
    lock_file.write_text("# compiled requirements\n", encoding="utf-8")

    pip_log = tmp_path / "pip_called.log"
    pip_state_dir = tmp_path / "pip_state"
    pip_state_dir.mkdir()

    pip_bin = bin_dir / "pip"
    pip_bin.write_text(
        dedent(
            """
            #!/usr/bin/env bash
            printf '%s\n' "$*" >> __PIP_LOG__
            state_dir=__STATE_DIR__
            mkdir -p "$state_dir"
            fail_marker="$state_dir/fail_once"
            check_ok="$state_dir/check_ok"

            if [[ $1 == 'cache' && ${2-} == 'dir' ]]; then
              echo __CACHE_DIR__
              exit 0
            fi

            if [[ $1 == 'install' ]]; then
              if [[ ! -f "$fail_marker" ]]; then
                touch "$fail_marker"
                echo 'missing wheels' >&2
                exit 1
              fi
              touch "$check_ok"
              exit 0
            fi

            if [[ $1 == 'check' ]]; then
              if [[ -f "$check_ok" ]]; then
                exit 0
              fi
              echo 'dependency issue' >&2
              exit 1
            fi

            exit 0
            """
        )
        .replace("__PIP_LOG__", shlex.quote(str(pip_log)))
        .replace("__STATE_DIR__", shlex.quote(str(pip_state_dir)))
        .replace("__CACHE_DIR__", shlex.quote(str(cache_dir)))
    )
    pip_bin.chmod(0o755)

    bash_script = dedent(
        f"""
        set -euo pipefail
        ROOT_DIR={shlex.quote(str(REPO_ROOT))}
        VENV_DIR={shlex.quote(str(venv_dir))}
        PYTHON_BIN={shlex.quote(str(python_bin))}
        PIP_BIN={shlex.quote(str(pip_bin))}
        LOG_DIR={shlex.quote(str(tmp_path))}
        RUNTIME_ARTIFACT_ROOT={shlex.quote(str(artifact_root))}
        EXIT_DEPS=5
        timestamp() {{ echo ts; }}
        log_info() {{ printf '%s\\n' "$*"; }}
        log_warn() {{ printf '%s\\n' "$*" >&2; }}
        {detect_fn}
        {remediation_fn}
        {fallback_fn}
        {ensure_fn}
        export PIP_NO_INDEX=1
        ensure_virtualenv
        """
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0, completed.stderr
    calls = pip_log.read_text().splitlines()
    install_calls = [line for line in calls if line.startswith("install ")]
    assert len(install_calls) >= 2
    assert any(str(lock_file) in line for line in calls)
    assert any("find-links" in line and str(wheel_dir) in line for line in calls)

    deps_log = tmp_path / "deps_install.log"
    assert "Offline remediation" in deps_log.read_text()

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


def test_validate_env_reports_missing_rpc_and_redis(tmp_path: Path) -> None:
    env_values = _build_complete_env(tmp_path)
    env_values.pop("SOLANA_RPC_URL", None)
    env_values.pop("REDIS_URL", None)

    env_file = tmp_path / "env"
    _write_env_file(env_file, env_values)

    completed = subprocess.run(
        [sys.executable, "-c", VALIDATE_ENV_SNIPPET, str(env_file)],
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 1
    stderr = completed.stderr
    assert "RPC endpoints missing" in stderr
    assert "SOLANA_RPC_URL (Solana RPC URL)" in stderr
    assert "Provide SOLANA_RPC_URL and SOLANA_WS_URL" in stderr
    assert "Redis cache missing" in stderr
    assert "REDIS_URL (Redis endpoint)" in stderr


def test_validate_env_reports_missing_helius_and_jito(tmp_path: Path) -> None:
    env_values = _build_complete_env(tmp_path)
    for key in [
        "HELIUS_API_KEY",
        "HELIUS_RPC_URL",
        "JITO_RPC_URL",
        "JITO_AUTH",
    ]:
        env_values.pop(key, None)

    env_file = tmp_path / "env"
    _write_env_file(env_file, env_values)

    completed = subprocess.run(
        [sys.executable, "-c", VALIDATE_ENV_SNIPPET, str(env_file)],
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 1
    stderr = completed.stderr
    assert "Helius credentials missing" in stderr
    assert "HELIUS_API_KEY (Helius API key)" in stderr
    assert "Jito bundle endpoints missing" in stderr
    assert "JITO_RPC_URL (Jito RPC URL)" in stderr
    assert "Configure JITO_* URLs and auth tokens" in stderr


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
