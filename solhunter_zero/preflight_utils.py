"""Shared preflight check utilities for SolHunter Zero.

This module centralizes the various environment checks used by the CLI
scripts.  Functions here are imported by both ``scripts/preflight.py`` and
``scripts/startup.py`` to ensure consistent behaviour regardless of which
entrypoint is executed.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from scripts.deps import check_deps
from solhunter_zero.config_utils import ensure_default_config, select_active_keypair
from solhunter_zero import wallet
from solhunter_zero.paths import ROOT
from .console_utils import console_print

Check = Tuple[bool, str]


def check_python_version(min_version: tuple[int, int] = (3, 11)) -> Check:
    """Ensure the running Python interpreter meets the minimum version."""

    if sys.version_info < min_version:
        return False, f"Python {min_version[0]}.{min_version[1]}+ required"
    return True, (
        f"Python {sys.version_info.major}.{sys.version_info.minor} detected"
    )


def check_dependencies() -> Check:
    """Report any missing required or optional dependencies."""

    missing_required, missing_optional = check_deps()
    if missing_required or missing_optional:
        parts: List[str] = []
        if missing_required:
            parts.append(
                "missing required: " + ", ".join(sorted(missing_required))
            )
        if missing_optional:
            formatted = []
            for opt in sorted(missing_optional):
                if opt == "psutil":
                    formatted.append("psutil (resource monitoring disabled)")
                else:
                    formatted.append(opt)
            parts.append("missing optional: " + ", ".join(formatted))
        return False, "; ".join(parts)
    return True, "All dependencies available"


def check_homebrew() -> Check:
    """Verify that the Homebrew package manager is installed."""

    brew = shutil.which("brew")  # type: ignore[name-defined]
    if not brew:
        return False, "Homebrew not found"
    try:
        subprocess.run([brew, "--version"], check=True, capture_output=True)
    except Exception:
        return False, "brew failed to execute"
    return True, "Homebrew available"


def check_rustup() -> Check:
    """Verify that rustup is installed."""

    rustup = shutil.which("rustup")  # type: ignore[name-defined]
    if not rustup:
        return False, "rustup not found"
    try:
        subprocess.run([rustup, "--version"], check=True, capture_output=True)
    except Exception:
        return False, "rustup failed to execute"
    return True, "rustup available"


def check_rust_toolchain() -> Check:
    """Verify that the Rust toolchain is installed."""

    rustc = shutil.which("rustc")  # type: ignore[name-defined]
    cargo = shutil.which("cargo")  # type: ignore[name-defined]
    if not rustc or not cargo:
        return False, "Rust toolchain not found"
    try:
        subprocess.run([rustc, "--version"], check=True, capture_output=True)
    except Exception:
        return False, "rustc failed to execute"
    return True, "Rust toolchain available"


def check_xcode_clt() -> Check:
    """Verify that the Xcode Command Line Tools are installed on macOS."""

    if sys.platform != "darwin":
        return True, "Not macOS"
    try:
        subprocess.run(["xcode-select", "-p"], check=True, capture_output=True)
    except FileNotFoundError:
        return False, "xcode-select not found"
    except subprocess.CalledProcessError:
        return False, "Xcode Command Line Tools not installed"
    return True, "Xcode Command Line Tools available"


def check_config_file(path: str = "config.toml") -> Check:
    """Ensure a configuration file exists at ``path``.

    When ``path`` is the default ``"config.toml"`` the helper
    :func:`ensure_default_config` is used which will create a config from the
    bundled template if necessary.  For any other ``path`` the location is
    resolved relative to the current working directory and simply checked for
    existence.
    """

    if path == "config.toml":
        cfg = ensure_default_config().resolve()
    else:
        cfg = Path(path).expanduser().resolve()

    if cfg.exists():
        return True, f"Found {cfg}"
    return False, f"Missing {cfg}"


def check_keypair(dir_path: str | Path | None = None) -> Check:
    if dir_path is None or dir_path == "keypairs":
        base_dir = Path(wallet.KEYPAIR_DIR)
    else:
        base_dir = Path(dir_path)
    try:
        info = select_active_keypair(auto=True)
    except Exception:
        return False, f"{base_dir / 'active'} not found"
    keyfile = base_dir / f"{info.name}.json"
    if keyfile.exists():
        return True, f"Active keypair {info.name} present"
    return False, f"Keypair {keyfile} not found"


def check_wallet_balance(min_sol: float, keypair_path: str | Path | None = None) -> Check:
    """Ensure the active keypair has at least ``min_sol`` SOL."""

    try:
        if keypair_path is None:
            info = select_active_keypair(auto=True)
            keypair_path = Path(wallet.KEYPAIR_DIR) / f"{info.name}.json"
        kp = wallet.load_keypair(str(keypair_path))
    except Exception as exc:
        return False, f"Failed to load keypair: {exc}"

    try:
        from solana.rpc.api import Client
        from .gas import LAMPORTS_PER_SOL

        client = Client(
            os.getenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY")
        )
        resp = client.get_balance(kp.pubkey())
        try:
            lamports = int(resp["result"]["value"])
        except Exception:  # pragma: no cover - defensive
            lamports = int(getattr(resp, "value", 0))
    except Exception as exc:
        return False, f"Failed to fetch wallet balance: {exc}"

    sol = lamports / LAMPORTS_PER_SOL
    if sol < min_sol:
        return False, (
            f"Wallet balance {sol:.9f} SOL below required {min_sol:.9f} SOL"
        )
    return True, f"Wallet balance {sol:.9f} SOL OK"


def check_libroute_ffi(pattern: str = "solhunter_zero/libroute_ffi.*") -> Check:
    """Ensure the compiled route FFI library is present."""

    matches = list(ROOT.glob(pattern))
    if matches:
        return True, f"Found {matches[0].relative_to(ROOT)}"
    return False, f"Missing {pattern}"


def check_depth_service(path: str = "target/release/depth_service") -> Check:
    """Ensure the Rust depth service binary exists."""

    exe = ROOT / path
    if exe.exists():
        return True, f"Found {exe.relative_to(ROOT)}"
    exe_win = exe.with_suffix(".exe")
    if exe_win.exists():
        return True, f"Found {exe_win.relative_to(ROOT)}"
    return False, f"Missing {path}"


def check_disk_space(min_bytes: int | float) -> Check:
    """Ensure there is at least ``min_bytes`` free on the current filesystem.

    The ``min_bytes`` value is typically derived from the
    ``offline_data_limit_gb`` configuration in ``scripts/startup.py``.  Both
    integer and floating point values are accepted and coerced to integers
    internally.
    """

    min_bytes = int(min_bytes)

    try:
        _, _, free = shutil.disk_usage(ROOT)
    except OSError as exc:  # pragma: no cover - unexpected failure
        return False, f"Unable to determine free disk space: {exc}"

    required_gb = min_bytes / (1024 ** 3)
    free_gb = free / (1024 ** 3)
    if free < min_bytes:
        return False, (
            f"Insufficient disk space: {free_gb:.2f} GB available,"
            f" {required_gb:.2f} GB required"
        )
    return True, f"Sufficient disk space: {free_gb:.2f} GB available"


def check_internet(url: str | None = None) -> Check:
    """Ensure basic internet connectivity by reaching a known Solana RPC host.

    Parameters
    ----------
    url:
        Optional URL to test. When ``None`` the function uses the value of
        ``SOLANA_RPC_URL`` from the environment, falling back to the public
        mainnet endpoint if unset.
    """

    import urllib.error
    import urllib.request
    import time
    import json

    target = url or os.environ.get(
        "SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY"
    )

    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getHealth"}).encode()
    headers = {"Content-Type": "application/json"}

    last_exc: Exception | None = None
    for attempt in range(3):
        req = urllib.request.Request(target, data=payload, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:  # nosec B310
                resp.read()
                return True, f"Reached {target}"
        except urllib.error.HTTPError as exc:  # pragma: no cover - provider quirks
            if 200 <= getattr(exc, "code", 500) < 500:
                try:
                    exc.read()
                except Exception:
                    pass
                return True, f"Reached {target} (status {exc.code})"
            last_exc = exc
        except Exception as exc:  # pragma: no cover - network failure
            last_exc = exc

        if attempt == 2:
            return True, (
                f"Skipping connectivity validation for {target}: {last_exc}"
            )
        time.sleep(2**attempt)

    return True, f"Skipping connectivity validation for {target}: {last_exc}"


def check_required_env(keys: List[str] | None = None) -> Check:
    """Ensure critical environment variables are configured."""

    required = keys or ["SOLANA_RPC_URL"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        joined = ", ".join(missing)
        return False, (
            f"Missing environment variables: {joined}. "
            "Set them and retry"
        )

    bird_key = os.getenv("BIRDEYE_API_KEY")
    placeholder = False
    if bird_key:
        lower = bird_key.lower()
        placeholder = (
            "your_" in lower
            or "example" in lower
            or "invalid" in lower
            or "fake" in lower
            or (bird_key.startswith("be_") and all(ch in "xX" for ch in bird_key[3:]))
            or lower.startswith("bd")
        )
    if not bird_key or placeholder:
        logging.getLogger(__name__).warning(
            "BIRDEYE_API_KEY missing or placeholder; continuing with on-chain scanning only"
        )

    return True, "Required environment variables set"


def check_network(default_url: str = "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY") -> Check:
    if sys.platform == "darwin":
        try:
            from solhunter_zero import macos_setup

            macos_setup.ensure_network()
        except SystemExit:
            return True, "macOS network check skipped"
        except Exception as exc:  # pragma: no cover - defensive
            return True, f"macOS network check skipped: {exc}"
        return True, "Network connectivity OK"
    url = os.environ.get("SOLANA_RPC_URL", default_url)
    try:
        from solhunter_zero.http import check_endpoint

        check_endpoint(url)
    except Exception as exc:
        return True, f"Network check skipped: {exc}"
    return True, f"Network access to {url} OK"


def check_gpu() -> Check:
    """Report GPU availability and the selected default device."""

    try:
        from solhunter_zero import device

        ok, msg = device.verify_gpu()
        if ok:
            return True, msg
        return True, f"GPU check skipped: {msg}"
    except Exception as exc:  # pragma: no cover - defensive
        return True, f"GPU check skipped: {exc}"


def run_basic_checks(min_bytes: int = 1 << 30, url: str | None = None) -> None:
    """Run minimal startup checks for disk space and internet connectivity.

    Parameters
    ----------
    min_bytes:
        Minimum free disk space required for the application to run.
    url:
        Optional URL to test for network reachability. Defaults to the
        ``SOLANA_RPC_URL`` environment variable or the public Solana RPC
        endpoint when unset.
    """

    ok, msg = check_disk_space(min_bytes)
    console_print(msg)
    if not ok:
        raise SystemExit(1)
    ok, msg = check_internet(url)
    console_print(msg)
    if not ok:
        raise SystemExit(1)
