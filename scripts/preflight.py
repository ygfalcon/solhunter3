#!/usr/bin/env python3
"""Environment preflight checks for Solsniper-zero."""

from __future__ import annotations

import os
import sys
from typing import Callable, List, Tuple
import json

from solhunter_zero.paths import ROOT
from solhunter_zero.preflight_utils import (
    Check,
    check_dependencies,
    check_gpu,
    check_homebrew,
    check_keypair,
    check_libroute_ffi,
    check_depth_service,
    check_internet,
    check_network,
    check_python_version,
    check_required_env,
    check_wallet_balance,
    check_rust_toolchain,
    check_rustup,
    check_xcode_clt,
    check_config_file,
    run_basic_checks,
)
from solhunter_zero.logging_utils import log_startup

__all__ = ["CHECKS", "run_preflight", "main", "check_internet"]


CHECKS: List[Tuple[str, Callable[[], Check]]] = [
    ("Python", check_python_version),
    ("Dependencies", check_dependencies),
    ("Homebrew", check_homebrew),
    ("Rustup", check_rustup),
    ("Rust", check_rust_toolchain),
    ("Route FFI", check_libroute_ffi),
    ("Depth Service", check_depth_service),
    ("Xcode CLT", check_xcode_clt),
    ("Config", check_config_file),
    ("Keypair", check_keypair),
    ("Environment", check_required_env),
    ("Internet", check_internet),
    (
        "Network",
        lambda: check_network(
            os.environ.get(
                "SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY"
            )
        ),
    ),
    ("GPU", check_gpu),
]

_MIN_BAL_STR = os.getenv("MIN_STARTING_BALANCE", "0")
try:
    _MIN_BAL = float(_MIN_BAL_STR or 0)
except ValueError:
    log_startup(
        f"WARNING: invalid MIN_STARTING_BALANCE '{_MIN_BAL_STR}' - defaulting to 0"
    )
    _MIN_BAL = 0.0
if _MIN_BAL > 0:
    CHECKS.append(("Balance", lambda: check_wallet_balance(_MIN_BAL)))


def run_preflight() -> List[Tuple[str, bool, str]]:
    """Run all preflight checks and return their results.

    The return value is a list of tuples containing the check name, a boolean
    indicating success, and a descriptive message.
    """

    results: List[Tuple[str, bool, str]] = []
    for name, func in CHECKS:
        try:
            ok, msg = func()
        except Exception as exc:  # noqa: BLE001
            results.append((name, False, str(exc)))
            log_startup("%s check failed: %s", name, exc)
        else:
            results.append((name, ok, msg))

    data = {
        "successes": [
            {"name": name, "message": msg} for name, ok, msg in results if ok
        ],
        "failures": [
            {"name": name, "message": msg}
            for name, ok, msg in results
            if not ok
        ],
    }
    try:
        with open(ROOT / "preflight.json", "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
    except OSError as exc:
        log_startup("preflight.json write failed: %s", exc)

    lines: List[str] = []
    failures: List[Tuple[str, str]] = []
    for name, ok, msg in results:
        status = "OK" if ok else "FAIL"
        line = f"{name}: {status} - {msg}"
        lines.append(line)
        log_startup(line)
        if not ok:
            failures.append((name, msg))
    try:
        with open(ROOT / "preflight.log", "a", encoding="utf-8") as log:
            for line in lines:
                log.write(line + "\n")
    except OSError as exc:
        log_startup("preflight.log write failed: %s", exc)
    for name, msg in failures:
        log_startup(f"Preflight failure: {name} - {msg}")

    return results


def main() -> None:
    failures: List[Tuple[str, str]] = []
    results = run_preflight()
    for name, ok, msg in results:
        status = "OK" if ok else "FAIL"
        print(f"{name}: {status} - {msg}")
        if not ok:
            failures.append((name, msg))

    if failures:
        print("\nSummary of failures:")
        for name, msg in failures:
            print(f"- {name}: {msg}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
