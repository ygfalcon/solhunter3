# solhunter_zero/startup_checks.py
from __future__ import annotations

import asyncio
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Iterable, Optional

try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    aiohttp = None

logger = logging.getLogger(__name__)

# Endpoints / env
BIRDEYE_API = os.getenv("BIRDEYE_API", "https://public-api.birdeye.so/defi/tokenlist")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d")


# -----------------------------
# Helpers
# -----------------------------
def _short_timeout(total: float = 5.0) -> "aiohttp.ClientTimeout":
    return aiohttp.ClientTimeout(total=total, connect=max(1.0, total / 2))  # type: ignore[return-value]


async def _json_rpc_health(session: "aiohttp.ClientSession", rpc_url: str) -> tuple[bool, str]:
    """Call a simple Solana JSON-RPC method with a short timeout."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
    try:
        async with session.post(rpc_url, json=payload, timeout=_short_timeout(5.0)) as resp:
            text = await resp.text()
            if resp.status == 200:
                return True, "RPC reachable"
            return False, f"RPC HTTP {resp.status}: {text[:200]}"
    except Exception as exc:
        return False, f"RPC error: {exc!s}"


async def _probe_birdeye(session: "aiohttp.ClientSession") -> tuple[bool, str]:
    """Try a single BirdEye page with chain=solana; non-fatal."""
    if not BIRDEYE_API_KEY:
        return True, "BirdEye key not set (skipping)"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "Accept": "application/json"}
    params = {"offset": 0, "limit": 1, "sortBy": "v24hUSD", "chain": "solana"}
    try:
        async with session.get(BIRDEYE_API, params=params, headers=headers, timeout=_short_timeout(6.0)) as resp:
            if 200 <= resp.status < 300:
                return True, "BirdEye OK"
            text = await resp.text()
            return False, f"BirdEye HTTP {resp.status}: {text[:200]}"
    except Exception as exc:
        return False, f"BirdEye error: {exc!s}"


# -----------------------------
# Public API used by startup
# -----------------------------
def ensure_rpc(*, warn_only: bool = False) -> None:
    """Synchronous wrapper that checks RPC reachability with a small JSON-RPC call."""
    if aiohttp is None:  # pragma: no cover
        logger.warning("aiohttp not available; skipping RPC check")
        return

    async def _run() -> None:
        async with aiohttp.ClientSession() as session:
            ok, msg = await _json_rpc_health(session, SOLANA_RPC_URL)
            if ok:
                logger.info("RPC: %s", msg)
            else:
                if warn_only:
                    logger.warning("RPC: %s", msg)
                else:
                    raise SystemExit(f"RPC not reachable: {msg}")

    asyncio.run(_run())


def ensure_endpoints(_cfg: Optional[dict[str, Any]] = None) -> None:
    """
    Check HTTP endpoints briefly and non-blockingly.

    - RPC JSON-RPC: informational only here (fatality handled in ensure_rpc).
    - BirdEye tokenlist: adds `chain=solana`, warns on failure, does not block.
    """
    if aiohttp is None:  # pragma: no cover
        logger.warning("aiohttp not available; skipping endpoint checks")
        return

    async def _run() -> None:
        async with aiohttp.ClientSession() as session:
            ok, msg = await _json_rpc_health(session, SOLANA_RPC_URL)
            logger.log(logging.INFO if ok else logging.WARNING, "Endpoint RPC: %s", msg)

            ok, msg = await _probe_birdeye(session)
            logger.log(logging.INFO if ok else logging.WARNING, "Endpoint BirdEye: %s", msg)

    asyncio.run(_run())


def ensure_target(name: str) -> None:
    """Ensure a Rust/Proto build target exists.

    This is a lightweight wrapper around :mod:`solhunter_zero.bootstrap` so
    callers interacting through :mod:`scripts.startup` can continue reaching
    the implementation from a single module.
    """

    from . import bootstrap

    bootstrap.ensure_target(name)


def ensure_wallet_cli() -> None:
    """Ensure the ``solhunter-wallet`` CLI is available."""

    from . import bootstrap_utils

    if shutil.which("solhunter-wallet") is not None:
        return

    bootstrap_utils._pip_install("solhunter-wallet")
    if shutil.which("solhunter-wallet") is None:
        print(
            "'solhunter-wallet' still not available after installation. "
            "Please install it manually with 'pip install solhunter-wallet' "
            "and ensure it is in your PATH.",
        )
        raise SystemExit(1)


def ensure_cargo() -> None:
    """Ensure the Rust toolchain (cargo) is available."""

    from . import bootstrap_utils

    bootstrap_utils.ensure_cargo()


def run_quick_setup(*, auto: bool = True) -> dict[str, Any]:
    """Ensure a config file and keypair exist, returning context details."""

    from . import config_utils, wallet
    from .config import load_config

    config_path = Path(config_utils.ensure_default_config())
    cfg = load_config(config_path)
    keypair_info = config_utils.select_active_keypair(auto=auto)
    keypair_path = Path(wallet.KEYPAIR_DIR) / f"{keypair_info.name}.json"

    summary_rows: list[tuple[str, str]] = [
        ("Configuration", str(config_path)),
        ("Keypair", keypair_info.name),
    ]

    return {
        "config_path": config_path,
        "config": cfg,
        "keypair_path": keypair_path if keypair_path.exists() else keypair_path,
        "mnemonic_path": keypair_info.mnemonic_path,
        "active_keypair": keypair_info.name,
        "summary_rows": summary_rows,
    }


# -----------------------------
# Compatibility shim
# -----------------------------
def _merge_context(base: dict[str, Any], updates: Optional[dict[str, Any]]) -> bool:
    """Merge helper results into ``base`` and return True if summaries were added."""

    if not updates:
        return False

    added_summary = False
    summary_rows = updates.get("summary_rows") if isinstance(updates, dict) else None
    if summary_rows:
        base.setdefault("summary_rows", []).extend(summary_rows)
        added_summary = True

    if isinstance(updates, dict):
        for key, value in updates.items():
            if key == "summary_rows" or value is None:
                continue
            base[key] = value

    return added_summary


def _append_summary(ctx: dict[str, Any], row: tuple[str, str]) -> None:
    ctx.setdefault("summary_rows", []).append(row)


def perform_checks(
    args,
    rest: Iterable[str],
    *,
    ensure_deps,
    ensure_target,
    ensure_wallet_cli,
    ensure_rpc,
    ensure_endpoints,
    ensure_cargo,
    run_quick_setup,
    log_startup,
    apply_env_overrides,
    load_config,
) -> dict[str, Any]:
    """Run startup checks and collect context for :mod:`scripts.startup`.

    The heavy lifting lives in helper modules which are injected from
    :mod:`scripts.startup`; this function orchestrates their execution so tests
    can exercise the flow in isolation.
    """

    ctx: dict[str, Any] = {"summary_rows": [], "rest": list(rest), "code": 0}

    log_startup("Starting dependency checks")
    if getattr(args, "skip_deps", False):
        _append_summary(ctx, ("Dependencies", "Skipped"))
    else:
        result = ensure_deps(
            install_optional=getattr(args, "full_deps", False),
            ensure_wallet_cli=False,
        )
        if not _merge_context(ctx, result):
            _append_summary(ctx, ("Dependencies", "Ensured"))

    log_startup("Ensuring build targets")
    targets = []
    for name in ("protos", "route_ffi", "depth_service"):
        ensure_target(name)
        targets.append(name)
    _append_summary(ctx, ("Build targets", ", ".join(targets)))

    log_startup("Checking solhunter-wallet CLI availability")
    if not _merge_context(ctx, ensure_wallet_cli()):
        _append_summary(ctx, ("Wallet CLI", "Available"))

    log_startup("Ensuring Rust toolchain")
    if not _merge_context(ctx, ensure_cargo()):
        _append_summary(ctx, ("Rust toolchain", "Ready"))

    if not getattr(args, "skip_setup", False):
        log_startup("Running quick setup")
        setup_ctx = run_quick_setup(auto=getattr(args, "one_click", True))
        _merge_context(ctx, setup_ctx)
    else:
        _append_summary(ctx, ("Quick setup", "Skipped"))

    config = ctx.get("config")
    config_path = ctx.get("config_path")
    if config_path and config is None:
        try:
            raw_cfg = load_config(config_path)
        except Exception as exc:  # pragma: no cover - surfaced to caller
            logger.error("Failed to load config %s: %s", config_path, exc)
            ctx["code"] = 1
            _append_summary(ctx, ("Configuration", f"Failed: {exc}"))
            return ctx
        config = raw_cfg
    if config is not None:
        config = apply_env_overrides(config)
        ctx["config"] = config

    if not getattr(args, "offline", False) and not getattr(args, "skip_rpc_check", False):
        log_startup("Checking Solana RPC health")
        ensure_rpc(warn_only=False)
        _append_summary(ctx, ("RPC", "Reachable"))
    else:
        _append_summary(ctx, ("RPC", "Skipped"))

    if not getattr(args, "offline", False) and not getattr(args, "skip_endpoint_check", False):
        log_startup("Checking configured endpoints")
        ensure_endpoints(ctx.get("config", {}))
        _append_summary(ctx, ("Endpoints", "Checked"))
    else:
        _append_summary(ctx, ("Endpoints", "Skipped"))

    return ctx
