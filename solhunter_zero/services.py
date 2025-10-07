"""Background service utilities for SolHunter Zero."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import subprocess
import time
from pathlib import Path
import shutil
import socket as _sock


# Manifest for the optional Rust ``depth_service`` companion process.
_SERVICE_MANIFEST = (
    Path(__file__).resolve().parent.parent / "depth_service" / "Cargo.toml"
)

# Default Jupiter stats WS used upstream in some configs; we do NOT want to pass it
# unless the user has explicitly set a non-empty override.
_DEFAULT_JUPITER_WS = "wss://stats.jup.ag/ws"


def _get_cfg_or_env(cfg: dict, key: str) -> str:
    """Fetch value from env (uppercase) or cfg (lowercase), as a string."""
    env_key = key.upper()
    val = os.getenv(env_key)
    if val is None:
        val = cfg.get(key)  # type: ignore[assignment]
    return str(val) if val is not None else ""


def start_depth_service(cfg: dict) -> subprocess.Popen | None:
    """Launch the Rust ``depth_service`` if enabled."""
    if not cfg.get("depth_service"):
        return None

    # Allow graceful degradation when binary is missing or during live drills
    if not _SERVICE_MANIFEST.exists():
        logging.warning("depth_service manifest not found; skipping depth service startup")
        return None
    if not shutil.which("cargo"):
        logging.warning("cargo not found; skipping depth service startup")
        return None

    args = [
        "cargo",
        "run",
        "--manifest-path",
        str(_SERVICE_MANIFEST),
        "--release",
        "--",
    ]

    # Helper to add flags when a value is provided
    def add(flag: str, key: str) -> None:
        val = _get_cfg_or_env(cfg, key)
        if val:
            args.extend([flag, val])

    # DEX venue WS endpoints (optional)
    add("--raydium", "raydium_ws_url")
    add("--orca", "orca_ws_url")
    add("--phoenix", "phoenix_ws_url")
    add("--meteora", "meteora_ws_url")
    add("--serum", "serum_ws_url")

    # Jupiter WS is **opt-in only**. We intentionally skip the hard-coded default
    # (wss://stats.jup.ag/ws) unless the user explicitly set a non-empty override.
    jup_ws = _get_cfg_or_env(cfg, "jupiter_ws_url").strip()
    if jup_ws and jup_ws != _DEFAULT_JUPITER_WS:
        args.extend(["--jupiter", jup_ws])
    else:
        logging.info("Jupiter WS disabled (no override provided)")

    # RPC endpoint
    rpc = _get_cfg_or_env(cfg, "solana_rpc_url")
    if rpc:
        args.extend(["--rpc", rpc])

    # Keypair path
    keypair = os.getenv("SOLANA_KEYPAIR") or os.getenv("KEYPAIR_PATH") or str(cfg.get("solana_keypair") or "")
    if keypair:
        args.extend(["--keypair", keypair])

    # Optional unix socket path that the Rust service will bind to
    socket_path = os.getenv("DEPTH_SERVICE_SOCKET", "/tmp/depth_service.sock")
    socket_path = Path(socket_path).resolve()
    socket_path.parent.mkdir(parents=True, exist_ok=True)

    logging.debug("Launching depth_service with args: %s", " ".join(args))
    proc = subprocess.Popen(args)

    timeout = float(os.getenv("DEPTH_START_TIMEOUT", "10") or 10)

    # Synchronous wait for unix socket so we don't nest event loops
    deadline = time.monotonic() + timeout
    while True:
        try:
            s = _sock.socket(_sock.AF_UNIX, _sock.SOCK_STREAM)
            s.settimeout(0.2)
            s.connect(str(socket_path))
            s.close()
            break
        except OSError:
            if time.monotonic() > deadline:
                with contextlib.suppress(Exception):
                    proc.terminate()
                with contextlib.suppress(Exception):
                    proc.wait(timeout=1)
                # Permit skipping in live drill or when optional
                if str(os.getenv("LIVE_DRILL", "")).lower() in {"1", "true", "yes"} or \
                   str(os.getenv("DEPTH_SERVICE_OPTIONAL", "")).lower() in {"1", "true", "yes"} or \
                   bool(cfg.get("depth_service_optional", False)):
                    logging.warning("depth_service unavailable; continuing without it (optional mode)")
                    return None
                raise RuntimeError(f"Failed to start depth_service within {timeout}s")
            time.sleep(0.05)
    return proc


async def depth_service_watchdog(
    cfg: dict, proc_ref: list[subprocess.Popen | None]
) -> None:
    """Monitor the ``depth_service`` process and attempt limited restarts."""
    proc = proc_ref[0]
    if not proc:
        return

    max_restarts = int(
        os.getenv("DEPTH_MAX_RESTARTS") or cfg.get("depth_max_restarts", 1)
    )
    restart_count = 0

    try:
        while True:
            await asyncio.sleep(1.0)
            if proc.poll() is None:
                continue
            if restart_count >= max_restarts:
                logging.error(
                    "depth_service exited after %d restarts; aborting",
                    restart_count,
                )
                raise RuntimeError("depth_service terminated")

            restart_count += 1
            logging.warning(
                "depth_service exited; attempting restart (%d/%d)",
                restart_count,
                max_restarts,
            )

            try:
                proc = await asyncio.to_thread(start_depth_service, cfg)
            except Exception as exc:
                logging.error("Failed to restart depth_service: %s", exc)
                raise
            if not proc:
                logging.error("depth_service restart returned None; aborting")
                raise RuntimeError("depth_service restart failed")
            proc_ref[0] = proc
    except asyncio.CancelledError:
        pass
