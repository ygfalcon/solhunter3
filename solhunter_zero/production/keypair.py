"""Live keypair resolution utilities for production startup."""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Mapping

from solders.pubkey import Pubkey

from solhunter_zero import wallet
from solhunter_zero.feature_flags import get_feature_flags
from solhunter_zero.gas import LAMPORTS_PER_SOL

try:  # Optional dependency; the runtime guards callers as well.
    from solana.rpc.api import Client
except Exception:  # pragma: no cover - solana client is an optional dependency
    Client = None  # type: ignore[assignment]

import logging


log = logging.getLogger(__name__)


def _home_default_keypair() -> Path:
    home = Path.home()
    return home / ".config" / "solana" / "id.json"


def _normalise_candidate(path: Path) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        return Path(os.getcwd()) / expanded
    return expanded


def _candidate_paths(cfg: Mapping[str, object] | None) -> list[tuple[str, Path]]:
    candidates: list[tuple[str, Path]] = []

    alt = os.getenv("SOLANA_KEYPAIR")
    if alt:
        candidates.append(("SOLANA_KEYPAIR", Path(alt)))

    if cfg and isinstance(cfg.get("solana_keypair"), str):
        cfg_path = str(cfg["solana_keypair"]).strip()
        if cfg_path:
            candidates.append(("config.solana_keypair", Path(cfg_path)))

    candidates.append(("default", _home_default_keypair()))

    seen: set[Path] = set()
    ordered: list[tuple[str, Path]] = []
    for label, raw_path in candidates:
        normalized = _normalise_candidate(raw_path)
        if normalized in seen:
            continue
        seen.add(normalized)
        ordered.append((label, normalized))
    return ordered


def _check_permissions(path: Path) -> None:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError:
        raise FileNotFoundError(path)
    if mode != 0o600:
        raise PermissionError(
            f"Keypair {path} must have permissions 0600; current mode is {mode:03o}"
        )


def _load_pubkey(path: Path) -> Pubkey:
    keypair = wallet.load_keypair(str(path))
    return keypair.pubkey()


def resolve_live_keypair(
    cfg: Mapping[str, object] | None = None,
    *,
    announce: bool = True,
    force: bool = False,
) -> tuple[Path, Pubkey]:
    """Resolve and validate the signing keypair for live trading.

    The search order follows the operator instructions:

    1. ``KEYPAIR_PATH`` when present.
    2. ``SOLANA_KEYPAIR`` when present.
    3. ``config.solana_keypair`` from the loaded configuration.
    4. ``$HOME/.config/solana/id.json``.

    The selected keypair must exist, be readable and have permissions ``0600``.
    On success ``KEYPAIR_PATH`` and ``SOLANA_KEYPAIR`` are synchronised and the
    environment is forced into live mode (``PAPER_TRADING=0``).
    """

    try:
        mode = get_feature_flags().mode.lower()
    except Exception:  # pragma: no cover - feature flag resolution is defensive
        mode = "paper"

    if not force and mode != "live":
        return Path(os.environ.get("KEYPAIR_PATH", "")), Pubkey.default()

    explicit = os.getenv("KEYPAIR_PATH")
    if explicit:
        path = _normalise_candidate(Path(explicit))
        try:
            if not path.exists():
                raise FileNotFoundError(path)
            if not path.is_file():
                raise FileExistsError(f"Keypair {path} is not a file")
            if not os.access(path, os.R_OK):
                raise PermissionError(f"Keypair {path} not readable")
            _check_permissions(path)
            pubkey = _load_pubkey(path)
        except Exception as exc:
            raise SystemExit(
                f"Configured KEYPAIR_PATH {path} is invalid: {exc}. "
                "Fix the permissions or provide a valid signing key."
            ) from exc
        resolved = path.resolve()
        os.environ["KEYPAIR_PATH"] = str(resolved)
        os.environ["SOLANA_KEYPAIR"] = str(resolved)
        os.environ["PAPER_TRADING"] = "0"
        for flag in ("LIVE_TRADING_DISABLED", "SHADOW_EXECUTOR_ONLY"):
            if flag in os.environ:
                os.environ.pop(flag)
        os.environ["LIVE_KEYPAIR_PUBKEY"] = str(pubkey)
        os.environ["LIVE_KEYPAIR_READY"] = "1"
        if announce and not os.environ.get("LIVE_KEYPAIR_ANNOUNCED"):
            print(f"LIVE_KEYPAIR_LOADED={pubkey}")
            os.environ["LIVE_KEYPAIR_ANNOUNCED"] = "1"
        log.info("Live keypair resolved from KEYPAIR_PATH (%s)", resolved)
        return resolved, pubkey

    last_error: Exception | None = None
    for label, candidate in _candidate_paths(cfg):
        try:
            if not candidate.exists():
                raise FileNotFoundError(candidate)
            if not candidate.is_file():
                raise FileExistsError(f"Keypair {candidate} is not a file")
            if not os.access(candidate, os.R_OK):
                raise PermissionError(f"Keypair {candidate} not readable")
            _check_permissions(candidate)
            pubkey = _load_pubkey(candidate)
        except Exception as exc:  # pragma: no cover - failure paths logged
            last_error = exc
            log.debug("Keypair candidate %s (%s) rejected: %s", label, candidate, exc)
            continue

        resolved = candidate.resolve()
        os.environ["KEYPAIR_PATH"] = str(resolved)
        os.environ["SOLANA_KEYPAIR"] = str(resolved)
        os.environ["PAPER_TRADING"] = "0"
        for flag in ("LIVE_TRADING_DISABLED", "SHADOW_EXECUTOR_ONLY"):
            if flag in os.environ:
                os.environ.pop(flag)
        os.environ["LIVE_KEYPAIR_PUBKEY"] = str(pubkey)
        os.environ["LIVE_KEYPAIR_READY"] = "1"
        if announce and not os.environ.get("LIVE_KEYPAIR_ANNOUNCED"):
            print(f"LIVE_KEYPAIR_LOADED={pubkey}")
            os.environ["LIVE_KEYPAIR_ANNOUNCED"] = "1"
        log.info("Live keypair resolved from %s (%s)", label, resolved)
        return resolved, pubkey

    message = "Live trading requires a valid signing keypair. "
    if last_error:
        message += f"Last error: {last_error}"
    raise SystemExit(
        message
        + " Set KEYPAIR_PATH to your keypair JSON or ensure ~/.config/solana/id.json exists."
    )


def verify_onchain_funds(
    *,
    min_sol: float = 0.0,
    rpc_timeout: float | None = None,
) -> tuple[float, str]:
    """Ensure the configured keypair has funds and the RPC is reachable."""

    keypair_path = os.getenv("KEYPAIR_PATH")
    if not keypair_path:
        raise SystemExit("KEYPAIR_PATH not configured; run resolve_live_keypair first")
    if Client is None:
        raise SystemExit("solana RPC client not available to verify keypair balance")

    rpc_url = os.getenv("SOLANA_RPC_URL")
    if not rpc_url:
        raise SystemExit("SOLANA_RPC_URL is required to verify wallet balance")

    keypair = wallet.load_keypair(keypair_path)
    pubkey = keypair.pubkey()
    client = Client(rpc_url, timeout=rpc_timeout)

    try:
        balance_resp = client.get_balance(pubkey)
        try:
            lamports = int(balance_resp["result"]["value"])
        except Exception:
            lamports = int(getattr(balance_resp, "value", 0))
    except Exception as exc:  # pragma: no cover - RPC failure bubble up
        raise SystemExit(f"Failed to fetch SOL balance via {rpc_url}: {exc}") from exc

    balance_sol = lamports / LAMPORTS_PER_SOL
    if balance_sol <= min_sol:
        raise SystemExit(
            f"Keypair {pubkey} has insufficient SOL ({balance_sol:.9f}). Fund the account before launching live trading."
        )

    try:
        blockhash_resp = client.get_latest_blockhash()
        blockhash_value = None
        if isinstance(blockhash_resp, dict):
            value = blockhash_resp.get("result", {}).get("value")
            if isinstance(value, dict):
                blockhash_value = value.get("blockhash")
        else:
            value = getattr(blockhash_resp, "value", None)
            if value is not None:
                blockhash_value = getattr(value, "blockhash", None)
        if not blockhash_value:
            raise RuntimeError("RPC did not return a blockhash")
    except Exception as exc:  # pragma: no cover - RPC failure bubble up
        raise SystemExit(f"Failed to fetch latest blockhash: {exc}") from exc

    os.environ["LIVE_KEYPAIR_BALANCE_SOL"] = f"{balance_sol:.9f}"
    os.environ["LIVE_RPC_BLOCKHASH"] = str(blockhash_value)
    log.info(
        "Keypair %s balance %.6f SOL verified via %s", pubkey, balance_sol, rpc_url
    )
    return balance_sol, str(blockhash_value)

