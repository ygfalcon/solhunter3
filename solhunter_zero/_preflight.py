from __future__ import annotations
import os, time, random
from pathlib import Path
from urllib.parse import urlparse, urlunparse
from typing import Dict, Any, Optional
import requests

# Import central config helpers for loading and environment overrides
from .config import load_config as _load_cfg, apply_env_overrides as _apply_env

PKG_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PKG_ROOT.parent


def derive_ws_url(http_url: str) -> str:
    p = urlparse(http_url)
    if p.scheme not in ("http", "https"):
        raise ValueError(f"RPC URL must be http/https, got: {http_url}")
    scheme = "ws" if p.scheme == "http" else "wss"
    return urlunparse((scheme, p.netloc, p.path, p.params, p.query, p.fragment))


def _retry(fn, tries=3, base=0.25, cap=2.0):
    for i in range(1, tries + 1):
        try:
            return fn()
        except Exception:
            if i == tries:
                raise
            time.sleep(min(cap, base * (2 ** (i - 1))) + random.random() * 0.1)


def rpc_ping(url: str, timeout=8):
    r = _retry(lambda: requests.post(url, json={"jsonrpc": "2.0", "id": 1, "method": "getVersion"}, timeout=timeout))
    r.raise_for_status()
    return r.json()


def rpc_blockhash(url: str, timeout=8):
    r = _retry(
        lambda: requests.post(
            url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getLatestBlockhash",
                "params": [{"commitment": "processed"}],
            },
            timeout=timeout,
        )
    )
    r.raise_for_status()
    return r.json()


def check_artifacts():
    """Verify compiled artifacts exist (skipped in CI for speed)."""
    if os.getenv("SELFTEST_SKIP_ARTIFACTS") == "1" or os.getenv("CI") == "true":
        return
    # Package-relative FFI
    if not any(PKG_ROOT.glob("libroute_ffi.*")):
        raise RuntimeError(f"Missing FFI library at {PKG_ROOT}/libroute_ffi.*")
    # Repo-relative depth_service
    if not (REPO_ROOT / "target" / "release" / "depth_service").exists():
        raise RuntimeError("Missing depth_service (build with: cargo build --release)")


def load_and_validate_config(explicit_path: Optional[str]) -> Dict[str, Any]:
    """Load config and apply environment overrides via central module."""
    data = _load_cfg(explicit_path)
    data = _apply_env(data)
    for k in ("solana_rpc_url", "dex_base_url", "agents"):
        if k not in data or not data[k]:
            raise KeyError(f"config missing `{k}`")
    return data


def resolve_keypair(dir_="keypairs", auto_env="AUTO_SELECT_KEYPAIR") -> Path:
    """Return the active keypair path, preferring explicit configuration."""

    def _normalize(path: Path) -> Path:
        if path.is_absolute():
            return path
        return (Path(dir_) / path).resolve()

    auto_select = os.getenv(auto_env, "").strip().lower()

    kp_env = os.getenv("KEYPAIR_PATH")
    if kp_env and auto_select not in {"1", "true", "yes"}:
        candidate = _normalize(Path(kp_env).expanduser())
        if not candidate.exists():
            raise FileNotFoundError(f"KEYPAIR_PATH not found: {candidate}")
        os.environ.setdefault("KEYPAIR_PATH", str(candidate))
        os.environ.setdefault("SOLANA_KEYPAIR", str(candidate))
        return candidate

    kp_dir = Path(dir_)
    candidates = sorted(kp_dir.glob("*.json"))
    if not candidates:
        raise FileNotFoundError(f"No keypairs in {kp_dir}")

    if len(candidates) == 1:
        chosen = candidates[0]
        os.environ.setdefault("KEYPAIR_PATH", str(chosen))
        os.environ.setdefault("SOLANA_KEYPAIR", str(chosen))
        return chosen

    if auto_select in {"1", "true", "yes"}:
        raise RuntimeError(
            "Multiple keypairs available and AUTO_SELECT_KEYPAIR is enabled. "
            "Export KEYPAIR_PATH to select one explicitly."
        )

    raise RuntimeError(
        "Multiple keypairs available and KEYPAIR_PATH not set. "
        "Export KEYPAIR_PATH to select one explicitly."
    )


def validate_agent_weights(cfg: Dict[str, Any]) -> Dict[str, float]:
    agents = list(cfg.get("agents", []))
    weights = dict(cfg.get("agent_weights", {}))
    for a in agents:
        if a not in weights:
            weights[a] = 1.0
    total = sum(weights.get(a, 0.0) for a in agents) or 1.0
    cfg["agent_weights"] = {a: weights[a] / total for a in agents}
    return cfg["agent_weights"]


def verify_flashloan_prereqs(cfg: Dict[str, Any]):
    use_flash = cfg.get("use_flash_loans", False)
    if isinstance(use_flash, str):
        use_flash = use_flash.strip().lower() in {"1", "true", "yes"}
    elif isinstance(use_flash, (int, float)):
        use_flash = bool(use_flash)
    if not use_flash:
        return
    pv = float(cfg.get("portfolio_value", 0.0))
    if pv <= 0.0:
        raise RuntimeError("Flash loans enabled but portfolio_value <= 0")
    proto = cfg.get("flash_loan_protocol", {})
    required = ("program_id", "pool", "fee_bps")
    missing = [k for k in required if k not in proto]
    if missing:
        raise RuntimeError(f"Flash loans enabled but protocol config missing: {missing}")
