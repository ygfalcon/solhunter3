"""Helpers for resolving Pyth price feed identifiers before startup."""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

HERMES_BASE = os.environ.get("PYTH_HERMES_URL", "https://hermes.pyth.network")

# Map token mints -> desired Pyth symbols.
_MINT_TO_SYMBOL = {
    "So11111111111111111111111111111111111111112": "SOL/USD",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9": "USDC/USD",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT/USD",
}


def _hermes_get(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=5.0) as resp:
        return resp.read()


def discover_price_feed_ids() -> dict[str, str]:
    """Return mapping of mint -> price feed id hex discovered via Hermes."""

    out: dict[str, str] = {}
    for mint, symbol in _MINT_TO_SYMBOL.items():
        q = urllib.parse.urlencode({"query": symbol})
        url = f"{HERMES_BASE}/v2/price_feeds?{q}"
        data = json.loads(_hermes_get(url).decode("utf-8"))
        if not data:
            raise RuntimeError(f"No Hermes feed found for symbol {symbol}")
        feed_id = data[0]["id"]
        out[mint] = feed_id
    return out


def ensure_env_has_pyth_price_ids(env_path: str | None = None) -> dict[str, str]:
    """Ensure ``PYTH_PRICE_IDS`` env var contains a valid mint->feed mapping."""

    del env_path  # reserved for future expansion; silences unused argument warnings
    mapping = os.getenv("PYTH_PRICE_IDS")
    ok = False
    if mapping:
        try:
            parsed = json.loads(mapping)
        except Exception:
            parsed = None
        else:
            ok = all(
                isinstance(k, str)
                and isinstance(v, str)
                and k in _MINT_TO_SYMBOL
                and v.startswith("0x")
                for k, v in parsed.items()
            )
    if not ok:
        discovered = discover_price_feed_ids()
        os.environ["PYTH_PRICE_IDS"] = json.dumps(discovered)
        return discovered
    return json.loads(mapping)

