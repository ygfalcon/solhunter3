"""Canonical stream and key names for the Golden Snapshot pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StreamNames:
    """Container describing the message bus stream identifiers."""

    discovery_candidates: str = "x:discovery.candidates"
    token_snapshot: str = "x:token.snap"
    market_ohlcv: str = "x:market.ohlcv.5m"
    market_depth: str = "x:market.depth"
    golden_snapshot: str = "x:mint.golden"
    trade_suggested: str = "x:trade.suggested"
    vote_decisions: str = "x:vote.decisions"
    virtual_fills: str = "x:virt.fills"
    live_fills: str = "x:live.fills"


STREAMS = StreamNames()


def discovery_seen_key(mint: str) -> str:
    """Return the discovery dedupe key for ``mint``."""

    return f"discovery:seen:{mint}"


def discovery_cursor_key() -> str:
    """Return the global discovery cursor key."""

    return "discovery:cursor"


def golden_hash_key(mint: str) -> str:
    """Return the KV key storing the latest Golden hash for ``mint``."""

    return f"snap:golden:{mint}"


def vote_dedupe_key(client_order_id: str) -> str:
    """Return the vote idempotency key for ``client_order_id``."""

    return f"vote:dupe:{client_order_id}"


__all__ = [
    "STREAMS",
    "StreamNames",
    "discovery_seen_key",
    "discovery_cursor_key",
    "golden_hash_key",
    "vote_dedupe_key",
]
