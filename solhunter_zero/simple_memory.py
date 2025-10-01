from __future__ import annotations

"""A minimal in-memory Memory fallback used for investor_demo."""

from typing import Any, Dict, List


class SimpleMemory:
    """In-memory stand-in for :class:`Memory`.

    Stores trades and VaR measurements in plain Python lists. Methods mirror a
    subset of the asynchronous interface provided by the real SQLAlchemy-backed
    implementation so that demos can run without the dependency.
    """

    def __init__(self) -> None:
        self._trades: List[Dict[str, Any]] = []
        self._vars: List[float] = []
        self._next_id = 1

    async def log_trade(self, **kwargs: Any) -> int:
        """Record a trade and return its identifier."""
        entry = dict(kwargs)
        entry["id"] = self._next_id
        self._next_id += 1
        self._trades.append(entry)
        return entry["id"]

    async def list_trades(
        self,
        *,
        token: str | None = None,
        limit: int | None = None,
        since_id: int | None = None,
    ) -> List[Dict[str, Any]]:
        """Return trades optionally filtered by ``token`` or ``since_id``."""
        trades = self._trades
        if token is not None:
            trades = [t for t in trades if t.get("token") == token]
        if since_id is not None:
            trades = [t for t in trades if t.get("id", 0) > since_id]
        trades = sorted(trades, key=lambda t: t.get("id", 0))
        if limit is not None:
            trades = trades[:limit]
        return list(trades)

    def log_var(self, value: float) -> None:
        """Record a value-at-risk measurement."""
        self._vars.append(value)

    async def close(self) -> None:  # pragma: no cover - nothing to clean up
        """Compatibility method for API parity with :class:`Memory`."""
        return None

    def list_vars(self) -> List[float]:  # pragma: no cover - unused in demo
        return list(self._vars)
