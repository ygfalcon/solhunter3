from __future__ import annotations

import inspect
import os
from typing import Any, Dict, Iterable, Mapping, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - hints only
    from .memory import Memory as MemoryProtocol
else:
    MemoryProtocol = Any

_MEMORY_CLS: Any | None = None


class TradeAnalyzer:
    """Analyze trades stored by :class:`MemoryAgent`."""

    def __init__(self, memory: MemoryProtocol) -> None:
        self.memory = memory

    # --------------------------------------------------------------
    def roi_by_agent(self) -> Dict[str, float]:
        """Return ROI aggregated by agent name."""
        trades_obj = self.memory.list_trades(limit=1000)
        if inspect.isawaitable(trades_obj):
            import asyncio

            trades = asyncio.run(trades_obj)
        else:
            trades = trades_obj
        trades = list(trades or [])
        summary: Dict[str, Dict[str, float]] = {}
        def _get(obj: Any, key: str) -> Any:
            if isinstance(obj, Mapping):
                return obj.get(key)
            return getattr(obj, key, None)

        for t in trades:
            name = _get(t, "reason") or ""
            if not isinstance(name, str):
                name = str(name)
            info = summary.setdefault(name, {"buy": 0.0, "sell": 0.0})
            direction = str(_get(t, "direction") or "").lower()
            if direction not in ("buy", "sell"):
                continue
            realized_roi = _get(t, "realized_roi")
            realized_notional = _get(t, "realized_notional")
            use_realized = False
            if realized_roi is not None and realized_notional is not None:
                try:
                    realized_notional = float(realized_notional)
                    realized_roi = float(realized_roi)
                except (TypeError, ValueError):
                    realized_roi = None
                    realized_notional = None
                else:
                    use_realized = True
            if use_realized:
                # Use realized metrics when provided.
                if realized_notional <= 0:
                    continue
                if direction == "buy":
                    info["buy"] += realized_notional
                else:
                    info["sell"] += realized_notional * (1.0 + realized_roi)
                continue
            try:
                amount = float(_get(t, "amount") or 0.0)
                price = float(_get(t, "price") or 0.0)
            except (TypeError, ValueError):
                continue
            amount = max(0.0, amount)
            price = max(0.0, price)
            info[direction] += amount * price
        rois = {}
        for name, info in summary.items():
            spent = info.get("buy", 0.0)
            revenue = info.get("sell", 0.0)
            if spent > 0:
                rois[name] = float((revenue - spent) / spent)
        return rois

    # --------------------------------------------------------------
    def recommend_weights(
        self, base_weights: Dict[str, float] | None = None, *, step: float = 0.1
    ) -> Dict[str, float]:
        """Suggest weight updates based on historical ROI."""
        try:
            step = float(step)
        except (TypeError, ValueError):
            step = 0.1
        step = max(0.0, step)
        base = {
            str(k): float(v)
            for k, v in (base_weights or {}).items()
            if v is not None
        }
        rois = self.roi_by_agent()
        new_weights = dict(base)
        for name, roi in rois.items():
            w = new_weights.get(name, 1.0)
            try:
                roi = float(roi)
            except (TypeError, ValueError):
                continue
            if roi > 0:
                w = w * (1.0 + step)
            elif roi < 0:
                w = w * (1.0 - step)
            new_weights[name] = max(0.0, w)
        return new_weights


# ------------------------------------------------------------------
def analyze_trades(
    memory_url: str,
    config_paths: Iterable[str] | None = None,
    *,
    weights_out: str | None = None,
    step: float = 0.1,
) -> Dict[str, float]:
    """Convenience helper used by ``backtest_cli``."""

    global _MEMORY_CLS
    if _MEMORY_CLS is None:
        from .memory import Memory as _Memory

        _MEMORY_CLS = _Memory

    mem = _MEMORY_CLS(memory_url)
    analyzer = TradeAnalyzer(mem)
    base: Dict[str, float] = {}
    if config_paths:
        import tomllib

        for path in config_paths:
            with open(path, "rb") as fh:
                cfg = tomllib.load(fh)
            weights = cfg.get("agent_weights", {})
            for k, v in weights.items():
                try:
                    base[str(k)] = float(v)
                except (TypeError, ValueError):
                    continue
    new = analyzer.recommend_weights(base, step=step)
    if weights_out:
        os.makedirs(os.path.dirname(weights_out) or ".", exist_ok=True)
        lines = [f"{k} = {new[k]}" for k in sorted(new)]
        with open(weights_out, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    return new
