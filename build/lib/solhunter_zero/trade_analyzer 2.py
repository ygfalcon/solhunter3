from __future__ import annotations

from typing import Dict, Iterable

from .memory import Memory


class TradeAnalyzer:
    """Analyze trades stored by :class:`MemoryAgent`."""

    def __init__(self, memory: Memory) -> None:
        self.memory = memory

    # --------------------------------------------------------------
    def roi_by_agent(self) -> Dict[str, float]:
        """Return ROI aggregated by agent name."""
        trades = self.memory.list_trades(limit=1000)
        summary: Dict[str, Dict[str, float]] = {}
        for t in trades:
            name = t.reason or ""
            info = summary.setdefault(name, {"buy": 0.0, "sell": 0.0})
            info[t.direction] += float(t.amount) * float(t.price)
        rois = {}
        for name, info in summary.items():
            spent = info.get("buy", 0.0)
            revenue = info.get("sell", 0.0)
            if spent > 0:
                rois[name] = (revenue - spent) / spent
        return rois

    # --------------------------------------------------------------
    def recommend_weights(
        self, base_weights: Dict[str, float] | None = None, *, step: float = 0.1
    ) -> Dict[str, float]:
        """Suggest weight updates based on historical ROI."""
        base = {str(k): float(v) for k, v in (base_weights or {}).items()}
        rois = self.roi_by_agent()
        new_weights = dict(base)
        for name, roi in rois.items():
            w = new_weights.get(name, 1.0)
            if roi > 0:
                new_weights[name] = w * (1.0 + step)
            elif roi < 0:
                new_weights[name] = w * (1.0 - step)
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

    mem = Memory(memory_url)
    analyzer = TradeAnalyzer(mem)
    base: Dict[str, float] = {}
    if config_paths:
        import tomllib

        for path in config_paths:
            with open(path, "rb") as fh:
                cfg = tomllib.load(fh)
            weights = cfg.get("agent_weights", {})
            for k, v in weights.items():
                base[str(k)] = float(v)
    new = analyzer.recommend_weights(base, step=step)
    if weights_out:
        lines = [f"{k} = {v}" for k, v in new.items()]
        with open(weights_out, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    return new
