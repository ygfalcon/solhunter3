"""Compatibility shim exporting :class:`~solhunter_zero.arbitrage.ArbitrageAgent`."""

from __future__ import annotations

from ..arbitrage import ArbitrageAgent, PriceFeed

__all__ = ["ArbitrageAgent", "PriceFeed"]
