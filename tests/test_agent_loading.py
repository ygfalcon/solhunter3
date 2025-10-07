"""Ensure agent registry gracefully handles legacy module layouts."""

import importlib


def test_arbitrage_agent_fallback(monkeypatch):
    """AgentManager should fall back to legacy module exports."""

    agents_pkg = importlib.import_module("solhunter_zero.agents")
    module = importlib.import_module("solhunter_zero.agents.arbitrage")

    original = getattr(module, "ArbitrageAgent")
    monkeypatch.delattr(module, "ArbitrageAgent", raising=True)

    try:
        cls = agents_pkg._load_agent_class("arbitrage", "arbitrage", "ArbitrageAgent")
    finally:
        setattr(module, "ArbitrageAgent", original)

    assert cls is original
