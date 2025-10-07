"""Tests for the agent bootstrap helpers."""
from __future__ import annotations

import importlib
import os

import pytest

from solhunter_zero.agents import BUILT_IN_AGENTS, _ensure_agents_loaded


@pytest.fixture(autouse=True)
def _default_rpc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "SOLANA_RPC_URL",
        os.getenv(
            "SOLANA_RPC_URL",
            "https://mainnet.helius-rpc.com/?api-key=test",
        ),
    )


def test_ensure_agents_loaded_skips_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    """Import failures should not abort agent registration."""

    original_import = importlib.import_module

    def fake_import(name: str, package: str | None = None):
        if name in {"solhunter_zero.agents.dqn", ".dqn"} and package in {
            None,
            __package__,
            "solhunter_zero.agents",
        }:
            raise RuntimeError("boom")
        return original_import(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import)

    original_registry = BUILT_IN_AGENTS.copy()
    try:
        BUILT_IN_AGENTS.clear()
        _ensure_agents_loaded()

        assert "simulation" in BUILT_IN_AGENTS
        assert "dqn" not in BUILT_IN_AGENTS
    finally:
        BUILT_IN_AGENTS.clear()
        BUILT_IN_AGENTS.update(original_registry)
