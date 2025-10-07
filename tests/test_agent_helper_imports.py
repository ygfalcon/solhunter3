"""Ensure legacy helper modules remain importable via solhunter_zero.agents."""
from __future__ import annotations

import importlib
import os

import pytest

LEGACY_HELPERS = [
    "config",
    "dex_config",
    "exchange",
    "prices",
    "depth_client",
    "order_book_ws",
    "scanner_common",
    "token_scanner",
    "token_discovery",
    "onchain_metrics",
    "http",
    "dynamic_limit",
    "resource_monitor",
    "util",
]


@pytest.mark.parametrize("module", LEGACY_HELPERS)
def test_legacy_helper_imports(monkeypatch: pytest.MonkeyPatch, module: str) -> None:
    # Provide a default RPC URL so helper modules that inspect the environment
    # (e.g. token_scanner) do not raise during import in test environments.
    monkeypatch.setenv(
        "SOLANA_RPC_URL",
        os.getenv(
            "SOLANA_RPC_URL",
            "https://mainnet.helius-rpc.com/?api-key=test",  # harmless default
        ),
    )
    importlib.import_module(f"solhunter_zero.agents.{module}")
