from __future__ import annotations

import importlib
import pkgutil

import solhunter_zero.agents as agents_pkg

LEGACY_AGENT_ALIASES = {
    f"solhunter_zero.agents.{name}"
    for name in (
        "config",
        "dex_config",
        "discovery",
        "dynamic_limit",
        "exchange",
        "http",
        "order_book_ws",
        "onchain_metrics",
        "prices",
        "resource_monitor",
        "runtime",
        "scanner_common",
        "system",
        "token_discovery",
        "token_scanner",
        "util",
    )
}


def test_agent_modules_discoverable_and_importable() -> None:
    discovered = {
        module_info.name
        for module_info in pkgutil.walk_packages(
            agents_pkg.__path__, agents_pkg.__name__ + "."
        )
    }

    for module_name in sorted(discovered | LEGACY_AGENT_ALIASES):
        importlib.import_module(module_name)


def test_onchain_metrics_private_helper_available() -> None:
    module = importlib.import_module("solhunter_zero.agents.onchain_metrics")
    assert hasattr(module, "_helius_price_overview")
    assert hasattr(module, "_birdeye_price_overview")
    from solhunter_zero.agents.onchain_metrics import (  # noqa: F401
        _birdeye_price_overview,
        _helius_price_overview,
    )

