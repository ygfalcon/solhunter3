from __future__ import annotations

import importlib
import pkgutil

import solhunter_zero.agents as agents_pkg

LEGACY_AGENT_ALIASES = {
    f"solhunter_zero.agents.{name}"
    for name in (
        "util",
        "http",
        "dynamic_limit",
        "resource_monitor",
        "system",
        "onchain_metrics",
        "runtime",
        "discovery",
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
    from solhunter_zero.agents.onchain_metrics import _helius_price_overview  # noqa: F401

