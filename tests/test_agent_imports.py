from __future__ import annotations

import importlib
import pkgutil

import pytest

import solhunter_zero.agents as agents_pkg

LEGACY_SHIMS = {
    "solhunter_zero.agents.dynamic_limit",
    "solhunter_zero.agents.discovery",
    "solhunter_zero.agents.http",
    "solhunter_zero.agents.onchain_metrics",
    "solhunter_zero.agents.scanner_common",
    "solhunter_zero.agents.resource_monitor",
    "solhunter_zero.agents.runtime",
    "solhunter_zero.agents.system",
    "solhunter_zero.agents.util",
}


def _discovered_agent_modules() -> set[str]:
    return {
        module_info.name
        for module_info in pkgutil.walk_packages(
            agents_pkg.__path__, agents_pkg.__name__ + "."
        )
    }


@pytest.mark.parametrize(
    "module_name", sorted(_discovered_agent_modules() | LEGACY_SHIMS)
)
def test_agent_modules_discoverable_and_importable(module_name: str) -> None:
    importlib.import_module(module_name)


def test_onchain_metrics_private_helper_available() -> None:
    module = importlib.import_module("solhunter_zero.agents.onchain_metrics")
    assert hasattr(module, "_helius_price_overview")
    assert hasattr(module, "_birdeye_price_overview")
    from solhunter_zero.agents.onchain_metrics import (  # noqa: F401
        _birdeye_price_overview,
        _helius_price_overview,
    )

