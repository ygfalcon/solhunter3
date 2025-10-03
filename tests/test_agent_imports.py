from __future__ import annotations

import importlib
import pkgutil

import solhunter_zero.agents as agents_pkg

LEGACY_SHIMS = {
    "solhunter_zero.agents.dynamic_limit",
    "solhunter_zero.agents.http",
    "solhunter_zero.agents.onchain_metrics",
    "solhunter_zero.agents.resource_monitor",
    "solhunter_zero.agents.system",
    "solhunter_zero.agents.util",
}


def test_agent_modules_discoverable_and_importable() -> None:
    discovered = {
        module_info.name
        for module_info in pkgutil.walk_packages(
            agents_pkg.__path__, agents_pkg.__name__ + "."
        )
    }

    missing = LEGACY_SHIMS - discovered
    assert not missing, f"Missing legacy shims: {sorted(missing)}"

    for module_name in sorted(LEGACY_SHIMS):
        importlib.import_module(module_name)
