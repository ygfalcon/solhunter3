from __future__ import annotations

import importlib


def test_agents_system_shim_exports_expected_helpers() -> None:
    module = importlib.import_module("solhunter_zero.agents.system")

    from solhunter_zero import system as root_system

    assert module.detect_cpu_count is root_system.detect_cpu_count
    assert module.set_rayon_threads is root_system.set_rayon_threads
    assert module.main is root_system.main
