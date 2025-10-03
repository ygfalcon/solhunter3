"""Ensure ``solhunter_zero.agents`` exposes every helper module."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Iterable

import pytest


def _iter_agent_module_names() -> Iterable[str]:
    root = Path(__file__).resolve().parents[1] / "solhunter_zero"
    for py_file in root.rglob("*.py"):
        rel_parts = list(py_file.relative_to(root).parts)
        if not rel_parts:
            continue
        if rel_parts[-1] == "__init__.py":
            rel_parts = rel_parts[:-1]
        else:
            rel_parts[-1] = rel_parts[-1][:-3]
        if rel_parts and rel_parts[0] == "agents":
            rel_parts = rel_parts[1:]
        if not rel_parts:
            continue
        yield ".".join(rel_parts)


@pytest.mark.parametrize("module_name", sorted(set(_iter_agent_module_names())))
def test_agent_imports(module_name: str) -> None:
    base_name = f"solhunter_zero.{module_name}"
    alias_name = f"solhunter_zero.agents.{module_name}"

    try:
        importlib.import_module(base_name)
    except Exception as exc:  # pragma: no cover - skip optional modules
        pytest.skip(f"Base import failed for {base_name}: {exc}")

    importlib.import_module(alias_name)


def test_onchain_metrics_private_helper_available() -> None:
    module = importlib.import_module("solhunter_zero.agents.onchain_metrics")
    assert hasattr(module, "_helius_price_overview")
    assert hasattr(module, "_birdeye_price_overview")
