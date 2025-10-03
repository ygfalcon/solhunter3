from __future__ import annotations

import importlib


def test_http_shim_reexports_http_module() -> None:
    shim = importlib.import_module("solhunter_zero.agents.http")
    real = importlib.import_module("solhunter_zero.http")

    assert shim.get_session is real.get_session
    assert shim.close_session is real.close_session
    assert shim.dumps is real.dumps
    assert shim.loads is real.loads
    assert shim.check_endpoint is real.check_endpoint
