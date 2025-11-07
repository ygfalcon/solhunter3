import types

import pytest

from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_collect_status_handles_depth_proc_without_poll():
    runtime = trading_runtime.TradingRuntime()
    runtime.depth_proc = types.SimpleNamespace()

    runtime._collect_rl_status = lambda: {}
    runtime._collect_iteration = lambda: {}
    runtime.activity.snapshot = lambda: []

    status = runtime._collect_status()

    assert status["depth_service"] is True
