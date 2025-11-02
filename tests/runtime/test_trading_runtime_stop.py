import subprocess
from unittest.mock import Mock

import pytest

from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_stop_waits_for_depth_process():
    runtime = trading_runtime.TradingRuntime()

    proc = Mock()
    proc.poll.return_value = None
    runtime.depth_proc = proc

    await runtime.stop()

    proc.terminate.assert_called_once_with()
    proc.wait.assert_called_once_with(
        timeout=trading_runtime.DEPTH_PROCESS_SHUTDOWN_TIMEOUT
    )


@pytest.mark.anyio("asyncio")
async def test_stop_reports_depth_process_timeout(caplog):
    runtime = trading_runtime.TradingRuntime()

    proc = Mock()
    proc.poll.return_value = None
    proc.wait.side_effect = subprocess.TimeoutExpired(cmd="depth_service", timeout=1)
    runtime.depth_proc = proc

    await runtime.stop()

    message = (
        "depth_service process did not exit within "
        f"{trading_runtime.DEPTH_PROCESS_SHUTDOWN_TIMEOUT} seconds"
    )
    entries = runtime.activity.snapshot()
    assert any(
        entry["stage"] == "depth_service" and entry["detail"] == message
        and entry["ok"] is False
        for entry in entries
    )

    assert any(message in record.getMessage() for record in caplog.records)
