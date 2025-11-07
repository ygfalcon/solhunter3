import subprocess
from unittest.mock import Mock

import pytest

from solhunter_zero.runtime.orchestrator import RuntimeOrchestrator
from solhunter_zero.runtime.trading_runtime import DEPTH_PROCESS_SHUTDOWN_TIMEOUT


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_stop_all_terminates_depth_process():
    orch = RuntimeOrchestrator(run_http=False)

    proc = Mock()
    proc.poll.return_value = None
    proc.wait.side_effect = [
        subprocess.TimeoutExpired(cmd="depth_service", timeout=1),
        None,
    ]
    orch.handles.depth_proc = proc

    await orch.stop_all()

    proc.terminate.assert_called_once_with()
    proc.kill.assert_called_once_with()
    assert proc.wait.call_count == 2
    first_call = proc.wait.call_args_list[0]
    assert first_call.kwargs == {"timeout": DEPTH_PROCESS_SHUTDOWN_TIMEOUT}
    second_call = proc.wait.call_args_list[1]
    assert second_call.kwargs == {"timeout": DEPTH_PROCESS_SHUTDOWN_TIMEOUT}
