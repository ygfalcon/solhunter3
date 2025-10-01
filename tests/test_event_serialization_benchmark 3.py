import importlib
import time
import pytest

pytest.importorskip("google.protobuf")

import solhunter_zero.event_bus as ev


def _measure(mode: str) -> float:
    import os
    os.environ["EVENT_SERIALIZATION"] = mode
    importlib.reload(ev)
    data = {"a": 1, "b": [1, 2, 3]}
    start = time.perf_counter()
    for _ in range(20000):
        ev._loads(ev._dumps(data))
    return time.perf_counter() - start


def test_serialization_benchmark(monkeypatch):
    pytest.importorskip("msgpack")
    assert _measure("json") > 0
    assert _measure("msgpack") > 0
