import importlib
import time

import pytest
pytest.importorskip("google.protobuf")

from solhunter_zero import event_pb2
import solhunter_zero.event_bus as ev


def _measure(monkeypatch, threshold: int) -> float:
    monkeypatch.setenv("EVENT_COMPRESSION", "zlib")
    monkeypatch.setenv("EVENT_COMPRESSION_THRESHOLD", str(threshold))
    ev_mod = importlib.reload(ev)
    data = event_pb2.WeightsUpdated(weights={"x": 1.0})
    start = time.perf_counter()
    for _ in range(5000):
        ev_mod._encode_event("weights_updated", data)
    return time.perf_counter() - start


def test_compression_threshold_benchmark(monkeypatch):
    t_compress = _measure(monkeypatch, 0)
    t_skip = _measure(monkeypatch, 512)
    assert t_skip <= t_compress
