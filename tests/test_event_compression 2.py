import importlib
import os

import pytest

import solhunter_zero.event_bus as ev


@pytest.mark.parametrize(
    "algo,lib",
    [
        ("zstd", "zstandard"),
        ("lz4", "lz4.frame"),
        ("zlib", None),
    ],
)
def test_event_compression_round_trip(monkeypatch, algo, lib):
    if lib is not None:
        pytest.importorskip(lib)
    monkeypatch.setenv("EVENT_COMPRESSION", algo)
    monkeypatch.delenv("EVENT_COMPRESSION_THRESHOLD", raising=False)
    ev_mod = importlib.reload(ev)
    data = os.urandom(ev_mod.EVENT_COMPRESSION_THRESHOLD + 1)
    assert ev_mod._maybe_decompress(ev_mod._compress_event(data)) == data
