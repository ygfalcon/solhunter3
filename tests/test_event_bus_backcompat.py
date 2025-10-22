import importlib
import json as std_json
import os

import pytest

pytest.importorskip("google.protobuf")
pytest.importorskip("msgpack")

import solhunter_zero.event_bus as event_bus


def test_loads_falls_back_to_json(monkeypatch):
    prev = os.environ.get("EVENT_SERIALIZATION")
    monkeypatch.setenv("EVENT_SERIALIZATION", "msgpack")
    module = importlib.reload(event_bus)
    payload = {"topic": "legacy", "payload": {"value": 1}}
    blob = std_json.dumps(payload).encode()
    try:
        assert module._loads(blob) == payload
    finally:
        if prev is None:
            os.environ.pop("EVENT_SERIALIZATION", None)
        else:
            os.environ["EVENT_SERIALIZATION"] = prev
        importlib.reload(module)
