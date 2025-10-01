import importlib
import os
import sys
import types


def test_event_bus_url_alias(monkeypatch):
    dummy_config = types.ModuleType("solhunter_zero.config")
    dummy_config.get_event_bus_peers = lambda cfg=None: []
    dummy_config.get_event_bus_url = (
        lambda cfg=None: os.getenv("EVENT_BUS_URL", "")
    )
    sys.modules["solhunter_zero.config"] = dummy_config

    import solhunter_zero.event_bus as ev
    importlib.reload(ev)
    monkeypatch.delenv("BROKER_WS_URLS", raising=False)
    monkeypatch.setenv("EVENT_BUS_URL", "ws://bus")
    assert ev._resolve_ws_urls(None) == {"ws://bus"}
    monkeypatch.setenv("BROKER_WS_URLS", "ws://other")
    assert ev._resolve_ws_urls(None) == {"ws://other"}
    monkeypatch.delenv("BROKER_WS_URLS", raising=False)
