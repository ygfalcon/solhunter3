import logging
import importlib
import solhunter_zero.event_bus as ev


def test_validate_ws_urls_warns_on_empty(caplog):
    importlib.reload(ev)
    caplog.set_level(logging.WARNING)
    urls = ev._validate_ws_urls(["", "   "])
    assert urls == {ev.DEFAULT_WS_URL}
    assert any("BROKER_WS_URLS is empty" in record.message for record in caplog.records)
