import importlib
import asyncio
import pytest


def test_json_fallback(monkeypatch):
    import solhunter_zero.optional_imports as oi
    orig_try = oi.try_import

    def fake_try(name, stub=oi._SENTINEL):
        if name == "orjson":
            return stub
        return orig_try(name, stub)

    monkeypatch.setattr(oi, "try_import", fake_try)

    import solhunter_zero.http as http
    http = importlib.reload(http)

    data = http.dumps({"a": 1})
    assert http.loads(data)["a"] == 1
    assert not http.USE_ORJSON


def test_connector_limit_env(monkeypatch):
    monkeypatch.setenv("HTTP_CONNECTOR_LIMIT", "5")
    import solhunter_zero.http as http
    http = importlib.reload(http)
    assert http.CONNECTOR_LIMIT == 5


@pytest.mark.asyncio
async def test_get_session_singleton(monkeypatch):
    import solhunter_zero.http as http
    http = importlib.reload(http)
    await http.close_session()
    s1 = await http.get_session()
    s2 = await http.get_session()
    assert s1 is s2
    await http.close_session()


def test_module_imports(monkeypatch):
    import solhunter_zero.http as http
    import solhunter_zero.jito_stream as js
    assert js.get_session is http.get_session
