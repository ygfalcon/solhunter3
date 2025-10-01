import sys
import types
import importlib
import importlib.util


def test_metrics_export(monkeypatch):
    posted = {}

    class FakeResp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def raise_for_status(self):
            pass

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def post(self, url, json=None, timeout=5):
            posted['url'] = url
            posted['data'] = json
            return FakeResp()

    aiohttp_mod = types.ModuleType('aiohttp')
    aiohttp_mod.ClientSession = lambda: FakeSession()
    monkeypatch.setitem(sys.modules, 'aiohttp', aiohttp_mod)

    bus_mod = types.ModuleType('solhunter_zero.event_bus')
    _subs = []

    def subscribe(topic, handler):
        _subs.append((topic, handler))
        return lambda: _subs.remove((topic, handler))

    def publish(topic, payload):
        for t, h in list(_subs):
            if t == topic:
                h(payload)

    bus_mod.subscribe = subscribe
    bus_mod.publish = publish
    monkeypatch.setitem(sys.modules, 'solhunter_zero.event_bus', bus_mod)

    from solhunter_zero import event_bus as eb
    import solhunter_zero.metrics_client as mc
    importlib.reload(mc)

    unsub = mc.start_metrics_exporter('http://endpoint')
    eb.publish('rl_metrics', {'loss': 1.0, 'reward': 0.5})
    unsub()

    assert posted['url'] == 'http://endpoint'
    assert posted['data'] == {'loss': 1.0, 'reward': 0.5}
