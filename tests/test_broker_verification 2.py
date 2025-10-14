import asyncio
import logging
import types
import importlib

import pytest


@pytest.mark.asyncio
async def test_verify_broker_connection_success(monkeypatch):
    monkeypatch.delenv("BROKER_URL", raising=False)
    monkeypatch.delenv("BROKER_URLS", raising=False)
    import solhunter_zero.event_bus as ev
    ev = importlib.reload(ev)

    class FakePubSub:
        def __init__(self):
            self.queue = asyncio.Queue()

        async def subscribe(self, ch):
            self.ch = ch

        async def get_message(self, ignore_subscribe_messages=True, timeout=0):
            try:
                return await asyncio.wait_for(self.queue.get(), timeout)
            except asyncio.TimeoutError:
                return None

        async def unsubscribe(self, ch):
            pass

    class FakeRedis:
        def __init__(self, url):
            self.pubsub_obj = FakePubSub()

        def pubsub(self):
            return self.pubsub_obj

        async def publish(self, ch, msg):
            await self.pubsub_obj.queue.put({"type": "message", "data": msg})

        async def close(self):
            pass

    fake = types.SimpleNamespace(from_url=lambda url: FakeRedis(url))
    monkeypatch.setattr(ev, "aioredis", fake)

    assert await ev.verify_broker_connection(["redis://test"]) is True


@pytest.mark.asyncio
async def test_verify_broker_connection_failure(monkeypatch, caplog):
    monkeypatch.delenv("BROKER_URL", raising=False)
    monkeypatch.delenv("BROKER_URLS", raising=False)
    import solhunter_zero.event_bus as ev
    ev = importlib.reload(ev)

    class FakePubSub:
        async def subscribe(self, ch):
            pass

        async def get_message(self, ignore_subscribe_messages=True, timeout=0):
            return None

        async def unsubscribe(self, ch):
            pass

    class FakeRedis:
        def __init__(self, url):
            self.pubsub_obj = FakePubSub()

        def pubsub(self):
            return self.pubsub_obj

        async def publish(self, ch, msg):
            raise RuntimeError("boom")

        async def close(self):
            pass

    fake = types.SimpleNamespace(from_url=lambda url: FakeRedis(url))
    monkeypatch.setattr(ev, "aioredis", fake)

    with caplog.at_level(logging.ERROR):
        ok = await ev.verify_broker_connection(["redis://fail"], timeout=0.01)
    assert not ok
    assert "redis://fail" in caplog.text


def test_startup_abort_on_verify_failure(monkeypatch):
    cfg_text = (
        "solana_rpc_url='https://mainnet.helius-rpc.com/?api-key=demo-helius-key'\n"
        "dex_base_url='https://quote-api.jup.ag'\n"
        "agents=['dummy']\n"
        "agent_weights={dummy=1.0}\n"
    )
    monkeypatch.syspath_prepend(".")
    path = "config.toml"
    with open(path, "w", encoding="utf8") as f:
        f.write(cfg_text)
    monkeypatch.setenv("SOLHUNTER_CONFIG", path)

    import solhunter_zero.main as main_module

    monkeypatch.setattr(main_module, "ensure_connectivity", lambda *a, **k: None)
    monkeypatch.setattr(main_module.metrics_aggregator, "start", lambda: None)
    monkeypatch.setenv("BROKER_VERIFY_ABORT", "1")

    async def _fail(*a, **k):
        return False

    monkeypatch.setattr(main_module.event_bus, "verify_broker_connection", _fail)

    with pytest.raises(SystemExit):
        main_module.main(
            memory_path="sqlite:///:memory:",
            loop_delay=0,
            dry_run=True,
            iterations=0,
        )
