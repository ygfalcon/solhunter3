import asyncio
import solhunter_zero.exchange as exchange
import solhunter_zero.agents.execution as execution_mod
import solhunter_zero.agents.mev_sandwich as sandwich_mod
import solhunter_zero.arbitrage as arb_mod
import solhunter_zero.depth_client as depth_client_mod


def test_agents_use_resolved_swap_endpoints(monkeypatch):
    mapping = {"service": "https://venue.test/api/"}
    monkeypatch.setattr(exchange, "VENUE_URLS", dict(mapping), raising=False)
    monkeypatch.setattr(execution_mod, "VENUE_URLS", dict(mapping), raising=False)
    monkeypatch.setattr(arb_mod, "VENUE_URLS", dict(mapping), raising=False)
    monkeypatch.setattr(exchange, "DEX_BASE_URL", "https://venue.test/api/", raising=False)
    monkeypatch.setattr(execution_mod, "DEX_BASE_URL", "https://venue.test/api/", raising=False)
    monkeypatch.setattr(exchange, "SWAP_PATHS", {"service": "/custom_swap"}, raising=False)
    monkeypatch.setattr(exchange, "DEFAULT_SWAP_PATH", "/custom_swap", raising=False)
    monkeypatch.setattr(exchange, "SWAP_PATH", "/custom_swap", raising=False)

    class DummySubscription:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(execution_mod, "subscription", lambda *a, **k: DummySubscription())

    execution_calls: list[str] = []

    class ExecutionResponse:
        def __init__(self, url: str):
            self.url = url

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {"swapTransaction": "encoded"}

        def raise_for_status(self):
            return None

    class ExecutionSession:
        def post(self, url, json, timeout=10):
            execution_calls.append(url)
            return ExecutionResponse(url)

    async def fake_exec_session():
        return ExecutionSession()

    async def fake_submit_raw_tx(*_a, **_k):
        return None

    monkeypatch.setattr(execution_mod, "get_session", fake_exec_session)
    monkeypatch.setattr(execution_mod, "submit_raw_tx", fake_submit_raw_tx)
    monkeypatch.setattr(depth_client_mod, "snapshot", lambda token: ({}, 0.0))

    async def fake_prepare_signed_tx(tx_b64, priority_fee=None):
        return f"signed:{tx_b64}:{priority_fee}"

    monkeypatch.setattr(depth_client_mod, "prepare_signed_tx", fake_prepare_signed_tx)

    sandwich_calls: list[str] = []

    class SandwichResponse:
        def __init__(self, url: str):
            self.url = url

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {"swapTransaction": "sandwich"}

        def raise_for_status(self):
            return None

    class SandwichSession:
        def post(self, url, json, timeout=10):
            sandwich_calls.append(url)
            return SandwichResponse(url)

    async def fake_sandwich_session():
        return SandwichSession()

    monkeypatch.setattr(sandwich_mod, "get_session", fake_sandwich_session)

    arb_calls: list[str] = []

    class ArbResponse:
        def __init__(self, url: str):
            self.url = url

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {"swapTransaction": "arb"}

        def raise_for_status(self):
            return None

    class ArbSession:
        def post(self, url, json, timeout=10):
            arb_calls.append(url)
            return ArbResponse(url)

    async def fake_arb_session():
        return ArbSession()

    async def fake_prepare_bundle(tx_b64):
        return f"bundle:{tx_b64}"

    monkeypatch.setattr(arb_mod, "get_session", fake_arb_session)
    monkeypatch.setattr(arb_mod, "prepare_signed_tx", fake_prepare_bundle)

    async def run_checks():
        agent = execution_mod.ExecutionAgent(depth_service=True, dry_run=False)

        action = {
            "token": "TEST",
            "side": "buy",
            "amount": 1.0,
            "price": 0.0,
            "venues": ["service"],
        }

        result = await agent.execute(action)
        agent.close()

        assert result == {"queued": True}
        assert execution_calls == ["https://venue.test/api/custom_swap"]

        message = await sandwich_mod._fetch_swap_tx_message(
            "TEST", "buy", 1.0, 0.0, "https://venue.test/api/"
        )

        assert message == "sandwich"
        assert sandwich_calls == ["https://venue.test/api/custom_swap"]

        bundle = await arb_mod._prepare_service_tx(
            "TEST", "buy", 1.0, 0.0, "https://venue.test/api/"
        )

        assert bundle == "bundle:arb"
        assert arb_calls == ["https://venue.test/api/custom_swap"]

    asyncio.run(run_checks())
