import asyncio
import pytest
from solhunter_zero import scanner_onchain

try:  # pragma: no cover - optional dependency in test environment
    from solana.publickey import PublicKey
except Exception:  # pragma: no cover
    class PublicKey:  # type: ignore[override]
        def __init__(self, value: str):
            self.value = value


def setup_function(_):
    scanner_onchain.MEMPOOL_RATE_CACHE = scanner_onchain.TTLCache(
        maxsize=256, ttl=scanner_onchain.METRIC_CACHE_TTL
    )
    scanner_onchain.WHALE_ACTIVITY_CACHE = scanner_onchain.TTLCache(
        maxsize=256, ttl=scanner_onchain.METRIC_CACHE_TTL
    )
    scanner_onchain.AVG_SWAP_SIZE_CACHE = scanner_onchain.TTLCache(
        maxsize=256, ttl=scanner_onchain.METRIC_CACHE_TTL
    )
    scanner_onchain._SEEN_LOCAL_CACHE.clear()
    scanner_onchain._DISCOVERY_STATE_MEMORY.clear()
    scanner_onchain._DISCOVERY_REDIS = None
    scanner_onchain._DISCOVERY_REDIS_ATTEMPTED = False
    scanner_onchain._DISCOVERY_HEALTH.last_success_ts = None
    scanner_onchain._DISCOVERY_HEALTH.last_error = None
    scanner_onchain._DISCOVERY_HEALTH.last_error_ts = None
    scanner_onchain._DISCOVERY_HEALTH.last_cursor = None
    scanner_onchain._DISCOVERY_HEALTH.breaker_open_until = 0.0
    scanner_onchain._DISCOVERY_HEALTH.consecutive_failures = 0
    scanner_onchain._DISCOVERY_OVERFETCH_MULT = 1.5
    scanner_onchain._DISCOVERY_CIRCUIT_FAILS = 5
    scanner_onchain._DISCOVERY_CIRCUIT_COOLDOWN = 60.0
    scanner_onchain._DISCOVERY_CIRCUIT_WINDOW = 30.0
    scanner_onchain._DAS_SHADOW_ONLY = False
    scanner_onchain._DAS_MUST = False
    scanner_onchain._DISCOVERY_BUDGET = scanner_onchain._DiscoveryBudget(
        scanner_onchain._DISCOVERY_REQUESTS_PER_MIN
    )

class FakeClient:
    def __init__(self, url):
        self.url = url

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def get_program_accounts(self, program_id, encoding="jsonParsed", **kwargs):
        assert encoding == "jsonParsed"
        return {
            "result": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "name": "mybonk",
                                    "mint": "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz",
                                }
                            }
                        }
                    },
                    "pubkey": "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz",
                },
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "name": "other",
                                    "mint": "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp",
                                }
                            }
                        }
                    },
                    "pubkey": "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp",
                },
            ]
        }

def test_scan_tokens_onchain(monkeypatch):
    captured = {}
    def fake_client(url):
        captured['url'] = url
        return FakeClient(url)
    monkeypatch.setattr(scanner_onchain, "AsyncClient", fake_client)
    monkeypatch.setattr(scanner_onchain, "Client", fake_client)
    tokens = scanner_onchain.scan_tokens_onchain_sync("http://node")
    assert captured['url'] == "http://node"
    assert tokens == [
        "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz",
        "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp",
    ]


def test_scan_tokens_onchain_requires_url():
    with pytest.raises(ValueError):
        scanner_onchain.scan_tokens_onchain_sync("")


class FlakyClient:
    def __init__(self, url):
        self.url = url
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def get_program_accounts(self, program_id, encoding="jsonParsed", **kwargs):
        assert encoding == "jsonParsed"
        self.calls += 1
        if self.calls < 3:
            raise Exception("rpc fail")
        return {
            "result": [
                {
                    "account": {
                        "data": {
                            "parsed": {
                                "info": {
                                    "name": "mybonk",
                                    "mint": "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz",
                                }
                            }
                        }
                    },
                    "pubkey": "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz",
                }
            ]
        }


def test_scan_tokens_onchain_retries(monkeypatch):
    captured = {}
    created: dict[str, FlakyClient] = {}

    def fake_client(url):
        if "client" not in created:
            created["client"] = FlakyClient(url)
        captured["client"] = created["client"]
        return created["client"]

    sleeps = []

    monkeypatch.setattr(scanner_onchain, "AsyncClient", fake_client)
    monkeypatch.setattr(scanner_onchain, "Client", fake_client)

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(scanner_onchain.asyncio, "sleep", fake_sleep)

    tokens = scanner_onchain.scan_tokens_onchain_sync("http://node")

    assert tokens == ["5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz"]
    assert captured["client"].calls >= 3
    assert sleeps == [1, 2]


def test_scan_tokens_onchain_das(monkeypatch):
    monkeypatch.setenv("ONCHAIN_USE_DAS", "1")
    monkeypatch.setenv("ONCHAIN_DAS_PAGE_LIMIT", "5")
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_MIN_ACCOUNTS", 1)
    monkeypatch.setattr(scanner_onchain, "MAX_PROGRAM_SCAN_ACCOUNTS", 5)
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_OVERFETCH", 1.0)

    class FakeSession:
        pass

    session = FakeSession()

    async def fake_get_session():
        return session

    async def fake_search(sess, cursor=None, limit=None):
        assert sess is session
        assert limit == 5
        return (
            [
                {"id": "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz"},
                {"id": "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp"},
            ],
            None,
        )

    monkeypatch.setattr(scanner_onchain, "get_session", fake_get_session)
    monkeypatch.setattr(scanner_onchain, "search_fungible_recent", fake_search)

    result = asyncio.run(
        scanner_onchain.scan_tokens_onchain(
        "http://node",
        return_metrics=False,
        )
    )

    assert result == [
        "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz",
        "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp",
    ]


def test_scan_tokens_onchain_das_metrics(monkeypatch):
    monkeypatch.setenv("ONCHAIN_USE_DAS", "1")
    monkeypatch.setenv("ONCHAIN_DAS_PAGE_LIMIT", "5")
    monkeypatch.setenv("ONCHAIN_DAS_METADATA_LIMIT", "4")
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_MIN_ACCOUNTS", 1)
    monkeypatch.setattr(scanner_onchain, "MAX_PROGRAM_SCAN_ACCOUNTS", 5)
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_OVERFETCH", 1.0)

    class FakeSession:
        pass

    session = FakeSession()

    async def fake_get_session():
        return session

    async def fake_search(sess, cursor=None, limit=None):
        assert sess is session
        assert limit == 5
        return (
            [
                {"id": "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz"},
                {"id": "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp"},
            ],
            None,
        )

    async def fake_batch(sess, mints):
        assert sess is session
        assert mints == [
            "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz",
            "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp",
        ]
        return [
            {
                "id": "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz",
                "token_info": {"symbol": "AAA", "decimals": 6},
            },
            {
                "id": "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp",
                "token_info": {"symbol": "BBB", "decimals": 9},
            },
        ]

    async def fake_liquidity(mint, rpc_url):
        return {
            "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz": 12.0,
            "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp": 7.5,
        }[mint]

    async def fake_volume(mint, rpc_url):
        return {
            "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz": 3.0,
            "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp": 5.5,
        }[mint]

    monkeypatch.setattr(scanner_onchain, "get_session", fake_get_session)
    monkeypatch.setattr(scanner_onchain, "search_fungible_recent", fake_search)
    monkeypatch.setattr(scanner_onchain, "get_asset_batch", fake_batch)
    monkeypatch.setattr(scanner_onchain, "fetch_liquidity_onchain_async", fake_liquidity)
    monkeypatch.setattr(scanner_onchain, "fetch_volume_onchain_async", fake_volume)

    results = asyncio.run(
        scanner_onchain.scan_tokens_onchain(
        "http://node",
        return_metrics=True,
        )
    )

    assert isinstance(results, list)
    expected_mints = {
        "5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz",
        "6ZqSgjdF7Qab3Z9dxMsrdp7sKnzGAbUMHbVgZHbEUcQp",
    }
    assert {item["address"] for item in results} == expected_mints
    result_map = {item["address"]: item for item in results}
    first = result_map["5Y6XQJvZRbGd1U1ur2jcWcN6iYGkc3AuNEqXWiwxC6xz"]
    assert first["symbol"] == "AAA"
    assert first["decimals"] == 6
    assert first["liquidity"] == pytest.approx(12.0)
    assert first["volume"] == pytest.approx(3.0)
    assert first["sources"] == {"onchain", "helius-das"}
    assert first["meta"]["symbol"] == "AAA"
    assert first["meta"]["decimals"] == 6
    assert first["meta"]["verified"] is True


def test_scan_tokens_onchain_das_filters(monkeypatch):
    monkeypatch.setenv("ONCHAIN_USE_DAS", "1")
    monkeypatch.setenv("ONCHAIN_DAS_PAGE_LIMIT", "5")
    scanner_onchain._DISCOVERY_OVERFETCH_MULT = 1.0
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_MIN_ACCOUNTS", 1)
    monkeypatch.setattr(scanner_onchain, "MAX_PROGRAM_SCAN_ACCOUNTS", 5)
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_OVERFETCH", 1.0)

    class FakeSession:
        pass

    session = FakeSession()

    async def fake_get_session():
        return session

    async def fake_search(sess, cursor=None, limit=None):
        assert sess is session
        return (
            [
                {"id": str(scanner_onchain.TOKEN_PROGRAM_ID)},
                {
                    "id": "8fjZs4quYpVqn1nVbWcv16Mo3MHuvb6jVWeNseyXfGPA",
                    "token_info": {"decimals": 15},
                },
                {
                    "id": "4Nd1mQvjcRGnaYCurS9ZwTe74MkRUgyM7pHd3EGf1SLg",
                    "token_info": {"decimals": 6},
                },
            ],
            None,
        )

    monkeypatch.setattr(scanner_onchain, "get_session", fake_get_session)
    monkeypatch.setattr(scanner_onchain, "search_fungible_recent", fake_search)

    result = asyncio.run(
        scanner_onchain._scan_tokens_via_das(
            "http://node",
            return_metrics=False,
            max_tokens=5,
        )
    )

    assert result == ["4Nd1mQvjcRGnaYCurS9ZwTe74MkRUgyM7pHd3EGf1SLg"]


def test_scan_tokens_onchain_das_cursor_persistence(monkeypatch):
    monkeypatch.setenv("ONCHAIN_USE_DAS", "1")
    scanner_onchain._DISCOVERY_OVERFETCH_MULT = 1.0
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_MIN_ACCOUNTS", 1)
    monkeypatch.setattr(scanner_onchain, "MAX_PROGRAM_SCAN_ACCOUNTS", 5)
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_OVERFETCH", 1.0)

    class FakeSession:
        pass

    session = FakeSession()

    async def fake_get_session():
        return session

    async def fake_search(sess, cursor=None, limit=None):
        assert sess is session
        if cursor is None:
            return (
                [
                    {"id": "7YkX5s7Kfdr8N1sC9y1yvE4s46HpPazTA7kGEXUXrXfL"},
                ],
                "cursor-123",
            )
        return ([], None)

    monkeypatch.setattr(scanner_onchain, "get_session", fake_get_session)
    monkeypatch.setattr(scanner_onchain, "search_fungible_recent", fake_search)

    asyncio.run(
        scanner_onchain._scan_tokens_via_das(
            "http://node",
            return_metrics=False,
            max_tokens=1,
        )
    )

    stored = scanner_onchain._DISCOVERY_STATE_MEMORY.get("discovery:cursor")
    assert stored is not None
    assert stored[0] == "cursor-123"


def test_scan_tokens_onchain_das_deduper(monkeypatch):
    monkeypatch.setenv("ONCHAIN_USE_DAS", "1")
    scanner_onchain._DISCOVERY_OVERFETCH_MULT = 1.0
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_MIN_ACCOUNTS", 1)
    monkeypatch.setattr(scanner_onchain, "MAX_PROGRAM_SCAN_ACCOUNTS", 5)
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_OVERFETCH", 1.0)

    class FakeSession:
        pass

    session = FakeSession()

    async def fake_get_session():
        return session

    async def fake_search(sess, cursor=None, limit=None):
        return (
            [
                {"id": "9xzVnKqD9L6s7YQDhCjTiMQuuLHfXEGuVYBnNvKcJste"},
            ],
            None,
        )

    monkeypatch.setattr(scanner_onchain, "get_session", fake_get_session)
    monkeypatch.setattr(scanner_onchain, "search_fungible_recent", fake_search)

    first = asyncio.run(
        scanner_onchain._scan_tokens_via_das(
            "http://node",
            return_metrics=False,
            max_tokens=1,
        )
    )
    second = asyncio.run(
        scanner_onchain._scan_tokens_via_das(
            "http://node",
            return_metrics=False,
            max_tokens=1,
        )
    )

    assert first == ["9xzVnKqD9L6s7YQDhCjTiMQuuLHfXEGuVYBnNvKcJste"]
    assert second == []


def test_scan_tokens_onchain_das_circuit_breaker(monkeypatch):
    monkeypatch.setenv("ONCHAIN_USE_DAS", "1")
    scanner_onchain._DISCOVERY_CIRCUIT_FAILS = 2
    scanner_onchain._DISCOVERY_CIRCUIT_COOLDOWN = 0.1
    scanner_onchain._DISCOVERY_CIRCUIT_WINDOW = 30.0

    class FakeSession:
        pass

    session = FakeSession()

    async def fake_get_session():
        return session

    async def failing_search(*args, **kwargs):
        raise RuntimeError("search failure")

    async def fake_sleep(delay):
        return None

    monkeypatch.setattr(scanner_onchain, "get_session", fake_get_session)
    monkeypatch.setattr(scanner_onchain, "search_fungible_recent", failing_search)
    monkeypatch.setattr(scanner_onchain.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(scanner_onchain, "_should_backoff_heavy_scan", lambda url: True)

    asyncio.run(scanner_onchain.scan_tokens_onchain("http://node"))
    asyncio.run(scanner_onchain.scan_tokens_onchain("http://node"))

    health = scanner_onchain.get_discovery_health()
    assert health["breaker_open"] is True


def test_mempool_tx_rate(monkeypatch):
    class Client:
        def __init__(self, url):
            self.url = url

        def get_signatures_for_address(self, addr, limit=20):
            return {"result": [{"blockTime": 1}, {"blockTime": 3}, {"blockTime": 4}]}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)
    monkeypatch.setattr(scanner_onchain, "fetch_whale_wallet_activity", lambda *a, **k: 0.0)
    monkeypatch.setattr(scanner_onchain, "fetch_average_swap_size", lambda *a, **k: 0.0)
    rate = scanner_onchain.fetch_mempool_tx_rate("tok", "http://node")
    assert rate == pytest.approx(3 / 3)


def test_whale_wallet_activity(monkeypatch):
    class Client:
        def __init__(self, url):
            self.url = url

        def get_token_largest_accounts(self, addr):
            return {"result": {"value": [{"uiAmount": 2000.0}, {"uiAmount": 50.0}]}}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)
    activity = scanner_onchain.fetch_whale_wallet_activity(
        "tok", "http://node", threshold=1000.0
    )
    assert activity == pytest.approx(2000.0 / 2050.0)


def test_average_swap_size(monkeypatch):
    class Client:
        def __init__(self, url):
            self.url = url

        def get_signatures_for_address(self, addr, limit=20):
            return {"result": [{"amount": 2.0}, {"amount": 4.0}]}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)
    size = scanner_onchain.fetch_average_swap_size("tok", "http://node")
    assert size == pytest.approx(3.0)


def test_mempool_tx_rate_cache(monkeypatch):
    calls = {"count": 0}

    class Client:
        def __init__(self, url):
            self.url = url

        def get_signatures_for_address(self, addr, limit=20):
            calls["count"] += 1
            return {"result": [{"blockTime": 1}, {"blockTime": 2}]}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)
    monkeypatch.setattr(scanner_onchain, "fetch_whale_wallet_activity", lambda *a, **k: 0.0)
    monkeypatch.setattr(scanner_onchain, "fetch_average_swap_size", lambda *a, **k: 0.0)

    scanner_onchain.fetch_mempool_tx_rate("tok", "http://node")
    scanner_onchain.fetch_mempool_tx_rate("tok", "http://node")

    assert calls["count"] == 1


def test_whale_wallet_activity_cache(monkeypatch):
    calls = {"count": 0}

    class Client:
        def __init__(self, url):
            self.url = url

        def get_token_largest_accounts(self, addr):
            calls["count"] += 1
            return {"result": {"value": [{"uiAmount": 2000.0}, {"uiAmount": 50.0}]}}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)

    scanner_onchain.fetch_whale_wallet_activity("tok", "http://node")
    scanner_onchain.fetch_whale_wallet_activity("tok", "http://node")

    assert calls["count"] == 1


def test_average_swap_size_cache(monkeypatch):
    calls = {"count": 0}

    class Client:
        def __init__(self, url):
            self.url = url

        def get_signatures_for_address(self, addr, limit=20):
            calls["count"] += 1
            return {"result": [{"amount": 2.0}, {"amount": 4.0}]}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)

    scanner_onchain.fetch_average_swap_size("tok", "http://node")
    scanner_onchain.fetch_average_swap_size("tok", "http://node")

    assert calls["count"] == 1


def test_mempool_tx_rate_model(monkeypatch, tmp_path):
    from solhunter_zero.models.onchain_forecaster import LSTMForecaster, save_model
    import pytest
    torch = pytest.importorskip("torch")
    if not hasattr(torch.nn, "LSTM"):
        pytest.skip("torch LSTM unavailable")

    model = LSTMForecaster(input_dim=4, hidden_dim=2, num_layers=1, seq_len=2)
    with torch.no_grad():
        for p in model.parameters():
            p.zero_()
        model.fc.bias.fill_(0.5)
    path = tmp_path / "oc.pt"
    save_model(model, path)
    monkeypatch.setenv("ONCHAIN_MODEL_PATH", str(path))

    class Client:
        def __init__(self, url):
            self.url = url

        def get_signatures_for_address(self, addr, limit=20):
            return {"result": [{"blockTime": 1}, {"blockTime": 2}]}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)
    import types, sys
    oc = types.SimpleNamespace(order_book_depth_change=lambda *a, **k: 0.0)
    sys.modules["solhunter_zero.onchain_metrics"] = oc
    monkeypatch.setattr(scanner_onchain, "fetch_whale_wallet_activity", lambda t, u: 0.0)
    monkeypatch.setattr(scanner_onchain, "fetch_average_swap_size", lambda t, u: 0.0)

    scanner_onchain.fetch_mempool_tx_rate("tok", "http://node")
    scanner_onchain.MEMPOOL_RATE_CACHE.clear()
    rate = scanner_onchain.fetch_mempool_tx_rate("tok", "http://node")
    assert rate == pytest.approx(0.5)

